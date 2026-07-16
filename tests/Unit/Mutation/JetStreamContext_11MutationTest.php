<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\Consumers\PullPipelineConfig;
use IDCT\NATS\JetStream\Consumers\PullPipelineControl;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for the #120 pipelined pull engine on JetStreamContext:
 * effectivePullDepth() (serial-until-pinned, idle-streak clamp, max(1,depth) floor) and the
 * pull-request overlap it drives via consumePipelined().
 *
 * Chunk 4/4 survivors. The harness mirrors PullPipelineTest::pullServer(): a FakeTransport onWrite
 * responder learns the long-lived pull inbox sid from its wildcard SUB and, for each pull PUB, echoes
 * the next queued frame-set on that pull's captured reply-to. Replies are delivered synchronously at
 * write time, so the tests are fully deterministic with no real network and no wall-clock timing.
 */
final class JetStreamContext_11MutationTest extends TestCase
{
    /** @return list<string> INFO + PONG so connect() succeeds. */
    private function infoPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    /**
     * Installs a pull "server": for each pull PUB to $JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer> it
     * pops the next FIFO entry of $perPull and emits its frames on that pull's captured reply-to.
     *
     * @param list<list<array{msg?: string, status?: int, desc?: string}>> $perPull
     */
    private function pullServer(FakeTransport $transport, string $stream, string $consumer, array $perPull): void
    {
        $pullSubject = '$JS.API.CONSUMER.MSG.NEXT.' . $stream . '.' . $consumer;
        $pullSid = 1;
        $index = 0;
        $transport->onWrite = static function (string $bytes) use ($pullSubject, &$pullSid, &$index, $perPull): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB _INBOX.JS.PULL.') && str_contains($head, '.* ')) {
                $pullSid = (int) (explode(' ', $head)[2] ?? 1);

                return [];
            }

            if (!str_starts_with($head, 'PUB ' . $pullSubject . ' ')) {
                return [];
            }

            $replyTo = explode(' ', $head)[2] ?? '';
            if ($replyTo === '' || !isset($perPull[$index])) {
                return [];
            }

            $frames = [];
            foreach ($perPull[$index] as $frame) {
                if (isset($frame['msg'])) {
                    // DATA: original subject + $JS.ACK reply (never the pull token); attributed FIFO.
                    $frames[] = sprintf("MSG evt.s %d \$JS.ACK.S.C.1.1.1.0.0 %d\r\n%s\r\n", $pullSid, strlen($frame['msg']), $frame['msg']);
                } elseif (isset($frame['status'])) {
                    // STATUS: on the reply token so the engine correlates it to this pull.
                    $hdr = sprintf("NATS/1.0 %d %s\r\n\r\n", $frame['status'], $frame['desc'] ?? '');
                    $frames[] = sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $pullSid, strlen($hdr), strlen($hdr), $hdr);
                }
            }
            ++$index;

            return $frames;
        };
    }

    private function context(FakeTransport $transport): JetStreamContext
    {
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        return $client->jetStream();
    }

    /** @return int count of pull-request PUBs written so far */
    private function pullPubCount(FakeTransport $transport): int
    {
        return count(array_filter(
            $transport->writes,
            static fn (string $w): bool => str_starts_with($w, 'PUB $JS.API.CONSUMER.MSG.NEXT.'),
        ));
    }

    /**
     * effectivePullDepth() returns 1 for a grouped run whose pin id is not yet resolved
     * (`$cfg->grouped && $ctl->getPinId() === null`), so a pinned fan-out cannot race the server's pin
     * assignment. With depth=2 that forces a SERIAL first generation: only pull #1 is on the wire when
     * its first message is delivered (contrast PullPipelineTest's ungrouped depth=2 -> 2 pulls).
     *
     * A plain MSG carries no Nats-Pin-Id, so the pin stays null and depth stays clamped to 1.
     */
    public function testGroupedRunStaysSerialUntilPinResolved(): void
    {
        // kills NotIdentical (=== -> !==) @ 2499 and LogicalAndAllSubExprNegation @ 2499:
        // both mutations flip "grouped & unpinned" so effectivePullDepth would fan out to depth (2)
        // instead of 1, putting 2 pulls on the wire before the first delivery.
        $transport = new FakeTransport($this->infoPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $pubsAtFirstDelivery = null;
        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setGroup('g1')
            ->handle(function (NatsMessage $m) use (&$pubsAtFirstDelivery, &$received, $transport): void {
                $pubsAtFirstDelivery ??= $this->pullPubCount($transport);
                $received[] = $m->payload;
            })->await();

        self::assertSame(1, $processed);
        self::assertSame(['a'], $received);
        self::assertSame(
            1,
            $pubsAtFirstDelivery,
            'grouped + unresolved pin must stay serial (effectivePullDepth=1): only pull #1 on the wire',
        );
    }

    /**
     * effectivePullDepth() clamps to 1 while an idle streak is active (`$consecutiveEmptyPulls > 0`),
     * keeping the idle poll rate at one pull per backoff window (#153). After a whole depth=2 generation
     * comes back empty (two 404s, no_wait) and the streak increments, the NEXT generation issues exactly
     * ONE pull, not depth (2). Total pulls on the wire: gen1 (2) + gen2 (1 terminal 409) = 3.
     */
    public function testIdleStreakClampsEffectiveDepthToOne(): void
    {
        // kills ReturnRemoval @ 2504 (drop "return 1;") which would fall through to max(1,depth)=2,
        // issuing a SECOND pull in the post-idle generation -> 4 pulls total instead of 3.
        $transport = new FakeTransport($this->infoPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 404, 'desc' => 'No Messages']],
            [['status' => 404, 'desc' => 'No Messages']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setNoWait(true)
            ->handle(static function (): void {})->await();

        self::assertSame(0, $processed);
        self::assertSame(
            3,
            $this->pullPubCount($transport),
            'post-idle generation must be clamped to a single pull (gen1=2 + gen2=1)',
        );
    }

    /**
     * effectivePullDepth() floors the steady-state depth at 1 via max(1, $cfg->depth): a depth of 0 must
     * still issue (and deliver) one pull. Driven straight through consumePipelined() with a depth-0
     * config (the public iterator's setDepth() rejects depth<=0, so max(0,depth) would be silently
     * indistinguishable there - the guard only bites below 1).
     */
    public function testZeroDepthIsFlooredToOneAndStillDelivers(): void
    {
        // kills DecrementInteger @ 2507 (max(1,depth) -> max(0,depth)): with depth=0 the mutant yields
        // effectiveDepth=0, the issue loop fires no pull, inflight stays empty and the run returns 0.
        $transport = new FakeTransport($this->infoPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'x']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $config = new PullPipelineConfig(
            batch: 1,
            expiresMs: 1000,
            depth: 0,
            iterations: null,
            noWait: false,
            grouped: false,
            pullFields: [],
            idleHeartbeatNs: null,
            onError: null,
        );
        $control = new PullPipelineControl(
            stopFn: static fn (): bool => false,
            drainFn: static fn (): bool => false,
            getPinFn: static fn (): ?string => null,
            setPinFn: static function (?string $pinId): void {},
        );

        $received = [];
        $processed = $js->consumePipelined('S', 'C', $config, function (NatsMessage $m) use (&$received): void {
            $received[] = $m->payload;
        }, $control)->await();

        self::assertSame(1, $processed, 'max(1,depth) must floor a depth-0 run to one live pull');
        self::assertSame(['x'], $received);
        self::assertGreaterThanOrEqual(1, $this->pullPubCount($transport));
    }
}
