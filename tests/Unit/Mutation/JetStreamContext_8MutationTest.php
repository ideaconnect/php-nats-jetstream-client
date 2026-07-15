<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Consumers\PullPipelineConfig;
use IDCT\NATS\JetStream\Consumers\PullPipelineControl;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see JetStreamContext::consumePipelined()} / effectivePullDepth() /
 * pullStatusException() (#120 pipelined pull engine), chunk 8. Each test pins one specific observable
 * behavior a surviving mutant would change. The harness mirrors PullPipelineTest: a FakeTransport whose
 * onWrite learns the long-lived pull inbox sid from its wildcard SUB and echoes per-pull replies on the
 * captured token-routed reply-to `_INBOX.JS.PULL.<base>.<token>`.
 */
final class JetStreamContext_8MutationTest extends TestCase
{
    /** @return list<string> INFO + PONG so connect() succeeds. */
    private function infoPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function context(FakeTransport $transport, ?NatsOptions $options = null): JetStreamContext
    {
        $client = new NatsClient($options ?? new NatsOptions(), $transport);
        $client->connect()->await();

        return $client->jetStream();
    }

    /**
     * FIFO per-pull "server" that emits each pull's frames as SEPARATE read chunks (one chunk per
     * frame), exactly like PullPipelineTest::pullServer.
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
                ++$index;

                return [];
            }

            $frames = [];
            foreach ($perPull[$index] as $frame) {
                $frames[] = self::wireFrame($replyTo, $pullSid, $frame);
            }
            ++$index;

            return $frames;
        };
    }

    /**
     * Like {@see pullServer()} but packs ALL of a pull's frames into a SINGLE read chunk, so the
     * connection enqueues them into the per-sid pending queue together (before draining) - the only way
     * to exercise the router's post-batch drop and the slow-consumer bound in-process.
     *
     * @param list<list<array{msg?: string, status?: int, desc?: string}>> $perPull
     */
    private function packedPullServer(FakeTransport $transport, string $stream, string $consumer, array $perPull): void
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
                ++$index;

                return [];
            }

            $chunk = '';
            foreach ($perPull[$index] as $frame) {
                $chunk .= self::wireFrame($replyTo, $pullSid, $frame);
            }
            ++$index;

            return $chunk === '' ? [] : [$chunk];
        };
    }

    /** @param array{msg?: string, status?: int, desc?: string} $frame */
    private static function wireFrame(string $replyTo, int $sid, array $frame): string
    {
        if (isset($frame['msg'])) {
            // DATA: a real server delivers stored messages on their ORIGINAL subject with a $JS.ACK
            // reply (never the pull reply token), so the engine attributes them FIFO, not by token.
            return sprintf("MSG evt.s %d \$JS.ACK.S.C.1.1.1.0.0 %d\r\n%s\r\n", $sid, strlen($frame['msg']), $frame['msg']);
        }

        // STATUS/heartbeat: delivered ON the reply token so the engine correlates it to its pull.
        $hdr = sprintf("NATS/1.0 %d %s\r\n\r\n", $frame['status'] ?? 0, $frame['desc'] ?? '');

        return sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $sid, strlen($hdr), strlen($hdr), $hdr);
    }

    /** @return int count of pull-request PUBs written so far */
    private function pullPubCount(FakeTransport $transport): int
    {
        return count(array_filter(
            $transport->writes,
            static fn (string $w): bool => str_starts_with($w, 'PUB $JS.API.CONSUMER.MSG.NEXT.'),
        ));
    }

    private function minimalConfig(?int $iterations = 1): PullPipelineConfig
    {
        return new PullPipelineConfig(
            batch: 1,
            expiresMs: 1000,
            depth: 1,
            iterations: $iterations,
            noWait: false,
            grouped: false,
            pullFields: [],
            idleHeartbeatNs: null,
            onError: null,
        );
    }

    private function inertControl(): PullPipelineControl
    {
        return new PullPipelineControl(
            stopFn: static fn (): bool => false,
            drainFn: static fn (): bool => false,
            getPinFn: static fn (): ?string => null,
            setPinFn: static function (?string $pinId): void {},
        );
    }

    // -----------------------------------------------------------------------------------------------
    // kills MethodCallRemoval @ 2171 (self::assertValidJsName($stream, 'stream'))
    // pullConsumer() also validates, so the engine-internal assert is only reachable by calling
    // consumePipelined() directly. A bad stream + valid consumer throws on real code; with the stream
    // assert removed the (valid) consumer assert passes and a Future is returned instead of throwing.
    public function testConsumePipelinedRejectsInvalidStreamNameSynchronously(): void
    {
        $js = $this->context(new FakeTransport($this->infoPong()));

        $this->expectException(JetStreamException::class);
        $js->consumePipelined('bad.stream', 'C', $this->minimalConfig(), static function (): void {}, $this->inertControl());
    }

    // kills MethodCallRemoval @ 2172 (self::assertValidJsName($consumer, 'consumer'))
    // Valid stream + bad consumer: real throws on the consumer assert; with it removed the run proceeds.
    public function testConsumePipelinedRejectsInvalidConsumerNameSynchronously(): void
    {
        $js = $this->context(new FakeTransport($this->infoPong()));

        $this->expectException(JetStreamException::class);
        $js->consumePipelined('S', 'bad*consumer', $this->minimalConfig(), static function (): void {}, $this->inertControl());
    }

    // kills ConcatOperandRemoval @ 2177 ($prefix = $base . '.'  ->  $prefix = $base)
    // kills DecrementInteger  @ 2235 ($tokenSeq = 0  ->  $tokenSeq = -1)
    // The first pull's captured reply-to must be exactly "<base>.0": dropping the '.' makes it "<base>0",
    // and starting tokenSeq at -1 makes the token dechex(-1) = "ffffffffffffffff".
    public function testFirstPullReplyToIsBaseDotZeroToken(): void
    {
        $transport = new FakeTransport($this->infoPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setIterations(1)
            ->handle(static function (): void {})->await();

        self::assertSame(1, $processed);

        $subLine = null;
        $pubReplyTo = null;
        foreach ($transport->writes as $write) {
            $head = (string) strtok($write, "\r\n");
            if ($subLine === null && str_starts_with($head, 'SUB _INBOX.JS.PULL.')) {
                $subLine = $head;
            }
            if ($pubReplyTo === null && str_starts_with($head, 'PUB $JS.API.CONSUMER.MSG.NEXT.S.C ')) {
                $pubReplyTo = explode(' ', $head)[2] ?? '';
            }
        }

        self::assertNotNull($subLine);
        self::assertNotNull($pubReplyTo);

        // SUB subject is "<base>.*"; strip the trailing ".*" to recover the base.
        $subject = explode(' ', $subLine)[1] ?? '';
        $base = substr($subject, 0, -2);

        self::assertSame($base . '.0', $pubReplyTo);
    }

    // kills LogicalOr @ 2198 (if ($pull === null || $pull->done)  ->  ... && ...)
    // Over-deliver a batch=1 pull with two messages in ONE chunk: the pull is done after 'a', so the
    // real router DROPS the trailing 'b'. With && the pull is not-null-and-done => 'b' is buffered too.
    public function testTrailingFrameAfterBatchCompletionIsDropped(): void
    {
        $transport = new FakeTransport($this->infoPong());
        $this->packedPullServer($transport, 'S', 'C', [
            [['msg' => 'a'], ['msg' => 'b']],
        ]);
        $js = $this->context($transport);

        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setIterations(1)
            ->handle(function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();

        self::assertSame(1, $processed);
        self::assertSame(['a'], $received);
    }

    // kills DecrementInteger @ 2206 (if ($status === 100)  ->  === 99)
    // kills IncrementInteger @ 2206 (if ($status === 100)  ->  === 101)
    // A status-100 heartbeat must NOT be buffered. If 100 stops matching, the empty-payload heartbeat is
    // treated as a data message: it fills the batch=1 pull and is delivered instead of the real 'a'.
    public function testIdleHeartbeatIsNotBufferedAsAMessage(): void
    {
        $transport = new FakeTransport($this->infoPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 100, 'desc' => ''], ['msg' => 'a']],
        ]);
        $js = $this->context($transport);

        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setIterations(1)
            ->handle(function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();

        self::assertSame(1, $processed);
        self::assertSame(['a'], $received);
    }

    // kills GreaterThanOrEqualTo @ 2211 (if ($status >= 400)  ->  if ($status > 400))
    // Exactly the boundary value 400: real treats it as terminal (fires onError, delivers nothing);
    // with > 400 the status-400 frame is buffered and delivered as a message and onError never fires.
    public function testStatus400IsTerminalAtTheBoundary(): void
    {
        $transport = new FakeTransport($this->infoPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 400, 'desc' => 'Bad Request']],
        ]);
        $js = $this->context($transport);

        $errors = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setIterations(1)
            ->setOnError(static function (\Throwable $e) use (&$errors): void {
                $errors[] = $e;
            })
            ->handle(static function (): void {})->await();

        self::assertSame(0, $processed);
        self::assertCount(1, $errors);
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertSame(400, $errors[0]->getCode());
    }

    // kills MethodCallRemoval @ 2231 ($this->client->markSubscriptionUnbounded($sid))
    // With the pull inbox bounded (limit 2, DropOldest) three messages in one chunk lose the oldest 'a'
    // before the router runs, so a batch=1 pull delivers 'b'. The unbounded mark keeps all three, so the
    // real router delivers 'a' (and drops 'b','c' after the batch completes).
    public function testPullInboxIsExemptFromSlowConsumerBound(): void
    {
        $transport = new FakeTransport($this->infoPong());
        $this->packedPullServer($transport, 'S', 'C', [
            [['msg' => 'a'], ['msg' => 'b'], ['msg' => 'c']],
        ]);
        $js = $this->context($transport, new NatsOptions(maxPendingMessagesPerSubscription: 2));

        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setIterations(1)
            ->handle(function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();

        self::assertSame(1, $processed);
        self::assertSame(['a'], $received);
    }

    // kills FalseValue @ 2242 ($backoffWarranted = false  ->  = true)
    // A first generation of WAITING empties (404, not no_wait/409) must leave backoffWarranted false, so
    // consecutiveEmptyPulls stays 0 and the NEXT generation fans out to the full depth (2). Initialising
    // it true increments consecutiveEmptyPulls, clamping the next generation's depth to 1.
    public function testWaitingEmptyGenerationDoesNotArmBackoff(): void
    {
        $transport = new FakeTransport($this->infoPong());
        // gen1: two depth-2 pulls both 404 (waiting empties). gen2: pull #2 delivers 'a'.
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 404, 'desc' => 'No Messages']],
            [['status' => 404, 'desc' => 'No Messages']],
            [['msg' => 'a']],
        ]);
        $js = $this->context($transport);

        $iter = $js->pullConsumer('S', 'C')->setBatching(1)->setDepth(2);
        $pubsAtFirstDelivery = null;
        $iter->handle(function () use (&$pubsAtFirstDelivery, $transport, $iter): void {
            // The issue phase runs to full effective depth BEFORE this handler is reached, so the pull
            // PUB count already reflects gen2's fan-out here: 2 (gen1) + 2 (gen2) = 4 on real code, but
            // 2 + 1 = 3 if the empty generation wrongly armed the backoff and clamped depth to 1.
            $pubsAtFirstDelivery ??= $this->pullPubCount($transport);
            $iter->stop();
        })->await();

        self::assertSame(4, $pubsAtFirstDelivery);
    }

    // kills LogicalNot @ 2259 (if (!$finite)  ->  if ($finite))
    // A mid-run reconnect (infinite mode) must clear the in-flight pull and re-issue fresh: a reply that
    // arrives on the pre-reconnect token is then dropped. If the guard flips to $finite, infinite mode
    // skips the reset and the stale in-flight pull survives, so that pre-reconnect reply IS delivered.
    public function testInfiniteModeDropsInflightPullsOnReconnect(): void
    {
        $transport = new FakeTransport($this->infoPong());
        $options = new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0);

        $pullSubject = '$JS.API.CONSUMER.MSG.NEXT.S.C';
        $pullSid = 1;
        $base = '';
        $subSeen = 0;
        $pubSeen = 0;
        $transport->onWrite = static function (string $bytes) use ($pullSubject, &$pullSid, &$base, &$subSeen, &$pubSeen): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB _INBOX.JS.PULL.') && str_contains($head, '.* ')) {
                $subject = explode(' ', $head)[1] ?? '';
                $base = substr($subject, 0, -2);
                $pullSid = (int) (explode(' ', $head)[2] ?? 1);
                ++$subSeen;
                if ($subSeen === 2) {
                    // The SUB replay during reconnect: a reply for the pre-reconnect pull (token "0")
                    // now arrives. Real code has cleared inflight["0"], so its router drops it; the
                    // mutant kept it and delivers it.
                    return [sprintf("MSG %s.0 %d %d\r\n%s\r\n", $base, $pullSid, 8, 'survivor')];
                }

                return [];
            }

            if (!str_starts_with($head, 'PUB ' . $pullSubject . ' ')) {
                return [];
            }

            ++$pubSeen;
            if ($pubSeen === 1) {
                // First pull issued -> sever the socket so the next read reconnects, then feed the
                // reconnect handshake (INFO + PONG).
                return [
                    FakeTransport::EOF,
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ];
            }

            // Any post-reconnect pull (token "1") gets a terminal 409 so the run stops promptly.
            $hdr = "NATS/1.0 409 Consumer Deleted\r\n\r\n";

            return [sprintf("HMSG %s.1 %d %d %d\r\n%s\r\n", $base, $pullSid, strlen($hdr), strlen($hdr), $hdr)];
        };

        $js = $this->context($transport, $options);

        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(1)
            ->setOnError(static function (): void {})
            ->handle(function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();

        // Guard: the reconnect actually happened (initial connect + one reconnect), so the assertion
        // below is exercising the epoch-change branch rather than passing vacuously.
        self::assertGreaterThanOrEqual(2, count($transport->connectCalls));
        self::assertSame([], $received, 'a reply on the pre-reconnect token must not be delivered after an infinite-mode reconnect');
        self::assertSame(0, $processed);
    }
}
