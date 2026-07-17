<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\PullServerTrait;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}
 * (#120 pipelined pull engine), chunk 12 - survivors surfaced after the router was rewritten to
 * classify by subject (DATA on the original subject, attributed FIFO; STATUS on the reply token).
 * Each test pins one specific observable behavior a surviving mutant would change, with no timing
 * assertions. Uses the shared {@see PullServerTrait} harness (DATA on 'evt.s', STATUS on the token).
 */
final class JetStreamContext_12MutationTest extends TestCase
{
    use PullServerTrait;

    /** @return int count of pull-request PUBs written so far */
    private function pullPubCount(FakeTransport $transport): int
    {
        return count(array_filter(
            $transport->writes,
            static fn (string $w): bool => str_starts_with($w, 'PUB $JS.API.CONSUMER.MSG.NEXT.'),
        ));
    }

    // kills LogicalOr @ router control-branch guard (if ($pull === null || $pull->done) -> && )
    // pull#0 fills its batch with data 'a' and is retired (token "0" unset from inflight in an earlier
    // readIncoming cycle); a LATE 409 then arrives on that retired token. The real `||` guard sees
    // $pull === null and drops it; the `&&` mutant skips the guard and dereference-assigns
    // $pull->terminalCode on the null pull, throwing a fatal Error that rejects the run's Future.
    public function testStatusForRetiredPullTokenIsDroppedNotReprocessed(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a'], ['status' => 409, 'desc' => 'Consumer Deleted']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $errors = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(1)
            ->setOnError(static function (\Throwable $e) use (&$errors): void {
                $errors[] = $e;
            })
            ->handle(static function (): void {})->await();

        self::assertSame(1, $processed);
        self::assertCount(1, $errors);
    }

    // kills LogicalNot @ reconnect-epoch reset gate (if (!$finite) -> if ($finite))
    // After a forced reconnect (infinite mode) real code clears inflight + re-issues a fresh pull that
    // receives and delivers a token-less data message 'x'; the mutant skips the reset, stays stuck on
    // the retired pre-reconnect pull, terminates on the token-'0' 409, and delivers nothing.
    public function testReconnectReissuesFreshPullInInfiniteMode(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $handshake = $this->infoAndPong();
        $options = new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0);

        $pullSubject = '$JS.API.CONSUMER.MSG.NEXT.S.C';
        $pullSid = 1;
        $base = '';
        $subSeen = 0;
        $pubSeen = 0;
        $transport->onWrite = static function (string $bytes) use ($pullSubject, $handshake, &$pullSid, &$base, &$subSeen, &$pubSeen): array {
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
                    $hdr = "NATS/1.0 409 Consumer Deleted\r\n\r\n";

                    return [sprintf("HMSG %s.0 %d %d %d\r\n%s\r\n", $base, $pullSid, strlen($hdr), strlen($hdr), $hdr)];
                }

                return [];
            }
            if (!str_starts_with($head, 'PUB ' . $pullSubject . ' ')) {
                return [];
            }
            ++$pubSeen;
            if ($pubSeen === 1) {
                return [FakeTransport::EOF, $handshake[0], $handshake[1]];
            }

            return ["MSG evt.s $pullSid \$JS.ACK.S.C.1.1.1.0.0 1\r\nx\r\n"];
        };

        $client = new NatsClient($options, $transport);
        $client->connect()->await();
        $js = $client->jetStream();

        $received = [];
        $iter = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(1)
            ->setOnError(static function (): void {});
        $iter->handle(function (NatsMessage $m) use (&$received, $iter): void {
            $received[] = $m->payload;
            $iter->stop();
        })->await();

        self::assertGreaterThanOrEqual(2, count($transport->connectCalls));
        self::assertSame(['x'], $received);
    }

    // kills DecrementInteger @ reconnect reset ($consecutiveEmptyPulls = 0 -> = -1)
    // Real seeds 0: the post-reconnect no_wait-404 streak's boundary ++ makes it 1, clamping the
    // next generation to depth 1 -> 6 total pull PUBs. The mutant seeds -1: the ++ makes it 0, so the
    // next generation stays depth 2 -> 7 total pull PUBs.
    public function testReconnectResetsConsecutiveEmptyPullsToZero(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $handshake = $this->infoAndPong();
        $options = new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0);

        $pullSubject = '$JS.API.CONSUMER.MSG.NEXT.S.C';
        $pullSid = 1;
        $base = '';
        $pubSeen = 0;
        $transport->onWrite = static function (string $bytes) use ($pullSubject, $handshake, &$pullSid, &$base, &$pubSeen): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }
            if (str_starts_with($head, 'SUB _INBOX.JS.PULL.') && str_contains($head, '.* ')) {
                $subject = explode(' ', $head)[1] ?? '';
                $base = substr($subject, 0, -2);
                $pullSid = (int) (explode(' ', $head)[2] ?? 1);

                return [];
            }
            if (!str_starts_with($head, 'PUB ' . $pullSubject . ' ')) {
                return [];
            }
            $replyTo = explode(' ', $head)[2] ?? '';
            ++$pubSeen;
            if ($pubSeen === 1) {
                return [FakeTransport::EOF, $handshake[0], $handshake[1]];
            }
            if ($pubSeen === 2) {
                return [];
            }
            if ($pubSeen === 3 || $pubSeen === 4 || $pubSeen === 5) {
                // #169 streak semantics (depth 2): empty#1 (refill), empty#2 (latch), empty#3
                // (drain-out) -> warranted boundary, THEN the terminal generation.
                $hdr = "NATS/1.0 404 No Messages\r\n\r\n";

                return [sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $pullSid, strlen($hdr), strlen($hdr), $hdr)];
            }

            $hdr = "NATS/1.0 409 Consumer Deleted\r\n\r\n";

            return [sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $pullSid, strlen($hdr), strlen($hdr), $hdr)];
        };

        $client = new NatsClient($options, $transport);
        $client->connect()->await();
        $js = $client->jetStream();

        $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setNoWait(true)
            ->setOnError(static function (): void {})
            ->handle(static function (): void {})->await();

        self::assertGreaterThanOrEqual(2, count($transport->connectCalls));
        self::assertSame(6, $this->pullPubCount($transport));
    }

    // kills FalseValue @ reconnect reset ($backoffWarranted = false -> = true)
    // Real resets false: the post-reconnect waiting-404 streak's boundary does NOT ++, so the next
    // generation fans out to depth 2 -> 7 total pull PUBs. The mutant resets true: the boundary ++
    // clamps the next generation to depth 1 -> 6 total pull PUBs.
    public function testReconnectClearsBackoffWarranted(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $handshake = $this->infoAndPong();
        $options = new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0);

        $pullSubject = '$JS.API.CONSUMER.MSG.NEXT.S.C';
        $pullSid = 1;
        $base = '';
        $pubSeen = 0;
        $transport->onWrite = static function (string $bytes) use ($pullSubject, $handshake, &$pullSid, &$base, &$pubSeen): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }
            if (str_starts_with($head, 'SUB _INBOX.JS.PULL.') && str_contains($head, '.* ')) {
                $subject = explode(' ', $head)[1] ?? '';
                $base = substr($subject, 0, -2);
                $pullSid = (int) (explode(' ', $head)[2] ?? 1);

                return [];
            }
            if (!str_starts_with($head, 'PUB ' . $pullSubject . ' ')) {
                return [];
            }
            $replyTo = explode(' ', $head)[2] ?? '';
            ++$pubSeen;
            if ($pubSeen === 1) {
                return [FakeTransport::EOF, $handshake[0], $handshake[1]];
            }
            if ($pubSeen === 2) {
                return [];
            }
            if ($pubSeen === 3 || $pubSeen === 4 || $pubSeen === 5) {
                // #169 streak semantics (depth 2): the post-reconnect waiting streak is empty#1
                // (refill), empty#2 (latch), empty#3 (drain-out) -> boundary, THEN the terminal gen.
                $hdr = "NATS/1.0 404 No Messages\r\n\r\n";

                return [sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $pullSid, strlen($hdr), strlen($hdr), $hdr)];
            }

            $hdr = "NATS/1.0 409 Consumer Deleted\r\n\r\n";

            return [sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $pullSid, strlen($hdr), strlen($hdr), $hdr)];
        };

        $client = new NatsClient($options, $transport);
        $client->connect()->await();
        $js = $client->jetStream();

        $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setOnError(static function (): void {})
            ->handle(static function (): void {})->await();

        self::assertGreaterThanOrEqual(2, count($transport->connectCalls));
        self::assertSame(7, $this->pullPubCount($transport));
    }

    // kills Continue_ @ 423 stale-pin retire (continue -> break)
    // Two depth-2 pulls are BOTH retire-ready in ONE packed chunk: pull #1 got a 423 (stale pin, empty),
    // pull #2 carries data 'a'. The 423 branch must `continue` so pull #2 is drained in the SAME pass -
    // no fresh pull issued before 'a' (exactly 2 pull PUBs). A `break` runs the (423 does NOT latch
    // idleDraining) issue phase first, fanning a THIRD pull before 'a' is delivered (3 pull PUBs).
    public function testStalePin423RetireDrainsNextReadyPullInSamePass(): void
    {
        $transport = new FakeTransport($this->infoAndPong());

        $pullSubject = '$JS.API.CONSUMER.MSG.NEXT.S.C';
        $pullSid = 1;
        $t1Reply = '';
        $pubCount = 0;
        $transport->onWrite = static function (string $bytes) use ($pullSubject, &$pullSid, &$t1Reply, &$pubCount): array {
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

            ++$pubCount;
            $replyTo = explode(' ', $head)[2] ?? '';
            if ($pubCount === 1) {
                $t1Reply = $replyTo;

                return [];
            }

            if ($pubCount === 2) {
                $hdr = "NATS/1.0 423 Pin ID mismatch\r\n\r\n";
                $status = sprintf("HMSG %s %d %d %d\r\n%s\r\n", $t1Reply, $pullSid, strlen($hdr), strlen($hdr), $hdr);
                $data = sprintf("MSG evt.s %d \$JS.ACK.S.C.1.1.1.0.0 %d\r\n%s\r\n", $pullSid, 1, 'a');

                return [$status . $data];
            }

            return [];
        };

        $js = $this->context($transport);

        $received = [];
        $pubsAtDelivery = null;
        $iter = $js->pullConsumer('S', 'C')->setBatching(1)->setDepth(2);
        $processed = $iter->handle(function (NatsMessage $m) use (&$received, &$pubsAtDelivery, $transport, $iter): void {
            $received[] = $m->payload;
            $pubsAtDelivery ??= $this->pullPubCount($transport);
            $iter->stop();
        })->await();

        self::assertSame(['a'], $received);
        self::assertSame(1, $processed);
        self::assertSame(2, $pubsAtDelivery, 'a 423 must continue draining the ready generation in the same pass, issuing no fresh pull before delivering the next ready pull');
    }

    // kills LogicalAnd @ generation-boundary backoff guard
    // (!finite && !drain && inflight===[] && idleDraining -> ((!finite && !drain) || inflight===[]) && idleDraining)
    // Pulls #0+#1 are plain WAITING 404s whose rolling streak (2 >= depth) latches idleDraining while the
    // refilled pull #2 is still in flight; at the boundary inflight !== [] and idleDraining is true. The
    // real guard needs inflight === [], so it does NOT enter the backoff block, keeps idleDraining
    // latched, and issues NO fourth pull before pull #2's terminal 409: 3 pull PUBs. The mutant enters
    // via its extra disjunct, un-latches idleDraining, and the issue phase fans out a FOURTH pull: 4 PUBs.
    public function testInflightNonEmptyIdleDrainingDoesNotUnlatchAndOverIssue(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        // #169 streak semantics (depth 2): pull#0's empty (streak 1) refills pull#2; pull#1's empty
        // (streak 2) latches idleDraining WHILE pull#2 is still in flight - exactly the
        // latched-with-inflight state the boundary guard must not enter.
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 404, 'desc' => 'No Messages']],       // pull#0: streak 1 (refills #2)
            [['status' => 404, 'desc' => 'No Messages']],       // pull#1: streak 2 -> latch (inflight: #2)
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#2: terminal while latched
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#3: only an over-issuing mutant reaches it
        ]);
        $js = $this->context($transport);

        $errors = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setOnError(static function (\Throwable $e) use (&$errors): void {
                $errors[] = $e;
            })
            ->handle(static function (): void {})->await();

        self::assertSame(0, $processed);
        self::assertCount(1, $errors);
        self::assertSame(3, $this->pullPubCount($transport));
    }

    // kills FalseValue @ generation-boundary backoff reset ($backoffWarranted = false -> = true)
    // WAITING empties (plain 404) must NEVER arm the backoff, so consecutiveEmptyPulls stays 0 and
    // EVERY generation fans out to depth 2: gen1(404,404) + gen2(404,404) + gen3(terminal 409) = 6 pull
    // PUBs. If the reset leaks true, gen1's boundary leaves it true, gen2's boundary wrongly increments
    // the empty streak, and gen3 is clamped to depth 1 -> 5 pull PUBs.
    public function testBackoffWarrantedResetSoWaitingEmptiesNeverArmBackoffAcrossGenerations(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        // #169 streak semantics (depth 2): each streak is empty#1 (refill), empty#2 (latch), empty#3
        // (drain-out), then the boundary; two full waiting streaks, then the terminal generation.
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 404, 'desc' => 'No Messages']],       // streak1: 1 (refills #2)
            [['status' => 404, 'desc' => 'No Messages']],       // streak1: 2 -> latch
            [['status' => 404, 'desc' => 'No Messages']],       // streak1: drains out -> boundary
            [['status' => 404, 'desc' => 'No Messages']],       // streak2: latches immediately (rolling >= 2)
            [['status' => 404, 'desc' => 'No Messages']],       // streak2: drains out -> boundary
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // gen3 head: terminal
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // gen3 tail: issued only at depth 2
        ]);
        $js = $this->context($transport);

        $errors = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setOnError(static function (\Throwable $e) use (&$errors): void {
                $errors[] = $e;
            })
            ->handle(static function (): void {})->await();

        self::assertSame(0, $processed);
        self::assertCount(1, $errors);
        self::assertSame(7, $this->pullPubCount($transport));
    }
}
