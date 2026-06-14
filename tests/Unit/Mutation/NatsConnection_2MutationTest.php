<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\DeferredCancellation;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Mutation-killing tests for the request / requestMany code paths in
 * {@see \IDCT\NATS\Connection\NatsConnection} (chunk 2, lines ~695-937).
 *
 * Each test pins a SPECIFIC observable behavior that one (or several closely-related) Infection
 * mutants would change. Timing-sensitive collection-loop mutants are exercised against a
 * FakeTransport so the difference shows up as either a different returned set/count or a
 * grossly-different completion deadline (asserted with a monotonic hrtime() bound, never wall clock).
 */
final class NatsConnection_2MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * Builds an already-open connection over a FakeTransport seeded with the given post-handshake
     * read chunks.
     *
     * @param list<string> $reads
     *
     * @return array{0: NatsConnection, 1: FakeTransport}
     */
    private function openConnection(array $reads = [], bool $blockWhenEmpty = false): array
    {
        $transport = new FakeTransport(
            array_merge([self::INFO, "PONG\r\n"], $reads),
            blockWhenEmpty: $blockWhenEmpty,
        );
        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        return [$connection, $transport];
    }

    /**
     * Returns elapsed seconds via the monotonic clock (never microtime/time).
     */
    private function elapsed(int $startNs): float
    {
        return (hrtime(true) - $startNs) / 1e9;
    }

    // kills MethodCallRemoval @ 695 (validateSubject in request())
    public function testRequestValidatesSubjectBeforeSubscribing(): void
    {
        [$connection, $transport] = $this->openConnection();

        $caught = null;
        try {
            $connection->request('', 'p', 100)->await();
        } catch (ProtocolException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(ProtocolException::class, $caught);
        // The real code rejects the subject BEFORE any inbox subscription is created, so no SUB frame
        // is written. With validateSubject removed the inbox SUB is written before publish() throws.
        self::assertSame([], $this->subFrames($transport));
    }

    // kills MethodCallRemoval @ 716 (validateSubject in requestWithHeaders())
    public function testRequestWithHeadersValidatesSubjectBeforeSubscribing(): void
    {
        [$connection, $transport] = $this->openConnection();

        $caught = null;
        try {
            $connection->requestWithHeaders('', 'p', ['X' => '1'], 100)->await();
        } catch (ProtocolException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(ProtocolException::class, $caught);
        self::assertSame([], $this->subFrames($transport));
    }

    // kills MethodCallRemoval @ 839 (validateSubject in requestMany())
    public function testRequestManyValidatesSubjectBeforeSubscribing(): void
    {
        [$connection, $transport] = $this->openConnection();

        $caught = null;
        try {
            $connection->requestMany('', 'p', null, 3, 100)->await();
        } catch (ProtocolException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(ProtocolException::class, $caught);
        self::assertSame([], $this->subFrames($transport));
    }

    // kills Concat @ 783 and ConcatOperandRemoval @ 783 (timeout message keeps "<prefix> <subject>")
    public function testRequestTimeoutMessageIncludesPrefixThenSubject(): void
    {
        [$connection] = $this->openConnection();

        $message = null;
        try {
            $connection->request('svc.echo', '{"x":1}', 20)->await();
            self::fail('expected TimeoutException');
        } catch (TimeoutException $e) {
            $message = $e->getMessage();
        }

        // Real message: "Request timed out for subject svc.echo".
        //  - Concat mutant swaps operands -> "svc.echoRequest timed out for subject "
        //  - ConcatOperandRemoval drops the subject -> "Request timed out for subject "
        self::assertSame('Request timed out for subject svc.echo', $message);
    }

    // kills TrueValue @ 886 ($noResponders flag must stop collection immediately on a 503 sentinel)
    public function testRequestManyStopsImmediatelyOnNoRespondersSentinel(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $sentinel = 'HMSG _INBOX.any 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n";
        [$connection] = $this->openConnection([$sentinel]);

        $start = hrtime(true);
        // Large total budget: the real code returns [] the instant the 503 arrives. If $noResponders is
        // never set to true the loop keeps polling until the whole 2s budget elapses.
        $replies = $connection->requestMany('svc.scan', 'q', null, null, 2000)->await();

        self::assertSame([], $replies);
        self::assertLessThan(0.5, $this->elapsed($start), 'no-responders must short-circuit, not run the full budget');
    }

    // kills GreaterThanOrEqualTo @ 913 and LogicalAndAllSubExprNegation @ 913 (maxResponses cap "count >= max")
    public function testRequestManyStopsExactlyAtMaxResponses(): void
    {
        [$connection] = $this->openConnection(["MSG _INBOX.any 1 1\r\nA\r\n"]);

        $start = hrtime(true);
        // Exactly one reply with maxResponses=1: "count >= max" breaks at the boundary. With ">" or the
        // negated guard the cap never fires and collection runs until the 2s total budget expires.
        $replies = $connection->requestMany('svc.scan', 'q', null, 1, 2000)->await();

        self::assertCount(1, $replies);
        self::assertSame('A', $replies[0]->payload);
        self::assertLessThan(0.5, $this->elapsed($start), 'maxResponses cap must fire at the boundary');
    }

    // kills Minus @ 920, Multiplication @ 920, GreaterThanOrEqualToNegotiation @ 920
    // (these mutants make the stall fire immediately after the first reply)
    public function testRequestManyDoesNotStallBeforeStallIntervalElapses(): void
    {
        // Two replies in SEPARATE reads so the loop re-evaluates the stall guard between them.
        [$connection] = $this->openConnection([
            "MSG _INBOX.any 1 1\r\nA\r\n",
            "MSG _INBOX.any 1 1\r\nB\r\n",
        ]);

        // Large stall (5s) so the genuine gap between the two reads never trips it; maxResponses=2 makes
        // the real code stop promptly after collecting both. Mutants that compute the gap as
        // (now+lastAt)*1000, now-lastAt/1000, or test "< stallMs" all break after the first reply.
        $replies = $connection->requestMany('svc.scan', 'q', null, 2, 5000, 5000)->await();

        $payloads = array_map(static fn(NatsMessage $m): string => $m->payload, $replies);
        self::assertSame(['A', 'B'], $payloads);
    }

    // kills NotIdentical @ 920 (stallMs===null), LogicalAndAllSubExprNegation @ 920,
    //       NotIdentical @ 936 (stallMs===null), NotIdentical @ 936 (lastAt===null),
    //       LogicalAndAllSubExprNegation @ 936, LogicalAndNegation @ 936, Division @ 937
    // (all of these either disable the stall break or stop shortening the wake slice, so collection
    //  runs until the full total budget instead of stopping shortly after the stall interval)
    public function testRequestManyStopsShortlyAfterStallInterval(): void
    {
        // One reply, then the socket blocks (idle). The stall must wake us ~15ms later and stop. The
        // blocking transport makes the wake-slice computation load-bearing, so the @936/@937 slice
        // mutants are observable too.
        [$connection] = $this->openConnection(
            ["MSG _INBOX.any 1 1\r\nA\r\n"],
            blockWhenEmpty: true,
        );

        $start = hrtime(true);
        // stall=15ms, total=3000ms: the real code returns ~15-40ms after the reply. Any mutant that
        // disables the stall or fails to shorten the idle wake slice runs the full 3s budget.
        $replies = $connection->requestMany('svc.scan', 'q', null, null, 3000, 15)->await();

        self::assertCount(1, $replies);
        self::assertSame('A', $replies[0]->payload);
        self::assertLessThan(0.5, $this->elapsed($start), 'stall interval must stop collection well before the total budget');
    }

    // kills LogicalAnd @ 926 ($cancellation !== null && $cancellation->isRequested())
    public function testRequestManyReturnsCollectedSetWhenTotalElapsesWithUnrequestedExternalCancellation(): void
    {
        // An external cancellation is supplied but never requested. When the total budget elapses the
        // real code must RETURN the (empty) collected set, not throw. With "&&" mutated to "||" the
        // mere presence of a cancellation token throws CancelledException at the deadline.
        [$connection] = $this->openConnection([], blockWhenEmpty: true);

        $external = new DeferredCancellation();

        $replies = $connection->requestMany('svc.scan', 'q', null, null, 50, null, $external->getCancellation())->await();

        self::assertSame([], $replies);
    }

    /**
     * Extracts the inbox SUB control frames written to the transport (handshake writes excluded).
     *
     * @return list<string>
     */
    private function subFrames(FakeTransport $transport): array
    {
        return array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_starts_with($w, 'SUB '),
        ));
    }
}
