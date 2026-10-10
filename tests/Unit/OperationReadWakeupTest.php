<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Transport\CancellableDialTransportInterface;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * Each operation that reads the socket while it waits for a result of its own gets that result as soon as it is
 * delivered, by whichever fiber's read (#174). The operation used to check whether its result was in, find nothing,
 * and start a read of the socket while its message reached it another way; that read then waited for the server's
 * next bytes, or for the operation's deadline, with the result already there. Each test asserts that the operation
 * returns its result, and well before its deadline, in each of the ways a delivery can reach it:
 *
 *  - A delivery still under way: the application's own read took a chunk that holds, ahead of the operation's
 *    message, a message for a subscription with a lower sid, whose handler awaits something other than NATS. The
 *    operation's message is queued behind that delivery, and the operation takes it at once, before the handler is
 *    let go (#179). In the second form a server PING ahead of the operation's message, whose PONG the socket takes
 *    only after a while, holds the dispatch of the chunk up before the message is queued: the operation reads the
 *    socket, and the delivery that brings its message, once the PONG goes out, wakes that read.
 *  - The hop window: between the operation's check and the moment its read takes the read slot, a few event-loop
 *    hops later, another fiber's read takes and delivers the operation's message.
 *  - A reconnect: the operation's read waits for a reconnect that another fiber's read runs, and that reconnect
 *    delivers the operation's message, which the server sent right after the replayed SUB. The operation's read then
 *    reads the new socket.
 *
 * The tests drive each interleaving by the order of events: a hold-up ends as soon as a read takes the socket during
 * it, which can only be the operation's, and the hop sweeps count event-loop hops. Timers only end a hold-up that no
 * read ends, and time the outage of the reconnect tests, whose operation starts waiting within a hop or two, tens of
 * milliseconds before the reconnect. The old reads returned at their deadline in every case: 3 s, or 1.5 s in the hop
 * sweeps, where the fixed ones return within milliseconds.
 */
final class OperationReadWakeupTest extends TestCase
{
    use ReconnectScenarios;

    /** The deadline of each operation outside the hop sweeps, in seconds. */
    private const TIMEOUT = 3.0;

    /** An operation that gets its result in time returns within this many seconds; a broken one at its deadline. */
    private const PROMPT = 1.5;

    /**
     * A hold-up that no read of the socket ends lasts this long: a fix that waits for the delivery under way instead
     * of reading returns after it, well within {@see PROMPT}.
     */
    private const FALLBACK = 0.5;

    /** The deadline of each operation in the hop sweeps, in seconds. */
    private const HOP_WAIT = 1.5;

    /** A run of a hop sweep is prompt below this many seconds. */
    private const HOP_PROMPT = 0.5;

    /** The payload of the message each operation waits for. */
    private const RESULTS = [
        'next' => 'job-1',
        'fetchAll' => 'job-1',
        'request' => 'reply-1',
        'requestMany' => 'reply-1',
        'fetchBatch' => 'm1',
        'directGet' => 'v1',
        'pullConsumer' => 'm1',
        'kvKeys' => 'k',
        'kvHistory' => 'v1',
    ];

    private const DIRECT_GET_HEADERS = "NATS/1.0\r\nNats-Subject: k\r\nNats-Num-Pending: 0\r\n\r\n";

    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';

    /** The reply subject of the one record a Key/Value replay finds: nothing pending behind it. */
    private const KV_ACK_SUBJECT = '$JS.ACK.KV_b.c1.1.1.1.1700000000000000000.0';

    /** @var (\Closure(): void)|null Called as each read of a hooked client's socket starts ({@see connectHooked()}). */
    private ?\Closure $onRead = null;

    /** @var (\Closure(string): void)|null Called with the bytes as each write of a hooked client starts. */
    private ?\Closure $onWrite = null;

    protected function tearDown(): void
    {
        $this->onRead = null;
        $this->onWrite = null;
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, string}> */
    public static function operationsBehindAHandlerThatAwaits(): iterable
    {
        yield 'SubscriptionQueue::next()' => ['next', 'handler'];
        yield 'SubscriptionQueue::fetchAll(1)' => ['fetchAll', 'handler'];
        yield 'request()' => ['request', 'handler'];
        yield 'requestMany(max 1)' => ['requestMany', 'handler'];
        yield 'JetStreamContext::fetchBatch(1)' => ['fetchBatch', 'handler'];
        yield 'JetStreamContext::directGetBatch()' => ['directGet', 'handler'];
        yield 'PullConsumerIterator::handle()' => ['pullConsumer', 'handler'];
        yield 'KeyValueBucket::keys()' => ['kvKeys', 'handler'];
        yield 'KeyValueBucket::history()' => ['kvHistory', 'handler'];
        yield 'guard: next() behind a handler that makes a request()' => ['next', 'request'];
        yield 'guard: request() behind a handler that makes a request()' => ['request', 'request'];
    }

    /**
     * A delivery still under way: the application's read delivers a message to a handler that awaits, and the
     * operation's message is queued behind it. The handler awaits a timer, the way it would await an HTTP or a database call, until a read
     * takes the socket meanwhile, which only the operation's can, or for half a second when none does.
     *
     * The guards hold the same staging with a handler that makes a request() of its own: the reason the read slot is
     * free during a delivery. That request must get its reply, and the operation its message, without either waiting
     * for the other.
     *
     * With the message queued behind the awaiting handler, the operation takes it at once and the handler is still held
     * when the result is in (#179): the operation never waits for the handler to be let go.
     */
    #[DataProvider('operationsBehindAHandlerThatAwaits')]
    public function testOperationGetsWhatADeliveryUnderWayInAnotherFiberBrings(string $operation, string $handlerAwaits): void
    {
        [$payloads, $elapsed, $error, $endedBy, $heldWhenDone] = $this->runBehindAHeldUpDelivery($operation, $handlerAwaits);

        $this->assertPrompt($operation, $payloads, $elapsed, $error, self::PROMPT, 'the handler was let go by ' . $endedBy);
        if ($handlerAwaits === 'handler' && $operation !== 'request' && $operation !== 'requestMany') {
            // request() and requestMany() read with no own sid and are out of #179's scope: they still read the socket
            // and wake on their reply, letting the hold go. The own-sid operations take their queued message at once.
            self::assertTrue($heldWhenDone, 'the operation took its queued message while the handler ahead of it was still held (#179)');
        }
    }

    /** @return iterable<string, array{string}> */
    public static function operationsBehindAPing(): iterable
    {
        yield 'SubscriptionQueue::next()' => ['next'];
        yield 'SubscriptionQueue::fetchAll(1)' => ['fetchAll'];
        yield 'request()' => ['request'];
        yield 'requestMany(max 1)' => ['requestMany'];
        yield 'JetStreamContext::fetchBatch(1)' => ['fetchBatch'];
        yield 'JetStreamContext::directGetBatch()' => ['directGet'];
        yield 'PullConsumerIterator::handle()' => ['pullConsumer'];
        yield 'KeyValueBucket::keys()' => ['kvKeys'];
        yield 'KeyValueBucket::history()' => ['kvHistory'];
    }

    /**
     * A dispatch still under way: the chunk the application's read takes holds a server PING ahead of the operation's
     * message. The
     * dispatch answers the PING inline and awaits the PONG's write, which a socket under backpressure takes only after a
     * while; the operation's message is not even queued yet. The test transport holds that write up until a read takes
     * the socket meanwhile, which only the operation's can, or for half a second when none does.
     */
    #[DataProvider('operationsBehindAPing')]
    public function testOperationGetsWhatADispatchWaitingOnAPongWriteBrings(string $operation): void
    {
        [$payloads, $elapsed, $error, $endedBy] = $this->runBehindAHeldUpDelivery($operation, 'ping');

        $this->assertPrompt($operation, $payloads, $elapsed, $error, self::PROMPT, 'the PONG write was let go by ' . $endedBy);
    }

    /** @return iterable<string, array{string, string}> */
    public static function operationsAndTheReadsAroundThem(): iterable
    {
        yield 'next(), its message arriving around it' => ['next', 'arrival'];
        yield 'fetchAll(1), its message arriving around it' => ['fetchAll', 'arrival'];
        yield 'next(), a processIncoming() around it' => ['next', 'processIncoming'];
        yield 'fetchAll(1), a processIncoming() around it' => ['fetchAll', 'processIncoming'];
        yield 'next(), a request() around it' => ['next', 'request'];
        yield 'fetchAll(1), a request() around it' => ['fetchAll', 'request'];
        yield 'fetchBatch(1), a processIncoming() around it' => ['fetchBatch', 'processIncoming'];
        yield 'fetchBatch(1), a request() around it' => ['fetchBatch', 'request'];
        yield 'directGetBatch(), a processIncoming() around it' => ['directGet', 'processIncoming'];
        yield 'directGetBatch(), a request() around it' => ['directGet', 'request'];
        yield 'guard: request(), a processIncoming() around it' => ['request', 'processIncoming'];
        yield 'guard: request(), a request() around it' => ['request', 'request'];
        yield 'guard: requestMany(max 1), a processIncoming() around it' => ['requestMany', 'processIncoming'];
        yield 'guard: requestMany(max 1), a request() around it' => ['requestMany', 'request'];
        yield 'PullConsumerIterator::handle(), a processIncoming() around it' => ['pullConsumer', 'processIncoming'];
        yield 'PullConsumerIterator::handle(), a request() around it' => ['pullConsumer', 'request'];
        yield 'KeyValueBucket::keys(), a processIncoming() around it' => ['kvKeys', 'processIncoming'];
        yield 'KeyValueBucket::keys(), a request() around it' => ['kvKeys', 'request'];
        yield 'KeyValueBucket::history(), a processIncoming() around it' => ['kvHistory', 'processIncoming'];
        yield 'KeyValueBucket::history(), a request() around it' => ['kvHistory', 'request'];
    }

    /**
     * The hop window: the operation and another party start 0 to 32 event-loop hops apart, in both orders. The other party is another fiber's read, a processIncoming() or a request() the server
     * never answers, or, for a queue poll, the arrival of the queue's message while the application's processIncoming()
     * loop holds the socket. The server answers a request or a pull as soon as it is written; a queue's message is on the
     * socket before the reads start, or arrives as the other party. Each run is fresh, and the operation must have its
     * result within half a second, a third of its 1.5 s deadline: the failures list every hop count where it did not.
     *
     * The guards, request() and requestMany(), look at the read slot themselves before they read, and wait on their
     * reply, or the slot, while another fiber reads: no hop count catches them out.
     */
    #[DataProvider('operationsAndTheReadsAroundThem')]
    public function testOperationEndsPromptlyWhateverTheHopsBetweenItAndAnotherParty(string $operation, string $other): void
    {
        $range = self::hopRange();
        $failures = [];
        for ($hops = -$range; $hops <= $range; $hops++) {
            [$payloads, $elapsed, $error] = $this->runHopsApart($operation, $other, $hops);
            if ($payloads !== [self::RESULTS[$operation]] || $elapsed >= self::HOP_PROMPT) {
                $failures[] = sprintf(
                    '%s %d hop(s) %s it: %s after %.3f s%s',
                    $other,
                    abs($hops),
                    $hops < 0 ? 'after' : 'before',
                    json_encode($payloads),
                    $elapsed,
                    $error === null ? '' : ' (' . $error . ')',
                );
            }
        }

        self::report(sprintf('P2 %s / %s: %d of %d runs late or wrong%s', $operation, $other, count($failures), 2 * $range + 1, $failures === [] ? '' : "\n    " . implode("\n    ", $failures)));
        self::assertSame([], $failures);
    }

    /** @return iterable<string, array{string, string}> */
    public static function operationsWaitingThroughAReconnect(): iterable
    {
        yield "SubscriptionQueue::next(), waiting behind the application's read when the connection drops" => ['next', 'behind'];
        yield "SubscriptionQueue::fetchAll(1), waiting behind the application's read when the connection drops" => ['fetchAll', 'behind'];
        yield "request(), waiting behind the application's read when the connection drops" => ['request', 'behind'];
        yield "JetStreamContext::fetchBatch(1), waiting behind the application's read when the connection drops" => ['fetchBatch', 'behind'];
        yield 'SubscriptionQueue::next(), called while the reconnect is under way' => ['next', 'during'];
        yield 'SubscriptionQueue::fetchAll(1), called while the reconnect is under way' => ['fetchAll', 'during'];
    }

    /**
     * A reconnect: the application's read loop holds the socket when the connection drops.
     * Its read recovers the connection, the server refusing dials for 60 ms, and the server sends the operation's
     * message right after the replayed SUB it comes on: the reconnect reads it and delivers it once the connection is
     * back. The operation's read waited for that reconnect, and then reads the new socket. A request's reply comes on
     * the reconnected reply inbox, as one from a service that answers late, and a fetch's message on its reconnected
     * inbox, as one for a pull the server still held. In the second form the operation starts while the reconnect is
     * already under way.
     */
    #[DataProvider('operationsWaitingThroughAReconnect')]
    public function testOperationGetsWhatTheDeliveryAfterAReconnectBrings(string $operation, string $when): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $server = new class {
            /** The subject the operation's message comes on, once the operation asked for it. */
            public ?string $waitingOn = null;

            public bool $sent = false;
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $server): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.echo' || str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.')) {
                // Answered only once the client is back, below.
                $server->waitingOn = $replyTo;

                return [];
            }

            return $transport->replyFrame($replyTo, 'ok');
        };
        // Sets the reply inbox up before the application's read starts.
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;
        if ($queue !== null) {
            $server->waitingOn = 'jobs';
        }
        $transport->afterWrite = static function () use ($transport, $server, $operation): void {
            $subject = $server->waitingOn;
            if ($server->sent || $transport->epoch() < 1 || $subject === null) {
                return;
            }

            // The reconnect has replayed the SUB the message comes on.
            $sid = $transport->sidFor($subject);
            if ($sid === null) {
                return;
            }

            $server->sent = true;
            $transport->pushFrame(match ($operation) {
                'request' => ReconnectingTransport::msgFrame($subject, $sid, 'reply-1'),
                'fetchBatch' => ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT),
                default => ReconnectingTransport::msgFrame('jobs', $sid, 'job-1'),
            });
        };
        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        // Lets the loop's first read take the socket.
        delay(0.01);
        $transport->refuseDials();
        if ($when === 'behind') {
            // The operation's read waits behind the application's read from its first hop; these timers only have to
            // come after that, and the reconnect after the operation's read has gone to wait for it.
            EventLoop::delay(0.02, static fn() => $transport->dropConnection());
            EventLoop::delay(0.08, static fn() => $transport->acceptDials());
        } else {
            $transport->dropConnection();
            $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
            EventLoop::delay(0.06, static fn() => $transport->acceptDials());
        }

        [$payloads, $elapsed, $error] = $this->runOperation($operation, $client, $queue);
        $stop->cancel();

        self::assertSame(1, $transport->epoch(), 'the connection was reconnected once');
        self::assertTrue($server->sent, 'the server sent the message right after the replayed SUB');
        $this->assertPrompt($operation, $payloads, $elapsed, $error, self::PROMPT, $when === 'behind' ? 'dropped while waiting behind the application\'s read' : 'started during the reconnect');
    }

    /**
     * The staging of the deliveries still under way. The application's processIncoming() loop holds the socket read when the
     * operation starts, and reads one chunk that holds a lead frame ahead of the operation's message: a message for
     * "slow", the lowest sid, whose handler awaits ($lead 'handler', or 'request' for the guard), or a server PING
     * ($lead 'ping'). The read slot is free while the handler or the PONG's write is held up, and the operation's
     * message is not delivered yet. A queue poll starts once the hold-up began; the other operations start first, and the
     * server answers what they write with that chunk.
     *
     * @return array{list<string>, float, ?string, string, bool} The operation's payloads, the seconds it took, what it
     *         threw, what ended the hold-up, and whether the handler ahead of it was still held when the result was in.
     */
    private function runBehindAHeldUpDelivery(string $operation, string $lead): array
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectHooked($transport);
        $hold = new class {
            public bool $held = false;
            public bool $over = false;
            public string $endedBy = 'nothing';

            /** What the handler's own request got, for the guards: its reply, or what it threw. */
            public ?string $innerReply = null;

            public ?float $innerSeconds = null;

            /** @var DeferredFuture<null> */
            public DeferredFuture $began;

            /** @var DeferredFuture<null> */
            public DeferredFuture $released;

            public function __construct()
            {
                $this->began = new DeferredFuture();
                $this->released = new DeferredFuture();
            }

            public function begin(): void
            {
                $this->held = true;
                if (!$this->began->isComplete()) {
                    $this->began->complete();
                }
            }

            public function end(string $by): void
            {
                if ($this->over) {
                    return;
                }

                $this->over = true;
                $this->held = false;
                $this->endedBy = $by;
                $this->released->complete();
            }
        };

        $leadFrame = "PING\r\n";
        if ($lead !== 'ping') {
            // Subscribed first: the lowest sid, so its message is delivered ahead of the operation's.
            $slowSid = $client->subscribe('slow', static function () use ($lead, $hold, $client): void {
                if ($hold->held || $hold->over) {
                    return;
                }

                $hold->begin();
                if ($lead === 'request') {
                    // Awaits NATS: a request of its own, whose reply only a read of the socket brings.
                    $start = hrtime(true);
                    try {
                        $hold->innerReply = $client->request('svc.inner', 'x', 3_000)->await()->payload;
                    } catch (\Throwable $e) {
                        $hold->innerReply = $e::class . ': ' . $e->getMessage();
                    }
                    $hold->innerSeconds = (hrtime(true) - $start) / 1e9;
                    $hold->end("the handler's own request");

                    return;
                }

                // Awaits something other than NATS: an HTTP or a database call, say.
                $fallback = EventLoop::delay(self::FALLBACK, static fn() => $hold->end('its own time running out'));
                try {
                    $hold->released->getFuture()->await();
                } finally {
                    EventLoop::cancel($fallback);
                }
            })->await();
            $leadFrame = ReconnectingTransport::msgFrame('slow', $slowSid, 's');
        }

        $server = new class {
            public int $pulls = 0;

            /** The deliver subject of a Key/Value replay's push consumer, once the client created it. */
            public ?string $deliver = null;

            public bool $replayed = false;
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $leadFrame, $server): array {
            if ($replyTo === null) {
                return [];
            }

            $sid = (int) $transport->sidFor($replyTo);
            if ($subject === 'svc.echo') {
                return [$leadFrame . ReconnectingTransport::msgFrame($replyTo, $sid, 'reply-1')];
            }

            if (str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.')) {
                // Only the first pull finds a message; the server holds later ones until they expire.
                return ++$server->pulls === 1
                    ? [$leadFrame . ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT)]
                    : [];
            }

            if (str_starts_with($subject, '$JS.API.DIRECT.GET.')) {
                return [$leadFrame . ReconnectingTransport::hmsgFrame($replyTo, $sid, self::DIRECT_GET_HEADERS, 'v1')];
            }

            if (str_starts_with($subject, '$JS.API.CONSUMER.CREATE.KV_b')) {
                [$created, $server->deliver] = self::kvConsumerCreated($payload);

                return $transport->replyFrame($replyTo, $created);
            }

            return $transport->replyFrame($replyTo, 'ok');
        };
        // A PONG whose write got through before any read let it go ran out its own time. A Key/Value replay's record
        // comes once the client subscribed to the deliver subject.
        $transport->afterWrite = static function (string $bytes) use ($transport, $server, $leadFrame, $hold): void {
            if (str_contains($bytes, 'PONG')) {
                $hold->end('its own time running out');
            }

            $deliver = $server->deliver;
            if ($deliver !== null && !$server->replayed && str_contains($bytes, 'SUB ' . $deliver . ' ')) {
                $server->replayed = true;
                $transport->pushFrame($leadFrame . ReconnectingTransport::msgFrame('$KV.b.k', (int) $transport->sidFor($deliver), 'v1', self::KV_ACK_SUBJECT));
            }
        };
        // Sets the reply inbox up before the application's read starts.
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitNextRead();

        if ($lead === 'ping') {
            // The PONG's write is held up like one on a socket under backpressure, for half a second at most.
            $transport->stallNextWriteContaining('PONG', self::FALLBACK);
            $this->onWrite = static function (string $bytes) use ($hold): void {
                if (str_contains($bytes, 'PONG')) {
                    $hold->begin();
                }
            };
        }

        if ($lead !== 'request') {
            // While the delivery is held up the application's read cannot read: a read of the socket is the operation's.
            $this->onRead = static function () use ($hold, $transport, $lead): void {
                if (!$hold->held) {
                    return;
                }

                $hold->end("the operation's read taking the socket");
                if ($lead === 'ping') {
                    $transport->releaseStalledWrites();
                }
            };
        }

        if ($queue !== null) {
            $transport->pushFrame($leadFrame . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
            $hold->began->getFuture()->await(new TimeoutCancellation(2));
        }

        $heldWhenDone = false;
        try {
            $outcome = $this->runOperation($operation, $client, $queue);
            // Captured before the hold is let go below: with the message queued behind the handler, the operation takes
            // it at once and the handler is still held when its result is in (#179). The 'ping' and 'request' leads let
            // the hold go as part of getting the result, so this only holds for the plain 'handler' lead.
            $heldWhenDone = $hold->held;
            if ($lead === 'request') {
                // The handler's own request has to end as well, and in time.
                try {
                    $hold->released->getFuture()->await(new TimeoutCancellation(self::TIMEOUT + 1.0));
                } catch (CancelledException) {
                    // Reported below.
                }
            }
        } finally {
            $began = $hold->began->isComplete();
            $hold->end('the end of the test');
            $transport->releaseStalledWrites();
            $stop->cancel();
        }

        self::assertTrue($began, "the application's read met the hold-up ahead of the operation's message");
        if ($lead === 'request') {
            self::assertSame('ok', $hold->innerReply, "the handler's own request got its reply");
            self::assertNotNull($hold->innerSeconds);
            self::assertLessThan(self::PROMPT, $hold->innerSeconds, sprintf("the handler's own request took %.3f s", $hold->innerSeconds));
            self::report(sprintf("  the handler's own request got %s after %.3f s", $hold->innerReply, $hold->innerSeconds));
        }

        return [...$outcome, $hold->endedBy, $heldWhenDone];
    }

    /**
     * One run of a hop sweep ({@see testOperationEndsPromptlyWhateverTheHopsBetweenItAndAnotherParty()}): the operation
     * and the other party start $hops event-loop hops apart, the operation first when $hops is negative.
     *
     * @return array{list<string>, float, ?string}
     */
    private function runHopsApart(string $operation, string $other, int $hops): array
    {
        $transport = new ReconnectingTransport();
        $server = new class {
            /** The deliver subject of a Key/Value replay's push consumer, once the client created it. */
            public ?string $deliver = null;
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $server): array {
            if ($replyTo === null) {
                return [];
            }

            $sid = (int) $transport->sidFor($replyTo);
            if (str_starts_with($subject, '$JS.API.CONSUMER.CREATE.KV_b')) {
                [$created, $server->deliver] = self::kvConsumerCreated($payload);

                return $transport->replyFrame($replyTo, $created);
            }

            return match (true) {
                $subject === 'svc.warm' => $transport->replyFrame($replyTo, 'ok'),
                $subject === 'svc.echo' => [ReconnectingTransport::msgFrame($replyTo, $sid, 'reply-1')],
                str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.') => [ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT)],
                str_starts_with($subject, '$JS.API.DIRECT.GET.') => [ReconnectingTransport::hmsgFrame($replyTo, $sid, self::DIRECT_GET_HEADERS, 'v1')],
                // "svc": the other party's request, which nobody answers.
                default => [],
            };
        };
        // A Key/Value replay's record comes as soon as the client subscribed to the deliver subject.
        $transport->afterWrite = static function (string $bytes) use ($transport, $server): void {
            $deliver = $server->deliver;
            if ($deliver !== null && str_contains($bytes, 'SUB ' . $deliver . ' ')) {
                $server->deliver = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('$KV.b.k', (int) $transport->sidFor($deliver), 'v1', self::KV_ACK_SUBJECT));
            }
        };
        $client = $this->connectClient($transport);
        $client->request('svc.warm', 'x')->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;
        $jobFrame = $queue === null ? '' : ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1');
        $stop = new DeferredCancellation();
        if ($other === 'arrival') {
            $this->startApplicationReadLoop($client, $stop);
            // Lets the loop's first read take the socket and wait on it.
            delay(0.01);
        } elseif ($queue !== null) {
            // The queue's message is on the socket before either read starts.
            $transport->pushFrame($jobFrame);
        }

        $run = new class {
            /** @var Future<list<string>>|null */
            public ?Future $operation = null;

            /** @var Future<mixed>|null */
            public ?Future $other = null;
        };
        $startOperation = function () use ($run, $operation, $client, $queue): void {
            $run->operation = $this->startOperation($operation, $client, $queue, self::HOP_WAIT);
        };
        $startOther = static function () use ($run, $other, $client, $transport, $jobFrame): void {
            $run->other = match ($other) {
                'arrival' => (static function () use ($transport, $jobFrame): Future {
                    $transport->pushFrame($jobFrame);

                    return Future::complete();
                })(),
                'request' => $client->request('svc', 'unanswered', 3_000),
                default => $client->processIncoming(new TimeoutCancellation(3)),
            };
        };
        [$first, $second] = $hops < 0 ? [$startOperation, $startOther] : [$startOther, $startOperation];

        $start = hrtime(true);
        $first();
        $made = new DeferredFuture();
        self::afterHops(abs($hops), static function () use ($second, $made): void {
            $second();
            $made->complete();
        });
        $made->getFuture()->await();
        $run->other?->ignore();
        self::assertNotNull($run->operation);

        $error = null;
        try {
            $payloads = $run->operation->await();
        } catch (\Throwable $e) {
            $payloads = [];
            $error = $e::class . ': ' . $e->getMessage();
        }
        $elapsed = (hrtime(true) - $start) / 1e9;

        $stop->cancel();
        $client->disconnect()->await();

        return [$payloads, $elapsed, $error];
    }

    /**
     * Runs $operation to its end, waiting up to $wait seconds for its result.
     *
     * @return array{list<string>, float, ?string} The payloads it got, the seconds it took, and what it threw.
     */
    private function runOperation(string $operation, NatsClient $client, ?SubscriptionQueue $queue, float $wait = self::TIMEOUT): array
    {
        $start = hrtime(true);
        $error = null;
        try {
            $payloads = $this->startOperation($operation, $client, $queue, $wait)->await();
        } catch (\Throwable $e) {
            $payloads = [];
            $error = $e::class . ': ' . $e->getMessage();
        }

        return [$payloads, (hrtime(true) - $start) / 1e9, $error];
    }

    /**
     * Starts $operation, waiting up to $wait seconds for its result: the JetStream operations wait for their expiry
     * plus one second, so they expire a second earlier. The pull consumer pulls one message at a time, one pull in
     * flight, and stops at the first message. The Key/Value replays of bucket "b" wait $wait seconds for progress.
     *
     * @return Future<list<string>> The payloads the operation got.
     */
    private function startOperation(string $operation, NatsClient $client, ?SubscriptionQueue $queue, float $wait): Future
    {
        $ms = (int) round($wait * 1000);

        return match ($operation) {
            'next' => async(static function () use ($queue, $wait): array {
                $message = self::queueOf($queue)->setTimeout($wait)->next();

                return $message === null ? [] : [$message->payload];
            }),
            'fetchAll' => async(static fn(): array => self::payloads(self::queueOf($queue)->setTimeout($wait)->fetchAll(1))),
            'request' => $client->request('svc.echo', 'hi', $ms)->map(static fn(NatsMessage $message): array => [$message->payload]),
            'requestMany' => $client->requestMany('svc.echo', 'hi', null, 1, $ms)->map(self::payloads(...)),
            'fetchBatch' => $client->jetStream()->fetchBatch('S', 'C', 1, $ms - 1_000)->map(self::payloads(...)),
            'directGet' => $client->jetStream()->directGetBatch('S', ['batch' => 1, 'seq' => 1], $ms - 1_000)->map(self::payloads(...)),
            'pullConsumer' => async(static function () use ($client, $ms): array {
                $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs($ms - 1_000);
                $handled = new class {
                    /** @var list<string> */
                    public array $payloads = [];
                };
                $iterator->handle(static function (NatsMessage $message) use ($iterator, $handled): void {
                    $handled->payloads[] = $message->payload;
                    $iterator->stop();
                })->await();

                return $handled->payloads;
            }),
            'kvKeys' => $client->jetStream()->keyValue('b')->keys($wait),
            'kvHistory' => $client->jetStream()->keyValue('b')->history('k', $wait)->map(
                static fn(array $entries): array => array_map(static fn(KeyValueEntry $entry): string => (string) $entry->value, $entries),
            ),
            default => throw new \LogicException('Unknown operation ' . $operation),
        };
    }

    /**
     * The server's answer to the creation of the ephemeral push consumer behind a Key/Value replay, which finds one
     * record, and the deliver subject the request named.
     *
     * @return array{string, string}
     */
    private static function kvConsumerCreated(string $payload): array
    {
        /** @var array{config: array{deliver_subject: string}} $request */
        $request = json_decode($payload, true, flags: JSON_THROW_ON_ERROR);

        return [
            (string) json_encode(['stream_name' => 'KV_b', 'name' => 'c1', 'config' => $request['config'], 'num_pending' => 1]),
            $request['config']['deliver_subject'],
        ];
    }

    /**
     * @param list<string> $payloads
     */
    private function assertPrompt(string $operation, array $payloads, float $elapsed, ?string $error, float $bound, string $context): void
    {
        $summary = sprintf(
            '%s returned %s after %.3f s%s (%s)',
            $operation,
            json_encode($payloads),
            $elapsed,
            $error === null ? '' : ', throwing ' . $error,
            $context,
        );
        self::report($summary);

        self::assertSame([self::RESULTS[$operation]], $payloads, $summary);
        self::assertLessThan($bound, $elapsed, $summary);
    }

    /**
     * Connects a client through a decorator of $transport that calls {@see $onRead} as each read of the socket starts
     * and {@see $onWrite} as each write starts. A read reaches the transport once it holds the read slot, so the call
     * tells the test that a read waits on the socket without timing it.
     */
    private function connectHooked(ReconnectingTransport $transport): NatsClient
    {
        $onRead = function (): void {
            if ($this->onRead !== null) {
                ($this->onRead)();
            }
        };
        $onWrite = function (string $bytes): void {
            if ($this->onWrite !== null) {
                ($this->onWrite)($bytes);
            }
        };
        $hooked = new class ($transport, $onRead, $onWrite) implements CancellableDialTransportInterface {
            /**
             * @param \Closure(): void $onRead
             * @param \Closure(string): void $onWrite
             */
            public function __construct(
                private readonly ReconnectingTransport $server,
                private readonly \Closure $onRead,
                private readonly \Closure $onWrite,
            ) {}

            public function connect(string $dsn, int $timeoutMs, ?Cancellation $cancellation = null): Future
            {
                return $this->server->connect($dsn, $timeoutMs, $cancellation);
            }

            public function upgradeTls(): Future
            {
                return $this->server->upgradeTls();
            }

            public function write(string $bytes): Future
            {
                ($this->onWrite)($bytes);

                return $this->server->write($bytes);
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                ($this->onRead)();

                return $this->server->readLine($cancellation);
            }

            public function close(): Future
            {
                return $this->server->close();
            }
        };
        $client = $this->own(new NatsClient($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null), $hooked));
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /** Waits until a read of a hooked client takes the socket ({@see connectHooked()}). */
    private function awaitNextRead(): void
    {
        $reading = new DeferredFuture();
        $this->onRead = static function () use ($reading): void {
            if (!$reading->isComplete()) {
                $reading->complete();
            }
        };
        $reading->getFuture()->await(new TimeoutCancellation(2));
        $this->onRead = null;
    }

    /**
     * Starts the application's own read loop, processIncoming() over and over on a fiber of its own, until $stop is
     * cancelled or a read fails.
     */
    private function startApplicationReadLoop(NatsClient $client, DeferredCancellation $stop): void
    {
        async(static function () use ($client, $stop): void {
            while (!$stop->isCancelled()) {
                try {
                    $client->processIncoming($stop->getCancellation())->await();
                } catch (\Throwable) {
                    return;
                }
            }
        })->ignore();
    }

    /** Makes $call after $hops event-loop hops: a callback queued that many times in a row. */
    private static function afterHops(int $hops, \Closure $call): void
    {
        if ($hops === 0) {
            $call();

            return;
        }

        EventLoop::queue(static function () use ($hops, $call): void {
            self::afterHops($hops - 1, $call);
        });
    }

    /**
     * The hops each sweep runs on either side: 32, or the READ_WAKEUP_HOPS environment variable. The windows the old
     * reads missed lay 2 to 23 hops from the operation's start, the Key/Value replays' farthest.
     */
    private static function hopRange(): int
    {
        $hops = getenv('READ_WAKEUP_HOPS');

        return $hops === false || $hops === '' ? 32 : max(0, (int) $hops);
    }

    private static function isPoll(string $operation): bool
    {
        return $operation === 'next' || $operation === 'fetchAll';
    }

    private static function queueOf(?SubscriptionQueue $queue): SubscriptionQueue
    {
        self::assertNotNull($queue);

        return $queue;
    }

    /**
     * @param list<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_map(static fn(NatsMessage $message): string => $message->payload, $messages);
    }

    /** Prints a line of the measurements to STDERR when the READ_WAKEUP_VERBOSE environment variable is set. */
    private static function report(string $line): void
    {
        $verbose = getenv('READ_WAKEUP_VERBOSE');
        if ($verbose !== false && $verbose !== '' && $verbose !== '0') {
            fwrite(STDERR, $line . "\n");
        }
    }
}
