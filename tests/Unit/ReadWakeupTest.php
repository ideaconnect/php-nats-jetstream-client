<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Socket\BindContext;
use Amp\Socket\Certificate;
use Amp\Socket\ServerTlsContext;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\LoopbackNatsServer;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use IDCT\NATS\Transport\AmpSocketTransport;
use IDCT\NATS\Transport\WebSocketFrameCodec;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;
use function Amp\Socket\connect;
use function Amp\Socket\listen;

/**
 * The wake-up of an operation's read (#174): the read of an operation that waits for a result of its own ends without
 * reading as soon as that result may have come in another fiber's delivery, wherever the read waits then.
 * {@see OperationReadWakeupTest} shows each operation getting its result in time; these tests pin what those do not:
 *
 *  - A read the wake-up cancels has consumed nothing: the built-in transports leave every byte they did not return
 *    for the next read, over plain TCP, TLS and WebSocket, and on a real socket the stream stays whole through the
 *    connection's parser.
 *  - The wake-up is only a reason to look again. It belongs to the operation's own subscription, or to its own reply,
 *    so nothing else ends the read; one that finds nothing costs one more look; and an idle operation never wakes.
 *  - Application reads have none.
 *  - It fires once and is never reset, so no later state can match the one a read started from.
 */
final class ReadWakeupTest extends TestCase
{
    use ReconnectScenarios;

    /** @var list<\Closure(): void> Run after each test, before the connections close. */
    private array $teardowns = [];

    protected function tearDown(): void
    {
        foreach ($this->teardowns as $teardown) {
            try {
                $teardown();
            } catch (\Throwable) {
                // Already closed.
            }
        }

        $this->teardowns = [];
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function socketKinds(): iterable
    {
        yield 'plain TCP' => [false];
        yield 'TLS' => [true];
    }

    /**
     * The built-in socket transport's read, ended by its cancellation, has consumed nothing: whatever the peer sent is
     * there for the next read, whichever came first, the bytes or the cancellation, and over TLS too, whose records a
     * read can find half received. An operation's read relies on this when its wake-up cancels it. The pieces, 1 B to
     * 70 KiB, the larger ones spanning several TLS records, go out after the read was cancelled, right before it is in
     * the same event-loop turn, or well before it is, when the read gets them.
     */
    #[DataProvider('socketKinds')]
    public function testASocketReadEndedByItsCancellationLeavesEveryByteForTheNextRead(bool $tls): void
    {
        [$transport, $peer] = $this->connectedSocketTransport($tls);

        $sent = '';
        $received = '';
        $cancelledReads = 0;
        foreach ([1, 100, 20_000, 70_000, 5, 40_000] as $i => $size) {
            $piece = self::distinctBytes($size, $i);
            $stop = new DeferredCancellation();
            $read = async(static fn(): string => $transport->readLine($stop->getCancellation())->await());
            // Lets the read start and wait on the socket.
            delay(0);

            if ($i % 3 === 0) {
                $stop->cancel();
                $peer->write($piece);
            } elseif ($i % 3 === 1) {
                $peer->write($piece);
                $stop->cancel();
            } else {
                $peer->write($piece);
                delay(0.02);
                $stop->cancel();
            }

            $sent .= $piece;
            try {
                $received .= $read->await();
            } catch (CancelledException) {
                $cancelledReads++;
            }

            while (strlen($received) < strlen($sent)) {
                $received .= $transport->readLine(new TimeoutCancellation(3))->await();
            }
        }

        self::assertGreaterThanOrEqual(2, $cancelledReads, 'reads that ended by their cancellation');
        self::assertSame(strlen($sent), strlen($received));
        self::assertSame(md5($sent), md5($received), 'the bytes read are the bytes sent, in order');
    }

    /** @return iterable<string, array{int, int}> */
    public static function framesReadInPart(): iterable
    {
        // The payload's size, and how many bytes of the frame arrive before the read is cancelled.
        yield 'a small frame' => [10, 6];
        // Over 64 KiB, with more than 32 KiB still to come: the transport collects the rest chunk by chunk (#164).
        yield 'a large frame collected in chunks' => [200_000, 60_000];
    }

    /**
     * The WebSocket transport's read, ended by its cancellation while it waits for the rest of a frame, keeps the part it
     * has already taken off the socket, and the next read returns the whole frame once the rest arrives. Its reads of the
     * socket end like the socket transport's ({@see testASocketReadEndedByItsCancellationLeavesEveryByteForTheNextRead()}).
     */
    #[DataProvider('framesReadInPart')]
    public function testAWebSocketReadEndedByItsCancellationKeepsThePartOfAFrameItHadRead(int $size, int $split): void
    {
        [$transport, $peer] = $this->connectedWebSocketTransport();
        $payload = self::distinctBytes($size, 7);
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);

        $stop = new DeferredCancellation();
        $read = async(static fn(): string => $transport->readLine($stop->getCancellation())->await());
        $peer->write(substr($frame, 0, $split));
        // The read takes those bytes off the socket into the transport, then waits for the rest.
        $this->waitUntil(static fn(): bool => self::webSocketBufferedBytes($transport) === $split);
        $stop->cancel();
        try {
            $read->await();
            self::fail('the read returned before the frame was complete');
        } catch (CancelledException) {
            // Ended by its cancellation, as an operation's read is by its wake-up.
        }

        self::assertSame($split, self::webSocketBufferedBytes($transport), 'the cancelled read kept the part of the frame it had read');
        $peer->write(substr($frame, $split));
        $whole = $transport->readLine(new TimeoutCancellation(3))->await();
        self::assertSame(strlen($payload), strlen($whole));
        self::assertSame(md5($payload), md5($whole), 'the next read returned the whole frame');
    }

    /**
     * Problem 1 of #174 on a real socket, through the built-in socket transport: a poll's read waits on the socket while
     * the application's delivery that brings its message is held up behind a handler that awaits. The delivery goes on,
     * and the wake-up cancels the poll's read, which returns the message long before its 3 s deadline. The server sends
     * nothing meanwhile, so nothing else could end that read: the old read returned at the deadline.
     */
    public function testAnOperationsReadOnARealSocketEndsWithTheDeliveryThatBringsItsResult(): void
    {
        [$client, $server, $watched] = $this->connectToLoopbackServer();
        $hold = new HeldUpDelivery();
        $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();
        [$slowSid, $jobsSid] = $this->sidsAtTheServer($client, $server, 'slow', 'jobs');

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $server->send(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $jobsSid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = $hold->endWhenARead();

        $start = hrtime(true);
        $message = $queue->setTimeout(3.0)->next();
        $elapsed = $this->secondsSince($start);
        $stop->cancel();

        self::assertSame('a read taking the socket', $hold->endedBy, "the poll's read took the socket during the hold-up");
        self::assertSame('job-1', $message?->payload);
        self::assertLessThan(1.5, $elapsed, sprintf('next() returned after %.3f s, its deadline being 3 s', $elapsed));
        self::assertGreaterThanOrEqual(1, $watched->cancelledReads, "the wake-up ended the poll's read of the socket");
    }

    /**
     * The same on a real socket, with bytes on the socket when the wake-up cancels the poll's read: a subscription
     * delivered right after the poll's, in the same held-up delivery, sends the first half of a 120 KB message, which
     * the server has on the socket before the wake-up reaches the read, since the event loop polls the socket only once
     * its queued callbacks have run. The cancelled read leaves those bytes on the socket: the next read hands them to
     * the parser, and the message arrives whole with its second half.
     */
    public function testAnOperationsReadWokenWithBytesOnARealSocketLeavesThemForTheNextRead(): void
    {
        [$client, $server, $watched] = $this->connectToLoopbackServer();
        $hold = new HeldUpDelivery();
        $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $large = self::distinctBytes(120_000, 3);
        $largeFrame = '';
        $client->subscribe('tail', static function () use ($server, &$largeFrame): void {
            $server->send(substr($largeFrame, 0, intdiv(strlen($largeFrame), 2)));
        })->await();
        [$slowSid, $jobsSid, $tailSid] = $this->sidsAtTheServer($client, $server, 'slow', 'jobs', 'tail');
        $largeFrame = ReconnectingTransport::msgFrame('jobs', $jobsSid, $large);

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $server->send(
            ReconnectingTransport::msgFrame('slow', $slowSid, 's')
            . ReconnectingTransport::msgFrame('jobs', $jobsSid, 'job-1')
            . ReconnectingTransport::msgFrame('tail', $tailSid, 't'),
        );
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = $hold->endWhenARead();

        $first = $queue->setTimeout(3.0)->next();
        $watched->onRead = null;

        self::assertSame('a read taking the socket', $hold->endedBy, "the poll's read took the socket during the hold-up");
        self::assertSame('job-1', $first?->payload);
        self::assertGreaterThanOrEqual(1, $watched->cancelledReads, "the wake-up ended the poll's read, the bytes on the socket notwithstanding");

        $server->send(substr($largeFrame, intdiv(strlen($largeFrame), 2)));
        $second = $queue->setTimeout(3.0)->next();
        $stop->cancel();

        self::assertNotNull($second);
        self::assertSame(strlen($large), strlen($second->payload));
        self::assertSame(md5($large), md5($second->payload), 'the next message arrived whole');
    }

    /**
     * The wake-up also ends an operation's wait for another fiber's read, which can go on long after the delivery that
     * brings the operation's result: here the application's delivery, held up behind a handler that awaits, brings the
     * poll's message while a second application read holds the socket, with nothing coming from the server. The poll
     * returns the message at once; it used to wait for that read, up to the read's deadline.
     */
    public function testAnOperationWaitingForAnotherFibersReadGetsWhatADeliveryStillUnderWayBrings(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $hold = new HeldUpDelivery(fallbackSeconds: 2.5);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $reading = new DeferredFuture();
        $watched->onRead = static function () use ($reading): void {
            if (!$reading->isComplete()) {
                $reading->complete();
            }
        };
        $holder = $client->processIncoming(new TimeoutCancellation(3));
        $holder->ignore();
        $reading->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
        // The poll goes to wait for the read the second application read holds.
        delay(0.05);
        self::assertFalse($poll->isComplete());
        $readsBefore = $watched->reads;
        $start = hrtime(true);
        $hold->end('the test');
        $message = $poll->await(new TimeoutCancellation(5));
        $elapsed = $this->secondsSince($start);

        self::assertSame('job-1', $message?->payload);
        self::assertLessThan(1.0, $elapsed, sprintf('the poll returned %.3f s after the delivery went on', $elapsed));
        self::assertSame($readsBefore, $watched->reads, 'the poll returned without reading the socket');
        self::assertFalse($holder->isComplete(), 'the other read still holds the socket');
        $stop->cancel();
    }

    /**
     * The wake-up also ends an operation's wait for a reconnect: a delivery still under way when the connection dropped
     * brings the poll's message while another fiber's reconnect backs off between refused dials. The poll returns it
     * during the outage; it used to wait for the reconnect, and then for the server's next bytes or its deadline.
     */
    public function testAnOperationWaitingForAReconnectGetsWhatADeliveryStillUnderWayBrings(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $hold = new HeldUpDelivery(fallbackSeconds: 2.5);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        // The connection drops with dials refused, and another fiber's read starts the reconnect.
        $this->startRecoveryInBackground($client, $transport);

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
        // The poll goes to wait for the reconnect.
        delay(0.05);
        self::assertFalse($poll->isComplete());
        $start = hrtime(true);
        $hold->end('the test');
        $message = $poll->await(new TimeoutCancellation(5));
        $elapsed = $this->secondsSince($start);

        self::assertSame(ConnectionState::Connecting, $client->state(), 'the connection is still down');
        self::assertSame('job-1', $message?->payload);
        self::assertLessThan(1.0, $elapsed, sprintf('the poll returned %.3f s after the delivery went on', $elapsed));
        $transport->acceptDials();
        $stop->cancel();
    }

    /**
     * The same when the operation's own read meets the dropped connection while another fiber already runs the
     * reconnect, here the heartbeat's, after the server stopped answering: the read joins that reconnect, and the
     * delivery still under way brings the poll's message meanwhile. The poll returns it during the outage.
     */
    public function testAnOperationWhoseReadJoinsAReconnectGetsWhatADeliveryStillUnderWayBrings(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = new NatsClient($this->options(true, 2_000, 1_000, 0.05, 1, null, 5, 20, null), $watched);
        $this->opened[] = $client;
        $client->connect()->await();
        $hold = new HeldUpDelivery(fallbackSeconds: 2.5);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $cancelledBefore = $watched->cancelledReads;
        $readsBefore = $watched->reads;

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
        $this->waitUntil(static fn(): bool => $watched->reads > $readsBefore);
        // The server stops answering, and dials are refused: the heartbeat gives the connection up and reconnects,
        // closing the socket the poll's read waits on, and that read joins the reconnect.
        $transport->refuseDials();
        $transport->silence();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting && $watched->readsUnderWay === 0);
        delay(0.05);
        self::assertFalse($poll->isComplete());
        $start = hrtime(true);
        $hold->end('the test');
        $message = $poll->await(new TimeoutCancellation(5));
        $elapsed = $this->secondsSince($start);

        self::assertSame($cancelledBefore, $watched->cancelledReads, "the poll's read ended with the socket, not by a cancellation");
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the connection is still down');
        self::assertSame('job-1', $message?->payload);
        self::assertLessThan(1.0, $elapsed, sprintf('the poll returned %.3f s after the delivery went on', $elapsed));
        $transport->acceptDials();
        $stop->cancel();
    }

    /**
     * A reconnect that gives up fails the operations waiting for it with its own error, the cause included (#172), also
     * when the Closed listener suspends, as one that hands the event to an async logger or a queue does. The terminal
     * close fires the wake-ups of the subscriptions it releases; fired at once, they woke the poll waiting for the
     * reconnect while that listener ran, and the poll found nothing, paused, read again once the reconnect was over and
     * got "Connection is not open" instead of "Reconnect attempts exhausted". They now fire once the reconnect has
     * published its outcome.
     */
    public function testAnOperationWaitingForAReconnectThatGivesUpGetsItsErrorWhenTheClosedListenerSuspends(): void
    {
        $transport = new ReconnectingTransport();
        $listener = static function (ConnectionEvent $event): void {
            if ($event === ConnectionEvent::Closed) {
                delay(0);
            }
        };
        $client = new NatsClient($this->options(true, 2_000, 3, 0, 2, $listener, 5, 20, null), $transport);
        $this->opened[] = $client;
        $client->connect()->await();
        $queue = $client->subscribeQueue('jobs')->await();
        // Dials refused: another fiber's read runs the reconnect, which gives up after three attempts.
        $this->startRecoveryInBackground($client, $transport);

        $error = null;
        try {
            $queue->setTimeout(3.0)->next();
        } catch (ConnectionException $e) {
            $error = $e;
        }

        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertNotNull($error, 'the poll fails with the connection');
        self::assertSame('Reconnect attempts exhausted', $error->getMessage(), 'the reconnect surfaces its own error');
        self::assertNotNull($error->getPrevious(), 'with its cause');
    }

    /** @return iterable<string, array{string}> */
    public static function pollsThatWait(): iterable
    {
        yield 'next()' => ['next'];
        yield 'fetchAll(1)' => ['fetchAll'];
    }

    /**
     * An operation waiting on an idle connection reads the socket once for its whole wait: nothing wakes its read, so a
     * wake-up never makes an idle operation spin. A wake-up that fired with no delivery, or one handed out already
     * fired, would have it read again after every 1 ms pause.
     */
    #[DataProvider('pollsThatWait')]
    public function testAnIdleOperationReadsTheSocketOnceForItsWholeWait(string $poll): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $queue = $client->subscribeQueue('jobs')->await()->setTimeout(0.3);

        $before = $watched->reads;
        $result = $poll === 'next' ? $queue->next() : $queue->fetchAll(1);

        self::assertContains($result, [null, []], 'nothing came');
        self::assertSame(1, $watched->reads - $before, 'reads of the socket during the 0.3 s wait');
    }

    /**
     * A wake-up belongs to the operation's own subscription: a delivery to another one, made by the application's
     * delivery held up ahead of it, does not end the poll's read, which stays the only read of the socket until the
     * poll's own message comes.
     */
    public function testADeliveryToAnotherSubscriptionDoesNotEndAnOperationsRead(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $hold = new HeldUpDelivery();
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();
        /** @var DeferredFuture<null> $otherDelivered */
        $otherDelivered = new DeferredFuture();
        $otherSid = $client->subscribe('other', static function () use ($otherDelivered): void {
            if (!$otherDelivered->isComplete()) {
                $otherDelivered->complete();
            }
        })->await();

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('other', $otherSid, 'o'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = $hold->endWhenARead();

        $readsBefore = $watched->reads;
        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
        $otherDelivered->getFuture()->await(new TimeoutCancellation(2));
        // Room for a wake-up, had that delivery fired one, to end the poll's read and the poll to read again.
        delay(0.02);

        self::assertSame('a read taking the socket', $hold->endedBy, "the poll's read took the socket during the hold-up");
        self::assertSame(1, $watched->reads - $readsBefore, "the poll's read is still the only one on the socket");
        self::assertFalse($poll->isComplete());

        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        self::assertSame('job-1', $poll->await(new TimeoutCancellation(2))?->payload);
        $stop->cancel();
    }

    /**
     * A wake-up is only a reason to look again: what the operation looks at is its own state. Here another poller of the
     * same queue takes the message the delivery brought before the woken poll looks. The poll finds nothing, returns
     * nothing wrong, and waits on: once the application's read loop stops, the poll's read takes the socket and stays
     * there, the only read under way, where a spin would read again, or look again without reading, every millisecond.
     * It returns the next message.
     */
    public function testAWakeUpThatFindsNothingCostsOneLookAndTheOperationWaitsOn(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $hold = new HeldUpDelivery();
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $stolen = new class {
            public ?string $payload = null;
        };
        // Queued as the handler returns, ahead of the wake-up that the delivery of job-1 behind it fires.
        $hold->beforeReturning = static function () use ($queue, $stolen): void {
            EventLoop::queue(static function () use ($queue, $stolen): void {
                $stolen->payload = $queue->fetch()?->payload;
            });
        };

        $stop = new DeferredCancellation();
        $loop = $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = $hold->endWhenARead();

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
        $this->waitUntil(static fn(): bool => $stolen->payload !== null);
        // The application's read loop stops, so the poll's read is the one to take the socket.
        $stop->cancel();
        $loop->await(new TimeoutCancellation(2));
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $readsBefore = $watched->reads;
        // A poll that spun would read, or look, about once a millisecond meanwhile.
        delay(0.05);

        self::assertSame('a read taking the socket', $hold->endedBy, "the poll's read took the socket during the hold-up");
        self::assertSame('job-1', $stolen->payload, 'the other poller took the message');
        self::assertFalse($poll->isComplete(), 'the woken poll found nothing, and waits on');
        self::assertSame(1, $watched->readsUnderWay, "the poll's read waits on the socket");
        self::assertSame($readsBefore, $watched->reads, 'the poll read the socket once in 50 ms of waiting');

        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-2'));
        self::assertSame('job-2', $poll->await(new TimeoutCancellation(2))?->payload);
    }

    /**
     * The application's own reads have no wake-up: processIncoming() reads until the server sends something, or its
     * cancellation fires, whatever another fiber delivers meanwhile. Here it takes the socket while the application's read
     * loop is held up in a handler, and that delivery goes on to deliver a message: the read still waits for the server.
     */
    public function testAnApplicationReadIsNotEndedByADeliveryInAnotherFiber(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $hold = new HeldUpDelivery();
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $delivered = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $jobsSid = $client->subscribe('jobs', static function (NatsMessage $message) use ($delivered): void {
            $delivered->payloads[] = $message->payload;
        })->await();

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $jobsSid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = $hold->endWhenARead();

        $read = $client->processIncoming(new TimeoutCancellation(3));
        $hold->returned->getFuture()->await(new TimeoutCancellation(2));
        delay(0.02);

        self::assertSame('a read taking the socket', $hold->endedBy, 'the application read took the socket during the hold-up');
        self::assertSame(['job-1'], $delivered->payloads);
        self::assertFalse($read->isComplete(), 'the application read still waits for the server');

        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $jobsSid, 'job-2'));
        self::assertSame(1, $read->await(new TimeoutCancellation(2)));
        self::assertSame(['job-1', 'job-2'], $delivered->payloads);
        $stop->cancel();
    }

    /**
     * The wake-up an operation's read takes for its subscription: reads for the same sid share it until it fires, the
     * next delivery fires it and the next read gets a new one, the subscription's removal fires it, and a terminal close
     * fires every one and leaves none behind. One that fired stays fired, and a sid is never used again, by the same
     * connection after a terminal close and a connect() either: no later state can look like the one a read started
     * from.
     */
    public function testAWakeUpFiresOnceOnTheNextDeliveryAndIsNeverReset(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('jobs', static function (): void {})->await();

        $first = self::nextDeliveryTo($connection, $sid);
        self::assertNotNull($first);
        self::assertFalse($first->isRequested());
        self::assertSame($first, self::nextDeliveryTo($connection, $sid), 'reads for the same sid share one until it fires');

        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $sid, 'a'));
        self::assertSame(1, $connection->processIncoming(new TimeoutCancellation(1))->await());
        self::assertTrue($first->isRequested(), 'the delivery fired it');
        $second = self::nextDeliveryTo($connection, $sid);
        self::assertNotNull($second);
        self::assertNotSame($first, $second);
        self::assertFalse($second->isRequested(), 'the next read gets a new one');

        $connection->unsubscribe($sid)->await();
        self::assertTrue($second->isRequested(), "the subscription's removal fired it");
        self::assertNull(self::nextDeliveryTo($connection, $sid), 'nothing will be delivered to a removed subscription');

        $other = $connection->subscribe('other', static function (): void {})->await();
        $third = self::nextDeliveryTo($connection, $other);
        self::assertNotNull($third);
        $connection->disconnect()->await();
        self::assertTrue($third->isRequested(), 'the terminal close fired it');
        self::assertSame([], (new \ReflectionProperty(NatsConnection::class, 'deliveryWakes'))->getValue($connection), 'none is left behind');

        $connection->connect()->await();
        $fresh = $connection->subscribe('jobs', static function (): void {})->await();
        self::assertGreaterThan($other, $fresh, 'a subscription made after the terminal close gets a sid never used before');
    }

    /**
     * A client connected through the built-in socket transport, watched, to a {@see LoopbackNatsServer}.
     *
     * @return array{NatsClient, LoopbackNatsServer, WatchedTransport}
     */
    private function connectToLoopbackServer(): array
    {
        $server = LoopbackNatsServer::start();
        $this->teardowns[] = $server->close(...);
        $options = new NatsOptions(servers: [$server->url()], connectTimeoutMs: 2_000, pingIntervalSeconds: 0);
        $watched = new WatchedTransport(new AmpSocketTransport($options));
        $client = new NatsClient($options, $watched);
        $this->opened[] = $client;
        $client->connect()->await();

        return [$client, $server, $watched];
    }

    /**
     * The sids the server knows the client subscribed for $subjects: it has read their SUBs once it answered the PING of
     * a flush written behind them.
     *
     * @return list<int>
     */
    private function sidsAtTheServer(NatsClient $client, LoopbackNatsServer $server, string ...$subjects): array
    {
        $client->flush()->await();
        $sids = [];
        foreach ($subjects as $subject) {
            $sid = $server->sidFor($subject);
            self::assertNotNull($sid, 'the server read the SUB for ' . $subject);
            $sids[] = $sid;
        }

        return $sids;
    }

    private function connectWatched(WatchedTransport $watched): NatsClient
    {
        $client = new NatsClient($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null), $watched);
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /** Waits until a read takes the socket of $watched. */
    private function awaitRead(WatchedTransport $watched): void
    {
        /** @var DeferredFuture<null> $reading */
        $reading = new DeferredFuture();
        $watched->onRead = static function () use ($reading): void {
            if (!$reading->isComplete()) {
                $reading->complete();
            }
        };
        $reading->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;
    }

    /**
     * Starts the application's own read loop, processIncoming() over and over on a fiber of its own, until $stop is
     * cancelled or a read fails.
     *
     * @return Future<void> Completes when the loop has stopped.
     */
    private function startApplicationReadLoop(NatsClient $client, DeferredCancellation $stop): Future
    {
        $loop = async(static function () use ($client, $stop): void {
            while (!$stop->isCancelled()) {
                try {
                    $client->processIncoming($stop->getCancellation())->await();
                } catch (\Throwable) {
                    return;
                }
            }
        });
        $loop->ignore();

        return $loop;
    }

    private static function nextDeliveryTo(NatsConnection $connection, int $sid): ?Cancellation
    {
        $wake = (new \ReflectionMethod(NatsConnection::class, 'nextDeliveryTo'))->invoke($connection, $sid);
        self::assertTrue($wake === null || $wake instanceof Cancellation);

        return $wake;
    }

    /**
     * The built-in socket transport connected to a loopback server, plain or over TLS with a self-signed certificate,
     * and the server's end of the socket.
     *
     * @return array{AmpSocketTransport, Socket}
     */
    private function connectedSocketTransport(bool $tls): array
    {
        $bind = new BindContext();
        if ($tls) {
            $certFile = self::selfSignedCertificateFile();
            $this->teardowns[] = static function () use ($certFile): void {
                @unlink($certFile);
            };
            $bind = $bind->withTlsContext((new ServerTlsContext())->withDefaultCertificate(new Certificate($certFile)));
        }

        $server = listen('tcp://127.0.0.1:0', $bind);
        $accepted = async(static function () use ($server, $tls): ?Socket {
            $peer = $server->accept();
            if ($peer !== null && $tls) {
                $peer->setupTls();
            }

            return $peer;
        });
        $transport = new AmpSocketTransport(new NatsOptions(tlsRequired: $tls, tlsVerifyPeer: false));
        $transport->connect(($tls ? 'tls://' : 'nats://') . $server->getAddress()->toString(), 2_000)->await();
        if ($tls) {
            $transport->upgradeTls()->await();
            self::assertTrue($transport->tlsActive());
        }

        $peer = $accepted->await(new TimeoutCancellation(3));
        self::assertNotNull($peer);
        $this->teardowns[] = static function () use ($transport, $peer, $server): void {
            $transport->close()->await();
            $peer->close();
            $server->close();
        };

        return [$transport, $peer];
    }

    /**
     * A WebSocketTransport over a loopback socket, with the HTTP upgrade skipped, and the server's end of the socket, to
     * write frames to.
     *
     * @return array{WebSocketTransport, Socket}
     */
    private function connectedWebSocketTransport(): array
    {
        $server = listen('tcp://127.0.0.1:0');
        $accept = async(static fn(): ?Socket => $server->accept());
        $clientSocket = connect('tcp://' . $server->getAddress()->toString());
        $peer = $accept->await(new TimeoutCancellation(3));
        self::assertNotNull($peer);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $clientSocket);
        $this->teardowns[] = static function () use ($transport, $peer, $server): void {
            $transport->close()->await();
            $peer->close();
            $server->close();
        };

        return [$transport, $peer];
    }

    /** The bytes the WebSocket transport has taken off the socket and holds, toward the frame it has not completed. */
    private static function webSocketBufferedBytes(WebSocketTransport $transport): int
    {
        $buffer = (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->getValue($transport);
        $chunks = (new \ReflectionProperty(WebSocketTransport::class, 'readChunksLength'))->getValue($transport);
        self::assertIsString($buffer);
        self::assertIsInt($chunks);

        return strlen($buffer) + $chunks;
    }

    /** $size bytes that differ from one 4-byte step to the next, so a lost, doubled or reordered piece changes them. */
    private static function distinctBytes(int $size, int $seed): string
    {
        $bytes = '';
        for ($i = 0; strlen($bytes) < $size; $i++) {
            $bytes .= pack('N', $seed * 1_000_003 + $i);
        }

        return substr($bytes, 0, $size);
    }

    /** Writes a throwaway self-signed certificate and its key, in one PEM file, for the TLS test server. */
    private static function selfSignedCertificateFile(): string
    {
        if (!\extension_loaded('openssl')) {
            self::markTestSkipped('ext-openssl is required to generate the self-signed test certificate');
        }

        $key = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
        if ($key === false) {
            throw new \RuntimeException('openssl_pkey_new() failed');
        }

        $csr = openssl_csr_new(['commonName' => '127.0.0.1'], $key, ['digest_alg' => 'sha256']);
        if (!$csr instanceof \OpenSSLCertificateSigningRequest) {
            throw new \RuntimeException('openssl_csr_new() failed');
        }

        $x509 = openssl_csr_sign($csr, null, $key, 2, ['digest_alg' => 'sha256']);
        if ($x509 === false) {
            throw new \RuntimeException('openssl_csr_sign() failed');
        }

        $certPem = '';
        $keyPem = '';
        if (!openssl_x509_export($x509, $certPem) || !openssl_pkey_export($key, $keyPem)) {
            throw new \RuntimeException('exporting the self-signed certificate failed');
        }

        $path = tempnam(sys_get_temp_dir(), 'nats-wakeup-tls-');
        if ($path === false) {
            throw new \RuntimeException('tempnam() failed');
        }

        file_put_contents($path, $certPem . $keyPem);

        return $path;
    }
}
