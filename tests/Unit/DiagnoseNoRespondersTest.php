<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Enum\NoRespondersCause;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\SpinBreakingTransport;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * JetStreamContext::diagnoseNoResponders(): why a pull ended with status 503 (no responders), told by a CONSUMER.INFO
 * asked while the connection the pull came on is watched (ideaconnect/symfony-nats-messenger#106).
 *
 * nats-server answers a pull with 503 when the consumer or its stream does not exist, and also while a server that is
 * going away (a graceful stop, lame duck mode) has stopped the consumer and not yet closed the connection. A server that
 * stays answers the question; one that is going away answers nothing, and then closes the connection (a graceful stop)
 * or announces lame duck mode and keeps it open for a while. Over the scripted server in
 * tests/Support/ReconnectingTransport.php: each session is a server, and a responder answers the question per session,
 * or leaves it unanswered. An operation that has to end with an answer gets a budget it never uses up (5 s); only the
 * question that has to go unanswered on a connection that stays gets a short one.
 */
final class DiagnoseNoRespondersTest extends TestCase
{
    use ReconnectScenarios;

    private const QUESTION = '$JS.API.CONSUMER.INFO.ORDERS.worker';

    private const BUDGET_MS = 5_000;

    private const CONSUMER = '{"type":"io.nats.jetstream.api.v1.consumer_info_response","stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker","ack_policy":"explicit"}}';

    private const LAME_DUCK_INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"ldm":true}' . "\r\n";

    private const REFUSAL = "-ERR 'Permissions Violation for Publish to \"" . self::QUESTION . "\"'\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, NoRespondersCause}> */
    public static function answers(): iterable
    {
        yield 'the consumer' => [self::CONSUMER, NoRespondersCause::ConsumerFound];
        yield '404 consumer not found' => ['{"error":{"code":404,"err_code":10014,"description":"consumer not found"}}', NoRespondersCause::ConsumerNotFound];
        yield '404 stream not found' => ['{"error":{"code":404,"err_code":10059,"description":"stream not found"}}', NoRespondersCause::StreamNotFound];
        yield 'a 404 without err_code' => ['{"error":{"code":404,"description":"consumer not found"}}', NoRespondersCause::ConsumerNotFound];
        yield 'JetStream not enabled for the account' => ['{"error":{"code":503,"err_code":10039,"description":"jetstream not enabled for account"}}', NoRespondersCause::JetStreamError];
        yield 'JetStream temporarily unavailable' => ['{"error":{"code":503,"err_code":10008,"description":"JetStream system temporarily unavailable"}}', NoRespondersCause::JetStreamError];
        yield 'another error' => ['{"error":{"code":500,"description":"backend failure"}}', NoRespondersCause::JetStreamError];
        yield 'a malformed answer' => ['not json', NoRespondersCause::JetStreamError];
        yield 'an answer that is JSON but not an object' => ['123', NoRespondersCause::JetStreamError];
    }

    /**
     * A server that stays answers the question at once, and the answer is the cause: one CONSUMER.INFO, the connection
     * Open and nothing left waiting.
     */
    #[DataProvider('answers')]
    public function testTheAnswerOfAServerThatStaysTellsTheCause(string $answer, NoRespondersCause $cause): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);
        $transport->responder = self::answering($transport, [0 => $answer]);

        self::assertSame($cause, $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await());
        self::assertSame([1], self::questionsPerSession($transport, 1));
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame([], self::waitingRequests($client));
    }

    /**
     * A server without JetStream has no responder for the question either, and answers it with status 503 at once:
     * JetStream is not there to answer, which is a JetStream error, not a server going away.
     */
    public function testNoResponderForTheQuestionIsAJetStreamError(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport): array {
            $sid = $replyTo === null ? null : $transport->sidFor($replyTo);

            return $subject === self::QUESTION && $replyTo !== null && $sid !== null
                ? [ReconnectingTransport::hmsgFrame($replyTo, $sid, "NATS/1.0 503\r\n\r\n", '')]
                : [];
        };

        self::assertSame(NoRespondersCause::JetStreamError, $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await());
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * Without reconnect, the server that is going away closes the connection with the question unanswered: the client
     * fails it as it closes, and the cause is ServerGoingAway, long before the 5 s timeout.
     */
    public function testAQuestionTheServerClosesTheConnectionOnIsServerGoingAway(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);
        $transport->responder = self::answering($transport, []);
        self::dropAfterTheQuestionOn($transport, 0);

        $start = hrtime(true);
        $cause = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(NoRespondersCause::ServerGoingAway, $cause);
        self::assertLessThan(2.5, $elapsed, sprintf('the diagnosis took %.3f s, with a 5 s timeout', $elapsed));
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /** @return iterable<string, array{bool}> */
    public static function recoveries(): iterable
    {
        yield 'the client is still recovering the connection' => [true];
        yield 'the client has replaced the connection' => [false];
    }

    /**
     * With reconnect, the client keeps the question waiting for its reply across the reconnect, up to its timeout, but
     * the server that would have answered it is gone: the cause is ServerGoingAway as soon as the client is recovering
     * the connection or has replaced it, long before the 5 s timeout. The question is not asked again on the new
     * connection, whose server would answer it with the consumer, and it is given up.
     */
    #[DataProvider('recoveries')]
    public function testAQuestionWhoseConnectionTheClientRecoversIsServerGoingAway(bool $holdTheDial): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: true);
        $transport->responder = self::answering($transport, [1 => self::CONSUMER]);
        if ($holdTheDial) {
            $transport->holdNextDial();
        }
        self::dropAfterTheQuestionOn($transport, 0);

        $start = hrtime(true);
        $cause = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(NoRespondersCause::ServerGoingAway, $cause);
        self::assertLessThan(2.5, $elapsed, sprintf('the diagnosis took %.3f s, with a 5 s timeout', $elapsed));
        if ($holdTheDial) {
            self::assertSame(ConnectionState::Connecting, $client->state());
            $transport->releaseDial();
        }
        $this->waitUntilOpen($client);
        self::assertSame([1, 0], self::questionsPerSession($transport, 2));
        $this->waitUntil(static fn(): bool => self::waitingRequests($client) === []);
    }

    /**
     * With reconnect, a question whose write meets a dead socket goes out on the connection that follows, whose server
     * answers it with the consumer. That answer is not about the 503 of the server before: the cause is ServerGoingAway.
     */
    public function testAnAnswerReadOnceTheConnectionHasBeenReplacedIsServerGoingAway(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: true);
        $transport->responder = self::answering($transport, [1 => self::CONSUMER]);
        $transport->failNextWriteContaining(self::QUESTION);

        self::assertSame(NoRespondersCause::ServerGoingAway, $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await());
        self::assertSame([0, 1], self::questionsPerSession($transport, 2), 'the question went out on the new connection and was answered there');
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * With reconnect, a question whose write meets a dead socket, and whose write on the connection that follows fails
     * too, ends with the transport's own error (TransportClosedException here, Amp's StreamException on a real socket),
     * which is not a NatsException. The diagnosis still returns a cause: the connection the pull came on is gone. It
     * used to throw that error.
     */
    public function testAQuestionWhoseWriteFailsOnTheNewConnectionTooIsServerGoingAway(): void
    {
        $transport = new ReconnectingTransport();
        $armed = false;
        $client = $this->client($transport, reconnect: true, connectionListener: static function (ConnectionEvent $event) use ($transport, &$armed): void {
            if ($event === ConnectionEvent::Reconnected && !$armed) {
                $armed = true;
                $transport->failNextWriteContaining(self::QUESTION);
            }
        });
        $transport->responder = self::answering($transport, []);
        $transport->failNextWriteContaining(self::QUESTION);

        self::assertSame(NoRespondersCause::ServerGoingAway, $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await());
        self::assertTrue($armed, 'the client reconnected and its write of the question failed again');
    }

    /** @return iterable<string, array{bool}> */
    public static function reconnectModes(): iterable
    {
        yield 'without reconnect' => [false];
        yield 'with reconnect' => [true];
    }

    /**
     * In lame duck mode the server stops JetStream, which then answers nothing, and tells the client it is going away
     * (INFO with `ldm`) while it keeps the connection open: the cause is ServerGoingAway once the INFO is read, long
     * before the 5 s timeout. With one server to go to, the connection stays Open, and the question is given up.
     */
    #[DataProvider('reconnectModes')]
    public function testAQuestionLeftUnansweredByAServerThatAnnouncesLameDuckModeIsServerGoingAway(bool $reconnect): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: $reconnect, requestTimeoutMs: 30_000);
        $transport->responder = self::answering($transport, []);
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if (str_contains($bytes, ' ' . self::QUESTION . ' ')) {
                $transport->pushFrame(self::LAME_DUCK_INFO);
            }
        };

        $start = hrtime(true);
        $cause = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker')->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(NoRespondersCause::ServerGoingAway, $cause);
        self::assertLessThan(2.5, $elapsed, sprintf('the diagnosis took %.3f s, with a 30 s timeout', $elapsed));
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertTrue($client->serverInfo()?->lameDuckMode);
        self::assertSame([1], self::questionsPerSession($transport, 1));
        $this->waitUntil(static fn(): bool => self::waitingRequests($client) === []);
    }

    /**
     * A server that already announced lame duck mode is going away: nothing is asked.
     */
    public function testAServerThatAlreadyAnnouncedLameDuckModeIsServerGoingAwayWithoutAQuestion(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);
        $transport->responder = self::answering($transport, [0 => self::CONSUMER]);
        $transport->pushFrame(self::LAME_DUCK_INFO);
        $client->rtt()->await();
        self::assertTrue($client->serverInfo()?->lameDuckMode);

        self::assertSame(NoRespondersCause::ServerGoingAway, $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await());
        self::assertSame([0], self::questionsPerSession($transport, 1));
    }

    /**
     * A client recovering the connection when it is asked: the connection the pull came on is gone, and nothing is
     * asked, on it or on the connection that follows.
     */
    public function testAClientRecoveringTheConnectionIsServerGoingAwayWithoutAQuestion(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: true);
        $transport->responder = self::answering($transport, [0 => self::CONSUMER, 1 => self::CONSUMER]);
        $recovery = $this->startRecoveryHeldMidDial($client, $transport);

        self::assertSame(NoRespondersCause::ServerGoingAway, $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await());

        $transport->releaseDial();
        $recovery->await();
        $this->waitUntilOpen($client);
        self::assertSame([0, 0], self::questionsPerSession($transport, 2));
    }

    /**
     * A client the application has closed has no connection to ask on: nothing is asked.
     */
    public function testAClosedClientIsServerGoingAwayWithoutAQuestion(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);
        $transport->responder = self::answering($transport, [0 => self::CONSUMER]);
        $client->disconnect()->await();

        self::assertSame(NoRespondersCause::ServerGoingAway, $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS)->await());
        self::assertSame([0], self::questionsPerSession($transport, 1));
    }

    /**
     * The application closes the client while the question waits, with a close that takes a while (a TLS or WebSocket
     * close handshake): the close ends the question first, while the client still reports Open. The application's close
     * is the connection leaving the client's use, as a drain() is: ServerGoingAway. It was NoAnswer.
     */
    public function testAnApplicationCloseWhileTheQuestionWaitsIsServerGoingAway(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);
        $transport->responder = self::answering($transport, []);
        $transport->closeDelay = 0.3;

        $diagnosis = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', self::BUDGET_MS);
        $this->waitUntil(static fn(): bool => self::questionsPerSession($transport, 1) === [1]);
        $close = $client->disconnect();

        self::assertSame(NoRespondersCause::ServerGoingAway, $diagnosis->await());
        $close->await();
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A question left unanswered on a connection that stays - a user allowed to pull but not to ask for consumer info,
     * whom the server answers nothing - is NoAnswer at the timeout given, not the client's 5 s request timeout. The
     * connection stays Open.
     */
    #[DataProvider('reconnectModes')]
    public function testAQuestionLeftUnansweredOnAConnectionThatStaysIsNoAnswerAtItsTimeout(bool $reconnect): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: $reconnect);
        $transport->responder = self::answering($transport, []);

        $start = hrtime(true);
        $cause = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', 200)->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(NoRespondersCause::NoAnswer, $cause);
        self::assertGreaterThan(0.15, $elapsed, sprintf('the diagnosis ended after %.3f s, before its 0.2 s timeout', $elapsed));
        self::assertLessThan(2.5, $elapsed, sprintf('the diagnosis took %.3f s: not the 0.2 s it was given', $elapsed));
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame([1], self::questionsPerSession($transport, 1));
    }

    /**
     * A user allowed to pull but not to ask for consumer info: the server refuses the question at once, with a
     * permissions violation, which client 2.30.0 reads as soon as it comes. On a connection that stays that is still
     * NoAnswer at the timeout, not at the refusal: the refusal comes from a server going away as well.
     */
    #[DataProvider('reconnectModes')]
    public function testAQuestionTheServerRefusesOnAConnectionThatStaysIsNoAnswerAtItsTimeout(bool $reconnect): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: $reconnect);
        $transport->responder = self::refusing();

        $start = hrtime(true);
        $cause = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', 300)->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(NoRespondersCause::NoAnswer, $cause);
        self::assertGreaterThan(0.25, $elapsed, sprintf('the diagnosis ended after %.3f s, at the refusal, before its 0.3 s timeout', $elapsed));
        self::assertLessThan(2.5, $elapsed, sprintf('the diagnosis took %.3f s: not the 0.3 s it was given', $elapsed));
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame([1], self::questionsPerSession($transport, 1));
    }

    /** @return iterable<string, array{\Closure(ReconnectingTransport): void}> */
    public static function serversGoingAwayAfterTheRefusal(): iterable
    {
        yield 'lame duck mode, announced after the refusal' => [static function (ReconnectingTransport $transport): void {
            $transport->pushFrame(self::LAME_DUCK_INFO);
        }];
        yield 'a graceful stop, which closes the connection after the refusal' => [static function (ReconnectingTransport $transport): void {
            $transport->dropConnection();
        }];
    }

    /**
     * The same user on a server going away: the refusal comes at once, and the server announces lame duck mode or
     * closes the connection 150 ms later. The connection is watched, and read, after the refusal: ServerGoingAway,
     * long before the 30 s timeout. Reading the refusal as NoAnswer at once, as client 2.30.0's first form did, read
     * the 503 of a server going away as a missing consumer (ideaconnect/symfony-nats-messenger#106).
     *
     * @param \Closure(ReconnectingTransport): void $goAway
     */
    #[DataProvider('serversGoingAwayAfterTheRefusal')]
    public function testAQuestionTheServerRefusesBeforeItGoesAwayIsServerGoingAway(\Closure $goAway): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false, requestTimeoutMs: 30_000);
        $transport->responder = self::refusing();
        $transport->afterWrite = static function (string $bytes) use ($transport, $goAway): void {
            if (str_contains($bytes, ' ' . self::QUESTION . ' ')) {
                EventLoop::delay(0.15, static fn() => $goAway($transport));
            }
        };

        $start = hrtime(true);
        $cause = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker')->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(NoRespondersCause::ServerGoingAway, $cause);
        self::assertGreaterThan(0.1, $elapsed, sprintf('the diagnosis ended after %.3f s, before the server went away', $elapsed));
        self::assertLessThan(2.5, $elapsed, sprintf('the diagnosis took %.3f s, with a 30 s timeout', $elapsed));
    }

    /** @return iterable<string, array{\Closure(ReconnectingTransport): void}> */
    public static function serversGoingAwayWhileTheNextDialIsHeld(): iterable
    {
        yield 'the server closes the connection' => [static function (ReconnectingTransport $transport): void {
            $transport->dropConnection();
        }];
    }

    /**
     * The same user with reconnect on, and the dial of the next server held: 150 ms after the refusal the server
     * closes the connection. The watch's read meets it first and runs the reconnect itself, waiting for all of it; the
     * watch goes on looking at the connection every 20 ms while that read is in flight, and the cause is ServerGoingAway
     * while the dial is still held, the client Connecting, long before the 30 s timeout. The reconnect goes on without
     * the watch, and the question is not asked again on the new connection. In 2.30.0 the watch awaited that read, and
     * so returned only once the reconnect was over (#257): here only after the dial's release, which the test makes
     * only once the diagnosis has returned.
     *
     * @param \Closure(ReconnectingTransport): void $goAway
     */
    #[DataProvider('serversGoingAwayWhileTheNextDialIsHeld')]
    public function testARefusedQuestionIsServerGoingAwayWhileTheReconnectItsWatchStartedIsStillDialling(\Closure $goAway): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: true, requestTimeoutMs: 30_000);
        $transport->responder = self::refusing();
        $transport->holdNextDial();
        $this->resources()->onRelease($transport->releaseDial(...), 'held dial of the next server');
        $transport->afterWrite = static function (string $bytes) use ($transport, $goAway): void {
            if ($transport->epoch() === 0 && str_contains($bytes, ' ' . self::QUESTION . ' ')) {
                EventLoop::delay(0.15, static fn() => $goAway($transport));
            }
        };

        $start = hrtime(true);
        $diagnosis = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker');
        $this->waitUntil(static fn(): bool => $diagnosis->isComplete(), 2.5);
        $elapsed = $this->secondsSince($start);

        self::assertSame(NoRespondersCause::ServerGoingAway, $diagnosis->await());
        self::assertGreaterThan(0.1, $elapsed, sprintf('the diagnosis ended after %.3f s, before the server went away', $elapsed));
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the dial of the next server is still held');
        self::assertCount(2, $transport->connectCalls, 'the reconnect is dialling the next server');

        $transport->releaseDial();
        $this->waitUntilOpen($client);
        self::assertSame([1, 0], self::questionsPerSession($transport, 2));
        $this->waitUntil(static fn(): bool => self::waitingRequests($client) === []);
    }

    /**
     * The reconnect that the watch's read runs gives up once the watch has returned: the dial of the next server is
     * refused when it is let go, and the client has one attempt. The read then fails ("Reconnect attempts exhausted")
     * with no one awaiting it any more, and the failure is not reported as an unhandled one: the client is Closed, and
     * nothing reaches the event loop. A guard: it passes without the watch's explicit ignore() of that read too, since
     * Amp counts a future awaited once as handled, and the watch awaits each read it starts.
     */
    public function testAReconnectThatTheWatchsReadStartedMayFailAfterTheWatchHasReturned(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: true, requestTimeoutMs: 30_000, maxReconnectAttempts: 1);
        $transport->responder = self::refusing();
        $transport->holdNextDial();
        $this->resources()->onRelease($transport->releaseDial(...), 'held dial of the next server');
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($transport->epoch() === 0 && str_contains($bytes, ' ' . self::QUESTION . ' ')) {
                EventLoop::delay(0.15, static fn() => $transport->dropConnection());
            }
        };

        $diagnosis = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker');
        $this->waitUntil(static fn(): bool => $diagnosis->isComplete(), 2.5);
        self::assertSame(NoRespondersCause::ServerGoingAway, $diagnosis->await());
        $stateThen = $client->state();
        self::assertSame(ConnectionState::Connecting, $stateThen);

        $transport->refuseDials();
        $transport->releaseDial();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Closed);
        unset($diagnosis);
        gc_collect_cycles();
        delay(0.01);
        self::assertSame([1], self::questionsPerSession($transport, 1));
    }

    /**
     * The application closes the client while the watch waits, with a close that takes 0.3 s (a TLS or WebSocket close
     * handshake): ServerGoingAway at the next look, and the watch's read of the socket, still waiting, ends with the
     * watch rather than at the question's 30 s timeout or once the close is over. A guard of the read kept across the
     * looks: 2.30.0, whose read lasted one look, passes it as well.
     */
    public function testTheWatchsReadOfTheSocketEndsWithTheWatch(): void
    {
        $inner = new ReconnectingTransport();
        $transport = new WatchedTransport($inner);
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 30_000,
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
        ), $transport);
        $this->opened[] = $this->own($client);
        $client->connect()->await();
        $inner->responder = self::refusing();
        $inner->closeDelay = 0.3;

        $diagnosis = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker');
        $this->waitUntil(static fn(): bool => self::questionsPerSession($inner, 1) === [1] && $transport->readsUnderWay === 1);
        $close = $client->disconnect();

        self::assertSame(NoRespondersCause::ServerGoingAway, $diagnosis->await());
        $this->waitUntil(static fn(): bool => $transport->readsUnderWay === 0 || $close->isComplete());
        self::assertFalse($close->isComplete(), 'the watch\'s read was still waiting on the socket when the close was over');
        self::assertSame(1, $transport->cancelledReads);
        $close->await();
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The refused question on a connection that stays, and 50 ms later an -ERR that fails the read that brings it and
     * keeps the connection open (`Invalid Publish Subject`, for another publish of the application): the watch's read
     * fails, the connection is still there, and the watch goes on until the question's 0.3 s are over: NoAnswer, the
     * connection Open. A watch that took the failed read for the connection gone would make it ServerGoingAway at once.
     */
    public function testAFailedReadOfTheWatchOnAConnectionThatStaysIsStillNoAnswerAtItsTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);
        $transport->responder = self::refusing();
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if (str_contains($bytes, ' ' . self::QUESTION . ' ')) {
                EventLoop::delay(0.05, static fn() => $transport->pushFrame("-ERR 'Invalid Publish Subject'\r\n"));
            }
        };

        $start = hrtime(true);
        $cause = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', 300)->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(NoRespondersCause::NoAnswer, $cause);
        self::assertGreaterThan(0.25, $elapsed, sprintf('the diagnosis ended after %.3f s, before its 0.3 s timeout', $elapsed));
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame([1], self::questionsPerSession($transport, 1));
    }

    /** @return iterable<string, array{bool}> */
    public static function idleReads(): iterable
    {
        yield 'an idle read that waits, as a socket\'s does' => [true];
        yield 'an idle read that returns at once' => [false];
    }

    /**
     * The refused question on a connection that stays, over a transport whose idle read waits and over one whose idle
     * read returns '' at once, as the read contract allows (TransportInterface::readLine()): NoAnswer at the 0.3 s
     * timeout either way. The watch pauses 1 ms after a read that consumed nothing, as the client's other wait loops
     * do. In 2.30.0 it read again at once, and over the second transport it spun at full CPU with no timer of the
     * process firing, its own timeout included, and never returned (#256). The reads are counted, not timed: past
     * 10,000 the transport pauses each read (tests/Support/SpinBreakingTransport.php), so that a watch that spins ends
     * at its timeout too, with a count far above what a watch that pauses makes.
     */
    #[DataProvider('idleReads')]
    public function testARefusedQuestionOnAConnectionThatStaysIsNoAnswerAtItsTimeoutWhateverAnIdleReadDoes(bool $idleReadWaits): void
    {
        $fake = new FakeTransport([ReconnectingTransport::INFO, "PONG\r\n"], $idleReadWaits);
        $fake->onWrite = static fn(string $bytes): array => str_contains($bytes, 'PUB ' . self::QUESTION . ' ') ? [self::REFUSAL] : [];
        $transport = new SpinBreakingTransport($fake, 10_000);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: self::BUDGET_MS, pingIntervalSeconds: 0), $transport);
        $this->opened[] = $this->own($client);
        $client->connect()->await();
        $readsBefore = $transport->reads;

        $start = hrtime(true);
        $cause = $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', 300)->await();
        $elapsed = $this->secondsSince($start);
        $reads = $transport->reads - $readsBefore;

        self::assertSame(NoRespondersCause::NoAnswer, $cause);
        self::assertLessThan(1_000, $reads, sprintf('%d reads of the socket in %.3f s: the watch spins on idle reads', $reads, $elapsed));
        self::assertGreaterThan(0.25, $elapsed, sprintf('the diagnosis ended after %.3f s, before its 0.3 s timeout', $elapsed));
        self::assertLessThan(2.5, $elapsed, sprintf('the diagnosis took %.3f s: not the 0.3 s it was given', $elapsed));
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /** @return iterable<string, array{string, string}> */
    public static function invalidNames(): iterable
    {
        yield 'a stream with a dot' => ['OR.DERS', 'worker'];
        yield 'an empty consumer' => ['ORDERS', ''];
        yield 'a consumer with a wildcard' => ['ORDERS', 'work*'];
    }

    /**
     * Invalid names are refused before anything is sent, as by every JetStream call that takes them.
     */
    #[DataProvider('invalidNames')]
    public function testInvalidNamesAreRefusedBeforeAnythingIsSent(string $stream, string $consumer): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);

        try {
            $client->jetStream()->diagnoseNoResponders($stream, $consumer);
            self::fail('expected the names to be refused');
        } catch (JetStreamException $refused) {
            self::assertStringStartsWith('Invalid ', $refused->getMessage());
        }

        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.'));
    }

    /** @return iterable<string, array{int}> */
    public static function timeoutsThatAreNotPositive(): iterable
    {
        yield 'zero' => [0];
        yield 'negative' => [-1];
    }

    /**
     * A timeout that is not positive is refused before anything is sent: the request would refuse it as well, but only
     * once awaited, and the diagnosis would have taken that for a question nothing answered.
     */
    #[DataProvider('timeoutsThatAreNotPositive')]
    public function testATimeoutThatIsNotPositiveIsRefusedBeforeAnythingIsSent(int $timeoutMs): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, reconnect: false);

        try {
            $client->jetStream()->diagnoseNoResponders('ORDERS', 'worker', $timeoutMs);
            self::fail('expected the timeout to be refused');
        } catch (\InvalidArgumentException $refused) {
            self::assertSame('The timeout of diagnoseNoResponders() must be a positive number of milliseconds', $refused->getMessage());
        }

        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.'));
    }

    /**
     * @param (\Closure(ConnectionEvent, ?\Throwable): void)|null $connectionListener
     */
    private function client(ReconnectingTransport $transport, bool $reconnect, int $requestTimeoutMs = self::BUDGET_MS, ?\Closure $connectionListener = null, int $maxReconnectAttempts = 1_000): NatsClient
    {
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: $reconnect,
            maxReconnectAttempts: $maxReconnectAttempts,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $connectionListener,
        ), $transport);
        $this->opened[] = $this->own($client);
        $client->connect()->await();

        return $client;
    }

    /**
     * Answers the question on session N with $answers[N], and leaves it unanswered on a session without one.
     *
     * @param array<int, string> $answers
     * @return \Closure(string, ?string, string): list<string>
     */
    private static function answering(ReconnectingTransport $transport, array $answers): \Closure
    {
        return static function (string $subject, ?string $replyTo) use ($transport, $answers): array {
            $answer = $answers[$transport->epoch()] ?? null;
            if ($subject !== self::QUESTION || $replyTo === null || $answer === null) {
                return [];
            }

            return $transport->replyFrame($replyTo, $answer);
        };
    }

    /**
     * Refuses the question, as nats-server refuses a user who may not publish to its subject: an -ERR, and the connection
     * kept open.
     *
     * @return \Closure(string, ?string, string): list<string>
     */
    private static function refusing(): \Closure
    {
        return static fn(string $subject): array => $subject === self::QUESTION ? [self::REFUSAL] : [];
    }

    /** Drops the connection right after the question was written on session $epoch, before any answer. */
    private static function dropAfterTheQuestionOn(ReconnectingTransport $transport, int $epoch): void
    {
        $transport->afterWrite = static function (string $bytes) use ($transport, $epoch): void {
            if ($transport->epoch() === $epoch && str_contains($bytes, ' ' . self::QUESTION . ' ')) {
                $transport->dropConnection();
            }
        };
    }

    /**
     * How many times each of the first $sessions sessions was asked the question.
     *
     * @return list<int>
     */
    private static function questionsPerSession(ReconnectingTransport $transport, int $sessions): array
    {
        $counts = array_fill(0, $sessions, 0);
        foreach ($transport->writes as $write) {
            if ($write['epoch'] < $sessions) {
                $counts[$write['epoch']] += substr_count($write['bytes'], 'PUB ' . self::QUESTION . ' ');
            }
        }

        return array_values($counts);
    }

    /**
     * The requests the client's connection still waits for a reply to, by their reply token.
     *
     * @return array<string, mixed>
     */
    private static function waitingRequests(NatsClient $client): array
    {
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);
        $waiters = (new \ReflectionProperty(NatsConnection::class, 'muxWaiters'))->getValue($connection);
        self::assertIsArray($waiters);

        /** @var array<string, mixed> $waiters */
        return $waiters;
    }
}
