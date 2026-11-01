<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\Future;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\JetStream\Models\PubAck;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * A JetStream publish() with a `$msgId` whose connection is lost after its frame went out and before its PubAck came
 * (#249). With reconnect on, the reconnect replays the reply inbox, but nothing sent the request again: when the server
 * that took the frame was the stream's leader and went down with the connection, the publish waited out the rest of
 * its request timeout on the new connection and threw a TimeoutException, though the frame could safely go out again
 * there, its Nats-Msg-Id making a second copy harmless. Under `NatsOptions::$resendJetStreamPublishOnReconnect` the
 * frame now goes out once more on the new connection, within the publish's own timeout, and whichever reply comes
 * first resolves the publish. A `409 duplicate message id is in process` (err_code 10158) answering that second frame
 * leaves the publish waiting for the first frame's ack.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php: each session is a server, and a session that
 * answers nothing to the publish stands for the stream leader that went down with the connection. The connection is
 * dropped right after the publish's frame was written to it (see {@see dropAfterThePublishOn()}). The time bounds sit
 * far from both the expected and the old behaviour: the old one waits for the whole request timeout.
 */
final class JetStreamPublishResendOnReconnectTest extends TestCase
{
    use ReconnectScenarios;

    private const SUBJECT = 'orders.created';

    private const PAYLOAD = '{"order":"secret-payload-1"}';

    private const MSG_ID = 'order-1';

    private const IN_PROCESS = '{"error":{"code":409,"err_code":10158,"description":"duplicate message id is in process"}}';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function acksOnTheNewConnection(): iterable
    {
        yield 'the first copy was lost: stored now' => [false];
        yield 'the first copy was stored: a duplicate' => [true];
    }

    /**
     * The connection drops right after the publish's HPUB was written to it, and the server that took it answers
     * nothing: the same HPUB - the same subject, payload, Nats-Msg-Id and reply subject - goes out on the new session,
     * whose ack resolves the publish well before its 5 s timeout, whether that server stores the message now or
     * answers that it has it already. The second write is logged with the subject and both connections' generations,
     * never the payload. The publish used to throw its TimeoutException after the whole 5 s.
     */
    #[DataProvider('acksOnTheNewConnection')]
    public function testAPublishWhoseConnectionDropsAfterItsFrameWentOutSendsItAgainOnTheNewConnection(bool $duplicate): void
    {
        $transport = new ReconnectingTransport();
        $logger = new class extends AbstractLogger {
            /** @var list<array{string, string, array<mixed>}> */
            public array $records = [];

            /** @param array<mixed> $context */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->records[] = [is_string($level) ? $level : '', (string) $message, $context];
            }
        };
        $client = $this->client($transport, requestTimeoutMs: 5_000, logger: $logger);
        $transport->responder = self::stream($transport, [1 => self::ack(7, $duplicate)]);
        self::dropAfterThePublishOn($transport, 0);

        $start = hrtime(true);
        $ack = $client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID)->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(7, $ack->seq);
        self::assertSame($duplicate, $ack->duplicate);
        self::assertLessThan(2.0, $elapsed, sprintf('the publish took %.3f s, with a 5 s timeout', $elapsed));
        self::assertSame([1, 1], self::publishesPerSession($transport, 2));
        [$first, $second] = self::publishFrames($transport);
        self::assertSame($first, $second, 'the same frame went out again: subject, headers, payload and reply subject');
        self::assertStringContainsString('Nats-Msg-Id:' . self::MSG_ID, $second);

        $resent = array_values(array_filter($logger->records, static fn(array $record): bool => $record[1] === 'NATS request sent again on the new connection'));
        self::assertCount(1, $resent);
        self::assertSame('info', $resent[0][0]);
        self::assertSame(['subject' => self::SUBJECT, 'generation' => 2, 'previousGeneration' => 1], $resent[0][2]);
        self::assertStringNotContainsString('secret-payload', serialize($logger->records), 'no log record carries the payload');
    }

    /** @return iterable<string, array{?string, bool, array<string,string>}> */
    public static function publishesThatAreNotSentAgain(): iterable
    {
        yield 'no msgId, option on' => [null, true, []];
        yield 'other headers but no msgId, option on' => [null, true, ['X-Trace' => 't-1']];
        yield 'a msgId, option off (the default)' => [self::MSG_ID, false, []];
    }

    /**
     * A publish without a `$msgId` (with headers of its own or none), or with one while the option is off: nothing is
     * written again, and the publish times out at its 1 s deadline as it always did, the connection open again on the
     * second session meanwhile.
     *
     * @param array<string,string> $headers
     */
    #[DataProvider('publishesThatAreNotSentAgain')]
    public function testAPublishThatMayNotGoOutTwiceTimesOutAsBefore(?string $msgId, bool $option, array $headers): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, resend: $option, requestTimeoutMs: 1_000);
        $transport->responder = self::stream($transport, [1 => self::ack(7)]);
        self::dropAfterThePublishOn($transport, 0);

        $start = hrtime(true);
        $error = self::failure($client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, $headers, msgId: $msgId));
        $elapsed = $this->secondsSince($start);

        self::assertInstanceOf(TimeoutException::class, $error);
        self::assertSame('Request timed out for subject ' . self::SUBJECT, $error->getMessage());
        self::assertGreaterThan(0.8, $elapsed, sprintf('the publish ended after %.3f s, before its 1 s deadline', $elapsed));
        self::assertSame(ConnectionState::Open, $client->state(), 'the reconnect was over long before the deadline');
        self::assertSame([1, 0], self::publishesPerSession($transport, 2));
    }

    /**
     * With waiting for a reconnect disabled the publish fails at once with "Connection is not open", as every request
     * whose read notices a lost connection does then, and its frame never goes out on the new connection.
     */
    public function testWithWaitingForAReconnectDisabledThePublishFailsAtOnceAsBefore(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, waitForReconnect: false, requestTimeoutMs: 5_000);
        $transport->responder = self::stream($transport, [1 => self::ack(7)]);
        self::dropAfterThePublishOn($transport, 0);

        $start = hrtime(true);
        $error = self::failure($client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID));
        $elapsed = $this->secondsSince($start);

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertLessThan(2.0, $elapsed);
        $this->waitUntilOpen($client);
        delay(0.05);
        self::assertSame([1, 0], self::publishesPerSession($transport, 2));
    }

    /**
     * No reconnect, only a slow server: its ack after 0.5 s resolves the publish, and nothing is written again. A second
     * write after a plain wait misfired in ideaconnect/symfony-nats-messenger#95: only a replaced connection triggers it.
     */
    public function testASlowAckWithoutAReconnectIsWaitedForWithNoSecondWrite(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, requestTimeoutMs: 3_000);
        $transport->responder = self::stream($transport, [0 => self::ack(3)]);
        $transport->responseDelay = 0.5;

        $start = hrtime(true);
        $ack = $client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID)->await();

        self::assertSame(3, $ack->seq);
        self::assertGreaterThan(0.4, $this->secondsSince($start));
        self::assertSame([1], self::publishesPerSession($transport, 1));
    }

    /**
     * The new server answers the second frame with `409 duplicate message id is in process` - the old server outlived
     * the connection, and its stream leader is still storing the first frame - and the first frame's ack comes 0.3 s
     * later, to the same reply subject: the publish returns that ack. The 409 is not a refusal and does not end it.
     */
    public function testADuplicateInProcessAnswerToTheSecondFrameLeavesThePublishWaitingForTheFirstFramesAck(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, requestTimeoutMs: 5_000);
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport): array {
            if ($subject !== self::SUBJECT || $replyTo === null || $transport->epoch() !== 1) {
                return [];
            }

            EventLoop::delay(0.3, static function () use ($transport, $replyTo): void {
                foreach ($transport->replyFrame($replyTo, self::ack(11)) as $frame) {
                    $transport->pushFrame($frame);
                }
            });

            return $transport->replyFrame($replyTo, self::IN_PROCESS);
        };
        self::dropAfterThePublishOn($transport, 0);

        $start = hrtime(true);
        $ack = $client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID)->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(11, $ack->seq);
        self::assertGreaterThan(0.25, $elapsed, 'the publish waited past the 409 for the first frame\'s ack');
        self::assertLessThan(2.0, $elapsed);
        self::assertSame([1, 1], self::publishesPerSession($transport, 2));
    }

    /**
     * The same 409 with no ack after it: the publish ends with its TimeoutException at its 1.5 s deadline, the outcome
     * unknown, not with the 409 as a JetStreamException.
     */
    public function testADuplicateInProcessAnswerWithNoAckAfterItEndsWithTheTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, requestTimeoutMs: 1_500);
        $transport->responder = self::stream($transport, [1 => self::IN_PROCESS]);
        self::dropAfterThePublishOn($transport, 0);

        $start = hrtime(true);
        $error = self::failure($client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID));
        $elapsed = $this->secondsSince($start);

        self::assertInstanceOf(TimeoutException::class, $error);
        self::assertSame('Request timed out for subject ' . self::SUBJECT, $error->getMessage());
        self::assertGreaterThan(1.3, $elapsed);
        self::assertLessThan(2.3, $elapsed);
        self::assertSame([1, 1], self::publishesPerSession($transport, 2));
    }

    /**
     * A 409 10158 answering a publish whose frame went out once still ends it with that JetStreamException, as before:
     * only the answer to a frame written again is waited past.
     */
    public function testADuplicateInProcessAnswerToAFrameWrittenOnceStillEndsThePublish(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, requestTimeoutMs: 3_000);
        $transport->responder = self::stream($transport, [0 => self::IN_PROCESS]);

        $error = self::failure($client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID));

        self::assertInstanceOf(JetStreamException::class, $error);
        self::assertSame(409, $error->getCode());
        self::assertSame(10158, $error->getErrCode());
        self::assertSame('duplicate message id is in process', $error->getMessage());
    }

    /**
     * Any other answer to the second frame ends the publish as it would have ended it the first time: an API error is
     * the publish's JetStreamException.
     */
    public function testAnotherErrorAnsweringTheSecondFrameEndsThePublish(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, requestTimeoutMs: 5_000);
        $transport->responder = self::stream($transport, [1 => '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 6"}}']);
        self::dropAfterThePublishOn($transport, 0);

        $start = hrtime(true);
        $error = self::failure($client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID));

        self::assertInstanceOf(JetStreamException::class, $error);
        self::assertSame(10071, $error->getErrCode());
        self::assertLessThan(2.0, $this->secondsSince($start));
        self::assertSame([1, 1], self::publishesPerSession($transport, 2));
    }

    /**
     * One budget: the reconnect takes 1 s (dials refused), the frame goes out again once it is over, and nothing answers
     * it. The publish still ends at its 1.5 s deadline: the second write draws on the same timeout and never extends it.
     */
    public function testTheSecondWriteDrawsOnThePublishsOwnTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, requestTimeoutMs: 1_500);
        $transport->responder = self::stream($transport, []);
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($transport->epoch() === 0 && self::isPublish($bytes)) {
                $transport->refuseDials();
                $transport->dropConnection();
                EventLoop::delay(1.0, static fn() => $transport->acceptDials());
            }
        };

        $start = hrtime(true);
        $error = self::failure($client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID));
        $elapsed = $this->secondsSince($start);

        self::assertInstanceOf(TimeoutException::class, $error);
        self::assertGreaterThan(1.3, $elapsed);
        self::assertLessThan(2.0, $elapsed, sprintf('the publish ended after %.3f s, with a 1.5 s timeout', $elapsed));
        self::assertSame([1, 1], self::publishesPerSession($transport, 2), 'the frame went out again once the reconnect was over');
    }

    /**
     * The connection drops again right after the second write: the third session gets no third copy, and the publish
     * ends at its 1.5 s deadline.
     */
    public function testASecondReconnectAfterTheSecondWriteWritesNothingMore(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, requestTimeoutMs: 1_500);
        $transport->responder = self::stream($transport, [2 => self::ack(9)]);
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($transport->epoch() <= 1 && self::isPublish($bytes)) {
                $transport->dropConnection();
            }
        };

        $error = self::failure($client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID));

        self::assertInstanceOf(TimeoutException::class, $error);
        self::assertSame(2, $transport->epoch(), 'the connection was replaced twice');
        self::assertSame([1, 1, 0], self::publishesPerSession($transport, 3));
    }

    /**
     * The publish waits behind the application's own read loop, which holds the read slot as the connection drops
     * (dials refused for 0.2 s): the loop's read fails and frees the slot, the publish waits for the reconnect, and its
     * frame goes out again as soon as the new connection is open. Its ack resolves the publish well before its 5 s
     * timeout. Issue #249 asked whether a publish parked there needs a wake-up of its own: it does not.
     */
    public function testAPublishWaitingBehindTheApplicationsReadSendsItsFrameAgainOnceTheReconnectIsOver(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->client($watched, requestTimeoutMs: 5_000);
        $transport->responder = self::stream($transport, [0 => null, 1 => self::ack(5)]);
        // Sets the reply inbox up before the application's read starts, so that the publish writes no SUB of its own.
        $client->jetStream()->publish('orders.warm', 'x')->await();
        $stop = new DeferredCancellation();
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
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);

        $start = hrtime(true);
        $publish = $client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID);
        $this->waitUntil(static fn(): bool => self::publishesPerSession($transport, 1) === [1]);
        // The publish now waits behind the application's read, which is the one on the socket.
        delay(0.05);
        self::assertSame(1, $watched->readsUnderWay);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 0.2);
        $ack = $publish->await();
        $elapsed = $this->secondsSince($start);
        $stop->cancel();
        $loop->await();

        self::assertSame(5, $ack->seq);
        self::assertGreaterThan(0.15, $elapsed, 'the frame went out again only once the reconnect was over');
        self::assertLessThan(2.0, $elapsed, sprintf('the publish took %.3f s, with a 5 s timeout', $elapsed));
        self::assertSame([1, 1], self::publishesPerSession($transport, 2));
    }

    /**
     * A lame-duck failover replaces the connection the same way: the leaving server's INFO answers the publish instead
     * of an ack, the client fails over to the server it names, and the frame goes out again there.
     */
    public function testALameDuckFailoverSendsTheFrameAgainOnTheServerItFailsOverTo(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, requestTimeoutMs: 5_000);
        $lameDuck = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $lameDuck): array {
            if ($subject !== self::SUBJECT || $replyTo === null) {
                return [];
            }

            return $transport->epoch() === 0 ? [$lameDuck] : $transport->replyFrame($replyTo, self::ack(4));
        };

        $start = hrtime(true);
        $ack = $client->jetStream()->publish(self::SUBJECT, self::PAYLOAD, msgId: self::MSG_ID)->await();

        self::assertSame(4, $ack->seq);
        self::assertLessThan(2.0, $this->secondsSince($start));
        self::assertSame([1, 1], self::publishesPerSession($transport, 2));
        self::assertStringStartsWith('tcp://10.0.0.2:4222', $transport->connectCalls[count($transport->connectCalls) - 1]);
    }

    private function client(
        ReconnectingTransport|WatchedTransport $transport,
        bool $resend = true,
        bool $waitForReconnect = true,
        int $requestTimeoutMs = 3_000,
        ?AbstractLogger $logger = null,
    ): NatsClient {
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: true,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            logger: $logger,
            waitForReconnect: $waitForReconnect,
            resendJetStreamPublishOnReconnect: $resend,
        ), $transport);
        $this->opened[] = $this->own($client);
        $client->connect()->await();

        return $client;
    }

    /**
     * A stream that answers a publish to {@see SUBJECT} on session N with $answers[N] (no answer when absent or null),
     * and any other publish with an ack.
     *
     * @param array<int, ?string> $answers
     * @return \Closure(string, ?string, string): list<string>
     */
    private static function stream(ReconnectingTransport $transport, array $answers): \Closure
    {
        return static function (string $subject, ?string $replyTo) use ($transport, $answers): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject !== self::SUBJECT) {
                return $transport->replyFrame($replyTo, self::ack(1));
            }

            $answer = $answers[$transport->epoch()] ?? null;

            return $answer === null ? [] : $transport->replyFrame($replyTo, $answer);
        };
    }

    /** Drops the connection right after the publish's frame was written on session $epoch, before any answer. */
    private static function dropAfterThePublishOn(ReconnectingTransport $transport, int $epoch): void
    {
        $transport->afterWrite = static function (string $bytes) use ($transport, $epoch): void {
            if ($transport->epoch() === $epoch && self::isPublish($bytes)) {
                $transport->dropConnection();
            }
        };
    }

    private static function isPublish(string $bytes): bool
    {
        return str_starts_with($bytes, 'HPUB ' . self::SUBJECT . ' ') || str_starts_with($bytes, 'PUB ' . self::SUBJECT . ' ');
    }

    /**
     * How many frames of the publish each of the first $sessions sessions got.
     *
     * @return list<int>
     */
    private static function publishesPerSession(ReconnectingTransport $transport, int $sessions): array
    {
        $counts = array_fill(0, $sessions, 0);
        foreach ($transport->writes as $write) {
            if ($write['epoch'] < $sessions && self::isPublish($write['bytes'])) {
                $counts[$write['epoch']]++;
            }
        }

        return array_values($counts);
    }

    /** @return list<string> The publish's frames, in the order they went out. */
    private static function publishFrames(ReconnectingTransport $transport): array
    {
        $frames = [];
        foreach ($transport->writes as $write) {
            if (self::isPublish($write['bytes'])) {
                $frames[] = $write['bytes'];
            }
        }

        return $frames;
    }

    private static function ack(int $seq, bool $duplicate = false): string
    {
        return (string) json_encode(['stream' => 'ORDERS', 'seq' => $seq, 'duplicate' => $duplicate]);
    }

    /** @param Future<PubAck> $publish */
    private static function failure(Future $publish): ?\Throwable
    {
        try {
            $publish->await();
        } catch (\Throwable $error) {
            return $error;
        }

        return null;
    }
}
