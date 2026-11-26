<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\ApiErrCode;
use IDCT\NATS\JetStream\FeatureSupport;
use IDCT\NATS\JetStream\JetStreamApi;
use IDCT\NATS\JetStream\Models\ConsumerInfo;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\OrderedConsumerResetServer;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The consumer delivery state reset of ADR-60 (#274): `$JS.API.CONSUMER.RESET.<stream>.<consumer>`, NATS server 2.14+.
 *
 * {@see \IDCT\NATS\JetStream\JetStreamContext::resetConsumer()} sends an empty request (reset to the ack floor) or
 * `{"seq":N}` (deliver from stream sequence N on), and resolves to the consumer info of the reply with its `reset_seq`.
 * ADR-60 also asks clients not to fail when a consumer is reset by another process (the CLI, an operator), which
 * restarts its consumer sequence at 1: a pull consume and a push subscription carry on, and an ordered consumer
 * recreates itself from its last in-order message, so it delivers neither a gap nor a duplicate.
 *
 * The replies are those of nats-server 2.14.10 (taken from a live run), trimmed to the fields that matter here.
 */
final class ConsumerResetTest extends TestCase
{
    use OwnsTestResources;

    private const RESET_SUBJECT = '$JS.API.CONSUMER.RESET.ORDERS.PROC';

    protected function setUp(): void
    {
        $this->resources();
    }

    protected function tearDown(): void
    {
        $this->releaseOwnedResourcesAndTheirWatchdogs();
    }

    /**
     * Without a sequence the request is empty, as ADR-60 has it for a reset to the ack floor; `{}` is what every
     * other API call of the client sends for an empty request, and nats-server 2.14 reads it the same way.
     */
    public function testAResetWithoutASequenceSendsAnEmptyRequest(): void
    {
        [$client, $transport] = $this->connectedTo('2.14.10', [self::resetReply(4)]);

        $client->jetStream()->resetConsumer('ORDERS', 'PROC')->await();

        self::assertSame('{}', self::requestBody($transport, self::RESET_SUBJECT));
        self::assertSame('$JS.API.CONSUMER.RESET.', JetStreamApi::CONSUMER_RESET_PREFIX);
    }

    /**
     * A sequence goes out as `{"seq":N}`. 0 is sent as given: the server reads it as no sequence (back to the ack
     * floor), as ADR-60 says.
     */
    public function testAResetToASequenceSendsTheSequence(): void
    {
        foreach ([42 => '{"seq":42}', 1 => '{"seq":1}', 0 => '{"seq":0}'] as $sequence => $body) {
            [$client, $transport] = $this->connectedTo('2.14.10', [self::resetReply(max(1, $sequence))]);

            $client->jetStream()->resetConsumer('ORDERS', 'PROC', $sequence)->await();

            self::assertSame($body, self::requestBody($transport, self::RESET_SUBJECT), (string) $sequence);
        }
    }

    /**
     * The reply is a consumer info plus `reset_seq`: the call resolves to the ConsumerInfo with its config and its
     * delivery state, and the sequence the next delivery starts from.
     */
    public function testTheReplyIsTheConsumerWithItsResetSequence(): void
    {
        [$client] = $this->connectedTo('2.14.10', [self::resetReply(2)]);

        $info = $client->jetStream()->resetConsumer('ORDERS', 'PROC', 2)->await();

        self::assertSame('ORDERS', $info->streamName);
        self::assertSame('PROC', $info->name);
        self::assertFalse($info->push);
        self::assertSame(2, $info->resetSequence);
        self::assertSame(['consumer_seq' => 0, 'stream_seq' => 1], $info->raw['delivered'] ?? null);
        self::assertSame(['consumer_seq' => 0, 'stream_seq' => 1], $info->raw['ack_floor'] ?? null);
        self::assertSame(5, $info->raw['num_pending'] ?? null);
        self::assertIsArray($info->raw['config'] ?? null);
        self::assertSame('all', $info->raw['config']['deliver_policy'] ?? null);
    }

    /**
     * A push consumer's reply says so, as a create's does.
     */
    public function testTheReplyOfAPushConsumerIsAPushConsumer(): void
    {
        $reply = json_encode([
            'type' => 'io.nats.jetstream.api.v1.consumer_reset_response',
            'stream_name' => 'ORDERS',
            'name' => 'PROC',
            'config' => ['durable_name' => 'PROC', 'deliver_subject' => 'deliver.proc', 'deliver_policy' => 'all'],
            'reset_seq' => 7,
        ], JSON_THROW_ON_ERROR);
        [$client] = $this->connectedTo('2.14.10', [$reply]);

        $info = $client->jetStream()->resetConsumer('ORDERS', 'PROC')->await();

        self::assertTrue($info->push);
        self::assertSame(7, $info->resetSequence);
    }

    /**
     * Only an integer `reset_seq` is read; any other reply (a create's, an info's) has none.
     */
    public function testOnlyAnIntegerResetSequenceIsRead(): void
    {
        self::assertNull(ConsumerInfo::fromArray(['stream_name' => 'ORDERS', 'name' => 'PROC', 'config' => []])->resetSequence);
        self::assertNull(ConsumerInfo::fromArray(['name' => 'PROC', 'reset_seq' => '5'])->resetSequence);
        self::assertNull(ConsumerInfo::fromArray(['name' => 'PROC', 'reset_seq' => 5.0])->resetSequence);
        self::assertNull(ConsumerInfo::fromArray(['name' => 'PROC', 'reset_seq' => null])->resetSequence);
        self::assertSame(0, ConsumerInfo::fromArray(['name' => 'PROC', 'reset_seq' => 0])->resetSequence);
        self::assertNull((new ConsumerInfo('ORDERS', 'PROC', false, []))->resetSequence);
    }

    /**
     * A sequence the consumer's config does not allow (here a consumer delivering only new messages) is refused with
     * 400 and err_code 10204; the call fails with a JetStreamException carrying both and the server's description.
     */
    public function testAnInvalidResetFailsWithItsErrCode(): void
    {
        $reply = '{"type":"io.nats.jetstream.api.v1.consumer_reset_response","error":{"code":400,"err_code":10204,"description":"invalid reset: not allowed"},"reset_seq":0}';
        [$client] = $this->connectedTo('2.14.10', [$reply]);

        try {
            $client->jetStream()->resetConsumer('ORDERS', 'PROC', 2)->await();
            self::fail('expected the invalid reset to fail');
        } catch (JetStreamException $e) {
            self::assertNotInstanceOf(UnsupportedFeatureException::class, $e);
            self::assertSame('invalid reset: not allowed', $e->getMessage());
            self::assertSame(400, $e->getCode());
            self::assertSame(ApiErrCode::CONSUMER_INVALID_RESET, $e->getErrCode());
            self::assertSame(10204, ApiErrCode::CONSUMER_INVALID_RESET);
        }
    }

    /**
     * An answer that is neither an error nor a consumer leaves the outcome open (nats.go ErrConsumerResetResponseEmpty):
     * the call fails saying so instead of resolving to an empty ConsumerInfo.
     */
    public function testAReplyWithoutTheConsumerFails(): void
    {
        $replies = [
            '{"type":"io.nats.jetstream.api.v1.consumer_reset_response","reset_seq":4}',
            '{"type":"io.nats.jetstream.api.v1.consumer_reset_response","config":null,"reset_seq":4}',
            '{"type":"io.nats.jetstream.api.v1.consumer_reset_response","config":"all","reset_seq":4}',
        ];
        foreach ($replies as $reply) {
            [$client] = $this->connectedTo('2.14.10', [$reply]);

            try {
                $client->jetStream()->resetConsumer('ORDERS', 'PROC')->await();
                self::fail('expected a reply without the consumer to fail: ' . $reply);
            } catch (JetStreamException $e) {
                self::assertSame(
                    'The reset reply for consumer "PROC" on stream "ORDERS" carries no consumer info: the reset may or may not have taken effect',
                    $e->getMessage(),
                );
            }
        }
    }

    /**
     * @return iterable<string, array{string, string, non-empty-string}>
     */
    public static function invalidNames(): iterable
    {
        yield 'a dotted stream' => ['bad.stream', 'PROC', 'Invalid stream name "bad.stream"'];
        yield 'an empty stream' => ['', 'PROC', 'Invalid stream name ""'];
        yield 'a dotted consumer' => ['ORDERS', 'a.b', 'Invalid consumer name "a.b"'];
        yield 'an empty consumer' => ['ORDERS', '', 'Invalid consumer name ""'];
        yield 'a consumer with a space' => ['ORDERS', 'a b', 'Invalid consumer name "a b"'];
        yield 'a wildcard consumer' => ['ORDERS', '>', 'Invalid consumer name ">"'];
    }

    /**
     * An invalid stream or consumer name would change the API subject: it is refused before anything is written
     * after the CONNECT + PING handshake, as the other consumer calls refuse it.
     *
     * @param non-empty-string $message
     */
    #[DataProvider('invalidNames')]
    public function testInvalidNamesAreRefusedWithNothingSent(string $stream, string $consumer, string $message): void
    {
        [$client, $transport] = $this->connectedTo('2.14.10');

        try {
            $client->jetStream()->resetConsumer($stream, $consumer, 2)->await();
            self::fail('expected the name to be refused');
        } catch (JetStreamException $e) {
            self::assertStringStartsWith($message, $e->getMessage());
        }

        self::assertCount(2, $transport->writes);
    }

    /**
     * A negative sequence is refused before anything is sent (a 2.14 server would answer it with an "invalid JSON"
     * error: its sequence is unsigned).
     */
    public function testANegativeSequenceIsRefusedWithNothingSent(): void
    {
        foreach ([-1, PHP_INT_MIN] as $sequence) {
            [$client, $transport] = $this->connectedTo('2.14.10');

            try {
                $client->jetStream()->resetConsumer('ORDERS', 'PROC', $sequence)->await();
                self::fail('expected a negative sequence to be refused: ' . $sequence);
            } catch (\InvalidArgumentException $e) {
                self::assertSame('The sequence of resetConsumer() must not be negative (null or 0 resets the consumer to its ack floor)', $e->getMessage());
            }

            self::assertCount(2, $transport->writes, (string) $sequence);
        }
    }

    /**
     * The reset is sent whatever version the connected server reports in INFO (ADR-44, #272): the consumer's own
     * server answers it, and in a cluster under a rolling upgrade that may be a 2.14 server behind a connected 2.12
     * one. With the reset answered as 2.14.10 answers it, every version gets the request once and the reply's
     * consumer: versions before 2.14 (which 2.33.0 refused with "nothing was sent"), 2.14+ ones (pre-release tags
     * included) and versions that do not parse. `consumer_reset` stays in the registry as documentation.
     */
    public function testTheResetIsSentWhateverVersionInfoReports(): void
    {
        self::assertSame('2.14', FeatureSupport::requiredVersion('consumer_reset'));

        foreach (['2.12.15', '2.12.0', 'v2.11.4', '2.13.9', '1.99.0', '2', '2.14.0', '2.14.0-beta.1', 'v2.14.10', '2.15.2', '3.0.0', 'custom-build', null] as $version) {
            [$client, $transport] = $this->connectedTo($version, [self::resetReply(2)]);

            $info = $client->jetStream()->resetConsumer('ORDERS', 'PROC', 2)->await();

            self::assertSame(2, $info->resetSequence, (string) $version);
            // requestBody() also asserts the reset went out exactly once.
            self::assertSame('{"seq":2}', self::requestBody($transport, self::RESET_SUBJECT), (string) $version);
        }
    }

    /**
     * A server before 2.14 does not answer the reset at all: with INFO reporting such a version and nothing
     * answering, the request is still written once and the call fails with the request timeout, not with an
     * UnsupportedFeatureException decided from the version.
     */
    public function testAServerBefore214LetsTheResetTimeOut(): void
    {
        foreach (['2.12.15', 'v2.11.4', '2.10.29'] as $version) {
            [$client, $transport] = $this->connectedTo($version, [], 200);

            try {
                $client->jetStream()->resetConsumer('ORDERS', 'PROC', 2)->await();
                self::fail('expected the reset to time out on ' . $version);
            } catch (TimeoutException $e) {
                self::assertSame('Request timed out for subject ' . self::RESET_SUBJECT, $e->getMessage(), $version);
            }

            self::assertSame('{"seq":2}', self::requestBody($transport, self::RESET_SUBJECT), $version);
        }
    }

    /**
     * Nothing answers when the consumer or the stream does not exist (the consumer itself answers the reset), or when
     * the server runs a release before 2.14: the call fails with the request timeout, as its docblock says.
     */
    public function testARequestNothingAnswersTimesOut(): void
    {
        [$client] = $this->connectedTo('2.14.10', [], 200);

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Request timed out for subject ' . self::RESET_SUBJECT);

        $client->jetStream()->resetConsumer('ORDERS', 'PROC')->await();
    }

    /**
     * A server without JetStream answers with no responders: the call fails with JetStreamException(503), as every
     * JetStream API call does (#161).
     */
    public function testNoRespondersIsAJetStreamException503(): void
    {
        $transport = new FakeTransport([self::info('2.14.10'), "PONG\r\n"]);
        $this->muxReplies($transport, ["HMSG _INBOX.a 1 16 16\r\nNATS/1.0 503\r\n\r\n\r\n"]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport));
        $client->connect()->await();

        try {
            $client->jetStream()->resetConsumer('ORDERS', 'PROC')->await();
            self::fail('expected the call to fail');
        } catch (NatsException $failure) {
        }

        self::assertInstanceOf(JetStreamException::class, $failure, $failure::class . ': ' . $failure->getMessage());
        self::assertSame(503, $failure->getCode());
        self::assertSame('No JetStream responder for subject ' . self::RESET_SUBJECT, substr($failure->getMessage(), 0, strlen('No JetStream responder for subject ' . self::RESET_SUBJECT)));
    }

    /**
     * A running pull consume whose consumer is reset by another client (to stream sequence 2, after m-1 to m-3 were
     * delivered) gets the replay with consumer sequences starting again at 1 and goes on: nothing in the pull path
     * expects the consumer sequence to grow, and nothing is reported.
     */
    public function testARunningPullConsumeKeepsDeliveringAfterTheConsumerIsReset(): void
    {
        $transport = new FakeTransport([self::info('2.12.0'), "PONG\r\n"]);
        $this->pullServer($transport, [
            // Stream sequence => consumer sequence, before the reset ...
            [[1, 1, 'm-1'], [2, 2, 'm-2'], [3, 3, 'm-3']],
            // ... and after it: the consumer delivers from stream sequence 2 again, its sequence back at 1.
            [[2, 1, 'm-2'], [3, 2, 'm-3'], [4, 3, 'm-4']],
        ]);
        $errors = [];
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e->getMessage();
        }), $transport));
        $client->connect()->await();

        $handled = [];
        $total = $client->jetStream()->pullConsumer('ORDERS', 'PROC')
            ->setBatching(3)
            ->setExpiresMs(500)
            ->setIterations(2)
            ->handle(static function (NatsMessage $message) use (&$handled): void {
                $handled[] = $message->payload;
            })->await();

        self::assertSame(6, $total);
        self::assertSame(['m-1', 'm-2', 'm-3', 'm-2', 'm-3', 'm-4'], $handled);
        self::assertSame([], $errors);
    }

    /**
     * A durable push subscription whose consumer is reset by another client (to stream sequence 2, after m-1 to m-3
     * were delivered) hands the replay and what follows to the handler. The delivery whose consumer sequence went back
     * to 1 rebases the gap check silently, so the next heartbeat (last delivered 3, as many as the client got since)
     * reports nothing.
     */
    public function testADurablePushConsumeKeepsDeliveringAfterTheConsumerIsReset(): void
    {
        $errors = [];
        [$client, $transport] = $this->pushConsumerClient($errors);
        $transport->enqueueOnWriteContaining = [
            'SUB deliver.proc' => [
                self::pushDelivery(1, 1, 'm-1'),
                self::pushDelivery(2, 2, 'm-2'),
                self::pushDelivery(3, 3, 'm-3'),
                // The reset, made by another client: stream sequence 2 again, consumer sequence 1.
                self::pushDelivery(2, 1, 'm-2'),
                self::pushDelivery(3, 2, 'm-3'),
                self::pushDelivery(4, 3, 'm-4'),
                OrderedConsumerResetServer::heartbeat('deliver.proc', 2, 3, 4),
            ],
        ];

        $handled = [];
        $client->jetStream()->subscribePushConsumer(
            'ORDERS',
            'PROC',
            static function (NatsMessage $message) use (&$handled): void {
                $handled[] = $message->payload;
            },
            'deliver.proc',
            consumerOptions: ['idle_heartbeat' => 5_000_000_000],
        )->await();

        for ($i = 0; $i < 10 && count($handled) < 6; $i++) {
            $client->processIncoming()->await();
        }
        $client->processIncoming()->await();

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-2', 'm-3', 'm-4'], $handled);
        self::assertSame([], self::mismatches($errors));
    }

    /**
     * A durable push subscription whose consumer is reset while idle learns of it from the heartbeat (last delivered
     * 0, below the 3 it got): the mismatch is reported once, naming a reset, the gap check rebases, and the delivery
     * after it reaches the handler with no further report.
     */
    public function testAnIdleDurablePushConsumeReportsTheResetOnceAndKeepsDelivering(): void
    {
        $errors = [];
        [$client, $transport] = $this->pushConsumerClient($errors);
        $transport->enqueueOnWriteContaining = [
            'SUB deliver.proc' => [
                self::pushDelivery(1, 1, 'm-1'),
                self::pushDelivery(2, 2, 'm-2'),
                self::pushDelivery(3, 3, 'm-3'),
                // Reset while idle, by another client: the heartbeats report the consumer sequence back at 0.
                OrderedConsumerResetServer::heartbeat('deliver.proc', 2, 0, 3),
                OrderedConsumerResetServer::heartbeat('deliver.proc', 2, 0, 3),
                self::pushDelivery(4, 1, 'm-4'),
                OrderedConsumerResetServer::heartbeat('deliver.proc', 2, 1, 4),
            ],
        ];

        $handled = [];
        $client->jetStream()->subscribePushConsumer(
            'ORDERS',
            'PROC',
            static function (NatsMessage $message) use (&$handled): void {
                $handled[] = $message->payload;
            },
            'deliver.proc',
            consumerOptions: ['idle_heartbeat' => 5_000_000_000],
        )->await();

        for ($i = 0; $i < 10 && count($handled) < 4; $i++) {
            $client->processIncoming()->await();
        }
        $client->processIncoming()->await();

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-4'], $handled);
        $mismatches = self::mismatches($errors);
        self::assertCount(1, $mismatches, 'one report of the reset, none for the repeated heartbeat or after the delivery');
        self::assertStringContainsString('last delivered sequence 0 below the 3 already seen', $mismatches[0]);
        self::assertStringContainsString('appears to have been replaced or reset (sequence regressed)', $mismatches[0]);
    }

    /**
     * An ordered consumer whose consumer is reset backward by another client (to stream sequence 2, after m-1 to m-3)
     * gets m-2 again with consumer sequence 1: it discards it, deletes the consumer and recreates it from stream
     * sequence 4, so the handler gets m-1 to m-4 once each, in order: no duplicate.
     */
    public function testAnOrderedConsumerResetBackwardRecreatesItselfWithoutADuplicate(): void
    {
        $server = new OrderedConsumerResetServer(
            [1 => 'm-1', 2 => 'm-2', 3 => 'm-3', 4 => 'm-4'],
            3,
            static fn(string $deliver, int $sid, string $name): array => [OrderedConsumerResetServer::delivery($deliver, $sid, 'EVENTS', $name, 2, 1, 'm-2')],
        );

        $handled = $this->runOrderedConsumer($server, 4);

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-4'], $handled);
        self::assertCount(2, $server->creates, 'the initial consumer and one recreate');
        self::assertSame('by_start_sequence', $server->creates[1]['deliver_policy'] ?? null);
        self::assertSame(4, $server->creates[1]['opt_start_seq'] ?? null);
        self::assertSame(['ORD1'], $server->deleted, 'the reset consumer (named by the server, the first create named none) is deleted');
    }

    /**
     * An ordered consumer whose consumer is reset forward past the stream's tail while idle (to stream sequence 100,
     * with 3 messages delivered) would get nothing until the stream reached sequence 100. The heartbeat that reports
     * the consumer sequence back at 0 makes it recreate itself from stream sequence 4 at once, so m-4 and m-5 reach the
     * handler: no gap.
     */
    public function testAnOrderedConsumerResetForwardWhileIdleRecreatesItselfWithoutAGap(): void
    {
        $server = new OrderedConsumerResetServer(
            [1 => 'm-1', 2 => 'm-2', 3 => 'm-3', 4 => 'm-4', 5 => 'm-5'],
            3,
            static fn(string $deliver, int $sid): array => [OrderedConsumerResetServer::heartbeat($deliver, $sid, 0, 99)],
        );

        $handled = $this->runOrderedConsumer($server, 5);

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-4', 'm-5'], $handled);
        self::assertCount(2, $server->creates, 'the initial consumer and one recreate');
        self::assertSame(4, $server->creates[1]['opt_start_seq'] ?? null);
    }

    /**
     * An ordered consumer's heartbeat that reports what it got (last delivered 3 after three deliveries) recreates
     * nothing: only a report above or below it does.
     */
    public function testAnOrderedConsumerHeartbeatInStepWithItsDeliveriesRecreatesNothing(): void
    {
        $server = new OrderedConsumerResetServer(
            [1 => 'm-1', 2 => 'm-2', 3 => 'm-3'],
            3,
            static fn(string $deliver, int $sid): array => [OrderedConsumerResetServer::heartbeat($deliver, $sid, 3, 3)],
        );

        $handled = $this->runOrderedConsumer($server, 3);

        self::assertSame(['m-1', 'm-2', 'm-3'], $handled);
        self::assertCount(1, $server->creates);
        self::assertSame([], $server->deleted);
    }

    // ─── Helpers ──────────────────────────────────────────────────────────

    /**
     * A client connected to a scripted server advertising $version (none when null), answering each request with the
     * next of $replies on its mux reply subject.
     *
     * @param list<string> $replies JSON reply bodies.
     * @return array{NatsClient, FakeTransport}
     */
    private function connectedTo(?string $version, array $replies = [], int $requestTimeoutMs = 1000): array
    {
        $transport = new FakeTransport([self::info($version), "PONG\r\n"]);
        $this->muxReplies($transport, array_map(
            static fn(string $reply): string => sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
            $replies,
        ));

        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: $requestTimeoutMs), $transport));
        $client->connect()->await();

        return [$client, $transport];
    }

    /**
     * A client whose durable push consumer "PROC" on "ORDERS" is created by the scripted server (deliveries are
     * enqueued by the test on the deliver subject's SUB, sid 2), collecting what the error listener gets.
     *
     * @param list<string> $errors
     * @return array{NatsClient, FakeTransport}
     */
    private function pushConsumerClient(array &$errors): array
    {
        $createReply = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc","ack_policy":"explicit","idle_heartbeat":5000000000}}';
        $transport = new FakeTransport([self::info('2.12.0'), "PONG\r\n"]);
        $this->muxReplies($transport, [sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply)]);

        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e->getMessage();
        }), $transport));
        $client->connect()->await();

        return [$client, $transport];
    }

    /**
     * Runs an ordered consumer on "EVENTS" against $server until the handler got $expected messages (or the pump gives
     * up), and returns what it got.
     *
     * @return list<string>
     */
    private function runOrderedConsumer(OrderedConsumerResetServer $server, int $expected): array
    {
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $server->transport));
        $client->connect()->await();

        $handled = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$handled): void {
            $handled[] = $message->payload;
        })->await();

        for ($i = 0; $i < 20 && count($handled) < $expected; $i++) {
            $client->processIncoming()->await();
        }
        $client->processIncoming()->await();

        return $handled;
    }

    /**
     * A pull server for consumer "PROC" on "ORDERS": each pull is answered with the next list of deliveries, each a
     * [stream sequence, consumer sequence, payload], on the pull inbox's sid; data arrives on the message's own subject
     * with a $JS.ACK reply subject, as nats-server delivers it.
     *
     * @param list<list<array{int, int, string}>> $perPull
     */
    private function pullServer(FakeTransport $transport, array $perPull): void
    {
        $pullSid = 1;
        $index = 0;
        $transport->onWrite = static function (string $bytes) use ($perPull, &$pullSid, &$index): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB _INBOX.JS.PULL.') && str_contains($head, '.* ')) {
                $pullSid = (int) (explode(' ', $head)[2] ?? 1);

                return array_fill(0, substr_count($bytes, "PING\r\n"), "PONG\r\n");
            }

            if (str_starts_with($head, 'UNSUB ')) {
                return array_fill(0, substr_count($bytes, "PING\r\n"), "PONG\r\n");
            }

            if (!str_starts_with($head, 'PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.PROC ') || !isset($perPull[$index])) {
                return [];
            }

            $frames = [];
            foreach ($perPull[$index] as [$streamSeq, $consumerSeq, $payload]) {
                $frames[] = OrderedConsumerResetServer::delivery('orders.created', $pullSid, 'ORDERS', 'PROC', $streamSeq, $consumerSeq, $payload);
            }
            ++$index;

            return $frames;
        };
    }

    /**
     * Answers each request with the next frame on the request's own mux reply subject, as a server would (the
     * request inbox is muxed since #118: one wildcard "<base>.*" subscription, sid 1).
     *
     * @param list<string> $frames Pre-built MSG/HMSG reply frames; only their subject and sid are rewritten.
     */
    private function muxReplies(FakeTransport $transport, array $frames): void
    {
        $queue = $frames;
        $base = null;

        $transport->onWrite = static function (string $bytes) use (&$queue, &$base): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1);
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                return [];
            }

            $frame = array_shift($queue);
            $pos = strpos($frame, "\r\n");
            self::assertNotFalse($pos, 'reply frame must have a head line');
            $tokens = explode(' ', substr($frame, 0, $pos));
            $tokens[1] = $replyTo;
            $tokens[2] = '1';

            return [implode(' ', $tokens) . substr($frame, $pos)];
        };
    }

    /** The body of the one PUB written to $subject. */
    private static function requestBody(FakeTransport $transport, string $subject): string
    {
        $bodies = [];
        foreach ($transport->writes as $write) {
            if (preg_match('/^PUB ' . preg_quote($subject, '/') . ' \S+ (\d+)\r\n/', $write, $match) === 1) {
                $bodies[] = substr($write, strlen($match[0]), (int) $match[1]);
            }
        }

        self::assertCount(1, $bodies, 'one request to ' . $subject);

        return $bodies[0];
    }

    /** A reset reply of nats-server 2.14.10 for consumer "PROC" on "ORDERS", reset to $resetSequence. */
    private static function resetReply(int $resetSequence): string
    {
        return json_encode([
            'type' => 'io.nats.jetstream.api.v1.consumer_reset_response',
            'stream_name' => 'ORDERS',
            'name' => 'PROC',
            'created' => '2026-11-26T07:31:58.768481147Z',
            'config' => ['durable_name' => 'PROC', 'name' => 'PROC', 'deliver_policy' => 'all', 'ack_policy' => 'explicit', 'max_deliver' => -1],
            'delivered' => ['consumer_seq' => 0, 'stream_seq' => $resetSequence - 1],
            'ack_floor' => ['consumer_seq' => 0, 'stream_seq' => $resetSequence - 1],
            'num_ack_pending' => 0,
            'num_redelivered' => 0,
            'num_waiting' => 0,
            'num_pending' => 5,
            'ts' => '2026-11-26T07:31:58.976800897Z',
            'reset_seq' => $resetSequence,
        ], JSON_THROW_ON_ERROR);
    }

    /** The server's INFO line, advertising $version (none when null). */
    private static function info(?string $version): string
    {
        $info = ['server_id' => 'S1', 'server_name' => 'n1', 'jetstream' => true, 'max_payload' => 1048576, 'headers' => true];
        if ($version !== null) {
            $info['version'] = $version;
        }

        return 'INFO ' . json_encode($info, JSON_THROW_ON_ERROR) . "\r\n";
    }

    /** A delivery of consumer "PROC" on "ORDERS" to the push consumer's deliver subject (sid 2). */
    private static function pushDelivery(int $streamSeq, int $consumerSeq, string $payload): string
    {
        return OrderedConsumerResetServer::delivery('deliver.proc', 2, 'ORDERS', 'PROC', $streamSeq, $consumerSeq, $payload);
    }

    /**
     * @param list<string> $errors
     * @return list<string>
     */
    private static function mismatches(array $errors): array
    {
        return array_values(array_filter($errors, static fn(string $error): bool => str_contains($error, 'consumer sequence mismatch')));
    }
}
