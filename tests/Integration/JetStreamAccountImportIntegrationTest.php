<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * JetStream through an account import (#284), over the `nats-accounts` fixture of
 * docker-compose.yml: ORIGIN runs JetStream and exports it, TARGET has none of its own and imports
 * it (`build/nats/accounts.conf`, the README's "Account imports" block). A client on TARGET reaches
 * ORIGIN's JetStream under a prefix with `jetStream(apiPrefix: 'JS.from-a.API')`; its default context
 * finds no responders there (503), as a leaf node without JetStream does for a domain.
 *
 * Every test plays ORIGIN against TARGET the way the triage measured it: a client of ORIGIN creates
 * the data, reads the state afterwards and cleans up (also when the test fails), while a client of
 * TARGET performs the call under test through the prefix. Every stream and bucket name carries a
 * random token because this class runs three times per CI job against the same server (`composer
 * test:e2e` against `nats` and `nats-214`, then the coverage step) and the fixture's config exports
 * exactly one plain publish subject (`orders.>`), which two streams cannot share.
 *
 * The tests skip when the fixture is not running (a stack from before #284).
 */
final class JetStreamAccountImportIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    private const ORIGIN_API_PREFIX = 'JS.from-a.API';
    private const ORIGIN_USER = 'origin-user';
    private const ORIGIN_PASSWORD = 'origin-pass';
    private const TARGET_USER = 'target-user';
    private const TARGET_PASSWORD = 'target-pass';

    /** @var list<NatsClient> */
    private array $clients = [];

    protected function tearDown(): void
    {
        foreach ($this->clients as $client) {
            try {
                $client->disconnect()->await();
            } catch (\Throwable) {
            }
        }
        $this->clients = [];
    }

    /**
     * On TARGET the default context finds no JetStream (503, no account of its own), and the prefix
     * context reaches ORIGIN's: account info comes back without an error.
     */
    public function testTargetsDefaultContextHasNoJetStreamButThePrefixReachesOrigin(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();

        try {
            $target->jetStream()->accountInfo()->await();
            self::fail('TARGET has no JetStream of its own');
        } catch (JetStreamException $noJetStream) {
            self::assertSame(503, $noJetStream->getCode());
        }

        // The prefix context must reach the SAME account ORIGIN sees directly: both calls read the
        // same resource counts.
        $throughPrefix = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX)->accountInfo()->await();
        $direct = $originJs->accountInfo()->await();
        self::assertSame($direct->streams, $throughPrefix->streams);
        self::assertSame($direct->consumers, $throughPrefix->consumers);
    }

    /**
     * `fetchBatch()` of 5 through the prefix needs `response: stream` on the `$JS.API.>` export:
     * without it, only the first of the five replies arrives.
     */
    public function testFetchBatchOfFiveMessagesThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        [$stream, $subject] = $this->acctStreamAndSubject('FETCH');
        $originJs->createStream($stream, [$subject . '.>'])->await();

        try {
            $originJs->createConsumer($stream, 'reader', options: ['ack_policy' => 'explicit'])->await();

            for ($i = 1; $i <= 5; $i++) {
                $originJs->publish($subject . '.' . $i, (string) $i)->await();
            }

            $messages = $targetJs->fetchBatch($stream, 'reader', 5, 3000)->await();
            self::assertCount(5, $messages, 'fetchBatch() of 5 needs response: stream on the $JS.API.> export');

            foreach ($messages as $message) {
                $targetJs->ack($message)->await();
            }
        } finally {
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * `pullConsumer()->handle()` of 3 through the prefix, same dependency on `response: stream` as
     * `fetchBatch()` above 1.
     */
    public function testPullConsumerHandleThreeMessagesThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        [$stream, $subject] = $this->acctStreamAndSubject('PULL');
        $originJs->createStream($stream, [$subject . '.>'])->await();

        try {
            $originJs->createConsumer($stream, 'reader', options: ['ack_policy' => 'explicit'])->await();

            for ($i = 1; $i <= 3; $i++) {
                $originJs->publish($subject . '.' . $i, (string) $i)->await();
            }

            $handled = [];
            $consumed = $targetJs->pullConsumer($stream, 'reader')
                ->setBatching(3)->setDepth(1)->setExpiresMs(3000)->setIterations(1)
                ->handle(static function (NatsMessage $message, JetStreamContext $context) use (&$handled): void {
                    $handled[] = $message->payload;
                    $context->ackSync($message)->await();
                })->await();

            self::assertSame(3, $consumed);
            self::assertSame(['1', '2', '3'], $handled);
        } finally {
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * `ackSync()` through the prefix confirms with ORIGIN: it needs the `$JS.ACK.>` import, without
     * which it fails at once with "No responders".
     */
    public function testAckSyncConfirmedThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        [$stream, $subject] = $this->acctStreamAndSubject('ACKSYNC');
        $originJs->createStream($stream, [$subject . '.>'])->await();

        try {
            $originJs->createConsumer($stream, 'reader', options: ['ack_policy' => 'explicit'])->await();
            $originJs->publish($subject . '.1', 'payload')->await();

            $messages = $targetJs->fetchBatch($stream, 'reader', 1, 3000)->await();
            self::assertCount(1, $messages);

            $targetJs->ackSync($messages[0])->await();

            $consumer = $originJs->getConsumer($stream, 'reader')->await();
            self::assertSame(0, (int) ($consumer->raw['num_ack_pending'] ?? -1));
        } finally {
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * After `ack()` through the prefix (fire-and-forget), ORIGIN's consumer reaches
     * `num_ack_pending` 0 - polled for a short deadline, because the ack reaches ORIGIN
     * asynchronously through the `$JS.ACK.>` import.
     */
    public function testAckReachesOriginsConsumerThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        [$stream, $subject] = $this->acctStreamAndSubject('ACK');
        $originJs->createStream($stream, [$subject . '.>'])->await();

        try {
            $originJs->createConsumer($stream, 'reader', options: ['ack_policy' => 'explicit'])->await();
            $originJs->publish($subject . '.1', 'payload')->await();

            $messages = $targetJs->fetchBatch($stream, 'reader', 1, 3000)->await();
            self::assertCount(1, $messages);

            $targetJs->ack($messages[0])->await();

            $deadline = $this->monotonic() + 3.0;
            $pending = -1;
            while ($this->monotonic() < $deadline) {
                $consumer = $originJs->getConsumer($stream, 'reader')->await();
                $pending = (int) ($consumer->raw['num_ack_pending'] ?? -1);
                if ($pending === 0) {
                    break;
                }

                delay(0.1);
            }

            self::assertSame(0, $pending, 'ORIGIN must read num_ack_pending 0 after ack() through the $JS.ACK.> import');
        } finally {
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * `keys()` through the prefix, of a bucket that has keys, needs the `_INBOX.KV.KEYS.>` export.
     */
    public function testKeysOfABucketThatHasKeysThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        $bucket = $this->acctName('keys');
        $originKv = $originJs->keyValue($bucket);
        $originKv->create()->await();

        try {
            $originKv->put('alpha', '1')->await();
            $originKv->put('beta', '2')->await();

            $keys = $targetJs->keyValue($bucket)->keys()->await();
            sort($keys);
            self::assertSame(['alpha', 'beta'], $keys);
        } finally {
            $originKv->deleteBucket()->await();
        }
    }

    /**
     * `history()` through the prefix, of a key written twice, needs the `_INBOX.KV.HIST.>` export.
     */
    public function testHistoryOfAKeyWrittenTwiceThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        $bucket = $this->acctName('hist');
        $originKv = $originJs->keyValue($bucket);
        $originKv->create(['history' => 5])->await();

        try {
            $originKv->put('theme', 'light')->await();
            $originKv->put('theme', 'dark')->await();

            $history = $targetJs->keyValue($bucket)->history('theme')->await();
            self::assertSame(['light', 'dark'], array_map(static fn(KeyValueEntry $entry): ?string => $entry->value, $history));
        } finally {
            $originKv->deleteBucket()->await();
        }
    }

    /**
     * `getAll()` through the prefix, of 2 keys or more, needs `response: stream` on the `$JS.API.>`
     * export: without it, only the first key's reply arrives.
     */
    public function testKeyValueGetAllOfSeveralKeysThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        $bucket = $this->acctName('getall');
        $originKv = $originJs->keyValue($bucket);
        $originKv->create()->await();

        try {
            $originKv->put('username', 'alice')->await();
            $originKv->put('email', 'alice@example.com')->await();
            $originKv->put('role', 'admin')->await();

            $all = $targetJs->keyValue($bucket)->getAll()->await();
            self::assertSame('alice', $all['username'] ?? null);
            self::assertSame('alice@example.com', $all['email'] ?? null);
            self::assertSame('admin', $all['role'] ?? null);
        } finally {
            $originKv->deleteBucket()->await();
        }
    }

    /**
     * An Object Store object of several chunks, written and read back through the prefix: `put()`
     * needs the `$O.>` export, and `get()` of several chunks needs `response: stream`.
     */
    public function testObjectStoreMultiChunkObjectRoundTripThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        $bucket = $this->acctName('obj');
        $originStore = $originJs->objectStore($bucket);
        $originStore->create()->await();

        try {
            $data = random_bytes(300 * 1024);
            $info = $targetJs->objectStore($bucket)->put('report.bin', $data)->await();
            self::assertSame(strlen($data), $info->size);
            self::assertGreaterThan(1, $info->chunks, 'the object must span several chunks');

            $read = $targetJs->objectStore($bucket)->get('report.bin')->await();
            self::assertSame($data, $read?->data);
        } finally {
            $originStore->deleteBucket()->await();
        }
    }

    /**
     * A `watch()` through the prefix receives a value ORIGIN wrote, through the `_INBOX.JS.ORD.>`
     * export (the watch's delivery subject).
     */
    public function testWatchReceivesAValueOriginWroteThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        $bucket = $this->acctName('watch');
        $originKv = $originJs->keyValue($bucket);
        $originKv->create()->await();

        try {
            $watched = null;
            $sid = $targetJs->keyValue($bucket)->watch(
                static function (KeyValueEntry $entry) use (&$watched): void {
                    $watched = $entry;
                },
                'theme',
            )->await();

            $originKv->put('theme', 'dark')->await();

            $cancellation = new TimeoutCancellation(6.0);
            try {
                while ($watched === null) {
                    $target->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
            }

            self::assertNotNull($watched, 'the watch must receive the value ORIGIN wrote through _INBOX.JS.ORD.>');
            self::assertSame('theme', $watched->key);
            self::assertSame('dark', $watched->value);

            $target->unsubscribe($sid)->await();
        } finally {
            $originKv->deleteBucket()->await();
        }
    }

    /**
     * `subscribeOrderedConsumer()` through the prefix needs the `_INBOX.JS.ORD.>` export for its
     * delivery subject.
     */
    public function testSubscribeOrderedConsumerThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        [$stream, $subject] = $this->acctStreamAndSubject('ORDERED');
        $originJs->createStream($stream, [$subject . '.>'])->await();

        try {
            $received = [];
            $sid = $targetJs->subscribeOrderedConsumer(
                $stream,
                static function (NatsMessage $message) use (&$received): void {
                    $received[] = $message->payload;
                },
            )->await();

            $originJs->publish($subject . '.1', 'one')->await();

            $cancellation = new TimeoutCancellation(6.0);
            try {
                while (count($received) < 1) {
                    $target->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
            }

            self::assertSame(['one'], $received);

            $targetJs->stopOrderedConsumer($sid)->await();
        } finally {
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * `subscribeEphemeralPushConsumer()` without a `deliverSubject` through the prefix delivers on
     * the default `_INBOX.JS.PUSH.` literal, which needs its own export.
     */
    public function testSubscribeEphemeralPushConsumerWithoutDeliverSubjectThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        [$stream, $subject] = $this->acctStreamAndSubject('EPHPUSH');
        $originJs->createStream($stream, [$subject . '.>'])->await();

        try {
            $received = null;
            $sid = $targetJs->subscribeEphemeralPushConsumer(
                $stream,
                static function (NatsMessage $message) use (&$received, $targetJs): void {
                    $received = $message;
                    if ($message->replyTo !== null && $message->replyTo !== '') {
                        $targetJs->ack($message)->await();
                    }
                },
            )->await();

            $originJs->publish($subject . '.1', 'ephemeral')->await();

            $cancellation = new TimeoutCancellation(6.0);
            try {
                while ($received === null) {
                    $target->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
            }

            self::assertInstanceOf(NatsMessage::class, $received);
            self::assertSame('ephemeral', $received->payload);

            $target->unsubscribe($sid)->await();
        } finally {
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * `subscribePushConsumer()` with a `deliverSubject` under `DELIVER.READER.` through the prefix
     * needs the export of that literal.
     */
    public function testSubscribePushConsumerWithDeliverSubjectUnderDeliverReaderThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        [$stream, $subject] = $this->acctStreamAndSubject('PUSH');
        $originJs->createStream($stream, [$subject . '.>'])->await();
        $deliverSubject = 'DELIVER.READER.' . bin2hex(random_bytes(3));

        try {
            $received = null;
            $sid = $targetJs->subscribePushConsumer(
                $stream,
                'reader',
                static function (NatsMessage $message) use (&$received, $targetJs): void {
                    $received = $message;
                    $targetJs->ack($message)->await();
                },
                $deliverSubject,
            )->await();

            $originJs->publish($subject . '.1', 'push')->await();

            $cancellation = new TimeoutCancellation(6.0);
            try {
                while ($received === null) {
                    $target->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
            }

            self::assertInstanceOf(NatsMessage::class, $received);
            self::assertSame('push', $received->payload);

            $target->unsubscribe($sid)->await();
        } finally {
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * An ordered consumer through the prefix reads 300 messages of 16 KB to the end: it needs the
     * `$JS.FC.>` import for flow control once the replay passes the 2 MB mark, and the
     * `_INBOX.JS.ORD.>` export for delivery. The wait ends either when all 300 arrived or when
     * nothing new arrived for a few seconds, so a stall (as without `$JS.FC.>`, which stops at 2 MB)
     * ends the test instead of running out its whole timeout.
     */
    public function testOrderedConsumerReads300MessagesOf16KbToTheEndThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        [$stream, $subject] = $this->acctStreamAndSubject('FC');
        $originJs->createStream($stream, [$subject . '.>'])->await();

        try {
            $total = 300;
            $payload = bin2hex(random_bytes(8 * 1024)); // 16 KiB
            for ($i = 1; $i <= $total; $i++) {
                $originJs->publish($subject . '.' . $i, $payload)->await();
            }

            $received = 0;
            $sid = $targetJs->subscribeOrderedConsumer(
                $stream,
                static function (NatsMessage $message) use (&$received): void {
                    $received++;
                },
            )->await();

            $idleSeconds = 3.0;
            $hardDeadline = $this->monotonic() + 30.0;
            $lastProgress = $this->monotonic();
            while ($received < $total && $this->monotonic() < $hardDeadline && ($this->monotonic() - $lastProgress) < $idleSeconds) {
                $before = $received;
                try {
                    $target->processIncoming(new TimeoutCancellation(0.5))->await();
                } catch (CancelledException) {
                }
                if ($received > $before) {
                    $lastProgress = $this->monotonic();
                }
            }

            self::assertSame($total, $received, 'the ordered consumer must read all 300 messages through $JS.FC.>');

            $targetJs->stopOrderedConsumer($sid)->await();
        } finally {
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * A KV `put()` and a `publish()` through the prefix, each read back by ORIGIN's client: `put()`
     * needs the `$KV.>` import (renamed under the prefix), `publish()` needs the `orders.>` import.
     * Both write under `orders.<random>.>`/the bucket's own subjects, the only plain-publish subjects
     * the fixture's config exports to TARGET.
     */
    public function testKeyValuePutAndPublishReadBackByOriginThroughThePrefix(): void
    {
        $originJs = $this->connectOrigin();
        $target = $this->connectTarget();
        $targetJs = $target->jetStream(apiPrefix: self::ORIGIN_API_PREFIX);

        $bucket = $this->acctName('rt');
        $originKv = $originJs->keyValue($bucket);
        $originKv->create()->await();

        [$stream, $subject] = $this->acctStreamAndSubject('ORDERS', 'orders');
        $originJs->createStream($stream, [$subject . '.>'])->await();

        try {
            $targetJs->keyValue($bucket)->put('color', 'blue')->await();
            self::assertSame('blue', $originKv->get('color')->await()?->value);

            $targetJs->publish($subject . '.created', '{"id":1}')->await();
            self::assertSame(1, $originJs->getStream($stream)->await()->raw['state']['messages'] ?? null);
        } finally {
            $originKv->deleteBucket()->await();
            $originJs->deleteStream($stream)->await();
        }
    }

    /**
     * Connects ORIGIN directly (no prefix: it has JetStream of its own) and returns its default
     * JetStream context.
     */
    private function connectOrigin(): JetStreamContext
    {
        return $this->connect(self::ORIGIN_USER, self::ORIGIN_PASSWORD)->jetStream();
    }

    /**
     * Connects TARGET. Its default JetStream context has no responders (503); callers reach ORIGIN
     * through `jetStream(apiPrefix: self::ORIGIN_API_PREFIX)`.
     */
    private function connectTarget(): NatsClient
    {
        return $this->connect(self::TARGET_USER, self::TARGET_PASSWORD);
    }

    private function connect(string $username, string $password): NatsClient
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationAccountsServerUrl();

        // Probe the port without a client first: a NatsClient retries a refused first connect
        // (maxReconnectAttempts, with backoff), which would hold the test for minutes.
        $host = (string) (parse_url($url, PHP_URL_HOST) ?? '127.0.0.1');
        $port = (int) (parse_url($url, PHP_URL_PORT) ?? 4222);
        $probe = @stream_socket_client('tcp://' . $host . ':' . $port, $errorCode, $errorMessage, 2.0);
        if ($probe === false) {
            self::markTestSkipped('The nats-accounts fixture is not running at ' . $url . ' (' . $errorMessage . ').');
        }
        fclose($probe);

        $client = new NatsClient(new NatsOptions(
            servers: [$url],
            username: $username,
            password: $password,
            requestTimeoutMs: 5000,
        ));
        $client->connect()->await();
        $this->clients[] = $client;

        return $client;
    }

    /**
     * Returns a random stream name and the subject prefix it covers (`<prefix>.<random>`, matched by
     * `<prefix>.<random>.>`), so two runs of this class against the same server never collide.
     *
     * @return array{string, string}
     */
    private function acctStreamAndSubject(string $label, string $subjectPrefix = 'acct284'): array
    {
        $token = strtolower(bin2hex(random_bytes(4)));

        return ['ACCT_' . strtoupper($label) . '_' . strtoupper($token), $subjectPrefix . '.' . $token];
    }

    /**
     * Returns a random bucket/object-store name for this class's tests.
     */
    private function acctName(string $label): string
    {
        return 'acct284' . strtolower($label) . strtolower(bin2hex(random_bytes(4)));
    }
}
