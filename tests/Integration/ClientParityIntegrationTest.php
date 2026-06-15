<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\KeyValue\KeyWatchOptions;
use IDCT\NATS\Services\ServiceError;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * Live-server coverage for the P1 client-API parity features (#16-#32) against the Docker fixtures.
 */
final class ClientParityIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    private function client(): NatsClient
    {
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        return $client;
    }

    /**
     * Pumps a client's read loop in a background fiber until the returned canceller is cancelled.
     */
    private function pump(NatsClient $client): DeferredCancellation
    {
        $canceller = new DeferredCancellation();
        async(static function () use ($client, $canceller): void {
            $cancellation = $canceller->getCancellation();
            try {
                while (!$cancellation->isRequested()) {
                    $client->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
                // Stopped once the assertion completed.
            } catch (\Throwable) {
                // Connection torn down at test end.
            }
        });

        return $canceller;
    }

    /**
     * #17 - a delivered message replies to its own reply subject via respond().
     */
    public function testRespondHelperRepliesToRequester(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.respond.' . bin2hex(random_bytes(4));
        $server = $this->client();
        $client = $this->client();

        $server->subscribe($subject, static function (NatsMessage $message): void {
            $message->respondWithHeaders('pong', ['X-Echo' => $message->payload])->await();
        })->await();
        $server->flush()->await();

        $pump = $this->pump($server);
        try {
            $reply = $client->request($subject, 'ping', 3000)->await();
        } finally {
            $pump->cancel();
        }

        self::assertSame('pong', $reply->payload);
        $headers = \IDCT\NATS\Core\NatsHeaders::fromWireBlock($reply->rawHeaders);
        self::assertSame('ping', $headers['X-Echo'] ?? null);

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * #21 - requestMany collects multiple replies and stops at maxResponses.
     */
    public function testRequestManyCollectsMultipleReplies(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.scatter.' . bin2hex(random_bytes(4));
        $server = $this->client();
        $client = $this->client();

        // One responder emits three replies to the requester's inbox.
        $server->subscribe($subject, static function (NatsMessage $message) use ($server): void {
            if ($message->replyTo === null) {
                return;
            }
            foreach (['a', 'b', 'c'] as $part) {
                $server->publish($message->replyTo, $part)->await();
            }
        })->await();
        $server->flush()->await();

        $pump = $this->pump($server);
        try {
            $replies = $client->requestMany($subject, 'go', null, 3, 3000, 500)->await();
        } finally {
            $pump->cancel();
        }

        $payloads = array_map(static fn(NatsMessage $m): string => $m->payload, $replies);
        sort($payloads);
        self::assertSame(['a', 'b', 'c'], $payloads);

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * #42 - multi-value headers round-trip through the server and are preserved by fromWireBlockMulti.
     */
    public function testMultiValueHeadersRoundTrip(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.multihdr.' . bin2hex(random_bytes(4));
        $client = $this->client();

        $received = null;
        $client->subscribe($subject, static function (NatsMessage $m) use (&$received): void {
            $received = $m;
        })->await();
        $client->flush()->await();

        $client->publishWithHeaders($subject, 'body', ['X-Tag' => ['one', 'two'], 'X-Single' => 'solo'])->await();

        $cancellation = new TimeoutCancellation(3.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        self::assertNotNull($received);
        $multi = \IDCT\NATS\Core\NatsHeaders::fromWireBlockMulti($received->rawHeaders);
        self::assertSame(['one', 'two'], $multi['X-Tag'] ?? null);
        self::assertSame(['solo'], $multi['X-Single'] ?? null);

        $client->disconnect()->await();
    }

    /**
     * #22 - the connection listener observes Connected then Closed.
     */
    public function testConnectionLifecycleListenerObservesConnectAndClose(): void
    {
        $this->requireIntegrationEnabled();

        $events = [];
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationServerUrl()],
            connectionListener: static function (ConnectionEvent $event) use (&$events): void {
                $events[] = $event;
            },
        ));

        $client->connect()->await();
        $client->disconnect()->await();

        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $events);
    }

    /**
     * #24 - a token provider supplies credentials on connect (token-auth server).
     */
    public function testDynamicTokenProviderAuthenticates(): void
    {
        $this->requireIntegrationEnabled();

        $calls = 0;
        $token = $this->integrationToken();
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationTokenServerUrl()],
            tokenProvider: static function () use (&$calls, $token): string {
                $calls++;

                return $token;
            },
        ));

        $client->connect()->await();
        // A round trip proves the connection is authenticated and usable.
        $subject = 'it.tok.' . bin2hex(random_bytes(4));
        $received = null;
        $client->subscribe($subject, static function (NatsMessage $m) use (&$received): void {
            $received = $m->payload;
        })->await();
        $client->publish($subject, 'hi')->await();

        $cancellation = new TimeoutCancellation(3.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        self::assertSame('hi', $received);
        self::assertGreaterThanOrEqual(1, $calls);

        $client->disconnect()->await();
    }

    /**
     * #16 - optimistic-concurrency publish expectations: match succeeds, mismatch fails.
     */
    public function testPublishExpectationsEnforceLastSequence(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.exp';
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $first = $js->publish($subject, 'one')->await();
        // Correct expectation: appends at the known last sequence.
        $second = $js->publish($subject, 'two', expectedLastSequence: $first->seq)->await();
        self::assertSame($first->seq + 1, $second->seq);

        // Stale expectation is rejected by the server.
        $threw = false;
        try {
            $js->publish($subject, 'three', expectedLastSequence: $first->seq)->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'A stale expectedLastSequence must be rejected');

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #20 - deleteMessage removes a stored message by sequence.
     */
    public function testDeleteMessageRemovesStoredMessage(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.del';
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $ack = $js->publish($subject, 'gone-soon')->await();
        self::assertSame('gone-soon', $js->getStreamMessage($stream, $ack->seq)->await()->payload);

        self::assertTrue($js->deleteMessage($stream, $ack->seq)->await());

        $threw = false;
        try {
            $js->getStreamMessage($stream, $ack->seq)->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'The deleted sequence must no longer be retrievable');

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #18 + #30 - ackSync double-acks a pulled message and messageMetadata exposes the $JS.ACK tuple.
     */
    public function testAckSyncAndMessageMetadataOnPulledMessage(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.meta';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        $js->publish($subject, 'm1')->await();
        $js->publish($subject, 'm2')->await();

        $messages = $js->fetchBatch($stream, $consumer, 1, 2000)->await();
        $message = $messages[0];

        $meta = $js->messageMetadata($message);
        self::assertSame($stream, $meta->stream);
        self::assertSame($consumer, $meta->consumer);
        self::assertSame(1, $meta->streamSequence);
        self::assertSame(1, $meta->numDelivered);
        self::assertGreaterThanOrEqual(1, $meta->numPending);
        self::assertGreaterThan(0, $meta->timestampNanos);

        // Double-ack resolves only when the server confirms the ack was recorded.
        $js->ackSync($message, 2000)->await();

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #32 - a pull consumer's handle loop stops promptly when stop() is signalled from the handler.
     */
    public function testPullConsumerStopHaltsLoop(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.stop';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        for ($i = 1; $i <= 5; $i++) {
            $js->publish($subject, (string) $i)->await();
        }

        $iter = $js->pullConsumer($stream, $consumer)->setBatching(1)->setExpiresMs(700);
        $seen = [];
        $total = $iter->handle(function (NatsMessage $message, $ctx) use (&$seen, $iter): void {
            $seen[] = $message->payload;
            $ctx->ack($message)->await();
            if (count($seen) === 2) {
                $iter->stop();
            }
        })->await();

        self::assertSame(2, $total, 'stop() must halt the loop after the second message');

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #37 - credentials embedded in the server URL authenticate (user:pass against the userpass server).
     */
    public function testUrlEmbeddedUserPasswordAuthenticates(): void
    {
        $this->requireIntegrationEnabled();

        $host = parse_url($this->integrationUserPassServerUrl(), PHP_URL_HOST) . ':' . parse_url($this->integrationUserPassServerUrl(), PHP_URL_PORT);
        $url = sprintf('nats://%s:%s@%s', $this->integrationUsername(), $this->integrationPassword(), $host);

        $client = new NatsClient(new NatsOptions(servers: [$url]));
        $client->connect()->await();

        // A round trip proves the URL-derived credentials authenticated the connection.
        $subject = 'it.urlcred.' . bin2hex(random_bytes(4));
        $received = null;
        $client->subscribe($subject, static function (NatsMessage $m) use (&$received): void {
            $received = $m->payload;
        })->await();
        $client->flush()->await();
        $client->publish($subject, 'ok')->await();

        $cancellation = new TimeoutCancellation(3.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        self::assertSame('ok', $received);
        $client->disconnect()->await();
    }

    /**
     * #37 - a token embedded in the server URL authenticates (token@host against the token server).
     */
    public function testUrlEmbeddedTokenAuthenticates(): void
    {
        $this->requireIntegrationEnabled();

        $host = parse_url($this->integrationTokenServerUrl(), PHP_URL_HOST) . ':' . parse_url($this->integrationTokenServerUrl(), PHP_URL_PORT);
        $url = sprintf('nats://%s@%s', $this->integrationToken(), $host);

        $client = new NatsClient(new NatsOptions(servers: [$url]));
        $client->connect()->await();

        self::assertNotNull($client->serverInfo());
        $client->disconnect()->await();
    }

    /**
     * #45 - an injected ClientTlsContext is used verbatim for the handshake.
     */
    public function testInjectedTlsContextConnects(): void
    {
        $this->requireIntegrationEnabled();

        $caFile = $this->integrationTlsCaFile();
        $certFile = $this->integrationTlsCertFile();
        $keyFile = $this->integrationTlsKeyFile();
        if ($caFile === null || $certFile === null || $keyFile === null) {
            $this->markTestSkipped('TLS fixtures required for the injected-TLS-context test.');
        }

        $tls = (new ClientTlsContext('127.0.0.1'))
            ->withCaFile($caFile)
            ->withCertificate(new Certificate($certFile, $keyFile));
        if (getenv('NATS_TLS_SKIP_VERIFY') === '1') {
            $tls = $tls->withoutPeerVerification();
        }

        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationTlsServerUrl()],
            tlsHandshakeFirst: true,
            tlsContext: $tls,
        ));

        $client->connect()->await();
        self::assertNotNull($client->serverInfo());
        $client->disconnect()->await();
    }

    /**
     * #52 - connection accessors (connectedUrl, maxPayload, RTT, statistics) against a live server.
     */
    public function testConnectionAccessorsLive(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationServerUrl();
        $client = new NatsClient(new NatsOptions(servers: [$url]));
        $client->connect()->await();

        self::assertSame($url, $client->connectedUrl());
        self::assertGreaterThan(0, $client->maxPayload());

        $rtt = $client->rtt()->await();
        self::assertGreaterThan(0.0, $rtt);

        $subject = 'it.stats.' . bin2hex(random_bytes(4));
        $client->subscribe($subject, static function (NatsMessage $m): void {})->await();
        $client->flush()->await();
        $client->publish($subject, 'payload-12')->await();

        $cancellation = new TimeoutCancellation(3.0);
        try {
            while ($client->statistics()->inMsgs < 1) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        $stats = $client->statistics();
        self::assertGreaterThanOrEqual(1, $stats->outMsgs);
        self::assertGreaterThanOrEqual(1, $stats->inMsgs);

        $client->disconnect()->await();
    }

    /**
     * #46 - a bad credential fails fast with AuthenticationException, not an exhausted reconnect loop.
     */
    public function testAuthenticationErrorFailsFast(): void
    {
        $this->requireIntegrationEnabled();

        // Capture reconnect activity so fast-fail can be asserted on BEHAVIOR rather than wall-clock
        // latency (a wall-clock bound flakes under an occasional event-loop stall - see #70).
        $logger = new class extends \Psr\Log\AbstractLogger {
            /** @var list<string> */
            public array $messages = [];

            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->messages[] = (string) $message;
            }
        };

        // Reconnect ENABLED with several attempts; an auth failure must still return promptly.
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationTokenServerUrl()],
            reconnectEnabled: true,
            maxReconnectAttempts: 5,
            reconnectDelayMs: 200,
            token: 'definitely-the-wrong-token',
            logger: $logger,
        ));

        $threw = false;
        try {
            $client->connect()->await();
        } catch (\IDCT\NATS\Exception\AuthenticationException) {
            $threw = true;
        }

        self::assertTrue($threw, 'A bad token must raise AuthenticationException');
        // Fast-fail proof (timing-independent): a bad credential is not retried through the reconnect
        // loop, so no "reconnect attempt" was ever logged. Exhausting 5 attempts would log five.
        $reconnectAttempts = array_filter(
            $logger->messages,
            static fn(string $m): bool => str_contains($m, 'reconnect attempt'),
        );
        self::assertCount(0, $reconnectAttempts, 'A bad credential must fail fast without reconnect attempts');
    }

    /**
     * #60 - KV and Object Store bucket discovery lists created buckets.
     */
    public function testBucketDiscovery(): void
    {
        $this->requireIntegrationEnabled();

        $kvName = 'disc' . strtolower(bin2hex(random_bytes(2)));
        $osName = 'disc' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();

        $js->keyValue($kvName)->create()->await();
        $js->objectStore($osName)->create()->await();

        self::assertContains($kvName, $js->keyValueBucketNames()->await());
        self::assertContains($osName, $js->objectStoreBucketNames()->await());
        // KV discovery must not list the object-store bucket and vice versa.
        self::assertNotContains($osName, $js->keyValueBucketNames()->await());

        $js->keyValue($kvName)->deleteBucket()->await();
        $js->objectStore($osName)->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #33 + #41 - KV getRevision reads a historical revision and history() returns all revisions.
     */
    public function testKeyValueGetRevisionAndHistory(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'hi' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create(['history' => 10])->await();

        $r1 = $kv->put('color', 'red')->await();
        $kv->put('color', 'green')->await();
        $kv->put('color', 'blue')->await();

        // getRevision reads the value stored at the first revision.
        $old = $kv->getRevision('color', $r1->seq)->await();
        self::assertNotNull($old);
        self::assertSame('red', $old->value);

        // history returns every revision, oldest first.
        $history = $kv->history('color')->await();
        self::assertSame(['red', 'green', 'blue'], array_map(static fn($e): ?string => $e->value, $history));

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #62 - a KV mirror bucket is created with its mirror pointing at the source bucket's KV_ stream.
     */
    public function testKeyValueMirrorBucketConfig(): void
    {
        $this->requireIntegrationEnabled();

        $primary = 'pri' . strtolower(bin2hex(random_bytes(2)));
        $replica = 'rep' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();

        $js->keyValue($primary)->create()->await();
        $js->keyValue($replica)->create(['mirror' => $primary])->await();

        $stream = $js->getStream('KV_' . $replica)->await();
        self::assertSame('KV_' . $primary, $stream->raw['config']['mirror']['name'] ?? null);

        $js->keyValue($replica)->deleteBucket()->await();
        $js->keyValue($primary)->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #34 - compare-and-delete: a stale expected revision is rejected, the current one succeeds.
     */
    public function testKeyValueCompareAndDelete(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'cd' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create(['history' => 10])->await();

        $kv->put('lock', 'v1')->await();
        $current = $kv->put('lock', 'v2')->await();

        // A stale revision is rejected.
        $threw = false;
        try {
            $kv->delete('lock', null, $current->seq - 1)->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'compare-and-delete must reject a stale revision');
        self::assertNotNull($kv->get('lock')->await()?->value);

        // The current revision succeeds: the key now reads back as a tombstone (DEL, null value).
        $kv->delete('lock', null, $current->seq)->await();
        self::assertSame('DEL', $kv->get('lock')->await()->operation);

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #53 + #54 - typed StreamConfiguration / ConsumerConfiguration builders create matching assets.
     */
    public function testTypedStreamAndConsumerBuilders(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.builder';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();

        $created = $js->addStream(
            \IDCT\NATS\JetStream\Configuration\StreamConfiguration::create($stream)
                ->subjects($subject)
                ->retention(\IDCT\NATS\JetStream\Enum\RetentionPolicy::WorkQueue)
                ->maxBytes(1_000_000),
        )->await();
        self::assertSame($stream, $created->name);
        self::assertSame('workqueue', $created->raw['config']['retention'] ?? null);
        self::assertSame(1_000_000, (int) ($created->raw['config']['max_bytes'] ?? 0));

        $consumerInfo = $js->addConsumer(
            $stream,
            \IDCT\NATS\JetStream\Configuration\ConsumerConfiguration::create()
                ->durable($consumer)
                ->filterSubject($subject)
                ->ackPolicy(\IDCT\NATS\JetStream\Enum\AckPolicy::Explicit)
                ->maxDeliver(3)
                ->ackWait(2000),
        )->await();
        self::assertSame($consumer, $consumerInfo->name);

        $fetched = $js->getConsumer($stream, $consumer)->await();
        self::assertSame('explicit', $fetched->raw['config']['ack_policy'] ?? null);
        self::assertSame(3, (int) ($fetched->raw['config']['max_deliver'] ?? 0));

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #35 - streamNames() and consumerNames() list names without the full info payload.
     */
    public function testStreamAndConsumerNames(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.names';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        self::assertContains($stream, $js->streamNames()->await());
        self::assertContains($stream, $js->streamNames($subject)->await());
        self::assertSame([$consumer], $js->consumerNames($stream)->await());

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #36 - getLastMessageForSubject returns the most recent message for a subject (leader path).
     */
    public function testGetLastMessageForSubjectLive(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $base = 'it.' . strtolower($stream);
        $client = $this->client();
        $js = $client->jetStream();
        $js->createStream($stream, [$base . '.>'])->await();

        $js->publish($base . '.a', 'first-a')->await();
        $js->publish($base . '.b', 'only-b')->await();
        $js->publish($base . '.a', 'second-a')->await();

        $lastA = $js->getLastMessageForSubject($stream, $base . '.a')->await();
        self::assertSame('second-a', $lastA->payload);
        self::assertSame($base . '.a', $lastA->subject);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #44 - createOrUpdateStream upserts: creates first, then updates the existing stream's subjects.
     */
    public function testCreateOrUpdateStreamUpserts(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $base = 'it.' . strtolower($stream);
        $client = $this->client();
        $js = $client->jetStream();

        $created = $js->createOrUpdateStream($stream, [$base . '.one'])->await();
        self::assertSame([$base . '.one'], $created->subjects);

        // Second call on the existing stream updates its subject set (no "already in use" error).
        $updated = $js->createOrUpdateStream($stream, [$base . '.one', $base . '.two'])->await();
        self::assertSame([$base . '.one', $base . '.two'], $updated->subjects);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * #19 - KV createKey is exclusive: first create wins, a second throws.
     */
    public function testKeyValueCreateKeyIsExclusive(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'cr' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $ack = $kv->createKey('token', 'first')->await();
        self::assertGreaterThanOrEqual(1, $ack->seq);

        $threw = false;
        try {
            $kv->createKey('token', 'second')->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'createKey on an existing live key must throw');

        self::assertSame('first', $kv->get('token')->await()?->value);

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #25 - keys() returns live key names, excluding deleted keys.
     */
    public function testKeyValueKeysListsLiveKeys(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'ky' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $kv->put('alpha', '1')->await();
        $kv->put('beta', '2')->await();
        $kv->put('gamma', '3')->await();
        $kv->delete('beta')->await();

        $keys = $kv->keys()->await();
        sort($keys);
        self::assertSame(['alpha', 'gamma'], $keys);
        self::assertSame($keys, $kv->listKeys()->await());

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #26 - watch options replay history and signal end-of-initial-data, suppressing deletes.
     */
    public function testKeyValueWatchOptionsReplayHistoryAndSignalCaughtUp(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'wo' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $kv->put('one', 'A')->await();
        $kv->put('two', 'B')->await();
        $kv->delete('two')->await();

        $seen = [];
        $caughtUp = false;
        $sid = $kv->watch(
            static function ($entry) use (&$seen): void {
                $seen[$entry->key] = $entry->value;
            },
            '>',
            new KeyWatchOptions(ignoreDeletes: true, onCaughtUp: static function () use (&$caughtUp): void {
                $caughtUp = true;
            }),
        )->await();

        $cancellation = new TimeoutCancellation(4.0);
        try {
            while (!$caughtUp) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        self::assertTrue($caughtUp, 'onCaughtUp must fire once the initial replay completes');
        // Last-per-subject replay delivers the live value of 'one'; 'two' is a delete and is suppressed.
        self::assertSame('A', $seen['one'] ?? null);
        self::assertArrayNotHasKey('two', $seen);

        $client->unsubscribe($sid)->await();
        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #28 - ObjectStore updateMeta renames an object without re-uploading its bytes.
     */
    public function testObjectStoreUpdateMetaRenames(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'ob' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $store = $client->jetStream()->objectStore($bucket);
        $store->create()->await();

        $payload = 'the-quick-brown-fox';
        $store->put('logo.bin', $payload, ['team' => 'design'])->await();

        $info = $store->updateMeta('logo.bin', 'brand.bin', ['team' => 'brand'])->await();
        self::assertSame('brand.bin', $info->name);
        self::assertSame(['team' => 'brand'], $info->metadata);

        // The renamed object resolves and its bytes survived the rename (no re-upload).
        $fetched = $store->get('brand.bin')->await();
        self::assertNotNull($fetched);
        self::assertSame($payload, $fetched->data);
        // The old name is tombstoned (gone): its latest meta is a delete marker.
        $old = $store->info('logo.bin')->await();
        self::assertNotNull($old);
        self::assertTrue($old->deleted);

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #27 - a service endpoint handler replies with a custom ServiceError (code/description/body).
     */
    public function testServiceHandlerRepliesWithCustomError(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.svcerr.' . bin2hex(random_bytes(4));
        $serviceClient = $this->client();
        $requester = $this->client();

        $service = $serviceClient->service('errsvc', '1.0.0')
            ->addEndpoint('fail', $subject, static function (NatsMessage $message): string {
                throw new ServiceError(429, 'Rate limited', '{"retry_after":5}');
            }, queueGroup: null);
        $service->start()->await();

        $pump = $this->pump($serviceClient);
        $reply = null;
        try {
            // Retry to absorb the brief window before the endpoint SUB is registered server-side.
            for ($attempt = 0; $attempt < 10; $attempt++) {
                try {
                    $reply = $requester->request($subject, 'go', 2000)->await();
                    break;
                } catch (\IDCT\NATS\Exception\NatsException $e) {
                    if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                        throw $e;
                    }
                    \Amp\delay(0.1);
                }
            }
        } finally {
            $pump->cancel();
        }

        self::assertInstanceOf(NatsMessage::class, $reply);
        $headers = \IDCT\NATS\Core\NatsHeaders::fromWireBlock($reply->rawHeaders);
        self::assertSame('Rate limited', $headers['Nats-Service-Error'] ?? null);
        self::assertSame('429', $headers['Nats-Service-Error-Code'] ?? null);
        self::assertSame('{"retry_after":5}', $reply->payload);

        $service->stop()->await();
        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * #39 - ObjectStore typed config maps to the backing stream configuration.
     */
    public function testObjectStoreTypedConfigApplied(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'tc' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $store = $client->jetStream()->objectStore($bucket);
        $store->create(new \IDCT\NATS\JetStream\ObjectStore\ObjectStoreConfig(maxBytes: 2_000_000, storage: 'file'))->await();

        $stream = $client->jetStream()->getStream('OBJ_' . $bucket)->await();
        self::assertSame(2_000_000, (int) ($stream->raw['config']['max_bytes'] ?? 0));

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #58 - an object description is stored and surfaced on ObjectInfo.
     */
    public function testObjectStoreDescriptionStored(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'ds' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $store = $client->jetStream()->objectStore($bucket);
        $store->create()->await();

        $put = $store->put('readme.txt', 'hello', [], 'Project readme')->await();
        self::assertSame('Project readme', $put->description);

        $info = $store->info('readme.txt')->await();
        self::assertNotNull($info);
        self::assertSame('Project readme', $info->description);

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #59 - get() transparently follows an object link to the target's content.
     */
    public function testObjectStoreGetFollowsLink(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'lr' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $store = $client->jetStream()->objectStore($bucket);
        $store->create()->await();

        $store->put('target.bin', 'the-payload')->await();
        $store->addLink('shortcut', 'target.bin')->await();

        // get() on the link resolves to the target's bytes.
        $fetched = $store->get('shortcut')->await();
        self::assertNotNull($fetched);
        self::assertSame('the-payload', $fetched->data);
        self::assertSame('target.bin', $fetched->info->name);

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #48 - addLink writes a resolvable link object pointing at a target object.
     */
    public function testObjectStoreAddLink(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'ln' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $store = $client->jetStream()->objectStore($bucket);
        $store->create()->await();

        $store->put('target.bin', 'payload')->await();
        $link = $store->addLink('shortcut', 'target.bin')->await();
        self::assertTrue($link->isLink());

        // The link record is retrievable and carries its target.
        $info = $store->info('shortcut')->await();
        self::assertNotNull($info);
        self::assertTrue($info->isLink());
        self::assertSame(['bucket' => $bucket, 'name' => 'target.bin'], $info->link);

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #38 - a sealed bucket rejects further writes.
     */
    public function testObjectStoreSealRejectsWrites(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'sl' . strtolower(bin2hex(random_bytes(2)));
        $client = $this->client();
        $store = $client->jetStream()->objectStore($bucket);
        $store->create()->await();
        $store->put('before.bin', 'ok')->await();

        self::assertTrue($store->seal()->await());

        $threw = false;
        try {
            $store->put('after.bin', 'nope')->await();
        } catch (JetStreamException) {
            $threw = true;
        }
        self::assertTrue($threw, 'A sealed bucket must reject new writes');

        // Existing content is still readable.
        $fetched = $store->get('before.bin')->await();
        self::assertNotNull($fetched);
        self::assertSame('ok', $fetched->data);

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * #40 + #50 - a grouped endpoint forwards metadata to $SRV.INFO and a stats supplier to $SRV.STATS.
     */
    public function testServiceGroupedMetadataAndCustomStats(): void
    {
        $this->requireIntegrationEnabled();

        $name = 'svc' . strtolower(bin2hex(random_bytes(2)));
        $serviceClient = $this->client();
        $requester = $this->client();

        $service = $serviceClient->service($name, '1.0.0');
        $service->addGroup('v1')->addEndpoint(
            'work',
            'work.' . $name,
            static fn(NatsMessage $message): string => 'ok',
            null,
            null,
            ['team' => 'core'],
            static fn(\IDCT\NATS\Services\ServiceEndpoint $e): array => ['queue_depth' => 3],
        );
        $service->start()->await();
        $serviceClient->flush()->await();

        $pump = $this->pump($serviceClient);
        try {
            $info = json_decode($requester->request('$SRV.INFO.' . $name, '', 3000)->await()->payload, true, 512, JSON_THROW_ON_ERROR);
            $stats = json_decode($requester->request('$SRV.STATS.' . $name, '', 3000)->await()->payload, true, 512, JSON_THROW_ON_ERROR);
        } finally {
            $pump->cancel();
        }

        // #40: grouped endpoint subject + per-endpoint metadata forwarded to INFO.
        self::assertSame('v1.work.' . $name, $info['endpoints'][0]['subject'] ?? null);
        self::assertSame(['team' => 'core'], $info['endpoints'][0]['metadata'] ?? null);
        // #50: custom stats supplier data forwarded to STATS.
        self::assertSame(['queue_depth' => 3], $stats['endpoints'][0]['data'] ?? null);

        $service->stop()->await();
        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * #57 - the service done handler fires when run() stops.
     */
    public function testServiceDoneHandlerFiresWhenRunStops(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.done.' . bin2hex(random_bytes(4));
        $serviceClient = $this->client();

        $done = false;
        $service = $serviceClient->service('donesvc', '1.0.0')
            ->addEndpoint('echo', $subject, static fn(NatsMessage $message): string => 'r', queueGroup: null)
            ->onDone(static function () use (&$done): void {
                $done = true;
            });

        // run() starts, processes for the timeout window, then stops (firing the done handler).
        $service->run(0.3)->await();

        self::assertTrue($done, 'The done handler must fire when run() stops');

        $serviceClient->disconnect()->await();
    }

    /**
     * #51 - drain() stops the service serving requests (subsequent requests get no responder).
     */
    public function testServiceDrainStopsServing(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.drain.' . bin2hex(random_bytes(4));
        $serviceClient = $this->client();
        $requester = $this->client();

        $service = $serviceClient->service('drainsvc', '1.0.0')
            ->addEndpoint('echo', $subject, static fn(NatsMessage $message): string => 'reply', queueGroup: null);
        $service->start()->await();
        $serviceClient->flush()->await();

        $pump = $this->pump($serviceClient);
        try {
            // Sanity: the endpoint serves before draining.
            $reply = null;
            for ($attempt = 0; $attempt < 10; $attempt++) {
                try {
                    $reply = $requester->request($subject, 'go', 1500)->await();
                    break;
                } catch (\IDCT\NATS\Exception\NatsException $e) {
                    if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                        throw $e;
                    }
                    \Amp\delay(0.1);
                }
            }
            self::assertNotNull($reply);

            // Drain, then the endpoint no longer responds.
            $service->drain()->await();

            $drainedThrew = false;
            try {
                $requester->request($subject, 'after-drain', 1000)->await();
            } catch (\IDCT\NATS\Exception\NatsException $e) {
                $drainedThrew = str_contains($e->getMessage(), 'No responders');
            }
            self::assertTrue($drainedThrew, 'After drain the endpoint must no longer respond');
        } finally {
            $pump->cancel();
        }

        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * #43 - drainSubscription delivers the in-flight message then stops further delivery.
     */
    public function testSubscriptionDrainStopsDelivery(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.subdrain.' . bin2hex(random_bytes(4));
        $client = $this->client();

        $delivered = [];
        $sid = $client->subscribe($subject, static function (NatsMessage $m) use (&$delivered): void {
            $delivered[] = $m->payload;
        })->await();
        $client->flush()->await();

        $client->publish($subject, 'one')->await();
        // Drain: receives 'one' in-flight, then removes the subscription.
        $client->drainSubscription($sid)->await();

        // After drain, this publish has no subscriber on the client.
        $client->publish($subject, 'two')->await();
        $client->flush()->await();
        try {
            $client->processIncoming(new TimeoutCancellation(0.3))->await();
        } catch (CancelledException) {
        }

        self::assertSame(['one'], $delivered);

        $client->disconnect()->await();
    }

    /**
     * #61 - WebSocket with permessage-deflate compression + a custom upgrade header round-trips.
     */
    public function testWebSocketCompressionAndCustomHeaders(): void
    {
        $this->requireIntegrationEnabled();

        $options = new NatsOptions(
            servers: [$this->integrationWsServerUrl()],
            webSocketCompression: true,
            webSocketHeaders: ['X-Client' => 'idct-nats'],
        );
        $client = new NatsClient($options, new WebSocketTransport($options));
        $client->connect()->await();

        // A larger, compressible payload exercises the deflate/inflate path.
        $subject = 'it.wsz.' . bin2hex(random_bytes(4));
        $payload = str_repeat('compressible-nats-payload ', 100);
        $received = null;
        $client->subscribe($subject, static function (NatsMessage $m) use (&$received): void {
            $received = $m->payload;
        })->await();
        $client->publish($subject, $payload)->await();

        $cancellation = new TimeoutCancellation(4.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }

        self::assertSame($payload, $received);

        $client->disconnect()->await();
    }

    /**
     * #31 - the WebSocket transport carries core pub/sub and JetStream over ws://.
     */
    public function testWebSocketTransportCarriesPubSubAndJetStream(): void
    {
        $this->requireIntegrationEnabled();

        $options = new NatsOptions(servers: [$this->integrationWsServerUrl()]);
        $client = new NatsClient($options, new WebSocketTransport($options));
        $client->connect()->await();

        // Core pub/sub round trip over WebSocket frames.
        $subject = 'it.ws.' . bin2hex(random_bytes(4));
        $received = null;
        $client->subscribe($subject, static function (NatsMessage $m) use (&$received): void {
            $received = $m->payload;
        })->await();
        $client->publish($subject, 'ws-hello')->await();

        $cancellation = new TimeoutCancellation(4.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
        }
        self::assertSame('ws-hello', $received);

        // JetStream publish + read-back over the same WebSocket connection.
        $stream = 'IT_WS_' . strtoupper(bin2hex(random_bytes(3)));
        $jsSubject = 'it.ws.' . strtolower($stream) . '.events';
        $js = $client->jetStream();
        $js->createStream($stream, [$jsSubject])->await();
        $ack = $js->publish($jsSubject, '{"over":"websocket"}')->await();
        self::assertGreaterThanOrEqual(1, $ack->seq);
        self::assertSame('{"over":"websocket"}', $js->getStreamMessage($stream, $ack->seq)->await()->payload);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }
}
