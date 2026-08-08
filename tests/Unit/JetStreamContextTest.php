<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\Configuration\ConsumerConfiguration;
use IDCT\NATS\JetStream\Configuration\StreamConfiguration;
use IDCT\NATS\JetStream\Configuration\StreamSource;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\JetStream\HeartbeatWatchdogState;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\KeyValue\KeyValueBucket;
use IDCT\NATS\JetStream\Models\StreamInfo;
use IDCT\NATS\JetStream\Schedule;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use Revolt\EventLoop\CallbackType;

use function Amp\delay;

final class JetStreamContextTest extends TestCase
{
    /**
     * A subscription's idle-heartbeat watchdog (#113) is a live EventLoop timer that outlives a test
     * whose client is never disconnect()ed. Cancel every callback left registered so the shared loop
     * (Revolt keeps one per process) starts each test clean and a leaked watchdog cannot fire against
     * a lingering client from an earlier test.
     */
    protected function tearDown(): void
    {
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    /**
     * Counts EventLoop repeat timers currently registered (the watchdog is one). Used by the
     * teardown-leak regression to probe watchdog arm/cancel without reaching into private state.
     */
    private static function countRepeatTimers(): int
    {
        $count = 0;
        foreach (EventLoop::getIdentifiers() as $id) {
            if (EventLoop::getType($id) === CallbackType::Repeat) {
                $count++;
            }
        }

        return $count;
    }

    private function jsOkResponse(string $json): string
    {
        return sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($json), $json);
    }

    /**
     * Post-#118 the request inbox is MUXED: request()/requestWithHeaders() no longer subscribe a fresh
     * inbox per call; instead ONE long-lived wildcard "<base>.*" (sid 1) serves every reply, and each
     * request publishes reply-to "<base>.<token>" and routes by that token. A pre-seeded
     * "MSG _INBOX.x <sid>" therefore no longer reaches the waiting request (its subject is not the
     * request's random "<base>.<token>", so dispatchMuxReply drops it and the request times out).
     *
     * This installs a dynamic responder that mirrors a real server: it learns the mux base from the
     * wildcard SUB, and for each request PUB/HPUB (a publish whose reply-to lives under that base)
     * pops the next reply frame and re-emits it on the CAPTURED reply-to with the mux sid (1). Frames
     * are delivered FIFO in the order requests are written, so a method that issues several requests
     * (e.g. create->update, create->info->delete) still gets its replies in sequence.
     *
     * Only request replies belong here; subscription deliveries (a push consumer's deliver subject,
     * a pull fetch inbox) are NOT mux replies - keep those pre-seeded, or enqueue them on their SUB via
     * FakeTransport::$enqueueOnWriteContaining so they arrive after the deliver subscription exists.
     *
     * @param list<string> $frames Pre-built MSG/HMSG reply frames; only their subject+sid are rewritten.
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
                // Learn the mux base from the wildcard registration "SUB <base>.* <sid>".
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1); // strip the trailing "*" -> "_INBOX.<hex>."
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            // PUB <subject> <replyTo> <len>  |  HPUB <subject> <replyTo> <hdrLen> <totLen>
            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                // A plain publish, or a subscription's own inbox (fetch/direct-get) - not a mux request.
                return [];
            }

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo)];
        };
    }

    /**
     * Rewrites a pre-built MSG/HMSG reply frame's subject and sid to the request's captured mux reply-to
     * and the mux sid (1), preserving the payload/header bytes and declared lengths verbatim so the
     * reply's content is delivered unchanged - only its addressing is corrected for the mux inbox.
     */
    private static function rewriteReplyFrame(string $frame, string $replyTo): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo; // subject
        $tokens[2] = '1';      // mux sid

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    /**
     * Extracts the mux reply-to (the 3rd token) from a written PUB/HPUB request frame, or '' when the
     * frame is not a request publish. Lets a dynamic onWrite responder echo a reply on the request's
     * actual reply-to (the mux "<base>.<token>") instead of a guessed inbox subject (#118).
     */
    private static function requestReplyTo(string $bytes): string
    {
        $head = strtok($bytes, "\r\n");
        if ($head === false || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
            return '';
        }

        return explode(' ', $head)[2] ?? '';
    }

    /**
     * Builds a mux reply frame (MSG on the given reply-to, mux sid 1) carrying $payload - the wire form
     * the server sends back on the shared request inbox (#118).
     */
    private static function muxMsg(string $replyTo, string $payload): string
    {
        return sprintf("MSG %s 1 %d\r\n%s\r\n", $replyTo, strlen($payload), $payload);
    }

    /**
     * Installs a mux-aware server for ordered/push consumer recreate tests. Pre-#118 each request owned
     * a per-request SUB, so a CONSUMER.CREATE/DELETE reply routed by a fixed sid and the rotated deliver
     * inbox landed at a sid shifted by those per-request subs (2,4,7,...). Post-#118 all requests share
     * the mux inbox (no per-request SUB), so: (a) CREATE/DELETE replies must be echoed on the request's
     * captured reply-to, and (b) the rotated deliver sid collapses to the next free sid (2,3,4,...). This
     * responder removes both hazards by construction: request replies go on the captured reply-to, and
     * each deliver epoch's frames are emitted on the deliver SUB using the sid the server ACTUALLY
     * assigned (captured live), so tests never hard-code a sid.
     *
     * @param list<callable(string):list<string>> $onCreate FIFO, one per CONSUMER.CREATE PUB; gets the reply-to.
     * @param list<callable(string):list<string>> $onDelete FIFO, one per CONSUMER.DELETE PUB; gets the reply-to.
     * @param list<callable(int):list<string>> $deliverEpochs FIFO, one per deliver SUB; gets the captured sid.
     */
    private function orderedConsumerServer(
        FakeTransport $transport,
        array $onCreate,
        array $onDelete = [],
        array $deliverEpochs = [],
    ): void {
        // The callback arrays are captured BY VALUE (so PHPStan keeps their callable return types); FIFO
        // consumption is tracked by the by-reference cursors below.
        $createIdx = 0;
        $deleteIdx = 0;
        $deliverIdx = 0;
        $transport->onWrite = static function (string $bytes) use (
            $onCreate,
            $onDelete,
            $deliverEpochs,
            &$createIdx,
            &$deleteIdx,
            &$deliverIdx,
        ): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                $parts = explode(' ', $head);
                $subject = $parts[1] ?? '';
                // The mux wildcard SUB ends with ".*"; every other SUB here is a (rotated) deliver inbox.
                $epoch = $deliverEpochs[$deliverIdx] ?? null;
                if ($epoch !== null && !str_ends_with($subject, '.*')) {
                    $deliverIdx++;

                    return $epoch((int) ($parts[2] ?? 0));
                }

                return [];
            }

            $replyTo = self::requestReplyTo($bytes);
            if ($replyTo === '') {
                return [];
            }

            $create = $onCreate[$createIdx] ?? null;
            if ($create !== null && str_contains($bytes, '$JS.API.CONSUMER.CREATE.')) {
                $createIdx++;

                return $create($replyTo);
            }

            $delete = $onDelete[$deleteIdx] ?? null;
            if ($delete !== null && str_contains($bytes, '$JS.API.CONSUMER.DELETE.')) {
                $deleteIdx++;

                return $delete($replyTo);
            }

            return [];
        };
    }

    /**
     * Verifies accountInfo() returns parsed account metrics.
     */
    public function testAccountInfo(): void
    {
        $accountPayload = '{"memory":11,"storage":22,"streams":3,"consumers":4}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($accountPayload), $accountPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $account = $client->jetStream()->accountInfo()->await();

        self::assertSame(11, $account->memory);
        self::assertSame(22, $account->storage);
        self::assertStringStartsWith('PUB $JS.API.INFO _INBOX.', $transport->writes[3]);
    }

    /**
     * Verifies addStream() builds the CREATE payload from a typed StreamConfiguration (#53).
     */
    public function testAddStreamFromBuilder(): void
    {
        $reply = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $config = \IDCT\NATS\JetStream\Configuration\StreamConfiguration::create('ORDERS')
            ->subjects('orders.*', 'orders.archive')
            ->retention(\IDCT\NATS\JetStream\Enum\RetentionPolicy::WorkQueue)
            ->storage(\IDCT\NATS\JetStream\Enum\StorageBackend::Memory)
            ->maxBytes(4096)
            ->maxAge(60)
            ->replicas(3);

        $info = $client->jetStream()->addStream($config)->await();

        self::assertSame('ORDERS', $info->name);
        $create = $transport->writes[3];
        self::assertStringContainsString('$JS.API.STREAM.CREATE.ORDERS', $create);
        self::assertStringContainsString('"subjects":["orders.*","orders.archive"]', $create);
        self::assertStringContainsString('"retention":"workqueue"', $create);
        self::assertStringContainsString('"storage":"memory"', $create);
        self::assertStringContainsString('"max_bytes":4096', $create);
        self::assertStringContainsString('"max_age":60000000000', $create);
        self::assertStringContainsString('"num_replicas":3', $create);
    }

    /**
     * Verifies addConsumer() builds the CREATE payload from a typed ConsumerConfiguration (#54).
     */
    public function testAddConsumerFromBuilder(): void
    {
        $reply = '{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $config = \IDCT\NATS\JetStream\Configuration\ConsumerConfiguration::create()
            ->durable('worker')
            ->ackPolicy(\IDCT\NATS\JetStream\Enum\AckPolicy::Explicit)
            ->maxDeliver(5)
            ->ackWait(1000)
            ->backoff([1000, 2000]);

        $info = $client->jetStream()->addConsumer('ORDERS', $config)->await();

        self::assertSame('worker', $info->name);
        $create = $transport->writes[3];
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS.worker', $create);
        self::assertStringContainsString('"durable_name":"worker"', $create);
        self::assertStringContainsString('"ack_policy":"explicit"', $create);
        self::assertStringContainsString('"max_deliver":5', $create);
        self::assertStringContainsString('"ack_wait":1000000000', $create);
        self::assertStringContainsString('"backoff":[1000000000,2000000000]', $create);
    }

    /**
     * Verifies keyValueBucketNames() lists KV_-prefixed streams with the prefix stripped (#60).
     */
    public function testKeyValueBucketNames(): void
    {
        $reply = '{"streams":["KV_cfg","OBJ_assets","ORDERS","KV_sessions"],"total":4}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        self::assertSame(['cfg', 'sessions'], $client->jetStream()->keyValueBucketNames()->await());
    }

    /**
     * Verifies objectStoreBucketNames() lists OBJ_-prefixed streams with the prefix stripped (#60).
     */
    public function testObjectStoreBucketNames(): void
    {
        $reply = '{"streams":["KV_cfg","OBJ_assets","ORDERS","OBJ_media"],"total":4}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        self::assertSame(['assets', 'media'], $client->jetStream()->objectStoreBucketNames()->await());
    }

    /**
     * Verifies streamNames() returns names from STREAM.NAMES (#35).
     */
    public function testStreamNames(): void
    {
        $reply = '{"streams":["ORDERS","EVENTS"],"total":2,"offset":0,"limit":256}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $names = $client->jetStream()->streamNames()->await();

        self::assertSame(['ORDERS', 'EVENTS'], $names);
        self::assertStringContainsString('$JS.API.STREAM.NAMES', $transport->writes[3]);
    }

    /**
     * Verifies consumerNames() returns names from CONSUMER.NAMES (#35).
     */
    public function testConsumerNames(): void
    {
        $reply = '{"consumers":["worker-1","worker-2"],"total":2}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $names = $client->jetStream()->consumerNames('ORDERS')->await();

        self::assertSame(['worker-1', 'worker-2'], $names);
        self::assertStringContainsString('$JS.API.CONSUMER.NAMES.ORDERS', $transport->writes[3]);
    }

    /**
     * Verifies getLastMessageForSubject() uses last_by_subj and parses the stored message (#36).
     */
    public function testGetLastMessageForSubject(): void
    {
        $reply = '{"message":{"subject":"orders.new","seq":7,"data":"' . base64_encode('hello') . '"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getLastMessageForSubject('ORDERS', 'orders.new')->await();

        self::assertSame('orders.new', $message->subject);
        self::assertSame('hello', $message->payload);
        self::assertStringContainsString('$JS.API.STREAM.MSG.GET.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"last_by_subj":"orders.new"', $transport->writes[3]);
    }

    /**
     * Verifies getLastMessageForSubject() rejects wildcard subjects (#36).
     */
    public function testGetLastMessageForSubjectRejectsWildcard(): void
    {
        $client = new NatsClient(new NatsOptions());
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('non-wildcard');
        $client->jetStream()->getLastMessageForSubject('ORDERS', 'orders.*')->await();
    }

    /**
     * Verifies createOrUpdateStream() falls back to UPDATE when the stream already exists (#44).
     */
    public function testCreateOrUpdateStreamFallsBackToUpdate(): void
    {
        $createErr = '{"error":{"code":400,"err_code":10058,"description":"stream name already in use"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*","orders.archive"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Muxed replies, FIFO: CREATE -> already-in-use, then UPDATE -> ok (#118).
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updateOk), $updateOk),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*', 'orders.archive'])->await();

        self::assertSame('ORDERS', $info->name);
        // writes: [0]=CONNECT [1]=PING [2]=mux SUB [3]=CREATE PUB [4]=UPDATE PUB (no per-request SUB/UNSUB).
        self::assertStringContainsString('$JS.API.STREAM.CREATE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[4]);
    }

    /**
     * Verifies a JetStream API error envelope's err_code is exposed via getErrCode() alongside the
     * HTTP-like code kept in getCode() (#154).
     */
    public function testApiErrorEnvelopeExposesErrCode(): void
    {
        $err = '{"error":{"code":404,"err_code":10059,"description":"stream not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($err), $err),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->getStream('MISSING')->await();
            self::fail('Expected a JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(404, $e->getCode());
            self::assertSame(10059, $e->getErrCode());
        }
    }

    /**
     * Verifies a publish-expectation error ack exposes err_code 10071 via getErrCode(), and that an
     * envelope without err_code yields null (old servers) (#154).
     */
    public function testPublishExpectationMismatchExposesErrCode(): void
    {
        $withErrCode = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"}}';
        $withoutErrCode = '{"error":{"code":400,"description":"wrong last sequence: 5"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($withErrCode), $withErrCode),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($withoutErrCode), $withoutErrCode),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}', expectedLastSequence: 99)->await();
            self::fail('Expected a JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(400, $e->getCode());
            self::assertSame(10071, $e->getErrCode());
        }

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}', expectedLastSequence: 99)->await();
            self::fail('Expected a JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(400, $e->getCode());
            self::assertNull($e->getErrCode());
        }
    }

    /**
     * Verifies createOrUpdateStream() discriminates "stream name already in use" by err_code 10058,
     * not by description wording: a reworded description still falls back to UPDATE (#154).
     */
    public function testCreateOrUpdateStreamFallsBackToUpdateByErrCode(): void
    {
        $createErr = '{"error":{"code":400,"err_code":10058,"description":"cannot add stream: that name is taken"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updateOk), $updateOk),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'])->await();

        self::assertSame('ORDERS', $info->name);
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[4]);
    }

    /**
     * Verifies createOrUpdateStream() trusts a present err_code over a misleading description: an
     * error whose wording mentions "already in use" but whose err_code is not 10058 is re-thrown
     * instead of triggering the UPDATE fallback (#154).
     */
    public function testCreateOrUpdateStreamRethrowsWhenErrCodeIsNotStreamNameInUse(): void
    {
        $createErr = '{"error":{"code":400,"err_code":10065,"description":"subjects [orders.*] already in use by an existing stream"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
            // An UPDATE reply is queued so a wrong fallback would succeed silently instead of hanging.
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updateOk), $updateOk),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('already in use by an existing stream');

        $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'])->await();
    }

    /**
     * Verifies createOrUpdateStream() still falls back to UPDATE via the description substring when
     * the envelope carries no err_code (old servers) (#154).
     */
    public function testCreateOrUpdateStreamFallsBackToUpdateWithoutErrCode(): void
    {
        $createErr = '{"error":{"code":400,"description":"stream name already in use"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updateOk), $updateOk),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'])->await();

        self::assertSame('ORDERS', $info->name);
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[4]);
    }

    /**
     * Verifies create/get/delete stream operations map expected payload fields.
     */
    public function testStreamCrud(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Muxed replies, FIFO: CREATE, INFO, DELETE (#118).
        $this->muxReplies($transport, [
            "MSG _INBOX.a 1 52\r\n{\"config\":{\"name\":\"ORDERS\",\"subjects\":[\"orders.*\"]}}\r\n",
            "MSG _INBOX.b 2 52\r\n{\"config\":{\"name\":\"ORDERS\",\"subjects\":[\"orders.*\"]}}\r\n",
            "MSG _INBOX.c 3 16\r\n{\"success\":true}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $js = $client->jetStream();
        $created = $js->createStream('ORDERS', ['orders.*'])->await();
        $fetched = $js->getStream('ORDERS')->await();
        $deleted = $js->deleteStream('ORDERS')->await();

        self::assertSame('ORDERS', $created->name);
        self::assertSame(['orders.*'], $created->subjects);
        self::assertSame('ORDERS', $fetched->name);
        self::assertTrue($deleted);
        // writes: [0]=CONNECT [1]=PING [2]=mux SUB [3]=CREATE [4]=INFO [5]=DELETE (one mux SUB, no UNSUB).
        self::assertStringContainsString('$JS.API.STREAM.CREATE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.INFO.ORDERS', $transport->writes[4]);
        self::assertStringContainsString('$JS.API.STREAM.DELETE.ORDERS', $transport->writes[5]);
    }

    /**
     * Verifies JetStream API error payloads are converted to JetStreamException.
     */
    public function testJetStreamApiErrorMapping(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            "MSG _INBOX.a 1 48\r\n{\"error\":{\"code\":404,\"description\":\"not found\"}}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('not found');

        $client->jetStream()->getStream('MISSING')->await();
    }

    /**
     * Verifies the client returns the same JetStream context instance on repeated access.
     */
    public function testJetStreamContextIsCached(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $a = $client->jetStream();
        $b = $client->jetStream();

        self::assertInstanceOf(JetStreamContext::class, $a);
        self::assertSame($a, $b);
    }

    /**
     * Verifies objectStore() constructs an equivalent bucket wrapper per call instead of memoizing:
     * the per-name cache had no eviction, so a long-lived context accumulated one wrapper per bucket
     * name touched, forever (#133). Fresh instances must stay behaviorally interchangeable (equal
     * state, same backing stream).
     */
    public function testObjectStoreConstructsEquivalentBucketPerCall(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $a = $client->jetStream()->objectStore('assets');
        $b = $client->jetStream()->objectStore('assets');
        $c = $client->jetStream()->objectStore('other');

        // No memoization: each call builds a new wrapper (nothing is retained on the context)...
        self::assertNotSame($a, $b);
        // ...and the wrappers are all-readonly value objects, so repeated calls are interchangeable.
        self::assertEquals($a, $b);
        self::assertSame('OBJ_assets', $a->streamName());
        self::assertSame('OBJ_assets', $b->streamName());
        self::assertSame('OBJ_other', $c->streamName());
    }

    /**
     * Verifies keyValue() constructs an equivalent bucket wrapper per call instead of memoizing -
     * same no-eviction rationale as the object store accessor (#133).
     */
    public function testKeyValueConstructsEquivalentBucketPerCall(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $a = $client->jetStream()->keyValue('profiles');
        $b = $client->jetStream()->keyValue('profiles');
        $c = $client->jetStream()->keyValue('sessions');

        self::assertInstanceOf(KeyValueBucket::class, $a);
        // No memoization: each call builds a new wrapper (nothing is retained on the context)...
        self::assertNotSame($a, $b);
        // ...and the wrappers are all-readonly value objects, so repeated calls are interchangeable.
        self::assertEquals($a, $b);
        self::assertSame('KV_profiles', $a->streamName());
        self::assertSame('KV_profiles', $b->streamName());
        self::assertSame('KV_sessions', $c->streamName());
    }

    /**
     * Verifies pullConsumer helper returns an iterator wrapper.
     */
    public function testPullConsumerReturnsIterator(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $iterator = $client->jetStream()->pullConsumer('ORDERS', 'PROC');

        self::assertInstanceOf(PullConsumerIterator::class, $iterator);
    }

    /**
     * Verifies consumer create/get/delete operations map expected payload fields.
     */
    public function testConsumerCrud(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';
        $infoPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Muxed replies, FIFO: CREATE, INFO, DELETE (#118).
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($infoPayload), $infoPayload),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $js = $client->jetStream();
        $created = $js->createConsumer('ORDERS', 'PROC', 'orders.*')->await();
        $fetched = $js->getConsumer('ORDERS', 'PROC')->await();
        $deleted = $js->deleteConsumer('ORDERS', 'PROC')->await();

        self::assertSame('ORDERS', $created->streamName);
        self::assertSame('PROC', $created->name);
        self::assertSame('PROC', $fetched->name);
        self::assertTrue($deleted);
        // writes: [0]=CONNECT [1]=PING [2]=mux SUB [3]=CREATE [4]=INFO [5]=DELETE (one mux SUB, no UNSUB).
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS.PROC', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.CONSUMER.INFO.ORDERS.PROC', $transport->writes[4]);
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.ORDERS.PROC', $transport->writes[5]);
    }

    /**
     * Verifies createConsumer sends filter_subjects (and omits the singular filter_subject) when an
     * array of filters is supplied via options (issue #10).
     */
    public function testCreateConsumerWithFilterSubjects(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['filter_subjects' => ['orders.eu.>', 'orders.us.>']])->await();

        self::assertStringContainsString('"filter_subjects":["orders.eu.>","orders.us.>"]', $transport->writes[3]);
        self::assertStringNotContainsString('"filter_subject"', $transport->writes[3]);
    }

    /**
     * Verifies combining a single filter subject with filter_subjects is rejected before dispatch.
     */
    public function testCreateConsumerRejectsBothFilterForms(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Use either a single filter subject or filter_subjects, not both');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', 'orders.*', ['filter_subjects' => ['orders.eu.>']])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a filter_subjects array containing an empty subject is rejected before dispatch.
     */
    public function testCreateConsumerRejectsEmptyFilterSubjectEntry(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('filter_subjects must contain only non-empty subject strings');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['filter_subjects' => ['orders.eu.>', '']])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies the mutual-exclusion guard also fires when the singular filter_subject is smuggled in
     * via the options bag alongside filter_subjects (issue #10).
     */
    public function testCreateConsumerRejectsFilterSubjectInOptionsConflict(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Use either a single filter subject or filter_subjects, not both');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, [
                'filter_subject' => 'orders.eu.>',
                'filter_subjects' => ['orders.us.>'],
            ])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies an empty filter subject is rejected uniformly on the ephemeral path too (issue #10).
     */
    public function testCreateEphemeralConsumerRejectsEmptyFilterSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Consumer filter subject must not be empty');

        try {
            $client->jetStream()->createEphemeralConsumer('ORDERS', '')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies filter_subjects flows through the push-consumer create path too (issue #10).
     */
    public function testCreatePushConsumerWithFilterSubjects(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->createPushConsumer('ORDERS', 'PROC', '_INBOX.deliver', null, [
            'filter_subjects' => ['orders.eu.>', 'orders.us.>'],
        ])->await();

        self::assertStringContainsString('"filter_subjects":["orders.eu.>","orders.us.>"]', $transport->writes[3]);
        self::assertStringNotContainsString('"filter_subject"', $transport->writes[3]);
    }

    /**
     * Verifies createConsumer validates and forwards priority-group config (issue #7).
     */
    public function testCreateConsumerWithPriorityGroups(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->createConsumer('ORDERS', 'PROC', null, [
            'priority_groups' => ['g1'],
            'priority_policy' => 'pinned_client',
        ])->await();

        self::assertStringContainsString('"priority_groups":["g1"]', $transport->writes[3]);
        self::assertStringContainsString('"priority_policy":"pinned_client"', $transport->writes[3]);
    }

    /**
     * Verifies an invalid priority policy is rejected before dispatch.
     */
    public function testCreateConsumerRejectsInvalidPriorityPolicy(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('priority_policy must be one of');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['priority_policy' => 'bogus'])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies the pull request carries the priority/group fields (issue #7).
     */
    public function testFetchBatchWithPullOptions(): void
    {
        $msg = '{"event":"x"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, [
            'group' => 'g1',
            'min_pending' => 5,
            'max_bytes' => 1048576,
            'no_wait' => true,
        ])->await();

        $written = $transport->writes[3];
        self::assertStringContainsString('"group":"g1"', $written);
        self::assertStringContainsString('"min_pending":5', $written);
        self::assertStringContainsString('"max_bytes":1048576', $written);
        self::assertStringContainsString('"no_wait":true', $written);
    }

    /**
     * Verifies an out-of-range pull priority is rejected before dispatch.
     */
    public function testFetchBatchRejectsInvalidPriority(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Pull priority must be an integer between 0 and 9');

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['priority' => 10])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies an unknown pull-request field is rejected before dispatch instead of being silently
     * dropped (#132).
     */
    public function testFetchBatchRejectsUnknownPullField(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Unknown pull request field "heartbeat"; supported fields: group, id, min_pending, min_ack_pending, priority, max_bytes, no_wait, idle_heartbeat');

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['heartbeat' => 1_000_000_000])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies the ADR-13 idle_heartbeat pull field reaches the wire in the pull request JSON (#132).
     */
    public function testFetchBatchSendsIdleHeartbeat(): void
    {
        $msg = '{"event":"x"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, [
            'idle_heartbeat' => 1_000_000_000,
        ])->await();

        self::assertStringContainsString('"idle_heartbeat":1000000000', $transport->writes[3]);
    }

    /**
     * Verifies ADR-13 client-side validation: an idle_heartbeat above 50% of expires is rejected
     * with a clear InvalidArgumentException before anything reaches the wire, instead of being
     * forwarded for the server to reject (or silently misbehave) (#153).
     */
    public function testFetchBatchRejectsIdleHeartbeatAboveHalfOfExpires(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('must not exceed 50% of expires');

        try {
            // 150ms heartbeat against a 200ms expiry: above the ADR-13 50% ceiling (100ms).
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 200, ['idle_heartbeat' => 150_000_000])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a non-positive (or non-integer) idle_heartbeat is rejected client-side before
     * dispatch (#153).
     */
    public function testFetchBatchRejectsNonPositiveIdleHeartbeat(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('idle_heartbeat must be a positive integer');

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['idle_heartbeat' => 0])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * The fetch reply inbox must be slow-consumer-exempt (#118/#120 twin): readIncoming enqueues
     * every frame of a chunk before draining, so a burst of small pull deliveries above the
     * per-subscription pending cap (default 1024) arriving in ONE read chunk was DropOldest-
     * discarded silently - the batch returned with its head missing while the server counted every
     * message as delivered (permanently lost on a max_deliver=1 consumer). All frames of the burst
     * must reach the caller.
     */
    public function testFetchBatchSurvivesSingleChunkBurstAboveThePendingCap(): void
    {
        // 1300 deliveries (> the 1024 default cap) coalesced into ONE transport chunk.
        $burst = '';
        for ($i = 0; $i < 1300; $i++) {
            $payload = (string) $i;
            $burst .= sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($payload), $payload);
        }

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $burst,
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1300, 2500)->await();

        self::assertCount(1300, $messages, 'no delivery of the burst may be dropped by the slow-consumer bound');
        // Head intact and order preserved - DropOldest would have discarded exactly this head.
        self::assertSame('0', $messages[0]->payload);
        self::assertSame('1299', $messages[1299]->payload);
    }

    /**
     * Verifies the ADR-13 boundary: an idle_heartbeat of exactly 50% of expires is accepted and
     * reaches the wire (#153).
     */
    public function testFetchBatchAcceptsIdleHeartbeatAtExactlyHalfOfExpires(): void
    {
        $msg = '{"event":"x"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // 1250ms heartbeat against a 2500ms expiry: exactly the 50% ceiling - allowed.
        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, [
            'idle_heartbeat' => 1_250_000_000,
        ])->await();

        self::assertCount(1, $messages);
        self::assertStringContainsString('"idle_heartbeat":1250000000', $transport->writes[3]);
    }

    /**
     * Verifies missed pull idle heartbeats are detected: with idle_heartbeat requested and a
     * silent transport (no message, no status-100 heartbeat frame), fetchBatch() fails within
     * ~2 heartbeat intervals with a heartbeat-miss error instead of sitting out the full
     * expires+slack deadline (nats.go ErrNoHeartbeat semantics) (#153).
     */
    public function testFetchBatchFailsFastOnMissedIdleHeartbeats(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $startNs = hrtime(true);
        try {
            // 50ms heartbeat, 2000ms expiry: the server should heartbeat every 50ms, so silence
            // for 2 intervals (100ms) means it is gone.
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2000, ['idle_heartbeat' => 50_000_000])->await();
            self::fail('Expected a heartbeat-miss JetStreamException.');
        } catch (JetStreamException $e) {
            $elapsedNs = hrtime(true) - $startNs;
            self::assertStringContainsString('missed idle heartbeats', $e->getMessage());
            // ~2 heartbeat intervals (100ms), far below the expires+slack deadline (3000ms).
            self::assertLessThan(1_000_000_000, $elapsedNs, 'a heartbeat miss must fail fast, not wait out expires');
        }
    }

    /**
     * Verifies unpinConsumer issues the UNPIN request with the group (issue #7).
     */
    public function testUnpinConsumer(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen('{}'), '{}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $ok = $client->jetStream()->unpinConsumer('ORDERS', 'PROC', 'g1')->await();

        self::assertTrue($ok);
        self::assertStringContainsString('$JS.API.CONSUMER.UNPIN.ORDERS.PROC', $transport->writes[3]);
        self::assertStringContainsString('"group":"g1"', $transport->writes[3]);
    }

    /**
     * Verifies pinIdOf extracts the Nats-Pin-Id header (issue #7).
     */
    public function testPinIdOf(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $pinned = new NatsMessage(
            subject: 'orders.created',
            sid: 1,
            replyTo: null,
            payload: 'x',
            rawHeaders: "NATS/1.0\r\nNats-Pin-Id: pin-123\r\n\r\n",
        );
        $plain = new NatsMessage('orders.created', 1, null, 'x', null);

        self::assertSame('pin-123', $js->pinIdOf($pinned));
        self::assertNull($js->pinIdOf($plain));
    }

    /**
     * Verifies a batched Direct Get collects multiple replies and stops at the 204 EOB (issue #13).
     */
    public function testDirectGetBatchCollectsUntilEob(): void
    {
        $h1 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\n\r\n";
        $b1 = 'aaa';
        $h2 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.b\r\nNats-Sequence: 6\r\n\r\n";
        $b2 = 'bbb';
        $eob = "NATS/1.0 204 EOB\r\n\r\n";
        $h3 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.c\r\nNats-Sequence: 7\r\n\r\n";
        $b3 = 'ccc';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h1), strlen($h1) + strlen($b1), $h1, $b1),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h2), strlen($h2) + strlen($b2), $h2, $b2),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($eob), strlen($eob), $eob),
            // A frame AFTER the EOB must NOT be consumed: if termination were broken the loop would
            // read this and return 3 messages.
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h3), strlen($h3) + strlen($b3), $h3, $b3),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10])->await();

        self::assertCount(2, $messages);
        self::assertSame('aaa', $messages[0]->payload);
        self::assertSame('orders.a', $messages[0]->subject);
        self::assertSame('bbb', $messages[1]->payload);
        self::assertSame('orders.b', $messages[1]->subject);
        self::assertStringContainsString('$JS.API.DIRECT.GET.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"batch":10', $transport->writes[3]);
    }

    /**
     * The Direct Get batch inbox must be slow-consumer-exempt (#118/#120 twin): a reply burst above
     * the per-subscription pending cap arriving in one read chunk was DropOldest-discarded, and
     * since Direct Get replies are never redelivered while the 204 EOB still arrives, the call
     * returned a TRUNCATED result presented as complete. Every reply of the burst must be returned.
     */
    public function testDirectGetBatchSurvivesSingleChunkBurstAboveThePendingCap(): void
    {
        $burst = '';
        for ($i = 0; $i < 1100; $i++) {
            $header = sprintf("NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.s%d\r\nNats-Sequence: %d\r\n\r\n", $i, $i + 1);
            $body = (string) $i;
            $burst .= sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($header), strlen($header) + strlen($body), $header, $body);
        }
        $eob = "NATS/1.0 204 EOB\r\n\r\n";
        $burst .= sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($eob), strlen($eob), $eob);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $burst,
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetBatch('ORDERS', ['batch' => 1100])->await();

        self::assertCount(1100, $messages, 'a Direct Get burst must never be truncated by the slow-consumer bound');
        self::assertSame('0', $messages[0]->payload);
        self::assertSame('orders.s0', $messages[0]->subject);
        self::assertSame('1099', $messages[1099]->payload);
    }

    /**
     * ADR-31: a multi_last request whose subjects ALL have no stored message is answered with a
     * lone 404 status - "no matches", not an error. directGetLastForSubjects() must contribute
     * zero messages for such a chunk (and return [] when every chunk is empty) instead of throwing
     * away other chunks' results - previously the 404 aborted the whole call, making the KV/OS
     * batched enumerations throw spuriously when keys were purged between STREAM.INFO and the batch.
     */
    public function testDirectGetLastForSubjectsTreatsAllMiss404AsEmpty(): void
    {
        $status = "NATS/1.0 404 No Results\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($status), strlen($status), $status),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.a', 'orders.b'])->await();

        self::assertSame([], $messages, 'an all-absent subject set yields an empty result, not a 404 exception');
    }

    /**
     * Verifies directGetLastForSubjects sends multi_last and terminates on Nats-Num-Pending: 0.
     */
    public function testDirectGetLastForSubjects(): void
    {
        $h1 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\n\r\n";
        $b1 = 'aaa';
        $h2 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.b\r\nNats-Sequence: 6\r\nNats-Num-Pending: 0\r\n\r\n";
        $b2 = 'bbb';
        // A frame after the Nats-Num-Pending:0 terminator must NOT be consumed.
        $h3 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.c\r\nNats-Sequence: 7\r\n\r\n";
        $b3 = 'ccc';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h1), strlen($h1) + strlen($b1), $h1, $b1),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h2), strlen($h2) + strlen($b2), $h2, $b2),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h3), strlen($h3) + strlen($b3), $h3, $b3),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.a', 'orders.b'])->await();

        self::assertCount(2, $messages);
        self::assertStringContainsString('"multi_last":["orders.a","orders.b"]', $transport->writes[3]);
        self::assertStringContainsString('"batch":2', $transport->writes[3]);
    }

    /**
     * #110 regression: directGetLastForSubjects() must split an exact-subject list larger than the
     * server's per-request result cap into several batched requests, rather than sending one oversized
     * multi_last a NATS 2.11+ server rejects with "Too Many Results". With 1001 subjects it issues TWO
     * Direct Get requests (1000 + 1) and concatenates the replies in chunk order.
     *
     * Falsifies the pre-fix code, which sent all subjects in a single request (`batch: 1001`): that
     * produced exactly one DIRECT.GET PUB, so the two-request assertions below fail.
     */
    public function testDirectGetLastForSubjectsChunksAboveResultCap(): void
    {
        // 1001 exact subjects: one past the 1000-subject chunk cap, forcing a 1000 + 1 split.
        $subjects = [];
        for ($i = 0; $i <= 1000; $i++) {
            $subjects[] = 'orders.k' . $i;
        }

        // The server answers each chunk on its own inbox sid (monotonic: chunk 1 -> sid 1, chunk 2 ->
        // sid 2). Each reply is a single data message terminated by Nats-Num-Pending: 0.
        $h1 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.k0\r\nNats-Sequence: 1\r\nNats-Num-Pending: 0\r\n\r\n";
        $b1 = 'v0';
        $h2 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.k1000\r\nNats-Sequence: 2\r\nNats-Num-Pending: 0\r\n\r\n";
        $b2 = 'v1000';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h1), strlen($h1) + strlen($b1), $h1, $b1),
            sprintf("HMSG _INBOX.JS.DGET.y 2 %d %d\r\n%s%s\r\n", strlen($h2), strlen($h2) + strlen($b2), $h2, $b2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', $subjects)->await();

        // Merged in chunk order: chunk 1's message first, chunk 2's second.
        self::assertCount(2, $messages);
        self::assertSame('orders.k0', $messages[0]->subject);
        self::assertSame('v0', $messages[0]->payload);
        self::assertSame('orders.k1000', $messages[1]->subject);
        self::assertSame('v1000', $messages[1]->payload);

        // Exactly two batched Direct Get requests were sent (not one oversized request).
        $directGets = array_values(array_filter(
            $transport->writes,
            static fn (string $frame): bool => str_contains($frame, '$JS.API.DIRECT.GET.ORDERS'),
        ));
        self::assertCount(2, $directGets);

        // First chunk carries 1000 subjects (batch 1000) up to orders.k999; the 1001st subject is held
        // back for the second chunk (batch 1) - the split boundary is the 1000-subject cap.
        self::assertStringContainsString('"batch":1000', $directGets[0]);
        self::assertStringContainsString('"orders.k0"', $directGets[0]);
        self::assertStringContainsString('"orders.k999"', $directGets[0]);
        self::assertStringNotContainsString('"orders.k1000"', $directGets[0]);

        self::assertStringContainsString('"batch":1', $directGets[1]);
        self::assertStringContainsString('"orders.k1000"', $directGets[1]);
    }

    /**
     * #110 regression: directGetLastForSubjects() must also keep each batched request within the
     * server's negotiated max_payload, so a bucket whose subject list would not fit one PUB is
     * enumerated across several requests. With a deliberately small max_payload the five-subject list
     * splits, every request stays within max_payload, and the union of requested subjects is exactly
     * the input (no subject lost or duplicated).
     *
     * Falsifies the pre-fix code, which packed all subjects into a single request regardless of
     * max_payload (one DIRECT.GET PUB): the > 1 request and per-request size assertions below fail.
     */
    public function testDirectGetLastForSubjectsChunksToRespectMaxPayload(): void
    {
        $maxPayload = 200;
        // Subjects long enough that only a couple fit one request under the small max_payload.
        $letters = 'abcde';
        $subjects = [];
        for ($i = 0; $i < 5; $i++) {
            $subjects[] = 'orders.' . $letters[$i] . str_repeat('x', 40);
        }

        // Each chunk replies with a single empty end-of-batch marker (204) on its own sid; the split is
        // what is under test, not the payloads. sids are monotonic across the sequential chunks.
        $reads = [
            sprintf('INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":%d,"headers":true}' . "\r\n", $maxPayload),
            "PONG\r\n",
        ];
        $eob = "NATS/1.0 204\r\n\r\n";
        for ($sid = 1; $sid <= 5; $sid++) {
            $reads[] = sprintf("HMSG _INBOX.JS.DGET.c%d %d %d %d\r\n%s\r\n", $sid, $sid, strlen($eob), strlen($eob), $eob);
        }

        $transport = new FakeTransport($reads);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        self::assertSame($maxPayload, $client->maxPayload());

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', $subjects)->await();
        self::assertSame([], $messages);

        $directGets = array_values(array_filter(
            $transport->writes,
            static fn (string $frame): bool => str_contains($frame, '$JS.API.DIRECT.GET.ORDERS'),
        ));

        // The small max_payload forces more than one request, and no request's JSON payload (the part
        // max_payload bounds - not the PUB command/subject prefix) exceeds it. A single unchunked
        // request of all five subjects would exceed max_payload and be rejected by publish().
        self::assertGreaterThan(1, count($directGets));
        foreach ($directGets as $frame) {
            // PUB <subject> <reply> <len>\r\n<payload>\r\n - the payload follows the first CRLF.
            $payload = rtrim(explode("\r\n", $frame, 2)[1] ?? '', "\r\n");
            self::assertLessThanOrEqual($maxPayload, strlen($payload), 'a batched Direct Get payload exceeded max_payload');
        }

        // Every input subject appears in exactly one request - nothing lost or duplicated by the split.
        $seen = [];
        foreach ($subjects as $subject) {
            $hits = 0;
            foreach ($directGets as $frame) {
                if (str_contains($frame, '"' . $subject . '"')) {
                    $hits++;
                }
            }

            self::assertSame(1, $hits, sprintf('subject %s appeared in %d chunks', $subject, $hits));
            $seen[] = $subject;
        }

        self::assertCount(5, $seen);
    }

    /**
     * #110 regression: the per-chunk max_payload budget must count each subject's cost with the SAME
     * encoding the request is serialized with. json_encode escapes '/' to '\/' (and KV keys legally
     * contain '/'), so a slash-bearing subject encodes wider than strlen + 2 quotes. Estimating cost as
     * strlen + 3 under-counts every slash, packing too many subjects into a chunk whose real payload
     * then exceeds max_payload and is rejected by publish() - exactly the failure chunking prevents.
     *
     * Falsifies the strlen-based estimate: with these slash-heavy subjects and this small max_payload,
     * the under-count builds an oversized first chunk and publish() throws a ProtocolException before
     * any reply is read. The escaping-aware estimate keeps every chunk's real payload within budget.
     */
    public function testDirectGetLastForSubjectsChunkPayloadAccountsForJsonSlashEscaping(): void
    {
        $maxPayload = 400;
        // Slash-heavy subjects: strlen 27, but json_encode widens each of the 12 slashes by one byte.
        $subjects = [];
        for ($i = 0; $i < 20; $i++) {
            $subjects[] = 's' . $i . '.' . str_repeat('a/', 12) . 'z';
        }

        $reads = [
            sprintf('INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":%d,"headers":true}' . "\r\n", $maxPayload),
            "PONG\r\n",
        ];
        $eob = "NATS/1.0 204\r\n\r\n";
        // Over-seed EOB markers (one per possible chunk sid); unused frames are harmless.
        for ($sid = 1; $sid <= 20; $sid++) {
            $reads[] = sprintf("HMSG _INBOX.JS.DGET.c%d %d %d %d\r\n%s\r\n", $sid, $sid, strlen($eob), strlen($eob), $eob);
        }

        $transport = new FakeTransport($reads);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // With the escaping-aware budget this completes; with strlen + 3 the oversized chunk's PUB throws.
        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', $subjects)->await();
        self::assertSame([], $messages);

        $directGets = array_values(array_filter(
            $transport->writes,
            static fn (string $frame): bool => str_contains($frame, '$JS.API.DIRECT.GET.ORDERS'),
        ));

        self::assertGreaterThan(1, count($directGets));
        foreach ($directGets as $frame) {
            $payload = rtrim(explode("\r\n", $frame, 2)[1] ?? '', "\r\n");
            self::assertLessThanOrEqual(
                $maxPayload,
                strlen($payload),
                'a batched Direct Get payload exceeded max_payload despite slash escaping',
            );
        }
    }

    /**
     * #110 regression: when the server does not advertise max_payload, maxPayload() returns 0 and the
     * chunker must fall back to the NATS default 1 MiB budget, NOT collapse onto 0. A modest ten-subject
     * list then fits ONE batched request.
     *
     * Falsifies the guard mutations on the fallback ternary (`$maxPayload > 0` -> `>= 0`, and the `&&`
     * -> `||`): either would select max_payload==0 as the budget (floored to a 1-byte budget), forcing
     * one request PER subject - ten DIRECT.GET PUBs instead of one.
     */
    public function testDirectGetLastForSubjectsUsesDefaultBudgetWhenServerAdvertisesNoMaxPayload(): void
    {
        $subjects = [];
        for ($i = 0; $i < 10; $i++) {
            $subjects[] = 'orders.k' . $i;
        }

        // INFO WITHOUT a max_payload field: ServerInfo::maxPayload defaults to 0 (maxPayload() -> 0).
        $reads = [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
        $eob = "NATS/1.0 204\r\n\r\n";
        // Over-seed one EOB per possible chunk sid: with the mutants each subject becomes its own chunk (10).
        for ($sid = 1; $sid <= 10; $sid++) {
            $reads[] = sprintf("HMSG _INBOX.JS.DGET.c%d %d %d %d\r\n%s\r\n", $sid, $sid, strlen($eob), strlen($eob), $eob);
        }

        $transport = new FakeTransport($reads);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        self::assertSame(0, $client->maxPayload());

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', $subjects)->await();
        self::assertSame([], $messages);

        $directGets = array_values(array_filter(
            $transport->writes,
            static fn (string $frame): bool => str_contains($frame, '$JS.API.DIRECT.GET.ORDERS'),
        ));
        // Exactly ONE request: all ten subjects fit the 1 MiB fallback budget.
        self::assertCount(1, $directGets);
        self::assertStringContainsString('"batch":10', $directGets[0]);
    }

    /**
     * #110 regression: the per-chunk max_payload packing is byte-exact. Three 6-char slash-free subjects
     * each encode as `"o.aaaa"` (8 bytes) + 1 separator = 9 bytes. With max_payload 82 the budget is
     * 82 - 64 (envelope) = 18 = EXACTLY two subjects (2 * 9); the third overflows into a second chunk,
     * so the split is [2, 1].
     *
     * Falsifies two boundary mutations that would over-eagerly flush to [1, 1, 1] (three requests):
     * the fill comparison `> $payloadBudget` -> `>= $payloadBudget` (flushes AT the exact fit), and the
     * per-subject cost `+ 1` -> `+ 2` (inflates each subject so only one fits).
     */
    public function testDirectGetLastForSubjectsChunkBoundaryPacksToExactPayloadBudget(): void
    {
        $subjects = ['o.aaaa', 'o.bbbb', 'o.cccc'];

        $reads = [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":82,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
        $eob = "NATS/1.0 204\r\n\r\n";
        for ($sid = 1; $sid <= 3; $sid++) {
            $reads[] = sprintf("HMSG _INBOX.JS.DGET.c%d %d %d %d\r\n%s\r\n", $sid, $sid, strlen($eob), strlen($eob), $eob);
        }

        $transport = new FakeTransport($reads);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', $subjects)->await();
        self::assertSame([], $messages);

        $directGets = array_values(array_filter(
            $transport->writes,
            static fn (string $frame): bool => str_contains($frame, '$JS.API.DIRECT.GET.ORDERS'),
        ));
        self::assertCount(2, $directGets);
        self::assertStringContainsString('"multi_last":["o.aaaa","o.bbbb"]', $directGets[0]);
        self::assertStringContainsString('"batch":2', $directGets[0]);
        self::assertStringContainsString('"multi_last":["o.cccc"]', $directGets[1]);
        self::assertStringContainsString('"batch":1', $directGets[1]);
    }

    /**
     * #110 regression: one byte tighter, the packing must count every subject byte. With max_payload 81
     * the budget is 81 - 64 = 17 < 18, so even two 9-byte subjects no longer share a chunk - each subject
     * gets its own request, split [1, 1, 1].
     *
     * Falsifies the byte-accounting mutations that would let subjects share a chunk ([2, 1] or [1, 2]):
     * the initial `$currentBytes = 0` -> -1 (a phantom free byte in the first chunk), the flush-reset
     * `$currentBytes = 0` -> -1 (a phantom free byte in every later chunk), and the per-subject cost
     * `+ 1` -> `+ 0` / `- 1` (undercounting each subject).
     */
    public function testDirectGetLastForSubjectsChunkBoundaryCountsEverySubjectByte(): void
    {
        $subjects = ['o.aaaa', 'o.bbbb', 'o.cccc'];

        $reads = [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":81,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
        $eob = "NATS/1.0 204\r\n\r\n";
        for ($sid = 1; $sid <= 3; $sid++) {
            $reads[] = sprintf("HMSG _INBOX.JS.DGET.c%d %d %d %d\r\n%s\r\n", $sid, $sid, strlen($eob), strlen($eob), $eob);
        }

        $transport = new FakeTransport($reads);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', $subjects)->await();
        self::assertSame([], $messages);

        $directGets = array_values(array_filter(
            $transport->writes,
            static fn (string $frame): bool => str_contains($frame, '$JS.API.DIRECT.GET.ORDERS'),
        ));
        self::assertCount(3, $directGets);
        self::assertStringContainsString('"multi_last":["o.aaaa"]', $directGets[0]);
        self::assertStringContainsString('"multi_last":["o.bbbb"]', $directGets[1]);
        self::assertStringContainsString('"multi_last":["o.cccc"]', $directGets[2]);
        foreach ($directGets as $frame) {
            self::assertStringContainsString('"batch":1', $frame);
        }
    }

    /**
     * Verifies a batched Direct Get error status surfaces as a JetStreamException (issue #13).
     */
    public function testDirectGetBatchSurfacesError(): void
    {
        $err = "NATS/1.0 408 Request Timeout\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($err), strlen($err), $err),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(408);

        $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10])->await();
    }

    /**
     * Verifies supportsBatchedDirectGet() gates the batched multi_last Direct Get on NATS 2.11+, the
     * version gate KV getAll() / Object Store list() use to choose the batched path vs the per-subject
     * fan-out fallback (#110). An unknown/unparseable version is treated as unsupported so the
     * conservative fan-out (which works on any server) is used.
     */
    public function testSupportsBatchedDirectGetGatesOnServerVersion(): void
    {
        $cases = [
            '2.10.9' => false,            // pre-2.11: unsupported
            '2.11.0' => true,             // boundary: supported
            'v2.11.0' => true,            // leading 'v' consumed by v? - the MAJOR is match[1]==2, not
                                          // match[0]=="v2.11" ((int) of which is 0 -> would misread as < 2.11)
            '2.12.9' => true,
            '3.0.0' => true,
            '2.11.0-beta.1' => true,      // pre-release tag ignored, numeric prefix >= 2.11
            'dev-custom' => false,        // unparseable -> conservative fan-out fallback
            'x9.9' => false,              // leading non-digit: the ^ anchor forbids matching 9.9 mid-string,
                                          // so an unparseable prefix stays unsupported (fan-out fallback)
        ];

        foreach ($cases as $version => $expected) {
            $info = sprintf(
                'INFO {"server_id":"S1","server_name":"n1","version":"%s","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                $version,
            );
            $client = new NatsClient(new NatsOptions(), new FakeTransport([$info, "PONG\r\n"]));
            $client->connect()->await();

            self::assertSame($expected, $client->jetStream()->supportsBatchedDirectGet(), 'version ' . $version);
        }
    }

    /**
     * Verifies JetStream publish returns stream/sequence acknowledgment.
     */
    public function testPublishWithAck(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":42,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->publish('orders.created', '{"id":1}')->await();

        self::assertSame('ORDERS', $ack->stream);
        self::assertSame(42, $ack->seq);
        self::assertFalse($ack->duplicate);
        self::assertStringStartsWith('PUB orders.created _INBOX.', $transport->writes[3]);
    }

    public function testPublishWrapsMalformedAckAsJetStreamException(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            "MSG _INBOX.a 1 7\r\nnotjson\r\n", // a non-JSON publish ack
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed JetStream publish ack');
        $client->jetStream()->publish('orders.created', '{"id":1}')->await();
    }

    /**
     * Verifies JetStream publish maps API errors to JetStreamException.
     */
    public function testPublishMapsApiError(): void
    {
        $errorPayload = '{"error":{"code":500,"description":"publish failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('publish failed');

        $client->jetStream()->publish('orders.created', '{"id":1}')->await();
    }

    /**
     * Verifies stream creation forwards additional stream configuration options.
     */
    public function testCreateStreamWithOptions(): void
    {
        $streamPayload = '{"config":{"name":"SCHED","subjects":["schedules.>","events.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamPayload), $streamPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->createStream(
            'SCHED',
            ['schedules.>', 'events.>'],
            ['allow_msg_schedules' => true],
        )->await();

        self::assertStringContainsString('"allow_msg_schedules":true', $transport->writes[3]);
    }

    /**
     * Verifies a version-gated config field rejected by an older server surfaces as a typed
     * UnsupportedFeatureException carrying the feature, required version, and the server's reported
     * version (from the INFO handshake) - without any per-request version probe.
     */
    public function testUnsupportedFeatureRaisesTypedExceptionWithServerVersion(): void
    {
        $errorPayload = '{"error":{"code":400,"description":"invalid JSON: json: unknown field \"allow_atomic\""}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.5","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->createStream('S', ['s.>'], ['allow_atomic' => true])->await();
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_atomic', $e->feature);
            self::assertSame('2.12', $e->requiredVersion);
            self::assertSame('2.10.5', $e->serverVersion);
            self::assertSame(400, $e->getCode());
            // Still catchable as a JetStreamException (subclass).
            self::assertInstanceOf(JetStreamException::class, $e);
        }
    }

    /**
     * Verifies scheduled publish sends scheduler headers through HPUB request.
     */
    public function testPublishScheduled(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":7,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $when = new DateTimeImmutable('2030-01-01 00:00:00', new DateTimeZone('UTC'));

        $ack = $client->jetStream()->publishScheduled(
            'schedules.orders.one',
            'events.orders',
            '{"event":"scheduled"}',
            Schedule::at($when),
            '5m',
        )->await();

        self::assertSame('SCHED', $ack->stream);
        self::assertSame(7, $ack->seq);
        self::assertStringStartsWith('HPUB schedules.orders.one _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule:@at 2030-01-01T00:00:00Z', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Target:events.orders', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-TTL:5m', $transport->writes[3]);
    }

    /**
     * Verifies malformed schedule expressions are rejected before request dispatch.
     */
    public function testPublishScheduledRejectsUnsupportedPattern(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Unsupported schedule expression');

        try {
            $client->jetStream()->publishScheduled(
                'schedules.orders.one',
                'events.orders',
                '{"event":"scheduled"}',
                'not-a-schedule',
            )->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a recurring "@every" schedule emits the scheduler headers, including the optional
     * source and rollup headers.
     */
    public function testPublishScheduledEveryWithSourceAndRollup(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":9,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->publishScheduled(
            'schedules.heartbeat',
            'events.heartbeat',
            '{"event":"tick"}',
            Schedule::every('1h'),
            source: 'cluster-a',
            rollup: true,
        )->await();

        self::assertStringContainsString('Nats-Schedule:@every 1h', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Target:events.heartbeat', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Source:cluster-a', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Rollup:sub', $transport->writes[3]);
        self::assertStringNotContainsString('Nats-Schedule-Time-Zone', $transport->writes[3]);
    }

    /**
     * Verifies a cron schedule emits the cron expression plus the time-zone header.
     */
    public function testPublishScheduledCronWithTimeZone(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":10,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->publishScheduled(
            'schedules.report',
            'events.report',
            '{"event":"daily"}',
            Schedule::cron('0 0 0 * * *'),
            timeZone: 'Europe/Warsaw',
        )->await();

        self::assertStringContainsString('Nats-Schedule:0 0 0 * * *', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Time-Zone:Europe/Warsaw', $transport->writes[3]);
    }

    /**
     * Verifies a predefined alias schedule (ADR-51) reaches the wire and may carry a time zone.
     */
    public function testPublishScheduledPredefinedAlias(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":11,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->publishScheduled(
            'schedules.report',
            'events.report',
            '{"event":"daily"}',
            Schedule::predefined('daily'),
            timeZone: 'Europe/Warsaw',
        )->await();

        self::assertStringContainsString('Nats-Schedule:@daily', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Time-Zone:Europe/Warsaw', $transport->writes[3]);
    }

    /**
     * Verifies an @at schedule with a numeric RFC3339 offset (not just "Z") reaches the wire.
     */
    public function testPublishScheduledAtWithTimezoneOffset(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":12,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->publishScheduled(
            'schedules.orders.one',
            'events.orders',
            '{"event":"scheduled"}',
            '@at 2030-01-01T02:00:00+02:00',
        )->await();

        self::assertStringContainsString('Nats-Schedule:@at 2030-01-01T02:00:00+02:00', $transport->writes[3]);
    }

    /**
     * Verifies a time zone supplied with a non-cron schedule is rejected before dispatch.
     */
    public function testPublishScheduledRejectsTimeZoneForNonCron(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Nats-Schedule-Time-Zone is only valid for cron');

        try {
            $client->jetStream()->publishScheduled(
                'schedules.heartbeat',
                'events.heartbeat',
                '{"event":"tick"}',
                Schedule::every('1h'),
                timeZone: 'Europe/Warsaw',
            )->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies publish with a de-duplication id emits the Nats-Msg-Id header (issue #11).
     */
    public function testPublishWithMsgId(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":43,"duplicate":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->publish('orders.created', '{"id":1}', msgId: 'order-1')->await();

        self::assertTrue($ack->duplicate);
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Msg-Id:order-1', $transport->writes[3]);
    }

    /**
     * Verifies publish emits optimistic-concurrency expectation headers (#16).
     */
    public function testPublishWithExpectationHeaders(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":43,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->publish(
            'orders.created',
            '{"id":1}',
            expectedStream: 'ORDERS',
            expectedLastSequence: 42,
            expectedLastSubjectSequence: 0,
            expectedLastMsgId: 'order-41',
        )->await();

        $hpub = $transport->writes[3];
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $hpub);
        self::assertStringContainsString('Nats-Expected-Stream:ORDERS', $hpub);
        self::assertStringContainsString('Nats-Expected-Last-Sequence:42', $hpub);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:0', $hpub);
        self::assertStringContainsString('Nats-Expected-Last-Msg-Id:order-41', $hpub);
    }

    /**
     * Verifies a precondition mismatch (error ack) surfaces as a JetStreamException and is NOT retried (#16).
     */
    public function testPublishExpectationMismatchThrows(): void
    {
        $errorAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorAck), $errorAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('wrong last sequence');
        $client->jetStream()->publish('orders.created', '{"id":1}', expectedLastSequence: 99)->await();
    }

    /**
     * Verifies a publish that hits a transient no-responders (503) is retried and then succeeds (#29).
     */
    public function testPublishRetriesOnNoResponders(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $ackPayload = '{"stream":"ORDERS","seq":50,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // First publish request -> 503 no-responders on inbox sid 1.
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            // Retry publish request -> success ack on inbox sid 2.
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        // Tight retry wait so the test is fast.
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $js = new JetStreamContext($client, publishRetryAttempts: 3, publishRetryWaitMs: 1);

        $ack = $js->publish('orders.created', '{"id":1}')->await();

        self::assertSame(50, $ack->seq);
    }

    /**
     * Verifies ackSync sends +ACK as a request and resolves on the server confirmation (#18).
     */
    public function testAckSyncSendsAckAsRequestAndAwaitsConfirmation(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Empty confirmation reply on the double-ack inbox (sid 1).
            "MSG _INBOX.any 1 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $delivered = new NatsMessage('events.x', 9, '$JS.ACK.ORDERS.c1.1.5.5.0.0', 'body');
        $client->jetStream()->ackSync($delivered, 100)->await();

        // The +ACK travelled as a request: a SUB on a fresh inbox then a PUB carrying the reply inbox.
        self::assertStringStartsWith('SUB _INBOX.', $transport->writes[2]);
        self::assertStringStartsWith('PUB $JS.ACK.ORDERS.c1.1.5.5.0.0 _INBOX.', $transport->writes[3]);
        self::assertStringEndsWith("\r\n+ACK\r\n", $transport->writes[3]);
    }

    /**
     * Verifies deleteMessage issues a fast (no_erase) delete by default and a secure erase on request (#20).
     */
    public function testDeleteMessageFastAndSecure(): void
    {
        $ok = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ok), $ok),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ok), $ok),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $js = $client->jetStream();

        self::assertTrue($js->deleteMessage('ORDERS', 7)->await());
        self::assertTrue($js->deleteMessage('ORDERS', 8, secureErase: true)->await());

        self::assertStringContainsString('$JS.API.STREAM.MSG.DELETE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"no_erase":true', $transport->writes[3]);
        self::assertStringContainsString('"seq":7', $transport->writes[3]);
        // Secure erase omits no_erase so the server overwrites the data (second request's PUB).
        self::assertStringNotContainsString('no_erase', $transport->writes[4]);
        self::assertStringContainsString('"seq":8', $transport->writes[4]);
    }

    /**
     * Verifies messageMetadata parses the full $JS.ACK tuple, incl. domain form (#30).
     */
    public function testMessageMetadataParsesAckTuple(): void
    {
        $client = new NatsClient(new NatsOptions());
        $js = $client->jetStream();

        // 9-token form: $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        $short = new NatsMessage('events.x', 1, '$JS.ACK.ORDERS.worker.3.42.40.1700000000000000000.7', 'body');
        $meta = $js->messageMetadata($short);
        self::assertSame('ORDERS', $meta->stream);
        self::assertSame('worker', $meta->consumer);
        self::assertSame(3, $meta->numDelivered);
        self::assertSame(42, $meta->streamSequence);
        self::assertSame(40, $meta->consumerSequence);
        self::assertSame(7, $meta->numPending);
        self::assertNull($meta->domain);
        self::assertSame(1700000000000000000, $meta->timestampNanos);

        // Domain-qualified (11-token) form.
        $domainMsg = new NatsMessage('events.x', 1, '$JS.ACK.hub.ACCT.ORDERS.worker.2.99.50.1700000000000000000.4', 'body');
        $dmeta = $js->messageMetadata($domainMsg);
        self::assertSame('hub', $dmeta->domain);
        self::assertSame('ORDERS', $dmeta->stream);
        self::assertSame(99, $dmeta->streamSequence);
        self::assertSame(4, $dmeta->numPending);
    }

    /**
     * Verifies messageMetadata rejects a non-JetStream message (#30).
     */
    public function testMessageMetadataThrowsForNonJetStreamMessage(): void
    {
        $client = new NatsClient(new NatsOptions());
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('not a JetStream delivery');
        $client->jetStream()->messageMetadata(new NatsMessage('events.x', 1, '_INBOX.plain', 'body'));
    }

    /**
     * Verifies publish with an integer TTL emits Nats-TTL in seconds (issue #4).
     */
    public function testPublishWithTtlSeconds(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":44,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->publish('orders.created', '{"id":1}', ttl: 30)->await();

        self::assertStringStartsWith('HPUB orders.created _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-TTL:30s', $transport->writes[3]);
    }

    /**
     * Verifies the "never" TTL passes through unchanged.
     */
    public function testPublishWithTtlNever(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":45,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->publish('orders.created', '{"id":1}', ttl: 'never')->await();

        self::assertStringContainsString('Nats-TTL:never', $transport->writes[3]);
    }

    /**
     * Verifies an invalid (sub-second / zero) TTL is rejected before dispatch.
     */
    public function testPublishRejectsZeroTtl(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Per-message TTL must be at least 1 second');

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}', ttl: 0)->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies an empty Nats-Msg-Id is rejected before dispatch.
     */
    public function testPublishRejectsEmptyMsgId(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Nats-Msg-Id must not be empty');

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}', msgId: '')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies incrementCounter emits Nats-Incr and returns the new value (issue #9).
     */
    public function testIncrementCounter(): void
    {
        $ackPayload = '{"stream":"COUNTERS","seq":1,"val":"5"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->incrementCounter('counters.visits', '+5')->await();

        self::assertSame('5', $value);
        self::assertStringStartsWith('HPUB counters.visits _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Incr:+5', $transport->writes[3]);
    }

    /**
     * Verifies a counter value beyond PHP_INT_MAX is preserved as an exact string.
     */
    public function testIncrementCounterPreservesBigValue(): void
    {
        // Unquoted JSON number beyond PHP_INT_MAX: only JSON_BIGINT_AS_STRING preserves it exactly,
        // so this payload makes that flag load-bearing (a quoted string would pass regardless).
        $ackPayload = '{"stream":"COUNTERS","seq":2,"val":99999999999999999999}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->incrementCounter('counters.visits', '+1')->await();

        self::assertSame('99999999999999999999', $value);
    }

    /**
     * Verifies a malformed counter delta is rejected before dispatch.
     */
    public function testIncrementCounterRejectsMalformedDelta(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Counter increment must be an integer string');

        try {
            $client->jetStream()->incrementCounter('counters.visits', '5x')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies counterValue reads the latest value via Direct Get (issue #9).
     */
    public function testCounterValue(): void
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: COUNTERS\r\nNats-Subject: counters.visits\r\nNats-Sequence: 7\r\n\r\n";
        $body = '{"val":"42"}';
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("HMSG _INBOX.a 1 %d %d\r\n%s%s\r\n", $h, $h + strlen($body), $hdrs, $body),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->counterValue('COUNTERS', 'counters.visits')->await();

        self::assertSame('42', $value);
        self::assertStringStartsWith('PUB $JS.API.DIRECT.GET.COUNTERS _INBOX.', $transport->writes[3]);
    }

    /**
     * Verifies counterValue returns "0" for a counter with no stored message.
     */
    public function testCounterValueMissingReturnsZero(): void
    {
        $hdrs = "NATS/1.0 404 Message Not Found\r\n\r\n";
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("HMSG _INBOX.a 1 %d %d\r\n%s\r\n", $h, $h, $hdrs),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->counterValue('COUNTERS', 'counters.visits')->await();

        self::assertSame('0', $value);
    }

    /**
     * Verifies schedule publish omits TTL header when optional value is null.
     */
    public function testPublishScheduledOmitsTtlWhenNotProvided(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":8,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $when = new DateTimeImmutable('2030-01-01 00:00:00', new DateTimeZone('UTC'));

        $client->jetStream()->publishScheduled(
            'schedules.orders.one',
            'events.orders',
            '{"event":"scheduled"}',
            Schedule::at($when),
            null,
        )->await();

        self::assertStringNotContainsString('Nats-Schedule-TTL', $transport->writes[3]);
    }

    /**
     * Verifies schedule publish maps error payloads to JetStreamException.
     */
    public function testPublishScheduledMapsApiError(): void
    {
        $errorPayload = '{"error":{"code":503,"description":"scheduler down"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $when = new DateTimeImmutable('2030-01-01 00:00:00', new DateTimeZone('UTC'));

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('scheduler down');

        $client->jetStream()->publishScheduled(
            'schedules.orders.one',
            'events.orders',
            '{"event":"scheduled"}',
            Schedule::at($when),
        )->await();
    }

    /**
     * Verifies pull consumer fetch uses MSG.NEXT endpoint and returns message payload.
     */
    public function testFetchNext(): void
    {
        $deliveryPayload = '{"event":"created"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($deliveryPayload), $deliveryPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->fetchNext('ORDERS', 'PROC', 2500)->await();

        self::assertSame('{"event":"created"}', $message->payload);
        self::assertStringStartsWith('PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.PROC _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('"expires":2500000000', $transport->writes[3]);
    }

    /**
     * Verifies pull fetch rejects invalid expiration values.
     */
    public function testFetchNextRejectsInvalidExpiresMs(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Pull fetch expiresMs must be greater than zero');

        $client->jetStream()->fetchNext('ORDERS', 'PROC', 0)->await();
    }

    /**
     * Verifies ACK helpers publish expected protocol tokens to reply subject.
     */
    public function testAckHelpersPublishProtocolTokens(): void
    {
        $deliveryPayload = '{"event":"created"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 reply.ack %d\r\n%s\r\n", strlen($deliveryPayload), $deliveryPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->request('$JS.API.CONSUMER.MSG.NEXT.ORDERS.PROC', '{}')->await();

        $js = $client->jetStream();
        $js->ack($message)->await();
        $js->nak($message)->await();
        $js->nakWithDelay($message, 1500)->await();
        $js->term($message)->await();
        $js->inProgress($message)->await();

        $ackWrites = array_slice($transport->writes, -5);

        self::assertCount(5, $ackWrites);
        self::assertStringStartsWith('PUB reply.ack 4', $ackWrites[0]);
        self::assertStringStartsWith('PUB reply.ack 4', $ackWrites[1]);
        self::assertStringStartsWith('PUB reply.ack ', $ackWrites[2]);
        self::assertStringStartsWith('PUB reply.ack 5', $ackWrites[3]);
        self::assertStringStartsWith('PUB reply.ack 4', $ackWrites[4]);
        self::assertStringContainsString("\r\n+ACK\r\n", $ackWrites[0]);
        self::assertStringContainsString("\r\n-NAK\r\n", $ackWrites[1]);
        self::assertStringContainsString("\r\n-NAK {\"delay\":1500000000}\r\n", $ackWrites[2]);
        self::assertStringContainsString("\r\n+TERM\r\n", $ackWrites[3]);
        self::assertStringContainsString("\r\n+WPI\r\n", $ackWrites[4]);
    }

    /**
     * Verifies delayed NAK rejects invalid delay values.
     */
    public function testNakWithDelayRejectsInvalidDelay(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream delayed NAK requires delayMs greater than zero');

        $message = new \IDCT\NATS\Core\NatsMessage('orders.created', 1, 'reply.ack', '{"event":"created"}');
        $client->jetStream()->nakWithDelay($message, 0)->await();
    }

    /**
     * Verifies ACK helpers fail fast for messages without reply subject.
     */
    public function testAckRequiresReplySubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream ACK requires a reply subject on the delivered message');

        $message = new \IDCT\NATS\Core\NatsMessage('orders.created', 1, null, '{"event":"created"}');
        $client->jetStream()->ack($message)->await();
    }

    /**
     * Verifies push consumer creation sets deliver subject in consumer config.
     */
    public function testCreatePushConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $created = $client->jetStream()->createPushConsumer('ORDERS', 'PROC', 'deliver.proc', 'orders.*')->await();

        self::assertSame('PROC', $created->name);
        self::assertTrue($created->push);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS.PROC', $transport->writes[3]);
        self::assertStringContainsString('"ack_policy":"explicit"', $transport->writes[3]);
        self::assertStringContainsString('"deliver_subject":"deliver.proc"', $transport->writes[3]);
    }

    /**
     * Verifies explicit ephemeral push consumer creation omits durable_name in payload.
     */
    public function testCreateEphemeralPushConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"EP1","config":{"deliver_subject":"deliver.ep"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $consumer = $client->jetStream()->createEphemeralPushConsumer('ORDERS', 'deliver.ep', 'orders.*')->await();

        self::assertSame('EP1', $consumer->name);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"deliver_subject":"deliver.ep"', $transport->writes[3]);
        self::assertStringNotContainsString('"durable_name"', $transport->writes[3]);
    }

    /**
     * Verifies push subscription auto-responds to flow-control and forwards payload deliveries.
     */
    public function testSubscribePushConsumerHandlesFlowControl(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc"}}';
        $flowHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'FlowControl Request',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply travels through the mux inbox (#118); the deliver-subject frames (sid 2, the sub
        // after the mux sid 1) must arrive only once the deliver subscription exists, so enqueue them on
        // its SUB rather than pre-seeding them (a pre-seeded frame would be drained+dropped by CREATE).
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.proc '] = [
            sprintf(
                "HMSG deliver.proc 2 fc.reply %d %d\r\n%s\r\n",
                strlen($flowHeaders),
                strlen($flowHeaders),
                $flowHeaders,
            ),
            "MSG deliver.proc 2 5\r\nhello\r\n",
        ];

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = null;
        $client->jetStream()->subscribePushConsumer(
            'ORDERS',
            'PROC',
            static function (\IDCT\NATS\Core\NatsMessage $message) use (&$received): void {
                $received = $message;
            },
            'deliver.proc',
            'orders.*',
        )->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        self::assertStringContainsString("PUB fc.reply 0\r\n\r\n", implode('', $transport->writes));
        self::assertInstanceOf(\IDCT\NATS\Core\NatsMessage::class, $received);
        self::assertSame('hello', $received->payload);
    }

    /**
     * Verifies a stalled idle heartbeat is answered on the Nats-Consumer-Stalled subject (not the
     * empty message reply), so the server's flow-control stall is cleared instead of hanging.
     */
    public function testSubscribePushConsumerAnswersStalledHeartbeat(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc"}}';
        // Status-100 heartbeat with NO message reply; the FC reply subject is in the header value.
        $stalledHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Consumer-Stalled' => 'stall.reply',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); the deliver frame (sid 2) arrives after its SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.proc '] = [
            sprintf(
                "HMSG deliver.proc 2 %d %d\r\n%s\r\n", // no reply subject
                strlen($stalledHeaders),
                strlen($stalledHeaders),
                $stalledHeaders,
            ),
        ];

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $handled = false;
        $client->jetStream()->subscribePushConsumer(
            'ORDERS',
            'PROC',
            static function () use (&$handled): void {
                $handled = true;
            },
            'deliver.proc',
            'orders.*',
        )->await();

        $client->processIncoming()->await();

        self::assertFalse($handled); // not a user payload delivery
        self::assertStringContainsString("PUB stall.reply 0\r\n\r\n", implode('', $transport->writes));
    }

    /**
     * Verifies heartbeat control messages are ignored and not forwarded to user handlers.
     */
    public function testSubscribePushConsumerIgnoresHeartbeat(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc"}}';
        $heartbeatHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); the deliver frame (sid 2) arrives after its SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.proc '] = [
            sprintf(
                "HMSG deliver.proc 2 hb.reply %d %d\r\n%s\r\n",
                strlen($heartbeatHeaders),
                strlen($heartbeatHeaders),
                $heartbeatHeaders,
            ),
        ];

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $handled = false;
        $client->jetStream()->subscribePushConsumer(
            'ORDERS',
            'PROC',
            static function () use (&$handled): void {
                $handled = true;
            },
            'deliver.proc',
            'orders.*',
        )->await();

        $client->processIncoming()->await();

        self::assertFalse($handled);
        self::assertStringNotContainsString('PUB hb.reply 0', implode('', $transport->writes));
    }

    /**
     * Verifies ephemeral pull consumer creation uses stream-level create endpoint.
     */
    public function testCreateEphemeralConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"E1","config":{"ack_policy":"explicit"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $consumer = $client->jetStream()->createEphemeralConsumer('ORDERS', 'orders.*')->await();

        self::assertSame('E1', $consumer->name);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS', $transport->writes[3]);
        self::assertStringNotContainsString('$JS.API.CONSUMER.CREATE.ORDERS.', $transport->writes[3]);
        self::assertStringContainsString('"ack_policy":"explicit"', $transport->writes[3]);
        self::assertStringContainsString('"filter_subject":"orders.*"', $transport->writes[3]);
        self::assertStringNotContainsString('"durable_name"', $transport->writes[3]);
    }

    /**
     * Verifies ephemeral push subscription helper creates consumer and receives payload.
     */
    public function testSubscribeEphemeralPushConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"E_PUSH","config":{"deliver_subject":"deliver.ephemeral"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); the deliver frame (sid 2) arrives after its SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.ephemeral '] = [
            "MSG deliver.ephemeral 2 5\r\nhello\r\n",
        ];

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = null;
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (\IDCT\NATS\Core\NatsMessage $message) use (&$received): void {
                $received = $message;
            },
            'deliver.ephemeral',
            'orders.*',
        )->await();

        $client->processIncoming()->await();

        self::assertInstanceOf(\IDCT\NATS\Core\NatsMessage::class, $received);
        self::assertSame('hello', $received->payload);
        self::assertStringContainsString('"deliver_subject":"deliver.ephemeral"', $transport->writes[3]);
        self::assertStringNotContainsString('"durable_name"', $transport->writes[3]);
    }

    // ─── Input Validation ─────────────────────────────────────────────

    public function testCreateStreamRejectsEmptySubjects(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Stream subjects must not be empty unless mirror or sources configuration is provided');
        $client->jetStream()->createStream('test', [])->await();
    }

    public function testCreateStreamAllowsSourcesWithoutSubjects(): void
    {
        // A pure aggregate stream ingests only from sources and legitimately has no subjects of its own;
        // createStream() must not reject empty subjects when a non-empty sources config is provided.
        $streamPayload = '{"config":{"name":"AGG","subjects":[],"sources":[{"name":"ORDERS"},{"name":"PAYMENTS"}]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamPayload), $streamPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $created = $client->jetStream()->createStream('AGG', [], [
            'sources' => [
                StreamSource::source('ORDERS')->toArray(),
                StreamSource::source('PAYMENTS')->toArray(),
            ],
        ])->await();

        self::assertSame('AGG', $created->name);
        self::assertSame([], $created->subjects);
        self::assertStringContainsString('"sources":[', $transport->writes[3]);
        self::assertStringContainsString('"subjects":[]', $transport->writes[3]);
    }

    public function testCreateStreamAllowsMirrorWithoutSubjects(): void
    {
        $streamPayload = '{"config":{"name":"MIRROR","subjects":[],"mirror":{"name":"ORIGIN"}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamPayload), $streamPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $created = $client->jetStream()->createStream('MIRROR', [], [
            'mirror' => StreamSource::mirror('ORIGIN')->toArray(),
        ])->await();

        self::assertSame('MIRROR', $created->name);
        self::assertSame([], $created->subjects);
        self::assertStringContainsString('"mirror":{"name":"ORIGIN"}', $transport->writes[3]);
        self::assertStringContainsString('"subjects":[]', $transport->writes[3]);
    }

    public function testCreateConsumerRejectsEmptyFilterSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Consumer filter subject must not be empty');
        $client->jetStream()->createConsumer('ORDERS', 'c1', '')->await();
    }

    public function testRequestJsonWrapsJsonException(): void
    {
        $malformedPayload = 'NOT_JSON{';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($malformedPayload), $malformedPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed JetStream API response');
        $client->jetStream()->accountInfo()->await();
    }

    // ─── Phase 3: Feature Gaps ────────────────────────────────────────

    public function testUpdateStream(): void
    {
        $responsePayload = '{"config":{"name":"ORDERS","subjects":["orders.>","events.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($responsePayload), $responsePayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $updated = $client->jetStream()->updateStream('ORDERS', [
            'subjects' => ['orders.>', 'events.>'],
        ])->await();

        self::assertSame('ORDERS', $updated->name);
        self::assertSame(['orders.>', 'events.>'], $updated->subjects);
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[3]);
    }

    public function testCreateConsumerWithOptions(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","max_deliver":5}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $consumer = $client->jetStream()->createConsumer('ORDERS', 'PROC', 'orders.*', [
            'ack_policy' => 'all',
            'max_deliver' => 5,
            'ack_wait' => 30_000_000_000,
            'max_ack_pending' => 100,
        ])->await();

        self::assertSame('PROC', $consumer->name);
        $written = $transport->writes[3];
        self::assertStringContainsString('"ack_policy":"all"', $written);
        self::assertStringContainsString('"max_deliver":5', $written);
        self::assertStringContainsString('"ack_wait":30000000000', $written);
        self::assertStringContainsString('"max_ack_pending":100', $written);
        self::assertStringContainsString('"filter_subject":"orders.*"', $written);
    }

    public function testCreateConsumerDefaultsAckPolicyToExplicit(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","ack_policy":"explicit"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // No ack_policy passed: the durable createConsumer() path must default it to explicit.
        $client->jetStream()->createConsumer('ORDERS', 'PROC', 'orders.created')->await();

        self::assertStringContainsString('"ack_policy":"explicit"', $transport->writes[3]);
    }

    public function testCreatePushConsumerAllowsAckPolicyOverride(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->createPushConsumer('ORDERS', 'PROC', 'deliver.proc', 'orders.*', [
            'ack_policy' => 'none',
        ])->await();

        self::assertStringContainsString('"ack_policy":"none"', $transport->writes[3]);
    }

    public function testFetchBatch(): void
    {
        $msg1 = '{"event":"first"}';
        $msg2 = '{"event":"second"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg2), $msg2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 2, 2500)->await();

        self::assertCount(2, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
        self::assertSame('{"event":"second"}', $messages[1]->payload);

        $written = $transport->writes[3];
        self::assertStringContainsString('"batch":2', $written);
        self::assertStringContainsString('"expires":2500000000', $written);
    }

    public function testFetchBatchRejectsInvalidBatch(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Pull fetch batch must be greater than zero');
        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 0)->await();
    }

    public function testFetchBatchIgnoresTerminalStatusFrames(): void
    {
        $msg1 = '{"event":"first"}';
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\nDescription: No Messages\r\n\r\n";
        $headerBytes = strlen($statusHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 2, 2500)->await();

        self::assertCount(1, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
    }

    /**
     * Verifies a mid-batch terminal status (after >=1 message) is surfaced to the optional
     * onTerminalStatus callback while the partial batch is still returned (#92).
     */
    public function testFetchBatchSurfacesMidBatchTerminalStatusToCallback(): void
    {
        $msg1 = '{"event":"first"}';
        // A non-routine mid-batch termination: the consumer was deleted (409).
        $statusHeaders = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\nDescription: Consumer Deleted\r\n\r\n";
        $headerBytes = strlen($statusHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $observed = null;
        $messages = $client->jetStream()->fetchBatch(
            'ORDERS',
            'PROC',
            5,
            2500,
            [],
            static function (int $code, string $description) use (&$observed): void {
                $observed = ['code' => $code, 'description' => $description];
            },
        )->await();

        // The partial batch is still returned (nats.go parity)...
        self::assertCount(1, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
        // ...and the mid-batch terminal status was surfaced to the callback.
        self::assertSame(['code' => 409, 'description' => 'Consumer Deleted'], $observed);
    }

    public function testFetchBatchIgnoresStatus100ControlFrames(): void
    {
        $msg1 = '{"event":"first"}';
        $controlHeaders = "NATS/1.0 100 Idle Heartbeat\r\nStatus: 100\r\nDescription: Idle Heartbeat\r\n\r\n";
        $headerBytes = strlen($controlHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $controlHeaders),
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500)->await();

        self::assertCount(1, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
    }

    public function testFetchBatchThrowsWhenNoMessagesArrive(): void
    {
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\nDescription: No Messages\r\n\r\n";
        $headerBytes = strlen($statusHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream pull request ended with status 404: No Messages');

        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500)->await();
    }

    public function testFetchBatchThrowsTerminalStatusDescription(): void
    {
        $statusHeaders = "NATS/1.0 409 MaxAckPending Exceeded\r\nStatus: 409\r\nDescription: MaxAckPending Exceeded\r\n\r\n";
        $headerBytes = strlen($statusHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500)->await();
            self::fail('Expected terminal pull status to raise JetStreamException.');
        } catch (JetStreamException $e) {
            self::assertSame(409, $e->getCode());
            self::assertStringContainsString('status 409: MaxAckPending Exceeded', $e->getMessage());
        }
    }

    // ─── Consumer Pause/Resume ──────────────────────────────────────────

    public function testPauseConsumerSendsCorrectPayload(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse('{"paused":true,"pause_until":"2026-12-01T00:00:00Z"}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->pauseConsumer('ORDERS', 'PROC', '2026-12-01T00:00:00Z')->await();

        self::assertTrue($result['paused'] ?? false);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.PAUSE.ORDERS.PROC', $written);
        self::assertStringContainsString('"pause_until":"2026-12-01T00:00:00Z"', $written);
    }

    public function testResumeConsumerSendsEmptyBody(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse('{"paused":false}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->resumeConsumer('ORDERS', 'PROC')->await();

        self::assertFalse($result['paused'] ?? true);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.PAUSE.ORDERS.PROC', $written);
    }

    // ─── Ordered Consumer ───────────────────────────────────────────────

    public function testSubscribeOrderedConsumerSendsCorrectConfig(): void
    {
        $consumerCreateResponse = json_encode([
            'stream_name' => 'ORDERS',
            'name' => 'ephemeral_ordered',
            'config' => [
                'ack_policy' => 'none',
                'flow_control' => true,
                'idle_heartbeat' => 5000000000,
                'mem_storage' => true,
                'max_deliver' => 1,
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse($consumerCreateResponse),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->subscribeOrderedConsumer('ORDERS', function (NatsMessage $msg): void {})->await();

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS', $written);
        self::assertStringContainsString('"flow_control":true', $written);
        self::assertStringContainsString('"idle_heartbeat":5000000000', $written);
        self::assertStringContainsString('"ack_policy":"none"', $written);
        self::assertStringContainsString('"mem_storage":true', $written);
        // ADR-17 / nats.go ordered.go parity: ordered consumers pin R1 (#132).
        self::assertStringContainsString('"num_replicas":1', $written);
    }

    // ─── Stream Purge / List / Consumer List / Direct Get ────────────────

    public function testPurgeStream(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse('{"purged":42}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->purgeStream('ORDERS')->await();

        self::assertSame(42, $result['purged']);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.ORDERS', implode('', $transport->writes));
    }

    public function testPurgeStreamWithSubjectFilter(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse('{"purged":10}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->purgeStream('ORDERS', ['filter' => 'orders.old'])->await();

        self::assertSame(10, $result['purged']);
        self::assertStringContainsString('"filter":"orders.old"', implode('', $transport->writes));
    }

    public function testListStreams(): void
    {
        $listPayload = json_encode([
            'streams' => [
                ['config' => ['name' => 'ORDERS', 'subjects' => ['orders.>']]],
                ['config' => ['name' => 'EVENTS', 'subjects' => ['events.>']]],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse($listPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $streams = $client->jetStream()->listStreams()->await();

        self::assertCount(2, $streams);
        self::assertSame('ORDERS', $streams[0]->name);
        self::assertSame('EVENTS', $streams[1]->name);
        self::assertStringContainsString('$JS.API.STREAM.LIST', implode('', $transport->writes));
    }

    public function testListStreamsWithSubjectFilter(): void
    {
        $listPayload = json_encode([
            'streams' => [
                ['config' => ['name' => 'ORDERS', 'subjects' => ['orders.>']]],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse($listPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $streams = $client->jetStream()->listStreams(['subject' => 'orders.>'])->await();

        self::assertCount(1, $streams);
        self::assertSame('ORDERS', $streams[0]->name);
        self::assertStringContainsString('"subject":"orders.>"', implode('', $transport->writes));
    }

    public function testListConsumers(): void
    {
        $listPayload = json_encode([
            'consumers' => [
                ['stream_name' => 'ORDERS', 'name' => 'A', 'config' => ['durable_name' => 'A']],
                ['stream_name' => 'ORDERS', 'name' => 'B', 'config' => ['durable_name' => 'B', 'deliver_subject' => 'push']],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse($listPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $consumers = $client->jetStream()->listConsumers('ORDERS')->await();

        self::assertCount(2, $consumers);
        self::assertSame('A', $consumers[0]->name);
        self::assertFalse($consumers[0]->push);
        self::assertSame('B', $consumers[1]->name);
        self::assertTrue($consumers[1]->push);
        self::assertStringContainsString('$JS.API.CONSUMER.LIST.ORDERS', implode('', $transport->writes));
    }

    public function testListStreamsPaginatesAcrossPages(): void
    {
        $page1 = json_encode([
            'total' => 3,
            'offset' => 0,
            'streams' => [
                ['config' => ['name' => 'S1', 'subjects' => ['a.>']]],
                ['config' => ['name' => 'S2', 'subjects' => ['b.>']]],
            ],
        ], JSON_THROW_ON_ERROR);
        $page2 = json_encode([
            'total' => 3,
            'offset' => 2,
            'streams' => [
                ['config' => ['name' => 'S3', 'subjects' => ['c.>']]],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $page1), (string) $page1),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen((string) $page2), (string) $page2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $streams = $client->jetStream()->listStreams()->await();

        // All three streams are returned across two pages (server page size would otherwise truncate).
        self::assertSame(['S1', 'S2', 'S3'], array_map(static fn(StreamInfo $s): string => $s->name, $streams));

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('"offset":0', $writes);
        self::assertStringContainsString('"offset":2', $writes);
    }

    public function testGetStreamMessage(): void
    {
        $msgPayload = json_encode([
            'message' => [
                'subject' => 'orders.created',
                'data' => base64_encode('{"id":1}'),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->jsOkResponse($msgPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getStreamMessage('ORDERS', 1)->await();

        self::assertSame('orders.created', $message->subject);
        self::assertSame('{"id":1}', $message->payload);
        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.MSG.GET.ORDERS', $written);
        self::assertStringContainsString('"seq":1', $written);
    }

    public function testExtractStreamSequenceParsesReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'extractStreamSequence');

        $message = new NatsMessage('s', 1, '$JS.ACK.ORDERS.CONS.1.42.2.123.0', 'x');
        $parsed = $method->invoke($js, $message);

        self::assertSame(42, $parsed);
    }

    public function testExtractStreamSequenceParsesDomainQualifiedReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'extractStreamSequence');

        // Domain-qualified ACK subject (12 tokens): stream sequence sits at index 7.
        $message = new NatsMessage('s', 1, '$JS.ACK.hub.ACC123.ORDERS.CONS.1.42.2.123.0.rnd', 'x');
        $parsed = $method->invoke($js, $message);

        self::assertSame(42, $parsed);
    }

    /**
     * A 13-token ACK subject (a future server form with tokens beyond the known 12) still yields
     * the stream sequence at index 7 - offsets anchor from the front, extras are ignored, matching
     * nats.go's tolerant parser (#155).
     */
    public function testExtractStreamSequenceParses13TokenReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'extractStreamSequence');

        $message = new NatsMessage('s', 1, '$JS.ACK.hub.ACC123.ORDERS.CONS.1.42.2.123.0.rnd.extra', 'x');

        self::assertSame(42, $method->invoke($js, $message));
    }

    public function testKeyValueRejectsInvalidBucketName(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(\IDCT\NATS\Exception\JetStreamException::class);
        $this->expectExceptionMessage('Invalid bucket name');
        // A dotted name would mis-scope $KV.<bucket>.> subjects.
        $client->jetStream()->keyValue('bad.bucket');
    }

    public function testObjectStoreRejectsInvalidBucketName(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(\IDCT\NATS\Exception\JetStreamException::class);
        $this->expectExceptionMessage('Invalid bucket name');
        $client->jetStream()->objectStore('bad/bucket');
    }

    public function testExtractSequencesParseElevenTokenDomainReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $streamMethod = new \ReflectionMethod($js, 'extractStreamSequence');

        // Domain-qualified ACK subject WITHOUT a trailing random token (11 tokens): stream sequence at
        // index 7. Previously fell through to null, silently disabling KV/ObjectStore revision on
        // JetStream-domain/leaf deployments. (Consumer-sequence parsing for every token form is now
        // covered by JsMessageMetadataTest, which the ordered consumer uses.)
        $message = new NatsMessage('s', 1, '$JS.ACK.hub.ACC123.ORDERS.CONS.1.42.7.123.0', 'x');

        self::assertSame(42, $streamMethod->invoke($js, $message));
    }

    public function testExtractStreamSequenceReturnsNullForInvalidReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'extractStreamSequence');

        $noReply = new NatsMessage('s', 1, null, 'x');
        $shortReply = new NatsMessage('s', 1, '$JS.ACK.short', 'x');
        $wrongPrefix = new NatsMessage('s', 1, '$JS.FC.ORDERS.token', 'x');
        $nonInt = new NatsMessage('s', 1, '$JS.ACK.ORDERS.CONS.1.NaN.2.123.0', 'x');

        self::assertNull($method->invoke($js, $noReply));
        self::assertNull($method->invoke($js, $shortReply));
        self::assertNull($method->invoke($js, $wrongPrefix));
        self::assertNull($method->invoke($js, $nonInt));
    }

    /**
     * A data message carrying user headers but NO status line (Status 0) is real payload and must be
     * delivered (returns false). A non-100 status frame (e.g. 404/409/503) is a JetStream control/error
     * frame, never user data, and must be intercepted so it is not forwarded to the handler (#121).
     */
    public function testHandlePushControlMessageInterceptsNon100StatusButNotDataMessages(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'handlePushControlMessage');

        // A headered data message with no Status line -> delivered as data.
        $dataHeaders = NatsHeaders::toWireBlock(['My-Header' => 'value']);
        $dataMessage = new NatsMessage('deliver', 1, null, 'body', $dataHeaders);
        self::assertFalse($method->invoke($js, $dataMessage));

        // A non-100 status frame -> intercepted (withheld from the handler).
        $statusHeaders = NatsHeaders::toWireBlock([
            'Status' => '404',
            'Description' => 'No Messages',
        ]);
        $statusMessage = new NatsMessage('deliver', 1, null, '', $statusHeaders);
        self::assertTrue($method->invoke($js, $statusMessage));
    }

    public function testHandlePushControlMessageHeartbeatWithoutReplyReturnsTrue(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'handlePushControlMessage');

        $headers = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
        ]);
        $message = new NatsMessage('deliver', 1, null, '', $headers);

        self::assertTrue($method->invoke($js, $message));
    }

    public function testHandlePushControlMessageRepliesToJetStreamFlowControlSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'handlePushControlMessage');

        $headers = "NATS/1.0 100 Idle Heartbeat\r\nStatus: 100\r\nDescription: Idle Heartbeat\r\n\r\n";
        $message = new NatsMessage('deliver', 1, '$JS.FC.ORDERS.token', '', $headers);

        self::assertTrue($method->invoke($js, $message));
        self::assertStringContainsString('PUB $JS.FC.ORDERS.token 0' . "\r\n\r\n", implode('', $transport->writes));
    }

    /**
     * Verifies getStreamMessage() preserves a falsy body such as "0" instead of dropping it.
     */
    public function testGetStreamMessagePreservesZeroPayload(): void
    {
        $apiResponse = json_encode([
            'message' => ['subject' => 'events.zero', 'seq' => 1, 'data' => base64_encode('0')],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($apiResponse), $apiResponse),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getStreamMessage('EVENTS', 1)->await();

        self::assertSame('0', $message->payload);
        self::assertSame('events.zero', $message->subject);
        self::assertNull($message->rawHeaders);
    }

    /**
     * Verifies getStreamMessage() decodes the stored header block onto rawHeaders.
     */
    public function testGetStreamMessageDecodesHeaders(): void
    {
        $headerBlock = "NATS/1.0\r\nX-Custom: present\r\n\r\n";
        $apiResponse = json_encode([
            'message' => [
                'subject' => 'events.hdr',
                'seq' => 2,
                'data' => base64_encode('body'),
                'hdrs' => base64_encode($headerBlock),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($apiResponse), $apiResponse),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getStreamMessage('EVENTS', 2)->await();

        self::assertSame('body', $message->payload);
        self::assertSame($headerBlock, $message->rawHeaders);
        self::assertSame('present', NatsHeaders::fromWireBlock($message->rawHeaders)['X-Custom'] ?? null);
    }

    /**
     * Verifies getStreamMessage() leaves rawHeaders null when no header block is stored.
     */
    public function testGetStreamMessageWithoutHeadersReturnsNullRawHeaders(): void
    {
        $apiResponse = json_encode([
            'message' => ['subject' => 'events.plain', 'seq' => 3, 'data' => base64_encode('hello')],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($apiResponse), $apiResponse),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getStreamMessage('EVENTS', 3)->await();

        self::assertSame('hello', $message->payload);
        self::assertNull($message->rawHeaders);
    }

    public function testDirectGetStreamMessageReturnsRawBodyAndHeaders(): void
    {
        $headerBlock = "NATS/1.0\r\nNats-Stream: EVENTS\r\nNats-Subject: events.order\r\nNats-Sequence: 2\r\nNats-Time-Stamp: 2024-01-01T00:00:00.000000000Z\r\n\r\n";
        $body = '{"id":1}';
        $hdrLen = strlen($headerBlock);
        $totalLen = $hdrLen + strlen($body);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("HMSG _INBOX.any 1 %d %d\r\n%s%s\r\n", $hdrLen, $totalLen, $headerBlock, $body),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->directGetStreamMessage('EVENTS', 2)->await();

        // The original subject travels in Nats-Subject; the body is the raw payload.
        self::assertSame('events.order', $message->subject);
        self::assertSame($body, $message->payload);
        self::assertSame('2', NatsHeaders::fromWireBlock($message->rawHeaders)['Nats-Sequence'] ?? null);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.DIRECT.GET.EVENTS', $written);
        self::assertStringContainsString('"seq":2', $written);
    }

    public function testDirectGetLastMessageForSubjectRequestsLastBySubj(): void
    {
        $headerBlock = "NATS/1.0\r\nNats-Stream: EVENTS\r\nNats-Subject: events.order\r\nNats-Sequence: 7\r\n\r\n";
        $body = 'last';
        $hdrLen = strlen($headerBlock);
        $totalLen = $hdrLen + strlen($body);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("HMSG _INBOX.any 1 %d %d\r\n%s%s\r\n", $hdrLen, $totalLen, $headerBlock, $body),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->directGetLastMessageForSubject('EVENTS', 'events.order')->await();

        self::assertSame('events.order', $message->subject);
        self::assertSame('last', $message->payload);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.DIRECT.GET.EVENTS', $written);
        self::assertStringContainsString('"last_by_subj":"events.order"', $written);
    }

    public function testDirectGetStreamMessageThrowsOnNotFound(): void
    {
        $statusBlock = "NATS/1.0 404 Message Not Found\r\n\r\n";
        $len = strlen($statusBlock);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("HMSG _INBOX.any 1 %d %d\r\n%s\r\n", $len, $len, $statusBlock),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Message Not Found');
        $client->jetStream()->directGetStreamMessage('EVENTS', 999)->await();
    }

    public function testSubscribeOrderedConsumerRecreatesOnSequenceGap(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Mux inbox (#118): CREATE/DELETE replies echo on the request's captured reply-to; the deliver
        // frames use the sid the server actually assigned to the deliver SUB (no per-request SUB shift).
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // In-order delivery: consumer seq 1 / stream seq 1 -> next expected consumer seq 2.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // A missed push: consumer seq jumps to 3 (expected 2). The consumer is recreated from
                    // the stream sequence after the last in-order message (1+1=2) and THIS is DISCARDED.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
                // The rotated (second) epoch delivers nothing here: the server-side replay from
                // opt_start_seq is exercised against a real server in JetStreamIntegrationTest.
            ],
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        // The out-of-order message that exposed the gap (seq 3) is DISCARDED, never delivered out of
        // order. (The in-order replay the server then produces from opt_start_seq is exercised
        // end-to-end against a real server in JetStreamIntegrationTest, since FakeTransport cannot
        // model server-side replay.)
        self::assertSame(['msg1'], $received);

        $written = implode('', $transport->writes);
        // Exactly one recreate: one DELETE of the original consumer ...
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        // ... and two CREATEs total (the initial consumer plus the single recreate) - no storm.
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
        // The recreate resumes from the expected (first missing) sequence, not the out-of-order one.
        self::assertStringContainsString('"opt_start_seq":2', $written);
    }

    /**
     * Recreates rotate the deliver sid, so the sid subscribeOrderedConsumer() returned goes stale
     * after the first rotation and a plain unsubscribe() then matches nothing - pre-fix the
     * consumer (and its watchdog) could never be stopped again. stopOrderedConsumer() resolves the
     * CURRENT sid through the shared state: after a gap-driven rotation from sid 2 to sid 3, a stop
     * by the ORIGINAL sid must unsubscribe sid 3 and delete the CURRENT server-side consumer.
     */
    public function testStopOrderedConsumerStopsAfterRecreateRotatedTheSid(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap (consumer seq 3, expected 2) -> recreate rotates the deliver sid.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $sid = $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, 'events.>')->await();
        self::assertSame(2, $sid);

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        // The rotation happened: the initial inbox (sid 2) was unsubscribed by the recreate.
        $written = implode('', $transport->writes);
        self::assertStringContainsString("UNSUB 2\r\n", $written, 'precondition: the recreate must have rotated away from sid 2');
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));

        // Stop by the ORIGINAL sid: must stop the CURRENT instance (sid 3), not no-op on the stale one.
        $client->jetStream()->stopOrderedConsumer($sid)->await();

        $written = implode('', $transport->writes);
        self::assertStringContainsString("UNSUB 3\r\n", $written, 'stop must unsubscribe the CURRENT rotated sid');
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD2'), 'stop must delete the CURRENT server-side consumer');
    }

    /**
     * ADR-9: an idle heartbeat's Nats-Last-Consumer reveals deliveries that never reached this
     * client (interest gap / local drops). Heartbeats keep flowing, so the #113 total-silence
     * watchdog never fires - and with ack-none or max_deliver=1 nothing redelivers, making the
     * gap PERMANENT and previously invisible on every channel (the heartbeat frames were swallowed
     * before the user handler could inspect them). The mismatch must surface via the error
     * listener; a matching heartbeat must NOT false-positive.
     */
    public function testPushConsumerHeartbeatSequenceMismatchIsSurfaced(): void
    {
        $createReply = '{"stream_name":"EVENTS","name":"EPH1","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';
        $hbOk = "NATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 1\r\n\r\n";
        $hbGap = "NATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 3\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->enqueueOnWriteContaining = [
            'SUB _INBOX.JS.PUSH' => [
                // Delivery with consumer seq 1, then a heartbeat agreeing (no gap), then a
                // heartbeat revealing the server delivered up to seq 3.
                "MSG events.a 2 \$JS.ACK.EVENTS.EPH1.1.5.1.0.0 2\r\nm1\r\n",
                sprintf("HMSG _INBOX.hb 2 %d %d\r\n%s\r\n", strlen($hbOk), strlen($hbOk), $hbOk),
                sprintf("HMSG _INBOX.hb 2 %d %d\r\n%s\r\n", strlen($hbGap), strlen($hbGap), $hbGap),
            ],
        ];
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'EVENTS',
            static function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            },
            consumerOptions: ['idle_heartbeat' => 5_000_000_000],
        )->await();

        for ($i = 0; $i < 5; $i++) {
            $client->processIncoming()->await();
        }
        $client->disconnect()->await();

        self::assertSame(['m1'], $received, 'heartbeats stay withheld from the user handler');
        $mismatches = array_values(array_filter(
            $errors,
            static fn (\Throwable $e): bool => str_contains($e->getMessage(), 'consumer sequence mismatch'),
        ));
        self::assertCount(1, $mismatches, 'the gap heartbeat must surface exactly one mismatch (the agreeing one must not)');
        self::assertStringContainsString('up to sequence 3 but only 1', $mismatches[0]->getMessage());
    }

    /**
     * Re-attaching to a DURABLE with delivery history must NOT false-alarm: the first heartbeat of
     * a session reports the consumer's historical Nats-Last-Consumer, which reached a PREVIOUS
     * session - not lost traffic. The gap check arms only once a delivery fixes a session baseline
     * (nats.go checkForSequenceMismatch's empty-cmeta early return).
     */
    public function testPushConsumerHeartbeatMismatchNotSignaledBeforeFirstSessionDelivery(): void
    {
        $createReply = '{"stream_name":"EVENTS","name":"DUR1","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';
        $hbHistory = "NATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 41\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->enqueueOnWriteContaining = [
            'SUB _INBOX.JS.PUSH' => [
                // The first frame of the session is a heartbeat carrying the durable's HISTORY.
                sprintf("HMSG _INBOX.hb 2 %d %d\r\n%s\r\n", strlen($hbHistory), strlen($hbHistory), $hbHistory),
            ],
        ];
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $client->jetStream()->subscribeEphemeralPushConsumer(
            'EVENTS',
            static function (NatsMessage $m): void {},
            consumerOptions: ['idle_heartbeat' => 5_000_000_000],
        )->await();

        for ($i = 0; $i < 3; $i++) {
            $client->processIncoming()->await();
        }
        $client->disconnect()->await();

        self::assertSame([], array_values(array_filter(
            $errors,
            static fn (\Throwable $e): bool => str_contains($e->getMessage(), 'consumer sequence mismatch'),
        )), 'a pre-first-delivery heartbeat reporting history must not false-alarm');
    }

    /**
     * A stop RACING an in-flight recreate must not resurrect the consumer: the recreate here is
     * suspended awaiting its CONSUMER.CREATE reply (attempt #1 gets no reply and times out) when
     * stopOrderedConsumer() runs. When a later create attempt then SUCCEEDS, the recreate must see
     * the stopped latch, tear the fresh instance down (unsubscribe its inbox, delete the consumer),
     * and never install it or re-arm the watchdog - otherwise a live consumer would keep delivering
     * AFTER a successful stop, with the stop handle already deregistered: permanently unstoppable.
     */
    public function testStopOrderedConsumerDuringInFlightRecreateDoesNotResurrect(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                // Recreate attempt #1: NO reply - the recreate parks in this await while stop runs.
                static fn (string $rt): array => [],
                // Attempt #2 succeeds - but by then the consumer is stopped, so it must be torn down.
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD3'))],
            ],
            onDelete: [
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap -> recreate starts and parks awaiting attempt #1's (withheld) reply.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $js = $client->jetStream();
        $sid = $js->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, 'events.>')->await();

        // The whole gap->recreate saga runs INSIDE one dispatch (the recreate's awaits are part of
        // the delivering processIncoming call), so the stop cannot be issued from this fiber
        // mid-recreate - schedule it on the event loop instead: it fires at ~50 ms, squarely inside
        // attempt #1's 300 ms parked await, while this fiber is still blocked in the dispatch.
        $stopDone = false;
        \Revolt\EventLoop::delay(0.05, static function () use ($js, $sid, &$stopDone): void {
            $js->stopOrderedConsumer($sid)->await();
            $stopDone = true;
        });

        // One pump delivers the gap frames and blocks through the recreate saga: attempt #1 parks
        // (reply withheld), the scheduled stop fires, attempt #1 times out, attempt #2 succeeds -
        // and must then observe the stop and tear itself down instead of installing.
        $deadlineNs = hrtime(true) + 5_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.CREATE.EVENTS') < 3 && hrtime(true) < $deadlineNs) {
            try {
                $client->processIncoming(new \Amp\TimeoutCancellation(0.05))->await();
            } catch (\Amp\CancelledException) {
                // Idle slice; timers (the stop, the retry) advance regardless.
            }
        }
        self::assertTrue($stopDone, 'precondition: the scheduled stop must have completed');

        // The stop must have landed DURING the recreate: its UNSUB of the then-current inbox
        // (sid 2) precedes attempt #2's CREATE on the wire.
        $written = implode('', $transport->writes);
        $thirdCreate = strpos($written, '$JS.API.CONSUMER.CREATE.EVENTS', (int) strpos($written, '$JS.API.CONSUMER.CREATE.EVENTS', (int) strpos($written, '$JS.API.CONSUMER.CREATE.EVENTS') + 1) + 1);
        self::assertIsInt($thirdCreate);
        self::assertNotFalse(strpos($written, "UNSUB 2\r\n"));
        self::assertLessThan($thirdCreate, (int) strpos($written, "UNSUB 2\r\n"), 'precondition: the stop must land before attempt #2 - inside the in-flight recreate');

        // Grace window: a resurrected consumer would re-arm its watchdog / keep going here.
        delay(0.2);

        $client->disconnect()->await();

        $written = implode('', $transport->writes);
        // The successful attempt-#2 instance was torn down, not installed: its fresh inbox (sid 3,
        // subscribed by the recreate before the create) must be unsubscribed...
        self::assertStringContainsString("UNSUB 3\r\n", $written, 'the never-installed replacement inbox must be released');
        // ...and no further recreate ever ran (exactly 3 CREATEs: initial + the two attempts).
        self::assertSame(3, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'a stopped consumer must never be recreated again');
        // Stop + recreate-delete + stopped-teardown delete all reached the server.
        self::assertGreaterThanOrEqual(2, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'), 'the stopped instances must be deleted');
    }

    /**
     * The replayed messages from a recreated ordered consumer can arrive on the rotated deliver inbox
     * DURING the recreate CONSUMER.CREATE's own read-pump - before its reply is processed. The new
     * instance must therefore be adopted (name + expected-sequence reset) BEFORE the create await, not
     * after: otherwise those early replay frames are filtered out by the not-yet-adopted consumer name
     * and a later one trips a spurious gap, cascading into a recreate storm under load where only the
     * pre-gap message is ever delivered (#122 regression). Modeled deterministically by echoing the
     * client-chosen name and ordering the first replay frame ahead of the create reply.
     */
    public function testSubscribeOrderedConsumerDeliversReplayArrivingDuringRecreateCreate(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        // MSG <subject> <sid> <$JS.ACK reply-to> <len>\r\n<body>\r\n
        $dataFrame = static fn(int $sid, string $consumer, int $delivered, int $sseq, int $cseq, string $body): string => sprintf(
            "MSG deliver.ord %d %s %d\r\n%s\r\n",
            $sid,
            sprintf('$JS.ACK.EVENTS.%s.%d.%d.%d.0.0', $consumer, $delivered, $sseq, $cseq),
            strlen($body),
            $body,
        );
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // Dynamic server that echoes the client-chosen consumer name (the real server honours
        // config.name). On the INITIAL create it also queues an in-order msg1 then a gap-exposing bad3;
        // on the RECREATE create it queues the first replayed frame (msg2) BEFORE the create reply, so
        // that frame is dispatched during the create's own read-pump - the race the fix must survive.
        $createSeen = 0;
        $transport->onWrite = static function (string $bytes) use (&$createSeen, $dataFrame, $createReply): array {
            $replyTo = self::requestReplyTo($bytes);
            if (str_contains($bytes, '$JS.API.CONSUMER.DELETE.')) {
                // Mux inbox (#118): the DELETE reply echoes on the request's captured reply-to.
                return [self::muxMsg($replyTo, '{"success":true}')];
            }
            if (!str_contains($bytes, '$JS.API.CONSUMER.CREATE.')) {
                return [];
            }

            preg_match('/"name":"([^"]+)"/', $bytes, $m);
            $name = $m[1] ?? 'UNKNOWN';
            $reply = $createReply($name);
            $createSeen++;

            if ($createSeen === 1) {
                // Initial create reply on the mux inbox, then delivery on the deliver sid 2:
                // msg1 in order (cseq 1), then bad3 exposing a gap (cseq 3) which triggers the recreate.
                return [
                    self::muxMsg($replyTo, $reply),
                    $dataFrame(2, $name, 1, 1, 1, 'msg1'),
                    $dataFrame(2, $name, 3, 4, 3, 'bad3'),
                ];
            }

            // Recreate: msg2 (cseq 1, sseq 2) on the ROTATED deliver sid 3 (mux has no per-request subs,
            // so the second deliver SUB collapses from the old sid 4 to 3) BEFORE the create reply, so it
            // is delivered during the create pump - it must not be dropped or storm.
            return [
                $dataFrame(3, $name, 1, 2, 1, 'msg2'),
                self::muxMsg($replyTo, $reply),
            ];
        };

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 10; $i++) {
            $client->processIncoming()->await();
        }

        // msg1 delivered in order; the gap-exposer bad3 discarded; the replay msg2 that arrived DURING
        // the recreate create is delivered (not lost to a name-filter reject then gap storm).
        self::assertContains('msg1', $received);
        self::assertContains('msg2', $received);
        self::assertNotContains('bad3', $received);

        $written = implode('', $transport->writes);
        // Exactly one recreate - no storm.
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * Verifies a stale delivery from a previous (deleted) consumer instance - arriving on the reused
     * deliver inbox after a recreate - is ignored by consumer-instance name, so it cannot be
     * mis-delivered or trigger a spurious recreate even when its consumer sequence matches (#86).
     */
    public function testSubscribeOrderedConsumerIgnoresStaleDeliveryFromPreviousConsumerInstance(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Mux inbox (#118): CREATE reply on the captured reply-to; deliveries on the actual deliver sid.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [static fn (string $rt): array => [self::muxMsg($rt, (string) $createReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // In-order delivery from the current consumer ORD1 (consumer seq 1 -> expected next 2).
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // A STALE delivery from a DIFFERENT consumer instance (ORDX) whose consumer seq (2)
                    // would otherwise match the expected next sequence and be (wrongly) delivered.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORDX.1.2.2.0.0 5\r\nstale\r\n",
                ],
            ],
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 4; $i++) {
            $client->processIncoming()->await();
        }

        // Only the in-order ORD1 message is delivered; the stale ORDX delivery is ignored.
        self::assertSame(['msg1'], $received);

        $written = implode('', $transport->writes);
        // The stale delivery did not trigger a recreate (no delete, only the initial create).
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * Verifies an idle heartbeat whose Nats-Last-Consumer is ahead of what was processed triggers a
     * proactive recreate from the last in-order point - detecting a missed TAIL of deliveries that no
     * further message would otherwise expose (#86).
     */
    public function testSubscribeOrderedConsumerRecreatesOnHeartbeatTailGap(): void
    {
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        // Heartbeat reporting the server delivered up to consumer seq 3, while we only processed seq 1.
        $hbHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Last-Consumer' => '3',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Mux inbox (#118): CREATE/DELETE replies on captured reply-tos; deliveries on the actual sid.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // In-order msg1 (consumer seq 1, stream seq 1) -> expected next 2, lastStreamSeq 1.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Idle heartbeat: last delivered consumer seq 3 > processed (1) -> tail gap -> recreate.
                    sprintf("HMSG deliver.ord $sid %d %d\r\n%s\r\n", strlen($hbHeaders), strlen($hbHeaders), $hbHeaders),
                ],
                // Rotated epoch: no further delivery in this test.
            ],
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1'], $received);

        $written = implode('', $transport->writes);
        // Exactly one recreate from the last in-order point (stream seq 1 + 1 = 2).
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
        self::assertStringContainsString('"opt_start_seq":2', $written);
    }

    /**
     * A terminally dead ordered consumer must reach the PSR-3 LOGGER, not only the (optional,
     * default-null) errorListener: an application configured with a logger but no listener
     * previously observed a consumer that just stopped delivering forever, with no log line and no
     * exception anywhere - blocked flow with zero signal.
     */
    public function testTerminalRecreateFailureReachesTheLogger(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        $createError = '{"error":{"code":404,"description":"stream not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, (string) $createReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $logger = new class extends \Psr\Log\AbstractLogger {
            /** @var list<array{level:mixed,message:string}> */
            public array $records = [];

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                $this->records[] = ['level' => $level, 'message' => (string) $message];
            }
        };

        // Logger configured, NO errorListener - the previously fully silent configuration.
        $client = new NatsClient(new NatsOptions(logger: $logger), $transport);
        $client->connect()->await();

        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, 'events.>')->await();

        for ($i = 0; $i < 9; $i++) {
            $client->processIncoming()->await();
        }

        $recreateLogs = array_values(array_filter(
            $logger->records,
            static fn (array $r): bool => str_contains($r['message'], 'Ordered consumer recreate failed'),
        ));
        self::assertNotSame([], $recreateLogs, 'the terminal consumer death must produce a log line even with no errorListener');
        self::assertSame('error', (string) $recreateLogs[0]['level']);
    }

    public function testSubscribeOrderedConsumerContainsRecreateFailure(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        $createError = '{"error":{"code":404,"description":"stream not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Mux inbox (#118): initial create OK, then EVERY recreate create attempt fails (3 retries) so
        // the recreate is terminally dead; replies echo on captured reply-tos, deliveries on the sid.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, (string) $createReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // In-order msg1 (consumer seq 1).
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap (consumer seq 3) triggers recovery.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        // Pump all frames. A failed recreate must be CONTAINED - it must not throw out of the shared
        // subscription dispatch loop (which would abort delivery for every other subscription).
        for ($i = 0; $i < 9; $i++) {
            $client->processIncoming()->await();
        }

        // The in-order message was delivered; the out-of-order one was discarded; the recreate was
        // retried per attempt and the terminal failure surfaced via the error listener (#114) rather
        // than escaping the dispatch loop or staying silent.
        self::assertSame(['msg1'], $received);
        self::assertCount(1, $errors);
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertStringContainsString('after 3 attempts', $errors[0]->getMessage());
    }

    public function testSubscribeOrderedConsumerRecreateRetriesThroughTransientFailure(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $recreateReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD2',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        $createError = '{"error":{"code":10008,"description":"transient failure"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Mux inbox (#118): delete OK, first recreate create fails, the retry succeeds as ORD2. The
        // resumed delivery must arrive AFTER the successful create reply (the recreate adopts a fresh
        // client-chosen name before each create await and only re-adopts the response name once it
        // returns), so msg4 rides the successful create's reply on the rotated deliver sid (mux: no
        // per-request subs, so the single recreate's deliver SUB is sid 3, not 4).
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, (string) $createReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [
                    self::muxMsg($rt, (string) $recreateReply),
                    "MSG deliver.ord 3 \$JS.ACK.EVENTS.ORD2.1.4.1.0.0 4\r\nmsg4\r\n",
                ],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap exposes a missed push -> recovery.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 9; $i++) {
            $client->processIncoming()->await();
        }

        // Recovery succeeded on the second attempt: the new instance's first in-order message is
        // delivered and nothing was reported to the error listener.
        self::assertSame(['msg1', 'msg4'], $received);
        self::assertSame([], $errors);
    }

    /**
     * Verifies a deleteConsumer TimeoutException during gap recovery is treated as best-effort
     * cleanup: control still reaches the create-retry loop, the consumer is recreated, and
     * delivery resumes (#151). TimeoutException extends NatsException, NOT JetStreamException,
     * so a delete-leg-only catch would bypass the #114 retry loop entirely.
     */
    public function testSubscribeOrderedConsumerRecreatesWhenDeleteConsumerTimesOut(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $recreateReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD2',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // Mux inbox (#118): the DELETE request gets NO reply (onDelete omitted), so its wait loop spins
        // on an empty queue until the 25 ms request deadline fires -> TimeoutException. The recreate
        // reply and the resumed delivery ride the recreate CREATE's own reply, so the delete's timeout
        // wait cannot drain them first. The single recreate's rotated deliver SUB is sid 3 under the mux
        // (no per-request subs), and msg4 must land after the successful create reply (re-adopt point).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, (string) $createReply)],
                static fn (string $rt): array => [
                    self::muxMsg($rt, (string) $recreateReply),
                    "MSG deliver.ord 3 \$JS.ACK.EVENTS.ORD2.1.4.1.0.0 4\r\nmsg4\r\n",
                ],
            ],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap (consumer seq 3) triggers recovery; the queue is empty from here on.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $errors = [];
        $options = new NatsOptions(requestTimeoutMs: 25, errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        // The timed-out delete fell through to the create-retry loop: the consumer was recreated
        // as ORD2, its first in-order message was delivered, and no terminal recreate failure was
        // reported (the create leg was never exhausted).
        self::assertSame(['msg1', 'msg4'], $received);
        self::assertSame([], $errors);

        $written = implode('', $transport->writes);
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * Verifies a deleteConsumer ConnectionException during gap recovery is treated as best-effort
     * cleanup: control still reaches the create-retry loop, the consumer is recreated, and
     * delivery resumes (#151). ConnectionException extends NatsException, NOT JetStreamException,
     * so a delete-leg-only catch would bypass the #114 retry loop entirely.
     */
    public function testSubscribeOrderedConsumerRecreatesWhenDeleteConsumerFailsWithConnectionError(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $recreateReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD2',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // Mux inbox (#118): the DELETE reply wait reads a fatal -ERR frame, which handleFrame() raises
        // as a ConnectionException out of deleteConsumer() without closing the connection; the create-
        // retry loop still runs and succeeds as ORD2. Replies echo on captured reply-tos; the single
        // recreate's rotated deliver SUB is sid 3 (no per-request subs), and msg4 rides the successful
        // create reply (the re-adopt point).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, (string) $createReply)],
                static fn (string $rt): array => [
                    self::muxMsg($rt, (string) $recreateReply),
                    "MSG deliver.ord 3 \$JS.ACK.EVENTS.ORD2.1.4.1.0.0 4\r\nmsg4\r\n",
                ],
            ],
            onDelete: [static fn (string $rt): array => ["-ERR 'Stale Connection'\r\n"]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap (consumer seq 3) triggers recovery.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 8; $i++) {
            $client->processIncoming()->await();
        }

        // The failed delete fell through to the create-retry loop: the consumer was recreated as
        // ORD2, its first in-order message was delivered, and no terminal recreate failure was
        // reported (the create leg was never exhausted).
        self::assertSame(['msg1', 'msg4'], $received);
        self::assertSame([], $errors);

        $written = implode('', $transport->writes);
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    public function testSubscribeOrderedConsumerDeliversFilteredMessagesWithoutSpuriousRecreate(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // A filtered ordered consumer over a stream that also carries non-matching messages: the
        // matching deliveries have CONSECUTIVE consumer sequences (1,2,3) but NON-contiguous stream
        // sequences (2,4,6, because non-matching messages occupy 1,3,5). They must all be delivered
        // in order with NO recreate - detecting gaps by stream sequence would wrongly recreate here.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Mux inbox (#118): CREATE reply on the captured reply-to; deliveries on the actual deliver sid.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [static fn (string $rt): array => [self::muxMsg($rt, $createReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.2.1.0.0 4\r\nmsg1\r\n", // cseq 1, sseq 2
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.2.4.2.0.0 4\r\nmsg2\r\n", // cseq 2, sseq 4
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.6.3.0.0 4\r\nmsg3\r\n", // cseq 3, sseq 6
                ],
            ],
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 5; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1', 'msg2', 'msg3'], $received);

        $written = implode('', $transport->writes);
        // No gap was detected, so the consumer is never deleted/recreated (one initial CREATE only).
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    // ─── New coverage additions ─────────────────────────────────────────

    /**
     * Verifies createOrUpdateStream re-throws a JetStreamException whose message is NOT "already in use"
     * (the guard only swallows the "already in use" variant).
     */
    public function testCreateOrUpdateStreamRethrowsNonAlreadyInUseError(): void
    {
        $createErr = '{"error":{"code":503,"description":"JetStream not enabled"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream not enabled');

        $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'])->await();
    }

    /**
     * Verifies streamNames() handles a missing / null 'streams' key in the API response by
     * returning an empty list (the ternary else branch).
     */
    public function testStreamNamesWithNullStreamsKeyReturnsEmpty(): void
    {
        // The server returns a valid-but-empty response (no 'streams' key at all).
        $reply = '{"total":0,"offset":0}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $names = $client->jetStream()->streamNames()->await();

        self::assertSame([], $names);
    }

    /**
     * Verifies directGet (via directGetLastMessageForSubject) throws when the reply carries no
     * Nats-Stream or Nats-Sequence header - the "unrecognized response" guard.
     */
    public function testDirectGetThrowsForUnrecognizedResponse(): void
    {
        // A response with Nats-* headers missing (no status, no Nats-Stream, no Nats-Sequence).
        $hdrs = "NATS/1.0\r\nX-Unrelated: yes\r\n\r\n";
        $body = 'some payload';
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("HMSG _INBOX.a 1 %d %d\r\n%s%s\r\n", $h, $h + strlen($body), $hdrs, $body),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream direct get returned an unrecognized response');

        $client->jetStream()->directGetLastMessageForSubject('ORDERS', 'orders.1')->await();
    }

    /**
     * Verifies directGetLastForSubjects() returns an empty list immediately when the subjects
     * array is empty (early return).
     */
    public function testDirectGetLastForSubjectsWithEmptySubjectsReturnsEmpty(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', [])->await();

        self::assertSame([], $messages);
    }

    /**
     * Verifies directGetLastForSubjects() rejects a subject containing '*'.
     */
    public function testDirectGetLastForSubjectsRejectsWildcardSubjectWithStar(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('directGetLastForSubjects expects exact subjects');

        $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.*'])->await();
    }

    /**
     * Verifies directGetLastForSubjects() rejects a subject containing '>'.
     */
    public function testDirectGetLastForSubjectsRejectsWildcardSubjectWithGreaterThan(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('directGetLastForSubjects expects exact subjects');

        $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.>'])->await();
    }

    /**
     * Verifies directGetBatch() rejects a non-positive expiresMs.
     */
    public function testDirectGetBatchRejectsZeroExpiresMs(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Direct Get batch expiresMs must be greater than zero');

        $client->jetStream()->directGetBatch('ORDERS', ['batch' => 1], 0)->await();
    }

    /**
     * Verifies addOrUpdateConsumer delegates to createConsumer and produces the same
     * wire payload.
     */
    public function testAddOrUpdateConsumerDelegatesToCreateConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->addOrUpdateConsumer('ORDERS', 'PROC', 'orders.*')->await();

        self::assertSame('PROC', $info->name);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS.PROC', $transport->writes[3]);
    }

    /**
     * Verifies consumerNames() handles a missing / null 'consumers' key in the API response by
     * returning an empty list (the ternary else branch).
     */
    public function testConsumerNamesWithMissingConsumersKeyReturnsEmpty(): void
    {
        $reply = '{"total":0,"offset":0}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $names = $client->jetStream()->consumerNames('ORDERS')->await();

        self::assertSame([], $names);
    }

    /**
     * Verifies subscribeEphemeralPushConsumer silently absorbs a flow-control status 100 frame
     * and does not forward it to the user handler (early return for control message).
     */
    public function testSubscribeEphemeralPushConsumerIgnoresControlMessages(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"E1","config":{"deliver_subject":"deliver.eph"}}';
        $fcHeaders = NatsHeaders::toWireBlock(['Status' => '100', 'Description' => 'FlowControl Request']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); deliver frames (sid 2) arrive after their SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.eph '] = [
            sprintf(
                "HMSG deliver.eph 2 fc.reply %d %d\r\n%s\r\n",
                strlen($fcHeaders),
                strlen($fcHeaders),
                $fcHeaders,
            ),
            "MSG deliver.eph 2 5\r\nhello\r\n",
        ];

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            'deliver.eph',
        )->await();

        $client->processIncoming()->await(); // flow control - must NOT appear in $received
        $client->processIncoming()->await(); // real message

        self::assertSame(['hello'], $received);
        // The FC reply was published.
        self::assertStringContainsString("PUB fc.reply 0\r\n\r\n", implode('', $transport->writes));
    }

    /**
     * Verifies subscribeOrderedConsumer silently absorbs a flow-control / heartbeat status 100
     * and does not forward it to the user handler (early return for control message).
     */
    public function testSubscribeOrderedConsumerIgnoresControlMessages(): void
    {
        $createPayload = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD_HB',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $hbHeaders = NatsHeaders::toWireBlock(['Status' => '100', 'Description' => 'Idle Heartbeat']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); the ordered deliver inbox (_INBOX.JS.PUSH.*) is sid 2,
        // so its frame arrives after the deliver SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB _INBOX.JS.ORD'] = [
            sprintf(
                "HMSG _INBOX.JS.ORD.x 2 %d %d\r\n%s\r\n",
                strlen($hbHeaders),
                strlen($hbHeaders),
                $hbHeaders,
            ),
        ];

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
        )->await();

        $client->processIncoming()->await();

        self::assertSame([], $received);
    }

    /**
     * Verifies subscribeOrderedConsumer delivers a message that has no $JS.ACK reply subject
     * (no ordering metadata) best-effort to the user handler (null seq path), and does so
     * SILENTLY - the unparseable-ack protocol error (#155) applies only to reply subjects that
     * claim the $JS.ACK form, not to plain or absent reply subjects.
     */
    public function testSubscribeOrderedConsumerDeliversMessageWithoutAckMetadata(): void
    {
        $createPayload = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD_NO_ACK',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); deliver frames (sid 2) arrive after the deliver SUB.
        $this->muxReplies($transport, [
            // Ephemeral push consumer create response.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB _INBOX.JS.ORD'] = [
            // A delivery with no $JS.ACK reply subject - no ordering metadata.
            "MSG _INBOX.JS.ORD.x 2 5\r\nhello\r\n",
            // A delivery with a plain (non-$JS.ACK) reply subject - also no ordering metadata.
            "MSG _INBOX.JS.ORD.x 2 plain.reply 6\r\nhello2\r\n",
        ];

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
        )->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        // Best-effort delivery: the messages are forwarded despite having no ordering information,
        // and no error is emitted for reply subjects that never claimed the $JS.ACK form.
        self::assertSame(['hello', 'hello2'], $received);
        self::assertSame([], $errors);
    }

    /**
     * Verifies subscribeOrderedConsumer surfaces an unparseable $JS.ACK reply subject (an ack-form
     * claim the parser cannot read, so gap detection and the stale-consumer filter cannot run)
     * through the error listener while still delivering the message best-effort (#155). Silent
     * before the fix: the null-metadata branch bypassed every ordering check without a trace.
     */
    public function testSubscribeOrderedConsumerEmitsErrorForUnparseableAckSubject(): void
    {
        $createPayload = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD_BAD_ACK',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); the deliver frame (sid 2) arrives after the deliver SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB _INBOX.JS.ORD'] = [
            // 10 tokens: claims the $JS.ACK form but matches neither the 9-token v1 form nor the
            // >= 11-token v2 form, so it is unparseable even for the tolerant parser.
            "MSG _INBOX.JS.ORD.x 2 \$JS.ACK.EVENTS.ORD_BAD_ACK.1.1.1.0.0.X 4\r\nmsg1\r\n",
        ];

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
        )->await();

        $client->processIncoming()->await();

        // The message is still delivered best-effort (no recreate, no drop) ...
        self::assertSame(['msg1'], $received);
        // ... but the degradation is loud: ordering checks could not run for this delivery.
        self::assertCount(1, $errors);
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertStringContainsString('unparseable $JS.ACK reply subject', $errors[0]->getMessage());
        self::assertStringContainsString('EVENTS', $errors[0]->getMessage());
        // A parse failure must NOT trigger a recreate - only the initial CREATE goes on the wire.
        $written = implode('', $transport->writes);
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * Verifies the unparseable-ack error is emitted ONCE per consumer instance, not once per
     * message - a stream of unparseable deliveries must not become an error storm (#155). All
     * messages are still delivered best-effort.
     */
    public function testSubscribeOrderedConsumerEmitsUnparseableAckErrorOncePerConsumer(): void
    {
        $createPayload = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD_BAD_ACK',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); deliver frames (sid 2) arrive after the deliver SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $transport->enqueueOnWriteContaining['SUB _INBOX.JS.ORD'] = [
            // Two consecutive 10-token (unparseable) ack subjects on the same consumer instance.
            "MSG _INBOX.JS.ORD.x 2 \$JS.ACK.EVENTS.ORD_BAD_ACK.1.1.1.0.0.X 4\r\nmsg1\r\n",
            "MSG _INBOX.JS.ORD.x 2 \$JS.ACK.EVENTS.ORD_BAD_ACK.2.2.2.0.0.X 4\r\nmsg2\r\n",
        ];

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
        )->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        // Both messages delivered best-effort; the protocol error fired exactly once.
        self::assertSame(['msg1', 'msg2'], $received);
        self::assertCount(1, $errors);
    }

    /**
     * Verifies the once-per-consumer-instance latch of the unparseable-ack error re-arms on a
     * recreate: a new consumer epoch that again produces unparseable ack subjects emits a fresh
     * error, so long-lived subscriptions do not go permanently silent after the first one (#155).
     */
    public function testSubscribeOrderedConsumerUnparseableAckErrorRearmsAfterRecreate(): void
    {
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        // Heartbeat reporting the server delivered up to consumer seq 3 while only seq 1 was
        // processed in order - triggers the tail-gap recreate (a new consumer epoch).
        $hbHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Last-Consumer' => '3',
        ]);

        // Mux inbox (#118): CREATE/DELETE replies on captured reply-tos. Both epochs' unparseable ack
        // frames are delivered best-effort (the null-metadata branch runs before the name filter), so
        // the rotated epoch's frame can land on its deliver sid regardless of the re-adopt point.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // Unparseable (10-token) ack subject on the first epoch -> first error.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0.X 4\r\nbad1\r\n",
                    // In-order delivery so the tail-gap heartbeat below has a processed baseline.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Tail-gap heartbeat -> recreate (a new consumer epoch).
                    sprintf("HMSG deliver.ord $sid %d %d\r\n%s\r\n", strlen($hbHeaders), strlen($hbHeaders), $hbHeaders),
                ],
                static fn (int $sid): array => [
                    // Unparseable ack subject on the SECOND epoch -> the latch re-armed, second error.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD2.1.1.1.0.0.X 4\r\nbad2\r\n",
                ],
            ],
        );

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 8; $i++) {
            $client->processIncoming()->await();
        }

        // Best-effort deliveries from both epochs plus the one in-order message.
        self::assertSame(['bad1', 'msg1', 'bad2'], $received);
        // One unparseable-ack error per consumer epoch: first epoch and post-recreate epoch.
        self::assertCount(2, $errors);
        self::assertStringContainsString('unparseable $JS.ACK reply subject', $errors[0]->getMessage());
        self::assertStringContainsString('unparseable $JS.ACK reply subject', $errors[1]->getMessage());
    }

    /**
     * Verifies subscribeOrderedConsumer best-effort delivers when deleteConsumer throws a
     * JetStreamException during gap recovery (the inner catch block).
     */
    public function testSubscribeOrderedConsumerToleratesDeleteConsumerFailure(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => '_INBOX.JS.ORD.test', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // deleteConsumer returns an error payload, triggering JetStreamException inside the try block.
        $deleteError = '{"error":{"code":404,"description":"consumer not found"}}';
        $recreateReply = $createReply('ORD2');

        // Mux inbox (#118): CREATE replies and the DELETE error echo on captured reply-tos; deliveries
        // ride the deliver SUB on its actual sid. deleteConsumer's JetStreamException is caught and the
        // recreate still proceeds and succeeds.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $recreateReply)],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteError)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // In-order message (consumer seq 1 / stream seq 1).
                    "MSG _INBOX.JS.ORD.test $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Out-of-order message (consumer seq jumps to 3, triggers recreation).
                    "MSG _INBOX.JS.ORD.test $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        // msg1 is delivered; bad3 (out-of-order) is discarded; delete failure is absorbed.
        self::assertSame(['msg1'], $received);

        $client->disconnect()->await();
    }

    // ─── Idle-heartbeat watchdog (#113) ──────────────────────────────────

    /**
     * Verifies the idle-heartbeat watchdog (#113): an ordered consumer that requested heartbeats but
     * whose transport goes totally silent (no data, no status-100 heartbeat, no flow-control) for more
     * than two heartbeat intervals is recreated - the missed-heartbeat monitor nats.go runs. It detects
     * a consumer the server reaped (inactive_threshold lapse, mem_storage R1 restart, interest gap) that
     * the sequence-gap logic can never observe because that only fires when a frame arrives. A second
     * CONSUMER.CREATE reaching the wire proves the watchdog re-established the consumer.
     */
    public function testSubscribeOrderedConsumerRecreatesOnMissedHeartbeats(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        // blockWhenEmpty models a live-but-idle socket: reads suspend rather than return EOF, so the
        // only thing that can end the silence is the watchdog firing a recreate. The recreate's DELETE
        // (request inbox sid 3) and CREATE (request inbox sid 4) replies are held back until those
        // requests are actually written; only the recreate CREATE carries by_start_sequence.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        // Mux inbox (#118): the watchdog recreate's DELETE and CREATE replies echo on the request's
        // captured reply-to (no per-request sids); no delivery frames (the consumer stays silent).
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
        );

        $errors = [];
        $options = new NatsOptions(pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        // 30 ms heartbeat -> the watchdog fires after ~60 ms of silence.
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, null, 30_000_000)->await();

        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.CREATE.EVENTS') < 2 && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }

        $client->disconnect()->await();

        $written = implode('', $transport->writes);
        // Exactly one watchdog-driven recreate: one DELETE of the silent consumer and a second CREATE.
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'), 'the reaped consumer must be deleted once');
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'the watchdog must recreate the silent consumer exactly once');
        // Nothing was delivered before the silence, so recovery re-applies the INITIAL deliver
        // policy (here the server default, 'all' - semantically identical to the previous
        // by_start_sequence-from-1 restart). A by_start_sequence replay must only be used once a
        // delivery fixed a resume point: with a 'new'/'last_per_subject' initial policy (a KV/OS
        // watch), restarting from sequence 1 would wrongly replay the whole stream.
        self::assertStringNotContainsString('"by_start_sequence"', $written);
        self::assertStringNotContainsString('"opt_start_seq"', $written);
        self::assertSame([], $errors, 'a successful watchdog recreate must not surface an error');
    }

    /**
     * Verifies the watchdog does NOT fire while heartbeats keep arriving (#113): an ordered consumer
     * that receives an idle heartbeat every interval - a legitimately long-but-alive quiet period - is
     * never recreated. Every inbound frame (here, status-100 heartbeats) rearms the watchdog, so a
     * consumer that is quiet but alive is left untouched.
     */
    public function testSubscribeOrderedConsumerHeartbeatsPreventFalsePositiveRecreate(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        // A plain idle heartbeat with no Nats-Last-Consumer: proves liveness without reporting a tail gap.
        $hb = NatsHeaders::toWireBlock(['Status' => '100', 'Description' => 'Idle Heartbeat']);
        $hbFrame = sprintf("HMSG deliver.ord 2 %d %d\r\n%s\r\n", strlen($hb), strlen($hb), $hb);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
        ]);

        $errors = [];
        $options = new NatsOptions(pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        // 100 ms heartbeat -> 200 ms watchdog threshold.
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, null, 100_000_000)->await();

        // Feed a heartbeat roughly every 10 ms for ~500 ms (past two thresholds). Each frame rearms the
        // watchdog with a ~20x margin on the feed gap, so a slow CI must not produce a false recreate.
        $deadlineNs = hrtime(true) + 500_000_000;
        while (hrtime(true) < $deadlineNs) {
            $transport->pushReadChunk($hbFrame);
            $client->processIncoming()->await();
            delay(0.01);
        }

        $client->disconnect()->await();

        $written = implode('', $transport->writes);
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'), 'live heartbeats must not trigger a recreate');
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'only the initial consumer create must reach the wire');
        self::assertSame([], $errors, 'a heartbeating consumer must not surface a stall error');
    }

    /**
     * Verifies the watchdog surfaces a descriptive error for a caller-owned push consumer (#113): a
     * durable push consumer created with idle_heartbeat whose transport goes silent for more than two
     * intervals notifies the error listener (nats.go ErrConsumerNotActive). The library cannot recreate
     * a caller-owned consumer, so it surfaces the stall through the error listener instead - exactly
     * once per silence episode, no recreate on the wire.
     */
    public function testSubscribePushConsumerSurfacesErrorOnMissedHeartbeats(): void
    {
        $createReply = '{"stream_name":"EVENTS","name":"PROC","config":{"deliver_subject":"deliver.push"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $errors = [];
        $options = new NatsOptions(pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        // 30 ms heartbeat -> the watchdog surfaces a stall after ~60 ms of silence.
        $client->jetStream()->subscribePushConsumer(
            'EVENTS',
            'PROC',
            static function (NatsMessage $message): void {},
            'deliver.push',
            null,
            ['idle_heartbeat' => 30_000_000],
        )->await();

        $deadlineNs = hrtime(true) + 2_000_000_000;
        while ($errors === [] && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }

        $client->disconnect()->await();

        self::assertCount(1, $errors, 'a silent caller-owned push consumer must surface exactly one stall error');
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertStringContainsString('not active', $errors[0]->getMessage());
        self::assertStringContainsString('PROC', $errors[0]->getMessage());
        // A caller-owned consumer is never deleted/recreated by the library.
        self::assertSame(0, substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.DELETE'), 'the library must not recreate a caller-owned push consumer');
    }

    /**
     * Verifies the watchdog timer is cancelled when the subscription is torn down (#113): it must not
     * outlive the subscription it guards (mirrors the #126 ping-timer teardown, so no timer leaks and
     * roots an abandoned connection). Probed through the event loop's registered-timer set.
     */
    public function testSubscribeOrderedConsumerWatchdogTimerIsCancelledOnUnsubscribe(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
        ]);

        // pingIntervalSeconds: 0 disables the connection ping timer, so the only repeat timer the
        // subscription can add is the watchdog.
        $client = new NatsClient(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $before = self::countRepeatTimers();
        $sid = $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, null, 30_000_000)->await();
        self::assertSame($before + 1, self::countRepeatTimers(), 'subscribing an ordered consumer must arm exactly one heartbeat watchdog timer');

        $client->unsubscribe($sid)->await();

        // The next watchdog tick observes the dropped subscription and self-cancels; wait for it.
        $deadlineNs = hrtime(true) + 1_000_000_000;
        while (self::countRepeatTimers() > $before && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }
        self::assertSame($before, self::countRepeatTimers(), 'the watchdog timer must be cancelled after unsubscribe (no leaked timer)');

        $client->disconnect()->await();
    }

    /**
     * Verifies the ordered-consumer recreate is serialized across the two fibers that can trigger it -
     * the message-dispatch handler (sequence-gap / tail-gap) and the watchdog timer (onMiss) - so a
     * concurrent trigger cannot drive a SECOND CONSUMER.CREATE and orphan an ephemeral consumer (#113).
     *
     * The dispatch handler is serialized against itself by NatsConnection's per-sid dispatch guard, but
     * the watchdog runs in its own timer fiber that the per-sid guard does not cover. Here the watchdog
     * fires the first recreate on a silent transport; the instant that recreate writes its DELETE.ORD1
     * (and parks on a never-answered delete reply) a gap-bearing delivery for the still-current ORD1 is
     * fed, which the dispatch handler turns into a second recreate. With an in-flight-recreate guard the
     * second trigger is a no-op, so ORD1 is deleted exactly once; without it ORD1 is deleted twice and a
     * second consumer is created and orphaned. Falsifiable: this fails (two DELETE.ORD1) against a build
     * with no in-flight guard.
     */
    public function testSubscribeOrderedConsumerWatchdogAndGapRecreateDoNotRunConcurrently(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // A gap delivery for consumer ORD1: consumer seq 5 != expected 1 -> the dispatch handler calls
        // recreate(). Tokens: num_delivered=5, stream_seq=5, consumer_seq=5.
        $gapFrame = "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.5.5.5.0.0 4\r\ngap5\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        // Mux inbox (#118): the initial create reply echoes on its captured reply-to. The DELETE.ORD1's
        // reply is NEVER sent (onDelete emits only the gap frame, no reply), so the watchdog recreate
        // stays parked on its delete await - exactly the window in which the concurrent dispatch-driven
        // recreate must be suppressed. The gap frame rides that DELETE write on the still-current sid 2.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))]],
            onDelete: [static fn (string $rt): array => [$gapFrame]],
        );

        $errors = [];
        // Short request timeout so the parked delete/create fall through quickly for clean teardown.
        $options = new NatsOptions(requestTimeoutMs: 200, pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        // 30 ms heartbeat -> the watchdog fires the first recreate after ~60 ms of silence.
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, null, 30_000_000)->await();

        // Allow ample time for a bug's SECOND DELETE.ORD1 to appear before asserting it did not.
        $deadlineNs = hrtime(true) + 1_500_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.DELETE.EVENTS.ORD1') < 2 && hrtime(true) < $deadlineNs) {
            delay(0.02);
        }

        $written = implode('', $transport->writes);
        $client->disconnect()->await();

        self::assertSame(
            1,
            substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'),
            'a recreate already in flight must suppress the concurrent trigger (exactly one DELETE.ORD1, no orphaned consumer)',
        );
    }

    /**
     * Verifies a successful ordered recreate re-arms the watchdog for the NEW consumer (#113). The
     * watchdog sets its miss latch before invoking onMiss and only an inbound frame's touch() clears it,
     * so a recreate that succeeds but is then re-reaped before its first heartbeat would leave the latch
     * stuck and the watchdog wedged forever. Here the first (watchdog) recreate is answered so it
     * succeeds (ORD1 -> ORD2); ORD2 then also stays silent. A watchdog that clears the latch on a
     * successful recreate fires again and deletes ORD2 too; a wedged one never touches ORD2. Falsifiable:
     * this fails (no DELETE.ORD2) against a build that does not clear the latch on a successful recreate.
     */
    public function testSubscribeOrderedConsumerWatchdogRefiresAfterSuccessfulRecreate(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        // Mux inbox (#118): answer the first (watchdog) recreate's DELETE.ORD1 and CREATE (as ORD2) on
        // their captured reply-tos so it SUCCEEDS, leaving ORD2 live. The second recreate's DELETE.ORD2
        // is intentionally never answered (onDelete has one entry) - its presence on the wire suffices.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
        );

        $errors = [];
        $options = new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        // 30 ms heartbeat -> ~60 ms silence threshold.
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, null, 30_000_000)->await();

        // Wait for the SECOND recreate (of the still-silent ORD2), which only happens if the successful
        // first recreate re-armed the watchdog.
        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.DELETE.EVENTS.ORD2') < 1 && hrtime(true) < $deadlineNs) {
            delay(0.02);
        }

        $written = implode('', $transport->writes);
        $client->disconnect()->await();

        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'), 'the first watchdog recreate must delete ORD1 exactly once');
        self::assertGreaterThanOrEqual(
            1,
            substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD2'),
            'a successful recreate must re-arm the watchdog so a re-reaped consumer (ORD2) is recreated again',
        );
    }

    /**
     * Verifies the watchdog rebases its clock and neither fires nor cancels while the connection is
     * mid-reconnect (state Connecting), so it survives a transient reconnect (#113). No frame can arrive
     * during a reconnect and the consumer is not the culprit, so recreating or cancelling would be
     * wrong. Pins the ordering of armHeartbeatWatchdog's Closed / isSubscriptionActive / not-Open checks
     * against a future reorder. The connection is forced into Connecting via reflection (a real
     * reconnect cannot be driven deterministically in a unit test); the subscription stays registered,
     * so isSubscriptionActive remains true.
     */
    public function testSubscribeOrderedConsumerWatchdogSurvivesTransientReconnect(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
        ]);

        $errors = [];
        $options = new NatsOptions(pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $before = self::countRepeatTimers();
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, null, 30_000_000)->await();
        self::assertSame($before + 1, self::countRepeatTimers(), 'subscribing an ordered consumer must arm exactly one watchdog timer');

        // Force the connection into a mid-reconnect state; the subscription stays registered.
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertIsObject($connection);
        $stateProp = new \ReflectionProperty($connection::class, 'state');
        $stateProp->setValue($connection, ConnectionState::Connecting);

        // Let several 30 ms intervals elapse well past the 2-interval (60 ms) miss threshold.
        $deadlineNs = hrtime(true) + 300_000_000;
        while (hrtime(true) < $deadlineNs) {
            delay(0.02);
        }

        $written = implode('', $transport->writes);
        // Restore Open BEFORE asserting so a failed assertion cannot leave the connection un-teardownable.
        $stateProp->setValue($connection, ConnectionState::Open);

        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'), 'the watchdog must not recreate during a reconnect');
        self::assertSame([], $errors, 'the watchdog must not surface a stall during a reconnect');
        self::assertSame($before + 1, self::countRepeatTimers(), 'the watchdog timer must survive a transient reconnect (rebase, not cancel)');

        $client->disconnect()->await();
    }

    /**
     * Verifies the disconnect-collision deferral re-arms the watchdog (2nd-round review major): a
     * watchdog-triggered recreate that dies fail-fast while the connection is not Open takes the
     * deferral branch, which promises the watchdog "fires again once Open". The tick that fired
     * onMiss latched $state->notified, and only an inbound frame's touch() or a SUCCESSFUL recreate
     * clears it - but the reaped consumer can never send a frame again (that silence is WHY onMiss
     * fired) and the recreate's first step deleted it. Without the deferral's latch reset every
     * post-reconnect tick early-returns and the ordered consumer / KV / OS watch stalls permanently
     * with zero signal. A second CONSUMER.CREATE after the connection is Open again proves the
     * watchdog re-fired and re-established the consumer.
     *
     * Sequencing is wire-event-driven, not wall-clock: the connection leaves Open inside the
     * onDelete responder (guaranteed mid-recreate), and the deferral's completion is observed via
     * the shared watchdog state's recreateInFlight flag (cleared in the recreate's finally on
     * every path), reached through the orderedStops closure the subscription registers.
     */
    public function testWatchdogRecreateDeferredByReconnectRefiresOnceOpenAgain(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);

        $errors = [];
        $options = new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 100, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertIsObject($connection);
        $stateProp = new \ReflectionProperty($connection::class, 'state');

        // Installed AFTER connect (INFO/PONG are pre-queued reads) so the responder closures can
        // capture the reflection handles by value - the first frame it must answer is the initial
        // CONSUMER.CREATE written by subscribeOrderedConsumer() below.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [
                // The watchdog fired and its recreate wrote the DELETE: the connection leaves Open
                // HERE, deterministically inside the in-flight recreate. The delete's reply still
                // settles, then the recreate dies at the fresh-inbox subscribe fail-fast ("Connection
                // is not open") with $newSid never adopted, taking the deferral branch before the
                // create-attempt loop can run (the sibling test below covers the adopted-inbox arm).
                static function (string $rt) use ($connection, $stateProp, $deleteReply): array {
                    $stateProp->setValue($connection, ConnectionState::Connecting);

                    return [self::muxMsg($rt, $deleteReply)];
                },
                // The post-reconnect watchdog retry deletes the same never-replaced instance again.
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
        );

        // 30 ms heartbeat -> the watchdog fires after ~60 ms of total silence.
        $js = $client->jetStream();
        $sid = $js->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, null, 30_000_000)->await();

        $stops = (new \ReflectionProperty(JetStreamContext::class, 'orderedStops'))->getValue($js);
        self::assertIsArray($stops);
        self::assertArrayHasKey($sid, $stops);
        self::assertInstanceOf(\Closure::class, $stops[$sid]);
        $watchdogState = (new \ReflectionFunction($stops[$sid]))->getStaticVariables()['state'] ?? null;
        self::assertInstanceOf(HeartbeatWatchdogState::class, $watchdogState);

        // Phase 1: total silence -> the watchdog fires and the recreate's DELETE hits the wire (the
        // responder above then flips the connection out of Open mid-recreate).
        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.DELETE.EVENTS.ORD1') < 1 && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }
        self::assertSame(1, substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'), 'the watchdog must fire a recreate for the silent consumer');

        // Phase 2: the recreate fail-fasts at the fresh-inbox subscribe and takes the deferral
        // branch; recreateInFlight is cleared in its finally on every path, so this observes the
        // deferral completing.
        $deadlineNs = hrtime(true) + 2_000_000_000;
        while ($watchdogState->recreateInFlight && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }
        self::assertFalse($watchdogState->recreateInFlight, 'the deferred recreate must settle');
        $writtenDeferred = implode('', $transport->writes);
        self::assertSame(1, substr_count($writtenDeferred, '$JS.API.CONSUMER.CREATE.EVENTS'), 'no create can succeed while the connection is not Open');
        self::assertFalse($watchdogState->notified, 'the deferral must clear the miss latch so the watchdog can re-fire once Open');

        // Phase 3: Open again, silence continues -> two idle intervals later the watchdog must fire
        // the fresh-attempt retry the deferral promised.
        $stateProp->setValue($connection, ConnectionState::Open);

        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.CREATE.EVENTS') < 2 && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }
        $written = implode('', $transport->writes);

        $client->disconnect()->await();

        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'the watchdog must re-fire after reconnect and re-establish the deferred consumer');
        // Both episodes target ORD1 because the first one died BEFORE the attempt loop could adopt
        // a rotated candidate name (adopt-before-await) - the sibling test covers that rotation.
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'), 'both recreate episodes delete the same never-replaced instance');
        self::assertSame([], $errors, 'a deferred-then-recovered recreate must stay silent');
    }

    /**
     * Sibling of {@see testWatchdogRecreateDeferredByReconnectRefiresOnceOpenAgain} covering the
     * OTHER deferral arm: here the connection leaves Open inside the recreate's first
     * CONSUMER.CREATE attempt - AFTER the fresh deliver inbox was subscribed and the candidate name
     * adopted (adopt-before-await). Attempt 1 times out, attempts 2-3 fail fast while not Open, and
     * the deferral must release the adopted-but-never-confirmed fresh inbox ($newSid !== null arm),
     * clear the miss latch, and let the post-reconnect watchdog re-fire. The second episode then
     * deletes the ROTATED candidate name (not ORD1), pinning the adopt-before-await consequence.
     */
    public function testWatchdogRecreateDeferredAfterAdoptedInboxRefiresOnceOpenAgain(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);

        $errors = [];
        $options = new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 100, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertIsObject($connection);
        $stateProp = new \ReflectionProperty($connection::class, 'state');

        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                // Recreate attempt 1: the fresh inbox is already subscribed and the candidate name
                // adopted. The connection leaves Open HERE and the reply is withheld, so attempt 1
                // times out (100 ms) and attempts 2-3 fail fast -> the deferral runs its
                // $newSid !== null cleanup.
                static function (string $rt) use ($connection, $stateProp): array {
                    $stateProp->setValue($connection, ConnectionState::Connecting);

                    return [];
                },
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
        );

        // 30 ms heartbeat -> the watchdog fires after ~60 ms of total silence.
        $js = $client->jetStream();
        $sid = $js->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, null, 30_000_000)->await();

        $stops = (new \ReflectionProperty(JetStreamContext::class, 'orderedStops'))->getValue($js);
        self::assertIsArray($stops);
        self::assertArrayHasKey($sid, $stops);
        self::assertInstanceOf(\Closure::class, $stops[$sid]);
        $watchdogState = (new \ReflectionFunction($stops[$sid]))->getStaticVariables()['state'] ?? null;
        self::assertInstanceOf(HeartbeatWatchdogState::class, $watchdogState);

        // Phase 1: silence -> watchdog fires -> DELETE ORD1 -> fresh-inbox SUB -> CREATE attempt 1
        // (which flips the connection out of Open and never answers).
        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.DELETE.EVENTS.ORD1') < 1 && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }

        // Phase 2: attempt 1 times out, 2-3 fail fast, the deferral releases the adopted inbox and
        // settles (recreateInFlight cleared in the finally on every path).
        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (($watchdogState->recreateInFlight || substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.CREATE.EVENTS') < 2) && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }
        self::assertFalse($watchdogState->recreateInFlight, 'the deferred recreate must settle');
        $writtenDeferred = implode('', $transport->writes);
        self::assertSame(2, substr_count($writtenDeferred, '$JS.API.CONSUMER.CREATE.EVENTS'), 'only the initial create and the timed-out attempt 1 can reach the wire');
        self::assertFalse($watchdogState->notified, 'the deferral must clear the miss latch so the watchdog can re-fire once Open');

        // Phase 3: Open again, silence continues -> the watchdog must fire a fresh episode, which
        // deletes the ROTATED adopted candidate (attempt 1's client-chosen name), not ORD1.
        $stateProp->setValue($connection, ConnectionState::Open);

        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.CREATE.EVENTS') < 3 && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }
        $written = implode('', $transport->writes);

        $client->disconnect()->await();

        self::assertSame(3, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'the watchdog must re-fire after reconnect and re-establish the deferred consumer');
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.'), 'each episode must delete exactly one prior instance');
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'), 'the second episode must target the rotated adopted candidate, not ORD1');
        self::assertSame([], $errors, 'a deferred-then-recovered recreate must stay silent');
    }

    /**
     * Verifies unpinConsumer rejects an empty priority group name.
     */
    public function testUnpinConsumerRejectsEmptyGroup(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Priority group must not be empty');

        $client->jetStream()->unpinConsumer('ORDERS', 'PROC', '')->await();
    }

    /**
     * Verifies publish() with an expectation header immediately re-throws a precondition-mismatch
     * JetStreamException without retrying (non-503 code triggers immediate throw).
     *
     * This variant uses expectedLastSubjectSequence to produce the header path, so the publish
     * goes via requestWithHeaders (HPUB), not plain PUB.
     */
    public function testPublishWithLastSubjectSequenceHeaderMismatchThrowsImmediately(): void
    {
        $errorAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorAck), $errorAck),
        ]);

        // Use retryAttempts=3 so that a retry would produce a second request (proving we don't retry).
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $js = new JetStreamContext($client, publishRetryAttempts: 3, publishRetryWaitMs: 1);

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('wrong last sequence');

        $js->publish('orders.created', 'body', expectedLastSubjectSequence: 0)->await();

        // If the exception propagates there was only one request (no retry).
        self::assertCount(4, $transport->writes); // CONNECT + PING + SUB + HPUB
    }

    /**
     * Verifies counterValue() re-throws a non-404 JetStreamException from the Direct Get
     * (the throw-if-not-404 branch).
     */
    public function testCounterValueRethrowsNon404Exception(): void
    {
        // A 403 status from directGet maps to a JetStreamException with code 403.
        $hdrs = "NATS/1.0 403 Forbidden\r\nDescription: access denied\r\n\r\n";
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("HMSG _INBOX.a 1 %d %d\r\n%s\r\n", $h, $h, $hdrs),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(403);

        $client->jetStream()->counterValue('COUNTERS', 'counters.visits')->await();
    }

    /**
     * Verifies parseCounterValue() wraps a JsonException in a JetStreamException
     * (malformed counter payload).
     */
    public function testIncrementCounterWithMalformedResponsePayload(): void
    {
        // The server returns a non-JSON body for the counter response.
        $badPayload = 'NOT_JSON{{{';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($badPayload), $badPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed counter response');

        $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
    }

    /**
     * Verifies parseCounterValue() maps an embedded API error to a JetStreamException
     * (error key present in counter response).
     */
    public function testIncrementCounterWithApiErrorInResponse(): void
    {
        $errorPayload = '{"error":{"code":400,"description":"counter not enabled on stream"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('counter not enabled on stream');

        $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
    }

    /**
     * Verifies parseCounterValue() returns a string for an integer val field
     * (is_int($val) branch, returns string cast).
     */
    public function testIncrementCounterWithIntegerValField(): void
    {
        // The server may return an unquoted integer for the val field on some code paths.
        $payload = '{"stream":"COUNTERS","seq":3,"val":7}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($payload), $payload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->incrementCounter('counters.visits', '+1')->await();

        self::assertSame('7', $value);
    }

    /**
     * Verifies parseCounterValue() throws when neither int nor string val is present
     * (missing val field).
     */
    public function testIncrementCounterWithMissingValFieldThrows(): void
    {
        $payload = '{"stream":"COUNTERS","seq":4}'; // no "val" key

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($payload), $payload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Counter response did not include a value');

        $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
    }

    /**
     * Verifies fetchBatch() throws a 408 JetStreamException when no messages arrive and no
     * terminal status is received (pure timeout path, empty messages, no status).
     */
    public function testFetchBatchThrowsTimeoutWhenNoMessagesAndNoTerminalStatus(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('No messages received within timeout');
        $this->expectExceptionCode(408);

        // expiresMs=1 causes the TimeoutCancellation to fire almost immediately with no messages.
        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 1)->await();
    }

    /**
     * Verifies ackSync() throws JetStreamException when replyTo is an empty string.
     */
    public function testAckSyncThrowsForEmptyReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream ACK requires a reply subject on the delivered message');

        $message = new NatsMessage('events.x', 1, '', 'body');
        $client->jetStream()->ackSync($message)->await();
    }

    /**
     * Verifies applyFilterSubjects() (via createConsumer) throws when filter_subjects is not an
     * array (the non-array branch of the array_key_exists guard).
     */
    public function testCreateConsumerRejectsNonArrayFilterSubjects(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('filter_subjects must be a non-empty array of subjects');

        try {
            // Passing a string instead of an array for filter_subjects.
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['filter_subjects' => 'orders.>'])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies applyFilterSubjects() (via createConsumer) throws when filter_subjects is an empty
     * array (the $subjects === [] branch).
     */
    public function testCreateConsumerRejectsEmptyArrayFilterSubjects(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('filter_subjects must be a non-empty array of subjects');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['filter_subjects' => []])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies buildPullRequest() (via fetchBatch) throws when the pull group name is invalid
     * (group validation failure).
     */
    public function testFetchBatchRejectsInvalidPullGroupName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Pull group must be 1..16 characters of [A-Za-z0-9-_/=]');

        try {
            // A group name with spaces is invalid.
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['group' => 'invalid group!'])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies assertValidPriorityConfig() (via createConsumer) throws when priority_groups is
     * an empty array.
     */
    public function testCreateConsumerRejectsEmptyPriorityGroups(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('priority_groups must be a non-empty array of group names');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['priority_groups' => []])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies assertValidPriorityConfig() (via createConsumer) throws when a priority group name
     * contains invalid characters.
     */
    public function testCreateConsumerRejectsInvalidPriorityGroupName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('priority_groups names must be 1..16 characters of [A-Za-z0-9-_/=]');

        try {
            // A group name with spaces is invalid.
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['priority_groups' => ['invalid name!']])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies directGetBatch() surfaces the CancelledException (as a JetStreamException) when the
     * wait-cancellation fires while processIncoming is blocked on an idle socket (blockWhenEmpty
     * path) before the batch completes.
     *
     * The method must NOT silently return a (here empty) prefix as if the batch were complete: the
     * deadline firing before any end-of-batch marker is an incomplete-result error (#121).
     */
    public function testDirectGetBatchThrowsWhenDeadlineFiresBeforeCompletion(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // expiresMs=1 makes the TimeoutCancellation fire after ~1001 ms; the blocking transport
        // keeps processIncoming suspended so the cancellation is the only way out. The batch never
        // completed, so it surfaces as a JetStreamException rather than a silent empty result.
        $this->expectException(JetStreamException::class);
        $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10], 1)->await();
    }

    /**
     * Verifies jsRequest() re-throws a non-"No responders" NatsException unchanged
     * when the underlying client->request() fails with a TimeoutException.
     *
     * Triggered via incrementCounter(), which calls jsRequest() directly.
     */
    public function testJsRequestRethrowsNonNoRespondersNatsException(): void
    {
        // blockWhenEmpty keeps processIncoming() suspended so the request's TimeoutCancellation
        // (requestTimeoutMs=1 ms) fires during the read, producing a TimeoutException.  That
        // exception is a NatsException whose message does NOT contain "No responders", so
        // jsRequest() re-throws it unchanged.
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1), $transport);
        $client->connect()->await();

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Request timed out');

        // incrementCounter calls jsRequest() directly (without publishWithRetry), so any
        // NatsException that isn't "No responders" surfaces.
        $client->jetStream()->incrementCounter('counters.hits', '+1')->await();
    }

    /**
     * Verifies publishWithRetry() re-throws a JetStreamException when all configured
     * retry attempts are exhausted on transient 503 "no-responder" failures.
     *
     * The publish() path goes: publish() -> publishWithRetry() -> jsRequest() -> client->request().
     * A 503 HMSG status makes requestInternal() throw NatsException("No responders..."), which
     * jsRequest() converts to JetStreamException(503).  publishWithRetry() retries up to
     * $publishRetryAttempts times; on the final attempt ($attempt >= $attempts) it re-throws.
     */
    public function testPublishWithRetryRethrowsWhenRetriesExhausted(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";

        // Two 503 frames: attempt 1 gets the first 503 (retried), attempt 2 gets the second 503
        // (attempt >= attempts=2 -> the retries-exhausted re-throw fires).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            'HMSG _INBOX.b 2 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // publishRetryAttempts=2, publishRetryWaitMs=1 (tight loop so the test is fast).
        $js = new JetStreamContext($client, publishRetryAttempts: 2, publishRetryWaitMs: 1);

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(503);
        $this->expectExceptionMessage('No JetStream responder');

        $js->publish('orders.created', '{"id":1}')->await();
    }

    /**
     * Verifies createStream() rejects a dotted stream name before any $JS.API write reaches the
     * wire: a '.' in the name would corrupt the API subject and hit the wrong endpoint (#131).
     */
    public function testCreateStreamRejectsDottedStreamNameBeforeDispatch(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->createStream('bad.name', ['s.*'])->await();
        } finally {
            // Only CONNECT and PING were written - the invalid name never produced a $JS.API PUB.
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies createConsumer() rejects a dotted consumer name before dispatch: the server would
     * route CONSUMER.CREATE.S.a.b as the filtered-create form (consumer "a", filter "b") (#131).
     */
    public function testCreateConsumerRejectsDottedConsumerName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'a.b')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies getConsumer() rejects a dotted consumer name before dispatch: CONSUMER.INFO.S.a.b
     * hits no API route and would surface a misleading 503 "subject is not bound to a stream" (#131).
     */
    public function testGetConsumerRejectsDottedConsumerName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->getConsumer('ORDERS', 'a.b')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies deleteConsumer() rejects a dotted consumer name before dispatch (#131).
     */
    public function testDeleteConsumerRejectsDottedConsumerName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->deleteConsumer('ORDERS', 'a.b')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies directGetStreamMessage() rejects a dotted stream name before dispatch: the server
     * could route DIRECT.GET.FOO.BAR as DIRECT.GET.<stream>.<last_by_subject> and silently return
     * data from a SIBLING stream (#131).
     */
    public function testDirectGetStreamMessageRejectsDottedStreamName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "FOO.BAR"');

        try {
            $client->jetStream()->directGetStreamMessage('FOO.BAR', 1)->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies an empty stream name is rejected before dispatch (#131).
     */
    public function testCreateStreamRejectsEmptyStreamName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('must be non-empty');

        try {
            $client->jetStream()->createStream('', ['s.*'])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a wildcard '*' stream name is rejected before dispatch (#131).
     */
    public function testGetStreamRejectsWildcardStreamName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "*"');

        try {
            $client->jetStream()->getStream('*')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a valid name (letters, digits, '-' and '_') passes validation and produces the
     * expected $JS.API subject on the wire (#131).
     */
    public function testCreateStreamAcceptsValidNameWithHyphenAndUnderscore(): void
    {
        $streamPayload = '{"config":{"name":"ORDERS-2_prod","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamPayload), $streamPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $created = $client->jetStream()->createStream('ORDERS-2_prod', ['orders.*'])->await();

        self::assertSame('ORDERS-2_prod', $created->name);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.ORDERS-2_prod', $transport->writes[3]);
    }

    /**
     * #122(1): a recreate whose FIRST CONSUMER.CREATE reply is lost (here a -ERR raised as a
     * ConnectionException, NOT a definitive JetStream API rejection) can leave the just-created
     * ephemeral alive server-side while the retry adopts a DIFFERENT consumer. The library now
     * chooses each recreate consumer name CLIENT-SIDE, so it can best-effort-delete that orphan by
     * name even though it never saw its create reply. Asserts a DELETE for the first (orphaned)
     * attempt name reaches the wire and the adopted name is NOT deleted.
     */
    public function testSubscribeOrderedConsumerReapsOrphanFromLostCreateReply(): void
    {
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        // Mux inbox (#118): replies echo on captured reply-tos; deliver1 is sid 2. The recreate deletes
        // ORD1, create attempt 1's reply is "lost" (a -ERR raised as ConnectionException), attempt 2
        // succeeds as ORD2, then the orphaned attempt-1 name is best-effort-reaped (a second DELETE).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                // Attempt 1: a -ERR (ConnectionException) - the reply is lost, the consumer may exist.
                static fn (string $rt): array => ["-ERR 'Stale Connection'\r\n"],
                // Attempt 2 succeeds as ORD2 and is adopted.
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            // The current ORD1 delete, then the best-effort reap of the orphaned attempt-1 name.
            onDelete: [
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // In-order msg1 (consumer seq 1, stream seq 1) -> expected next 2, lastStreamSeq 1.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap (consumer seq 3) triggers recovery.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 8; $i++) {
            $client->processIncoming()->await();
        }

        // Only the in-order message was delivered; the gap message was discarded; recovery succeeded
        // on the retry so nothing was reported as terminally dead.
        self::assertSame(['msg1'], $received);
        self::assertSame([], $errors);

        $written = implode('', $transport->writes);
        // Both recreate attempts carry a client-chosen name in the create config, so a lost-reply
        // orphan is deletable by name.
        preg_match_all('/"name":"(ord-[0-9a-f]+)"/', $written, $all);
        $attempted = $all[1];
        self::assertCount(2, $attempted, 'exactly two recreate create attempts, each with a client-chosen name');
        [$orphanName, $adoptedName] = $attempted;
        // The orphan (first attempt, reply lost) is best-effort-deleted by its client-chosen name ...
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.EVENTS.' . $orphanName, $written);
        // ... while the adopted (successful) consumer is NOT reaped.
        self::assertStringNotContainsString('$JS.API.CONSUMER.DELETE.EVENTS.' . $adoptedName, $written);
    }

    /**
     * #122(2), falsifiable core: after a recreate ROTATES the deliver inbox, an orphan's PLAIN idle
     * heartbeat arriving on the OLD (now unsubscribed) inbox must NOT drive a further recreate. The
     * client dropped its interest on that inbox, so neither the orphan's data nor its plain heartbeats
     * reach the tail-gap check. Before rotation, that plain heartbeat (attributable to no consumer by
     * subject) drove a spurious recreate storm - the exact defect #122 targets.
     */
    public function testSubscribeOrderedConsumerIgnoresOrphanHeartbeatOnRotatedOldInbox(): void
    {
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        // A PLAIN idle heartbeat (no FC reply subject, no Nats-Consumer-Stalled) reporting the server
        // delivered up to consumer seq 9 - far ahead of anything the current consumer processed.
        $orphanHb = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Last-Consumer' => '9',
        ]);

        // Mux inbox (#118): CREATE/DELETE replies echo on captured reply-tos; deliver1 is sid 2. The
        // orphan's PLAIN idle heartbeat must arrive on the OLD inbox AFTER the recreate unsubscribes it,
        // so it rides the successful recreate create's reply (which precedes the sid-2 UNSUB) - it must
        // then be dropped: no tail-gap check, no second recreate.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [
                    self::muxMsg($rt, $createReply('ORD2')),
                    sprintf("HMSG deliver.ord 2 %d %d\r\n%s\r\n", strlen($orphanHb), strlen($orphanHb), $orphanHb),
                ],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // In-order msg1 (consumer seq 1, stream seq 1) on the ORIGINAL inbox (sid 2).
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Data gap (consumer seq 3) -> the ONE legitimate recreate.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $errors = [];
        $options = new NatsOptions(requestTimeoutMs: 50, errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 8; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1'], $received);
        self::assertSame([], $errors);

        $written = implode('', $transport->writes);
        // Exactly ONE recreate (the data gap): the orphan plain heartbeat on the old inbox added no
        // second DELETE and no third CREATE - the storm #122 removes.
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'), 'the orphan heartbeat on the old inbox must not delete any consumer');
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'the orphan heartbeat on the old inbox must not recreate the consumer');
    }

    /**
     * #122(2): a recreate ROTATES the deliver inbox - it subscribes a FRESH inbox (a new sid), points
     * the new CONSUMER.CREATE at that fresh deliver subject, and UNSUBSCRIBES the previous inbox. This
     * is what makes an orphan on the old inbox unreachable (and lets its inactive_threshold reap it).
     */
    public function testSubscribeOrderedConsumerRotatesDeliverInboxAndUnsubscribesOld(): void
    {
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        // Mux inbox (#118): CREATE/DELETE replies echo on captured reply-tos; deliveries ride the
        // deliver SUB sid. deliver1 is sid 2 (mux is sid 1), so the rotation still UNSUBs sid 2.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Data gap -> recreate.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        $written = implode('', $transport->writes);
        // The original deliver subscription (sid 2) is unsubscribed on rotation.
        self::assertStringContainsString("UNSUB 2\r\n", $written);
        // Two DISTINCT deliver inboxes were subscribed (the initial + the rotated one).
        preg_match_all('/SUB (_INBOX\.JS\.ORD\.\S+) \d+\r\n/', $written, $subs);
        self::assertCount(2, $subs[1], 'a recreate must subscribe a fresh deliver inbox');
        self::assertNotSame($subs[1][0], $subs[1][1], 'the rotated deliver inbox must differ from the original');
        // The recreate CONSUMER.CREATE points at the ROTATED deliver subject, not the original.
        preg_match_all('/"deliver_subject":"(_INBOX\.JS\.ORD\.[^"]+)"/', $written, $delivers);
        self::assertCount(2, $delivers[1], 'both creates carry a rotated deliver_subject');
        self::assertNotSame($delivers[1][0], $delivers[1][1], 'the recreate must point CONSUMER.CREATE at the rotated deliver subject');
    }

    /**
     * #122(3): on a TERMINAL recreate failure (all attempts exhausted) the ordered subscription is
     * torn down (the deliver sid is unsubscribed) so "dead" is actually dead - the deliver inbox
     * stops receiving traffic - rather than lingering to receive filtered orphan/stale frames.
     */
    public function testSubscribeOrderedConsumerTearsDownSubscriptionOnTerminalRecreateFailure(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        $createError = '{"error":{"code":404,"description":"stream not found"}}';

        // Mux inbox (#118): initial create OK, every recreate create fails (terminal). Replies echo on
        // captured reply-tos; deliver1 is sid 2. The late "tail" frame must arrive AFTER the terminal
        // teardown unsubscribes sid 2, so it rides the LAST failing create's reply (post-teardown drop).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, (string) $createReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [
                    self::muxMsg($rt, $createError),
                    "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.2.2.2.0.0 4\r\ntail\r\n",
                ],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap triggers recovery.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 10; $i++) {
            $client->processIncoming()->await();
        }

        // The terminal failure was surfaced, and the deliver subscription (sid 2) was unsubscribed.
        self::assertCount(1, $errors);
        self::assertStringContainsString('after 3 attempts', $errors[0]->getMessage());
        self::assertStringContainsString("UNSUB 2\r\n", implode('', $transport->writes));
        // The late frame arriving after teardown is not delivered to the handler.
        self::assertSame(['msg1'], $received);
    }

    /**
     * #121: a non-100 status control frame (409/408/503) on a push deliver subject is a JetStream
     * control/error frame, never user data. It must be intercepted and NOT forwarded to the user
     * handler as if it were a message payload.
     */
    public function testSubscribeEphemeralPushConsumerDropsNon100StatusFrames(): void
    {
        $createReply = '{"stream_name":"ORDERS","name":"EPH","config":{"deliver_subject":"deliver.eph","ack_policy":"none"}}';
        $status = static fn (string $code, string $desc): string => NatsHeaders::toWireBlock([
            'Status' => $code,
            'Description' => $desc,
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); the status frames (sid 2) arrive after the deliver SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        $statusFrames = [];
        foreach ([['409', 'Consumer Deleted'], ['503', 'No Responders'], ['408', 'Request Timeout']] as [$code, $desc]) {
            $block = $status($code, $desc);
            $statusFrames[] = sprintf("HMSG deliver.eph 2 %d %d\r\n%s\r\n", strlen($block), strlen($block), $block);
        }
        $transport->enqueueOnWriteContaining['SUB deliver.eph '] = $statusFrames;

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            'deliver.eph',
        )->await();

        for ($i = 0; $i < 5; $i++) {
            $client->processIncoming()->await();
        }

        // None of the status control frames were delivered to the user handler as data.
        self::assertSame([], $received);
    }

    /**
     * #121: a CALLER-OWNED push consumer (subscribeEphemeralPushConsumer / subscribePushConsumer) that
     * receives a TERMINAL (4xx/5xx) status frame surfaces it through the error listener as a
     * descriptive JetStreamException instead of silently dropping it - the library cannot recreate a
     * consumer whose lifecycle it does not own, so the drop is made observable. A status-100 heartbeat
     * stays intercepted-and-silent (not surfaced).
     */
    public function testCallerOwnedPushConsumerSurfacesTerminalStatusViaErrorListener(): void
    {
        $createReply = '{"stream_name":"ORDERS","name":"EPH","config":{"deliver_subject":"deliver.eph","ack_policy":"none"}}';
        $hb = NatsHeaders::toWireBlock(['Status' => '100', 'Description' => 'Idle Heartbeat']);
        $terminal = NatsHeaders::toWireBlock(['Status' => '409', 'Description' => 'Consumer Deleted']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // CREATE reply via the mux inbox (#118); the status frames (sid 2) arrive after the deliver SUB.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.eph '] = [
            // A status-100 idle heartbeat: intercepted and NOT surfaced.
            sprintf("HMSG deliver.eph 2 %d %d\r\n%s\r\n", strlen($hb), strlen($hb), $hb),
            // A terminal 409 Consumer Deleted: intercepted AND surfaced via the error listener.
            sprintf("HMSG deliver.eph 2 %d %d\r\n%s\r\n", strlen($terminal), strlen($terminal), $terminal),
        ];

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            'deliver.eph',
        )->await();

        for ($i = 0; $i < 5; $i++) {
            $client->processIncoming()->await();
        }

        // Neither control frame reached the handler as data.
        self::assertSame([], $received);
        // Exactly the terminal 409 was surfaced; the status-100 heartbeat was not.
        self::assertCount(1, $errors);
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertSame(409, $errors[0]->getCode());
        self::assertStringContainsString('terminal status 409', $errors[0]->getMessage());
        self::assertStringContainsString('Consumer Deleted', $errors[0]->getMessage());
    }

    /**
     * #121 (mutation-hardening): the DURABLE caller-owned push path (subscribePushConsumer) must surface a
     * terminal status through the error listener exactly like the ephemeral path above. This pins the
     * still-uncovered corners of that path/helper: the durable subscribe callback surfacing at all
     * (surfaceCallerOwnedPushStatus reached with the REAL control headers, not [] - kills the durable
     * Coalesce/MethodCallRemoval), and the `status < 400` boundary at EXACTLY 400 (a 409 does not
     * distinguish `<` from `<=`). The ephemeral test above uses 409 on the ephemeral path, so neither is
     * exercised. (The Description's own trim() in surfaceCallerOwnedPushStatus is a no-op - NatsHeaders
     * already trims every parsed header value - so it is an equivalent mutant, not pinned here.)
     */
    public function testDurableCallerOwnedPushConsumerSurfacesStatus400(): void
    {
        $createReply = '{"stream_name":"ORDERS","name":"DUR","config":{"deliver_subject":"deliver.dur","ack_policy":"none"}}';
        // Status EXACTLY 400 - the `< 400` boundary that a 409 (used by the ephemeral test) cannot pin.
        $terminal = NatsHeaders::toWireBlock(['Status' => '400', 'Description' => 'Bad Request']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.dur '] = [
            sprintf("HMSG deliver.dur 2 %d %d\r\n%s\r\n", strlen($terminal), strlen($terminal), $terminal),
        ];

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribePushConsumer(
            'ORDERS',
            'DUR',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            'deliver.dur',
        )->await();

        for ($i = 0; $i < 5; $i++) {
            $client->processIncoming()->await();
        }

        // The status frame was intercepted (not delivered as data) and surfaced via the error listener.
        self::assertSame([], $received);
        self::assertCount(1, $errors);
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertSame(400, $errors[0]->getCode());
        self::assertStringContainsString('terminal status 400', $errors[0]->getMessage());
        self::assertStringContainsString('(Bad Request)', $errors[0]->getMessage());
    }

    /**
     * #121: a JetStream publish ack that carries neither an `error` nor a `stream` is invalid
     * (nats.go rejects an empty-stream ack). It must throw rather than be accepted as a bogus
     * PubAck('', 0) success.
     */
    public function testPublishRejectsAckWithoutStream(): void
    {
        // A JSON ack with no `error` and no `stream`.
        $ackPayload = '{"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('stream');

        $client->jetStream()->publish('orders.created', '{"id":1}')->await();
    }

    /**
     * #121: directGetBatch() must NOT silently return a truncated prefix when the deadline fires
     * before the batch completes (no 204 EOB and no Nats-Num-Pending: 0). It has an explicit
     * completion signal, so a timeout before completion is an error.
     */
    public function testDirectGetBatchThrowsOnTimeoutBeforeCompletion(): void
    {
        $h1 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\n\r\n";
        $b1 = 'aaa';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // One data message, then NO end-of-batch marker (204 / Num-Pending:0) - the batch never
            // completes and the wait deadline fires.
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h1), strlen($h1) + strlen($b1), $h1, $b1),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        // expiresMs=100 -> internal deadline ~1.1s; the incomplete batch surfaces as an error.
        $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10], 100)->await();
    }

    /**
     * A connected client over a FakeTransport seeded only with INFO + PONG, so the two handshake
     * writes (CONNECT, PING) are the sole entries in $transport->writes until a request dispatches
     * one more frame. The name-validation regressions below assert count stays at 2, proving an
     * invalid stream/consumer name is rejected before any $JS.API PUB reaches the wire.
     *
     * @return array{NatsClient, FakeTransport}
     */
    private function connectedForNameGuard(): array
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        return [$client, $transport];
    }

    /**
     * Verifies updateStream() rejects a dotted stream name before dispatch: the '.' would corrupt
     * the $JS.API.STREAM.UPDATE subject and hit the wrong endpoint (#131).
     */
    public function testUpdateStreamRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->updateStream('bad.name', ['max_msgs' => 1])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies addStream() rejects a StreamConfiguration whose name is dotted before dispatch (#131).
     */
    public function testAddStreamRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->addStream(StreamConfiguration::create('bad.name')->subjects('s.*'))->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies deleteStream() rejects a dotted stream name before dispatch (#131).
     */
    public function testDeleteStreamRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->deleteStream('bad.name')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies purgeStream() rejects a dotted stream name before dispatch (#131).
     */
    public function testPurgeStreamRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->purgeStream('bad.name')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies listConsumers() rejects a dotted stream name before dispatch (#131).
     */
    public function testListConsumersRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->listConsumers('bad.name')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies consumerNames() rejects a dotted stream name before dispatch (#131).
     */
    public function testConsumerNamesRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->consumerNames('bad.name')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies getStreamMessage() rejects a dotted stream name before dispatch (#131).
     */
    public function testGetStreamMessageRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->getStreamMessage('bad.name', 1)->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies getLastMessageForSubject() rejects a dotted stream name before dispatch (#131).
     */
    public function testGetLastMessageForSubjectRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->getLastMessageForSubject('bad.name', 'orders.created')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies deleteMessage() rejects a dotted stream name before dispatch (#131).
     */
    public function testDeleteMessageRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->deleteMessage('bad.name', 1)->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies directGetLastMessageForSubject() rejects a dotted stream name before dispatch (#131).
     */
    public function testDirectGetLastMessageForSubjectRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->directGetLastMessageForSubject('bad.name', 'orders.created')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies directGetBatch() rejects a dotted stream name before dispatch (#131).
     */
    public function testDirectGetBatchRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->directGetBatch('bad.name', ['batch' => 1])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies createConsumer() rejects a dotted STREAM name before dispatch (the consumer-name
     * guard is covered separately); a '.' in the stream corrupts the CONSUMER.CREATE subject (#131).
     */
    public function testCreateConsumerRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->createConsumer('bad.name', 'durable')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies createEphemeralConsumer() rejects a dotted stream name before dispatch (#131).
     */
    public function testCreateEphemeralConsumerRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->createEphemeralConsumer('bad.name')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies getConsumer() rejects a dotted STREAM name before dispatch (its consumer-name guard
     * is covered by testGetConsumerRejectsDottedConsumerName); CONSUMER.INFO.bad.name misroutes (#131).
     */
    public function testGetConsumerRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->getConsumer('bad.name', 'durable')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies pullConsumer() rejects a dotted STREAM name synchronously, before building the
     * iterator - a '.' in the stream would misroute the consumer's pull requests (#131).
     */
    public function testPullConsumerRejectsDottedStreamName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->pullConsumer('bad.name', 'durable');
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies pullConsumer() rejects a dotted CONSUMER name synchronously (valid stream), guarding
     * the second name check independently of the stream guard (#131).
     */
    public function testPullConsumerRejectsDottedConsumerName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->pullConsumer('ORDERS', 'a.b');
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies addConsumer() rejects a dotted CONSUMER name (from the config) before dispatch, with
     * a valid stream so the stream guard passes and the consumer guard is exercised in isolation (#131).
     */
    public function testAddConsumerRejectsDottedConsumerName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->addConsumer('ORDERS', ConsumerConfiguration::create()->durable('a.b'))->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies createPushConsumer() rejects a dotted CONSUMER name before dispatch (valid stream) (#131).
     */
    public function testCreatePushConsumerRejectsDottedConsumerName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->createPushConsumer('ORDERS', 'a.b', 'deliver.here')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies pauseConsumer() rejects a dotted CONSUMER name before dispatch (valid stream) (#131).
     */
    public function testPauseConsumerRejectsDottedConsumerName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->pauseConsumer('ORDERS', 'a.b', '2030-01-01T00:00:00Z')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies resumeConsumer() rejects a dotted CONSUMER name before dispatch (valid stream) (#131).
     */
    public function testResumeConsumerRejectsDottedConsumerName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->resumeConsumer('ORDERS', 'a.b')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies unpinConsumer() rejects a dotted CONSUMER name before dispatch (valid stream); the
     * consumer guard fires ahead of the empty-group check (#131).
     */
    public function testUnpinConsumerRejectsDottedConsumerName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->unpinConsumer('ORDERS', 'a.b', 'group1')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies fetchBatch() rejects a dotted CONSUMER name before dispatch (valid stream) (#131).
     */
    public function testFetchBatchRejectsDottedConsumerName(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'a.b', 1)->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * directGet() maps ONLY the no-responders NatsException to the catchable "Direct Get is
     * unavailable" 503; every OTHER NatsException must pass through UNCHANGED. Here a silent
     * responder times the request out: the caller must see the raw TimeoutException (a transient
     * transport condition, retryable) - not a 503 that would misdirect it to the leader-path
     * fallback for a stream whose allow_direct is actually fine.
     */
    public function testDirectGetRethrowsNonNoRespondersNatsExceptionUnchanged(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 100, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->directGetLastMessageForSubject('ORDERS', 'orders.1')->await();
            self::fail('a silent Direct Get responder must time the request out');
        } catch (\Throwable $e) {
            self::assertInstanceOf(TimeoutException::class, $e, 'a non-no-responders NatsException must not be re-wrapped');
            self::assertStringContainsString('Request timed out', $e->getMessage());
        }
    }

    /**
     * chunkExactSubjectsForBatch() documented degenerate case: when max_payload is smaller than the
     * batched Direct Get request envelope, the payload budget clamps to its floor, every subject
     * occupies its own chunk, and the resulting oversized request is rejected LOUDLY by publish()'s
     * max_payload guard - a clear failure instead of a silent hang, an infinite split loop, or
     * dropped subjects. The reply inbox subscribed for the failed chunk must still be released.
     */
    public function testDirectGetLastForSubjectsFailsLoudlyWhenMaxPayloadIsBelowTheBatchEnvelope(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":16,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.a', 'orders.b'])->await();
            self::fail('an unsendable single-subject chunk must be rejected by the max_payload guard');
        } catch (\IDCT\NATS\Exception\ProtocolException $e) {
            self::assertStringContainsString('exceeds server max_payload of 16', $e->getMessage());
        }

        // The failed chunk's reply inbox (the first SUB, sid 1 - no mux inbox exists because
        // Direct Get batches publish rather than request()) was released on the failure path.
        self::assertStringContainsString("UNSUB 1\r\n", implode('', $transport->writes));
    }

    /**
     * subscribeEphemeralPushConsumer()'s onConsumerCreated hook (#99): invoked with the CREATE
     * response's parsed ConsumerInfo BEFORE the deliver subscription is established, so a caller
     * can inspect num_pending and arm an end-of-initial-data signal for a consumer that starts
     * with nothing pending.
     */
    public function testSubscribeEphemeralPushConsumerInvokesOnConsumerCreatedBeforeSubscribing(): void
    {
        $createReply = '{"stream_name":"ORDERS","name":"E7","config":{"deliver_subject":"deliver.eph","ack_policy":"none"},"num_pending":0}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $hookName = '';
        $hookNumPending = null;
        $deliverSubsAtHook = -1;
        $sid = $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (NatsMessage $message): void {},
            deliverSubject: 'deliver.eph',
            onConsumerCreated: static function (\IDCT\NATS\JetStream\Models\ConsumerInfo $info) use ($transport, &$hookName, &$hookNumPending, &$deliverSubsAtHook): void {
                $hookName = $info->name;
                $hookNumPending = $info->raw['num_pending'] ?? null;
                $deliverSubsAtHook = count(array_filter(
                    $transport->writes,
                    static fn (string $w): bool => str_starts_with($w, 'SUB deliver.eph'),
                ));
            },
        )->await();

        self::assertSame('E7', $hookName, 'the hook must receive the created consumer');
        self::assertSame(0, $hookNumPending, 'num_pending must be inspectable from the create response');
        self::assertSame(0, $deliverSubsAtHook, 'the hook runs BEFORE the deliver subscription is established');
        // The subscription is then established normally.
        self::assertStringContainsString('SUB deliver.eph ' . $sid, implode('', $transport->writes));
    }

    /**
     * A THROWING onConsumerCreated hook on an ordered consumer must not abort the already-created
     * consumer's setup: the hook error is contained and surfaced through the error listener, and
     * the subscription still goes live and delivers.
     */
    public function testSubscribeOrderedConsumerContainsThrowingOnConsumerCreatedHook(): void
    {
        $createReply = '{"stream_name":"EVENTS","name":"ORD1","config":{"deliver_subject":"deliver.ord","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        // The deliver inbox is sid 2 (mux is sid 1); the delivery arrives once the SUB exists.
        $transport->enqueueOnWriteContaining['SUB _INBOX.JS.ORD'] = [
            "MSG _INBOX.JS.ORD.x 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 5\r\nhello\r\n",
        ];

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            onConsumerCreated: static function (\IDCT\NATS\JetStream\Models\ConsumerInfo $info): void {
                throw new \RuntimeException('consumer-created hook boom');
            },
        )->await();

        $client->processIncoming()->await();

        self::assertSame(['hello'], $received, 'a throwing hook must not abort the consumer setup');
        $hookErrors = array_values(array_filter(
            $errors,
            static fn (\Throwable $e): bool => str_contains($e->getMessage(), 'consumer-created hook boom'),
        ));
        self::assertCount(1, $hookErrors, 'the hook error must be surfaced through the error listener');
    }

    /**
     * Re-entrancy guard of the recreate closure (#113): a SECOND gap trigger dispatched while a
     * recreate is already in flight (here: an out-of-order frame read during the recreate's own
     * CONSUMER.DELETE await) must no-op. The in-flight recreate already resumes from
     * lastStreamSeq+1, so a nested recreate would only orphan an ephemeral: exactly ONE
     * DELETE+CREATE pair may reach the wire for the whole episode, and the replay must still be
     * delivered.
     */
    public function testSubscribeOrderedConsumerCoalescesGapTriggerDuringInFlightRecreate(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $dataFrame = static fn (int $sid, string $consumer, int $delivered, int $sseq, int $cseq, string $body): string => sprintf(
            "MSG deliver.ord %d %s %d\r\n%s\r\n",
            $sid,
            sprintf('$JS.ACK.EVENTS.%s.%d.%d.%d.0.0', $consumer, $delivered, $sseq, $cseq),
            strlen($body),
            $body,
        );
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $createSeen = 0;
        $transport->onWrite = static function (string $bytes) use (&$createSeen, $dataFrame, $createReply): array {
            $replyTo = self::requestReplyTo($bytes);
            if (str_contains($bytes, '$JS.API.CONSUMER.DELETE.')) {
                // The second gap frame (bad4, still from ORD1 on the OLD inbox sid 2) rides AHEAD
                // of the DELETE reply, so it is dispatched during the in-flight recreate's own
                // delete await - the re-entrant $recreate() call this test pins as a no-op.
                return [
                    $dataFrame(2, 'ORD1', 4, 5, 4, 'bad4'),
                    self::muxMsg($replyTo, '{"success":true}'),
                ];
            }
            if (!str_contains($bytes, '$JS.API.CONSUMER.CREATE.')) {
                return [];
            }

            preg_match('/"name":"([^"]+)"/', $bytes, $m);
            $name = $m[1] ?? 'ORD1';
            $createSeen++;

            if ($createSeen === 1) {
                // Initial create (server-assigned name ORD1), then msg1 in order and bad3 exposing
                // the FIRST gap (cseq 3) which starts the recreate.
                return [
                    self::muxMsg($replyTo, $createReply('ORD1')),
                    $dataFrame(2, 'ORD1', 1, 1, 1, 'msg1'),
                    $dataFrame(2, 'ORD1', 3, 4, 3, 'bad3'),
                ];
            }

            // The (single) recreate create: replay msg2 on the rotated inbox (sid 3).
            return [
                $dataFrame(3, $name, 1, 2, 1, 'msg2'),
                self::muxMsg($replyTo, $createReply($name)),
            ];
        };

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 10; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1', 'msg2'], $received, 'both gap exposers are discarded, the replay is delivered');
        self::assertSame([], $errors);

        $written = implode('', $transport->writes);
        // The guard pin: the mid-recreate gap trigger must NOT start a nested episode - one DELETE,
        // two CREATEs total (a nested recreate would have written a second DELETE).
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'), 'a gap trigger during an in-flight recreate must not start a second recreate');
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * The stopped-teardown arms of a stop-raced recreate are BEST-EFFORT: when the fresh (never
     * installed) inbox's UNSUB write fails AND the fresh instance's CONSUMER.DELETE is rejected by
     * the server, the teardown must still complete - local subscription state released, no
     * resurrection, no watchdog re-arm, no terminal error - because dropping local state is what
     * actually stops delivery. Extends the stop-during-recreate race with failing teardown I/O.
     */
    public function testStopDuringInFlightRecreateToleratesTeardownFailures(): void
    {
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        $status409 = NatsHeaders::toWireBlock(['Status' => '409', 'Description' => 'Consumer Deleted']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                // Recreate attempt #1: NO reply - the recreate parks in this await while stop runs.
                static fn (string $rt): array => [],
                // Attempt #2 succeeds - but by then the consumer is stopped: teardown must run.
                static fn (string $rt): array => [
                    // A status control frame on the ROTATED inbox (sid 3), read during this create's
                    // own pump - i.e. AFTER the stop latched - re-triggers $recreate(): the stopped
                    // guard at its head must swallow it (no nested recreate episode may start on a
                    // stopped consumer; status frames bypass the consumer-name filter, so only that
                    // guard stands between the frame and a resurrection).
                    sprintf("HMSG deliver.ord 3 %d %d\r\n%s\r\n", strlen($status409), strlen($status409), $status409),
                    self::muxMsg($rt, $createReply('ORD3')),
                ],
            ],
            onDelete: [
                // The recreate's best-effort delete of ORD1, then the stop closure's delete.
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
                // The stopped-teardown's delete of the never-installed ORD3: server-rejected.
                static fn (string $rt): array => [self::muxMsg($rt, '{"error":{"code":404,"err_code":10014,"description":"consumer not found"}}')],
            ],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap -> recreate starts and parks awaiting attempt #1's (withheld) reply.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );
        // The teardown's UNSUB of the never-installed fresh inbox (sid 3) fails at the transport.
        $transport->throwOnWriteContaining = "UNSUB 3\r\n";

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $js = $client->jetStream();
        $sid = $js->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, 'events.>')->await();

        // Schedule the stop on the event loop so it lands inside attempt #1's parked 300ms await
        // (the gap->recreate saga runs inside one dispatch; see the sibling stop-race test).
        $stopDone = false;
        \Revolt\EventLoop::delay(0.05, static function () use ($js, $sid, &$stopDone): void {
            $js->stopOrderedConsumer($sid)->await();
            $stopDone = true;
        });

        $deadlineNs = hrtime(true) + 5_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.CREATE.EVENTS') < 3 && hrtime(true) < $deadlineNs) {
            try {
                $client->processIncoming(new \Amp\TimeoutCancellation(0.05))->await();
            } catch (\Amp\CancelledException) {
                // Idle slice; timers (the stop, the retry) advance regardless.
            }
        }
        self::assertTrue($stopDone, 'precondition: the scheduled stop must have completed');

        // Wait (bounded) for the stopped-teardown to attempt its CONSUMER.DELETE of ORD3 - the
        // proof that the failed UNSUB right before it was contained rather than aborting teardown.
        $deadlineNs = hrtime(true) + 3_000_000_000;
        while (!str_contains(implode('', $transport->writes), '$JS.API.CONSUMER.DELETE.EVENTS.ORD3') && hrtime(true) < $deadlineNs) {
            delay(0.02);
        }
        // Let the teardown consume the delete rejection and finish.
        delay(0.1);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.EVENTS.ORD3', $written, 'the teardown must still delete the fresh instance after its UNSUB failed');
        // No resurrection: exactly the initial create + the two attempts, nothing after teardown.
        self::assertSame(3, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'a stopped consumer must never be recreated again');
        // The fresh inbox's local state is released even though its UNSUB write failed.
        self::assertFalse($client->isSubscriptionActive(3), 'the never-installed replacement inbox must be released locally');
        self::assertStringNotContainsString("UNSUB 3\r\n", $written, 'precondition: the UNSUB write must have failed before reaching the wire');
        // The contained teardown failures are silent best-effort: no terminal recreate error.
        self::assertSame([], array_values(array_filter(
            $errors,
            static fn (\Throwable $e): bool => str_contains($e->getMessage(), 'recreate failed'),
        )), 'teardown failures on a stopped consumer must not surface as a terminal recreate error');
        // No watchdog was re-armed for the never-installed instance.
        self::assertSame(0, self::countRepeatTimers(), 'a stopped consumer must not re-arm a heartbeat watchdog');

        $transport->throwOnWriteContaining = null;
        $client->disconnect()->await();
    }

    /**
     * A failed UNSUB of the OLD deliver inbox at the end of a SUCCESSFUL recreate is best-effort
     * (#122): it must not undo the recovery. The rotated consumer stays installed and keeps
     * delivering - both the replay read during the recreate and a later post-recreate delivery -
     * and no terminal error is surfaced.
     */
    public function testSubscribeOrderedConsumerRecoverySurvivesOldInboxUnsubscribeFailure(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $dataFrame = static fn (int $sid, string $consumer, int $delivered, int $sseq, int $cseq, string $body): string => sprintf(
            "MSG deliver.ord %d %s %d\r\n%s\r\n",
            $sid,
            sprintf('$JS.ACK.EVENTS.%s.%d.%d.%d.0.0', $consumer, $delivered, $sseq, $cseq),
            strlen($body),
            $body,
        );
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $createSeen = 0;
        $recreateName = '';
        $transport->onWrite = static function (string $bytes) use (&$createSeen, &$recreateName, $dataFrame, $createReply): array {
            $replyTo = self::requestReplyTo($bytes);
            if (str_contains($bytes, '$JS.API.CONSUMER.DELETE.')) {
                return [self::muxMsg($replyTo, '{"success":true}')];
            }
            if (!str_contains($bytes, '$JS.API.CONSUMER.CREATE.')) {
                return [];
            }

            preg_match('/"name":"([^"]+)"/', $bytes, $m);
            $name = $m[1] ?? 'ORD1';
            $createSeen++;

            if ($createSeen === 1) {
                return [
                    self::muxMsg($replyTo, $createReply('ORD1')),
                    $dataFrame(2, 'ORD1', 1, 1, 1, 'msg1'),
                    // Gap (cseq 3) -> recreate.
                    $dataFrame(2, 'ORD1', 3, 4, 3, 'bad3'),
                ];
            }

            // The recreate create: replay msg2 on the rotated inbox (sid 3), echoing the
            // client-chosen name so post-recreate frames can be attributed to it.
            $recreateName = $name;

            return [
                $dataFrame(3, $name, 1, 2, 1, 'msg2'),
                self::muxMsg($replyTo, $createReply($name)),
            ];
        };
        // The rotation's UNSUB of the OLD inbox (sid 2) fails at the transport.
        $transport->throwOnWriteContaining = "UNSUB 2\r\n";

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 10; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1', 'msg2'], $received, 'the recovery must complete despite the failed old-inbox UNSUB');
        self::assertNotSame('', $recreateName, 'precondition: the recreate create must have happened');

        // The rotated consumer is fully live AFTER the failed UNSUB: a later delivery on the new
        // inbox still reaches the handler (the failure occurred after adoption, before return).
        $transport->pushReadChunk($dataFrame(3, $recreateName, 2, 3, 2, 'msg3'));
        $client->processIncoming()->await();
        self::assertSame(['msg1', 'msg2', 'msg3'], $received, 'a failed old-inbox UNSUB must not undo the recovery');

        self::assertSame([], $errors, 'a best-effort UNSUB failure must not surface any error');
        $written = implode('', $transport->writes);
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'no recreate storm after the contained UNSUB failure');
    }

    /**
     * The TERMINAL teardown's inbox unsubscribes are best-effort (#122): when BOTH the fresh and
     * the old inbox UNSUB writes fail at the transport, the teardown must still complete - terminal
     * error surfaced, local subscription state for both inboxes released (so late frames are
     * dropped), nothing escaping the dispatch loop.
     */
    public function testSubscribeOrderedConsumerTerminalTeardownSurvivesUnsubscribeFailures(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        $createError = '{"error":{"code":404,"description":"stream not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, (string) $createReply)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [self::muxMsg($rt, $createError)],
                static fn (string $rt): array => [
                    self::muxMsg($rt, $createError),
                    // Late frame on the old inbox: read after the teardown, must be dropped.
                    "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.2.2.2.0.0 4\r\ntail\r\n",
                ],
            ],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)]],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap triggers recovery; every attempt fails -> terminal teardown.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
            ],
        );
        // Every teardown UNSUB (fresh inbox sid 3, old inbox sid 2) fails at the transport.
        $transport->throwOnWriteContaining = 'UNSUB ';

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 10; $i++) {
            $client->processIncoming()->await();
        }

        // The terminal failure is still surfaced despite both UNSUB writes failing...
        self::assertCount(1, $errors);
        self::assertStringContainsString('after 3 attempts', $errors[0]->getMessage());
        // ...and both inboxes' local state is released even though no UNSUB reached the wire,
        // which is precisely what makes the late tail frame undeliverable.
        self::assertFalse($client->isSubscriptionActive(2), 'the old deliver inbox must be released locally');
        self::assertFalse($client->isSubscriptionActive(3), 'the fresh (never-adopted) inbox must be released locally');
        self::assertStringNotContainsString('UNSUB', implode('', $transport->writes), 'precondition: every UNSUB write must have failed before the wire');
        self::assertSame(['msg1'], $received, 'the late frame arriving after the contained teardown is not delivered');
    }

    /**
     * #121 recreate arm: a non-100 STATUS control frame on the ordered deliver inbox (here a 409
     * "Consumer Deleted") means THIS consumer instance is terminal on the server. It is withheld
     * from the user handler and must trigger a recreate from the last in-order point - delivery
     * resumes with the replacement instead of waiting forever for pushes that cannot come.
     */
    public function testSubscribeOrderedConsumerRecreatesOnTerminalStatusControlFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $dataFrame = static fn (int $sid, string $consumer, int $delivered, int $sseq, int $cseq, string $body): string => sprintf(
            "MSG deliver.ord %d %s %d\r\n%s\r\n",
            $sid,
            sprintf('$JS.ACK.EVENTS.%s.%d.%d.%d.0.0', $consumer, $delivered, $sseq, $cseq),
            strlen($body),
            $body,
        );
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $status409 = NatsHeaders::toWireBlock(['Status' => '409', 'Description' => 'Consumer Deleted']);

        $createSeen = 0;
        $transport->onWrite = static function (string $bytes) use (&$createSeen, $dataFrame, $createReply, $status409): array {
            $replyTo = self::requestReplyTo($bytes);
            if (str_contains($bytes, '$JS.API.CONSUMER.DELETE.')) {
                return [self::muxMsg($replyTo, '{"success":true}')];
            }
            if (!str_contains($bytes, '$JS.API.CONSUMER.CREATE.')) {
                return [];
            }

            preg_match('/"name":"([^"]+)"/', $bytes, $m);
            $name = $m[1] ?? 'ORD1';
            $createSeen++;

            if ($createSeen === 1) {
                return [
                    self::muxMsg($replyTo, $createReply('ORD1')),
                    $dataFrame(2, 'ORD1', 1, 1, 1, 'msg1'),
                    // Terminal status control frame on the deliver inbox (sid 2): consumer gone.
                    sprintf("HMSG deliver.ord 2 %d %d\r\n%s\r\n", strlen($status409), strlen($status409), $status409),
                ];
            }

            // The recreate: delivery resumes on the rotated inbox (sid 3).
            return [
                $dataFrame(3, $name, 1, 2, 1, 'msg2'),
                self::muxMsg($replyTo, $createReply($name)),
            ];
        };

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 10; $i++) {
            $client->processIncoming()->await();
        }

        // The status frame never reached the handler; delivery resumed after the recreate.
        self::assertSame(['msg1', 'msg2'], $received);
        self::assertSame([], $errors);

        $written = implode('', $transport->writes);
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'), 'the 409 must trigger exactly one recreate');
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
        // The recreate resumed from the last in-order point (stream seq 1 -> opt_start_seq 2).
        self::assertStringContainsString('"opt_start_seq":2', $written);
    }

    /**
     * stopOrderedConsumer()'s teardown is BEST-EFFORT end to end: when the live inbox's UNSUB write
     * fails at the transport AND the server rejects the CONSUMER.DELETE, the stop future must still
     * resolve cleanly - watchdog cancelled, local subscription state dropped (which is what stops
     * delivery), no error surfaced.
     */
    public function testStopOrderedConsumerToleratesUnsubscribeAndDeleteFailures(): void
    {
        $createReply = '{"stream_name":"EVENTS","name":"ORD1","config":{"deliver_subject":"deliver.ord","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->orderedConsumerServer(
            $transport,
            onCreate: [static fn (string $rt): array => [self::muxMsg($rt, $createReply)]],
            onDelete: [static fn (string $rt): array => [self::muxMsg($rt, '{"error":{"code":404,"err_code":10014,"description":"consumer not found"}}')]],
        );

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $js = $client->jetStream();
        $sid = $js->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message): void {}, 'events.>')->await();
        $timersBefore = self::countRepeatTimers();
        self::assertGreaterThan(0, $timersBefore, 'precondition: the ordered consumer must have armed its watchdog');

        // The live inbox's UNSUB write fails; the delete is rejected by the server (above).
        $transport->throwOnWriteContaining = "UNSUB $sid\r\n";

        $js->stopOrderedConsumer($sid)->await();

        self::assertSame($timersBefore - 1, self::countRepeatTimers(), 'the watchdog must be cancelled despite the teardown failures');
        self::assertFalse($client->isSubscriptionActive($sid), 'local subscription state must be dropped despite the failed UNSUB write');
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.EVENTS.ORD1', implode('', $transport->writes), 'the best-effort delete must still be attempted after the failed UNSUB');
        self::assertSame([], $errors, 'best-effort stop failures must stay silent');
    }

    /**
     * stopOrderedConsumer() drop-in compatibility: a sid with NO registered ordered consumer (a
     * plain subscription) falls back to a plain unsubscribe rather than failing or silently
     * no-opping.
     */
    public function testStopOrderedConsumerFallsBackToPlainUnsubscribeForUnregisteredSid(): void
    {
        [$client, $transport] = $this->connectedForNameGuard();

        $sid = $client->subscribe('plain.subject', static function (NatsMessage $message): void {})->await();

        $client->jetStream()->stopOrderedConsumer($sid)->await();

        self::assertStringContainsString("UNSUB $sid\r\n", implode('', $transport->writes), 'an unregistered sid must fall back to a plain UNSUB');
        self::assertFalse($client->isSubscriptionActive($sid));
    }

    /**
     * fetchBatch() heartbeat-miss with a PARTIAL batch (#153): after >=1 delivery, two silent
     * idle_heartbeat intervals return the messages already received - they are real - instead of
     * throwing (which would discard them) or sitting out the full expires+slack deadline. Only an
     * EMPTY fetch turns the miss into the heartbeat-miss exception.
     */
    public function testFetchBatchReturnsPartialBatchOnMissedIdleHeartbeats(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
                // One delivery on the fetch inbox (sid 1), then total silence.
                "MSG _INBOX.JS.FETCH.a 1 2\r\nm1\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $startNs = hrtime(true);
        // batch 2, 50ms heartbeat, 2000ms expiry: one message arrives, then the server dies.
        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 2, 2000, ['idle_heartbeat' => 50_000_000])->await();
        $elapsedNs = hrtime(true) - $startNs;

        self::assertCount(1, $messages, 'the partial batch must be returned, not discarded by a throw');
        self::assertSame('m1', $messages[0]->payload);
        // The return happened at the miss deadline: after 2 silent intervals (>=100ms) but far
        // below the expires+slack deadline (3000ms).
        self::assertGreaterThanOrEqual(100_000_000, $elapsedNs, 'the partial return happens only after 2 silent heartbeat intervals');
        self::assertLessThan(1_500_000_000, $elapsedNs, 'a partial batch must fail fast on heartbeat miss, not wait out expires');
    }

    /**
     * surfaceCallerOwnedPushStatus() guard: a non-100, sub-400 status control frame (an unexpected
     * informational status leaking onto a push inbox) is withheld from the user handler like every
     * control frame, but it is NOT terminal - no error may be surfaced for it. Only 4xx/5xx report.
     */
    public function testCallerOwnedPushConsumerDoesNotSurfaceSub400StatusFrames(): void
    {
        $createReply = '{"stream_name":"ORDERS","name":"E1","config":{"deliver_subject":"deliver.eph","ack_policy":"none"}}';
        $status204 = NatsHeaders::toWireBlock(['Status' => '204', 'Description' => 'Not A Failure']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.eph '] = [
            sprintf("HMSG deliver.eph 2 %d %d\r\n%s\r\n", strlen($status204), strlen($status204), $status204),
            "MSG deliver.eph 2 5\r\nhello\r\n",
        ];

        $errors = [];
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        }), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            'deliver.eph',
        )->await();

        $client->processIncoming()->await(); // the 204 control frame
        $client->processIncoming()->await(); // the real message

        self::assertSame(['hello'], $received, 'a sub-400 status control frame must be withheld from the handler');
        self::assertSame([], $errors, 'a sub-400 status is not terminal and must not be surfaced as an error');
    }

    /**
     * emitClientError() containment (#150 twin): NatsConnection::emitError() logs BEFORE its
     * listener guard, so a THROWING user logger escapes it - JetStreamContext's wrapper must
     * swallow that throw so a logger blowing up on a surfaced push status cannot break the shared
     * dispatch loop: the next delivery still reaches the handler.
     */
    public function testThrowingLoggerOnSurfacedPushStatusDoesNotBreakDispatch(): void
    {
        $createReply = '{"stream_name":"ORDERS","name":"E1","config":{"deliver_subject":"deliver.eph","ack_policy":"none"}}';
        $status404 = NatsHeaders::toWireBlock(['Status' => '404', 'Description' => 'No Messages']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);
        $transport->enqueueOnWriteContaining['SUB deliver.eph '] = [
            // Terminal status -> surfaced through emitClientError -> the logger throws.
            sprintf("HMSG deliver.eph 2 %d %d\r\n%s\r\n", strlen($status404), strlen($status404), $status404),
            "MSG deliver.eph 2 5\r\nhello\r\n",
        ];

        // Throws on 'error'-level logs only (lifecycle logs at info level must pass so connect works).
        $logger = new class extends \Psr\Log\AbstractLogger {
            public int $errorLogAttempts = 0;

            public function log($level, \Stringable|string $message, array $context = []): void
            {
                if ((string) $level === 'error') {
                    $this->errorLogAttempts++;

                    throw new \RuntimeException('logger boom');
                }
            }
        };

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, logger: $logger), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            'deliver.eph',
        )->await();

        $client->processIncoming()->await(); // the surfaced 404 - the logger throws, contained
        $client->processIncoming()->await(); // dispatch must survive: the real message arrives

        self::assertGreaterThanOrEqual(1, $logger->errorLogAttempts, 'precondition: the throwing error-log path must actually have fired');
        self::assertSame(['hello'], $received, 'a throwing logger must never break the dispatch loop');
    }

    /**
     * NatsClient::options() hands back the exact NatsOptions instance the client was constructed
     * with, so components wired off the client (services, contexts) observe the same runtime
     * configuration object rather than a copy.
     */
    public function testClientOptionsAccessorReturnsConstructedOptionsInstance(): void
    {
        $options = new NatsOptions(requestTimeoutMs: 1234);
        $client = new NatsClient($options, new FakeTransport());

        self::assertSame($options, $client->options());
    }
}
