<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\JetStream\Configuration\ConsumerConfiguration;
use IDCT\NATS\JetStream\Configuration\StreamConfiguration;
use IDCT\NATS\JetStream\JetStreamApi;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\PrefixedJetStreamServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A JetStream served under a domain or behind an API prefix (#273). Every JetStream, KV and Object Store API request
 * went to the fixed `$JS.API.` subjects, so a client on a leaf node could not reach the hub's JetStream (it answers on
 * `$JS.<domain>.API.>`), and an account importing another account's JetStream API could not use it at all.
 * `NatsClient::jetStream(domain:)` and `jetStream(apiPrefix:)` now give a context whose requests use that prefix.
 *
 * The scripted server ({@see PrefixedJetStreamServer}) answers the JetStream API under one prefix only and gives the
 * no-responders status to every other `$JS.` subject, as a leaf node of another domain does, so a request sent to the
 * wrong prefix fails the call.
 */
final class JetStreamDomainTest extends TestCase
{
    use OwnsTestResources;

    protected function setUp(): void
    {
        $this->resources();
    }

    protected function tearDown(): void
    {
        $this->releaseOwnedResourcesAndTheirWatchdogs();
    }

    /**
     * @return iterable<string, array{?string, ?string, string}>
     */
    public static function prefixes(): iterable
    {
        yield 'default' => [null, null, '$JS.API.'];
        yield 'domain hub' => ['hub', null, '$JS.hub.API.'];
        yield 'api prefix $JS.from-a.API' => [null, '$JS.from-a.API', '$JS.from-a.API.'];
    }

    /**
     * Each JetStream API call, KV and Object Store calls included, with the API subject it must send (after the prefix)
     * and, for a call a server version check gates, the version the scripted server must advertise for the call to be
     * sent at all ({@see PrefixedJetStreamServer::VERSION} otherwise).
     *
     * @return iterable<string, array{0: \Closure(JetStreamContext): Future<mixed>, 1: string, 2?: string}>
     */
    public static function apiCalls(): iterable
    {
        yield 'accountInfo' => [static fn(JetStreamContext $js): Future => $js->accountInfo(), 'INFO'];
        yield 'createStream' => [static fn(JetStreamContext $js): Future => $js->createStream('ORDERS', ['orders.>']), 'STREAM.CREATE.ORDERS'];
        yield 'addStream' => [static fn(JetStreamContext $js): Future => $js->addStream(StreamConfiguration::create('ORDERS')->subjects('orders.>')), 'STREAM.CREATE.ORDERS'];
        yield 'updateStream' => [static fn(JetStreamContext $js): Future => $js->updateStream('ORDERS', ['subjects' => ['orders.>']]), 'STREAM.UPDATE.ORDERS'];
        yield 'createOrUpdateStream' => [static fn(JetStreamContext $js): Future => $js->createOrUpdateStream('ORDERS', ['orders.>']), 'STREAM.CREATE.ORDERS'];
        yield 'getStream' => [static fn(JetStreamContext $js): Future => $js->getStream('ORDERS'), 'STREAM.INFO.ORDERS'];
        yield 'deleteStream' => [static fn(JetStreamContext $js): Future => $js->deleteStream('ORDERS'), 'STREAM.DELETE.ORDERS'];
        yield 'purgeStream' => [static fn(JetStreamContext $js): Future => $js->purgeStream('ORDERS'), 'STREAM.PURGE.ORDERS'];
        yield 'streamNames' => [static fn(JetStreamContext $js): Future => $js->streamNames(), 'STREAM.NAMES'];
        yield 'listStreams' => [static fn(JetStreamContext $js): Future => $js->listStreams(), 'STREAM.LIST'];
        yield 'listConsumers' => [static fn(JetStreamContext $js): Future => $js->listConsumers('ORDERS'), 'CONSUMER.LIST.ORDERS'];
        yield 'consumerNames' => [static fn(JetStreamContext $js): Future => $js->consumerNames('ORDERS'), 'CONSUMER.NAMES.ORDERS'];
        yield 'getStreamMessage' => [static fn(JetStreamContext $js): Future => $js->getStreamMessage('ORDERS', 1), 'STREAM.MSG.GET.ORDERS'];
        yield 'getLastMessageForSubject' => [static fn(JetStreamContext $js): Future => $js->getLastMessageForSubject('ORDERS', 'orders.created'), 'STREAM.MSG.GET.ORDERS'];
        yield 'getNextMessageForSubject' => [static fn(JetStreamContext $js): Future => $js->getNextMessageForSubject('ORDERS', 'orders.created', 1), 'STREAM.MSG.GET.ORDERS'];
        yield 'deleteMessage' => [static fn(JetStreamContext $js): Future => $js->deleteMessage('ORDERS', 1), 'STREAM.MSG.DELETE.ORDERS'];
        yield 'directGetStreamMessage' => [static fn(JetStreamContext $js): Future => $js->directGetStreamMessage('ORDERS', 1), 'DIRECT.GET.ORDERS'];
        yield 'directGetLastMessageForSubject' => [static fn(JetStreamContext $js): Future => $js->directGetLastMessageForSubject('ORDERS', 'orders.created'), 'DIRECT.GET.ORDERS'];
        yield 'directGetLastForSubjects' => [static fn(JetStreamContext $js): Future => $js->directGetLastForSubjects('ORDERS', ['orders.created'], 500), 'DIRECT.GET.ORDERS'];
        yield 'directGetBatch' => [static fn(JetStreamContext $js): Future => $js->directGetBatch('ORDERS', ['seq' => 1, 'batch' => 2], 500), 'DIRECT.GET.ORDERS'];
        yield 'createConsumer' => [static fn(JetStreamContext $js): Future => $js->createConsumer('ORDERS', 'worker'), 'CONSUMER.CREATE.ORDERS.worker'];
        yield 'addConsumer' => [static fn(JetStreamContext $js): Future => $js->addConsumer('ORDERS', ConsumerConfiguration::create()->durable('worker')), 'CONSUMER.CREATE.ORDERS.worker'];
        yield 'addOrUpdateConsumer' => [static fn(JetStreamContext $js): Future => $js->addOrUpdateConsumer('ORDERS', 'worker'), 'CONSUMER.CREATE.ORDERS.worker'];
        yield 'createEphemeralConsumer' => [static fn(JetStreamContext $js): Future => $js->createEphemeralConsumer('ORDERS'), 'CONSUMER.CREATE.ORDERS'];
        yield 'createPushConsumer' => [static fn(JetStreamContext $js): Future => $js->createPushConsumer('ORDERS', 'pusher', 'deliver.orders'), 'CONSUMER.CREATE.ORDERS.pusher'];
        yield 'createEphemeralPushConsumer' => [static fn(JetStreamContext $js): Future => $js->createEphemeralPushConsumer('ORDERS', 'deliver.orders'), 'CONSUMER.CREATE.ORDERS'];
        yield 'getConsumer' => [static fn(JetStreamContext $js): Future => $js->getConsumer('ORDERS', 'worker'), 'CONSUMER.INFO.ORDERS.worker'];
        yield 'deleteConsumer' => [static fn(JetStreamContext $js): Future => $js->deleteConsumer('ORDERS', 'worker'), 'CONSUMER.DELETE.ORDERS.worker'];
        yield 'pauseConsumer' => [static fn(JetStreamContext $js): Future => $js->pauseConsumer('ORDERS', 'worker', '2030-01-01T00:00:00Z'), 'CONSUMER.PAUSE.ORDERS.worker'];
        yield 'resumeConsumer' => [static fn(JetStreamContext $js): Future => $js->resumeConsumer('ORDERS', 'worker'), 'CONSUMER.PAUSE.ORDERS.worker'];
        yield 'unpinConsumer' => [static fn(JetStreamContext $js): Future => $js->unpinConsumer('ORDERS', 'worker', 'jobs'), 'CONSUMER.UNPIN.ORDERS.worker'];
        yield 'resetConsumer' => [static fn(JetStreamContext $js): Future => $js->resetConsumer('ORDERS', 'worker'), 'CONSUMER.RESET.ORDERS.worker', '2.14.0'];
        yield 'fetchBatch' => [static fn(JetStreamContext $js): Future => $js->fetchBatch('ORDERS', 'worker', 1, 300), 'CONSUMER.MSG.NEXT.ORDERS.worker'];
        yield 'pullConsumer handle' => [
            static fn(JetStreamContext $js): Future => $js->pullConsumer('ORDERS', 'worker')
                ->setBatching(1)->setDepth(1)->setExpiresMs(300)->setIterations(1)
                ->handle(static function (): void {}),
            'CONSUMER.MSG.NEXT.ORDERS.worker',
        ];
        yield 'diagnoseNoResponders' => [static fn(JetStreamContext $js): Future => $js->diagnoseNoResponders('ORDERS', 'worker', 500), 'CONSUMER.INFO.ORDERS.worker'];
        yield 'keyValueBucketNames' => [static fn(JetStreamContext $js): Future => $js->keyValueBucketNames(), 'STREAM.NAMES'];
        yield 'objectStoreBucketNames' => [static fn(JetStreamContext $js): Future => $js->objectStoreBucketNames(), 'STREAM.NAMES'];
        yield 'kv create' => [static fn(JetStreamContext $js): Future => $js->keyValue('cfg')->create(), 'STREAM.CREATE.KV_cfg'];
        yield 'kv bind' => [static fn(JetStreamContext $js): Future => $js->keyValue('cfg')->bind(), 'STREAM.INFO.KV_cfg'];
        yield 'kv get' => [static fn(JetStreamContext $js): Future => $js->keyValue('cfg')->get('color'), 'DIRECT.GET.KV_cfg'];
        yield 'kv getRevision' => [static fn(JetStreamContext $js): Future => $js->keyValue('cfg')->getRevision('color', 1), 'STREAM.MSG.GET.KV_cfg'];
        yield 'kv getAll' => [static fn(JetStreamContext $js): Future => $js->keyValue('cfg')->getAll(), 'STREAM.INFO.KV_cfg'];
        yield 'kv getStatus' => [static fn(JetStreamContext $js): Future => $js->keyValue('cfg')->getStatus(), 'STREAM.INFO.KV_cfg'];
        yield 'kv deleteBucket' => [static fn(JetStreamContext $js): Future => $js->keyValue('cfg')->deleteBucket(), 'STREAM.DELETE.KV_cfg'];
        yield 'object store create' => [static fn(JetStreamContext $js): Future => $js->objectStore('files')->create(), 'STREAM.CREATE.OBJ_files'];
        yield 'object store list' => [static fn(JetStreamContext $js): Future => $js->objectStore('files')->list(), 'STREAM.INFO.OBJ_files'];
        yield 'object store getStatus' => [static fn(JetStreamContext $js): Future => $js->objectStore('files')->getStatus(), 'STREAM.INFO.OBJ_files'];
        yield 'object store deleteBucket' => [static fn(JetStreamContext $js): Future => $js->objectStore('files')->deleteBucket(), 'STREAM.DELETE.OBJ_files'];
    }

    /**
     * @return iterable<string, array{?string, ?string, string, \Closure(JetStreamContext): Future<mixed>, string, string}>
     */
    public static function routedApiCalls(): iterable
    {
        foreach (self::prefixes() as $prefixLabel => [$domain, $apiPrefix, $expectedPrefix]) {
            foreach (self::apiCalls() as $callLabel => $apiCall) {
                yield $prefixLabel . ' / ' . $callLabel => [
                    $domain,
                    $apiPrefix,
                    $expectedPrefix,
                    $apiCall[0],
                    $apiCall[1],
                    $apiCall[2] ?? PrefixedJetStreamServer::VERSION,
                ];
            }
        }
    }

    /**
     * Every API call sends its request under the context's prefix, and no request goes to another JetStream API (the
     * default `$JS.API.` included, for a domain or prefix context). The scripted server answers some of these calls
     * with an error (a stream it does not hold, a request it does not model); only the subjects matter here.
     *
     * @param \Closure(JetStreamContext): Future<mixed> $call
     */
    #[DataProvider('routedApiCalls')]
    public function testEveryJetStreamApiRequestGoesToTheContextsPrefix(
        ?string $domain,
        ?string $apiPrefix,
        string $expectedPrefix,
        \Closure $call,
        string $suffix,
        string $serverVersion,
    ): void {
        [$client, $server] = $this->connect($expectedPrefix, serverVersion: $serverVersion);

        try {
            $call($client->jetStream($domain, $apiPrefix))->await();
        } catch (NatsException) {
        }

        self::assertContains($expectedPrefix . $suffix, $server->publishedSubjects());
        self::assertSame([], self::apiRequestsOutside($expectedPrefix, $server->publishedSubjects()));
    }

    /**
     * The flow of the issue's "done when", over the scripted server: a stream and a consumer are created, a message
     * is published, pulled and acknowledged, a second one is consumed by a pull consume (`pullConsumer()->handle()`),
     * a KV bucket is created, written and read, and an Object Store bucket is created and written. The API requests use
     * the context's prefix; the JetStream publish and the Object Store's chunk and meta publishes go to their plain
     * subjects; a KV write goes through the API prefix when there is one (`$JS.hub.API.$KV.cfg.color`, nats.go useJSPfx
     * parity: a plain `$KV.` publish from a leaf node gets no responders); the ack goes to the delivered `$JS.ACK`
     * reply as it is.
     */
    #[DataProvider('prefixes')]
    public function testStreamsConsumersPullsKvAndObjectStoreWorkThroughTheContextsPrefix(
        ?string $domain,
        ?string $apiPrefix,
        string $expectedPrefix,
    ): void {
        [$client, $server] = $this->connect($expectedPrefix);
        $js = $client->jetStream($domain, $apiPrefix);
        self::assertSame($expectedPrefix, $js->apiPrefix());

        $js->createStream('ORDERS', ['orders.>'])->await();
        $js->createConsumer('ORDERS', 'worker')->await();
        $ack = $js->publish('orders.created', '{"id":1}')->await();
        self::assertSame('ORDERS', $ack->stream);

        $messages = $js->fetchBatch('ORDERS', 'worker', 1, 1000)->await();
        self::assertCount(1, $messages);
        self::assertSame('{"id":1}', $messages[0]->payload);
        $js->ack($messages[0])->await();

        $js->publish('orders.shipped', '{"id":2}')->await();
        $handled = [];
        $consumed = $js->pullConsumer('ORDERS', 'worker')
            ->setBatching(1)->setDepth(1)->setExpiresMs(1000)->setIterations(1)
            ->handle(static function (NatsMessage $message) use (&$handled): void {
                $handled[] = $message->payload;
            })->await();
        self::assertSame(1, $consumed);
        self::assertSame(['{"id":2}'], $handled);

        $kv = $js->keyValue('cfg');
        $kv->create()->await();
        $kv->put('color', 'blue')->await();
        self::assertSame('blue', $kv->get('color')->await()?->value);

        $files = $js->objectStore('files');
        $files->create()->await();
        self::assertSame(5, $files->put('report.txt', 'hello')->await()->size);

        $subjects = $server->publishedSubjects();
        foreach (['STREAM.CREATE.ORDERS', 'CONSUMER.CREATE.ORDERS.worker', 'CONSUMER.MSG.NEXT.ORDERS.worker', 'STREAM.CREATE.KV_cfg', 'DIRECT.GET.KV_cfg', 'STREAM.CREATE.OBJ_files'] as $suffix) {
            self::assertContains($expectedPrefix . $suffix, $subjects);
        }
        self::assertSame([], self::apiRequestsOutside($expectedPrefix, $subjects));

        self::assertContains('orders.created', $subjects);
        $kvWrite = $expectedPrefix === JetStreamApi::DEFAULT_PREFIX ? '$KV.cfg.color' : $expectedPrefix . '$KV.cfg.color';
        self::assertContains($kvWrite, $subjects);
        self::assertNotEmpty(array_filter($subjects, static fn(string $subject): bool => str_starts_with($subject, '$O.files.C.')));
        self::assertNotEmpty(array_filter($subjects, static fn(string $subject): bool => str_starts_with($subject, '$O.files.M.')));
        self::assertSame([], array_values(array_filter(
            $subjects,
            static fn(string $subject): bool => str_contains($subject, '$O.') && !str_starts_with($subject, '$O.'),
        )), 'Object Store publishes go to their plain subjects');
        self::assertContains($messages[0]->replyTo, $subjects);
        self::assertStringStartsWith('$JS.ACK.ORDERS.worker.', (string) $messages[0]->replyTo);
    }

    /**
     * A KV get on a bucket whose Direct Get nothing answers falls back to STREAM.MSG.GET under the context's prefix
     * too, and finds the value the put wrote.
     */
    #[DataProvider('prefixes')]
    public function testKvGetWithoutDirectGetFallsBackToStreamMsgGetUnderTheContextsPrefix(
        ?string $domain,
        ?string $apiPrefix,
        string $expectedPrefix,
    ): void {
        [$client, $server] = $this->connect($expectedPrefix, directGet: false);
        $kv = $client->jetStream($domain, $apiPrefix)->keyValue('cfg');
        $kv->create()->await();
        $kv->put('color', 'blue')->await();

        self::assertSame('blue', $kv->get('color')->await()?->value);
        $subjects = $server->publishedSubjects();
        self::assertContains($expectedPrefix . 'DIRECT.GET.KV_cfg', $subjects);
        self::assertContains($expectedPrefix . 'STREAM.MSG.GET.KV_cfg', $subjects);
        self::assertSame([], self::apiRequestsOutside($expectedPrefix, $subjects));
    }

    /**
     * A KV handle bound to an existing bucket (`bind()`, not `create()`) writes through the context's prefix too:
     * `bind()` clears the handle's mirror state, and a bucket that is no mirror keeps the context's prefix in front of
     * its `$KV.` write (`$JS.hub.API.$KV.cfg.color`), which a leaf node would otherwise answer with no responders.
     */
    #[DataProvider('prefixes')]
    public function testKvBucketBoundToAnExistingBucketWritesThroughTheContextsPrefix(
        ?string $domain,
        ?string $apiPrefix,
        string $expectedPrefix,
    ): void {
        [$client, $server] = $this->connect($expectedPrefix);
        $js = $client->jetStream($domain, $apiPrefix);
        $js->keyValue('cfg')->create()->await();

        $bound = $js->keyValue('cfg')->bind()->await();
        $bound->put('color', 'blue')->await();

        self::assertSame('blue', $bound->get('color')->await()?->value);
        $subjects = $server->publishedSubjects();
        self::assertContains($expectedPrefix . 'STREAM.INFO.KV_cfg', $subjects);
        $kvWrite = $expectedPrefix === JetStreamApi::DEFAULT_PREFIX ? '$KV.cfg.color' : $expectedPrefix . '$KV.cfg.color';
        self::assertContains($kvWrite, $subjects);
        self::assertSame([], self::apiRequestsOutside($expectedPrefix, $subjects));
    }

    /**
     * A KV mirror of a bucket in another domain writes through the mirror's own external API
     * (`$JS.hub.API.$KV.cfg.<key>`), never through the context's prefix in front of it; a mirror in the same domain
     * writes to the origin through the context's prefix (nats.go mapStreamToKVS: useJSPfx off for an external mirror).
     */
    public function testKvMirrorWritesUseTheExternalApiAloneAndASameDomainMirrorTheContextsPrefix(): void
    {
        [$client, $server] = $this->connect('$JS.leaf.API.');
        $js = $client->jetStream(domain: 'leaf');

        $external = $js->keyValue('cfg-copy');
        $external->create(['mirror' => ['bucket' => 'cfg', 'domain' => 'hub']])->await();
        $local = $js->keyValue('cfg-local');
        $local->create(['mirror' => 'cfg'])->await();

        foreach ([$external, $local] as $bucket) {
            try {
                $bucket->put('color', 'blue')->await();
            } catch (JetStreamException) {
                // The scripted leaf holds no origin bucket KV_cfg: only the subject matters here.
            }
        }

        $subjects = $server->publishedSubjects();
        self::assertContains('$JS.hub.API.$KV.cfg.color', $subjects);
        self::assertContains('$JS.leaf.API.$KV.cfg.color', $subjects);
        self::assertNotContains('$JS.leaf.API.$JS.hub.API.$KV.cfg.color', $subjects);
    }

    /**
     * @return iterable<string, array{?string, ?string, string}>
     */
    public static function resolvedPrefixes(): iterable
    {
        yield 'neither' => [null, null, '$JS.API.'];
        yield 'domain' => ['hub', null, '$JS.hub.API.'];
        yield 'domain with dashes' => ['edge-01_a', null, '$JS.edge-01_a.API.'];
        yield 'prefix without the trailing dot' => [null, '$JS.from-a.API', '$JS.from-a.API.'];
        yield 'prefix with the trailing dot' => [null, 'JS.from-a.API.', 'JS.from-a.API.'];
        yield 'one-token prefix' => [null, 'fromA', 'fromA.'];
        yield 'the default prefix given directly' => [null, '$JS.API', '$JS.API.'];
    }

    #[DataProvider('resolvedPrefixes')]
    public function testDomainAndApiPrefixResolveToTheirApiPrefix(?string $domain, ?string $apiPrefix, string $expected): void
    {
        self::assertSame($expected, JetStreamApi::resolvePrefix($domain, $apiPrefix));

        [$client] = $this->connect($expected);
        self::assertSame($expected, $client->jetStream($domain, $apiPrefix)->apiPrefix());
        self::assertSame($expected, (new JetStreamContext($client, domain: $domain, apiPrefix: $apiPrefix))->apiPrefix());
    }

    /**
     * @return iterable<string, array{?string, ?string, non-empty-string}>
     */
    public static function invalidPrefixes(): iterable
    {
        yield 'empty domain' => ['', null, 'JetStream domain must not be empty'];
        yield 'domain with a dot' => ['hub.eu', null, 'Invalid JetStream domain "hub.eu"'];
        yield 'domain with *' => ['h*', null, 'Invalid JetStream domain "h*"'];
        yield 'domain with >' => ['h>', null, 'Invalid JetStream domain "h>"'];
        yield 'domain with a space' => ['h ub', null, 'Invalid JetStream domain "h ub"'];
        yield 'domain with a tab' => ["h\tub", null, 'Invalid JetStream domain'];
        yield 'domain with CRLF' => ["hub\r\nPUB x 0", null, 'Invalid JetStream domain'];
        yield 'domain with a NUL' => ["h\0ub", null, 'Invalid JetStream domain'];
        yield 'empty prefix' => [null, '', 'JetStream API prefix must not be empty'];
        yield 'prefix of a dot' => [null, '.', 'JetStream API prefix must not be empty'];
        yield 'prefix with an empty token' => [null, 'JS..API', 'Invalid JetStream API prefix "JS..API"'];
        yield 'prefix with a leading dot' => [null, '.JS.API', 'Invalid JetStream API prefix ".JS.API"'];
        yield 'prefix with two trailing dots' => [null, 'JS.API..', 'Invalid JetStream API prefix "JS.API.."'];
        yield 'prefix with *' => [null, 'JS.*.API', 'Invalid JetStream API prefix "JS.*.API"'];
        yield 'prefix with >' => [null, 'JS.>', 'Invalid JetStream API prefix "JS.>"'];
        yield 'prefix with a space' => [null, 'JS.from a.API', 'Invalid JetStream API prefix "JS.from a.API"'];
        yield 'prefix with CRLF' => [null, "JS.API\r\nPUB x 0", 'Invalid JetStream API prefix'];
        yield 'domain and prefix together' => ['hub', '$JS.hub.API', 'Set either a JetStream domain or a JetStream API prefix, not both'];
    }

    /**
     * An invalid domain or prefix, and a domain together with a prefix, are refused by `jetStream()` and by the
     * context's constructor, before anything is written for them.
     *
     * @param non-empty-string $message
     */
    #[DataProvider('invalidPrefixes')]
    public function testInvalidDomainOrPrefixIsRejectedBeforeAnyRequest(?string $domain, ?string $apiPrefix, string $message): void
    {
        [$client, $server] = $this->connect('$JS.API.');

        foreach ([
            static fn(): JetStreamContext => $client->jetStream($domain, $apiPrefix),
            static fn(): JetStreamContext => new JetStreamContext($client, domain: $domain, apiPrefix: $apiPrefix),
        ] as $make) {
            try {
                $make();
                self::fail('expected the domain or prefix to be refused');
            } catch (JetStreamException $refused) {
                self::assertStringStartsWith($message, $refused->getMessage());
            }
        }

        self::assertSame([], $server->published);
    }

    /**
     * `jetStream()` keeps one context per resulting prefix: the no-argument call returns the same context as before,
     * a domain and the equivalent prefix share one, and different prefixes get different contexts.
     */
    public function testJetStreamKeepsOneContextPerApiPrefix(): void
    {
        [$client] = $this->connect('$JS.API.');

        $default = $client->jetStream();
        self::assertSame($default, $client->jetStream());
        self::assertSame($default, $client->jetStream(apiPrefix: '$JS.API'));
        self::assertSame('$JS.API.', $default->apiPrefix());

        $hub = $client->jetStream(domain: 'hub');
        self::assertNotSame($default, $hub);
        self::assertSame($hub, $client->jetStream(domain: 'hub'));
        self::assertSame($hub, $client->jetStream(apiPrefix: '$JS.hub.API.'));
        self::assertNotSame($hub, $client->jetStream(domain: 'edge'));
    }

    public function testApiSubjectRebasesOnlyTheDefaultApiPrefix(): void
    {
        self::assertSame('$JS.hub.API.STREAM.INFO.ORDERS', JetStreamApi::withPrefix(JetStreamApi::STREAM_INFO_PREFIX . 'ORDERS', '$JS.hub.API.'));
        self::assertSame('JS.from-a.API.INFO', JetStreamApi::withPrefix(JetStreamApi::ACCOUNT_INFO, 'JS.from-a.API.'));
        self::assertSame('$JS.API.INFO', JetStreamApi::withPrefix(JetStreamApi::ACCOUNT_INFO, JetStreamApi::DEFAULT_PREFIX));
        self::assertSame('orders.created', JetStreamApi::withPrefix('orders.created', '$JS.hub.API.'));
        self::assertSame('$JS.ACK.ORDERS.worker.1.1.1.0.0', JetStreamApi::withPrefix('$JS.ACK.ORDERS.worker.1.1.1.0.0', '$JS.hub.API.'));

        [$client] = $this->connect('$JS.hub.API.');
        self::assertSame('$JS.hub.API.CONSUMER.MSG.NEXT.ORDERS.worker', $client->jetStream(domain: 'hub')->apiSubject(JetStreamApi::CONSUMER_MSG_NEXT_PREFIX . 'ORDERS.worker'));
    }

    /**
     * The JetStream API requests among $subjects that do not use $prefix: a `$JS.` subject other than an ack or a
     * flow-control reply that does not start with it.
     *
     * @param list<string> $subjects
     * @return list<string>
     */
    private static function apiRequestsOutside(string $prefix, array $subjects): array
    {
        return array_values(array_filter(
            $subjects,
            static fn(string $subject): bool => str_starts_with($subject, '$JS.')
                && !str_starts_with($subject, '$JS.ACK.')
                && !str_starts_with($subject, '$JS.FC.')
                && !str_starts_with($subject, $prefix),
        ));
    }

    /**
     * A client connected over a {@see FakeTransport} to a {@see PrefixedJetStreamServer} answering under $apiPrefix
     * and advertising $serverVersion.
     *
     * @return array{NatsClient, PrefixedJetStreamServer}
     */
    private function connect(string $apiPrefix, bool $directGet = true, string $serverVersion = PrefixedJetStreamServer::VERSION): array
    {
        $server = new PrefixedJetStreamServer($apiPrefix, $directGet);
        $transport = new FakeTransport([PrefixedJetStreamServer::info($serverVersion)]);
        $server->install($transport);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport));
        $client->connect()->await();

        return [$client, $server];
    }
}
