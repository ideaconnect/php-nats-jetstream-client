<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Behat\Context;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Behat\Behat\Context\Context;
use DateTimeImmutable;
use IDCT\NATS\Auth\CredentialsParser;
use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Configuration\Republish;
use IDCT\NATS\JetStream\Configuration\StreamSource;
use IDCT\NATS\JetStream\Configuration\SubjectTransform;
use IDCT\NATS\JetStream\Enum\AckPolicy;
use IDCT\NATS\JetStream\Enum\DeliverPolicy;
use IDCT\NATS\JetStream\Enum\DiscardPolicy;
use IDCT\NATS\JetStream\Enum\ReplayPolicy;
use IDCT\NATS\JetStream\Enum\RetentionPolicy;
use IDCT\NATS\JetStream\Schedule;
use IDCT\NATS\JetStream\Enum\StorageBackend;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
use IDCT\NATS\Services\Service;
use IDCT\NATS\Tests\Behat\Support\ScenarioState;
use IDCT\NATS\Tests\Integration\IntegrationTestBootstrap;
use RuntimeException;
use Throwable;
use function Amp\async;
use function Amp\delay;

final class FeatureContext implements Context
{
    use IntegrationTestBootstrap;

    /** @var array<string,NatsClient> */
    private array $clients = [];

    /** @var array<string,list<int>> */
    private array $subscriptions = [];

    /** @var list<string> */
    private array $createdStreams = [];

    /** @var array<string,Service> */
    private array $services = [];

    /** @var array<string,DeferredCancellation> */
    private array $servicePumpCancellations = [];

    /** @var array<string,Future<void>> */
    private array $servicePumps = [];

    /** @var array<string,SubscriptionQueue> */
    private array $subscriptionQueues = [];

    /** @var array<string,list<string>> */
    private array $queueWorkerPayloads = [];

    public function __construct(
        private readonly ScenarioState $state = new ScenarioState(),
    ) {
    }

    /**
     * @BeforeScenario
     */
    public function beforeScenario(): void
    {
        $this->state->reset();
        $this->clients = [];
        $this->subscriptions = [];
        $this->createdStreams = [];
        $this->services = [];
        $this->servicePumpCancellations = [];
        $this->servicePumps = [];
        $this->subscriptionQueues = [];
        $this->queueWorkerPayloads = [];
    }

    /**
     * @AfterScenario
     */
    public function afterScenario(): void
    {
        foreach ($this->services as $service) {
            try {
                $service->stop()->await();
            } catch (Throwable) {
            }
        }

        foreach ($this->servicePumpCancellations as $cancellation) {
            $cancellation->cancel();
        }

        foreach ($this->servicePumps as $pump) {
            try {
                $pump->await();
            } catch (Throwable) {
            }
        }

        foreach ($this->subscriptions as $alias => $sids) {
            foreach ($sids as $sid) {
                try {
                    $this->client($alias)->unsubscribe($sid)->await();
                } catch (Throwable) {
                }
            }
        }

        $primary = $this->clients['primary'] ?? null;
        if ($primary !== null) {
            $js = $primary->jetStream();
            foreach (array_reverse($this->createdStreams) as $stream) {
                try {
                    $js->deleteStream($stream)->await();
                } catch (Throwable) {
                }
            }
        }

        foreach ($this->clients as $client) {
            try {
                $client->disconnect()->await();
            } catch (Throwable) {
            }
        }

        if ($this->state->tempCredentialsFile !== null && is_file($this->state->tempCredentialsFile)) {
            @unlink($this->state->tempCredentialsFile);
        }
    }

    /**
     * @Given I am connected to NATS
     */
    public function iAmConnectedToNats(): void
    {
        $this->client('primary');
    }

    /**
     * @Given a second client is connected to NATS
     */
    public function aSecondClientIsConnectedToNats(): void
    {
        $this->client('secondary');
    }

    /**
     * @Given I have a random subject
     */
    public function iHaveARandomSubject(): void
    {
        $this->state->subject = 'behat.subject.' . bin2hex(random_bytes(4));
    }

    /**
     * @Given I have a random request subject
     */
    public function iHaveARandomRequestSubject(): void
    {
        $this->state->requestSubject = 'behat.request.' . bin2hex(random_bytes(4));
    }

    /**
     * @Given I have a random JetStream stream and subject
     */
    public function iHaveARandomJetStreamStreamAndSubject(): void
    {
        $suffix = strtoupper(bin2hex(random_bytes(3)));
        $this->state->stream = 'BHAT_' . $suffix;
        $this->state->streamSubject = 'behat.jetstream.' . strtolower($suffix) . '.events';
    }

    /**
     * @Given I have a random scheduled JetStream stream and subjects
     */
    public function iHaveARandomScheduledJetStreamStreamAndSubjects(): void
    {
        $suffix = strtolower(bin2hex(random_bytes(3)));
        $this->state->stream = 'BHAT_' . strtoupper($suffix);
        $this->state->scheduleSubject = 'behat.schedules.' . $suffix . '.one';
        $this->state->streamSubject = 'behat.events.' . $suffix . '.scheduled';
    }

    /**
     * @Given I have a random KeyValue bucket
     */
    public function iHaveARandomKeyValueBucket(): void
    {
        $this->state->kvBucket = 'bhatkv' . strtolower(bin2hex(random_bytes(3)));
    }

    /**
     * @Given I have a random Object Store bucket
     */
    public function iHaveARandomObjectStoreBucket(): void
    {
        $this->state->objectStoreBucket = 'bhatobj' . strtolower(bin2hex(random_bytes(3)));
    }

    /**
     * @Given I have a random JetStream stream with primary and secondary subjects
     */
    public function iHaveARandomJetStreamStreamWithPrimaryAndSecondarySubjects(): void
    {
        $suffix = strtolower(bin2hex(random_bytes(3)));
        $this->state->stream = 'BHAT_' . strtoupper($suffix);
        $this->state->streamSubject = 'behat.jetstream.' . $suffix . '.a';
        $this->state->secondarySubject = 'behat.jetstream.' . $suffix . '.b';
    }

    /**
     * @Given I have a random durable consumer name
     */
    public function iHaveARandomDurableConsumerName(): void
    {
        $this->state->consumerName = 'C_' . strtoupper(bin2hex(random_bytes(2)));
    }

    /**
     * @Given I have a random service name and echo subject
     */
    public function iHaveARandomServiceNameAndEchoSubject(): void
    {
        $suffix = strtolower(bin2hex(random_bytes(3)));
        $this->state->serviceName = 'svc-' . $suffix;
        $this->state->serviceVersion = '1.0.0';
        $this->state->serviceSubjects = [
            'echo' => 'svc.' . $suffix . '.echo',
        ];
    }

    /**
     * @Given I have a random service with schema and plain subjects
     */
    public function iHaveARandomServiceWithSchemaAndPlainSubjects(): void
    {
        $suffix = strtolower(bin2hex(random_bytes(3)));
        $this->state->serviceName = 'svc-' . $suffix;
        $this->state->serviceVersion = '1.2.3';
        $this->state->serviceSubjects = [
            'schema' => 'svc.' . $suffix . '.schema',
            'plain' => 'svc.' . $suffix . '.plain',
        ];
    }

    /**
     * @Given I have a random grouped service subject hierarchy
     */
    public function iHaveARandomGroupedServiceSubjectHierarchy(): void
    {
        $suffix = strtolower(bin2hex(random_bytes(3)));
        $this->state->serviceName = 'grouped-' . $suffix;
        $this->state->serviceVersion = '1.0.0';
        $this->state->serviceSubjects = [
            'root' => 'svc.' . $suffix,
            'v1' => 'svc.' . $suffix . '.v1.echo',
            'v2' => 'svc.' . $suffix . '.v2.echo',
        ];
    }

    /**
     * @Given I have a random wildcard subscription pattern and subjects
     */
    public function iHaveARandomWildcardSubscriptionPatternAndSubjects(): void
    {
        $suffix = strtolower(bin2hex(random_bytes(4)));
        $this->state->wildcardSubjects = [
            'pattern' => 'behat.wild.' . $suffix . '.*',
            'match_a' => 'behat.wild.' . $suffix . '.a',
            'match_b' => 'behat.wild.' . $suffix . '.b',
            'non_match' => 'behat.wild.' . $suffix . '.a.tail',
        ];
    }

    /**
     * @When I subscribe to my subject
     */
    public function iSubscribeToMySubject(): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $sid = $this->client('primary')->subscribe($subject, function (NatsMessage $message): void {
            $this->state->receivedPayloads[] = $message->payload;
        })->await();

        $this->subscriptions['primary'][] = $sid;
    }

    /**
     * @When I publish :payload to my subject
     */
    public function iPublishToMySubject(string $payload): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $this->client('primary')->publish($subject, $payload)->await();
    }

    /**
     * @When I process incoming messages
     */
    public function iProcessIncomingMessages(): void
    {
        foreach (array_keys($this->clients) as $alias) {
            $this->pumpClient($alias);
        }
    }

    /**
     * @Then I should receive the message :payload
     */
    public function iShouldReceiveTheMessage(string $payload): void
    {
        if (!in_array($payload, $this->state->receivedPayloads, true)) {
            throw new RuntimeException(sprintf('Expected payload "%s" but received [%s].', $payload, implode(', ', $this->state->receivedPayloads)));
        }
    }

    /**
     * @Given the second client replies on my request subject with :payload
     */
    public function theSecondClientRepliesOnMyRequestSubjectWith(string $payload): void
    {
        $subject = $this->requireValue($this->state->requestSubject, 'request subject');
        $sid = $this->client('secondary')->subscribe($subject, function (NatsMessage $message) use ($payload): void {
            if ($message->replyTo === null) {
                return;
            }

            $this->client('secondary')->publish($message->replyTo, $payload)->await();
        })->await();

        $this->subscriptions['secondary'][] = $sid;
    }

    /**
     * @When I request :payload on my request subject
     */
    public function iRequestOnMyRequestSubject(string $payload): void
    {
        $subject = $this->requireValue($this->state->requestSubject, 'request subject');
        $secondary = $this->client('secondary');

        $processor = async(function () use ($secondary): void {
            $deadline = microtime(true) + 2.5;

            while (microtime(true) < $deadline) {
                $frames = $secondary->processIncoming()->await();
                if ($frames > 0) {
                    return;
                }

                delay(0.01);
            }

            throw new RuntimeException('Responder client did not receive the request within the expected time.');
        });

        try {
            $reply = $this->client('primary')->request($subject, $payload, 2000)->await();
            $this->state->lastReplyPayload = $reply->payload;
        } finally {
            $processor->await();
        }
    }

    /**
     * @Then the request reply should be :payload
     */
    public function theRequestReplyShouldBe(string $payload): void
    {
        if ($this->state->lastReplyPayload !== $payload) {
            throw new RuntimeException(sprintf('Expected request reply "%s" but got "%s".', $payload, (string) $this->state->lastReplyPayload));
        }
    }

    /**
     * @When I fetch JetStream account info
     */
    public function iFetchJetStreamAccountInfo(): void
    {
        $this->client('primary')->jetStream()->accountInfo()->await();
        $this->state->jetStreamAccountFetched = true;
    }

    /**
     * @When I create the JetStream stream for my subject
     */
    public function iCreateTheJetStreamStreamForMySubject(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $subject = $this->requireValue($this->state->streamSubject, 'stream subject');
        $this->client('primary')->jetStream()->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
    }

    /**
     * @Then the JetStream account info request should succeed
     */
    public function theJetStreamAccountInfoRequestShouldSucceed(): void
    {
        if (!$this->state->jetStreamAccountFetched) {
            throw new RuntimeException('Expected JetStream account info request to succeed.');
        }
    }

    /**
     * @Then the JetStream stream should be available
     */
    public function theJetStreamStreamShouldBeAvailable(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $streamInfo = $this->client('primary')->jetStream()->getStream($stream)->await();
        if ($streamInfo->name !== $stream) {
            throw new RuntimeException(sprintf('Expected stream "%s" but got "%s".', $stream, $streamInfo->name));
        }
    }

    /**
     * @When I delete the JetStream stream
     */
    public function iDeleteTheJetStreamStream(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $this->client('primary')->jetStream()->deleteStream($stream)->await();
        $this->state->jetStreamStreamDeleted = true;
        $this->createdStreams = array_values(array_filter(
            $this->createdStreams,
            static fn (string $item): bool => $item !== $stream,
        ));
    }

    /**
     * @Then the JetStream stream should be removed
     */
    public function theJetStreamStreamShouldBeRemoved(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        if (!$this->state->jetStreamStreamDeleted) {
            throw new RuntimeException('Expected JetStream stream deletion to be recorded.');
        }

        try {
            $this->client('primary')->jetStream()->getStream($stream)->await();
        } catch (JetStreamException) {
            return;
        }

        throw new RuntimeException(sprintf('Expected stream "%s" to be removed.', $stream));
    }

    /**
     * @When I subscribe to my subject and record headers
     */
    public function iSubscribeToMySubjectAndRecordHeaders(): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $sid = $this->client('primary')->subscribe($subject, function (NatsMessage $message): void {
            $this->state->receivedPayloads[] = $message->payload;
            $this->state->lastHeaders = NatsHeaders::fromWireBlock($message->rawHeaders);
        })->await();

        $this->subscriptions['primary'][] = $sid;
    }

    /**
     * @When I publish :payload with headers to my subject
     */
    public function iPublishWithHeadersToMySubject(string $payload): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $this->client('primary')->publishWithHeaders($subject, $payload, [
            'X-Request-Id' => 'behat-publish-header',
            'Content-Type' => 'text/plain',
        ])->await();
    }

    /**
     * @Then the published message should include the custom headers
     */
    public function thePublishedMessageShouldIncludeTheCustomHeaders(): void
    {
        if (($this->state->lastHeaders['X-Request-Id'] ?? null) !== 'behat-publish-header') {
            throw new RuntimeException('Expected published message to include X-Request-Id header.');
        }

        if (($this->state->lastHeaders['Content-Type'] ?? null) !== 'text/plain') {
            throw new RuntimeException('Expected published message to include Content-Type header.');
        }
    }

    /**
     * @When the second client replies with captured headers on my request subject
     */
    public function theSecondClientRepliesWithCapturedHeadersOnMyRequestSubject(): void
    {
        $subject = $this->requireValue($this->state->requestSubject, 'request subject');
        $sid = $this->client('secondary')->subscribe($subject, function (NatsMessage $message): void {
            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
            $this->state->lastObservedRequestHeader = $headers['X-Request-Id'] ?? null;

            if ($message->replyTo !== null) {
                $this->client('secondary')->publish($message->replyTo, 'ok')->await();
            }
        })->await();

        $this->subscriptions['secondary'][] = $sid;
    }

    /**
     * @When I request :payload with headers on my request subject
     */
    public function iRequestWithHeadersOnMyRequestSubject(string $payload): void
    {
        $subject = $this->requireValue($this->state->requestSubject, 'request subject');
        $secondary = $this->client('secondary');

        $processor = async(function () use ($secondary): void {
            $deadline = microtime(true) + 2.5;

            while (microtime(true) < $deadline) {
                $frames = $secondary->processIncoming()->await();
                if ($frames > 0) {
                    return;
                }

                delay(0.01);
            }

            throw new RuntimeException('Responder client did not receive the header-bearing request within the expected time.');
        });

        try {
            $reply = $this->client('primary')->requestWithHeaders($subject, $payload, [
                'X-Request-Id' => 'behat-request-header',
            ], 2000)->await();
            $this->state->lastReplyPayload = $reply->payload;
        } finally {
            $processor->await();
        }
    }

    /**
     * @Then the request handler should receive the custom request header
     */
    public function theRequestHandlerShouldReceiveTheCustomRequestHeader(): void
    {
        if ($this->state->lastObservedRequestHeader !== 'behat-request-header') {
            throw new RuntimeException('Expected request handler to receive propagated X-Request-Id header.');
        }
    }

    /**
     * @Then server info should be available
     */
    public function serverInfoShouldBeAvailable(): void
    {
        $serverInfo = $this->client('primary')->serverInfo();
        $this->state->lastServerName = $serverInfo?->serverName;

        if ($this->state->lastServerName === null || $this->state->lastServerName === '') {
            throw new RuntimeException('Expected server info to be available for the connected client.');
        }
    }

    /**
     * @When two worker clients subscribe to my subject with a shared queue group
     */
    public function twoWorkerClientsSubscribeToMySubjectWithASharedQueueGroup(): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $this->queueWorkerPayloads = ['worker-a' => [], 'worker-b' => []];

        $sidA = $this->client('primary')->subscribe($subject, function (NatsMessage $message): void {
            $this->queueWorkerPayloads['worker-a'][] = $message->payload;
        }, 'workers')->await();

        $sidB = $this->client('secondary')->subscribe($subject, function (NatsMessage $message): void {
            $this->queueWorkerPayloads['worker-b'][] = $message->payload;
        }, 'workers')->await();

        $this->subscriptions['primary'][] = $sidA;
        $this->subscriptions['secondary'][] = $sidB;
    }

    /**
     * @When I publish :count queue messages to my subject
     */
    public function iPublishQueueMessagesToMySubject(int $count): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $publisher = $this->client('tertiary');

        for ($i = 0; $i < $count; $i++) {
            $publisher->publish($subject, (string) $i)->await();
        }

        $this->waitFor(function () use ($count): bool {
            $this->client('primary')->processIncoming()->await();
            $this->client('secondary')->processIncoming()->await();

            return count($this->queueWorkerPayloads['worker-a'] ?? []) + count($this->queueWorkerPayloads['worker-b'] ?? []) >= $count;
        }, 5.0);
    }

    /**
     * @Then the queue group should distribute :count messages without duplicates
     */
    public function theQueueGroupShouldDistributeMessagesWithoutDuplicates(int $count): void
    {
        $allSeen = array_merge($this->queueWorkerPayloads['worker-a'] ?? [], $this->queueWorkerPayloads['worker-b'] ?? []);
        $unique = array_values(array_unique($allSeen));
        sort($allSeen);
        sort($unique);

        if (count($allSeen) !== $count || count($unique) !== $count) {
            throw new RuntimeException('Expected queue group delivery without duplicates across workers.');
        }

        if (($this->queueWorkerPayloads['worker-a'] ?? []) === [] || ($this->queueWorkerPayloads['worker-b'] ?? []) === []) {
            throw new RuntimeException('Expected both queue workers to receive at least one message.');
        }
    }

    /**
     * @When I create a polling subscription queue for my subject
     */
    public function iCreateAPollingSubscriptionQueueForMySubject(): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $queue = $this->client('primary')->subscribeQueue($subject, 'workers')->await();
        $queue->setTimeout(1.0);
        $this->subscriptionQueues['primary'] = $queue;
        $this->subscriptions['primary'][] = $queue->sid;
    }

    /**
     * @When the second client publishes :first, :second, and :third to my subject
     */
    public function theSecondClientPublishesAndToMySubject(string $first, string $second, string $third): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $publisher = $this->client('secondary');
        $publisher->publish($subject, $first)->await();
        $publisher->publish($subject, $second)->await();
        $publisher->publish($subject, $third)->await();
    }

    /**
     * @When I fetch queued messages using fetch, next, and fetchAll
     */
    public function iFetchQueuedMessagesUsingFetchNextAndFetchAll(): void
    {
        $queue = $this->subscriptionQueues['primary'] ?? null;
        if (!$queue instanceof SubscriptionQueue) {
            throw new RuntimeException('Expected polling subscription queue to be initialized.');
        }

        $this->state->lastQueuePayloads = [];

        $first = null;
        $deadline = microtime(true) + 2.0;
        while ($first === null && microtime(true) < $deadline) {
            $first = $queue->fetch();
            if ($first === null) {
                delay(0.01);
            }
        }

        if ($first !== null) {
            $this->state->lastQueuePayloads[] = $first->payload;
        }

        $next = $queue->next();
        if ($next !== null) {
            $this->state->lastQueuePayloads[] = $next->payload;
        }

        foreach ($queue->fetchAll(limit: 10) as $message) {
            $this->state->lastQueuePayloads[] = $message->payload;
        }
    }

    /**
     * @Then the polling subscription queue should return :first, :second, and :third
     */
    public function thePollingSubscriptionQueueShouldReturnAnd(string $first, string $second, string $third): void
    {
        $payloads = $this->state->lastQueuePayloads;
        sort($payloads);
        $expected = [$first, $second, $third];
        sort($expected);

        if ($payloads !== $expected) {
            throw new RuntimeException(sprintf('Expected polling queue payloads [%s], got [%s].', implode(', ', $expected), implode(', ', $payloads)));
        }
    }

    /**
     * @When I update the JetStream stream to include the secondary subject
     */
    public function iUpdateTheJetStreamStreamToIncludeTheSecondarySubject(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $primarySubject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $secondarySubject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');

        $this->client('primary')->jetStream()->updateStream($stream, [
            'subjects' => [$primarySubject, $secondarySubject],
        ])->await();
    }

    /**
     * @When I create a durable consumer for the primary subject
     */
    public function iCreateADurableConsumerForThePrimarySubject(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');

        $this->client('primary')->jetStream()->createConsumer($stream, $consumer, $subject)->await();
    }

    /**
     * @When I fetch the durable consumer info
     */
    public function iFetchTheDurableConsumerInfo(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $info = $this->client('primary')->jetStream()->getConsumer($stream, $consumer)->await();
        $this->state->lastConsumerRaw = $info->raw;
    }

    /**
     * @When I list consumers for the current stream
     */
    public function iListConsumersForTheCurrentStream(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumers = $this->client('primary')->jetStream()->listConsumers($stream)->await();
        $this->state->lastListedConsumers = array_map(static fn ($consumer): string => $consumer->name, $consumers);
    }

    /**
     * @When I list streams
     */
    public function iListStreams(): void
    {
        $streams = $this->client('primary')->jetStream()->listStreams()->await();
        $this->state->lastListedStreams = array_map(static fn ($stream): string => $stream->name, $streams);
    }

    /**
     * @Then the JetStream stream should include both configured subjects
     */
    public function theJetStreamStreamShouldIncludeBothConfiguredSubjects(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $primarySubject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $secondarySubject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');
        $info = $this->client('primary')->jetStream()->getStream($stream)->await();
        $this->state->lastStreamRaw = $info->raw;

        if (!in_array($primarySubject, $info->subjects, true) || !in_array($secondarySubject, $info->subjects, true)) {
            throw new RuntimeException('Expected updated JetStream stream to include both configured subjects.');
        }
    }

    /**
     * @Then the durable consumer info should match the current stream and consumer
     */
    public function theDurableConsumerInfoShouldMatchTheCurrentStreamAndConsumer(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');

        if (($this->state->lastConsumerRaw['stream_name'] ?? null) !== $stream || ($this->state->lastConsumerRaw['name'] ?? null) !== $consumer) {
            throw new RuntimeException('Expected durable consumer info to match the current stream and consumer.');
        }
    }

    /**
     * @Then the JetStream consumer list should include the current consumer
     */
    public function theJetStreamConsumerListShouldIncludeTheCurrentConsumer(): void
    {
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        if (!in_array($consumer, $this->state->lastListedConsumers, true)) {
            throw new RuntimeException('Expected JetStream consumer list to include the current consumer.');
        }
    }

    /**
     * @Then the JetStream stream list should include the current stream
     */
    public function theJetStreamStreamListShouldIncludeTheCurrentStream(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        if (!in_array($stream, $this->state->lastListedStreams, true)) {
            throw new RuntimeException('Expected JetStream stream list to include the current stream.');
        }
    }

    /**
     * @When I publish :payload to the primary JetStream subject
     */
    public function iPublishToThePrimaryJetStreamSubject(string $payload): void
    {
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $ack = $this->client('primary')->jetStream()->publish($subject, $payload)->await();
        $this->state->lastAckSequence = $ack->seq;
    }

    /**
     * @When I fetch the stream message using the last publish sequence
     */
    public function iFetchTheStreamMessageUsingTheLastPublishSequence(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $message = $this->client('primary')->jetStream()->getStreamMessage($stream, $this->state->lastAckSequence)->await();
        $this->state->lastReplyPayload = $message->payload;
    }

    /**
     * @Then the direct stream get should return :payload
     */
    public function theDirectStreamGetShouldReturn(string $payload): void
    {
        if ($this->state->lastReplyPayload !== $payload) {
            throw new RuntimeException(sprintf('Expected direct stream get payload "%s".', $payload));
        }
    }

    /**
     * @When I purge the current JetStream stream
     */
    public function iPurgeTheCurrentJetStreamStream(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $result = $this->client('primary')->jetStream()->purgeStream($stream)->await();
        $this->state->lastPurgeCount = (int) $result['purged'];
        $state = $this->client('primary')->jetStream()->getStream($stream)->await()->raw['state'] ?? [];
        $this->state->lastObservedStreamMessages = (int) ($state['messages'] ?? -1);
    }

    /**
     * @Then the current JetStream stream should have no stored messages
     */
    public function theCurrentJetStreamStreamShouldHaveNoStoredMessages(): void
    {
        if ($this->state->lastPurgeCount < 1 || $this->state->lastObservedStreamMessages !== 0) {
            throw new RuntimeException('Expected stream purge to remove all stored messages.');
        }
    }

    /**
     * @When I create a stream with republish from the primary to the secondary subject
     */
    public function iCreateAStreamWithRepublishFromThePrimaryToTheSecondarySubject(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $sourceSubject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $destinationSubject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');

        $this->client('primary')->jetStream()->createStream($stream, [$sourceSubject], [
            'republish' => Republish::create($sourceSubject, $destinationSubject)->toArray(),
        ])->await();
        $this->createdStreams[] = $stream;
    }

    /**
     * @When the second client subscribes to the secondary subject for republished messages
     */
    public function theSecondClientSubscribesToTheSecondarySubjectForRepublishedMessages(): void
    {
        $subject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');
        $sid = $this->client('secondary')->subscribe($subject, function (NatsMessage $message): void {
            $this->state->receivedPayloads[] = $message->payload;
            $this->state->receivedSubjects[] = $message->subject;
        })->await();

        $this->subscriptions['secondary'][] = $sid;
    }

    /**
     * @When I publish :payload to the republished primary subject
     */
    public function iPublishToTheRepublishedPrimarySubject(string $payload): void
    {
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $this->client('primary')->jetStream()->publish($subject, $payload)->await();
    }

    /**
     * @Then the republished subscriber should receive :payload on the secondary subject
     */
    public function theRepublishedSubscriberShouldReceiveOnTheSecondarySubject(string $payload): void
    {
        $secondarySubject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');

        $this->waitFor(function () use ($payload, $secondarySubject): bool {
            $this->client('secondary')->processIncoming()->await();

            foreach ($this->state->receivedPayloads as $index => $receivedPayload) {
                $receivedSubject = $this->state->receivedSubjects[$index] ?? null;
                if ($receivedPayload === $payload && $receivedSubject === $secondarySubject) {
                    return true;
                }
            }

            return false;
        }, 4.0);
    }

    /**
     * @When I create a stream with a subject transform from the primary to the secondary subject
     */
    public function iCreateAStreamWithASubjectTransformFromThePrimaryToTheSecondarySubject(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $sourceSubject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $destinationSubject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');

        $this->client('primary')->jetStream()->createStream($stream, [$sourceSubject], [
            'subject_transform' => SubjectTransform::create($sourceSubject, $destinationSubject)->toArray(),
        ])->await();
        $this->createdStreams[] = $stream;
    }

    /**
     * @When I publish :payload to the transformed primary subject
     */
    public function iPublishToTheTransformedPrimarySubject(string $payload): void
    {
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $ack = $this->client('primary')->jetStream()->publish($subject, $payload)->await();
        $this->state->lastAckSequence = $ack->seq;
    }

    /**
     * @When I fetch the transformed stream message by the last publish sequence
     */
    public function iFetchTheTransformedStreamMessageByTheLastPublishSequence(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $message = $this->client('primary')->jetStream()->getStreamMessage($stream, $this->state->lastAckSequence)->await();
        $this->state->lastReplyPayload = $message->payload;
        $this->state->lastDirectSubject = $message->subject;
    }

    /**
     * @Then the transformed stream message should be stored under the secondary subject with payload :payload
     */
    public function theTransformedStreamMessageShouldBeStoredUnderTheSecondarySubjectWithPayload(string $payload): void
    {
        $secondarySubject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');

        if ($this->state->lastDirectSubject !== $secondarySubject || $this->state->lastReplyPayload !== $payload) {
            throw new RuntimeException('Expected transformed stream message to be stored under the secondary subject with the original payload.');
        }
    }

    /**
     * @When I create an origin stream and a sourced stream filtered to the primary subject
     */
    public function iCreateAnOriginStreamAndASourcedStreamFilteredToThePrimarySubject(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $sourceSubject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $secondarySubject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');
        $separator = strrpos($sourceSubject, '.');
        $prefix = $separator === false ? $sourceSubject : substr($sourceSubject, 0, $separator);
        $originStream = $stream . '_ORIGIN';
        $aggregateSubject = 'behat.aggregate.' . strtolower($stream);
        $js = $this->client('primary')->jetStream();

        $js->createStream($originStream, [$prefix . '.>'])->await();
        $this->createdStreams[] = $originStream;

        $js->createStream($stream, [$aggregateSubject], [
            'sources' => [
                StreamSource::source($originStream)->filterSubject($sourceSubject)->toArray(),
            ],
        ])->await();
        $this->createdStreams[] = $stream;

        $js->publish($sourceSubject, 'sourced-event')->await();
        $js->publish($secondarySubject, 'ignored-event')->await();
    }

    /**
     * @Then the sourced stream should contain only :payload from the primary subject
     */
    public function theSourcedStreamShouldContainOnlyFromThePrimarySubject(string $payload): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $sourceSubject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $this->waitFor(function () use ($js, $stream): bool {
            $state = $js->getStream($stream)->await()->raw['state'] ?? [];
            $messages = max(0, (int) ($state['messages'] ?? 0));

            return $messages >= 1;
        }, 4.0);

        delay(0.5);

        $streamInfo = $js->getStream($stream)->await();
        $messageCount = max(0, (int) (($streamInfo->raw['state'] ?? [])['messages'] ?? 0));
        if ($messageCount !== 1) {
            throw new RuntimeException(sprintf('Expected sourced stream to contain exactly one replicated message, got %d.', $messageCount));
        }

        $message = $js->getStreamMessage($stream, 1)->await();
        if ($message->subject !== $sourceSubject || $message->payload !== $payload) {
            throw new RuntimeException('Expected sourced stream to retain only the filtered primary-subject message.');
        }
    }

    /**
     * @When I create an origin stream and a mirror stream from it
     */
    public function iCreateAnOriginStreamAndAMirrorStreamFromIt(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $sourceSubject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $originStream = $stream . '_ORIGIN';
        $js = $this->client('primary')->jetStream();

        $js->createStream($originStream, [$sourceSubject])->await();
        $this->createdStreams[] = $originStream;

        $js->createStream($stream, [], [
            'mirror' => StreamSource::mirror($originStream)->toArray(),
        ])->await();
        $this->createdStreams[] = $stream;
    }

    /**
     * @When I publish :payload to the mirrored origin subject
     */
    public function iPublishToTheMirroredOriginSubject(string $payload): void
    {
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $this->client('primary')->jetStream()->publish($subject, $payload)->await();
    }

    /**
     * @Then the mirror stream should contain :payload
     */
    public function theMirrorStreamShouldContain(string $payload): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $this->waitFor(function () use ($js, $stream): bool {
            $state = $js->getStream($stream)->await()->raw['state'] ?? [];
            $messages = max(0, (int) ($state['messages'] ?? 0));

            return $messages >= 1;
        }, 4.0);

        $message = $js->getStreamMessage($stream, 1)->await();
        if ($message->subject !== $subject || $message->payload !== $payload) {
            throw new RuntimeException('Expected mirror stream to replicate the origin message unchanged.');
        }
    }

    /**
     * @When I create the JetStream stream and consumer with typed configuration
     */
    public function iCreateTheJetStreamStreamAndConsumerWithTypedConfiguration(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject], [
            'retention' => RetentionPolicy::Limits->value,
            'storage' => StorageBackend::Memory->value,
            'discard' => DiscardPolicy::Old->value,
            'max_msgs' => 100_000,
            'max_bytes' => 50 * 1024 * 1024,
            'max_age' => 86_400_000_000_000,
            'num_replicas' => 1,
            'duplicate_window' => 120_000_000_000,
        ])->await();
        $this->createdStreams[] = $stream;

        $js->createConsumer($stream, $consumer, $subject, [
            'deliver_policy' => DeliverPolicy::New->value,
            'ack_policy' => AckPolicy::Explicit->value,
            'replay_policy' => ReplayPolicy::Instant->value,
            'max_deliver' => 5,
            'max_ack_pending' => 1000,
            'ack_wait' => 30_000_000_000,
        ])->await();

        $this->state->lastStreamRaw = $js->getStream($stream)->await()->raw;
        $this->state->lastConsumerRaw = $js->getConsumer($stream, $consumer)->await()->raw;
    }

    /**
     * @Then the typed JetStream configuration should persist on the stream and consumer
     */
    public function theTypedJetStreamConfigurationShouldPersistOnTheStreamAndConsumer(): void
    {
        $streamConfig = is_array($this->state->lastStreamRaw['config'] ?? null) ? $this->state->lastStreamRaw['config'] : [];
        $consumerConfig = is_array($this->state->lastConsumerRaw['config'] ?? null) ? $this->state->lastConsumerRaw['config'] : [];

        if (($streamConfig['retention'] ?? null) !== RetentionPolicy::Limits->value
            || ($streamConfig['storage'] ?? null) !== StorageBackend::Memory->value
            || ($streamConfig['discard'] ?? null) !== DiscardPolicy::Old->value) {
            throw new RuntimeException('Expected typed stream configuration values to persist.');
        }

        if (($consumerConfig['deliver_policy'] ?? null) !== DeliverPolicy::New->value
            || ($consumerConfig['ack_policy'] ?? null) !== AckPolicy::Explicit->value
            || ($consumerConfig['replay_policy'] ?? null) !== ReplayPolicy::Instant->value) {
            throw new RuntimeException('Expected typed consumer configuration values to persist.');
        }
    }

    /**
     * @When I fetch and ACK the next pull message :payload
     */
    public function iFetchAndAckTheNextPullMessage(string $payload): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $js->createConsumer($stream, $consumer, $subject)->await();
        $js->publish($subject, $payload)->await();

        $message = $js->fetchNext($stream, $consumer, 4000)->await();
        $this->state->lastReplyPayload = $message->payload;
        $js->ack($message)->await();
    }

    /**
     * @When I redeliver a pull message with delayed NAK and then ACK it
     */
    public function iRedeliverAPullMessageWithDelayedNakAndThenAckIt(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $js->createConsumer($stream, $consumer, $subject)->await();
        $js->publish($subject, 'redeliver-event')->await();

        $first = $js->fetchNext($stream, $consumer, 4000)->await();
        $js->nakWithDelay($first, 1200)->await();
        delay(1.5);

        $second = $js->fetchNext($stream, $consumer, 4000)->await();
        $this->state->lastReplyPayload = $second->payload;
        $js->ack($second)->await();
    }

    /**
     * @When I exercise in-progress heartbeats and TERM on a pull consumer
     */
    public function iExerciseInProgressHeartbeatsAndTermOnAPullConsumer(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $js->createConsumer($stream, $consumer, $subject, [
            'ack_wait' => 1_000_000_000,
            'max_deliver' => 3,
        ])->await();

        $js->publish($subject, 'wpi-event')->await();
        $first = $js->fetchNext($stream, $consumer, 4000)->await();
        delay(0.6);
        $js->inProgress($first)->await();

        try {
            $js->fetchBatch($stream, $consumer, 1, 500)->await();
            throw new RuntimeException('Expected no immediate redelivery after WPI heartbeat.');
        } catch (JetStreamException $e) {
            if (!preg_match('/status (404|408)|No messages received within timeout/i', $e->getMessage())) {
                throw $e;
            }
        }

        $redelivered = null;
        $deadline = microtime(true) + 4.0;
        while ($redelivered === null && microtime(true) < $deadline) {
            try {
                $redelivered = $js->fetchNext($stream, $consumer, 800)->await();
            } catch (JetStreamException $e) {
                if (!preg_match('/status (404|408)|No messages received within timeout/i', $e->getMessage())) {
                    throw $e;
                }
            }
        }

        if ($redelivered === null) {
            throw new RuntimeException('Expected redelivery after WPI heartbeat expired.');
        }
        $js->ack($redelivered)->await();

        $js->publish($subject, 'term-event')->await();
        $toTerm = $js->fetchNext($stream, $consumer, 4000)->await();
        $js->term($toTerm)->await();
        delay(1.3);

        try {
            $js->fetchBatch($stream, $consumer, 1, 700)->await();
            throw new RuntimeException('Expected TERM-ed message to stop redelivery.');
        } catch (JetStreamException $e) {
            if (!preg_match('/status (404|408)|No messages received within timeout/i', $e->getMessage())) {
                throw $e;
            }
        }

        $this->state->lastReplyPayload = 'wpi-event';
    }

    /**
     * @When I subscribe with the durable push consumer helper and publish :payload
     */
    public function iSubscribeWithTheDurablePushConsumerHelperAndPublish(string $payload): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $sid = $js->subscribePushConsumer($stream, $consumer, function (NatsMessage $message) use ($js): void {
            $this->state->receivedPayloads[] = $message->payload;
            $js->ack($message)->await();
        }, null, $subject)->await();
        $this->subscriptions['primary'][] = $sid;

        $js->publish($subject, $payload)->await();
        $this->waitFor(function (): bool {
            $this->client('primary')->processIncoming()->await();

            return $this->state->receivedPayloads !== [];
        }, 4.0);
    }

    /**
     * @When I create an ephemeral pull consumer and fetch :payload
     */
    public function iCreateAnEphemeralPullConsumerAndFetch(string $payload): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $consumer = $js->createEphemeralConsumer($stream, $subject)->await();
        $this->state->consumerName = $consumer->name;
        $js->publish($subject, $payload)->await();

        $message = $js->fetchNext($stream, $consumer->name, 4000)->await();
        $this->state->lastReplyPayload = $message->payload;
        $js->ack($message)->await();
    }

    /**
     * @When I subscribe with the ephemeral push consumer helper and publish :payload
     */
    public function iSubscribeWithTheEphemeralPushConsumerHelperAndPublish(string $payload): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $sid = $js->subscribeEphemeralPushConsumer($stream, function (NatsMessage $message) use ($js): void {
            $this->state->receivedPayloads[] = $message->payload;
            if ($message->replyTo !== null && $message->replyTo !== '') {
                $js->ack($message)->await();
            }
        }, null, $subject)->await();
        $this->subscriptions['primary'][] = $sid;

        $js->publish($subject, $payload)->await();
        $this->waitFor(function (): bool {
            $this->client('primary')->processIncoming()->await();

            return $this->state->receivedPayloads !== [];
        }, 4.0);
    }

    /**
     * @When I subscribe with the ordered consumer helper and publish :payload after a non-matching message
     */
    public function iSubscribeWithTheOrderedConsumerHelperAndPublishAfterANonMatchingMessage(string $payload): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $otherSubject = $this->requireValue($this->state->secondarySubject, 'secondary stream subject');
        $separator = strrpos($subject, '.');
        $prefix = $separator === false ? $subject : substr($subject, 0, $separator);
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$prefix . '.>'])->await();
        $this->createdStreams[] = $stream;
        $js->publish($otherSubject, '{"event":"other"}')->await();

        $sid = $js->subscribeOrderedConsumer($stream, function (NatsMessage $message): void {
            $this->state->receivedPayloads[] = $message->payload;
        }, $subject)->await();
        $this->subscriptions['primary'][] = $sid;

        $js->publish($subject, $payload)->await();
        $this->waitFor(function (): bool {
            $this->client('primary')->processIncoming()->await();

            return $this->state->receivedPayloads !== [];
        }, 4.0);
    }

    /**
     * @Then the JetStream helper should receive :payload
     */
    public function theJetStreamHelperShouldReceive(string $payload): void
    {
        if (!in_array($payload, $this->state->receivedPayloads, true) && $this->state->lastReplyPayload !== $payload) {
            throw new RuntimeException(sprintf('Expected JetStream helper flow to receive "%s".', $payload));
        }
    }

    /**
     * @When I pause the current consumer, verify no delivery, then resume it
     */
    public function iPauseTheCurrentConsumerVerifyNoDeliveryThenResumeIt(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $js->createConsumer($stream, $consumer, $subject)->await();
        $js->publish($subject, 'paused-event')->await();

        $pauseResult = $js->pauseConsumer($stream, $consumer, gmdate('Y-m-d\TH:i:s\Z', time() + 30))->await();
        if (!(bool) ($pauseResult['paused'] ?? false)) {
            throw new RuntimeException('Expected consumer to be paused.');
        }

        try {
            $js->fetchBatch($stream, $consumer, 1, 500)->await();
            throw new RuntimeException('Expected paused consumer to suppress pull delivery.');
        } catch (JetStreamException $e) {
            if (!preg_match('/status (404|408)|No messages received within timeout/i', $e->getMessage())) {
                throw $e;
            }
        }

        $resumeResult = $js->resumeConsumer($stream, $consumer)->await();
        if ((bool) ($resumeResult['paused'] ?? true)) {
            throw new RuntimeException('Expected consumer resume to clear paused state.');
        }

        $message = $js->fetchNext($stream, $consumer, 2000)->await();
        $this->state->lastReplyPayload = $message->payload;
        $js->ack($message)->await();
    }

    /**
     * @When I fetch a batch of :count JetStream messages and ACK them
     */
    public function iFetchABatchOfJetStreamMessagesAndAckThem(int $count): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $js->createConsumer($stream, $consumer, $subject)->await();

        for ($i = 0; $i < $count; $i++) {
            $js->publish($subject, 'log entry ' . $i)->await();
        }

        $messages = $js->fetchBatch($stream, $consumer, $count, 3000)->await();
        $this->state->lastBatchPayloads = [];
        foreach ($messages as $message) {
            $this->state->lastBatchPayloads[] = $message->payload;
            $js->ack($message)->await();
        }
    }

    /**
     * @Then the fetched batch should contain :count JetStream messages
     */
    public function theFetchedBatchShouldContainJetStreamMessages(int $count): void
    {
        if (count($this->state->lastBatchPayloads) !== $count) {
            throw new RuntimeException(sprintf('Expected fetched batch to contain %d messages.', $count));
        }
    }

    /**
     * @When I process pull-consumer iteration for :count JetStream messages in batches of :batch
     */
    public function iProcessPullConsumerIterationForJetStreamMessagesInBatchesOf(int $count, int $batch): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $consumer = $this->requireValue($this->state->consumerName, 'consumer');
        $subject = $this->requireValue($this->state->streamSubject, 'primary stream subject');
        $js = $this->client('primary')->jetStream();

        $js->createStream($stream, [$subject])->await();
        $this->createdStreams[] = $stream;
        $js->createConsumer($stream, $consumer, $subject)->await();

        for ($i = 1; $i <= $count; $i++) {
            $js->publish($subject, json_encode(['n' => $i], JSON_THROW_ON_ERROR))->await();
        }

        $seen = [];
        $this->state->lastPullIteratorTotal = $js->pullConsumer($stream, $consumer)
            ->setBatching($batch)
            ->setExpiresMs(700)
            ->setIterations(4)
            ->handle(static function (NatsMessage $message, $context) use (&$seen): void {
                $seen[] = $message->payload;
                if ($message->replyTo !== null && $message->replyTo !== '') {
                    $context->ack($message)->await();
                }
            })->await();

        sort($seen);
        $this->state->lastBatchPayloads = $seen;
    }

    /**
     * @Then pull-consumer iteration should process :count messages total
     */
    public function pullConsumerIterationShouldProcessMessagesTotal(int $count): void
    {
        if ($this->state->lastPullIteratorTotal !== $count || count($this->state->lastBatchPayloads) !== $count) {
            throw new RuntimeException(sprintf('Expected pull-consumer iteration to process %d messages.', $count));
        }
    }

    /**
     * @When I create the JetStream stream with scheduling enabled
     */
    public function iCreateTheJetStreamStreamWithSchedulingEnabled(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $scheduleSubject = $this->requireValue($this->state->scheduleSubject, 'schedule subject');
        $targetSubject = $this->requireValue($this->state->streamSubject, 'stream subject');

        $this->client('primary')->jetStream()->createStream(
            $stream,
            [$scheduleSubject, $targetSubject],
            ['allow_msg_schedules' => true],
        )->await();

        $this->createdStreams[] = $stream;
    }

    /**
     * @When I publish the scheduled message :payload
     */
    public function iPublishTheScheduledMessage(string $payload): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $scheduleSubject = $this->requireValue($this->state->scheduleSubject, 'schedule subject');
        $targetSubject = $this->requireValue($this->state->streamSubject, 'stream subject');

        $ack = $this->client('primary')->jetStream()->publishScheduled(
            $scheduleSubject,
            $targetSubject,
            $payload,
            Schedule::at(new DateTimeImmutable('+2 seconds')),
            null,
        )->await();

        $this->state->lastScheduledAckStream = $ack->stream;
        if ($ack->stream !== $stream) {
            throw new RuntimeException(sprintf('Expected scheduled publish ack for stream "%s" but got "%s".', $stream, $ack->stream));
        }
    }

    /**
     * @Then the scheduled publish should be acknowledged for my stream
     */
    public function theScheduledPublishShouldBeAcknowledgedForMyStream(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        if ($this->state->lastScheduledAckStream !== $stream) {
            throw new RuntimeException(sprintf('Expected scheduled publish ack for stream "%s" but got "%s".', $stream, (string) $this->state->lastScheduledAckStream));
        }
    }

    /**
     * @Then the scheduled message should become visible in the stream
     */
    public function theScheduledMessageShouldBecomeVisibleInTheStream(): void
    {
        $stream = $this->requireValue($this->state->stream, 'stream');
        $js = $this->client('primary')->jetStream();

        $this->waitFor(function () use ($js, $stream): bool {
            $state = $js->getStream($stream)->await()->raw['state'] ?? [];
            $this->state->lastObservedStreamMessages = max(0, (int) ($state['messages'] ?? 0));

            return $this->state->lastObservedStreamMessages >= 1;
        }, 6.0);
    }

    /**
     * @When I create the KeyValue bucket
     */
    public function iCreateTheKeyValueBucket(): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $this->client('primary')->jetStream()->keyValue($bucket)->create()->await();
        $this->createdStreams[] = 'KV_' . $bucket;
    }

    /**
     * @When I watch the KeyValue key :key
     */
    public function iWatchTheKeyValueKey(string $key): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $sid = $this->client('primary')->jetStream()->keyValue($bucket)->watch(function ($entry): void {
            $this->state->observedKvEntries[] = $entry;
        }, $key)->await();

        $this->subscriptions['primary'][] = $sid;
    }

    /**
     * @When I put the KeyValue entry :key with value :value
     */
    public function iPutTheKeyValueEntryWithValue(string $key, string $value): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $this->client('primary')->jetStream()->keyValue($bucket)->put($key, $value)->await();
    }

    /**
     * @Then the KeyValue watch should observe :key with value :value
     */
    public function theKeyValueWatchShouldObserveWithValue(string $key, string $value): void
    {
        $this->waitFor(function () use ($key, $value): bool {
            $this->pumpClient('primary', 0.05);

            foreach ($this->state->observedKvEntries as $entry) {
                if ($entry->key === $key && $entry->value === $value) {
                    return true;
                }
            }

            return false;
        }, 4.0);
    }

    /**
     * @Then the KeyValue entry :key should have value :value
     */
    public function theKeyValueEntryShouldHaveValue(string $key, string $value): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $entry = $this->client('primary')->jetStream()->keyValue($bucket)->get($key)->await();
        $this->state->lastKvEntry = $entry;

        if ($entry === null || $entry->value !== $value) {
            throw new RuntimeException(sprintf('Expected KeyValue entry "%s" to have value "%s".', $key, $value));
        }
    }

    /**
     * @When I delete the KeyValue entry :key
     */
    public function iDeleteTheKeyValueEntry(string $key): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $this->client('primary')->jetStream()->keyValue($bucket)->delete($key)->await();
    }

    /**
     * @Then the KeyValue entry :key should be marked as deleted
     */
    public function theKeyValueEntryShouldBeMarkedAsDeleted(string $key): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $entry = $this->client('primary')->jetStream()->keyValue($bucket)->get($key)->await();
        $this->state->lastKvEntry = $entry;

        if ($entry === null || $entry->key !== $key || $entry->operation !== 'DEL' || $entry->value !== null) {
            throw new RuntimeException(sprintf('Expected KeyValue entry "%s" to be marked as deleted.', $key));
        }
    }

    /**
     * @When I update the KeyValue entry :key from :oldValue to :newValue
     */
    public function iUpdateTheKeyValueEntryFromTo(string $key, string $oldValue, string $newValue): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $kv = $this->client('primary')->jetStream()->keyValue($bucket);

        $entry = $kv->get($key)->await();
        if ($entry === null || $entry->value !== $oldValue || $entry->revision === null) {
            throw new RuntimeException(sprintf('Expected KeyValue entry "%s" with value "%s" before update.', $key, $oldValue));
        }

        $kv->update($key, $newValue, $entry->revision)->await();
        $this->state->lastKvEntry = $kv->get($key)->await();
    }

    /**
     * @When I fetch all KeyValue entries
     */
    public function iFetchAllKeyValueEntries(): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $this->state->lastKvValues = $this->client('primary')->jetStream()->keyValue($bucket)->getAll()->await();
    }

    /**
     * @Then the KeyValue bucket should contain :key with value :value
     */
    public function theKeyValueBucketShouldContainWithValue(string $key, string $value): void
    {
        if (($this->state->lastKvValues[$key] ?? null) !== $value) {
            throw new RuntimeException(sprintf('Expected KeyValue bucket to contain "%s" => "%s".', $key, $value));
        }
    }

    /**
     * @When I purge the KeyValue entry :key
     */
    public function iPurgeTheKeyValueEntry(string $key): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $this->client('primary')->jetStream()->keyValue($bucket)->purge($key)->await();
    }

    /**
     * @Then the KeyValue bucket should not contain :key
     */
    public function theKeyValueBucketShouldNotContain(string $key): void
    {
        if (array_key_exists($key, $this->state->lastKvValues)) {
            throw new RuntimeException(sprintf('Expected KeyValue bucket not to contain "%s".', $key));
        }
    }

    /**
     * @When I fetch the KeyValue status
     */
    public function iFetchTheKeyValueStatus(): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        $this->state->lastKvStatus = $this->client('primary')->jetStream()->keyValue($bucket)->getStatus()->await();
    }

    /**
     * @Then the KeyValue status should reference the current bucket
     */
    public function theKeyValueStatusShouldReferenceTheCurrentBucket(): void
    {
        $bucket = $this->requireValue($this->state->kvBucket, 'KeyValue bucket');
        if (($this->state->lastKvStatus['bucket'] ?? null) !== $bucket) {
            throw new RuntimeException(sprintf('Expected KeyValue status bucket "%s".', $bucket));
        }

        if (($this->state->lastKvStatus['stream'] ?? null) !== 'KV_' . $bucket) {
            throw new RuntimeException(sprintf('Expected KeyValue status stream "%s".', 'KV_' . $bucket));
        }
    }

    /**
     * @When I create the Object Store bucket
     */
    public function iCreateTheObjectStoreBucket(): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $this->client('primary')->jetStream()->objectStore($bucket)->create()->await();
        $this->createdStreams[] = 'OBJ_' . $bucket;
    }

    /**
     * @When I watch Object Store metadata updates
     */
    public function iWatchObjectStoreMetadataUpdates(): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $sid = $this->client('primary')->jetStream()->objectStore($bucket)->watch(function ($info): void {
            $this->state->observedObjectInfos[] = $info;
        })->await();

        $this->subscriptions['primary'][] = $sid;
    }

    /**
     * @When I store the object :name with content :data and content type :contentType
     */
    public function iStoreTheObjectWithContentAndContentType(string $name, string $data, string $contentType): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $this->state->lastObjectInfo = $this->client('primary')->jetStream()->objectStore($bucket)->put($name, $data, [
            'content-type' => $contentType,
        ])->await();
    }

    /**
     * @Then the Object Store watch should observe :name
     */
    public function theObjectStoreWatchShouldObserve(string $name): void
    {
        $this->waitFor(function () use ($name): bool {
            $this->pumpClient('primary', 0.05);

            foreach ($this->state->observedObjectInfos as $info) {
                if ($info->name === $name && !$info->deleted) {
                    return true;
                }
            }

            return false;
        }, 4.0);
    }

    /**
     * @Then the object info for :name should include content type :contentType
     */
    public function theObjectInfoForShouldIncludeContentType(string $name, string $contentType): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $info = $this->client('primary')->jetStream()->objectStore($bucket)->info($name)->await();
        $this->state->lastObjectInfo = $info;

        if ($info === null || $info->name !== $name || ($info->metadata['content-type'] ?? null) !== $contentType) {
            throw new RuntimeException(sprintf('Expected object info for "%s" to include content type "%s".', $name, $contentType));
        }
    }

    /**
     * @Then downloading the object :name should return :data
     */
    public function downloadingTheObjectShouldReturn(string $name, string $data): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $objectData = $this->client('primary')->jetStream()->objectStore($bucket)->get($name)->await();
        $this->state->lastObjectData = $objectData?->data;

        if ($objectData === null || $objectData->data !== $data) {
            throw new RuntimeException(sprintf('Expected object "%s" to download as "%s".', $name, $data));
        }
    }

    /**
     * @Then streaming the object :name to a callback should return :data
     */
    public function streamingTheObjectToACallbackShouldReturn(string $name, string $data): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $this->state->downloadedObjectChunks = [];

        $info = $this->client('primary')->jetStream()->objectStore($bucket)->getToCallback($name, function (string $chunk): void {
            $this->state->downloadedObjectChunks[] = $chunk;
        })->await();

        $this->state->lastObjectInfo = $info;
        $combined = implode('', $this->state->downloadedObjectChunks);

        if ($info === null || $combined !== $data) {
            throw new RuntimeException(sprintf('Expected callback download for "%s" to return "%s".', $name, $data));
        }
    }

    /**
     * @When I list the stored objects
     */
    public function iListTheStoredObjects(): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $this->state->lastObjectList = $this->client('primary')->jetStream()->objectStore($bucket)->list()->await();
    }

    /**
     * @Then the object list should include :name
     */
    public function theObjectListShouldInclude(string $name): void
    {
        foreach ($this->state->lastObjectList as $info) {
            if ($info->name === $name && !$info->deleted) {
                return;
            }
        }

        throw new RuntimeException(sprintf('Expected object list to include "%s".', $name));
    }

    /**
     * @When I fetch the Object Store status
     */
    public function iFetchTheObjectStoreStatus(): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $this->state->lastObjectStatus = $this->client('primary')->jetStream()->objectStore($bucket)->getStatus()->await();
    }

    /**
     * @Then the Object Store status should reference the current bucket
     */
    public function theObjectStoreStatusShouldReferenceTheCurrentBucket(): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        if (($this->state->lastObjectStatus['bucket'] ?? null) !== $bucket) {
            throw new RuntimeException(sprintf('Expected Object Store status bucket "%s".', $bucket));
        }

        if (($this->state->lastObjectStatus['stream'] ?? null) !== 'OBJ_' . $bucket) {
            throw new RuntimeException(sprintf('Expected Object Store status stream "%s".', 'OBJ_' . $bucket));
        }
    }

    /**
     * @When I delete the object :name
     */
    public function iDeleteTheObject(string $name): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $this->state->lastObjectInfo = $this->client('primary')->jetStream()->objectStore($bucket)->delete($name)->await();
    }

    /**
     * @Then the object :name should be marked as deleted
     */
    public function theObjectShouldBeMarkedAsDeleted(string $name): void
    {
        $bucket = $this->requireValue($this->state->objectStoreBucket, 'Object Store bucket');
        $objectData = $this->client('primary')->jetStream()->objectStore($bucket)->get($name)->await();
        $this->state->lastObjectData = $objectData?->data;
        $this->state->lastObjectInfo = $objectData?->info;

        if ($objectData === null || !$objectData->info->deleted || $objectData->data !== null) {
            throw new RuntimeException(sprintf('Expected object "%s" to be marked as deleted.', $name));
        }
    }

    /**
     * @When I start the echo service
     */
    public function iStartTheEchoService(): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $version = $this->requireValue($this->state->serviceVersion, 'service version');
        $subject = $this->requireServiceSubject('echo');

        $service = $this->client('service')->service($serviceName, $version, 'Echo demo')
            ->addEndpoint('echo', $subject, static fn (NatsMessage $message): string => 'reply:' . $message->payload);

        $this->startManagedService('service', $service);
    }

    /**
     * @When I request :payload from the echo service
     */
    public function iRequestFromTheEchoService(string $payload): void
    {
        $reply = $this->requestWithRetry('requester', $this->requireServiceSubject('echo'), $payload);
        $this->state->lastReplyPayload = $reply->payload;
    }

    /**
     * @Then the echo service reply should be :payload
     */
    public function theEchoServiceReplyShouldBe(string $payload): void
    {
        if ($this->state->lastReplyPayload !== $payload) {
            throw new RuntimeException(sprintf('Expected echo service reply "%s" but got "%s".', $payload, (string) $this->state->lastReplyPayload));
        }
    }

    /**
     * @When I start the discovery service with schema and plain endpoints
     */
    public function iStartTheDiscoveryServiceWithSchemaAndPlainEndpoints(): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $version = $this->requireValue($this->state->serviceVersion, 'service version');
        $schemaSubject = $this->requireServiceSubject('schema');
        $plainSubject = $this->requireServiceSubject('plain');

        $service = $this->client('service')->service($serviceName, $version, 'Discovery contract')
            ->addEndpoint('schema-endpoint', $schemaSubject, static fn (NatsMessage $message): string => 'ok:' . $message->payload, schema: [
                'type' => 'object',
                'required' => ['id'],
                'properties' => [
                    'id' => ['type' => 'integer'],
                ],
            ])
            ->addEndpoint('plain-endpoint', $plainSubject, static fn (NatsMessage $message): string => 'ok:' . $message->payload);

        $this->startManagedService('service', $service);
    }

    /**
     * @When I query the service discovery subjects
     */
    public function iQueryTheServiceDiscoverySubjects(): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $subjects = [
            '$SRV.PING.' . $serviceName,
            '$SRV.INFO.' . $serviceName,
            '$SRV.STATS.' . $serviceName,
            '$SRV.SCHEMA.' . $serviceName,
        ];

        $this->state->lastDiscoveryResponses = [];
        foreach ($subjects as $subject) {
            $message = $this->requestWithRetry('requester', $subject, '');
            /** @var array<string,mixed> $payload */
            $payload = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);
            $this->state->lastDiscoveryResponses[$subject] = $payload;
        }
    }

    /**
     * @Then the ping discovery response should describe the current service
     */
    public function thePingDiscoveryResponseShouldDescribeTheCurrentService(): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $version = $this->requireValue($this->state->serviceVersion, 'service version');
        $payload = $this->state->lastDiscoveryResponses['$SRV.PING.' . $serviceName] ?? null;

        if (!is_array($payload)) {
            throw new RuntimeException('Expected ping discovery payload to be present.');
        }

        if (($payload['type'] ?? null) !== 'io.nats.micro.v1.ping_response' || ($payload['name'] ?? null) !== $serviceName || ($payload['version'] ?? null) !== $version) {
            throw new RuntimeException('Ping discovery payload does not match the current service metadata.');
        }
    }

    /**
     * @Then the info discovery response should list :count endpoints
     */
    public function theInfoDiscoveryResponseShouldListEndpoints(int $count): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $payload = $this->state->lastDiscoveryResponses['$SRV.INFO.' . $serviceName] ?? null;

        if (!is_array($payload) || ($payload['type'] ?? null) !== 'io.nats.micro.v1.info_response') {
            throw new RuntimeException('Expected info discovery payload to be present.');
        }

        $endpoints = $payload['endpoints'] ?? null;
        if (!is_array($endpoints) || count($endpoints) !== $count) {
            throw new RuntimeException(sprintf('Expected info discovery response to list %d endpoints.', $count));
        }
    }

    /**
     * @Then the stats discovery response should list :count endpoints
     */
    public function theStatsDiscoveryResponseShouldListEndpoints(int $count): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $payload = $this->state->lastDiscoveryResponses['$SRV.STATS.' . $serviceName] ?? null;

        if (!is_array($payload) || ($payload['type'] ?? null) !== 'io.nats.micro.v1.stats_response') {
            throw new RuntimeException('Expected stats discovery payload to be present.');
        }

        $endpoints = $payload['endpoints'] ?? null;
        if (!is_array($endpoints) || count($endpoints) !== $count) {
            throw new RuntimeException(sprintf('Expected stats discovery response to list %d endpoints.', $count));
        }
    }

    /**
     * @Then the schema discovery response should include schema only for the schema endpoint
     */
    public function theSchemaDiscoveryResponseShouldIncludeSchemaOnlyForTheSchemaEndpoint(): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $schemaSubject = $this->requireServiceSubject('schema');
        $plainSubject = $this->requireServiceSubject('plain');
        $payload = $this->state->lastDiscoveryResponses['$SRV.SCHEMA.' . $serviceName] ?? null;

        if (!is_array($payload) || ($payload['type'] ?? null) !== 'io.nats.micro.v1.schema_response') {
            throw new RuntimeException('Expected schema discovery payload to be present.');
        }

        $endpoints = $payload['endpoints'] ?? null;
        if (!is_array($endpoints)) {
            throw new RuntimeException('Expected schema discovery endpoints to be an array.');
        }

        $bySubject = [];
        foreach ($endpoints as $endpoint) {
            if (is_array($endpoint) && is_string($endpoint['subject'] ?? null)) {
                $bySubject[$endpoint['subject']] = $endpoint;
            }
        }

        if (!isset($bySubject[$schemaSubject]) || !is_array($bySubject[$schemaSubject]['schema'] ?? null)) {
            throw new RuntimeException('Expected schema endpoint to expose a schema payload.');
        }

        if (!isset($bySubject[$plainSubject]) || array_key_exists('schema', $bySubject[$plainSubject])) {
            throw new RuntimeException('Expected plain endpoint not to expose a schema payload.');
        }
    }

    /**
     * @When I start the validated service with observers
     */
    public function iStartTheValidatedServiceWithObservers(): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $version = $this->requireValue($this->state->serviceVersion, 'service version');
        $subject = $this->requireServiceSubject('schema');
        $this->state->serviceObserverEvents = [];
        $this->state->serviceCorrelationIds = [
            'invalid' => 'svc-invalid-' . strtolower(bin2hex(random_bytes(2))),
            'valid' => 'svc-valid-' . strtolower(bin2hex(random_bytes(2))),
        ];

        $service = $this->client('service')->service($serviceName, $version, 'Echo stats')
            ->withSchemaValidator(new BasicJsonSchemaValidator())
            ->addObserver(function (string $event, $endpoint, NatsMessage $message, array $context): void {
                $this->state->serviceObserverEvents[] = [
                    'event' => $event,
                    'correlation_id' => $context['correlation_id'] ?? null,
                    'subject' => $message->subject,
                ];
            })
            ->addEndpoint('echo', $subject, static fn (NatsMessage $message): string => 'reply:' . $message->payload, schema: [
                'type' => 'object',
                'required' => ['id'],
                'properties' => [
                    'id' => ['type' => 'integer'],
                ],
            ]);

        $this->startManagedService('service', $service);
    }

    /**
     * @When I send invalid and valid requests to the validated service
     */
    public function iSendInvalidAndValidRequestsToTheValidatedService(): void
    {
        $subject = $this->requireServiceSubject('schema');
        $invalidCorrelationId = $this->state->serviceCorrelationIds['invalid'] ?? null;
        $validCorrelationId = $this->state->serviceCorrelationIds['valid'] ?? null;
        if (!is_string($invalidCorrelationId) || !is_string($validCorrelationId)) {
            throw new RuntimeException('Expected service correlation ids to be initialized.');
        }

        $invalidReply = $this->requestWithHeadersRetry('requester', $subject, '{"id":"bad"}', [
            'X-Request-Id' => $invalidCorrelationId,
        ]);
        $validReply = $this->requestWithHeadersRetry('requester', $subject, '{"id":1}', [
            'X-Request-Id' => $validCorrelationId,
        ]);

        $this->state->serviceResponsePayloads['invalid'] = json_decode($invalidReply->payload, true, 512, JSON_THROW_ON_ERROR);
        $this->state->serviceResponsePayloads['valid'] = $validReply->payload;
        $this->state->lastServiceStats = $this->services['service']->statsSnapshot();
    }

    /**
     * @Then the invalid service response should be a validation error
     */
    public function theInvalidServiceResponseShouldBeAValidationError(): void
    {
        $payload = $this->state->serviceResponsePayloads['invalid'] ?? null;
        $correlationId = $this->state->serviceCorrelationIds['invalid'] ?? null;

        if (!is_array($payload)) {
            throw new RuntimeException('Expected invalid service response payload to be decoded JSON.');
        }

        if (($payload['type'] ?? null) !== 'io.nats.micro.v1.error' || ($payload['code'] ?? null) !== 'VALIDATION_ERROR' || ($payload['correlation_id'] ?? null) !== $correlationId) {
            throw new RuntimeException('Expected invalid service response to be a validation error with correlation id.');
        }
    }

    /**
     * @Then the valid service response should be :payload
     */
    public function theValidServiceResponseShouldBe(string $payload): void
    {
        if (($this->state->serviceResponsePayloads['valid'] ?? null) !== $payload) {
            throw new RuntimeException(sprintf('Expected valid service response "%s".', $payload));
        }
    }

    /**
     * @Then the valid service response should echo the valid request
     */
    public function theValidServiceResponseShouldEchoTheValidRequest(): void
    {
        $this->theValidServiceResponseShouldBe('reply:{"id":1}');
    }

    /**
     * @Then the service stats should record :requests requests and :errors errors
     */
    public function theServiceStatsShouldRecordRequestsAndErrors(int $requests, int $errors): void
    {
        $stats = $this->state->lastServiceStats;
        $endpoint = is_array($stats['endpoints'][0] ?? null) ? $stats['endpoints'][0] : null;
        if ($endpoint === null) {
            throw new RuntimeException('Expected service stats to include endpoint data.');
        }

        if (($endpoint['num_requests'] ?? null) !== $requests || ($endpoint['num_errors'] ?? null) !== $errors) {
            throw new RuntimeException(sprintf('Expected service stats to record %d requests and %d errors.', $requests, $errors));
        }
    }

    /**
     * @Then the service observers should capture both correlation ids
     */
    public function theServiceObserversShouldCaptureBothCorrelationIds(): void
    {
        $events = $this->state->serviceObserverEvents;
        $eventNames = array_map(static fn (array $event): string => (string) ($event['event'] ?? ''), $events);
        if (!in_array('request_start', $eventNames, true) || !in_array('request_error', $eventNames, true) || !in_array('request_end', $eventNames, true)) {
            throw new RuntimeException('Expected service observers to capture start, error, and end events.');
        }

        $correlationIds = array_values(array_filter(array_map(
            static fn (array $event): ?string => is_string($event['correlation_id'] ?? null) ? $event['correlation_id'] : null,
            $events,
        )));

        foreach ($this->state->serviceCorrelationIds as $correlationId) {
            if (!in_array($correlationId, $correlationIds, true)) {
                throw new RuntimeException(sprintf('Expected service observers to capture correlation id "%s".', $correlationId));
            }
        }
    }

    /**
     * @When I start the grouped echo service
     */
    public function iStartTheGroupedEchoService(): void
    {
        $serviceName = $this->requireValue($this->state->serviceName, 'service name');
        $version = $this->requireValue($this->state->serviceVersion, 'service version');
        $rootSubject = $this->requireServiceSubject('root');

        $service = $this->client('service')->service($serviceName, $version, 'Grouped endpoints');
        $root = $service->addGroup($rootSubject);
        $root->addGroup('v1')->addEndpoint('echo-v1', 'echo', static fn (NatsMessage $message): string => 'v1:' . $message->payload);
        $root->addGroup('v2')->addEndpoint('echo-v2', 'echo', static fn (NatsMessage $message): string => 'v2:' . $message->payload);

        $this->startManagedService('service', $service);
    }

    /**
     * @When I request both grouped service endpoints with payload :payload
     */
    public function iRequestBothGroupedServiceEndpointsWithPayload(string $payload): void
    {
        $replyV1 = $this->requestWithRetry('requester', $this->requireServiceSubject('v1'), $payload);
        $replyV2 = $this->requestWithRetry('requester', $this->requireServiceSubject('v2'), $payload);

        $this->state->serviceResponsePayloads['v1'] = $replyV1->payload;
        $this->state->serviceResponsePayloads['v2'] = $replyV2->payload;
        $this->state->lastServiceStats = $this->services['service']->statsSnapshot();
    }

    /**
     * @Then the grouped service replies should be :v1Reply and :v2Reply
     */
    public function theGroupedServiceRepliesShouldBeAnd(string $v1Reply, string $v2Reply): void
    {
        if (($this->state->serviceResponsePayloads['v1'] ?? null) !== $v1Reply || ($this->state->serviceResponsePayloads['v2'] ?? null) !== $v2Reply) {
            throw new RuntimeException('Expected grouped service replies to match both registered endpoint handlers.');
        }
    }

    /**
     * @Then the grouped service stats should list both grouped subjects
     */
    public function theGroupedServiceStatsShouldListBothGroupedSubjects(): void
    {
        $stats = $this->state->lastServiceStats;
        $subjects = array_map(
            static fn (array $endpoint): string => (string) ($endpoint['subject'] ?? ''),
            is_array($stats['endpoints'] ?? null) ? $stats['endpoints'] : [],
        );

        if (!in_array($this->requireServiceSubject('v1'), $subjects, true) || !in_array($this->requireServiceSubject('v2'), $subjects, true)) {
            throw new RuntimeException('Expected grouped service stats to list both grouped endpoint subjects.');
        }
    }

    /**
     * @When I connect with no_echo enabled and subscribe to my subject
     */
    public function iConnectWithNoEchoEnabledAndSubscribeToMySubject(): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationServerUrl()],
            noEcho: true,
            name: 'behat-noecho',
        ));
        $client->connect()->await();
        $this->clients['noecho'] = $client;

        $sid = $client->subscribe($subject, function (NatsMessage $message): void {
            $this->state->receivedPayloads[] = $message->payload;
            $this->state->receivedSubjects[] = $message->subject;
        })->await();
        $this->subscriptions['noecho'][] = $sid;
    }

    /**
     * @When I publish :payload to my subject from the no_echo client
     */
    public function iPublishToMySubjectFromTheNoEchoClient(string $payload): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $this->client('noecho')->publish($subject, $payload)->await();
    }

    /**
     * @Then I should not receive my own no_echo message
     */
    public function iShouldNotReceiveMyOwnNoEchoMessage(): void
    {
        $deadline = microtime(true) + 0.8;
        while (microtime(true) < $deadline) {
            $this->client('noecho')->processIncoming()->await();
            if ($this->state->receivedPayloads !== []) {
                break;
            }

            delay(0.01);
        }

        if ($this->state->receivedPayloads !== []) {
            throw new RuntimeException('Expected no_echo client not to receive its own published message.');
        }
    }

    /**
     * @When I request :payload on my subject without responders
     */
    public function iRequestOnMySubjectWithoutResponders(string $payload): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');

        try {
            $this->client('primary')->request($subject, $payload, 2000)->await();
        } catch (Throwable $e) {
            $this->recordException($e);
            return;
        }

        throw new RuntimeException('Expected request without responders to fail.');
    }

    /**
     * @Then the request should fail with a no responders error
     */
    public function theRequestShouldFailWithANoRespondersError(): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        if ($this->state->lastExceptionClass !== NatsException::class || !str_contains((string) $this->state->lastExceptionMessage, 'No responders') || !str_contains((string) $this->state->lastExceptionMessage, $subject)) {
            throw new RuntimeException('Expected request to fail with a no responders error mentioning the subject.');
        }
    }

    /**
     * @When a silent responder is subscribed on my request subject
     */
    public function aSilentResponderIsSubscribedOnMyRequestSubject(): void
    {
        $subject = $this->requireValue($this->state->requestSubject, 'request subject');
        $this->state->observedRequestCount = 0;
        $sid = $this->client('secondary')->subscribe($subject, function (NatsMessage $message): void {
            $this->state->observedRequestCount++;
        })->await();

        $this->subscriptions['secondary'][] = $sid;
    }

    /**
     * @When I request :payload on my request subject and wait for timeout
     */
    public function iRequestOnMyRequestSubjectAndWaitForTimeout(string $payload): void
    {
        $subject = $this->requireValue($this->state->requestSubject, 'request subject');
        $secondary = $this->client('secondary');
        $pumpCancellation = new DeferredCancellation();
        $pump = async(function () use ($secondary, $pumpCancellation): void {
            $cancellation = $pumpCancellation->getCancellation();

            while (!$cancellation->isRequested()) {
                try {
                    $secondary->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (Throwable) {
                    delay(0.02);
                }
            }
        });

        try {
            $this->client('primary')->request($subject, $payload, 300)->await();
        } catch (Throwable $e) {
            $this->recordException($e);
        } finally {
            $pumpCancellation->cancel();
            $pump->await();
        }
    }

    /**
     * @Then the request should fail with a timeout error
     */
    public function theRequestShouldFailWithATimeoutError(): void
    {
        $subject = $this->requireValue($this->state->requestSubject, 'request subject');
        if ($this->state->lastExceptionClass !== TimeoutException::class || !str_contains((string) $this->state->lastExceptionMessage, 'Request timed out') || !str_contains((string) $this->state->lastExceptionMessage, $subject)) {
            throw new RuntimeException('Expected request to fail with a timeout error mentioning the subject.');
        }
    }

    /**
     * @Then the silent responder should have received the request
     */
    public function theSilentResponderShouldHaveReceivedTheRequest(): void
    {
        if ($this->state->observedRequestCount < 1) {
            throw new RuntimeException('Expected the silent responder to receive at least one request before timeout.');
        }
    }

    /**
     * @When I drain a subscriber after publishing an in-flight message
     */
    public function iDrainASubscriberAfterPublishingAnInFlightMessage(): void
    {
        $subject = $this->requireValue($this->state->subject, 'subject');
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationServerUrl()],
            name: 'behat-drain',
        ));
        $client->connect()->await();
        $this->clients['drain'] = $client;

        $sid = $client->subscribe($subject, function (NatsMessage $message): void {
            $this->state->receivedPayloads[] = $message->payload;
        })->await();
        $this->subscriptions['drain'][] = $sid;

        $client->publish($subject, 'inflight-1')->await();
        $client->drain()->await();

        try {
            $client->publish($subject, 'after-drain')->await();
        } catch (Throwable $e) {
            $this->recordException($e);
        }
    }

    /**
     * @Then draining should flush the in-flight message and close the client
     */
    public function drainingShouldFlushTheInFlightMessageAndCloseTheClient(): void
    {
        if ($this->state->receivedPayloads !== ['inflight-1']) {
            throw new RuntimeException('Expected drain to flush the in-flight message before closing the connection.');
        }

        if ($this->state->lastExceptionClass !== ConnectionException::class || !str_contains((string) $this->state->lastExceptionMessage, 'Connection is not open')) {
            throw new RuntimeException('Expected subsequent publish after drain to fail on a closed connection.');
        }
    }

    /**
     * @When I subscribe to the wildcard pattern and publish matching and non-matching subjects
     */
    public function iSubscribeToTheWildcardPatternAndPublishMatchingAndNonMatchingSubjects(): void
    {
        $pattern = $this->requireWildcardSubject('pattern');
        $matchA = $this->requireWildcardSubject('match_a');
        $matchB = $this->requireWildcardSubject('match_b');
        $nonMatch = $this->requireWildcardSubject('non_match');

        $subscriber = $this->client('primary');
        $publisher = $this->client('secondary');
        $sid = $subscriber->subscribe($pattern, function (NatsMessage $message): void {
            $this->state->receivedSubjects[] = $message->subject;
            $this->state->receivedPayloads[] = $message->payload;
        })->await();
        $this->subscriptions['primary'][] = $sid;

        $publisher->publish($matchA, 'a')->await();
        $publisher->publish($matchB, 'b')->await();
        $publisher->publish($nonMatch, 'x')->await();

        $this->waitFor(function () use ($subscriber): bool {
            $subscriber->processIncoming()->await();

            return count($this->state->receivedSubjects) >= 2;
        }, 3.0);
    }

    /**
     * @Then I should receive only the matching wildcard subjects and payloads
     */
    public function iShouldReceiveOnlyTheMatchingWildcardSubjectsAndPayloads(): void
    {
        $subjects = $this->state->receivedSubjects;
        $payloads = $this->state->receivedPayloads;
        sort($subjects);
        sort($payloads);

        $expectedSubjects = [
            $this->requireWildcardSubject('match_a'),
            $this->requireWildcardSubject('match_b'),
        ];
        sort($expectedSubjects);

        if ($subjects !== $expectedSubjects || $payloads !== ['a', 'b']) {
            throw new RuntimeException('Expected wildcard subscription to receive only matching subjects and payloads.');
        }
    }

    /**
     * @When I publish a payload larger than the server max payload
     */
    public function iPublishAPayloadLargerThanTheServerMaxPayload(): void
    {
        $client = $this->client('primary');
        $serverInfo = $client->serverInfo();
        if ($serverInfo === null) {
            throw new RuntimeException('Expected server info to be available before oversized publish test.');
        }

        $this->state->serverMaxPayload = $serverInfo->maxPayload;
        $subject = $this->requireValue($this->state->subject, 'subject');
        $oversized = str_repeat('x', $this->state->serverMaxPayload + 1);

        try {
            $client->publish($subject, $oversized)->await();
        } catch (Throwable $e) {
            $this->recordException($e);
            return;
        }

        throw new RuntimeException('Expected oversized publish to be rejected.');
    }

    /**
     * @Then the oversized publish should be rejected by the client
     */
    public function theOversizedPublishShouldBeRejectedByTheClient(): void
    {
        if ($this->state->lastExceptionClass !== ProtocolException::class || !str_contains((string) $this->state->lastExceptionMessage, 'exceeds server max_payload')) {
            throw new RuntimeException('Expected oversized publish to be rejected with a max_payload protocol error.');
        }
    }

    /**
     * @When I connect with valid token authentication
     */
    public function iConnectWithValidTokenAuthentication(): void
    {
        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationTokenServerUrl()],
            token: $this->integrationToken(),
            reconnectEnabled: false,
        ));
    }

    /**
     * @When I connect with invalid token authentication
     */
    public function iConnectWithInvalidTokenAuthentication(): void
    {
        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationTokenServerUrl()],
            token: $this->integrationInvalidToken(),
            reconnectEnabled: false,
        ));
    }

    /**
     * @When I connect with valid username and password authentication
     */
    public function iConnectWithValidUsernameAndPasswordAuthentication(): void
    {
        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationUserPassServerUrl()],
            username: $this->integrationUsername(),
            password: $this->integrationPassword(),
            reconnectEnabled: false,
        ));
    }

    /**
     * @When I connect with an invalid password
     */
    public function iConnectWithAnInvalidPassword(): void
    {
        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationUserPassServerUrl()],
            username: $this->integrationUsername(),
            password: $this->integrationBadPassword(),
            reconnectEnabled: false,
        ));
    }

    /**
     * @When I connect with TLS handshake-first authentication
     */
    public function iConnectWithTlsHandshakeFirstAuthentication(): void
    {
        $caFile = $this->integrationTlsCaFile();
        $certFile = $this->integrationTlsCertFile();
        $keyFile = $this->integrationTlsKeyFile();
        if ($caFile === null || $certFile === null || $keyFile === null) {
            throw new RuntimeException('TLS fixture files are not available for Behat TLS scenarios.');
        }

        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationTlsServerUrl()],
            tlsRequired: true,
            tlsHandshakeFirst: true,
            tlsCaFile: $caFile,
            tlsCertFile: $certFile,
            tlsKeyFile: $keyFile,
            tlsVerifyPeer: (getenv('NATS_TLS_SKIP_VERIFY') !== '1'),
            reconnectEnabled: false,
        ));
    }

    /**
     * @When I attempt a TLS connection without a client certificate
     */
    public function iAttemptATlsConnectionWithoutAClientCertificate(): void
    {
        $caFile = $this->integrationTlsCaFile();
        if ($caFile === null) {
            throw new RuntimeException('TLS CA fixture file is not available for Behat TLS scenarios.');
        }

        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationTlsServerUrl()],
            tlsRequired: true,
            tlsHandshakeFirst: true,
            tlsCaFile: $caFile,
            tlsVerifyPeer: true,
            reconnectEnabled: false,
        ));
    }

    /**
     * @When I connect with JWT nonce authentication
     */
    public function iConnectWithJwtNonceAuthentication(): void
    {
        $jwt = $this->integrationJwt();
        $seed = $this->integrationJwtSeed();
        if ($jwt === null || $seed === null) {
            throw new RuntimeException('JWT fixture files are not available for Behat JWT scenarios.');
        }

        $signer = new NkeySeedSigner($seed);
        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationJwtServerUrl()],
            jwt: $jwt,
            nkey: $signer->publicKey(),
            nonceSigner: $signer,
            reconnectEnabled: false,
        ));
    }

    /**
     * @When I connect with standalone NKey authentication
     */
    public function iConnectWithStandaloneNkeyAuthentication(): void
    {
        $signer = new NkeySeedSigner($this->integrationNkeySeed());
        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationNkeyServerUrl()],
            nkey: $signer->publicKey(),
            nonceSigner: $signer,
            reconnectEnabled: false,
        ));
    }

    /**
     * @When I connect with a generated credentials file
     */
    public function iConnectWithAGeneratedCredentialsFile(): void
    {
        $jwt = $this->integrationJwt();
        $seed = $this->integrationJwtSeed();
        if ($jwt === null || $seed === null) {
            throw new RuntimeException('JWT fixture files are not available for credentials-file Behat scenarios.');
        }

        $tempFile = tempnam(sys_get_temp_dir(), 'nats-creds-');
        if ($tempFile === false) {
            throw new RuntimeException('Failed to create a temporary credentials file.');
        }

        $contents = <<<CREDS
-----BEGIN NATS USER JWT-----
{$jwt}
    -----END NATS USER JWT-----

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
{$seed}
    -----END USER NKEY SEED-----
CREDS;
        file_put_contents($tempFile, $contents);
        $this->state->tempCredentialsFile = $tempFile;

        $creds = CredentialsParser::fromFile($tempFile);
        $signer = new NkeySeedSigner($creds['nkeySeed']);

        $this->attemptConnection('auth', new NatsOptions(
            servers: [$this->integrationJwtServerUrl()],
            jwt: $creds['jwt'],
            nkey: $signer->publicKey(),
            nonceSigner: $signer,
            reconnectEnabled: false,
        ));
    }

    /**
     * @Then the authenticated connection should succeed
     */
    public function theAuthenticatedConnectionShouldSucceed(): void
    {
        if (!$this->state->lastConnectionSucceeded) {
            throw new RuntimeException(sprintf(
                'Expected connection success but got failure %s: %s',
                (string) $this->state->lastConnectionErrorClass,
                (string) $this->state->lastConnectionError,
            ));
        }
    }

    /**
     * @Then the connection should be rejected
     */
    public function theConnectionShouldBeRejected(): void
    {
        if ($this->state->lastConnectionSucceeded) {
            throw new RuntimeException('Expected connection rejection but the connection succeeded.');
        }

        if ($this->state->lastConnectionErrorClass !== ConnectionException::class) {
            throw new RuntimeException(sprintf(
                'Expected %s but got %s.',
                ConnectionException::class,
                (string) $this->state->lastConnectionErrorClass,
            ));
        }
    }

    private function client(string $alias): NatsClient
    {
        if (!isset($this->clients[$alias])) {
            $client = new NatsClient(new NatsOptions(
                servers: [$this->integrationServerUrl()],
                name: 'behat-' . $alias,
            ));
            $client->connect()->await();
            $this->clients[$alias] = $client;
        }

        return $this->clients[$alias];
    }

    private function attemptConnection(string $alias, NatsOptions $options): void
    {
        $this->state->lastConnectionSucceeded = false;
        $this->state->lastConnectionError = null;
        $this->state->lastConnectionErrorClass = null;

        try {
            $client = new NatsClient($options);
            $client->connect()->await();
            $this->clients[$alias] = $client;
            $this->state->lastConnectionSucceeded = true;
        } catch (Throwable $e) {
            $this->state->lastConnectionError = $e->getMessage();
            $this->state->lastConnectionErrorClass = $e::class;
        }
    }

    private function pumpClient(string $alias, float $timeoutSeconds = 1.0): void
    {
        $deadline = microtime(true) + $timeoutSeconds;
        $client = $this->client($alias);

        while (microtime(true) < $deadline) {
            $frames = $client->processIncoming()->await();
            if ($frames === 0) {
                delay(0.01);
            }
        }
    }

    private function startManagedService(string $alias, Service $service): void
    {
        $service->start()->await();
        $this->services[$alias] = $service;

        $cancellation = new DeferredCancellation();
        $this->servicePumpCancellations[$alias] = $cancellation;
        $client = $this->client($alias);

        $this->servicePumps[$alias] = async(function () use ($client, $cancellation): void {
            $token = $cancellation->getCancellation();

            while (!$token->isRequested()) {
                try {
                    $client->processIncoming()->await($token);
                } catch (CancelledException) {
                    break;
                } catch (Throwable) {
                    delay(0.02);
                }
            }
        });
    }

    private function requestWithRetry(string $alias, string $subject, string $payload, int $timeoutMs = 2000): NatsMessage
    {
        for ($attempt = 0; $attempt < 10; $attempt++) {
            try {
                return $this->client($alias)->request($subject, $payload, $timeoutMs)->await();
            } catch (NatsException $e) {
                if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                    throw $e;
                }

                delay(0.1);
            }
        }

        throw new RuntimeException(sprintf('Timed out waiting for responders on subject "%s".', $subject));
    }

    /**
     * @param array<string,string> $headers
     */
    private function requestWithHeadersRetry(string $alias, string $subject, string $payload, array $headers, int $timeoutMs = 2000): NatsMessage
    {
        for ($attempt = 0; $attempt < 10; $attempt++) {
            try {
                return $this->client($alias)->requestWithHeaders($subject, $payload, $headers, $timeoutMs)->await();
            } catch (NatsException $e) {
                if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                    throw $e;
                }

                delay(0.1);
            }
        }

        throw new RuntimeException(sprintf('Timed out waiting for responders on subject "%s".', $subject));
    }

    private function recordException(Throwable $e): void
    {
        $this->state->lastExceptionClass = $e::class;
        $this->state->lastExceptionMessage = $e->getMessage();
    }

    private function requireServiceSubject(string $name): string
    {
        $subject = $this->state->serviceSubjects[$name] ?? null;
        if (!is_string($subject) || $subject === '') {
            throw new RuntimeException(sprintf('Expected service subject "%s" to be initialized.', $name));
        }

        return $subject;
    }

    private function requireWildcardSubject(string $name): string
    {
        $subject = $this->state->wildcardSubjects[$name] ?? null;
        if (!is_string($subject) || $subject === '') {
            throw new RuntimeException(sprintf('Expected wildcard subject "%s" to be initialized.', $name));
        }

        return $subject;
    }

    private function waitFor(callable $condition, float $timeoutSeconds = 4.0): void
    {
        $deadline = microtime(true) + $timeoutSeconds;

        while (microtime(true) < $deadline) {
            if ($condition()) {
                return;
            }

            delay(0.05);
        }

        throw new RuntimeException(sprintf('Timed out after %.2f seconds waiting for scenario condition.', $timeoutSeconds));
    }

    private function requireValue(?string $value, string $name): string
    {
        if ($value === null || $value === '') {
            throw new RuntimeException(sprintf('Expected %s to be initialized in the scenario.', $name));
        }

        return $value;
    }
}
