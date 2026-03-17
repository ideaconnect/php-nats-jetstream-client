<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Behat\Support;

use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\JetStream\ObjectStore\ObjectInfo;

final class ScenarioState
{
    public ?string $subject = null;

    public ?string $requestSubject = null;

    public ?string $stream = null;

    public ?string $streamSubject = null;

    public ?string $scheduleSubject = null;

    public ?string $kvBucket = null;

    public ?string $objectStoreBucket = null;

    public ?string $serviceName = null;

    public ?string $serviceVersion = null;

    public ?string $secondarySubject = null;

    public ?string $consumerName = null;

    public ?string $lastReplyPayload = null;

    public ?string $lastExceptionMessage = null;

    public ?string $lastExceptionClass = null;

    public ?string $lastConnectionError = null;

    public ?string $lastConnectionErrorClass = null;

    public ?string $tempCredentialsFile = null;

    public bool $jetStreamAccountFetched = false;

    public bool $jetStreamStreamDeleted = false;

    public bool $lastConnectionSucceeded = false;

    public int $lastObservedStreamMessages = 0;

    public ?string $lastScheduledAckStream = null;

    public ?KeyValueEntry $lastKvEntry = null;

    /** @var list<KeyValueEntry> */
    public array $observedKvEntries = [];

    /** @var array<string,string> */
    public array $lastKvValues = [];

    /** @var array<string,mixed> */
    public array $lastKvStatus = [];

    public ?ObjectInfo $lastObjectInfo = null;

    /** @var list<ObjectInfo> */
    public array $observedObjectInfos = [];

    /** @var list<ObjectInfo> */
    public array $lastObjectList = [];

    /** @var array<string,mixed> */
    public array $lastObjectStatus = [];

    public ?string $lastObjectData = null;

    /** @var list<string> */
    public array $downloadedObjectChunks = [];

    /** @var list<string> */
    public array $receivedSubjects = [];

    /** @var array<string,string> */
    public array $wildcardSubjects = [];

    public int $observedRequestCount = 0;

    public int $serverMaxPayload = 0;

    public int $lastAckSequence = 0;

    public int $lastPurgeCount = 0;

    public int $lastPullIteratorTotal = 0;

    public ?string $lastObservedRequestHeader = null;

    public ?string $lastServerName = null;

    public ?string $lastDirectSubject = null;

    /** @var array<string,string> */
    public array $lastHeaders = [];

    /** @var list<string> */
    public array $lastQueuePayloads = [];

    /** @var list<string> */
    public array $lastListedStreams = [];

    /** @var list<string> */
    public array $lastListedConsumers = [];

    /** @var list<string> */
    public array $lastBatchPayloads = [];

    /** @var array<string,mixed> */
    public array $lastStreamRaw = [];

    /** @var array<string,mixed> */
    public array $lastConsumerRaw = [];

    /** @var array<string,string> */
    public array $serviceSubjects = [];

    /** @var array<string,string> */
    public array $serviceCorrelationIds = [];

    /** @var array<string,mixed> */
    public array $serviceResponsePayloads = [];

    /** @var array<string,array<string,mixed>> */
    public array $lastDiscoveryResponses = [];

    /** @var array<string,mixed> */
    public array $lastServiceStats = [];

    /** @var list<array<string,mixed>> */
    public array $serviceObserverEvents = [];

    /** @var list<string> */
    public array $receivedPayloads = [];

    public function reset(): void
    {
        $this->subject = null;
        $this->requestSubject = null;
        $this->stream = null;
        $this->streamSubject = null;
        $this->scheduleSubject = null;
        $this->kvBucket = null;
        $this->objectStoreBucket = null;
        $this->serviceName = null;
        $this->serviceVersion = null;
        $this->secondarySubject = null;
        $this->consumerName = null;
        $this->lastReplyPayload = null;
        $this->lastExceptionMessage = null;
        $this->lastExceptionClass = null;
        $this->lastConnectionError = null;
        $this->lastConnectionErrorClass = null;
        $this->tempCredentialsFile = null;
        $this->jetStreamAccountFetched = false;
        $this->jetStreamStreamDeleted = false;
        $this->lastConnectionSucceeded = false;
        $this->lastObservedStreamMessages = 0;
        $this->lastScheduledAckStream = null;
        $this->lastKvEntry = null;
        $this->observedKvEntries = [];
        $this->lastKvValues = [];
        $this->lastKvStatus = [];
        $this->lastObjectInfo = null;
        $this->observedObjectInfos = [];
        $this->lastObjectList = [];
        $this->lastObjectStatus = [];
        $this->lastObjectData = null;
        $this->downloadedObjectChunks = [];
        $this->receivedSubjects = [];
        $this->wildcardSubjects = [];
        $this->observedRequestCount = 0;
        $this->serverMaxPayload = 0;
        $this->lastAckSequence = 0;
        $this->lastPurgeCount = 0;
        $this->lastPullIteratorTotal = 0;
        $this->lastObservedRequestHeader = null;
        $this->lastServerName = null;
        $this->lastDirectSubject = null;
        $this->lastHeaders = [];
        $this->lastQueuePayloads = [];
        $this->lastListedStreams = [];
        $this->lastListedConsumers = [];
        $this->lastBatchPayloads = [];
        $this->lastStreamRaw = [];
        $this->lastConsumerRaw = [];
        $this->serviceSubjects = [];
        $this->serviceCorrelationIds = [];
        $this->serviceResponsePayloads = [];
        $this->lastDiscoveryResponses = [];
        $this->lastServiceStats = [];
        $this->serviceObserverEvents = [];
        $this->receivedPayloads = [];
    }
}
