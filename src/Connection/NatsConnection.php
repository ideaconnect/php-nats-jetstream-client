<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

use Amp\Future;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Core\Inbox;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Protocol\ProtocolCodec;
use IDCT\NATS\Protocol\ProtocolFrame;
use IDCT\NATS\Protocol\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolParser;
use IDCT\NATS\Protocol\ServerInfo;
use IDCT\NATS\Transport\TransportInterface;
use SplQueue;
use function Amp\async;
use function Amp\delay;

final class NatsConnection
{
    private ConnectionState $state = ConnectionState::Idle;
    private ?ServerInfo $serverInfo = null;
    private ProtocolParser $parser;
    private int $nextSid = 1;
    private int $serverCursor = 0;
    /** @var array<int, callable(NatsMessage):void> */
    private array $subscriptions = [];
    /** @var array<int, array{subject: string, queue: ?string}> */
    private array $subscriptionMeta = [];
    /** @var array<int, SplQueue<NatsMessage>> */
    private array $pendingMessages = [];

    /**
     * Creates a connection runtime with transport and protocol dependencies.
     */
    public function __construct(
        private readonly NatsOptions $options,
        private readonly TransportInterface $transport,
        private readonly ProtocolCodec $codec = new ProtocolCodec(),
    ) {
        $this->parser = new ProtocolParser();
    }

    /**
     * Returns the current connection state.
     */
    public function state(): ConnectionState
    {
        return $this->state;
    }

    /**
     * Returns server capabilities discovered during handshake.
     */
    public function serverInfo(): ?ServerInfo
    {
        return $this->serverInfo;
    }

    /**
     * Opens a transport connection and completes NATS CONNECT/PING handshake.
     *
     * @return Future<void>
     */
    public function connect(): Future
    {
        return async(function (): void {
            if ($this->state === ConnectionState::Open) {
                return;
            }

            try {
                $this->connectOnce();
            } catch (\Throwable $e) {
                if ($this->options->reconnectEnabled && $this->options->maxReconnectAttempts > 0) {
                    $this->recoverConnection();

                    return;
                }

                $this->state = ConnectionState::Closed;
                throw new ConnectionException($e->getMessage(), (int) $e->getCode(), $e);
            }
        });
    }

    /**
     * Closes the transport and marks the runtime as closed.
     *
     * @return Future<void>
     */
    public function disconnect(): Future
    {
        return async(function (): void {
            $this->transport->close()->await();
            $this->state = ConnectionState::Closed;
        });
    }

    /**
     * Publishes payload bytes to the given subject.
     *
     * @return Future<void>
     */
    public function publish(string $subject, string $payload, ?string $replyTo = null): Future
    {
        return async(function () use ($subject, $payload, $replyTo): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            try {
                $this->transport->write($this->codec->encodePublish($subject, $payload, $replyTo))->await();
            } catch (\Throwable) {
                $this->recoverConnection();
                $this->transport->write($this->codec->encodePublish($subject, $payload, $replyTo))->await();
            }
        });
    }

    /**
     * Publishes payload bytes with NATS headers to the given subject.
     *
     * @param array<string,string> $headers
     * @return Future<void>
     */
    public function publishWithHeaders(
        string $subject,
        string $payload,
        array $headers,
        ?string $replyTo = null,
    ): Future {
        return async(function () use ($subject, $payload, $headers, $replyTo): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            try {
                $this->transport->write($this->codec->encodeHeaderPublish($subject, $payload, $headers, $replyTo))->await();
            } catch (\Throwable) {
                $this->recoverConnection();
                $this->transport->write($this->codec->encodeHeaderPublish($subject, $payload, $headers, $replyTo))->await();
            }
        });
    }

    /**
     * Registers a subscription callback and sends a SUB command.
     *
     * @param callable(NatsMessage):void $handler
     * @return Future<int>
     */
    public function subscribe(string $subject, callable $handler, ?string $queue = null): Future
    {
        return async(function () use ($subject, $handler, $queue): int {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $sid = $this->nextSid++;
            $this->subscriptions[$sid] = $handler;
            $this->subscriptionMeta[$sid] = ['subject' => $subject, 'queue' => $queue];
            $this->pendingMessages[$sid] = new SplQueue();

            $this->transport->write($this->codec->encodeSubscribe($subject, $sid, $queue))->await();

            return $sid;
        });
    }

    /**
     * Removes a subscription callback and sends an UNSUB command.
     *
     * @return Future<void>
     */
    public function unsubscribe(int $sid, ?int $maxMessages = null): Future
    {
        return async(function () use ($sid, $maxMessages): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            unset($this->subscriptions[$sid]);
            unset($this->subscriptionMeta[$sid]);
            unset($this->pendingMessages[$sid]);
            $this->transport->write($this->codec->encodeUnsubscribe($sid, $maxMessages))->await();
        });
    }

    /**
     * Reads one transport chunk, parses frames, and dispatches message callbacks.
     *
     * @return Future<int>
     */
    public function processIncoming(): Future
    {
        return async(function (): int {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            try {
                $chunk = $this->transport->readLine()->await();
            } catch (CancelledException $cancelledException) {
                throw $cancelledException;
            } catch (\Throwable) {
                $this->recoverConnection();

                return 0;
            }

            if ($chunk === '') {
                return 0;
            }

            $frames = $this->parser->push($chunk);
            foreach ($frames as $frame) {
                $this->handleFrame($frame);
            }

            // Drain buffered deliveries after each chunk to preserve wire-order delivery.
            $this->drainAllPending();

            return count($frames);
        });
    }

    /**
     * Sends a request and awaits the first response on an auto-generated inbox subject.
     *
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<NatsMessage>
     */
    public function request(
        string $subject,
        string $payload,
        ?int $timeoutMs = null,
        ?Cancellation $cancellation = null,
    ): Future
    {
        return async(function () use ($subject, $payload, $timeoutMs, $cancellation): NatsMessage {
            return $this->requestInternal($subject, $payload, null, $timeoutMs, $cancellation);
        });
    }

    /**
     * Sends a request with headers and awaits the first response.
     *
     * @param array<string,string> $headers
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<NatsMessage>
     */
    public function requestWithHeaders(
        string $subject,
        string $payload,
        array $headers,
        ?int $timeoutMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return async(function () use ($subject, $payload, $headers, $timeoutMs, $cancellation): NatsMessage {
            return $this->requestInternal($subject, $payload, $headers, $timeoutMs, $cancellation);
        });
    }

    /**
     * Executes request/reply flow using plain publish or header publish variants.
     *
     * @param array<string,string>|null $headers
     */
    private function requestInternal(
        string $subject,
        string $payload,
        ?array $headers,
        ?int $timeoutMs,
        ?Cancellation $cancellation,
    ): NatsMessage {
        if ($this->state !== ConnectionState::Open) {
            throw new ConnectionException('Connection is not open');
        }

        $inbox = Inbox::generate($this->options->inboxPrefix);
        $response = null;

        $sid = $this->subscribe($inbox, static function (NatsMessage $message) use (&$response): void {
            $response = $message;
        })->await();

        try {
            if ($headers === null) {
                $this->publish($subject, $payload, $inbox)->await();
            } else {
                $this->publishWithHeaders($subject, $payload, $headers, $inbox)->await();
            }

            $deadlineMs = $timeoutMs ?? $this->options->requestTimeoutMs;
            if ($deadlineMs <= 0) {
                throw new TimeoutException('Request timeout must be greater than zero');
            }
            $startedAt = (int) floor(microtime(true) * 1000);

            while ($response === null) {
                $elapsed = (int) floor(microtime(true) * 1000) - $startedAt;
                if ($elapsed >= $deadlineMs) {
                    throw new TimeoutException('Request timed out for subject ' . $subject);
                }

                $remaining = max(1, $deadlineMs - $elapsed);
                    $timeoutCancellation = new TimeoutCancellation($remaining / 1000);
                $waitCancellation = $cancellation === null
                    ? $timeoutCancellation
                    : new CompositeCancellation($cancellation, $timeoutCancellation);

                try {
                    $this->processIncoming()->await($waitCancellation);
                } catch (CancelledException $cancelledException) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw $cancelledException;
                    }

                    throw new TimeoutException('Request timed out for subject ' . $subject);
                }

                if ($response === null) {
                    // Yield briefly when no response has arrived to avoid a tight loop.
                    delay(0.001);
                }
            }

            return $response;
        } finally {
            $this->unsubscribe($sid)->await();
        }
    }

    /**
     * Normalizes NATS DSN scheme to the transport-compatible scheme.
     */
    private function normalizeDsn(string $server): string
    {
        $normalized = preg_replace('#^nats://#', 'tcp://', $server);
        if ($normalized === null) {
            throw new ConnectionException('Invalid server DSN');
        }

        return $normalized;
    }

    /**
     * Establishes a fresh connection against the next available server.
     */
    private function connectOnce(): void
    {
        $this->state = ConnectionState::Connecting;

        $server = $this->nextServer();
        $dsn = $this->normalizeDsn($server);
        $this->transport->connect($dsn, $this->options->connectTimeoutMs)->await();

        $this->serverInfo = $this->awaitServerInfo();

        $this->transport->write($this->codec->encodeConnect($this->options, $this->serverInfo->nonce))->await();
        $this->transport->write($this->codec->encodePing())->await();

        $this->awaitInitialPong();
        // Reset parser state after handshake to avoid carrying partial bootstrap chunks.
        $this->parser = new ProtocolParser();
        $this->state = ConnectionState::Open;
    }

    /**
     * Returns the next server endpoint using round-robin rotation.
     */
    private function nextServer(): string
    {
        $servers = $this->options->servers;
        if ($servers === []) {
            return 'nats://127.0.0.1:4222';
        }

        $index = $this->serverCursor % count($servers);
        $this->serverCursor++;

        return $servers[$index];
    }

    /**
     * Reconnects using retry policy and restores subscription state.
     */
    private function recoverConnection(): void
    {
        if (!$this->options->reconnectEnabled) {
            $this->state = ConnectionState::Closed;
            throw new ConnectionException('Reconnect is disabled');
        }

        $maxAttempts = max(1, $this->options->maxReconnectAttempts);
        $lastError = null;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            try {
                $this->transport->close()->await();
            } catch (\Throwable) {
                // Ignore close failures during reconnect transitions.
            }

            try {
                $this->connectOnce();
                $this->resubscribeAll();

                return;
            } catch (\Throwable $e) {
                $lastError = $e;
                $delayMs = $this->backoffDelayMs($attempt);
                delay($delayMs / 1000);
            }
        }

        $this->state = ConnectionState::Closed;
        throw new ConnectionException(
            'Reconnect attempts exhausted',
            0,
            $lastError,
        );
    }

    /**
     * Replays existing SUB registrations after a reconnect.
     */
    private function resubscribeAll(): void
    {
        foreach ($this->subscriptionMeta as $sid => $meta) {
            $this->transport->write($this->codec->encodeSubscribe($meta['subject'], $sid, $meta['queue']))->await();
        }
    }

    /**
     * Computes reconnect delay including optional jitter.
     */
    private function backoffDelayMs(int $attempt): int
    {
        $base = max(1, $this->options->reconnectDelayMs) * $attempt;
        $jitter = $this->options->reconnectJitterMs > 0 ? random_int(0, $this->options->reconnectJitterMs) : 0;

        return $base + $jitter;
    }

    /**
     * Waits for initial PONG while handling expected intermediary control lines.
     */
    private function awaitInitialPong(): void
    {
        for ($attempt = 0; $attempt < 8; $attempt++) {
            $chunk = $this->transport->readLine()->await();
            if ($chunk === '') {
                continue;
            }

            $frames = $this->parser->push($chunk);
            if ($frames === []) {
                $line = trim($chunk);

                if ($line === '' || $line === '+OK') {
                    continue;
                }

                if ($line === 'PING') {
                    $this->transport->write($this->codec->encodePong())->await();
                    continue;
                }

                if ($line === 'PONG') {
                    return;
                }

                if (str_starts_with($line, '-ERR')) {
                    throw new ConnectionException('Server error during connect: ' . $line);
                }

                continue;
            }

            foreach ($frames as $frame) {
                if ($frame->type === ProtocolFrameType::Ok) {
                    continue;
                }

                if ($frame->type === ProtocolFrameType::Ping) {
                    $this->transport->write($this->codec->encodePong())->await();
                    continue;
                }

                if ($frame->type === ProtocolFrameType::Pong) {
                    return;
                }

                if ($frame->type === ProtocolFrameType::Err) {
                    throw new ConnectionException('Server error during connect: ' . ($frame->error ?? 'unknown'));
                }
            }
        }

        throw new ConnectionException('Expected PONG after CONNECT');
    }

    /**
     * Waits for and parses the initial INFO frame sent by the server.
     */
    private function awaitServerInfo(): ServerInfo
    {
        for ($attempt = 0; $attempt < 8; $attempt++) {
            $chunk = $this->transport->readLine()->await();
            if ($chunk === '') {
                continue;
            }

            $frames = $this->parser->push($chunk);
            if ($frames === []) {
                $line = trim($chunk);
                if (str_starts_with($line, 'INFO ')) {
                    return $this->codec->parseServerInfo($line);
                }

                continue;
            }

            foreach ($frames as $frame) {
                if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
                    /** @var array<string,mixed> $data */
                    $data = json_decode($frame->infoPayload, true, 512, JSON_THROW_ON_ERROR);

                    return ServerInfo::fromInfoPayload($data);
                }
            }
        }

        throw new ConnectionException('Expected INFO during connect');
    }

    /**
     * Handles non-message frames immediately and queues message frames for delivery.
     */
    private function handleFrame(ProtocolFrame $frame): void
    {
        if ($frame->type === ProtocolFrameType::Ping) {
            $this->transport->write($this->codec->encodePong())->await();

            return;
        }

        if ($frame->type === ProtocolFrameType::Err) {
            throw new ConnectionException('Server sent error frame: ' . ($frame->error ?? 'unknown'));
        }

        if ($frame->type === ProtocolFrameType::Msg || $frame->type === ProtocolFrameType::HMsg) {
            $sid = $frame->sid;
            if ($sid === null || !isset($this->subscriptions[$sid])) {
                return;
            }

            [$rawHeaders, $payload] = $this->extractHeadersAndPayload($frame);
            $message = new NatsMessage(
                subject: $frame->subject ?? '',
                sid: $sid,
                replyTo: $frame->replyTo,
                payload: $payload,
                rawHeaders: $rawHeaders,
            );

            $this->enqueueMessage($sid, $message);
        }
    }

    /**
     * Splits HMSG combined data into raw headers and payload body bytes.
     *
     * @return array{0: ?string, 1: string}
     */
    private function extractHeadersAndPayload(ProtocolFrame $frame): array
    {
        $payload = $frame->payload ?? '';

        if ($frame->type !== ProtocolFrameType::HMsg || $frame->headerBytes === null || $frame->headerBytes <= 0) {
            return [null, $payload];
        }

        // Header bytes include only the wire header block; remainder is message body.
        $headerBytes = min($frame->headerBytes, strlen($payload));
        $headers = substr($payload, 0, $headerBytes);
        $body = substr($payload, $headerBytes);

        return [$headers, $body];
    }

    /**
     * Adds a message to a subscription queue and applies slow-consumer policy when full.
     */
    private function enqueueMessage(int $sid, NatsMessage $message): void
    {
        if (!isset($this->pendingMessages[$sid])) {
            $this->pendingMessages[$sid] = new SplQueue();
        }

        $queue = $this->pendingMessages[$sid];
        $limit = max(1, $this->options->maxPendingMessagesPerSubscription);

        if ($queue->count() >= $limit) {
            if ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropOldest) {
                $queue->dequeue();
            } elseif ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropNewest) {
                return;
            } else {
                throw new ConnectionException('Subscription queue overflow for sid ' . $sid);
            }
        }

        $queue->enqueue($message);
    }

    /**
     * Drains all queued subscription messages in SID order.
     */
    private function drainAllPending(): void
    {
        foreach (array_keys($this->pendingMessages) as $sid) {
            $this->drainPendingForSid($sid);
        }
    }

    /**
     * Delivers buffered messages to a single subscription callback in FIFO order.
     */
    private function drainPendingForSid(int $sid): void
    {
        if (!isset($this->pendingMessages[$sid], $this->subscriptions[$sid])) {
            return;
        }

        $queue = $this->pendingMessages[$sid];

        while (!$queue->isEmpty()) {
            if (!array_key_exists($sid, $this->subscriptions)) {
                break;
            }

            /** @var NatsMessage $message */
            $message = $queue->dequeue();
            $this->subscriptions[$sid]($message);
        }
    }
}
