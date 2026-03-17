<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolFrame;
use IDCT\NATS\Protocol\ServerInfo;
use IDCT\NATS\Transport\TransportInterface;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\FlakyTransport;
use PHPUnit\Framework\TestCase;
use function Amp\async;
use function Amp\delay;

final class NatsConnectionInternalsTest extends TestCase
{
    public function testNormalizeDsnConvertsNatsScheme(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $normalized = $this->invokePrivate($connection, 'normalizeDsn', 'nats://127.0.0.1:4222');
        $passthrough = $this->invokePrivate($connection, 'normalizeDsn', 'tls://example.org:4443');

        self::assertSame('tcp://127.0.0.1:4222', $normalized);
        self::assertSame('tls://example.org:4443', $passthrough);
    }

    public function testNextServerRoundRobinAndFallback(): void
    {
        $rotating = new NatsConnection(
            new NatsOptions(servers: ['nats://a:4222', 'nats://b:4222']),
            new FakeTransport(),
        );

        self::assertSame('nats://a:4222', $this->invokePrivate($rotating, 'nextServer'));
        self::assertSame('nats://b:4222', $this->invokePrivate($rotating, 'nextServer'));
        self::assertSame('nats://a:4222', $this->invokePrivate($rotating, 'nextServer'));

        $fallback = new NatsConnection(new NatsOptions(servers: []), new FakeTransport());
        self::assertSame('nats://127.0.0.1:4222', $this->invokePrivate($fallback, 'nextServer'));
    }

    public function testValidateSubjectPrivateBranches(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards must occupy an entire token');
        $this->invokePrivate($connection, 'validateSubject', 'orders.a*', true);
    }

    public function testValidateSubjectRejectsGreaterThanMiddleToken(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcard ">" must be the last token');
        $this->invokePrivate($connection, 'validateSubject', 'orders.>.created', true);
    }

    public function testIsNoRespondersStatusPrivateChecks(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $noHeaders = new NatsMessage('s', 1, null, '', null);
        self::assertFalse($this->invokePrivate($connection, 'isNoRespondersStatus', $noHeaders));

        $badHeaders = new NatsMessage('s', 1, null, '', "NATS/1.0 200\r\n\r\n");
        self::assertFalse($this->invokePrivate($connection, 'isNoRespondersStatus', $badHeaders));

        $noResponders = new NatsMessage('s', 1, null, '', "NATS/1.0 503 No Responders\r\n\r\n");
        self::assertTrue($this->invokePrivate($connection, 'isNoRespondersStatus', $noResponders));
    }

    public function testExtractHeadersAndPayloadPrivatePaths(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $msgFrame = new ProtocolFrame(type: ProtocolFrameType::Msg, payload: 'abc');
        [$rawHeaders, $payload] = $this->invokePrivate($connection, 'extractHeadersAndPayload', $msgFrame);
        self::assertNull($rawHeaders);
        self::assertSame('abc', $payload);

        $hmsgFrame = new ProtocolFrame(type: ProtocolFrameType::HMsg, payload: "NATS/1.0\r\n\r\nbody", headerBytes: 12);
        [$hRaw, $hPayload] = $this->invokePrivate($connection, 'extractHeadersAndPayload', $hmsgFrame);
        self::assertSame("NATS/1.0\r\n\r\n", $hRaw);
        self::assertSame('body', $hPayload);

        $invalid = new ProtocolFrame(type: ProtocolFrameType::HMsg, payload: 'short', headerBytes: 10);
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Malformed HMSG frame');
        $this->invokePrivate($connection, 'extractHeadersAndPayload', $invalid);
    }

    public function testCleanupRequestSubscriptionFallsBackToLocalDropWhenClosed(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $sid = 99;
        $this->setPrivate($connection, 'state', ConnectionState::Closed);
        $this->setPrivate($connection, 'subscriptions', [
            $sid => static function (NatsMessage $message): void {
            },
        ]);
        $this->setPrivate($connection, 'subscriptionMeta', [
            $sid => ['subject' => '_INBOX.x', 'queue' => null],
        ]);
        $this->setPrivate($connection, 'pendingMessages', [
            $sid => new \SplQueue(),
        ]);

        $this->invokePrivate($connection, 'cleanupRequestSubscription', $sid);

        self::assertSame([], $this->getPrivate($connection, 'subscriptions'));
        self::assertSame([], $this->getPrivate($connection, 'subscriptionMeta'));
        self::assertSame([], $this->getPrivate($connection, 'pendingMessages'));
    }

    public function testRecoverConnectionDisabledThrowsImmediately(): void
    {
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Reconnect is disabled');
        $this->invokePrivate($connection, 'recoverConnection');
    }

    public function testRecoverConnectionExhaustedSetsClosedState(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
                    'PONG',
                ],
            ],
            connectFailures: 10,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Reconnect attempts exhausted');

        try {
            $this->invokePrivate($connection, 'recoverConnection');
        } finally {
            self::assertSame(ConnectionState::Closed, $connection->state());
        }
    }

    public function testConnectReturnsImmediatelyWhenAlreadyOpen(): void
    {
        $transport = new FakeTransport();
        $connection = new NatsConnection(new NatsOptions(), $transport);
        $this->setPrivate($connection, 'state', ConnectionState::Open);

        $connection->connect()->await();

        self::assertSame([], $transport->connectCalls);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testAwaitServerInfoThrowsWhenInfoNeverArrives(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport(['PONG', '+OK', 'PING', '', '', '', '', '']));

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Expected INFO during connect');
        $this->invokePrivate($connection, 'awaitServerInfo');
    }

    public function testAwaitInitialPongThrowsWhenPongNeverArrives(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport(['+OK', 'PING', '', 'INFO {}', '', '', '', '']));

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Expected PONG after CONNECT');
        $this->invokePrivate($connection, 'awaitInitialPong');
    }

    public function testAwaitInitialPongHandlesParsedControlFrames(): void
    {
        $transport = new FakeTransport([
            "+OK\r\nPING\r\nPONG\r\n",
        ]);
        $connection = new NatsConnection(new NatsOptions(), $transport);

        $this->invokePrivate($connection, 'awaitInitialPong');

        self::assertSame("PONG\r\n", $transport->writes[0] ?? null);
    }

    public function testAwaitInitialPongThrowsOnParsedErrFrame(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport([
            "-ERR 'boom'\r\n",
        ]));

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server error during connect');
        $this->invokePrivate($connection, 'awaitInitialPong');
    }

    public function testAwaitServerInfoAllowsMoreThanEightPollsBeforeInfoArrives(): void
    {
        $queue = array_merge(
            array_fill(0, 12, ''),
            ['INFO {"server_id":"S9","server_name":"n9","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}'],
        );

        $connection = new NatsConnection(new NatsOptions(connectTimeoutMs: 100), new FakeTransport($queue));

        $info = $this->invokePrivate($connection, 'awaitServerInfo');

        self::assertSame('S9', $info->serverId);
    }

    public function testAwaitInitialPongAllowsMoreThanEightPollsBeforePongArrives(): void
    {
        $queue = array_merge(array_fill(0, 12, '+OK'), ['PONG']);

        $connection = new NatsConnection(new NatsOptions(connectTimeoutMs: 100), new FakeTransport($queue));

        self::assertNull($this->invokePrivate($connection, 'awaitInitialPong'));
    }

    public function testAwaitServerInfoRespondsToPingBeforeInfo(): void
    {
        $transport = new FakeTransport([
            'PING',
            'INFO {"server_id":"S4","server_name":"n4","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
        ]);
        $connection = new NatsConnection(new NatsOptions(connectTimeoutMs: 100), $transport);

        $info = $this->invokePrivate($connection, 'awaitServerInfo');

        self::assertSame('S4', $info->serverId);
        self::assertSame("PONG\r\n", $transport->writes[0] ?? null);
    }

    public function testAwaitServerInfoParsesInfoLine(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
        ]));

        $info = $this->invokePrivate($connection, 'awaitServerInfo');

        self::assertInstanceOf(ServerInfo::class, $info);
        self::assertSame('S1', $info->serverId);
    }

    public function testAwaitServerInfoParsesInfoFrame(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport([
            "INFO {\"server_id\":\"S2\",\"server_name\":\"n2\",\"version\":\"2.12.0\",\"jetstream\":true,\"max_payload\":1048576,\"headers\":true}\r\n",
        ]));

        $info = $this->invokePrivate($connection, 'awaitServerInfo');

        self::assertInstanceOf(ServerInfo::class, $info);
        self::assertSame('S2', $info->serverId);
    }

    public function testAwaitInitialPongThrowsOnErrLine(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport([
            '-ERR Permissions Violation',
        ]));

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server error during connect');
        $this->invokePrivate($connection, 'awaitInitialPong');
    }

    public function testHandleFramePongResetsOutstandingPingAndDrainFlag(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());
        $this->setPrivate($connection, 'outstandingPings', 5);
        $this->setPrivate($connection, 'drainFlushPending', true);

        $this->invokePrivate($connection, 'handleFrame', new ProtocolFrame(type: ProtocolFrameType::Pong));

        self::assertSame(0, $this->getPrivate($connection, 'outstandingPings'));
        self::assertFalse($this->getPrivate($connection, 'drainFlushPending'));
    }

    public function testHandleFrameErrThrowsConnectionException(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server sent error frame');
        $this->invokePrivate($connection, 'handleFrame', new ProtocolFrame(type: ProtocolFrameType::Err, error: 'boom'));
    }

    public function testHandleFrameInfoUpdatesServerInfo(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->invokePrivate($connection, 'handleFrame', new ProtocolFrame(
            type: ProtocolFrameType::Info,
            infoPayload: '{"server_id":"S2","server_name":"n2","version":"2.12.1","jetstream":true,"max_payload":2048,"headers":true}',
        ));

        $serverInfo = $connection->serverInfo();
        self::assertInstanceOf(ServerInfo::class, $serverInfo);
        self::assertSame('S2', $serverInfo->serverId);
        self::assertSame(2048, $serverInfo->maxPayload);
    }

    public function testHandleFrameRecoverableErrDoesNotThrow(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());
        $this->setPrivate($connection, 'state', ConnectionState::Open);

        $this->invokePrivate($connection, 'handleFrame', new ProtocolFrame(
            type: ProtocolFrameType::Err,
            error: "'Permissions Violation for Publish to updates'",
        ));

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testHandleFrameIgnoresUnknownSubscriptionSid(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->invokePrivate($connection, 'handleFrame', new ProtocolFrame(
            type: ProtocolFrameType::Msg,
            subject: 'updates',
            sid: 42,
            payload: 'ignored',
        ));

        self::assertSame([], $this->getPrivate($connection, 'pendingMessages'));
    }

    public function testDrainAllPendingDeliversBufferedMessagesInOrder(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $queue = new \SplQueue();
        $queue->enqueue(new NatsMessage('subj', 1, null, 'a'));
        $queue->enqueue(new NatsMessage('subj', 1, null, 'b'));

        $seen = [];
        $this->setPrivate($connection, 'subscriptions', [
            1 => static function (NatsMessage $message) use (&$seen): void {
                $seen[] = $message->payload;
            },
        ]);
        $this->setPrivate($connection, 'pendingMessages', [1 => $queue]);

        $this->invokePrivate($connection, 'drainAllPending');

        self::assertSame(['a', 'b'], $seen);
    }

    public function testEnforceMaxPayloadAllowsUnknownServerInfoAndThrowsWhenExceeded(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        // No server info yet: no payload enforcement should happen.
        $this->invokePrivate($connection, 'enforceMaxPayload', 10);

        $serverInfo = ServerInfo::fromInfoPayload([
            'server_id' => 'S1',
            'server_name' => 'n1',
            'version' => '2.12.0',
            'jetstream' => true,
            'max_payload' => 8,
            'headers' => true,
        ]);
        $this->setPrivate($connection, 'serverInfo', $serverInfo);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('exceeds server max_payload');
        $this->invokePrivate($connection, 'enforceMaxPayload', 9);
    }

    public function testCleanupRequestSubscriptionNoOpForUnknownSid(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->invokePrivate($connection, 'cleanupRequestSubscription', 777);

        self::assertSame([], $this->getPrivate($connection, 'subscriptions'));
        self::assertSame([], $this->getPrivate($connection, 'subscriptionMeta'));
        self::assertSame([], $this->getPrivate($connection, 'pendingMessages'));
    }

    public function testCleanupRequestSubscriptionFallsBackWhenUnsubscribeThrows(): void
    {
        $transport = new class () implements TransportInterface {
            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(static function () use ($bytes): void {
                    if (str_starts_with($bytes, 'UNSUB ')) {
                        throw new \RuntimeException('write failed');
                    }
                });
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(static function (): string {
                    return '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {
                });
            }
        };

        $connection = new NatsConnection(new NatsOptions(), $transport);

        $sid = 77;
        $this->setPrivate($connection, 'state', ConnectionState::Open);
        $this->setPrivate($connection, 'subscriptions', [
            $sid => static function (NatsMessage $message): void {
            },
        ]);
        $this->setPrivate($connection, 'subscriptionMeta', [
            $sid => ['subject' => '_INBOX.req', 'queue' => null],
        ]);
        $this->setPrivate($connection, 'pendingMessages', [
            $sid => new \SplQueue(),
        ]);

        $this->invokePrivate($connection, 'cleanupRequestSubscription', $sid);

        self::assertSame([], $this->getPrivate($connection, 'subscriptions'));
        self::assertSame([], $this->getPrivate($connection, 'subscriptionMeta'));
        self::assertSame([], $this->getPrivate($connection, 'pendingMessages'));
    }

    public function testDrainPendingForSidNoOpWhenStateMissing(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        self::assertNull($this->invokePrivate($connection, 'drainPendingForSid', 101));
    }

    public function testIsNoRespondersStatusHandlesEmptyRawHeaderString(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());
        $message = new NatsMessage('s', 1, null, '', '');

        self::assertFalse($this->invokePrivate($connection, 'isNoRespondersStatus', $message));
    }

    public function testStartPingTimerCancelsWhenConnectionStateIsNotOpen(): void
    {
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 1), new FakeTransport());
        $this->setPrivate($connection, 'state', ConnectionState::Closed);

        $this->invokePrivate($connection, 'startPingTimer');
        delay(1.1);

        self::assertNull($this->getPrivate($connection, 'pingTimerId'));
    }

    public function testStartPingTimerWriteFailureClosesWhenReconnectDisabled(): void
    {
        $transport = new class () implements TransportInterface {
            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(static function () use ($bytes): void {
                    if ($bytes === "PING\r\n") {
                        throw new \RuntimeException('ping write failed');
                    }
                });
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(static function (): string {
                    return '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {
                });
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 1,
                maxPingsOut: 3,
                reconnectEnabled: false,
            ),
            $transport,
        );

        $this->setPrivate($connection, 'state', ConnectionState::Open);
        $this->invokePrivate($connection, 'startPingTimer');
        delay(1.1);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertNull($this->getPrivate($connection, 'pingTimerId'));
    }

    public function testDropSubscriptionStateRemovesEntries(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());
        $sid = 5;

        $this->setPrivate($connection, 'subscriptions', [
            $sid => static function (NatsMessage $message): void {
            },
        ]);
        $this->setPrivate($connection, 'subscriptionMeta', [
            $sid => ['subject' => 'events', 'queue' => null],
        ]);
        $this->setPrivate($connection, 'pendingMessages', [
            $sid => new \SplQueue(),
        ]);

        $this->invokePrivate($connection, 'dropSubscriptionState', $sid);

        self::assertSame([], $this->getPrivate($connection, 'subscriptions'));
        self::assertSame([], $this->getPrivate($connection, 'subscriptionMeta'));
        self::assertSame([], $this->getPrivate($connection, 'pendingMessages'));
    }

    private function invokePrivate(object $object, string $method, mixed ...$args): mixed
    {
        $ref = new \ReflectionMethod($object, $method);
        return $ref->invoke($object, ...$args);
    }

    private function setPrivate(object $object, string $property, mixed $value): void
    {
        $ref = new \ReflectionProperty($object, $property);
        $ref->setValue($object, $value);
    }

    private function getPrivate(object $object, string $property): mixed
    {
        $ref = new \ReflectionProperty($object, $property);
        return $ref->getValue($object);
    }
}
