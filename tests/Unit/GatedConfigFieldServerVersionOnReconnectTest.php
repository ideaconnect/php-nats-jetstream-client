<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * The stream and consumer calls and a failover between server versions (#266). Their pre-flight reads the INFO of
 * the connection when the call is made, not in the step that writes the frame as publishScheduled() does (#267): a
 * call made while the client reconnects is checked against the INFO the connection being replaced left behind.
 * Over tests/Support/ReconnectingTransport.php, whose sessions greet with the versions under test.
 *
 * - From a newer server to an older one, the pre-flight lets the call through and the older server takes the
 *   request; it drops the field, and the reply check refuses the reply, reporting the older server's version.
 * - From an older server to a newer one, the pre-flight refuses the call before anything is sent, although the
 *   newer server would have honored the field: the call can be made again once the client is connected.
 */
final class GatedConfigFieldServerVersionOnReconnectTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        // Every client and connection a test makes is registered as it is constructed (#183), weakly: the shutdown
        // closes what is left and checks that nothing goes on running into the next test.
        $this->closeOpenedConnections();
    }

    /**
     * A createStream() with allow_msg_ttl made while the client fails over from 2.11.0 to 2.10.29 passes the
     * pre-flight on the 2.11.0 INFO, waits for the new connection and is sent there, once. The 2.10.29 server drops
     * the field, and the reply check throws, naming the field and the 2.10.29 server, with the stream existing
     * without the feature.
     */
    public function testACreateMadeWhileFailingOverToAnOlderServerIsRefusedByTheReplyCheck(): void
    {
        [$client, $transport] = $this->reconnectingFromTo('2.11.0', '2.10.29');

        $create = $client->jetStream()->createStream('TTL', ['ttl.>'], ['allow_msg_ttl' => true]);
        $create->ignore();
        delay(0.05);
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the call was made while the client reconnected');
        self::assertFalse($create->isComplete(), 'the pre-flight let the call through, and it waits for the new connection');
        $transport->releaseDial();

        try {
            $create->await(new TimeoutCancellation(2));
            self::fail('expected an UnsupportedFeatureException from the reply check');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_msg_ttl', $e->feature);
            self::assertSame('2.11', $e->requiredVersion);
            self::assertSame('2.10.29', $e->serverVersion);
            self::assertSame(
                'The "allow_msg_ttl" feature requires NATS server 2.11+, and the server (connected server reports 2.10.29) ignored the field: stream "TTL" exists without it (it may have existed before this request: inspect it before changing or deleting it)',
                $e->getMessage(),
            );
        }

        self::assertSame(1, $transport->epoch(), 'the client reconnected once');
        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.STREAM.CREATE.TTL', 0), 'nothing went to the 2.11.0 server');
        self::assertCount(1, $transport->controlLinesStartingWith('PUB $JS.API.STREAM.CREATE.TTL', 1), 'the create went to the 2.10.29 server, once');
    }

    /**
     * A createStream() with allow_msg_ttl made while the client fails over from 2.10.29 to 2.11.0 is refused by the
     * 2.10.29 INFO the replaced connection left behind, before anything is sent, although the 2.11.0 server would
     * have honored it. The refusal says nothing was sent, and no create reaches either server.
     */
    public function testACreateMadeWhileFailingOverToANewerServerIsRefusedByTheReplacedConnectionsVersion(): void
    {
        [$client, $transport] = $this->reconnectingFromTo('2.10.29', '2.11.0');

        $create = $client->jetStream()->createStream('TTL', ['ttl.>'], ['allow_msg_ttl' => true]);
        $create->ignore();
        delay(0.05);
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the call was made while the client reconnected');
        $transport->releaseDial();

        try {
            $create->await(new TimeoutCancellation(2));
            self::fail('expected an UnsupportedFeatureException from the pre-flight');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_msg_ttl', $e->feature);
            self::assertSame('2.10.29', $e->serverVersion);
            self::assertStringEndsWith('(nothing was sent: an older server ignores the field and would set up the stream without it)', $e->getMessage());
        }

        $this->waitUntilOpen($client);
        self::assertSame('2.11.0', $client->serverInfo()?->version);
        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.STREAM.CREATE.'), 'no create reached either server');
    }

    /**
     * A client connected to a server advertising $from whose connection is lost, with the recovery's dial held: the
     * session it reconnects to advertises $to. Each session answers a stream create with a config that lacks every
     * version-gated field, as a server too old for them does.
     *
     * @return array{NatsClient, ReconnectingTransport}
     */
    private function reconnectingFromTo(string $from, string $to): array
    {
        $transport = new ReconnectingTransport();
        $transport->infoByEpoch = [0 => ReconnectingTransport::infoWithVersion($from), 1 => ReconnectingTransport::infoWithVersion($to)];
        $transport->responder = static fn(string $subject, ?string $replyTo): array => $replyTo === null || !str_starts_with($subject, '$JS.API.STREAM.CREATE.')
            ? []
            : $transport->replyFrame($replyTo, '{"type":"io.nats.jetstream.api.v1.stream_create_response","config":{"name":"TTL","subjects":["ttl.>"]}}');
        $client = $this->connectClient($transport);
        self::assertSame($from, $client->serverInfo()?->version);
        $this->startRecoveryHeldMidDial($client, $transport);

        return [$client, $transport];
    }
}
