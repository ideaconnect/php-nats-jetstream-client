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
 * The stream and consumer calls and a failover between server versions (#266, #272). No INFO version decides
 * anything (ADR-44): a call made while the client reconnects waits for the new connection and is sent there, and the
 * reply of the server that takes it is judged. Over tests/Support/ReconnectingTransport.php, whose sessions greet with
 * the versions under test.
 *
 * - From a newer server to an older one, the older server takes the request and drops the field: the reply check
 *   refuses its reply.
 * - From an older server to a newer one, the newer server takes the request and honors the field: the call succeeds.
 *   Up to 2.35.0 a pre-flight refused it on the INFO the replaced connection left behind, before anything was sent.
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
     * A createStream() with allow_msg_ttl made while the client fails over from 2.11.0 to 2.10.29 waits for the new
     * connection and is sent there, once. The 2.10.29 server drops the field, and the reply check throws, naming the
     * field and the 2.10.29 server, with the stream existing without the feature.
     */
    public function testACreateMadeWhileFailingOverToAnOlderServerIsRefusedByTheReplyCheck(): void
    {
        [$client, $transport] = $this->reconnectingFromTo('2.11.0', '2.10.29', honoredOnTheNewSession: false);

        $create = $client->jetStream()->createStream('TTL', ['ttl.>'], ['allow_msg_ttl' => true]);
        $create->ignore();
        delay(0.05);
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the call was made while the client reconnected');
        self::assertFalse($create->isComplete(), 'the call waits for the new connection');
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
     * A createStream() with allow_msg_ttl made while the client fails over from 2.10.29 to 2.11.0 is not judged by
     * the 2.10.29 INFO the replaced connection left behind: it waits for the new connection, goes to the 2.11.0
     * server, once, and returns that server's stream with the feature (#272).
     */
    public function testACreateMadeWhileFailingOverToANewerServerIsSentThere(): void
    {
        [$client, $transport] = $this->reconnectingFromTo('2.10.29', '2.11.0', honoredOnTheNewSession: true);

        $create = $client->jetStream()->createStream('TTL', ['ttl.>'], ['allow_msg_ttl' => true]);
        $create->ignore();
        delay(0.05);
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the call was made while the client reconnected');
        $transport->releaseDial();

        $info = $create->await(new TimeoutCancellation(2));

        self::assertSame('TTL', $info->name);
        self::assertIsArray($info->raw['config'] ?? null);
        self::assertTrue($info->raw['config']['allow_msg_ttl'] ?? null);
        self::assertSame('2.11.0', $client->serverInfo()?->version);
        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.STREAM.CREATE.TTL', 0), 'nothing went to the 2.10.29 server');
        self::assertCount(1, $transport->controlLinesStartingWith('PUB $JS.API.STREAM.CREATE.TTL', 1), 'the create went to the 2.11.0 server, once');
    }

    /**
     * A client connected to a server advertising $from whose connection is lost, with the recovery's dial held: the
     * session it reconnects to advertises $to. The first session answers a stream create with a config that lacks
     * every version-gated field, as a server too old for them does; the new session does the same, or, with
     * $honoredOnTheNewSession, echoes allow_msg_ttl.
     *
     * @return array{NatsClient, ReconnectingTransport}
     */
    private function reconnectingFromTo(string $from, string $to, bool $honoredOnTheNewSession): array
    {
        $transport = new ReconnectingTransport();
        $transport->infoByEpoch = [0 => ReconnectingTransport::infoWithVersion($from), 1 => ReconnectingTransport::infoWithVersion($to)];
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $honoredOnTheNewSession): array {
            if ($replyTo === null || !str_starts_with($subject, '$JS.API.STREAM.CREATE.')) {
                return [];
            }

            $config = $honoredOnTheNewSession && $transport->epoch() === 1
                ? '{"name":"TTL","subjects":["ttl.>"],"allow_msg_ttl":true}'
                : '{"name":"TTL","subjects":["ttl.>"]}';

            return $transport->replyFrame($replyTo, '{"type":"io.nats.jetstream.api.v1.stream_create_response","config":' . $config . '}');
        };
        $client = $this->connectClient($transport);
        self::assertSame($from, $client->serverInfo()?->version);
        $this->startRecoveryHeldMidDial($client, $transport);

        return [$client, $transport];
    }
}
