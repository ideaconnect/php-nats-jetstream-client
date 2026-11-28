<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\Models\PubAck;
use IDCT\NATS\JetStream\Schedule;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * publishScheduled() and the server version (#267, #272). nats-server 2.12 accepts only "@at" schedules: it rejects
 * "@every", cron and the predefined aliases with a 400 ("message schedules pattern is invalid", err_code 10189), and
 * stores an "@at" schedule while ignoring Nats-Schedule-Source and Nats-Schedule-Rollup. Those arrived in 2.14.0, with
 * Nats-Schedule-Time-Zone.
 *
 * From 2.31.0 to 2.35.0 the call refused what needs 2.14 when the connected server's INFO version parsed as older. ADR-44
 * (revision 4) forbids that: the schedule is stored by the server that hosts the stream, which in a cluster during a
 * rolling upgrade may run a newer release than the one the client is connected to. Every call is now published
 * whatever the INFO version, and the server that stores the schedule decides: its 400 reaches the caller as an ordinary
 * JetStreamException. A call made while the client reconnects, or retried after a failed write, goes to the new
 * session whatever either session's version (over tests/Support/ReconnectingTransport.php, whose sessions greet with
 * the versions under test).
 */
final class ScheduledPublishServerVersionTest extends TestCase
{
    use ReconnectScenarios;

    private const AT = '@at 2030-01-01T00:00:00Z';

    private const ACK = '{"stream":"SCHED","seq":5,"duplicate":false}';

    /** What a 2.12.15 server answers a recurring schedule with (seen live, #272). */
    private const PATTERN_INVALID = '{"error":{"code":400,"err_code":10189,"description":"message schedules pattern is invalid"}}';

    protected function tearDown(): void
    {
        // Every client and connection a test makes is registered as it is constructed (#183), weakly: the shutdown
        // closes what is left and checks that nothing goes on running into the next test.
        $this->closeOpenedConnections();
    }

    /**
     * The calls that need a 2.14 server.
     *
     * @return iterable<string, array{\Closure(JetStreamContext): Future<PubAck>}>
     */
    public static function callsNeeding214(): iterable
    {
        yield 'an @every schedule' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::every(2)),
        ];
        yield 'a cron schedule' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::cron('0 0 * * * *')),
        ];
        yield 'a cron schedule with a time zone' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::cron('0 0 * * * *'), timeZone: 'Europe/Amsterdam'),
        ];
        yield 'a predefined alias' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::predefined('hourly')),
        ];
        yield 'an @at schedule with a source' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: 'sensors.temperature'),
        ];
        yield 'an @at schedule with rollup' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, rollup: true),
        ];
        yield 'an @at schedule with a source and rollup' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: 'sensors.temperature', rollup: true),
        ];
        yield 'an @every schedule with a source' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::every('1m'), source: 'sensors.temperature'),
        ];
    }

    /**
     * Every call that needs 2.14 is published, once, whatever version the connected server's INFO reports: older ones
     * (2.10.29 and 2.11.17 as in a rolling upgrade, 2.12.x, a bare major), 2.14 and later, pre-release tags, an
     * unparseable version and none at all. The acknowledgement of the server that stores it is returned (#272).
     *
     * @param \Closure(JetStreamContext): Future<PubAck> $call
     */
    #[DataProvider('callsNeeding214')]
    public function testCallsNeeding214ArePublishedWhateverTheInfoVersion(\Closure $call): void
    {
        $versions = ['2.10.29', '2.11.17', '2.12.15', '2.12.0', 'v2.12.0', '1.99.0', '2.13.9', '2', '2.14.0', '2.14.0-beta.1', 'v2.14.10', '3.0.0', 'custom-build', null];
        foreach ($versions as $version) {
            [$client, $transport] = $this->connectedTo($version, self::ACK);

            $ack = $call($client->jetStream())->await();

            self::assertSame('SCHED', $ack->stream, (string) $version);
            self::assertSame(5, $ack->seq, (string) $version);
            self::assertCount(4, $transport->writes, sprintf('CONNECT + PING, the inbox SUB, one schedule on %s', $version));
            self::assertStringStartsWith('HPUB schedules.tick _INBOX.', $transport->writes[3], (string) $version);
            self::assertStringContainsString('Nats-Schedule-Target:events.tick', $transport->writes[3], (string) $version);
        }
    }

    /**
     * Every header of a call reaches the server as it is, on a connected server whose INFO reports 2.12.15 as on one
     * that reports 2.14.0: the INFO version changes and drops nothing (ADR-44, #272).
     */
    public function testEveryHeaderIsSentWhateverTheInfoVersion(): void
    {
        foreach (['2.12.15', '2.14.0'] as $version) {
            [$client, $transport] = $this->connectedTo($version, self::ACK);

            $client->jetStream()->publishScheduled(
                'schedules.tick',
                'events.tick',
                'p',
                Schedule::cron('0 0 * * * *'),
                scheduleTtl: '5m',
                source: 'sensors.temperature',
                timeZone: 'Europe/Amsterdam',
                rollup: true,
            )->await();

            $frame = $transport->writes[3];
            self::assertStringContainsString('Nats-Schedule:0 0 * * * *', $frame, $version);
            self::assertStringContainsString('Nats-Schedule-TTL:5m', $frame, $version);
            self::assertStringContainsString('Nats-Schedule-Source:sensors.temperature', $frame, $version);
            self::assertStringContainsString('Nats-Schedule-Time-Zone:Europe/Amsterdam', $frame, $version);
            self::assertStringContainsString('Nats-Schedule-Rollup:sub', $frame, $version);
        }
    }

    /**
     * A recurring schedule that reaches a 2.12 server is refused by that server: its 400 (err_code 10189) is the
     * caller's JetStreamException, not an UnsupportedFeatureException of the client's making, and the schedule went
     * out once. A server's answer decides, not the client's reading of a version (#272).
     */
    public function testA212ServersRejectionOfARecurringScheduleIsTheServersOwnError(): void
    {
        [$client, $transport] = $this->connectedTo('2.12.15', self::PATTERN_INVALID);

        try {
            $client->jetStream()->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::every('5m'))->await();
            self::fail('expected the server\'s 400');
        } catch (JetStreamException $e) {
            self::assertNotInstanceOf(UnsupportedFeatureException::class, $e);
            self::assertSame('message schedules pattern is invalid', $e->getMessage());
            self::assertSame(400, $e->getCode());
            self::assertSame(10189, $e->getErrCode());
        }

        self::assertCount(1, array_filter($transport->writes, static fn(string $write): bool => str_starts_with($write, 'HPUB schedules.tick ')));
    }

    /**
     * A plain "@at" schedule - with a TTL, an empty source or rollup off - is published on a 2.12 server without the
     * 2.14 headers.
     */
    public function testPlainAtScheduleIsStillPublishedOnA212Server(): void
    {
        foreach (
            [
                static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT),
                static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::atTimestamp(1893456000), '5m'),
                static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: '', rollup: false),
                static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: null),
            ] as $index => $call
        ) {
            [$client, $transport] = $this->connectedTo('2.12.15', '{"stream":"SCHED","seq":3,"duplicate":false}');

            $ack = $call($client->jetStream())->await();

            self::assertSame(3, $ack->seq, 'call #' . $index);
            self::assertStringContainsString('Nats-Schedule:' . self::AT, $transport->writes[3], 'call #' . $index);
            self::assertStringNotContainsString('Nats-Schedule-Source', $transport->writes[3], 'call #' . $index);
            self::assertStringNotContainsString('Nats-Schedule-Rollup', $transport->writes[3], 'call #' . $index);
        }
    }

    /**
     * A time zone with an "@at" schedule is wrong on every server, so it is still refused before anything is written:
     * a check of the arguments, not of a version.
     */
    public function testTimeZoneOnAnAtScheduleStillFailsTheCronOnlyCheckOnA212Server(): void
    {
        [$client, $transport] = $this->connectedTo('2.12.15');

        try {
            $client->jetStream()->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, timeZone: 'Europe/Amsterdam')->await();
            self::fail('expected the cron-only time zone check to fail');
        } catch (JetStreamException $e) {
            self::assertNotInstanceOf(UnsupportedFeatureException::class, $e);
            self::assertSame('Nats-Schedule-Time-Zone is only valid for cron schedule expressions', $e->getMessage());
        }

        self::assertCount(2, $transport->writes);
    }

    /**
     * A malformed expression is still refused before anything is written, as malformed.
     */
    public function testMalformedScheduleIsStillReportedAsMalformedOnA212Server(): void
    {
        [$client, $transport] = $this->connectedTo('2.12.15');

        try {
            $client->jetStream()->publishScheduled('schedules.tick', 'events.tick', 'p', '@every')->await();
            self::fail('expected the malformed schedule to be rejected');
        } catch (JetStreamException $e) {
            self::assertNotInstanceOf(UnsupportedFeatureException::class, $e);
            self::assertStringStartsWith('Unsupported schedule expression "@every"', $e->getMessage());
        }

        self::assertCount(2, $transport->writes);
    }

    /**
     * A schedule subject with an empty token fails with the ProtocolException every request raises, before the reply
     * inbox is subscribed or anything is published.
     */
    public function testAnInvalidScheduleSubjectIsRefusedBeforeAnyWrite(): void
    {
        [$client, $transport] = $this->connectedTo('2.12.15', self::ACK);

        try {
            $client->jetStream()->publishScheduled('schedules..tick', 'events.tick', 'p', Schedule::every(2))->await();
            self::fail('expected the invalid subject to be refused');
        } catch (ProtocolException $e) {
            self::assertSame('Subject must not contain empty tokens', $e->getMessage());
        }

        self::assertCount(2, $transport->writes, 'nothing written after the CONNECT + PING handshake');
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function failovers(): iterable
    {
        yield 'from 2.14 to 2.12' => ['2.14.0', '2.12.0'];
        yield 'from 2.12 to 2.14' => ['2.12.0', '2.14.0'];
        yield 'from 2.11 to 2.10' => ['2.11.17', '2.10.29'];
    }

    /**
     * A call that needs 2.14, made while the client reconnects, waits for the new connection and is published there,
     * once, whatever version either session reports; nothing goes to the session being replaced. Before #272 a call
     * reconnecting to a server that reported 2.12 was refused there.
     *
     * @param \Closure(JetStreamContext): Future<PubAck> $call
     */
    #[DataProvider('callsNeeding214')]
    public function testACallWaitingForAReconnectIsPublishedOnTheNewConnectionWhateverItsVersion(\Closure $call): void
    {
        foreach (self::failovers() as $label => [$from, $to]) {
            [$client, $transport, $recovery] = $this->reconnectingFromTo($from, $to);

            $publish = $this->issuedWhileTheReconnectIsHeld($client, $transport, $call);
            $ack = $publish->await(new TimeoutCancellation(2));

            self::assertSame(5, $ack->seq, $label);
            $recovery->await(new TimeoutCancellation(2));
            self::assertSame(1, $transport->epoch(), $label . ': the client reconnected once');
            self::assertSame([], $transport->controlLinesStartingWith('HPUB schedules.tick', 0), $label . ': nothing went to the replaced session');
            self::assertCount(1, $transport->controlLinesStartingWith('HPUB schedules.tick', 1), $label . ': one schedule went to the new session');

            $client->disconnect()->await();
        }
    }

    /**
     * A schedule whose write fails on the session that reports 2.14, and whose recovery lands on one that reports
     * 2.12, is written there by the retry, once, and its acknowledgement returned. The retry is not judged by the new
     * session's version: before #272 it was refused there.
     */
    public function testARetryAfterAFailedWriteGoesToTheNewSessionWhateverItsVersion(): void
    {
        $transport = new ReconnectingTransport();
        $transport->infoByEpoch = [0 => ReconnectingTransport::infoWithVersion('2.14.0'), 1 => ReconnectingTransport::infoWithVersion('2.12.0')];
        $transport->responder = self::acking($transport);
        $client = $this->connectClient($transport);
        $transport->failNextWriteContaining('HPUB schedules.tick');

        $ack = $client->jetStream()->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: 'sensors.temperature')
            ->await(new TimeoutCancellation(2));

        self::assertSame(5, $ack->seq);
        self::assertSame(1, $transport->epoch(), 'the failed write reconnected once');
        self::assertCount(1, $transport->controlLinesStartingWith('HPUB schedules.tick', 1), 'the retry wrote the schedule on the new session, once');
    }

    /**
     * A client connected to a server advertising $from whose connection is lost, with the recovery's dial held: the
     * session it reconnects to advertises $to. Returns the client, the transport and the recovery's reader.
     *
     * @return array{NatsClient, ReconnectingTransport, Future<int>}
     */
    private function reconnectingFromTo(string $from, string $to): array
    {
        $transport = new ReconnectingTransport();
        $transport->infoByEpoch = [0 => ReconnectingTransport::infoWithVersion($from), 1 => ReconnectingTransport::infoWithVersion($to)];
        $transport->responder = self::acking($transport);
        $client = $this->connectClient($transport);
        self::assertSame($from, $client->serverInfo()?->version);
        $recovery = $this->startRecoveryHeldMidDial($client, $transport);

        return [$client, $transport, $recovery];
    }

    /**
     * Issues $call while the recovery's dial is held, gives it time to reach its wait for the new connection, then lets
     * the dial go.
     *
     * @param \Closure(JetStreamContext): Future<PubAck> $call
     * @return Future<PubAck>
     */
    private function issuedWhileTheReconnectIsHeld(NatsClient $client, ReconnectingTransport $transport, \Closure $call): Future
    {
        $publish = $call($client->jetStream());
        $publish->ignore();
        delay(0.05);
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the call was made while the client reconnected');
        self::assertFalse($publish->isComplete(), 'the call waits for the new connection');
        $transport->releaseDial();

        return $publish;
    }

    /** Answers every request with {@see ACK} on its reply subject. */
    private static function acking(ReconnectingTransport $transport): \Closure
    {
        return static fn(string $subject, ?string $replyTo): array => $replyTo === null ? [] : $transport->replyFrame($replyTo, self::ACK);
    }

    /**
     * A client whose server advertises $version in its INFO (no "version" field when null), and, when $ack is
     * given, answers the first request with it.
     *
     * @return array{NatsClient, FakeTransport}
     */
    private function connectedTo(?string $version, ?string $ack = null): array
    {
        $info = ['server_id' => 'S1', 'server_name' => 'n1', 'jetstream' => true, 'max_payload' => 1048576, 'headers' => true];
        if ($version !== null) {
            $info['version'] = $version;
        }

        $transport = new FakeTransport(['INFO ' . json_encode($info, JSON_THROW_ON_ERROR) . "\r\n", "PONG\r\n"]);
        if ($ack !== null) {
            $this->muxReplies($transport, [sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ack), $ack)]);
        }

        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport));
        $client->connect()->await();

        return [$client, $transport];
    }

    /**
     * Answers each request with the next frame on the request's own mux reply subject, as a server would (the
     * request inbox is muxed since #118: one wildcard "<base>.*" subscription, sid 1).
     *
     * @param list<string> $frames Pre-built MSG reply frames; only their subject and sid are rewritten.
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
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1);
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                return [];
            }

            $frame = array_shift($queue);
            $pos = strpos($frame, "\r\n");
            self::assertNotFalse($pos, 'reply frame must have a head line');
            $tokens = explode(' ', substr($frame, 0, $pos));
            $tokens[1] = $replyTo;
            $tokens[2] = '1';

            return [implode(' ', $tokens) . substr($frame, $pos)];
        };
    }
}
