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
 * publishScheduled() and the server version (#267). nats-server 2.12 accepts only "@at" schedules: it
 * rejects "@every", cron and the predefined aliases with an opaque 400 ("message schedules pattern is
 * invalid"), and stores an "@at" schedule while ignoring Nats-Schedule-Source and Nats-Schedule-Rollup.
 * Those arrived in 2.14.0, with Nats-Schedule-Time-Zone. A server advertising a parseable version
 * older than 2.14 therefore gets an UnsupportedFeatureException before anything is written; 2.14+ and
 * an unparseable version get the publish as before.
 *
 * The version is the one of the server the schedule is written to: a call made while a reconnect is under way, or
 * whose first write fails and is retried after the recovery, is checked against the INFO of the session that takes
 * the frame, not the one it replaces (over tests/Support/ReconnectingTransport.php, whose sessions greet with the
 * versions under test).
 */
final class ScheduledPublishServerVersionTest extends TestCase
{
    use ReconnectScenarios;

    private const AT = '@at 2030-01-01T00:00:00Z';

    private const ACK = '{"stream":"SCHED","seq":5,"duplicate":false}';

    protected function tearDown(): void
    {
        // Every client and connection a test makes is registered as it is constructed (#183), weakly: the shutdown
        // closes what is left and checks that nothing goes on running into the next test.
        $this->closeOpenedConnections();
    }

    /**
     * The calls that need a 2.14 server, each with the feature the refusal must name.
     *
     * @return iterable<string, array{\Closure(JetStreamContext): Future<PubAck>, string}>
     */
    public static function callsNeeding214(): iterable
    {
        yield 'an @every schedule' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::every(2)),
            'schedule_every',
        ];
        yield 'a cron schedule' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::cron('0 0 * * * *')),
            'schedule_cron',
        ];
        yield 'a cron schedule with a time zone' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::cron('0 0 * * * *'), timeZone: 'Europe/Amsterdam'),
            'schedule_cron',
        ];
        yield 'a predefined alias' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::predefined('hourly')),
            'schedule_cron',
        ];
        yield 'an @at schedule with a source' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: 'sensors.temperature'),
            'schedule_source',
        ];
        yield 'an @at schedule with rollup' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, rollup: true),
            'schedule_rollup',
        ];
        yield 'an @at schedule with a source and rollup' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: 'sensors.temperature', rollup: true),
            'schedule_source',
        ];
        yield 'an @every schedule with a source' => [
            static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', Schedule::every('1m'), source: 'sensors.temperature'),
            'schedule_every',
        ];
    }

    /**
     * On a server advertising 2.12.15 (or anything parseable below 2.14), every call that needs 2.14 fails with
     * an UnsupportedFeatureException naming the feature, 2.14 and the server's version, and nothing is written
     * after the CONNECT + PING handshake.
     *
     * @param \Closure(JetStreamContext): Future<PubAck> $call
     */
    #[DataProvider('callsNeeding214')]
    public function testCallsNeeding214AreRefusedBeforeAnyWriteOnAnOlderServer(\Closure $call, string $feature): void
    {
        foreach (['2.12.15', '2.12.0', 'v2.12.0', '2.11.4', '1.99.0', '2.13.9', '2'] as $version) {
            [$client, $transport] = $this->connectedTo($version);

            try {
                $call($client->jetStream())->await();
                self::fail(sprintf('expected an UnsupportedFeatureException on %s', $version));
            } catch (UnsupportedFeatureException $e) {
                self::assertSame($feature, $e->feature, $version);
                self::assertSame('2.14', $e->requiredVersion, $version);
                self::assertSame($version, $e->serverVersion);
                self::assertSame(0, $e->getCode(), $version);
                self::assertNull($e->getErrCode(), $version);
                self::assertStringEndsWith(sprintf('(connected server %s; nothing was published)', $version), $e->getMessage());
            }

            self::assertCount(2, $transport->writes, sprintf('nothing may be written on %s', $version));
        }
    }

    /**
     * Every call is published unchanged when the server advertises 2.14 or later (pre-release tags count as
     * their version), and when the version does not parse (a proxy, a custom build, none at all): then the
     * server decides, as before.
     *
     * @param \Closure(JetStreamContext): Future<PubAck> $call
     */
    #[DataProvider('callsNeeding214')]
    public function testCallsNeeding214ArePublishedOnA214ServerOrAnUnparseableVersion(\Closure $call): void
    {
        foreach (['2.14.0', '2.14.0-beta.1', 'v2.14.10', '2.15.3', '3.0.0', 'custom-build', 'release-2.12.0', null] as $version) {
            [$client, $transport] = $this->connectedTo($version, '{"stream":"SCHED","seq":5,"duplicate":false}');

            $ack = $call($client->jetStream())->await();

            self::assertSame('SCHED', $ack->stream, (string) $version);
            self::assertSame(5, $ack->seq, (string) $version);
            self::assertStringStartsWith('HPUB schedules.tick _INBOX.', $transport->writes[3], (string) $version);
            self::assertStringContainsString('Nats-Schedule-Target:events.tick', $transport->writes[3], (string) $version);
        }
    }

    /**
     * The headers of each call reach a 2.14 server as they did before the version check.
     */
    public function testA214ServerGetsTheSameHeadersAsBefore(): void
    {
        [$client, $transport] = $this->connectedTo('2.14.0', '{"stream":"SCHED","seq":5,"duplicate":false}');

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
        self::assertStringContainsString('Nats-Schedule:0 0 * * * *', $frame);
        self::assertStringContainsString('Nats-Schedule-TTL:5m', $frame);
        self::assertStringContainsString('Nats-Schedule-Source:sensors.temperature', $frame);
        self::assertStringContainsString('Nats-Schedule-Time-Zone:Europe/Amsterdam', $frame);
        self::assertStringContainsString('Nats-Schedule-Rollup:sub', $frame);
    }

    /**
     * A plain "@at" schedule - with a TTL, an empty source or rollup off - needs only 2.12 and is still published
     * there.
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
     * A time zone with an "@at" schedule is wrong on every server, so a 2.12 server still gets the cron-only
     * error rather than a request to upgrade.
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
     * A malformed expression is reported as such on a 2.12 server, not as a version problem.
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
     * The exact refusal messages: the schedule itself for a recurring pattern, the header and the argument for a
     * header, the required version, what older servers do with it, and the connected server's version.
     */
    public function testRefusalMessagesAreExact(): void
    {
        $cases = [
            [
                static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', '  @every 2s  '),
                'The schedule "@every 2s" requires NATS server 2.14+; older servers accept only "@at" schedules'
                    . ' (connected server 2.12.15; nothing was published)',
            ],
            [
                static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', '@daily'),
                'The schedule "@daily" requires NATS server 2.14+; older servers accept only "@at" schedules'
                    . ' (connected server 2.12.15; nothing was published)',
            ],
            [
                static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: 'sensors.temperature'),
                'Nats-Schedule-Source (the source argument) requires NATS server 2.14+; older servers ignore it'
                    . ' (connected server 2.12.15; nothing was published)',
            ],
            [
                static fn(JetStreamContext $js): Future => $js->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, rollup: true),
                'Nats-Schedule-Rollup (the rollup argument) requires NATS server 2.14+; older servers ignore it'
                    . ' (connected server 2.12.15; nothing was published)',
            ],
        ];

        foreach ($cases as [$call, $message]) {
            [$client] = $this->connectedTo('2.12.15');

            try {
                $call($client->jetStream())->await();
                self::fail('expected an UnsupportedFeatureException: ' . $message);
            } catch (UnsupportedFeatureException $e) {
                self::assertSame($message, $e->getMessage());
            }
        }
    }

    /**
     * A call that needs 2.14 goes through the guarded request, which checks its subject as every request does: a
     * schedule subject with an empty token fails with the ProtocolException before the reply inbox is subscribed or
     * anything is published, on a 2.14 server that would take the schedule.
     */
    public function testAnInvalidScheduleSubjectIsRefusedBeforeAnyWriteOnA214Server(): void
    {
        [$client, $transport] = $this->connectedTo('2.14.0', self::ACK);

        try {
            $client->jetStream()->publishScheduled('schedules..tick', 'events.tick', 'p', Schedule::every(2))->await();
            self::fail('expected the invalid subject to be refused');
        } catch (ProtocolException $e) {
            self::assertSame('Subject must not contain empty tokens', $e->getMessage());
        }

        self::assertCount(2, $transport->writes, 'nothing written after the CONNECT + PING handshake');
    }

    /**
     * A call made while the client reconnects from a 2.14 server to a 2.12 one waits for the new connection and is
     * refused there, by the 2.12 INFO of the session that would take the frame: no schedule reaches either server.
     * The check used to read the INFO of the connection being replaced, so the 2.12 server got the schedule and
     * could acknowledge it without its source or rollup.
     *
     * @param \Closure(JetStreamContext): Future<PubAck> $call
     */
    #[DataProvider('callsNeeding214')]
    public function testACallWaitingForAReconnectToAnOlderServerIsRefusedThere(\Closure $call, string $feature): void
    {
        [$client, $transport, $recovery] = $this->reconnectingFromTo('2.14.0', '2.12.0');

        $publish = $this->issuedWhileTheReconnectIsHeld($client, $transport, $call);

        try {
            $publish->await(new TimeoutCancellation(2));
            self::fail('expected an UnsupportedFeatureException from the 2.12 session');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame($feature, $e->feature);
            self::assertSame('2.14', $e->requiredVersion);
            self::assertSame('2.12.0', $e->serverVersion);
            self::assertStringEndsWith('(connected server 2.12.0; nothing was published)', $e->getMessage());
        }

        $recovery->await(new TimeoutCancellation(2));
        self::assertSame(1, $transport->epoch(), 'the client reconnected once');
        self::assertSame([], $transport->controlLinesStartingWith('HPUB schedules.tick'), 'no schedule reached either server');
    }

    /**
     * A call made while the client reconnects from a 2.12 server to a 2.14 one is not refused by the 2.12 INFO the
     * connection being replaced left behind: it waits for the new connection and is published there, once.
     *
     * @param \Closure(JetStreamContext): Future<PubAck> $call
     */
    #[DataProvider('callsNeeding214')]
    public function testACallWaitingForAReconnectToANewerServerIsPublishedThere(\Closure $call): void
    {
        [$client, $transport, $recovery] = $this->reconnectingFromTo('2.12.0', '2.14.0');

        $publish = $this->issuedWhileTheReconnectIsHeld($client, $transport, $call);
        $ack = $publish->await(new TimeoutCancellation(2));

        self::assertSame(5, $ack->seq);
        $recovery->await(new TimeoutCancellation(2));
        self::assertSame([], $transport->controlLinesStartingWith('HPUB schedules.tick', 0), 'nothing went to the 2.12 server');
        self::assertCount(1, $transport->controlLinesStartingWith('HPUB schedules.tick', 1), 'one schedule went to the 2.14 server');
    }

    /**
     * The check runs again before the retry that follows a failed write: a schedule whose write fails on the 2.14
     * server it was checked against, and whose recovery lands on a 2.12 server, is refused there, before the retry
     * writes it. The retry used to send it to the 2.12 server.
     */
    public function testARetryAfterAFailedWriteIsCheckedAgainstTheServerItWouldGoTo(): void
    {
        $transport = new ReconnectingTransport();
        $transport->infoByEpoch = [0 => ReconnectingTransport::infoWithVersion('2.14.0'), 1 => ReconnectingTransport::infoWithVersion('2.12.0')];
        $transport->responder = self::acking($transport);
        $client = $this->connectClient($transport);
        $transport->failNextWriteContaining('HPUB schedules.tick');

        try {
            $client->jetStream()->publishScheduled('schedules.tick', 'events.tick', 'p', self::AT, source: 'sensors.temperature')
                ->await(new TimeoutCancellation(2));
            self::fail('expected an UnsupportedFeatureException from the 2.12 session');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('schedule_source', $e->feature);
            self::assertSame('2.12.0', $e->serverVersion);
        }

        self::assertSame(1, $transport->epoch(), 'the failed write reconnected once');
        self::assertSame([], $transport->controlLinesStartingWith('HPUB schedules.tick'), 'the retry wrote nothing');
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
