<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\PermissionViolationException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A pull whose publish the server refuses for lack of permission fails at once (#260). nats-server answers a PUB to
 * "$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>" by a user who may not publish there with
 * "-ERR 'Permissions Violation for Publish to "<subject>"'" and keeps the connection open: the pull reaches no
 * consumer, so no message and no status ever come for it. fetchBatch() publishes with an inbox of its own, not through
 * request(), so 2.30.0's refusal of requests did not reach it: it waited out its expiry plus a second and threw the
 * JetStreamException(408) of an empty pull, and a worker pulling in a loop stayed idle for ever without an error
 * (ideaconnect/symfony-nats-messenger#130). The connection now watches the pull's subject while the fetch runs
 * ({@see NatsConnection::watchPublishRefusal()}), and the fetch fails with a {@see PermissionViolationException} that
 * names the subject, which is not a JetStreamException, so a caller that reads a 408 as "nothing to do" cannot take it
 * for one. Messages delivered before the refusal are returned. A refusal read by another fiber's read is covered in
 * MuxInboxDropWakeupTest.
 *
 * Over `tests/Support/FakeTransport.php`, whose onWrite answers each PUB the client writes: with the messages a test
 * gives for the subject, delivered on the reply inbox, then with the -ERR for a refused subject. Each pull has a 5 s
 * expiry, so the fetch's budget is 6 s. Each regression test fails on the code before the fix, the fetch ending only
 * at that deadline: with the 408, or, with messages delivered, with them.
 */
final class RefusedPullTest extends TestCase
{
    use OwnsTestResources;

    private const STREAM = 'ORDERS';
    private const PULL = '$JS.API.CONSUMER.MSG.NEXT.ORDERS.worker';
    private const OTHER_PULL = '$JS.API.CONSUMER.MSG.NEXT.ORDERS.other';

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string}> */
    public static function fetches(): iterable
    {
        yield 'fetchBatch()' => ['fetchBatch'];
        yield 'fetchNext()' => ['fetchNext'];
    }

    /**
     * The issue's shape: the pull fails within 2.5 s of its 6 s budget with the server's refusal, which names the
     * subject and the permission needed, and leaves the connection open, the inbox unsubscribed and no watch behind.
     * It used to fail at the deadline with JetStreamException(408) 'No messages received within timeout'.
     */
    #[DataProvider('fetches')]
    public function testAPullWhosePublishTheServerRefusesFailsAtOnce(string $fetch): void
    {
        $transport = self::server([self::PULL]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $start = hrtime(true);
        $failure = self::failureOf(self::fetch($fetch, $client->jetStream(), 'worker'));
        $elapsed = (hrtime(true) - $start) / 1e9;

        self::assertInstanceOf(PermissionViolationException::class, $failure, sprintf('%s: %s', $fetch, self::describe($failure)));
        self::assertSame(self::PULL, $failure->getSubject());
        self::assertSame(
            'The server refused the pull to "' . self::PULL . '": Permissions Violation for Publish to "' . self::PULL
            . '". No message was delivered for it. Grant this connection\'s user permission to publish to "' . self::PULL . '".',
            $failure->getMessage(),
            'the message carries the server\'s text, without its quotes, and names the permission needed',
        );
        self::assertLessThan(2.5, $elapsed, sprintf('%s failed after %.3f s, with a 6 s budget', $fetch, $elapsed));
        self::assertSame(ConnectionState::Open, $client->state(), 'a refused pull leaves the connection open');
        self::assertCount(1, self::linesStartingWith($transport, 'UNSUB '), 'the fetch unsubscribed its inbox');
        self::assertNoWatchLeft($client);
    }

    /**
     * A refusal that names another consumer's pull, read before the pull's message, leaves the pull alone: it gets its
     * message. A regression that ended every pull in flight on any publish refusal would fail it with the refusal.
     */
    public function testARefusalOfAnotherSubjectLeavesThePullAlone(): void
    {
        $transport = self::server([], [self::PULL => ['first']], errorsBefore: [self::PULL => [self::OTHER_PULL]]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 5_000)->await();

        self::assertSame(['first'], self::payloads($messages));
        self::assertNoWatchLeft($client);
    }

    /**
     * Of two pulls in flight, the one to the refused subject fails at once, and the other gets the message that comes
     * after the -ERR. A pull made afterwards works: nothing was latched, and the connection stayed open.
     */
    public function testOnlyThePullToTheRefusedSubjectFails(): void
    {
        $transport = self::server([self::PULL], [self::OTHER_PULL => ['other']]);
        $inner = $transport->onWrite;
        self::assertNotNull($inner);
        /** @var list<string> $held */
        $held = [];
        $holding = true;
        // The other consumer's message is held back until the refused pull is written, and then comes after its -ERR.
        $transport->onWrite = static function (string $bytes) use ($inner, &$held, &$holding): array {
            $frames = $inner($bytes);
            if ($holding && str_contains($bytes, 'PUB ' . self::OTHER_PULL . ' ')) {
                $held = $frames;

                return [];
            }
            if ($holding && str_contains($bytes, 'PUB ' . self::PULL . ' ')) {
                $holding = false;

                return [...$frames, ...$held];
            }

            return $frames;
        };
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();
        $js = $client->jetStream();

        $start = hrtime(true);
        $other = $js->fetchBatch(self::STREAM, 'other', 1, 5_000);
        $other->ignore();
        $refused = $js->fetchBatch(self::STREAM, 'worker', 1, 5_000);
        $refused->ignore();

        $failure = self::failureOf($refused);
        self::assertInstanceOf(PermissionViolationException::class, $failure, self::describe($failure));
        self::assertSame(['other'], self::payloads($other->await()));
        self::assertLessThan(2.5, (hrtime(true) - $start) / 1e9, 'both ended well before their 6 s budget');
        self::assertSame(['other'], self::payloads($js->fetchBatch(self::STREAM, 'other', 1, 5_000)->await()));
        self::assertNoWatchLeft($client);
    }

    /**
     * A pull that holds messages when its publish is refused returns them at once, as at its deadline: they are real,
     * and the server counted them as delivered. Here the server delivers one message of a batch of 5 and then refuses
     * a publish to the subject. The fetch used to return the same message at its 6 s deadline.
     */
    public function testAPullWithMessagesReturnsThemAtOnceWhenItsPublishIsRefused(): void
    {
        $transport = self::server([], [self::PULL => ['first']], errorsAfter: [self::PULL => [self::PULL]]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $start = hrtime(true);
        $messages = $client->jetStream()->fetchBatch(self::STREAM, 'worker', 5, 5_000)->await();
        $elapsed = (hrtime(true) - $start) / 1e9;

        self::assertSame(['first'], self::payloads($messages));
        self::assertLessThan(2.5, $elapsed, sprintf('returned after %.3f s, with a 6 s budget', $elapsed));
        self::assertNoWatchLeft($client);
    }

    /**
     * A pull nothing refuses still ends as an empty pull at its deadline: the watch changes nothing for it. A guard: it
     * keeps the 408 a caller reads as "nothing to do" from turning into the refusal. On the code before the fix it
     * fails only at the check of the watch state, which does not exist there.
     */
    public function testAnUnansweredPullStillEndsAsAnEmptyPull(): void
    {
        $transport = self::server([]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $failure = self::failureOf($client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 200));

        self::assertInstanceOf(JetStreamException::class, $failure, self::describe($failure));
        self::assertSame(408, $failure->getCode());
        self::assertNoWatchLeft($client);
    }

    /**
     * A server that answers every SUB silently and every PUB with, in order: the -ERRs refusing the subjects
     * $errorsBefore gives for that subject, the messages $deliveries gives for it, delivered on its reply inbox, the
     * -ERRs refusing the subjects $errorsAfter gives for it, and the -ERR refusing it when it is in $refused, as
     * nats-server writes them.
     *
     * @param list<string> $refused
     * @param array<string, list<string>> $deliveries
     * @param array<string, list<string>> $errorsAfter
     * @param array<string, list<string>> $errorsBefore
     */
    private static function server(array $refused, array $deliveries = [], array $errorsAfter = [], array $errorsBefore = []): FakeTransport
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        /** @var array<string, int> $sids */
        $sids = [];
        $transport->onWrite = static function (string $bytes) use ($refused, $deliveries, $errorsAfter, $errorsBefore, &$sids): array {
            $frames = [];
            $lines = explode("\r\n", $bytes);
            for ($index = 0, $count = count($lines); $index < $count; $index++) {
                $parts = explode(' ', $lines[$index]);
                if ($parts[0] === 'SUB' && count($parts) >= 3) {
                    $sids[$parts[1]] = (int) $parts[count($parts) - 1];

                    continue;
                }
                if ($parts[0] !== 'PUB' || count($parts) < 3) {
                    continue;
                }

                // The payload line.
                $index++;
                $subject = $parts[1];
                $replyTo = count($parts) === 4 ? $parts[2] : null;
                foreach ($errorsBefore[$subject] ?? [] as $other) {
                    $frames[] = self::refusal($other);
                }
                if ($replyTo !== null && isset($sids[$replyTo])) {
                    foreach ($deliveries[$subject] ?? [] as $payload) {
                        $frames[] = sprintf("MSG %s %d %d\r\n%s\r\n", $replyTo, $sids[$replyTo], strlen($payload), $payload);
                    }
                }
                foreach ($errorsAfter[$subject] ?? [] as $other) {
                    $frames[] = self::refusal($other);
                }
                if (in_array($subject, $refused, true)) {
                    $frames[] = self::refusal($subject);
                }
            }

            return $frames;
        };

        return $transport;
    }

    private static function refusal(string $subject): string
    {
        return "-ERR 'Permissions Violation for Publish to \"" . $subject . "\"'\r\n";
    }

    /** @return Future<list<NatsMessage>> */
    private static function fetch(string $operation, JetStreamContext $js, string $consumer): Future
    {
        return $operation === 'fetchNext'
            ? $js->fetchNext(self::STREAM, $consumer, 5_000)->map(static fn(NatsMessage $message): array => [$message])
            : $js->fetchBatch(self::STREAM, $consumer, 1, 5_000);
    }

    /** @param Future<mixed> $future */
    private static function failureOf(Future $future): ?\Throwable
    {
        try {
            $future->await();
        } catch (\Throwable $e) {
            return $e;
        }

        return null;
    }

    private static function describe(?\Throwable $error): string
    {
        return $error === null ? 'no failure' : $error::class . ': ' . $error->getMessage();
    }

    /**
     * @param list<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_map(static fn(NatsMessage $message): string => $message->payload, $messages);
    }

    /** @return list<string> */
    private static function linesStartingWith(FakeTransport $transport, string $prefix): array
    {
        $lines = [];
        foreach ($transport->writes as $write) {
            foreach (explode("\r\n", $write) as $line) {
                if (str_starts_with($line, $prefix)) {
                    $lines[] = $line;
                }
            }
        }

        return $lines;
    }

    /** No watch and no refusal is left for a pull that has ended. */
    private static function assertNoWatchLeft(NatsClient $client): void
    {
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);
        foreach (['publishRefusalWatches', 'publishRefusals'] as $property) {
            $state = (new \ReflectionProperty(NatsConnection::class, $property))->getValue($connection);
            self::assertSame([], $state, $property . ' is empty once every pull has ended');
        }
    }
}
