<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\PermissionViolationException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * A request whose publish the server refuses for lack of permission fails at once. nats-server answers a PUB to a
 * subject the user may not publish to with "-ERR 'Permissions Violation for Publish to "<subject>"'" and keeps the
 * connection open; the request reaches no responder, so no reply ever comes. The client reported the -ERR to the error
 * listener and left the request waiting: it failed with a TimeoutException at the end of its timeout, 10 s by default.
 * A JetStream API call made by a user who may not use that API, such as a getConsumer() without the right to publish
 * to "$JS.API.CONSUMER.INFO.>", waited the same 10 s, and a JetStream publish refused that way looked like a publish
 * whose acknowledgement was late, which may still have been stored, where it was never stored.
 *
 * The -ERR names the subject, not the request, so every request in flight to that subject fails, with a
 * {@see PermissionViolationException} that names it. Permissions belong to the connection's user, so a request to the
 * same subject is refused as well, whichever publish to it the -ERR answers. A request to another subject is left
 * alone, and the connection stays open.
 *
 * Over `tests/Support/FakeTransport.php`, whose onWrite answers each PUB or HPUB the client writes: with the -ERR for
 * a refused subject, with a reply on the request inbox for any other. Each regression test fails on the code before
 * the fix, the request ending only at its 5 s timeout.
 */
final class RefusedRequestTest extends TestCase
{
    use OwnsTestResources;

    private const REFUSED = 'svc.secret';

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string}> */
    public static function requestOperations(): iterable
    {
        yield 'request()' => ['request'];
        yield 'requestWithHeaders()' => ['requestWithHeaders'];
        yield 'requestMany()' => ['requestMany'];
    }

    /**
     * The request fails within 2.5 s of its 5 s timeout with the server's refusal, which names the subject, and leaves
     * nothing behind for the request. The server's text, quoted or bare, is in the message.
     */
    #[DataProvider('requestOperations')]
    public function testARequestWhosePublishTheServerRefusesFailsAtOnce(string $operation): void
    {
        foreach (['quoted' => true, 'bare' => false] as $form => $quoted) {
            $transport = self::server([self::REFUSED], $quoted);
            $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 5_000), $transport));
            $connection->connect()->await();

            $start = hrtime(true);
            $failure = self::failureOf(self::call($operation, $connection, self::REFUSED));
            $elapsed = (hrtime(true) - $start) / 1e9;

            self::assertInstanceOf(PermissionViolationException::class, $failure, sprintf('%s, %s: %s', $operation, $form, self::describe($failure)));
            self::assertSame(self::REFUSED, $failure->getSubject());
            self::assertSame(
                'The server refused the request to "svc.secret": Permissions Violation for Publish to '
                . ($quoted ? '"svc.secret"' : 'svc.secret') . '. No responder received it. Grant this connection\'s user'
                . ' permission to publish to "svc.secret".',
                $failure->getMessage(),
                'the message carries the server\'s text, without its quotes, and names the permission needed',
            );
            self::assertLessThan(2.5, $elapsed, sprintf('%s, %s: failed after %.3f s, with a 5 s timeout', $operation, $form, $elapsed));
            self::assertSame(ConnectionState::Open, $connection->state(), 'a refused publish leaves the connection open');
            self::assertNoRequestStateLeft($connection);
        }
    }

    /** @return iterable<string, array{string, string}> */
    public static function subjectsGoQuotesWithACharacterOfItsQuoting(): iterable
    {
        yield 'an apostrophe, inside the single quotes of the -ERR' => ["svc.o'brien", 'svc.o'];
        yield 'a backslash, which the quoting doubles' => ['svc.a\\b', 'svc.a'];
        yield 'a double quote, which the quoting escapes' => ['svc.a"b', 'svc.a'];
    }

    /**
     * nats-server quotes the subject with Go's %q, which escapes a double quote and a backslash and leaves an apostrophe,
     * although the text of the -ERR is in single quotes. The refused request fails at once, and a request in flight to
     * the subject before that character gets its reply. Reading the subject up to the first quote, as before, refused
     * that other request for an apostrophe, and for each of them left the refused one to time out.
     */
    #[DataProvider('subjectsGoQuotesWithACharacterOfItsQuoting')]
    public function testASubjectTheServerQuotesIsReadAsItsSubject(string $refused, string $prefix): void
    {
        $transport = self::server([$refused]);
        $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $connection->connect()->await();

        $other = $connection->request($prefix, 'x');
        $other->ignore();
        $start = hrtime(true);
        $failure = self::failureOf($connection->request($refused, 'x'));

        self::assertInstanceOf(PermissionViolationException::class, $failure, self::describe($failure));
        self::assertSame($refused, $failure->getSubject());
        self::assertLessThan(2.5, (hrtime(true) - $start) / 1e9, 'the refused request failed well before its 5 s timeout');
        self::assertSame('ok', $other->await()->payload);
        self::assertNoRequestStateLeft($connection);
    }

    /**
     * A refusal that names another subject leaves the request alone: it gets its reply. A regression that refused
     * every request on any publish refusal would fail it.
     */
    public function testARefusalOfAnotherSubjectLeavesTheRequestAlone(): void
    {
        $transport = self::server([]);
        $inner = $transport->onWrite;
        self::assertNotNull($inner);
        $transport->onWrite = static function (string $bytes) use ($inner): array {
            $frames = $inner($bytes);
            if (str_starts_with($bytes, 'PUB svc.echo ')) {
                array_unshift($frames, "-ERR 'Permissions Violation for Publish to \"svc.echo.other\"'\r\n");
            }

            return $frames;
        };
        $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $connection->connect()->await();

        self::assertSame('ok', $connection->request('svc.echo', 'x')->await()->payload);
        self::assertNoRequestStateLeft($connection);
    }

    /**
     * Of two requests in flight, the one to the refused subject fails and the other gets the reply that comes after
     * the -ERR. A request made afterwards works: nothing was latched, and the connection stayed open.
     */
    public function testOnlyTheRequestToTheRefusedSubjectFails(): void
    {
        $transport = self::server([self::REFUSED]);
        $inner = $transport->onWrite;
        self::assertNotNull($inner);
        /** @var list<string> $held */
        $held = [];
        // The reply to "svc.slow" is held back until the refused request is written, and then comes after the -ERR.
        $transport->onWrite = static function (string $bytes) use ($inner, &$held): array {
            $frames = $inner($bytes);
            if (str_starts_with($bytes, 'PUB svc.slow ')) {
                $held = $frames;

                return [];
            }
            if (str_starts_with($bytes, 'PUB ' . self::REFUSED . ' ')) {
                return [...$frames, ...$held];
            }

            return $frames;
        };
        $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $connection->connect()->await();

        $slow = $connection->request('svc.slow', 'x');
        $slow->ignore();
        $refused = async(static fn(): NatsMessage => $connection->request(self::REFUSED, 'x')->await());
        $refused->ignore();

        self::assertInstanceOf(PermissionViolationException::class, self::failureOf($refused));
        self::assertSame('ok', $slow->await()->payload);
        self::assertSame('ok', $connection->request('svc.echo', 'y')->await()->payload);
        self::assertNoRequestStateLeft($connection);
    }

    /**
     * Every request in flight to the refused subject fails at the first -ERR that names it: permissions belong to the
     * user, so the other publish is refused as well. Here the server answers the first PUB with nothing and the second
     * with the one -ERR, written once both are on the wire, and both requests fail at once.
     */
    public function testEveryRequestInFlightToTheRefusedSubjectFails(): void
    {
        $transport = self::server([]);
        $refusals = 0;
        $inner = $transport->onWrite;
        self::assertNotNull($inner);
        $transport->onWrite = static function (string $bytes) use ($inner, &$refusals): array {
            if (str_starts_with($bytes, 'PUB ' . self::REFUSED . ' ')) {
                // Nothing for the first; both refused at the second.
                return ++$refusals === 2 ? ["-ERR 'Permissions Violation for Publish to \"" . self::REFUSED . "\"'\r\n"] : [];
            }

            return $inner($bytes);
        };
        $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $connection->connect()->await();

        $start = hrtime(true);
        $first = $connection->request(self::REFUSED, 'a');
        $first->ignore();
        $second = $connection->request(self::REFUSED, 'b');
        $second->ignore();

        self::assertInstanceOf(PermissionViolationException::class, self::failureOf($first));
        self::assertInstanceOf(PermissionViolationException::class, self::failureOf($second));
        self::assertLessThan(2.5, (hrtime(true) - $start) / 1e9, 'both failed well before their 5 s timeout');
        self::assertNoRequestStateLeft($connection);
    }

    /**
     * A requestMany() that has collected a reply returns it when a refusal of its subject arrives after the reply, as one
     * of a plain publish to the same subject would: the reply answered a publish the server let through. Before the
     * fix it waited out its timeout and returned the same reply.
     */
    public function testARequestManyWithRepliesCollectedReturnsThemWhenItsSubjectIsRefusedLater(): void
    {
        $transport = self::server([]);
        $inner = $transport->onWrite;
        self::assertNotNull($inner);
        $transport->onWrite = static function (string $bytes) use ($inner): array {
            $frames = $inner($bytes);
            if (str_starts_with($bytes, 'PUB svc.scatter ')) {
                $frames[] = "-ERR 'Permissions Violation for Publish to \"svc.scatter\"'\r\n";
            }

            return $frames;
        };
        $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $connection->connect()->await();

        $start = hrtime(true);
        $replies = $connection->requestMany('svc.scatter', 'x', null, 5, 5_000)->await();

        self::assertSame(['ok'], array_map(static fn(NatsMessage $message): string => $message->payload, $replies));
        self::assertLessThan(2.5, (hrtime(true) - $start) / 1e9, 'returned at the refusal, well before the 5 s timeout');
        self::assertNoRequestStateLeft($connection);
    }

    /** @return iterable<string, array{\Closure(JetStreamContext): Future<mixed>, string}> */
    public static function jetStreamCalls(): iterable
    {
        yield 'getConsumer' => [static fn(JetStreamContext $js): Future => $js->getConsumer('ORDERS', 'worker'), '$JS.API.CONSUMER.INFO.ORDERS.worker'];
        yield 'getStream' => [static fn(JetStreamContext $js): Future => $js->getStream('ORDERS'), '$JS.API.STREAM.INFO.ORDERS'];
        yield 'publish' => [static fn(JetStreamContext $js): Future => $js->publish('orders.created', 'x'), 'orders.created'];
    }

    /**
     * A JetStream API call or publish the server refuses fails at once with the refusal, not as a JetStreamException
     * and not as a timeout: no JetStream server got the request, so there is no JetStream answer to report, and a
     * refused publish was not stored.
     *
     * @param \Closure(JetStreamContext): Future<mixed> $call
     */
    #[DataProvider('jetStreamCalls')]
    public function testAJetStreamCallTheServerRefusesFailsAtOnceWithTheRefusal(\Closure $call, string $subject): void
    {
        $transport = self::server([$subject]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $start = hrtime(true);
        $failure = self::failureOf($call($client->jetStream()));

        self::assertInstanceOf(PermissionViolationException::class, $failure, self::describe($failure));
        self::assertSame($subject, $failure->getSubject());
        self::assertLessThan(2.5, (hrtime(true) - $start) / 1e9, 'the call failed well before its 5 s timeout');
    }

    /**
     * A server that answers every SUB of the request inbox silently, every PUB or HPUB to a subject in $refused with
     * the -ERR refusing it, quoted as nats-server quotes it or bare, and every other PUB or HPUB with "ok" on the inbox.
     *
     * @param list<string> $refused
     */
    private static function server(array $refused, bool $quoted = true): FakeTransport
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $sid = 1;
        $transport->onWrite = static function (string $bytes) use ($refused, $quoted, &$sid): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            $parts = explode(' ', $head);
            if ($parts[0] === 'SUB') {
                $sid = (int) ($parts[2] ?? 1);

                return [];
            }
            if ($parts[0] !== 'PUB' && $parts[0] !== 'HPUB') {
                return [];
            }

            $subject = $parts[1] ?? '';
            if (in_array($subject, $refused, true)) {
                return ["-ERR 'Permissions Violation for Publish to " . ($quoted ? self::goQuoted($subject) : $subject) . "'\r\n"];
            }

            $replyTo = $parts[2] ?? '';
            if ($subject === 'orders.created') {
                $ack = '{"stream":"ORDERS","seq":1}';

                return [sprintf("MSG %s %d %d\r\n%s\r\n", $replyTo, $sid, strlen($ack), $ack)];
            }

            return [sprintf("MSG %s %d 2\r\nok\r\n", $replyTo, $sid)];
        };

        return $transport;
    }

    /** The subject as nats-server writes it with Go's %q: in double quotes, a backslash before a quote or a backslash. */
    private static function goQuoted(string $subject): string
    {
        return '"' . addcslashes($subject, '"\\') . '"';
    }

    /** @return Future<mixed> */
    private static function call(string $operation, NatsConnection $connection, string $subject): Future
    {
        return match ($operation) {
            'request' => $connection->request($subject, 'x'),
            'requestWithHeaders' => $connection->requestWithHeaders($subject, 'x', ['X-Test' => '1']),
            default => $connection->requestMany($subject, 'x', null, 1, 5_000),
        };
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

    /** No waiter, wake-up, subject or refusal is left for a request that has ended. */
    private static function assertNoRequestStateLeft(NatsConnection $connection): void
    {
        foreach (['muxWaiters', 'muxWakes', 'muxSubjects', 'muxRefusals'] as $property) {
            $state = (new \ReflectionProperty(NatsConnection::class, $property))->getValue($connection);
            self::assertSame([], $state, $property . ' is empty once every request has ended');
        }
    }
}
