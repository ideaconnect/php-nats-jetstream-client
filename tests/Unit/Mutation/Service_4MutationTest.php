<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Future\await;

/**
 * A cancellation whose isRequested() throws. run()'s loop calls
 * $effectiveCancellation?->isRequested() at the top of every iteration, OUTSIDE the inner
 * read try/catch, so the thrown error propagates straight out of the while loop - exactly the
 * escape path the run() finally exists to clean up after.
 */
final class Service_4ThrowingIsRequestedCancellation implements Cancellation
{
    public function subscribe(\Closure $callback): string
    {
        return 'noop';
    }

    public function unsubscribe(string $id): void
    {
    }

    public function isRequested(): bool
    {
        throw new \RuntimeException('mut13-boom-isRequested');
    }

    public function throwIfRequested(): void
    {
    }
}

/**
 * Feeds INFO+PONG for connect(), then on the FIRST run-loop read throws a bare CancelledException
 * (a bounded socket read whose deadline fired; {@see NatsConnection::readIncoming()} re-throws it
 * verbatim). A second read would hand back a real endpoint request frame, and any later read is EOF
 * so a loop that failed to break still terminates (Closed) instead of hanging the test.
 */
final class Service_4CancelThenServeTransport implements TlsAwareTransportInterface
{
    /** @var list<string> */
    public array $writes = [];

    /** Number of readLine() calls made AFTER the connect handshake (INFO+PONG) was consumed. */
    public int $runLoopReads = 0;

    /** @var list<string> */
    private array $handshake;

    public function __construct()
    {
        $this->handshake = [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(static function (): void {});
    }

    public function upgradeTls(): Future
    {
        return async(static function (): void {});
    }

    public function tlsActive(): bool
    {
        return false;
    }

    public function write(string $bytes): Future
    {
        return async(function () use ($bytes): void {
            $this->writes[] = $bytes;
        });
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function (): string {
            if ($this->handshake !== []) {
                return (string) array_shift($this->handshake);
            }

            $this->runLoopReads++;

            if ($this->runLoopReads === 1) {
                // The read the run loop MUST treat as a clean cancellation -> break.
                throw new CancelledException();
            }

            if ($this->runLoopReads === 2) {
                // A live request. Only a loop that did NOT break on the cancellation reaches this.
                return "MSG svc.echo 13 _INBOX.mut14 5\r\nhello\r\n";
            }

            // EOF: with reconnect disabled the connection goes Closed, so even a non-breaking loop
            // terminates rather than hanging the test.
            throw new TransportClosedException('Socket closed by peer (EOF)');
        });
    }

    public function close(): Future
    {
        return async(static function (): void {});
    }
}

final class Service_4MutationTest extends TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    /**
     * kills UnwrapFinally @ line 471.
     *
     * The run() loop is wrapped in try { while (...) } finally { $this->stop()->await(); }. The
     * loop-top isRequested() check is OUTSIDE the inner read try/catch, so an error thrown there
     * escapes the whole while loop. The finally guarantees the service is still torn down
     * (unsubscribed, started=false) before that error propagates out of run().
     *
     * The mutant replaces the finally with a plain trailing $this->stop()->await(): when the loop
     * throws, that trailing statement is skipped, so stop() never runs - no UNSUB is sent and the
     * service stays marked started. Asserting the teardown happened despite the escaping error is
     * what pins the finally.
     */
    public function testFinallyStopsServiceWhenLoopConditionThrows(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);

        $caught = null;
        try {
            // isRequested() throws on the very first loop iteration -> escapes the while loop.
            $service->run(cancellation: new Service_4ThrowingIsRequestedCancellation())->await();
            self::fail('run() must propagate the error thrown out of the run loop');
        } catch (\RuntimeException $e) {
            $caught = $e;
        }

        self::assertSame('mut13-boom-isRequested', $caught->getMessage());

        // The finally ran stop(): the discovery + endpoint subscriptions were unsubscribed.
        $writes = implode('', $transport->writes);
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
        self::assertStringContainsString("UNSUB 13\r\n", $writes);

        // ...and stop() cleared the runtime state, so a retried start() is not silently a no-op.
        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
    }

    /**
     * kills CatchBlockRemoval @ line 477 (the outer `catch (CancelledException) { break; }`).
     *
     * readIncoming() re-throws a socket-read CancelledException verbatim. In the real loop the
     * dedicated `catch (CancelledException) { break; }` stops the service immediately on the FIRST
     * such read. The mutant removes that catch, so the CancelledException is caught by the generic
     * `catch (\Throwable)` instead: the connection is still Open (not Closed), so it falls through to
     * delay(0.02, cancellation: null) - which, with a null run cancellation, does NOT re-throw - and
     * the loop keeps going, reading and SERVING the next request that should never have been handled.
     *
     * Pinned observably: exactly ONE run-loop read happens (the loop broke on it) and the queued
     * request is never answered. On the mutant the loop reads again, so runLoopReads climbs and the
     * request gets a reply.
     */
    public function testCancelledReadBreaksLoopImmediatelyWithoutServingMore(): void
    {
        $transport = new Service_4CancelThenServeTransport();
        // reconnect disabled + no heartbeat: the mutant's later EOF read reaches Closed and stops,
        // so the test still terminates instead of hanging.
        $client = new NatsClient(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'DELIVERED');

        // Outer bound: both real and mutant complete; fail loudly rather than hang on a regression.
        $result = await(
            [async(static function () use ($service): void {
                $service->run()->await();
            })],
            new TimeoutCancellation(2.0),
        );
        self::assertSame([null], $result);

        // Real code: the first CancelledException read broke the loop -> exactly one run-loop read,
        // and the request behind it was never read, so no reply was published.
        self::assertSame(1, $transport->runLoopReads);

        $writes = implode('', $transport->writes);
        self::assertStringNotContainsString('DELIVERED', $writes);
        self::assertStringNotContainsString('_INBOX.mut14', $writes);
    }
}
