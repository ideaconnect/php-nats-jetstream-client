<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Core\NatsClient;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * What one unit test owns on the event loop, and its shutdown when the test ends (#183): the clients and connections it
 * made, the background operations it started together with what ends each, the fixture gates it holds (a held dial, a
 * stalled write, a held handler), and the loop callbacks it registered. A test registers each as it makes it, before
 * anything that can throw, usually through {@see OwnsTestResources}; its tearDown() calls {@see close()}, which runs
 * after a failed assertion as well as after a passing test.
 *
 * Connections are held weakly: a test that drops its last reference to one (a destructor or GC test) still sees it
 * collected, and a connection nothing else holds needs no shutdown. A client is registered through the connection inside
 * it: a test's client goes when its method returns, before tearDown(), while an Open connection lives on in a cycle of
 * its own closures until the collector runs. Nothing the scope keeps captures a connection strongly; while
 * {@see close()} runs it holds each one it found alive, until its last check.
 *
 * {@see close()} runs under one absolute deadline, a referenced timer, so that it ends even when nothing else keeps the
 * event loop running; no step starts a fresh timeout. In order:
 *
 * 1. Stop what starts new work: the registered loop callbacks are cancelled, and the stop hooks run (inline; they must
 *    not suspend: a fixture flag, an iterator's stop(), a listener disabled).
 * 2. Start every connection's disconnect() at once, and wait, per connection, until its close intent has actually run
 *    (isDiscardingUndelivered(), or Closed): disconnect() only queues its body, and a gate released before the close
 *    intent could let a handler or a dial carry on as if nothing closed.
 * 3. Once every close intent has run: the release hooks, each in a fiber of its own (a held dial, a held handler, a
 *    stalled write, a greeting held back), and the end controls of the owned operations (their own cancellation
 *    source, an iterator's stop). A held uncancellable dial is released before its disconnect is joined: the
 *    disconnect waits for it.
 * 4. Join each disconnect, concurrently: one that sticks does not hold the others, and each connection's after-close
 *    hooks (a watch's or a service's local cleanup, which must not touch the wire of an open client) run as soon as
 *    its own close is done.
 * 5. Join the owned operations. Their outcome is the test's business: an operation that failed is settled, and only
 *    one that is still pending at the deadline is reported.
 * 6. Check: every connection Closed, every owned operation settled, every idle check (a fixture's held I/O) true.
 *
 * What a cleanup step throws is recorded with its label and the shutdown goes on; at the end everything recorded, and
 * everything still outstanding, is thrown as one {@see TestResourceCleanupFailure}. PHPUnit keeps a test's own failure
 * when its tearDown() throws as well, and reports the cleanup failure of a test that passed. A fiber suspended for good
 * cannot be killed: a hold without a release path is reported at the deadline, not hidden, and the next test still
 * finds it on the loop.
 */
final class TestResourceScope
{
    /** @var \WeakMap<NatsConnection, array{label: string, afterClose: list<array{hook: \Closure(): void, label: string}>}> */
    private \WeakMap $connections;

    /** @var list<array{future: Future<mixed>, end: (\Closure(): void)|null, label: string}> */
    private array $operations = [];

    /** @var list<array{hook: \Closure(): void, label: string}> */
    private array $stopHooks = [];

    /** @var list<array{hook: \Closure(): void, label: string}> */
    private array $releaseHooks = [];

    /** @var list<array{check: \Closure(): bool, label: string}> */
    private array $idleChecks = [];

    /** @var array<string, string> Loop callback id => label. */
    private array $callbacks = [];

    private bool $closed = false;

    /** @var array<string, true> The loop callbacks registered when the scope was made. */
    private array $callbacksBefore = [];

    /**
     * @param float $budgetSeconds The whole shutdown's budget. It has to cover the longest close leg left unreleased:
     *        a disconnect() waits up to the connection's connectTimeoutMs for a dial it stopped (5 s by default).
     */
    public function __construct(private readonly float $budgetSeconds = 6.0)
    {
        $this->connections = new \WeakMap();
        $this->callbacksBefore = array_fill_keys(EventLoop::getIdentifiers(), true);
    }

    /**
     * The loop callbacks registered since the scope was made and still registered, by id: what the test itself
     * registered, when the scope is made before the test starts (in setUp()) and nothing an earlier test left runs.
     *
     * @return list<string>
     */
    public function callbacksRegisteredSince(): array
    {
        return array_values(array_filter(
            EventLoop::getIdentifiers(),
            fn(string $id): bool => !isset($this->callbacksBefore[$id]),
        ));
    }

    /**
     * Registers a client or connection, weakly, and returns it. Register it right after constructing it, before
     * connect() or anything else that can throw. Registering one twice keeps one registration.
     *
     * @template T of NatsConnection|NatsClient
     *
     * @param T $connection
     * @return T
     */
    public function own(NatsConnection|NatsClient $connection, ?string $label = null): NatsConnection|NatsClient
    {
        $this->assertOpen();
        $owned = self::connectionOf($connection);
        if (!isset($this->connections[$owned])) {
            $this->connections[$owned] = [
                'label' => $label ?? (new \ReflectionClass($connection))->getShortName() . '#' . spl_object_id($connection),
                'afterClose' => [],
            ];
        }

        return $connection;
    }

    /** Whether $connection is registered (and still alive). */
    public function owns(NatsConnection|NatsClient $connection): bool
    {
        return isset($this->connections[self::connectionOf($connection)]);
    }

    /**
     * Registers a background operation the test started, with what actually ends it ($end: its own cancellation
     * source, an iterator's stop(), ...), run once every close intent has run. Register the operation's own future, not
     * a wrapper awaiting it with a cancellation: the wrapper can finish first. The future is held strongly until the
     * shutdown ends; its result, or what it throws, is left to the test.
     *
     * @template T
     *
     * @param Future<T> $future
     * @param (\Closure(): void)|null $end
     * @return Future<T>
     */
    public function ownOperation(Future $future, ?\Closure $end = null, string $label = 'operation'): Future
    {
        $this->assertOpen();
        $future->ignore();
        $this->operations[] = ['future' => $future, 'end' => $end, 'label' => $label];

        return $future;
    }

    /**
     * A hook that stops a source of new work, run first and inline: it must not suspend.
     *
     * @param \Closure(): void $hook
     */
    public function onStop(\Closure $hook, string $label): void
    {
        $this->assertOpen();
        $this->stopHooks[] = ['hook' => $hook, 'label' => $label];
    }

    /**
     * A hook that releases a fixture gate (a held dial, a stalled write, a held handler, a greeting held back), run in a
     * fiber of its own once every connection's close intent has run, before the disconnects are joined. Register it when
     * arranging the hold, before the operation that meets it starts.
     *
     * @param \Closure(): void $hook
     */
    public function onRelease(\Closure $hook, string $label): void
    {
        $this->assertOpen();
        $this->releaseHooks[] = ['hook' => $hook, 'label' => $label];
    }

    /**
     * A hook run as soon as $connection's own close is done: a watch's or a service's local cleanup, which must not
     * start wire I/O on an open client. It must not capture $connection itself: the registration is weak.
     *
     * @param \Closure(): void $hook
     */
    public function afterClose(NatsConnection|NatsClient $connection, \Closure $hook, string $label): void
    {
        $this->assertOpen();
        $this->own($connection);
        $owned = self::connectionOf($connection);
        $entry = $this->connections[$owned];
        $this->connections[$owned] = [
            'label' => $entry['label'],
            'afterClose' => [...$entry['afterClose'], ['hook' => $hook, 'label' => $label]],
        ];
    }

    /** A loop callback the test registered: cancelled first, as a source of new work. */
    public function ownCallback(string $callbackId, string $label = 'callback'): string
    {
        $this->assertOpen();
        $this->callbacks[$callbackId] = $label;

        return $callbackId;
    }

    /**
     * A check that a fixture's I/O has ended (no stalled write, no read under way, no dial held), made last; a check
     * that is still false at the deadline is reported.
     *
     * @param \Closure(): bool $isIdle
     */
    public function expectIdle(\Closure $isIdle, string $label): void
    {
        $this->assertOpen();
        $this->idleChecks[] = ['check' => $isIdle, 'label' => $label];
    }

    /**
     * Shuts everything down (see the class docblock) and throws a {@see TestResourceCleanupFailure} listing what a step
     * threw and what is still outstanding at the end. Runs once; later calls do nothing.
     */
    public function close(?string $owner = null): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        /** @var \ArrayObject<int, string> $errors */
        $errors = new \ArrayObject();
        $deadline = new DeferredCancellation();
        // Referenced, unlike a TimeoutCancellation's timer: the shutdown has to end even when nothing else keeps the
        // event loop running.
        $timer = EventLoop::delay($this->budgetSeconds, static function () use ($deadline): void {
            $deadline->cancel();
        });
        $budget = $deadline->getCancellation();

        /** @var list<array{NatsConnection, string, list<array{hook: \Closure(): void, label: string}>}> $connections */
        $connections = [];
        foreach ($this->connections as $connection => $entry) {
            $connections[] = [$connection, $entry['label'], $entry['afterClose']];
        }

        $outstanding = [];

        try {
            // 1. What starts new work.
            foreach ($this->callbacks as $id => $label) {
                EventLoop::cancel($id);
            }
            foreach ($this->stopHooks as ['hook' => $hook, 'label' => $label]) {
                self::attempt($hook, 'stop ' . $label, $errors);
            }

            // 2. Every disconnect at once, and each close intent.
            /** @var list<Future<void>> $intents */
            $intents = [];
            /** @var list<Future<void>> $shutdowns */
            $shutdowns = [];
            foreach ($connections as [$connection, $label, $afterClose]) {
                try {
                    $close = $connection->disconnect();
                } catch (\Throwable $e) {
                    $errors[] = sprintf('%s: disconnect() threw %s', $label, self::describe($e));

                    continue;
                }
                $close->ignore();

                $intent = async(static function () use ($connection, $budget): void {
                    while (!$connection->isDiscardingUndelivered() && $connection->state() !== ConnectionState::Closed) {
                        delay(0.001, cancellation: $budget);
                    }
                });
                $intent->ignore();
                $intents[] = $intent;

                // 4. Its close joined, then its own after-close hooks, whatever the others do.
                $shutdowns[] = async(static function () use ($intent, $close, $label, $afterClose, $budget, $errors): void {
                    try {
                        $intent->await($budget);
                    } catch (CancelledException) {
                        $errors[] = sprintf('%s: its close intent never ran', $label);

                        return;
                    }

                    try {
                        $close->await($budget);
                    } catch (CancelledException) {
                        $errors[] = sprintf('%s: disconnect() did not complete in time', $label);

                        return;
                    } catch (\Throwable $e) {
                        $errors[] = sprintf('%s: disconnect() failed with %s', $label, self::describe($e));
                    }

                    foreach ($afterClose as ['hook' => $hook, 'label' => $hookLabel]) {
                        self::attempt($hook, $label . ' after close: ' . $hookLabel, $errors);
                    }
                });
            }

            // 3. Once every close intent has run: the gates, and what ends each owned operation.
            $releases = async(function () use ($intents, $budget, $errors): void {
                foreach ($intents as $intent) {
                    try {
                        $intent->await($budget);
                    } catch (\Throwable) {
                        // Reported by its connection's shutdown; the gates are released all the same.
                    }
                }

                $released = [];
                foreach ($this->releaseHooks as ['hook' => $hook, 'label' => $label]) {
                    $released[] = [async(static fn() => $hook()), 'release ' . $label];
                }
                foreach ($this->operations as ['end' => $end, 'label' => $label]) {
                    if ($end !== null) {
                        $released[] = [async(static fn() => $end()), 'end ' . $label];
                    }
                }
                foreach ($released as [$future, $label]) {
                    try {
                        $future->await($budget);
                    } catch (CancelledException) {
                        $errors[] = sprintf('%s did not return in time', $label);
                    } catch (\Throwable $e) {
                        $errors[] = sprintf('%s threw %s', $label, self::describe($e));
                    }
                }
            });

            foreach ([...$shutdowns, $releases] as $step) {
                try {
                    $step->await($budget);
                } catch (\Throwable) {
                    // The deadline: whatever has not settled is listed below.
                }
            }

            // 5. The owned operations, whatever their outcome.
            foreach ($this->operations as ['future' => $future]) {
                try {
                    $future->await($budget);
                } catch (\Throwable) {
                    // Its result is the test's; a pending one is listed below.
                }
            }

            // 6. The checks, after every step; references kept until here.
            foreach ($connections as [$connection, $label]) {
                $state = $connection->state();
                if ($state !== ConnectionState::Closed) {
                    $outstanding[] = sprintf('%s is still %s', $label, $state->value);
                }
            }
            foreach ($this->operations as ['future' => $future, 'label' => $label]) {
                if (!$future->isComplete()) {
                    $outstanding[] = sprintf('%s is still pending', $label);
                }
            }
            foreach ($this->idleChecks as ['check' => $check, 'label' => $label]) {
                try {
                    if (!$check()) {
                        $outstanding[] = sprintf('%s is not idle', $label);
                    }
                } catch (\Throwable $e) {
                    $errors[] = sprintf('idle check %s threw %s', $label, self::describe($e));
                }
            }
        } finally {
            EventLoop::cancel($timer);
            $this->operations = [];
            $this->stopHooks = [];
            $this->releaseHooks = [];
            $this->idleChecks = [];
            $this->callbacks = [];
            $this->connections = new \WeakMap();
        }

        $problems = [...$errors->getArrayCopy(), ...$outstanding];
        if ($problems !== []) {
            throw new TestResourceCleanupFailure(sprintf(
                "Test resources of %s did not shut down cleanly:\n- %s",
                $owner ?? 'a test',
                implode("\n- ", $problems),
            ));
        }
    }

    /**
     * The connection a registration stands for: a client's own connection. A test usually drops its client when its
     * method returns, before its tearDown() runs, while the connection inside it lives on, held in a cycle of its own
     * closures, Open and with its heartbeat (which holds it weakly) still running: registering the client alone would
     * lose sight of exactly what has to be closed.
     */
    private static function connectionOf(NatsConnection|NatsClient $connection): NatsConnection
    {
        if ($connection instanceof NatsConnection) {
            return $connection;
        }

        $inner = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($connection);
        \assert($inner instanceof NatsConnection);

        return $inner;
    }

    /** @param \ArrayObject<int, string> $errors */
    private static function attempt(\Closure $hook, string $label, \ArrayObject $errors): void
    {
        try {
            $hook();
        } catch (\Throwable $e) {
            $errors[] = sprintf('%s threw %s', $label, self::describe($e));
        }
    }

    private static function describe(\Throwable $e): string
    {
        return $e::class . ': ' . $e->getMessage();
    }

    private function assertOpen(): void
    {
        if ($this->closed) {
            throw new \LogicException('This test resource scope is closed; a test registers its resources before its tearDown().');
        }
    }
}
