<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Tests\Support\LoopRegistrationCounter;
use IDCT\NATS\Tests\Support\RegistrationCountingDriver;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Depends;
use PHPUnit\Framework\Attributes\PreserveGlobalState;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use Revolt\EventLoop\Driver;
use Revolt\EventLoop\DriverFactory;
use Revolt\EventLoop\UnsupportedFeatureException;

use function Amp\async;
use function Amp\delay;

/**
 * The event-loop registration count of the no-spin tests (#184): `tests/Support/LoopRegistrationCounter.php`, its
 * windows, and `tests/Support/RegistrationCountingDriver.php`, the transparent driver it puts around the loop's.
 *
 * The driver is checked on a driver of its own, made by Revolt's DriverFactory (the one REVOLT_DRIVER selects), never
 * installed: every call reaches the wrapped driver unchanged, each kind of registration is counted once the wrapped
 * driver has made it, a failed one and a queued microtask are not. The counter's install() swaps the process's loop
 * driver, so the cases that install it run in processes of their own, as the no-spin tests do: they check that such a
 * case gets a fresh process and a fresh loop, that install() refuses a loop with callbacks or one that has run, and
 * that a timer loop in a detached fiber is counted.
 */
final class LoopRegistrationCounterTest extends TestCase
{
    private const MARKER = 'IDCT_NATS_LOOP_REGISTRATION_COUNTER_TEST_PARENT_MARKER';

    /** A timer the parent process registered for the isolation check (null in a child process). */
    private static ?string $parentTimer = null;

    /** The file the isolated cases record their process ids in (null in a child process). */
    private static ?string $pidFile = null;

    /** Only the parent process has these set: a child starts with its own, empty, statics. */
    public static function tearDownAfterClass(): void
    {
        if (self::$parentTimer !== null) {
            EventLoop::cancel(self::$parentTimer);
            self::$parentTimer = null;
        }

        if (self::$pidFile !== null) {
            @unlink(self::$pidFile);
            self::$pidFile = null;
        }

        unset($GLOBALS[self::MARKER]);
    }

    /** Each kind of registration is counted once, in the open window, by kind, and the wrapped driver has made it. */
    public function testEachKindOfRegistrationIsCountedOnceTheWrappedDriverHasMadeIt(): void
    {
        [$inner, $driver, $counter] = self::wrapped();
        [$read, $write] = self::socketPair();
        $ids = [];

        $window = $counter->begin();
        try {
            $ids[] = $driver->defer(static function (): void {});
            $ids[] = $driver->delay(10.0, static function (): void {});
            $ids[] = $driver->repeat(10.0, static function (): void {});
            $ids[] = $driver->onReadable($read, static function (): void {});
            $ids[] = $driver->onWritable($write, static function (): void {});
            $signals = 1;
            try {
                $ids[] = $driver->onSignal(\defined('SIGUSR2') ? \SIGUSR2 : 12, static function (): void {});
            } catch (UnsupportedFeatureException) {
                // A driver without signal support: the failed registration is not counted (asserted below).
                $signals = 0;
            }
        } finally {
            $counter->end();
        }

        try {
            $expected = ['defer' => 1, 'delay' => 1, 'repeat' => 1, 'readable' => 1, 'writable' => 1];
            if ($signals === 1) {
                $expected['signal'] = 1;
            }
            self::assertSame(5 + $signals, $window->registrations);
            self::assertSame($expected, $window->kinds);
            foreach ($ids as $id) {
                self::assertContains($id, $inner->getIdentifiers(), 'the wrapped driver made the registration, with that id');
            }
        } finally {
            foreach ($ids as $id) {
                $inner->cancel($id);
            }
            fclose($read);
            fclose($write);
        }
    }

    /** A registration the wrapped driver refuses throws its error through the wrapper and is not counted. */
    public function testARefusedRegistrationIsNotCounted(): void
    {
        [, $driver, $counter] = self::wrapped();

        $window = $counter->begin();
        try {
            $driver->delay(-1.0, static function (): void {});
            self::fail('a negative delay was accepted');
        } catch (\Error $refused) {
            self::assertStringContainsString('Delay must be greater than or equal to zero', $refused->getMessage());
        } finally {
            $counter->end();
        }

        self::assertSame(0, $window->registrations);
        self::assertSame([], $window->kinds);
    }

    /**
     * Everything else goes to the wrapped driver unchanged: a microtask with its arguments (not counted), a deferred
     * callback with its id, enable, disable, reference, unreference and cancel, the type, the suspension, the error
     * handler, the handle and the debug info; run() runs the wrapped driver's loop, and stop() stops it.
     */
    public function testEverythingElseIsForwardedUnchanged(): void
    {
        [$inner, $driver, $counter] = self::wrapped();
        /** @var list<string> $calls */
        $calls = [];
        $window = $counter->begin();
        try {
            $driver->queue(static function (string $a, int $b) use (&$calls): void {
                $calls[] = 'queued ' . $a . ' ' . $b;
            }, 'x', 7);
            $deferred = $driver->defer(static function (string $id) use (&$calls, $driver): void {
                $calls[] = 'deferred ' . $id;
                $driver->stop();
            });
            $driver->run();
        } finally {
            $counter->end();
        }

        self::assertSame(['queued x 7', 'deferred ' . $deferred], $calls);
        self::assertSame(1, $window->registrations, 'the defer, not the queued microtask');
        self::assertFalse($driver->isRunning());

        $timer = $driver->delay(10.0, static function (): void {});
        try {
            self::assertSame(EventLoop\CallbackType::Delay, $driver->getType($timer));
            self::assertSame($timer, $driver->disable($timer));
            self::assertFalse($inner->isEnabled($timer));
            self::assertFalse($driver->isEnabled($timer));
            self::assertSame($timer, $driver->enable($timer));
            self::assertTrue($inner->isEnabled($timer));
            self::assertSame($timer, $driver->unreference($timer));
            self::assertFalse($inner->isReferenced($timer));
            self::assertFalse($driver->isReferenced($timer));
            self::assertSame($timer, $driver->reference($timer));
            self::assertTrue($inner->isReferenced($timer));
            self::assertSame($inner->getIdentifiers(), $driver->getIdentifiers());
        } finally {
            $driver->cancel($timer);
        }
        self::assertNotContains($timer, $inner->getIdentifiers());

        $handler = static function (\Throwable $error): void {};
        $driver->setErrorHandler($handler);
        self::assertSame($handler, $inner->getErrorHandler());
        self::assertSame($handler, $driver->getErrorHandler());
        $driver->setErrorHandler(null);
        self::assertNull($inner->getErrorHandler());

        self::assertSame($inner->getSuspension(), $driver->getSuspension());
        self::assertSame($inner->getHandle(), $driver->getHandle());
        self::assertSame($inner->__debugInfo(), $driver->__debugInfo());
    }

    /**
     * Only an open window counts: nothing before begin() or after end(), a closed window keeps its totals, a second
     * begin() while one is open is refused and leaves the open one counting, and the next window starts from zero.
     */
    public function testOnlyTheOpenWindowCounts(): void
    {
        $counter = new LoopRegistrationCounter();
        $counter->registration('delay');
        self::assertFalse($counter->counting());

        $first = $counter->begin();
        self::assertTrue($counter->counting());
        $counter->registration('delay');
        $counter->registration('defer');
        try {
            $counter->begin();
            self::fail('a second window was opened over the first');
        } catch (\LogicException $refused) {
            self::assertStringContainsString('open already', $refused->getMessage());
        }
        $counter->registration('delay');
        $counter->end();
        $counter->registration('delay');

        self::assertFalse($counter->counting());
        self::assertSame(3, $first->registrations);
        self::assertSame(['delay' => 2, 'defer' => 1], $first->kinds);

        $second = $counter->begin();
        $counter->registration('repeat');
        $counter->end();
        $counter->end();

        self::assertSame(1, $second->registrations);
        self::assertSame(['repeat' => 1], $second->kinds);
        self::assertSame(3, $first->registrations, 'the first window kept its totals');
    }

    /**
     * Runs in the parent process: leaves a timer on the parent's loop and a global marker, for the isolated cases to
     * check they have neither, and gives them the parent's process id and a file to record their own in.
     *
     * @return array{int, string, string}
     */
    public function testTheParentLeavesATimerAndAMarker(): array
    {
        self::$parentTimer = EventLoop::unreference(EventLoop::delay(3600.0, static function (): void {}));
        $GLOBALS[self::MARKER] = true;
        $pidFile = tempnam(sys_get_temp_dir(), 'loop-registration-pids-');
        self::assertIsString($pidFile);
        self::$pidFile = $pidFile;

        self::assertContains(self::$parentTimer, EventLoop::getIdentifiers());
        $pid = getmypid();
        self::assertIsInt($pid);

        return [$pid, self::$parentTimer, $pidFile];
    }

    /** @return iterable<string, array{string}> */
    public static function twoCases(): iterable
    {
        yield 'first case' => ['first'];
        yield 'second case' => ['second'];
    }

    /**
     * Each data case runs in a process of its own, as the no-spin tests' do: not the parent's, nor the other case's,
     * without the parent's global marker, and with a fresh loop - no callbacks, the parent's timer among them, and not
     * started - on which install() succeeds.
     *
     * @param array{int, string, string} $parent
     */
    #[DataProvider('twoCases')]
    #[Depends('testTheParentLeavesATimerAndAMarker')]
    #[RunInSeparateProcess]
    #[PreserveGlobalState(false)]
    public function testAnIsolatedCaseGetsAFreshProcessAndAFreshLoop(string $case, array $parent): void
    {
        [$parentPid, $parentTimer, $pidFile] = $parent;
        $earlier = array_filter(explode("\n", (string) file_get_contents($pidFile)));
        file_put_contents($pidFile, getmypid() . "\n", FILE_APPEND);

        self::assertNotSame($parentPid, getmypid(), $case . ': not the parent process');
        self::assertNotContains((string) getmypid(), $earlier, $case . ': not the process of an earlier case');
        self::assertArrayNotHasKey(self::MARKER, $GLOBALS, $case . ": the parent's global marker did not come along");
        self::assertNotContains($parentTimer, EventLoop::getIdentifiers(), $case . ": the parent's timer is not on this loop");
        self::assertSame([], EventLoop::getIdentifiers());
        self::assertFalse(EventLoop::getDriver()->isRunning());

        LoopRegistrationCounter::install();
        self::assertInstanceOf(RegistrationCountingDriver::class, EventLoop::getDriver());
    }

    /**
     * Installed, the counter sees the registrations made through the loop's static API - Amp's delay() among them -
     * also in a fiber of its own: a timer loop of 20 ticks in a detached fiber registers at least 20 timers. The
     * driver installed wraps the one the loop had.
     */
    #[RunInSeparateProcess]
    #[PreserveGlobalState(false)]
    public function testATimerLoopInADetachedFiberIsCounted(): void
    {
        $original = EventLoop::getDriver();
        $counter = LoopRegistrationCounter::install();
        $installed = EventLoop::getDriver();
        self::assertInstanceOf(RegistrationCountingDriver::class, $installed);
        self::assertSame($counter, $installed->counter);
        self::assertSame($original->getHandle(), $installed->getHandle(), 'the loop keeps its driver, wrapped');

        $window = $counter->begin();
        try {
            async(static function (): void {
                for ($tick = 0; $tick < 20; $tick++) {
                    delay(0.001);
                }
            })->await();
        } finally {
            $counter->end();
        }

        self::assertGreaterThanOrEqual(20, $window->registrations);
        self::assertGreaterThanOrEqual(20, $window->kinds['delay'] ?? 0);
    }

    /** install() refuses a loop with a callback registered: the case did not start from a fresh loop. */
    #[RunInSeparateProcess]
    #[PreserveGlobalState(false)]
    public function testInstallRefusesALoopWithCallbacks(): void
    {
        $timer = EventLoop::delay(10.0, static function (): void {});
        try {
            LoopRegistrationCounter::install();
            self::fail('the counter was installed on a loop with a callback');
        } catch (\LogicException $refused) {
            self::assertStringContainsString('1 callbacks registered', $refused->getMessage());
        } finally {
            EventLoop::cancel($timer);
        }

        self::assertNotInstanceOf(RegistrationCountingDriver::class, EventLoop::getDriver());
    }

    /** install() refuses a loop that has run - Revolt would refuse the swap too - even with no callback left. */
    #[RunInSeparateProcess]
    #[PreserveGlobalState(false)]
    public function testInstallRefusesALoopThatHasRun(): void
    {
        delay(0.001);
        self::assertSame([], EventLoop::getIdentifiers());

        try {
            LoopRegistrationCounter::install();
            self::fail('the counter was installed on a loop that has run');
        } catch (\LogicException $refused) {
            self::assertStringContainsString('running', $refused->getMessage());
        }

        self::assertNotInstanceOf(RegistrationCountingDriver::class, EventLoop::getDriver());
    }

    /** @return array{Driver, RegistrationCountingDriver, LoopRegistrationCounter} */
    private static function wrapped(): array
    {
        $inner = (new DriverFactory())->create();
        $counter = new LoopRegistrationCounter();

        return [$inner, new RegistrationCountingDriver($inner, $counter), $counter];
    }

    /** @return array{resource, resource} */
    private static function socketPair(): array
    {
        $pair = stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
        self::assertIsArray($pair);

        return [$pair[0], $pair[1]];
    }
}
