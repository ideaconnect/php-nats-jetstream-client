<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Revolt\EventLoop;

/**
 * Counts the callbacks registered on the event loop over a window of a test (#184), through a
 * {@see RegistrationCountingDriver} put around the loop's driver.
 *
 * A wait that parks on a future registers a handful of callbacks however long it lasts; one that polls on a timer
 * registers one per tick - hundreds in a second at 1 ms, tens of thousands with no delay - whether the loop returns
 * to the operation that waits or not, and in whichever fiber it runs. The count is of the whole process: install it
 * only in a test that runs in a process of its own (`#[RunInSeparateProcess]`, `#[PreserveGlobalState(false)]`), as
 * the first thing it does, so that no client of an earlier test, and nothing the test did not start, is counted.
 * {@see install()} refuses a loop that has already been started or has callbacks registered. The driver stays
 * installed until the process ends: Revolt refuses to swap a driver it has run.
 *
 * One window at a time: {@see begin()} opens it, {@see end()} closes it - put the measured wait in a try whose finally
 * calls end(), so a wait that fails does not leave the window open. A registration made while no window is open is
 * not counted, and a closed window keeps its totals.
 */
final class LoopRegistrationCounter
{
    private ?RegistrationWindow $window = null;

    /**
     * Puts a counting driver around the event loop's driver, the one REVOLT_DRIVER selects, whatever it is.
     *
     * @throws \LogicException When the loop has been started or has callbacks registered already: the case does not
     *                         run in a fresh process, or something ran on the loop before the counter was installed.
     */
    public static function install(): self
    {
        $driver = EventLoop::getDriver();
        if ($driver->isRunning() || $driver->getIdentifiers() !== []) {
            throw new \LogicException(sprintf(
                'Install the loop registration counter first thing in a test that runs in a separate process: the loop'
                . ' is %s and has %d callbacks registered',
                $driver->isRunning() ? 'running' : 'not running',
                count($driver->getIdentifiers()),
            ));
        }

        $counter = new self();
        EventLoop::setDriver(new RegistrationCountingDriver($driver, $counter));

        return $counter;
    }

    /**
     * Opens a window: the registrations from now on are counted in it, until {@see end()}.
     *
     * @throws \LogicException When a window is open already; that one stays open.
     */
    public function begin(): RegistrationWindow
    {
        if ($this->window !== null) {
            throw new \LogicException('A registration window is open already: end it before beginning another');
        }

        return $this->window = new RegistrationWindow();
    }

    /** Closes the open window, if there is one: it keeps its totals. */
    public function end(): void
    {
        $this->window = null;
    }

    /** Whether a window is open. */
    public function counting(): bool
    {
        return $this->window !== null;
    }

    /** Counts one registration of $kind in the open window; nothing when none is open. */
    public function registration(string $kind): void
    {
        if ($this->window === null) {
            return;
        }

        $this->window->registrations++;
        $this->window->kinds[$kind] = ($this->window->kinds[$kind] ?? 0) + 1;
    }
}
