<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Core\NatsClient;
use Revolt\EventLoop;
use Revolt\EventLoop\CallbackType;

/**
 * Gives a test case a {@see TestResourceScope} (#183), made on first use, and its shutdown: the test registers each
 * client or connection with {@see own()} right after constructing it, and anything else that must end with the test
 * through {@see resources()}; its tearDown() calls {@see releaseOwnedResources()}, which runs after a failed assertion
 * as well as after a passing test, and throws a {@see TestResourceCleanupFailure} when something did not shut down.
 */
trait OwnsTestResources
{
    private ?TestResourceScope $testResources = null;

    /**
     * @template T of NatsConnection|NatsClient
     *
     * @param T $connection
     * @return T
     */
    private function own(NatsConnection|NatsClient $connection, ?string $label = null): NatsConnection|NatsClient
    {
        return $this->resources()->own($connection, $label);
    }

    private function resources(): TestResourceScope
    {
        return $this->testResources ??= new TestResourceScope();
    }

    /**
     * For a class whose tests arm heartbeat watchdogs (#113): its setUp() makes the scope, so that the scope knows the
     * loop callbacks registered before the test, and its tearDown() calls this instead of {@see releaseOwnedResources()}.
     * Once every connection the test owned is Closed, what is left of the repeat timers the test registered are those
     * watchdogs: each holds its client weakly and cancels itself only at its next tick, up to an idle_heartbeat away
     * (5 s for an ordered consumer), and a raw push consumer has no stop handle that would cancel it sooner. They are
     * cancelled by the ids recorded since setUp(), never guessed from a count, and nothing else is touched.
     */
    private function releaseOwnedResourcesAndTheirWatchdogs(): void
    {
        $registered = $this->testResources?->callbacksRegisteredSince() ?? [];

        try {
            $this->releaseOwnedResources();
        } finally {
            $live = array_fill_keys(EventLoop::getIdentifiers(), true);
            foreach ($registered as $id) {
                if (isset($live[$id]) && EventLoop::getType($id) === CallbackType::Repeat) {
                    EventLoop::cancel($id);
                }
            }
        }
    }

    private function releaseOwnedResources(): void
    {
        $scope = $this->testResources;
        $this->testResources = null;

        try {
            $scope?->close(static::class . '::' . $this->name());
        } catch (TestResourceCleanupFailure $cleanupFailure) {
            // A test that already failed keeps its own failure, which PHPUnit reports in place of anything its
            // tearDown() throws: what the shutdown found goes alongside it, as additional information.
            if ($this->status()->isFailure() || $this->status()->isError()) {
                fwrite(STDERR, PHP_EOL . $cleanupFailure->getMessage() . PHP_EOL);

                return;
            }

            throw $cleanupFailure;
        }
    }
}
