<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Revolt\EventLoop\CallbackType;
use Revolt\EventLoop\Driver;
use Revolt\EventLoop\Suspension;

/**
 * A transparent event-loop driver that counts registrations for a {@see LoopRegistrationCounter} (#184).
 *
 * Every call goes to the driver it wraps, unchanged - the same callbacks, arguments, ids, suspensions, error handler
 * and enabled/referenced state, and the same errors. A defer, delay, repeat, readable, writable or signal registration
 * is counted once the wrapped driver has made it, so one that throws is not; queue() is a microtask, not a callback
 * registration, and is not counted. Ids are never decoded: what is counted is the calls.
 */
final class RegistrationCountingDriver implements Driver
{
    public function __construct(private readonly Driver $inner, public readonly LoopRegistrationCounter $counter) {}

    public function run(): void
    {
        $this->inner->run();
    }

    public function stop(): void
    {
        $this->inner->stop();
    }

    /** @return Suspension<mixed> */
    public function getSuspension(): Suspension
    {
        return $this->inner->getSuspension();
    }

    public function isRunning(): bool
    {
        return $this->inner->isRunning();
    }

    public function queue(\Closure $closure, mixed ...$args): void
    {
        $this->inner->queue($closure, ...$args);
    }

    public function defer(\Closure $closure): string
    {
        $id = $this->inner->defer($closure);
        $this->counter->registration('defer');

        return $id;
    }

    public function delay(float $delay, \Closure $closure): string
    {
        $id = $this->inner->delay($delay, $closure);
        $this->counter->registration('delay');

        return $id;
    }

    public function repeat(float $interval, \Closure $closure): string
    {
        $id = $this->inner->repeat($interval, $closure);
        $this->counter->registration('repeat');

        return $id;
    }

    public function onReadable(mixed $stream, \Closure $closure): string
    {
        $id = $this->inner->onReadable($stream, $closure);
        $this->counter->registration('readable');

        return $id;
    }

    public function onWritable(mixed $stream, \Closure $closure): string
    {
        $id = $this->inner->onWritable($stream, $closure);
        $this->counter->registration('writable');

        return $id;
    }

    public function onSignal(int $signal, \Closure $closure): string
    {
        $id = $this->inner->onSignal($signal, $closure);
        $this->counter->registration('signal');

        return $id;
    }

    public function enable(string $callbackId): string
    {
        return $this->inner->enable($callbackId);
    }

    public function cancel(string $callbackId): void
    {
        $this->inner->cancel($callbackId);
    }

    public function disable(string $callbackId): string
    {
        return $this->inner->disable($callbackId);
    }

    public function reference(string $callbackId): string
    {
        return $this->inner->reference($callbackId);
    }

    public function unreference(string $callbackId): string
    {
        return $this->inner->unreference($callbackId);
    }

    public function setErrorHandler(?\Closure $errorHandler): void
    {
        $this->inner->setErrorHandler($errorHandler);
    }

    public function getErrorHandler(): ?\Closure
    {
        return $this->inner->getErrorHandler();
    }

    public function getHandle(): mixed
    {
        return $this->inner->getHandle();
    }

    /** @return array<string> */
    public function getIdentifiers(): array
    {
        return $this->inner->getIdentifiers();
    }

    public function getType(string $callbackId): CallbackType
    {
        return $this->inner->getType($callbackId);
    }

    public function isEnabled(string $callbackId): bool
    {
        return $this->inner->isEnabled($callbackId);
    }

    public function isReferenced(string $callbackId): bool
    {
        return $this->inner->isReferenced($callbackId);
    }

    /** @return array<mixed> */
    public function __debugInfo(): array
    {
        return $this->inner->__debugInfo();
    }
}
