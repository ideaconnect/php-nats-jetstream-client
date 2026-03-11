<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

use IDCT\NATS\Core\NatsMessage;

final class ServiceGroup
{
    public function __construct(
        private readonly Service $service,
        private readonly string $prefix,
    ) {
    }

    /**
     * @param callable(NatsMessage):(string|array<string,mixed>|null) $handler
     * @param array<string,mixed>|null $schema
     */
    public function addEndpoint(string $name, string $subject, callable $handler, ?string $queueGroup = null, ?array $schema = null): self
    {
        $fullSubject = $this->joinSubject($this->prefix, $subject);
        $this->service->addEndpoint($name, $fullSubject, $handler, $queueGroup, $schema);

        return $this;
    }

    /**
     * Creates nested endpoint group within current prefix.
     */
    public function addGroup(string $name): self
    {
        return new self($this->service, $this->joinSubject($this->prefix, $name));
    }

    private function joinSubject(string $prefix, string $subject): string
    {
        $left = trim($prefix, '.');
        $right = trim($subject, '.');

        if ($left === '') {
            return $right;
        }

        if ($right === '') {
            return $left;
        }

        return $left . '.' . $right;
    }
}
