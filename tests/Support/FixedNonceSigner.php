<?php

declare(strict_types=1);

namespace Idct\Nats\Tests\Support;

use Idct\Nats\Auth\NonceSignerInterface;

final class FixedNonceSigner implements NonceSignerInterface
{
    /**
     * Creates a deterministic nonce signer used by unit tests.
     */
    public function __construct(private readonly string $prefix = 'signed:')
    {
    }

    /**
     * Returns a predictable signature for assertions.
     */
    public function sign(string $nonce): string
    {
        return $this->prefix . $nonce;
    }
}
