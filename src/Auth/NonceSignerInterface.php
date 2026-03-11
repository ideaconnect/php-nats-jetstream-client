<?php

declare(strict_types=1);

namespace Idct\Nats\Auth;

interface NonceSignerInterface
{
    /**
     * Signs a server nonce and returns a base64url-compatible signature.
     */
    public function sign(string $nonce): string;
}
