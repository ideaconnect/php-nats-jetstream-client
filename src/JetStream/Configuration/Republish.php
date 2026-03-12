<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Configuration;

/**
 * Configures stream republish rules.
 *
 * Usage:
 *   $js->createStream('ORDERS', ['orders.>'], [
 *       'republish' => Republish::create('orders.>', 'copy.orders.>')->headersOnly()->toArray(),
 *   ]);
 */
final class Republish
{
    private bool $headersOnly = false;

    /**
     * @param string $src Source subject filter to match retained stream messages.
     * @param string $dest Destination subject used when republishing matched messages.
     */
    private function __construct(
        private readonly string $src,
        private readonly string $dest,
    ) {
    }

    /**
     * Creates a republish rule mapping a source subject filter to a destination subject.
     */
    public static function create(string $src, string $dest): self
    {
        return new self($src, $dest);
    }

    /**
     * Only republish headers (strip payload).
     *
     * @return $this
     */
    public function headersOnly(bool $headersOnly = true): self
    {
        $this->headersOnly = $headersOnly;

        return $this;
    }

    /**
     * Serializes to the NATS JetStream API republish configuration array.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $result = [
            'src' => $this->src,
            'dest' => $this->dest,
        ];

        if ($this->headersOnly) {
            $result['headers_only'] = true;
        }

        return $result;
    }
}
