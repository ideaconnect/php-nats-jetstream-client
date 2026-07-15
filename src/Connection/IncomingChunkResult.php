<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

/**
 * Result of one {@see NatsConnection::readIncoming()} cycle: the count of complete frames parsed and
 * dispatched from the chunk, plus whether that read actually consumed bytes off the wire.
 *
 * The two are distinct on purpose (#119). A read can consume bytes yet complete zero frames when a
 * single frame spans several transport chunks (a large payload arriving in socket-sized pieces): its
 * leading chunks push bytes into the parser but no frame finishes until the last one lands. A wait
 * loop must treat that partial progress differently from a genuinely idle read (empty socket): it
 * should loop immediately to drain the rest of the frame that is already buffered, NOT pay a 1 ms
 * idle sleep per chunk. Only a read that consumed nothing ({@see $consumedBytes} false - an empty
 * read, or another fiber owning the socket) is truly idle and yields the 1 ms so the event loop
 * advances and deadlines fire.
 */
final class IncomingChunkResult
{
    /**
     * @param int $frames Number of complete frames parsed and dispatched from this read (>= 0).
     * @param bool $consumedBytes Whether this read pulled a non-empty chunk off the transport and fed
     *                            it to the parser - i.e. it made wire progress, even if $frames is 0
     *                            because the frame is not yet complete. False for an empty read, a
     *                            read skipped because another fiber owns the socket, or a read error.
     */
    public function __construct(
        public readonly int $frames,
        public readonly bool $consumedBytes,
    ) {}
}
