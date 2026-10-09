<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Core\NatsClient;

/**
 * Watches a drain() of a client over a {@see ReconnectingTransport} seen through a {@see WatchedTransport}, so that a
 * test can act on where the drain is, by the order of events: the drain writes an UNSUB for every subscription, then
 * the PING of its flush, and its flush is done once the PONG to that PING has been read. Installs the transport's
 * {@see ReconnectingTransport::$afterWrite}, which records the reads of the socket counted when the drain's PING went
 * out (a PING written while the client is Draining).
 */
final class DrainFlushWatcher
{
    /** The reads of the socket counted when the drain wrote its flush's PING; null before. */
    private ?int $readsAtThePing = null;

    public function __construct(
        private readonly ReconnectingTransport $transport,
        private readonly WatchedTransport $watched,
        NatsClient $client,
    ) {
        $transport->afterWrite = function (string $bytes) use ($client): void {
            if ($bytes === "PING\r\n" && $client->state() === ConnectionState::Draining) {
                $this->readsAtThePing ??= $this->watched->reads;
            }
        };
    }

    /** Whether the drain has written the PING of its flush, its UNSUBs written before it. */
    public function pingWritten(): bool
    {
        return $this->readsAtThePing !== null;
    }

    /**
     * Whether the drain's flush is done: a read of the socket started after its PING was written, and none is under
     * way any more, so the PONG, which the scripted server queues as it takes the PING, has been read. For a test in
     * which nothing but the drain reads once the PING is out (the runs wait in their handlers, or have ended): the
     * drain's fiber then goes on from that read with no further event, and has asked its participants by the time a
     * test that polls on a timer sees this.
     */
    public function flushDone(): bool
    {
        return $this->readsAtThePing !== null
            && $this->watched->reads > $this->readsAtThePing
            && $this->watched->readsUnderWay === 0;
    }

    /**
     * The pull requests the client wrote after the drain's UNSUB of the inbox with sid $sid, in wire order: a pull
     * written then is wasted, since nats-server drops a pull request whose reply subject has no interest (#207).
     *
     * @return list<string>
     */
    public function pullsAfterTheUnsubscribeOf(int $sid, string $pullSubject): array
    {
        $lines = $this->transport->controlLines();
        $unsubscribe = array_search('UNSUB ' . $sid, $lines, true);
        if ($unsubscribe === false) {
            return [];
        }

        return array_values(array_filter(
            array_slice($lines, $unsubscribe + 1),
            static fn(string $line): bool => str_starts_with($line, 'PUB ' . $pullSubject . ' '),
        ));
    }

    /** Whether the drain has written its UNSUB of the inbox with sid $sid. */
    public function unsubscribed(int $sid): bool
    {
        return in_array('UNSUB ' . $sid, $this->transport->controlLines(), true);
    }
}
