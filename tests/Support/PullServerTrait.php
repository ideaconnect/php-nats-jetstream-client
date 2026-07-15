<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\JetStreamContext;

/**
 * Shared pipelined-pull test harness (#120). Installs a {@see FakeTransport} onWrite responder that
 * models a JetStream "pull server": it learns the long-lived pull inbox sid from the wildcard
 * "SUB _INBOX.JS.PULL.<base>.* <sid>" and, for each CONSUMER.MSG.NEXT pull PUB, emits the next queued
 * per-pull frame-set on the learned sid.
 *
 * Crucially it delivers frames the way a REAL server does (verified live, #120):
 *   - DATA messages arrive on the stored message's ORIGINAL subject (here a synthetic "evt.<stream>")
 *     with a "$JS.ACK.<stream>.<consumer>..." reply - NOT on the pull reply token. They carry no token,
 *     so the engine attributes them FIFO to the oldest open pull.
 *   - STATUS / heartbeat frames (404/408/409/423/100 ...) arrive ON the pull's captured reply token
 *     "_INBOX.JS.PULL.<base>.<token>", which is how the engine correlates a status to its pull.
 * (An earlier version of this harness wrongly echoed data on the reply token too; that masked a bug
 * where the engine dropped every real data delivery - see the pull-delivery-wire-shape note.)
 */
trait PullServerTrait
{
    /** @return list<string> INFO + PONG so connect() succeeds. */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    /** Connects a client over the fake transport and returns its JetStream context. */
    private function context(FakeTransport $transport): JetStreamContext
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client->jetStream();
    }

    /**
     * Installs the pull "server": for each pull PUB to $JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>, it
     * pops the next entry of $perPull (FIFO) and emits its frames on that pull's captured reply-to using
     * the learned pull inbox sid. Each frame is one of:
     *   ['msg' => payload]                 -> a plain data message (MSG)
     *   ['msg' => payload, 'pin' => pinId] -> a data message carrying a Nats-Pin-Id header (HMSG, status 0)
     *   ['status' => code, 'desc' => text] -> a status frame (404/408/409/423 ...) (HMSG, status >= 400)
     *
     * A PUB whose $perPull entry is missing yields no reply (models a pull left in flight / abandoned).
     *
     * @param list<list<array{msg?: string, status?: int, desc?: string, pin?: string}>> $perPull
     */
    private function pullServer(FakeTransport $transport, string $stream, string $consumer, array $perPull): void
    {
        $pullSubject = '$JS.API.CONSUMER.MSG.NEXT.' . $stream . '.' . $consumer;
        // Synthetic original subject + ACK prefix for data deliveries (a real server delivers stored
        // messages on their own subject with a $JS.ACK reply, not on the pull reply token).
        $dataSubject = 'evt.' . strtolower($stream);
        $ackPrefix = '$JS.ACK.' . $stream . '.' . $consumer;
        $pullSid = 1;
        $index = 0;
        $seq = 0;
        $transport->onWrite = static function (string $bytes) use ($pullSubject, $dataSubject, $ackPrefix, &$pullSid, &$index, &$seq, $perPull): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            // Learn the long-lived pull inbox sid from "SUB _INBOX.JS.PULL.<base>.* <sid>".
            if (str_starts_with($head, 'SUB _INBOX.JS.PULL.') && str_contains($head, '.* ')) {
                $pullSid = (int) (explode(' ', $head)[2] ?? 1);

                return [];
            }

            if (!str_starts_with($head, 'PUB ' . $pullSubject . ' ')) {
                return [];
            }

            // The pull's reply token "_INBOX.JS.PULL.<base>.<token>" - the address STATUS frames use.
            $replyTo = explode(' ', $head)[2] ?? '';
            if ($replyTo === '' || !isset($perPull[$index])) {
                return [];
            }

            /** @var list<string> $frames */
            $frames = [];
            foreach ($perPull[$index] as $frame) {
                if (isset($frame['status'])) {
                    // STATUS frame: delivered ON the reply token so the engine correlates it to this pull.
                    $hdr = sprintf("NATS/1.0 %d %s\r\n\r\n", $frame['status'], $frame['desc'] ?? '');
                    $frames[] = sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $pullSid, strlen($hdr), strlen($hdr), $hdr);

                    continue;
                }

                if (!isset($frame['msg'])) {
                    continue;
                }

                // DATA message: delivered on the ORIGINAL subject with a $JS.ACK reply (never the token).
                $payload = $frame['msg'];
                $ack = $ackPrefix . '.1.' . (++$seq) . '.' . $seq . '.0.0';
                if (isset($frame['pin'])) {
                    // Data message carrying a Nats-Pin-Id header - delivered as an HMSG (still on the
                    // original subject) so pinIdOf() can capture the pin from the buffered message.
                    $hdr = sprintf("NATS/1.0\r\nNats-Pin-Id: %s\r\n\r\n", $frame['pin']);
                    $frames[] = sprintf(
                        "HMSG %s %d %s %d %d\r\n%s%s\r\n",
                        $dataSubject,
                        $pullSid,
                        $ack,
                        strlen($hdr),
                        strlen($hdr) + strlen($payload),
                        $hdr,
                        $payload,
                    );

                    continue;
                }

                $frames[] = sprintf("MSG %s %d %s %d\r\n%s\r\n", $dataSubject, $pullSid, $ack, strlen($payload), $payload);
            }
            ++$index;

            return $frames;
        };
    }

    /** @return list<string> The CONSUMER.MSG.NEXT pull requests recorded on the transport, in order. */
    private function pullWrites(FakeTransport $transport): array
    {
        return array_values(array_filter(
            $transport->writes,
            static fn (string $w): bool => str_contains($w, 'CONSUMER.MSG.NEXT'),
        ));
    }
}
