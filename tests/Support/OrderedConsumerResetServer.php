<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

/**
 * A scripted JetStream server for one ordered consumer on stream "EVENTS" whose consumer is reset by another client
 * (ADR-60, #274), on a {@see FakeTransport}. The stream holds $stored (stream sequence => payload). Each
 * CONSUMER.CREATE is answered on its mux reply subject with the consumer it names, which then delivers on its deliver
 * subject, with consumer sequences from 1:
 *
 * - the first consumer from stream sequence 1, once the client subscribed its deliver subject (the client subscribes
 *   it after the create); after $deliveredBeforeTheReset messages it stops and sends the frames $reset builds, what
 *   a reset made by another client has it send;
 * - a recreate from its `opt_start_seq` to the end of the stream, right after the create reply (the client subscribes
 *   a recreate's deliver subject before the create).
 *
 * DELETEs are confirmed and recorded.
 */
final class OrderedConsumerResetServer
{
    public readonly FakeTransport $transport;

    /** @var list<array<string, mixed>> The config of each CONSUMER.CREATE, in order. */
    public array $creates = [];

    /** @var list<string> The consumers deleted, in order. */
    public array $deleted = [];

    /** @var array<string, int> The sid of each subscribed subject. */
    private array $sids = [];

    /** @var array<string, \Closure(int): list<string>> The first consumer's frames, waiting for its deliver subject's SUB. */
    private array $waiting = [];

    /**
     * @param array<int, string> $stored
     * @param \Closure(string, int, string): list<string> $reset Builds the frames of the reset from the deliver subject,
     *        its sid and the consumer's name.
     */
    public function __construct(
        private readonly array $stored,
        private readonly int $deliveredBeforeTheReset,
        private readonly \Closure $reset,
    ) {
        $this->transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.14.10","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->transport->onWrite = $this->respond(...);
    }

    /** A delivery of $payload on $subject (sid $sid) with the $JS.ACK reply subject of the sequences given. */
    public static function delivery(string $subject, int $sid, string $stream, string $consumer, int $streamSeq, int $consumerSeq, string $payload): string
    {
        $ack = sprintf('$JS.ACK.%s.%s.1.%d.%d.0.0', $stream, $consumer, $streamSeq, $consumerSeq);

        return sprintf("MSG %s %d %s %d\r\n%s\r\n", $subject, $sid, $ack, strlen($payload), $payload);
    }

    /** An idle heartbeat on $subject (sid $sid) reporting the last delivered consumer and stream sequences. */
    public static function heartbeat(string $subject, int $sid, int $lastConsumer, int $lastStream): string
    {
        $headers = sprintf("NATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: %d\r\nNats-Last-Stream: %d\r\n\r\n", $lastConsumer, $lastStream);

        return sprintf("HMSG %s %d %d %d\r\n%s\r\n", $subject, $sid, strlen($headers), strlen($headers), $headers);
    }

    /** @return list<string> */
    private function respond(string $bytes): array
    {
        $head = strtok($bytes, "\r\n");
        if ($head === false) {
            return [];
        }

        $parts = explode(' ', $head);
        if ($parts[0] === 'SUB') {
            $subject = $parts[1] ?? '';
            $sid = (int) ($parts[2] ?? 0);
            $this->sids[$subject] = $sid;
            $frames = isset($this->waiting[$subject]) ? ($this->waiting[$subject])($sid) : [];
            unset($this->waiting[$subject]);

            return $frames;
        }

        if ($parts[0] !== 'PUB' || count($parts) < 4) {
            return [];
        }

        [, $subject, $replyTo, $length] = $parts;
        if (str_starts_with($subject, '$JS.API.CONSUMER.DELETE.EVENTS.')) {
            $this->deleted[] = substr($subject, strlen('$JS.API.CONSUMER.DELETE.EVENTS.'));

            return [self::reply($replyTo, '{"success":true}')];
        }

        if (!str_starts_with($subject, '$JS.API.CONSUMER.CREATE.EVENTS')) {
            return [];
        }

        $request = json_decode(substr($bytes, strlen($head) + 2, (int) $length), true, 16, JSON_THROW_ON_ERROR);
        /** @var array<string, mixed> $config */
        $config = is_array($request) && is_array($request['config'] ?? null) ? $request['config'] : [];
        $this->creates[] = $config;
        $first = count($this->creates) === 1;
        $name = is_string($config['name'] ?? null) ? $config['name'] : 'ORD' . count($this->creates);
        $deliver = is_string($config['deliver_subject'] ?? null) ? $config['deliver_subject'] : '';
        $start = $first ? 1 : (is_int($config['opt_start_seq'] ?? null) ? $config['opt_start_seq'] : 1);
        $reply = self::reply($replyTo, json_encode(['stream_name' => 'EVENTS', 'name' => $name, 'config' => $config], JSON_THROW_ON_ERROR));

        $frames = function (int $sid) use ($deliver, $name, $start, $first): array {
            $frames = [];
            $consumerSeq = 0;
            foreach ($this->stored as $streamSeq => $payload) {
                if ($streamSeq < $start || ($first && $consumerSeq === $this->deliveredBeforeTheReset)) {
                    continue;
                }

                $frames[] = self::delivery($deliver, $sid, 'EVENTS', $name, $streamSeq, ++$consumerSeq, $payload);
            }

            return $first ? [...$frames, ...($this->reset)($deliver, $sid, $name)] : $frames;
        };

        if (!isset($this->sids[$deliver])) {
            $this->waiting[$deliver] = $frames;

            return [$reply];
        }

        return [$reply, ...$frames($this->sids[$deliver])];
    }

    private static function reply(string $replyTo, string $payload): string
    {
        return sprintf("MSG %s 1 %d\r\n%s\r\n", $replyTo, strlen($payload), $payload);
    }
}
