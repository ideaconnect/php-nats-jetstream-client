<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

/**
 * A scripted JetStream server for {@see FakeTransport} that answers the JetStream API under ONE API prefix only (#273),
 * the way the JetStream of a hub looks from a client connected to a leaf node of another domain:
 *
 *   - a request under the prefix (`$JS.hub.API.STREAM.CREATE.ORDERS`) is answered: streams, consumers, pulls, Direct
 *     Get and STREAM.MSG.GET, purges and account info are modelled closely enough for the client's own checks;
 *   - a KV write under the prefix (`$JS.hub.API.$KV.cfg.color`) is stored as `$KV.cfg.color`, as nats-server's
 *     domain mapping `$JS.<domain>.API.$KV.>` -> `$KV.>` does;
 *   - a request to any other `$JS.` subject (`$JS.API.INFO` from a leaf node) and, under a non-default prefix, a
 *     plain `$KV.` write get the no-responders status 503, as the live hub/leaf run of #273 showed;
 *   - a publish to a stream's own subjects (`orders.created`, `$O.files.C.<nuid>`) is stored and acknowledged with a
 *     PubAck, and anything else with a reply gets 503.
 *
 * It answers every PING with a PONG (so give the transport only the {@see info()} line), delivers a reply on the
 * subscription matching its reply subject (the client's muxed request inbox or a pull inbox), and records every
 * publish the client writes in {@see $published}. With `$directGet` false it answers Direct Get with 503, as for a
 * stream without `allow_direct`.
 */
final class PrefixedJetStreamServer
{
    /**
     * The server version its INFO advertises, a 2.12 leaf node's. No call is decided from it (ADR-44, #272): the
     * consumer reset, a 2.14 API, is sent through it like every other request.
     */
    public const VERSION = '2.12.0';

    private const INFO_FORMAT = 'INFO {"server_id":"S1","server_name":"leaf","version":"%s","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    private const DEFAULT_PREFIX = '$JS.API.';

    private const TIMESTAMP = '2026-11-26T00:00:00.000000000Z';

    /** @var list<array{subject: string, reply: ?string, headers: string, payload: string}> Every PUB/HPUB written. */
    public array $published = [];

    /** @var array<int, string> Live subscriptions: sid => subject pattern. */
    private array $subscriptions = [];

    /** @var array<string, array<string,mixed>> Stream configs by stream name. */
    private array $streams = [];

    /** @var array<string, list<array{seq: int, subject: string, headers: string, payload: string}>> Messages by stream. */
    private array $messages = [];

    /** @var array<string, int> Next message index per "<stream>.<consumer>". */
    private array $consumers = [];

    /** @var array<string, int> Last sequence per stream. */
    private array $lastSequence = [];

    public function __construct(private readonly string $apiPrefix, private readonly bool $directGet = true) {}

    /** The INFO line the server greets the client with, advertising {@see VERSION}. */
    public static function info(): string
    {
        return sprintf(self::INFO_FORMAT, self::VERSION);
    }

    public function install(FakeTransport $transport): void
    {
        $transport->onWrite = fn(string $bytes): array => $this->respond($bytes);
    }

    /**
     * The subjects of every publish written (requests included), in order.
     *
     * @return list<string>
     */
    public function publishedSubjects(): array
    {
        return array_map(static fn(array $publish): string => $publish['subject'], $this->published);
    }

    /**
     * Parses the frames of one client write and returns the frames the server sends back.
     *
     * @return list<string>
     */
    private function respond(string $bytes): array
    {
        $replies = [];
        $offset = 0;
        $length = strlen($bytes);
        while ($offset < $length) {
            $end = strpos($bytes, "\r\n", $offset);
            if ($end === false) {
                break;
            }

            $head = substr($bytes, $offset, $end - $offset);
            $offset = $end + 2;
            $parts = explode(' ', $head);
            $op = strtoupper($parts[0]);

            if ($op === 'PING') {
                $replies[] = "PONG\r\n";
            } elseif ($op === 'SUB') {
                $this->subscriptions[(int) $parts[count($parts) - 1]] = $parts[1];
            } elseif ($op === 'UNSUB' && count($parts) === 2) {
                unset($this->subscriptions[(int) $parts[1]]);
            } elseif ($op === 'PUB' || $op === 'HPUB') {
                $headerLength = $op === 'HPUB' ? (int) $parts[count($parts) - 2] : 0;
                $totalLength = (int) $parts[count($parts) - 1];
                $reply = count($parts) === ($op === 'HPUB' ? 5 : 4) ? $parts[2] : null;
                $headers = substr($bytes, $offset, $headerLength);
                $payload = substr($bytes, $offset + $headerLength, $totalLength - $headerLength);
                $offset += $totalLength + 2;

                $this->published[] = ['subject' => $parts[1], 'reply' => $reply, 'headers' => $headers, 'payload' => $payload];
                array_push($replies, ...$this->handlePublish($parts[1], $reply, $headers, $payload));
            }
        }

        return $replies;
    }

    /**
     * @return list<string>
     */
    private function handlePublish(string $subject, ?string $reply, string $headers, string $payload): array
    {
        if (str_starts_with($subject, '$JS.ACK.')) {
            return $reply === null ? [] : [$this->message($reply, $reply, '')];
        }

        if (str_starts_with($subject, $this->apiPrefix)) {
            $rest = substr($subject, strlen($this->apiPrefix));
            if ($this->apiPrefix !== self::DEFAULT_PREFIX && str_starts_with($rest, '$KV.')) {
                return $this->store($rest, $reply, $headers, $payload);
            }

            return $reply === null ? [] : $this->api($rest, $reply, $payload);
        }

        $noResponders = str_starts_with($subject, '$JS.')
            || ($this->apiPrefix !== self::DEFAULT_PREFIX && str_starts_with($subject, '$KV.'));
        if ($noResponders) {
            return $reply === null ? [] : [$this->status($reply, 503)];
        }

        return $this->store($subject, $reply, $headers, $payload);
    }

    /**
     * Stores a publish in the stream whose subjects match it and acknowledges it (a 503 when none matches).
     *
     * @return list<string>
     */
    private function store(string $subject, ?string $reply, string $headers, string $payload): array
    {
        foreach ($this->streams as $name => $config) {
            /** @var list<string> $patterns */
            $patterns = is_array($config['subjects'] ?? null) ? $config['subjects'] : [];
            foreach ($patterns as $pattern) {
                if (!self::matches($pattern, $subject)) {
                    continue;
                }

                $seq = ($this->lastSequence[$name] ?? 0) + 1;
                $this->lastSequence[$name] = $seq;
                $this->messages[$name][] = ['seq' => $seq, 'subject' => $subject, 'headers' => $headers, 'payload' => $payload];

                return $reply === null ? [] : [$this->json($reply, ['stream' => $name, 'seq' => $seq] + $this->domainField())];
            }
        }

        return $reply === null ? [] : [$this->status($reply, 503)];
    }

    /**
     * Answers a JetStream API request ($rest is the subject after the prefix).
     *
     * @return list<string>
     */
    private function api(string $rest, string $reply, string $payload): array
    {
        /** @var array<string,mixed> $request */
        $request = $payload === '' ? [] : (array) json_decode($payload, true);

        if ($rest === 'INFO') {
            return [$this->json($reply, ['type' => 'io.nats.jetstream.api.v1.account_info_response', 'memory' => 0, 'storage' => 0, 'streams' => count($this->streams), 'consumers' => count($this->consumers)] + $this->domainField())];
        }

        if (preg_match('/^STREAM\.(CREATE|UPDATE)\.([^.]+)$/', $rest, $match) === 1) {
            $this->streams[$match[2]] = $request;

            return [$this->json($reply, $this->streamInfo($match[2], 'io.nats.jetstream.api.v1.stream_create_response'))];
        }

        if (preg_match('/^STREAM\.INFO\.([^.]+)$/', $rest, $match) === 1) {
            if (!isset($this->streams[$match[1]])) {
                return [$this->error($reply, 404, 10059, 'stream not found')];
            }

            return [$this->json($reply, $this->streamInfo($match[1], 'io.nats.jetstream.api.v1.stream_info_response'))];
        }

        if (preg_match('/^STREAM\.PURGE\.([^.]+)$/', $rest) === 1) {
            return [$this->json($reply, ['type' => 'io.nats.jetstream.api.v1.stream_purge_response', 'success' => true, 'purged' => 0])];
        }

        if (preg_match('/^CONSUMER\.CREATE\.([^.]+)(?:\.([^.]+))?/', $rest, $match) === 1) {
            /** @var array<string,mixed> $config */
            $config = is_array($request['config'] ?? null) ? $request['config'] : [];
            $name = (string) ($config['durable_name'] ?? $config['name'] ?? $match[2] ?? 'ephemeral');
            $this->consumers[$match[1] . '.' . $name] ??= 0;

            return [$this->json($reply, [
                'type' => 'io.nats.jetstream.api.v1.consumer_create_response',
                'stream_name' => $match[1],
                'name' => $name,
                'created' => self::TIMESTAMP,
                'config' => $config,
                'delivered' => ['consumer_seq' => 0, 'stream_seq' => 0],
                'ack_floor' => ['consumer_seq' => 0, 'stream_seq' => 0],
                'num_ack_pending' => 0,
                'num_redelivered' => 0,
                'num_waiting' => 0,
                'num_pending' => count($this->messages[$match[1]] ?? []),
            ])];
        }

        if (preg_match('/^CONSUMER\.MSG\.NEXT\.([^.]+)\.([^.]+)$/', $rest, $match) === 1) {
            return $this->pull($match[1], $match[2], $reply, max(1, (int) ($request['batch'] ?? 1)));
        }

        if (preg_match('/^DIRECT\.GET\.([^.]+)(?:\.(.+))?$/', $rest, $match) === 1) {
            if (!$this->directGet) {
                // A stream without allow_direct: nothing answers its Direct Get subject.
                return [$this->status($reply, 503)];
            }

            $found = $this->find($match[1], isset($match[2]) ? ['last_by_subj' => $match[2]] : $request);

            return [$found === null ? $this->status($reply, 404, 'Message Not Found') : $this->directGetReply($reply, $match[1], $found)];
        }

        if (preg_match('/^STREAM\.MSG\.GET\.([^.]+)$/', $rest, $match) === 1) {
            $found = $this->find($match[1], $request);
            if ($found === null) {
                return [$this->error($reply, 404, 10037, 'no message found')];
            }

            $message = ['subject' => $found['subject'], 'seq' => $found['seq'], 'data' => base64_encode($found['payload']), 'time' => self::TIMESTAMP];
            if ($found['headers'] !== '') {
                $message['hdrs'] = base64_encode($found['headers']);
            }

            return [$this->json($reply, ['type' => 'io.nats.jetstream.api.v1.stream_msg_get_response', 'message' => $message])];
        }

        return [$this->error($reply, 400, 10000, 'not modelled by the fake: ' . $rest)];
    }

    /**
     * Delivers up to $batch stored messages to a pull, as a server does: on their own subject, with a `$JS.ACK` reply,
     * on the subscription of the pull's reply subject; a 404 status on the reply subject when nothing is pending.
     *
     * @return list<string>
     */
    private function pull(string $stream, string $consumer, string $reply, int $batch): array
    {
        $sid = $this->sidFor($reply);
        if ($sid === null) {
            return [];
        }

        $key = $stream . '.' . $consumer;
        $index = $this->consumers[$key] ?? 0;
        $pending = array_slice($this->messages[$stream] ?? [], $index, $batch);
        if ($pending === []) {
            return [$this->status($reply, 404, 'No Messages')];
        }

        $frames = [];
        foreach ($pending as $message) {
            $index++;
            $left = count($this->messages[$stream] ?? []) - $index;
            $ack = sprintf('$JS.ACK.%s.%s.1.%d.%d.1764115200000000000.%d', $stream, $consumer, $message['seq'], $index, $left);
            $frames[] = sprintf("MSG %s %d %s %d\r\n%s\r\n", $message['subject'], $sid, $ack, strlen($message['payload']), $message['payload']);
        }
        $this->consumers[$key] = $index;

        return $frames;
    }

    /**
     * Finds a stored message by `seq` or `last_by_subj`.
     *
     * @param array<string,mixed> $query
     * @return array{seq: int, subject: string, headers: string, payload: string}|null
     */
    private function find(string $stream, array $query): ?array
    {
        $found = null;
        foreach ($this->messages[$stream] ?? [] as $message) {
            $bySequence = isset($query['seq']) && $message['seq'] === (int) $query['seq'];
            $bySubject = isset($query['last_by_subj']) && $message['subject'] === $query['last_by_subj'];
            if ($bySequence || $bySubject) {
                $found = $message;
            }
        }

        return $found;
    }

    /**
     * @param array{seq: int, subject: string, headers: string, payload: string} $message
     */
    private function directGetReply(string $reply, string $stream, array $message): string
    {
        $stored = $message['headers'] === '' ? '' : (string) preg_replace('/^NATS\/1\.0\r\n|\r\n$/', '', $message['headers']);
        $headers = sprintf(
            "NATS/1.0\r\nNats-Stream: %s\r\nNats-Subject: %s\r\nNats-Sequence: %d\r\nNats-Time-Stamp: %s\r\n%s\r\n",
            $stream,
            $message['subject'],
            $message['seq'],
            self::TIMESTAMP,
            $stored,
        );

        return sprintf(
            "HMSG %s %d %d %d\r\n%s%s\r\n",
            $reply,
            $this->sidFor($reply) ?? 0,
            strlen($headers),
            strlen($headers) + strlen($message['payload']),
            $headers,
            $message['payload'],
        );
    }

    /**
     * @return array<string,mixed>
     */
    private function streamInfo(string $name, string $type): array
    {
        $config = $this->streams[$name];
        $config['name'] = $name;
        $count = count($this->messages[$name] ?? []);

        return [
            'type' => $type,
            'config' => $config,
            'created' => self::TIMESTAMP,
            'state' => ['messages' => $count, 'bytes' => 0, 'first_seq' => $count > 0 ? 1 : 0, 'last_seq' => $this->lastSequence[$name] ?? 0, 'consumer_count' => 0],
        ];
    }

    /**
     * @return array<string,string>
     */
    private function domainField(): array
    {
        return preg_match('/^\$JS\.([^.]+)\.API\.$/', $this->apiPrefix, $match) === 1 ? ['domain' => $match[1]] : [];
    }

    /**
     * @param array<string,mixed> $body
     */
    private function json(string $reply, array $body): string
    {
        return $this->message($reply, $reply, json_encode($body, JSON_THROW_ON_ERROR));
    }

    private function error(string $reply, int $code, int $errCode, string $description): string
    {
        return $this->json($reply, ['error' => ['code' => $code, 'err_code' => $errCode, 'description' => $description]]);
    }

    private function message(string $reply, string $subject, string $payload): string
    {
        return sprintf("MSG %s %d %d\r\n%s\r\n", $subject, $this->sidFor($reply) ?? 0, strlen($payload), $payload);
    }

    private function status(string $reply, int $code, string $description = ''): string
    {
        $headers = sprintf("NATS/1.0 %d%s\r\n\r\n", $code, $description === '' ? '' : ' ' . $description);

        return sprintf("HMSG %s %d %d %d\r\n%s\r\n", $reply, $this->sidFor($reply) ?? 0, strlen($headers), strlen($headers), $headers);
    }

    /** The sid of the subscription a reply subject is delivered on: an exact one first, then a wildcard one. */
    private function sidFor(string $subject): ?int
    {
        $wildcard = null;
        foreach ($this->subscriptions as $sid => $pattern) {
            if ($pattern === $subject) {
                return $sid;
            }

            if ($wildcard === null && self::matches($pattern, $subject)) {
                $wildcard = $sid;
            }
        }

        return $wildcard;
    }

    /** NATS subject matching: `*` matches one token, a trailing `>` one or more. */
    private static function matches(string $pattern, string $subject): bool
    {
        $patternTokens = explode('.', $pattern);
        $subjectTokens = explode('.', $subject);
        foreach ($patternTokens as $index => $token) {
            if ($token === '>') {
                return count($subjectTokens) > $index;
            }

            if (!isset($subjectTokens[$index]) || ($token !== '*' && $token !== $subjectTokens[$index])) {
                return false;
            }
        }

        return count($subjectTokens) === count($patternTokens);
    }
}
