<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

/**
 * A scripted fast-ingest (ADR-50) server on a {@see FakeTransport}: it records the batch control inbox the client
 * subscribes (`SUB <inbox>.> <sid>`), its UNSUB, and every message published with a `$FI` reply subject, and answers
 * each such message with what the test's responder returns, on the message's own reply subject and the inbox's sid.
 * A responder entry is a JSON body, or `STATUS <code>` for a status-only reply (a 503 has no body). Replies can also
 * be pushed later with {@see reply()}, so that a test holds an ack back until it has looked at the client.
 *
 * Attach it after connect(): it answers every PING with a PONG (the inbox subscription's PING included).
 */
final class ScriptedFastIngestServer
{
    /** The subject of the control inbox subscription (`<inbox>.>`), once one was written. */
    public ?string $subscription = null;

    public ?int $sid = null;

    /** How many `SUB <inbox>.>` the client wrote. */
    public int $subscribes = 0;

    /** @var list<string> The sids of every UNSUB the client wrote. */
    public array $unsubscribes = [];

    /** When set, the control inbox SUB is answered with this `-ERR` (before the PONG of the PING behind it). */
    public ?string $rejectSubscriptionWith = null;

    /**
     * @var list<array{subject:string,reply:string,flow:int,gap:string,seq:int,op:int,headers:string,payload:string}>
     *      Every message with a `$FI` reply subject, in write order.
     */
    public array $messages = [];

    /**
     * @param \Closure(array{subject:string,reply:string,flow:int,gap:string,seq:int,op:int,headers:string,payload:string}): list<string> $respond
     */
    public function __construct(private readonly FakeTransport $transport, private \Closure $respond)
    {
        $transport->onWrite = $this->onWrite(...);
    }

    /**
     * @param \Closure(array{subject:string,reply:string,flow:int,gap:string,seq:int,op:int,headers:string,payload:string}): list<string> $respond
     */
    public function respondWith(\Closure $respond): void
    {
        $this->respond = $respond;
    }

    /** Pushes one reply on $replySubject now: a JSON body, or `STATUS <code>`. */
    public function reply(string $replySubject, string $body): void
    {
        $this->transport->pushReadChunk($this->frame($replySubject, $body));
    }

    /**
     * Pushes several replies on $replySubject now, in one read chunk, so that one read delivers them all.
     *
     * @param list<string> $bodies
     */
    public function replyInOneChunk(string $replySubject, array $bodies): void
    {
        $this->transport->pushReadChunk(implode('', array_map(fn(string $body): string => $this->frame($replySubject, $body), $bodies)));
    }

    /** The reply subject the client would use for $seq and $op, from the first message's reply subject. */
    public function replySubject(int $seq, int $op): string
    {
        $first = $this->messages[0] ?? null;
        if ($first === null) {
            throw new \LogicException('no fast-ingest message was written yet');
        }

        $tokens = explode('.', $first['reply']);
        $count = count($tokens);
        $tokens[$count - 3] = (string) $seq;
        $tokens[$count - 2] = (string) $op;

        return implode('.', $tokens);
    }

    /**
     * The `<seq>.<op>` pairs of the messages written, in order: `1.0`, `2.1`, ... (`n.4` is a ping).
     *
     * @return list<string>
     */
    public function sequenceAndOperations(): array
    {
        return array_map(static fn(array $m): string => $m['seq'] . '.' . $m['op'], $this->messages);
    }

    /**
     * @return list<string>
     */
    private function onWrite(string $bytes): array
    {
        $out = [];
        $offset = 0;
        $length = strlen($bytes);
        while ($offset < $length) {
            $eol = strpos($bytes, "\r\n", $offset);
            if ($eol === false) {
                break;
            }

            $parts = explode(' ', substr($bytes, $offset, $eol - $offset));
            $offset = $eol + 2;

            switch ($parts[0]) {
                case 'PING':
                    $out[] = "PONG\r\n";
                    break;
                case 'SUB':
                    if (str_ends_with($parts[1] ?? '', '.>')) {
                        $this->subscription = $parts[1];
                        $this->sid = (int) $parts[count($parts) - 1];
                        ++$this->subscribes;
                        if ($this->rejectSubscriptionWith !== null) {
                            $out[] = sprintf("-ERR '%s'\r\n", str_replace('{subject}', $parts[1], $this->rejectSubscriptionWith));
                        }
                    }
                    break;
                case 'UNSUB':
                    $this->unsubscribes[] = $parts[1] ?? '';
                    break;
                case 'PUB':
                    $size = (int) $parts[count($parts) - 1];
                    $payload = substr($bytes, $offset, $size);
                    $offset += $size + 2;
                    $reply = count($parts) === 4 ? $parts[2] : null;
                    array_push($out, ...$this->onMessage($parts[1], $reply, '', $payload));
                    break;
                case 'HPUB':
                    $total = (int) $parts[count($parts) - 1];
                    $headerSize = (int) $parts[count($parts) - 2];
                    $block = substr($bytes, $offset, $total);
                    $offset += $total + 2;
                    $reply = count($parts) === 5 ? $parts[2] : null;
                    array_push($out, ...$this->onMessage($parts[1], $reply, substr($block, 0, $headerSize), substr($block, $headerSize)));
                    break;
            }
        }

        return $out;
    }

    /**
     * @return list<string>
     */
    private function onMessage(string $subject, ?string $reply, string $headers, string $payload): array
    {
        if ($reply === null || !str_ends_with($reply, '.$FI')) {
            return [];
        }

        $tokens = explode('.', $reply);
        $count = count($tokens);
        $message = [
            'subject' => $subject,
            'reply' => $reply,
            'flow' => (int) $tokens[$count - 5],
            'gap' => $tokens[$count - 4],
            'seq' => (int) $tokens[$count - 3],
            'op' => (int) $tokens[$count - 2],
            'headers' => $headers,
            'payload' => $payload,
        ];
        $this->messages[] = $message;

        return array_map(fn(string $body): string => $this->frame($reply, $body), ($this->respond)($message));
    }

    private function frame(string $replySubject, string $body): string
    {
        $sid = $this->sid ?? 0;
        if (str_starts_with($body, 'STATUS ')) {
            $header = 'NATS/1.0 ' . substr($body, 7) . "\r\n\r\n";

            return sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replySubject, $sid, strlen($header), strlen($header), $header);
        }

        return sprintf("MSG %s %d %d\r\n%s\r\n", $replySubject, $sid, strlen($body), $body);
    }
}
