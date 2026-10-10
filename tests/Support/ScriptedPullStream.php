<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

/**
 * A scripted JetStream stream behind one pull consumer, on a {@see ReconnectingTransport} (#212): each pull is served
 * what is stored, up to its batch, and waits for the rest; a message published while a pull waits goes to it only while
 * its reply subject has interest (the client holds a subscription for it on the live session), as nats-server drops a
 * waiting pull whose reply subject has none, and stays stored otherwise. Install it with {@see serve()}, which also
 * confirms each +ACK request on its reply subject.
 */
final class ScriptedPullStream
{
    /** @var list<array{replyTo: string, left: int}> The pulls waiting for the rest of their batch, oldest first. */
    public array $waiting = [];

    /**
     * @param list<string> $stored The messages stored and not delivered yet, oldest first.
     */
    public function __construct(
        private readonly ReconnectingTransport $transport,
        private readonly string $pullSubject,
        private readonly string $ackSubject,
        public array $stored = [],
    ) {}

    /** Makes the transport's responder this stream's server. */
    public function serve(): void
    {
        $this->transport->responder = function (string $subject, ?string $replyTo, string $payload): array {
            if ($replyTo !== null && str_starts_with($subject, '$JS.ACK.')) {
                return $this->transport->replyFrame($replyTo, '');
            }
            if ($replyTo === null || $subject !== $this->pullSubject || $this->transport->sidFor($replyTo) === null) {
                return [];
            }

            $request = json_decode($payload, true, 8, JSON_THROW_ON_ERROR);
            $batch = is_array($request) && is_int($request['batch'] ?? null) ? $request['batch'] : 1;

            return $this->pull($replyTo, $batch);
        };
    }

    /** Publishes $payload: to the oldest waiting pull whose inbox has interest, or into the stream. */
    public function publish(string $payload): void
    {
        $waiting = $this->waiting;
        $this->waiting = [];
        $delivered = false;
        foreach ($waiting as $pull) {
            $sid = $this->transport->sidFor($pull['replyTo']);
            if ($sid === null) {
                // No interest: the server drops the waiting pull.
                continue;
            }

            if (!$delivered) {
                $delivered = true;
                $this->transport->pushFrame(ReconnectingTransport::msgFrame('evt.s', $sid, $payload, $this->ackSubject));
                if (--$pull['left'] === 0) {
                    continue;
                }
            }

            $this->waiting[] = $pull;
        }

        if (!$delivered) {
            $this->stored[] = $payload;
        }
    }

    /** @return list<string> What the pull to $replyTo is served now. */
    private function pull(string $replyTo, int $batch): array
    {
        $frames = '';
        $sid = (int) $this->transport->sidFor($replyTo);
        while ($batch > 0 && $this->stored !== []) {
            $frames .= ReconnectingTransport::msgFrame('evt.s', $sid, array_shift($this->stored), $this->ackSubject);
            --$batch;
        }
        if ($batch > 0) {
            $this->waiting[] = ['replyTo' => $replyTo, 'left' => $batch];
        }

        return $frames === '' ? [] : [$frames];
    }
}
