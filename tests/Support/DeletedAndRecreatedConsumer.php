<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

/**
 * The scripted server of #211's probe, on a {@see ReconnectingTransport}: a pull consumer whose consumer is deleted and
 * recreated under the same name while the handler works. Pull 1 is answered with m-1 and pull 2 is held, waiting;
 * {@see deleteTheConsumer()}, which the handler calls on m-1, has the server answer pull 2 with "409 Consumer Deleted",
 * queued on the socket and unread; the refill, pull 3, reaches the recreated consumer, which answers it at once with the
 * refill's messages: in the same read as the 409 (one-read timing), or in a read of its own after it. Each +ACK request
 * is confirmed. Install it with {@see serve()}.
 */
final class DeletedAndRecreatedConsumer
{
    /** The sid the client subscribed the run's inbox with. */
    public ?int $sid = null;

    /** The run's inbox, without the ".<token>" each pull's reply subject adds. */
    public ?string $base = null;

    /** @var list<string> Each pull's reply subject, in order. */
    public array $replyTos = [];

    /** @var list<int> The epoch (session) each pull came on, in order. */
    public array $epochs = [];

    /** Pull 2's 409, held for the refill's chunk in the one-read timing. */
    private string $deletedNotice = '';

    /**
     * @param list<string> $refill The payloads the recreated consumer answers the refill with.
     */
    public function __construct(
        private readonly ReconnectingTransport $transport,
        private readonly string $pullSubject,
        private readonly string $ackSubject,
        private readonly bool $oneRead,
        private readonly array $refill,
    ) {}

    /** Makes the transport's responder this server. */
    public function serve(): void
    {
        $this->transport->responder = function (string $subject, ?string $replyTo): array {
            if ($replyTo !== null && str_starts_with($subject, '$JS.ACK.')) {
                return $this->transport->replyFrame($replyTo, '');
            }
            if ($replyTo === null || $subject !== $this->pullSubject) {
                return [];
            }

            $this->replyTos[] = $replyTo;
            $this->epochs[] = $this->transport->epoch();
            $this->sid ??= $this->transport->sidFor($replyTo);
            $this->base ??= substr($replyTo, 0, (int) strrpos($replyTo, '.'));

            return match (count($this->replyTos)) {
                1 => [$this->messages(['m-1'])],
                3 => [$this->deletedNotice . $this->messages($this->refill)],
                default => [],
            };
        };
    }

    /** The consumer is deleted while the handler works: the server answers the waiting pull 2 with a 409. */
    public function deleteTheConsumer(): void
    {
        $notice = ReconnectingTransport::hmsgFrame($this->replyTos[1], (int) $this->sid, "NATS/1.0 409 Consumer Deleted\r\n\r\n", '');
        if ($this->oneRead) {
            $this->deletedNotice = $notice;
        } else {
            $this->transport->pushFrame($notice);
        }
    }

    /** @param list<string> $payloads */
    private function messages(array $payloads): string
    {
        $chunk = '';
        foreach ($payloads as $payload) {
            $chunk .= ReconnectingTransport::msgFrame('evt.s', (int) $this->sid, $payload, $this->ackSubject);
        }

        return $chunk;
    }
}
