<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

/**
 * The operations that read the socket while they wait for a result of their own, each scripted over
 * {@see ReconnectingTransport} so that the read bringing its result also brings frames of the test's choosing:
 * another subscription's overflow, say, or a message whose handler throws.
 */
trait OperationsThatRead
{
    /** @return iterable<string, array{string}> */
    public static function operationsThatRead(): iterable
    {
        yield 'request()' => ['request'];
        yield 'requestMany()' => ['requestMany'];
        yield 'flush()' => ['flush'];
        yield 'rtt()' => ['rtt'];
        yield 'JetStream fetchBatch()' => ['fetchBatch'];
        yield 'JetStream directGetBatch()' => ['directGetBatch'];
        yield 'JetStream pull consumer' => ['pullConsumer'];
        yield 'Key/Value keys()' => ['keys'];
        yield 'Key/Value history()' => ['history'];
        yield 'SubscriptionQueue::fetch()' => ['queueFetch'];
        yield 'SubscriptionQueue::next()' => ['queueNext'];
        yield 'SubscriptionQueue::next() with a timeout' => ['queueNextWithTimeout'];
        yield 'SubscriptionQueue::fetchAll()' => ['queueFetchAll'];
    }

    /**
     * Scripts the server so that the read bringing $operation its result also brings $frames, ahead of the
     * result, and returns the operation - run to its result - with the result expected.
     *
     * @return array{\Closure(): mixed, mixed}
     */
    private function prepareOperation(string $operation, NatsClient $client, ReconnectingTransport $transport, string $frames): array
    {
        switch ($operation) {
            case 'request':
            case 'requestMany':
                $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc' && $replyTo !== null
                    ? [$frames . implode('', $transport->replyFrame($replyTo, 'pong'))]
                    : [];

                return $operation === 'request'
                    ? [static fn(): string => $client->request('svc', 'ping')->await()->payload, 'pong']
                    : [static fn(): array => self::payloads($client->requestMany('svc', 'ping', maxResponses: 1)->await()), ['pong']];
            case 'flush':
            case 'rtt':
                $transport->answerPings = false;
                $transport->afterWrite = static function (string $bytes) use ($transport, $frames): void {
                    if ($bytes === "PING\r\n") {
                        $transport->afterWrite = null;
                        $transport->pushFrame($frames . "PONG\r\n");
                    }
                };

                return [static function () use ($client, $operation): string {
                    if ($operation === 'flush') {
                        $client->flush()->await();
                    } else {
                        $client->rtt()->await();
                    }

                    return 'answered';
                }, 'answered'];
            case 'fetchBatch':
            case 'directGetBatch':
            case 'pullConsumer':
                $request = match ($operation) {
                    'fetchBatch' => '$JS.API.CONSUMER.MSG.NEXT.ORDERS.worker',
                    'directGetBatch' => '$JS.API.DIRECT.GET.ORDERS',
                    default => '$JS.API.CONSUMER.MSG.NEXT.ORDERS.pipeline',
                };
                $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $frames, $request, $operation): array {
                    $sid = $replyTo === null ? null : $transport->sidFor($replyTo);
                    if ($subject !== $request || $replyTo === null || $sid === null) {
                        return [];
                    }

                    $message = $operation === 'directGetBatch'
                        ? ReconnectingTransport::hmsgFrame($replyTo, $sid, "NATS/1.0\r\nNats-Subject: orders.created\r\nNats-Sequence: 1\r\nNats-Num-Pending: 0\r\n\r\n", 'order-1')
                        : ReconnectingTransport::msgFrame('orders.created', $sid, 'order-1', '$JS.ACK.ORDERS.worker.1.1.1.0.0');

                    return [$frames . $message];
                };

                return match ($operation) {
                    'fetchBatch' => [static fn(): array => self::payloads($client->jetStream()->fetchBatch('ORDERS', 'worker', 1, 1_000)->await()), ['order-1']],
                    'directGetBatch' => [static fn(): array => self::payloads($client->jetStream()->directGetBatch('ORDERS', ['batch' => 1], 1_000)->await()), ['order-1']],
                    default => [static function () use ($client): array {
                        $received = new class {
                            /** @var list<string> */
                            public array $payloads = [];
                        };
                        $client->jetStream()->pullConsumer('ORDERS', 'pipeline')->setBatching(1)->setIterations(1)
                            ->handle(static function (NatsMessage $message) use ($received): void {
                                $received->payloads[] = $message->payload;
                            })->await();

                        return $received->payloads;
                    }, ['order-1']],
                };
            case 'keys':
            case 'history':
                $consumer = $operation === 'keys' ? 'KEYS' : 'HIST';
                $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => str_starts_with($subject, '$JS.API.CONSUMER.CREATE.') && $replyTo !== null
                    ? $transport->replyFrame($replyTo, sprintf('{"stream_name":"KV_cfg","name":"%s","num_pending":1,"config":{"ack_policy":"none"}}', $consumer))
                    : [];
                // The replayed record, with the overflow, once the consumer's deliver subscription is there.
                $transport->afterWrite = static function (string $bytes) use ($transport, $frames, $operation, $consumer): void {
                    if (preg_match('/^SUB _INBOX\.KV\.\S+ (\d+)\r\n$/', $bytes, $sub) !== 1) {
                        return;
                    }

                    $transport->afterWrite = null;
                    $ack = '$JS.ACK.KV_cfg.' . $consumer . '.1.4.1.0.0';
                    $record = $operation === 'keys'
                        ? ReconnectingTransport::hmsgFrame('$KV.cfg.email', (int) $sub[1], "NATS/1.0\r\nNats-Sequence: 4\r\n\r\n", '', $ack)
                        : ReconnectingTransport::msgFrame('$KV.cfg.theme', (int) $sub[1], 'blue', $ack);
                    $transport->pushFrame($frames . $record);
                };

                return $operation === 'keys'
                    ? [static fn(): array => $client->jetStream()->keyValue('cfg')->keys()->await(), ['email']]
                    : [static fn(): array => array_map(
                        static fn($entry): ?string => $entry->value,
                        $client->jetStream()->keyValue('cfg')->history('theme')->await(),
                    ), ['blue']];
            default:
                $queue = $client->subscribeQueue('jobs')->await();
                $transport->pushFrame($frames . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j1'));

                return match ($operation) {
                    'queueFetch' => [static fn(): ?string => $queue->fetch()?->payload, 'j1'],
                    'queueNext' => [static fn(): ?string => $queue->next()?->payload, 'j1'],
                    'queueNextWithTimeout' => [static fn(): ?string => $queue->setTimeout(1.0)->next()?->payload, 'j1'],
                    default => [static fn(): array => self::payloads($queue->fetchAll(1)), ['j1']],
                };
        }
    }

    /**
     * @param list<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_map(static fn(NatsMessage $message): string => $message->payload, $messages);
    }
}
