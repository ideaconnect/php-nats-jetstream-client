<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\JetStream\Configuration\ConsumerConfiguration;
use IDCT\NATS\JetStream\Configuration\StreamConfiguration;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A JetStream API call that nothing answers - a server without JetStream, or an account without it, has no responder
 * for `$JS.API.>`, and the server answers the request with status 503 at once - fails with JetStreamException(503), as
 * publish() and fetchBatch() do (#161). The calls that go through JetStreamContext::requestJson() let the connection's
 * bare NatsException ("No responders for subject ...") through instead, so a caller catching JetStreamException, as
 * the #161 rule says every JetStream path lets it, missed it. Found while telling a pull's 503 apart for
 * ideaconnect/symfony-nats-messenger#106: a CONSUMER.INFO asked of a server without JetStream threw the bare exception.
 *
 * The scripted server answers every request on the client's request inbox with a 503 status and no body, as
 * nats-server answers a request whose subject has no subscriber.
 */
final class JetStreamApiNoRespondersTest extends TestCase
{
    use OwnsTestResources;

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /**
     * @return iterable<string, array{\Closure(JetStreamContext): Future<mixed>}>
     */
    public static function apiCalls(): iterable
    {
        yield 'accountInfo' => [static fn(JetStreamContext $js): Future => $js->accountInfo()];
        yield 'createStream' => [static fn(JetStreamContext $js): Future => $js->createStream('ORDERS', ['orders.>'])];
        yield 'addStream' => [static fn(JetStreamContext $js): Future => $js->addStream(StreamConfiguration::create('ORDERS')->subjects('orders.>'))];
        yield 'updateStream' => [static fn(JetStreamContext $js): Future => $js->updateStream('ORDERS', ['subjects' => ['orders.>']])];
        yield 'getStream' => [static fn(JetStreamContext $js): Future => $js->getStream('ORDERS')];
        yield 'deleteStream' => [static fn(JetStreamContext $js): Future => $js->deleteStream('ORDERS')];
        yield 'purgeStream' => [static fn(JetStreamContext $js): Future => $js->purgeStream('ORDERS')];
        yield 'streamNames' => [static fn(JetStreamContext $js): Future => $js->streamNames()];
        yield 'listStreams' => [static fn(JetStreamContext $js): Future => $js->listStreams()];
        yield 'listConsumers' => [static fn(JetStreamContext $js): Future => $js->listConsumers('ORDERS')];
        yield 'consumerNames' => [static fn(JetStreamContext $js): Future => $js->consumerNames('ORDERS')];
        yield 'getStreamMessage' => [static fn(JetStreamContext $js): Future => $js->getStreamMessage('ORDERS', 1)];
        yield 'getLastMessageForSubject' => [static fn(JetStreamContext $js): Future => $js->getLastMessageForSubject('ORDERS', 'orders.created')];
        yield 'deleteMessage' => [static fn(JetStreamContext $js): Future => $js->deleteMessage('ORDERS', 1)];
        yield 'createConsumer' => [static fn(JetStreamContext $js): Future => $js->createConsumer('ORDERS', 'worker')];
        yield 'addConsumer' => [static fn(JetStreamContext $js): Future => $js->addConsumer('ORDERS', ConsumerConfiguration::create()->durable('worker'))];
        yield 'createEphemeralConsumer' => [static fn(JetStreamContext $js): Future => $js->createEphemeralConsumer('ORDERS')];
        yield 'createPushConsumer' => [static fn(JetStreamContext $js): Future => $js->createPushConsumer('ORDERS', 'pusher', 'deliver.orders')];
        yield 'createEphemeralPushConsumer' => [static fn(JetStreamContext $js): Future => $js->createEphemeralPushConsumer('ORDERS', 'deliver.orders')];
        yield 'getConsumer' => [static fn(JetStreamContext $js): Future => $js->getConsumer('ORDERS', 'worker')];
        yield 'deleteConsumer' => [static fn(JetStreamContext $js): Future => $js->deleteConsumer('ORDERS', 'worker')];
        yield 'pauseConsumer' => [static fn(JetStreamContext $js): Future => $js->pauseConsumer('ORDERS', 'worker', '2030-01-01T00:00:00Z')];
        yield 'resumeConsumer' => [static fn(JetStreamContext $js): Future => $js->resumeConsumer('ORDERS', 'worker')];
        yield 'unpinConsumer' => [static fn(JetStreamContext $js): Future => $js->unpinConsumer('ORDERS', 'worker', 'jobs')];
    }

    /**
     * Each call fails with JetStreamException(503) naming the API subject nothing answered, with the connection's
     * no-responders error as its previous one. Before the fix each threw that NatsException itself.
     *
     * @param \Closure(JetStreamContext): Future<mixed> $call
     */
    #[DataProvider('apiCalls')]
    public function testAJetStreamApiCallNothingAnswersFailsWithAJetStreamException503(\Closure $call): void
    {
        $transport = self::serverWithoutJetStream();
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport));
        $client->connect()->await();

        try {
            $call($client->jetStream())->await();
            self::fail('expected the call to fail');
        } catch (NatsException $failure) {
        }

        self::assertInstanceOf(JetStreamException::class, $failure, $failure::class . ': ' . $failure->getMessage());
        self::assertSame(503, $failure->getCode());
        self::assertStringStartsWith('No JetStream responder for subject $JS.API.', $failure->getMessage());
        $previous = $failure->getPrevious();
        self::assertInstanceOf(NatsException::class, $previous);
        self::assertStringContainsString('No responders', $previous->getMessage());
    }

    /**
     * A server that answers every request written to the client's request inbox ("PUB <subject> <base>.<token>") with
     * status 503 and no body, on the inbox's sid, as nats-server answers a request that has no responder.
     */
    private static function serverWithoutJetStream(): FakeTransport
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $base = null;
        $sid = 1;
        $transport->onWrite = static function (string $bytes) use (&$base, &$sid): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            $parts = explode(' ', $head);
            $subject = $parts[1] ?? '';
            if ($parts[0] === 'SUB' && str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                $base = substr($subject, 0, -1);
                $sid = (int) ($parts[2] ?? 1);

                return [];
            }

            $replyTo = $parts[2] ?? '';
            if ($base === null || ($parts[0] !== 'PUB' && $parts[0] !== 'HPUB') || !str_starts_with($replyTo, $base)) {
                return [];
            }

            $status = "NATS/1.0 503\r\n\r\n";

            return [sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $sid, strlen($status), strlen($status), $status)];
        };

        return $transport;
    }
}
