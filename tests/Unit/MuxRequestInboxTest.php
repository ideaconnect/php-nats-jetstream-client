<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * The muxed request inbox (#118): request()/requestMany() share ONE long-lived wildcard subscription
 * "<base>.*" and route replies by the per-request suffix token, instead of a fresh SUB/UNSUB per
 * request. These tests drive the mux end to end via a dynamic reply-responder that echoes each
 * request's captured reply-to on the mux sid.
 */
final class MuxRequestInboxTest extends TestCase
{
    /** INFO+PONG so connect() succeeds; the mux SUB is written lazily on the first request. */
    private function connect(FakeTransport $transport): NatsConnection
    {
        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        return $connection;
    }

    /**
     * Installs a responder that, for every `PUB <subject> <replyTo> ...` written, enqueues one reply
     * `MSG <replyTo> <muxSid> ...`. The mux sid is 1 (the first subscription after connect is the mux
     * wildcard). This mirrors what a real server does: deliver to the request's reply-to on the mux sub.
     */
    private function replyTo(FakeTransport $transport, string $subject, string $payload, int $muxSid = 1): void
    {
        $transport->onWrite = static function (string $bytes) use ($subject, $payload, $muxSid): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false || !str_starts_with($head, 'PUB ' . $subject . ' ')) {
                return [];
            }

            // PUB <subject> <replyTo> <len>
            $parts = explode(' ', $head);
            $replyTo = $parts[2] ?? '';
            if ($replyTo === '') {
                return [];
            }

            return [sprintf("MSG %s %d %d\r\n%s\r\n", $replyTo, $muxSid, strlen($payload), $payload)];
        };
    }

    public function testRequestRoutesReplyThroughMuxInbox(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->replyTo($transport, 'svc.echo', 'hello');
        $connection = $this->connect($transport);

        $response = $connection->request('svc.echo', '{"x":1}', 500)->await();

        self::assertSame('hello', $response->payload);

        // Exactly ONE wildcard mux SUB, and ZERO per-request UNSUB (the whole point of #118).
        $subs = array_values(array_filter($transport->writes, static fn (string $w): bool => str_starts_with($w, 'SUB ')));
        $unsubs = array_filter($transport->writes, static fn (string $w): bool => str_starts_with($w, 'UNSUB '));
        self::assertCount(1, $subs);
        self::assertStringMatchesFormat("SUB _INBOX.%s.* 1\r\n", $subs[0]);
        self::assertCount(0, $unsubs);
        // The request published its reply-to under the mux base's wildcard.
        self::assertStringStartsWith('PUB svc.echo _INBOX.', $transport->writes[3]);
    }

    public function testManyRequestsShareOneMuxSubscriptionWithNoUnsub(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->replyTo($transport, 'svc.echo', 'ok');
        $connection = $this->connect($transport);

        for ($i = 0; $i < 5; $i++) {
            self::assertSame('ok', $connection->request('svc.echo', (string) $i, 500)->await()->payload);
        }

        // 5 requests -> still exactly one SUB and zero UNSUB (pre-#118 this was 5 SUB + 5 UNSUB).
        $subs = array_filter($transport->writes, static fn (string $w): bool => str_starts_with($w, 'SUB '));
        $unsubs = array_filter($transport->writes, static fn (string $w): bool => str_starts_with($w, 'UNSUB '));
        self::assertCount(1, $subs);
        self::assertCount(0, $unsubs);
    }

    public function testDistinctRequestsGetDistinctReplyTokens(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->replyTo($transport, 'svc.echo', 'r');
        $connection = $this->connect($transport);

        $connection->request('svc.echo', 'a', 500)->await();
        $connection->request('svc.echo', 'b', 500)->await();

        // The two reply-to subjects (3rd token of each PUB) must differ - distinct per-request tokens.
        $replyTos = [];
        foreach ($transport->writes as $w) {
            if (str_starts_with($w, 'PUB svc.echo ')) {
                $replyTos[] = explode(' ', strtok($w, "\r\n") ?: '')[2] ?? '';
            }
        }
        self::assertCount(2, $replyTos);
        self::assertNotSame($replyTos[0], $replyTos[1]);
    }

    /** After a request completes, its waiter is removed - the finally cleanup that prevents leaks/late-leak. */
    public function testRequestRemovesItsWaiterAfterCompletion(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->replyTo($transport, 'svc.echo', 'ok');
        $connection = $this->connect($transport);

        $connection->request('svc.echo', 'x', 500)->await();

        $waiters = new \ReflectionProperty($connection, 'muxWaiters');
        self::assertSame([], $waiters->getValue($connection));
    }

    /** After requestMany completes, its waiter is removed (the requestMany finally cleanup). */
    public function testRequestManyRemovesItsWaiterAfterCompletion(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->replyTo($transport, 'svc.scan', 'r');
        $connection = $this->connect($transport);

        // maxResponses 1 so it completes as soon as the single reply lands.
        $messages = $connection->requestMany('svc.scan', 'x', null, 1, 500)->await();
        self::assertCount(1, $messages);

        $waiters = new \ReflectionProperty($connection, 'muxWaiters');
        self::assertSame([], $waiters->getValue($connection));
    }
}
