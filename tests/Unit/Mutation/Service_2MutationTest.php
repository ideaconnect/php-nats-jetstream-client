<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\Service;
use IDCT\NATS\Services\ServiceEndpoint;
use IDCT\NATS\Services\ServiceError;
use IDCT\NATS\Tests\Support\FakeTransport;

use function Amp\async;

/**
 * Targeted tests that kill Infection survivors in src/Services/Service.php (chunk 2).
 *
 * Each test pins the EXACT observable behavior a specific mutation would change: the precise SUB
 * subject bytes written for discovery subscriptions, the exact keys/values of the stats / discovery
 * payloads, the observer-context correlation precedence, the error-header collapsing, the reply
 * de-duplication, and the run-loop break boundary. All async work is driven by pumping
 * processIncoming() against pre-seeded frames — no sleeps, no real sockets.
 */
final class Service_2MutationTest extends \PHPUnit\Framework\TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function connectedClient(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    private function serviceId(Service $service): string
    {
        $id = (new \ReflectionProperty($service, 'id'))->getValue($service);
        self::assertIsString($id);

        return $id;
    }

    // ---------------------------------------------------------------------------------------------
    // Lines 580-590: discovery subject construction (ConcatOperandRemoval). Each discovery subject is
    // interpolated from the literal "$SRV.<verb>[.<name>[.<id>]]". The exact SUB control line written
    // pins every concat operand: dropping a literal/operand changes the subject string verbatim.
    // ---------------------------------------------------------------------------------------------

    public function testDiscoverySubjectsAreSubscribedVerbatim(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->connectedClient($transport);

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        $id = $this->serviceId($service);
        $writes = implode('', $transport->writes);

        // Discovery subjects subscribe with no queue group -> "SUB <subject> <sid>\r\n". SIDs are
        // assigned in array order starting at 1; endpoint subscriptions start at 13.
        // kills ConcatOperandRemoval @ line 580 (name) and @ line 581 (both .id operands)
        self::assertStringContainsString("SUB \$SRV.PING.echo 2\r\n", $writes);
        self::assertStringContainsString("SUB \$SRV.PING.echo.{$id} 3\r\n", $writes);
        // kills ConcatOperandRemoval @ line 583 (name) and @ line 584 (both .id operands)
        self::assertStringContainsString("SUB \$SRV.INFO.echo 5\r\n", $writes);
        self::assertStringContainsString("SUB \$SRV.INFO.echo.{$id} 6\r\n", $writes);
        // kills ConcatOperandRemoval @ line 586 (name) and @ line 587 (both .id operands)
        self::assertStringContainsString("SUB \$SRV.STATS.echo 8\r\n", $writes);
        self::assertStringContainsString("SUB \$SRV.STATS.echo.{$id} 9\r\n", $writes);
        // kills ConcatOperandRemoval @ line 589 (name) and @ line 590 (both .id operands)
        self::assertStringContainsString("SUB \$SRV.SCHEMA.echo 11\r\n", $writes);
        self::assertStringContainsString("SUB \$SRV.SCHEMA.echo.{$id} 12\r\n", $writes);
    }

    // ---------------------------------------------------------------------------------------------
    // Lines 532-535: statsSnapshot() per-endpoint entry keys (ArrayItemRemoval, ArrayItem '>').
    // ---------------------------------------------------------------------------------------------

    public function testStatsEndpointEntryCarriesNameAndQueueGroup(): void
    {
        $client = $this->connectedClient(new FakeTransport($this->infoAndPong()));

        $service = $client->service('svc', '1.0.0')
            ->addEndpoint('worker', 'svc.work', static fn(NatsMessage $m): string => 'ok', 'qg.work');

        $entry = $service->statsSnapshot()['endpoints'][0] ?? [];

        // kills ArrayItemRemoval @ line 532 and ArrayItem '>' @ line 533 (name key)
        self::assertArrayHasKey('name', $entry);
        self::assertSame('worker', $entry['name']);
        // kills ArrayItem '>' @ line 535 (queue_group key)
        self::assertArrayHasKey('queue_group', $entry);
        self::assertSame('qg.work', $entry['queue_group']);
        // Sanity: the surrounding subject key is intact (entry shape preserved).
        self::assertSame('svc.work', $entry['subject'] ?? null);
    }

    // ---------------------------------------------------------------------------------------------
    // Lines 558-563: statsSnapshot() top-level response keys (ArrayItem '>').
    // ---------------------------------------------------------------------------------------------

    public function testStatsResponseTopLevelKeysArePresent(): void
    {
        $client = $this->connectedClient(new FakeTransport($this->infoAndPong()));

        $service = $client->service('svc', '1.0.0', null, ['team' => 'core'])
            ->addEndpoint('worker', 'svc.work', static fn(NatsMessage $m): string => 'ok');

        $stats = $service->statsSnapshot();
        $id = $this->serviceId($service);

        // kills ArrayItem '>' @ line 558 (name)
        self::assertSame('svc', $stats['name'] ?? null);
        // kills ArrayItem '>' @ line 559 (id)
        self::assertSame($id, $stats['id'] ?? null);
        // kills ArrayItem '>' @ line 560 (version)
        self::assertSame('1.0.0', $stats['version'] ?? null);
        // kills ArrayItem '>' @ line 561 (started)
        self::assertArrayHasKey('started', $stats);
        self::assertIsString($stats['started']);
        self::assertNotSame('', $stats['started']);
        // kills ArrayItem '>' @ line 563 (metadata)
        self::assertSame(['team' => 'core'], $stats['metadata'] ?? null);
    }

    // ---------------------------------------------------------------------------------------------
    // Lines 714-718: discoveryPayloadForSubject() base array (ArrayItemRemoval, ArrayItem '>').
    // Driven via a $SRV.PING request whose ping_response is built from the base array ONLY.
    // ---------------------------------------------------------------------------------------------

    public function testPingResponseBaseCarriesNameIdVersionMetadata(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG \$SRV.PING 1 _INBOX.ping 0\r\n\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('echo', '1.0.0', null, ['region' => 'eu'])
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        $client->processIncoming()->await();

        $id = $this->serviceId($service);
        $payload = $this->decodePublishedJson($transport, '_INBOX.ping');

        self::assertSame('io.nats.micro.v1.ping_response', $payload['type'] ?? null);
        // kills ArrayItemRemoval @ line 714 and ArrayItem '>' @ line 715 (name)
        self::assertSame('echo', $payload['name'] ?? null);
        // kills ArrayItem '>' @ line 716 (id)
        self::assertSame($id, $payload['id'] ?? null);
        // kills ArrayItem '>' @ line 717 (version)
        self::assertSame('1.0.0', $payload['version'] ?? null);
        // kills ArrayItem '>' @ line 718 (metadata)
        self::assertSame(['region' => 'eu'], $payload['metadata'] ?? null);
    }

    // ---------------------------------------------------------------------------------------------
    // Lines 724-726: SCHEMA-response endpoint entry keys (ArrayItemRemoval, ArrayItem '>').
    // ---------------------------------------------------------------------------------------------

    public function testSchemaResponseEndpointEntryCarriesNameAndSubject(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG \$SRV.SCHEMA.echo 11 _INBOX.schema 0\r\n\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('worker', 'svc.work', static fn(NatsMessage $m): string => 'ok');
        $service->start()->await();

        $client->processIncoming()->await();

        $payload = $this->decodePublishedJson($transport, '_INBOX.schema');
        self::assertSame('io.nats.micro.v1.schema_response', $payload['type'] ?? null);
        $entry = $payload['endpoints'][0] ?? [];

        // kills ArrayItemRemoval @ line 724 and ArrayItem '>' @ line 725 (name)
        self::assertSame('worker', $entry['name'] ?? null);
        // kills ArrayItem '>' @ line 726 (subject)
        self::assertSame('svc.work', $entry['subject'] ?? null);
    }

    // ---------------------------------------------------------------------------------------------
    // Line 747: INFO-response endpoint entry name key (ArrayItemRemoval).
    // ---------------------------------------------------------------------------------------------

    public function testInfoResponseEndpointEntryCarriesName(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG \$SRV.INFO.echo 5 _INBOX.info 0\r\n\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('worker', 'svc.work', static fn(NatsMessage $m): string => 'ok');
        $service->start()->await();

        $client->processIncoming()->await();

        $payload = $this->decodePublishedJson($transport, '_INBOX.info');
        self::assertSame('io.nats.micro.v1.info_response', $payload['type'] ?? null);
        $entry = $payload['endpoints'][0] ?? [];

        // kills ArrayItemRemoval @ line 747 (name)
        self::assertArrayHasKey('name', $entry);
        self::assertSame('worker', $entry['name']);
        // Sanity: the surrounding subject key survives.
        self::assertSame('svc.work', $entry['subject'] ?? null);
    }

    // ---------------------------------------------------------------------------------------------
    // Lines 685-689: buildObserverContext() keys and correlation-id precedence.
    // ---------------------------------------------------------------------------------------------

    public function testObserverContextCarriesSubjectAndReplyTo(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $captured = null;
        $service = $client->service('echo', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$captured): void {
                if ($event === 'request_start') {
                    $captured = $context;
                }
            })
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        $client->processIncoming()->await();

        self::assertIsArray($captured);
        // kills ArrayItemRemoval @ line 685 (subject)
        self::assertArrayHasKey('subject', $captured);
        self::assertSame('svc.echo', $captured['subject']);
        // kills ArrayItem '>' @ line 687 (reply_to)
        self::assertArrayHasKey('reply_to', $captured);
        self::assertSame('_INBOX.req', $captured['reply_to']);
    }

    public function testCorrelationIdPrefersRequestIdOverTraceparent(): void
    {
        // kills Coalesce @ line 688
        // x-request-id MUST win over traceparent. The reordered coalesce would surface traceparent.
        $header = "NATS/1.0\r\nX-Request-Id:xr-1\r\nTraceparent:tp-1\r\n\r\n";
        $merged = $header . 'hi';
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            'HMSG svc.echo 13 _INBOX.req ' . strlen($header) . ' ' . strlen($merged) . "\r\n{$merged}\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $captured = null;
        $service = $client->service('echo', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$captured): void {
                if ($event === 'request_start') {
                    $captured = $context;
                }
            })
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        $client->processIncoming()->await();

        self::assertIsArray($captured);
        self::assertSame('xr-1', $captured['correlation_id'] ?? null);
    }

    public function testCorrelationIdPrefersTraceparentOverNatsMsgId(): void
    {
        // kills Coalesce @ line 689
        // With no x-request-id, traceparent MUST win over nats-msg-id. The reordered coalesce would
        // surface nats-msg-id instead.
        $header = "NATS/1.0\r\nTraceparent:tp-9\r\nNats-Msg-Id:nm-9\r\n\r\n";
        $merged = $header . 'hi';
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            'HMSG svc.echo 13 _INBOX.req ' . strlen($header) . ' ' . strlen($merged) . "\r\n{$merged}\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $captured = null;
        $service = $client->service('echo', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$captured): void {
                if ($event === 'request_start') {
                    $captured = $context;
                }
            })
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        $client->processIncoming()->await();

        self::assertIsArray($captured);
        self::assertSame('tp-9', $captured['correlation_id'] ?? null);
    }

    // ---------------------------------------------------------------------------------------------
    // Line 666: errorPayload() correlation_id guard (LogicalAnd && -> ||).
    // ---------------------------------------------------------------------------------------------

    public function testErrorPayloadOmitsCorrelationIdWhenAbsent(): void
    {
        // kills LogicalAnd @ line 666
        // With a plain (header-less) request the correlation id is null, so the payload must NOT carry a
        // correlation_id key. The '||' mutant is true for any value (a string is never both null and '')
        // so it would always add "correlation_id":null.
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.echo 13 _INBOX.req 4\r\nboom\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $m): never {
                throw new \RuntimeException('handler exploded');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"code":"HANDLER_ERROR"', $writes);
        self::assertStringNotContainsString('correlation_id', $writes);
    }

    // ---------------------------------------------------------------------------------------------
    // Line 634: publishResponse() early return after error-header publish (ReturnRemoval).
    // ---------------------------------------------------------------------------------------------

    public function testErrorReplyIsPublishedExactlyOnceWithHeaders(): void
    {
        // kills ReturnRemoval @ line 634
        // When error headers are present the reply is published once via HPUB and the method returns.
        // Removing the return falls through to the plain publish() too, double-sending the reply: one
        // HPUB plus one plain PUB to the SAME inbox.
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.echo 13 _INBOX.req 4\r\nboom\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $m): never {
                throw new ServiceError('429', 'Rate limited', '{"retry":1}');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        $hpubReplies = 0;
        $plainPubReplies = 0;
        foreach ($transport->writes as $write) {
            if (str_starts_with($write, 'HPUB _INBOX.req ')) {
                $hpubReplies++;
            }
            if (str_starts_with($write, 'PUB _INBOX.req ')) {
                $plainPubReplies++;
            }
        }

        self::assertSame(1, $hpubReplies);
        // No plain PUB reply to the inbox: the early return prevented the second send.
        self::assertSame(0, $plainPubReplies);
    }

    // ---------------------------------------------------------------------------------------------
    // Line 649: serviceErrorHeaders() collapse-then-trim (Coalesce).
    // ---------------------------------------------------------------------------------------------

    public function testErrorHeaderCollapsesInternalWhitespace(): void
    {
        // kills Coalesce @ line 649
        // The header value must be the collapsed-and-trimmed description: "Rate limited". The mutated
        // coalesce (trim($message ?? ...)) skips the preg_replace and keeps the internal multiple
        // spaces, yielding "Rate   limited". (UnwrapTrim is equivalent here because the downstream
        // toWireBlock() trims the value again.)
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.echo 13 _INBOX.req 2\r\ngo\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $m): never {
                throw new ServiceError('429', '  Rate   limited  ');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        // Real code: collapsed + trimmed single-spaced value on its own header line.
        self::assertStringContainsString('Nats-Service-Error:Rate limited' . "\r\n", $writes);
        // The header value must not retain the un-collapsed multi-space form. (The error PAYLOAD body
        // legitimately keeps the raw description, so scope this to the header line only.)
        self::assertStringNotContainsString('Nats-Service-Error:Rate   limited', $writes);
    }

    // ---------------------------------------------------------------------------------------------
    // Line 470: run() loop break boundary (FalseValue ?? false -> ?? true).
    // ---------------------------------------------------------------------------------------------

    public function testRunWithoutCancellationDoesNotBreakBeforeProcessing(): void
    {
        // kills FalseValue @ line 470
        // With no timeout/cancellation $effectiveCancellation is null, so the loop guard is
        // (null ?? false) = false and the loop processes the buffered request before the peer-close
        // (EOF -> Closed) ends it. The '?? true' mutant breaks on the FIRST iteration, before the
        // request is ever read, so no reply is published.
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
            FakeTransport::EOF, // peer close -> recover -> reconnect disabled -> Closed -> loop ends
        ]);
        $client = new NatsClient(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'reply:' . $m->payload);

        // Outer deadline guards against an accidental spin; run() itself uses no cancellation.
        async(static function () use ($service): void {
            $service->run()->await();
        })->await(new TimeoutCancellation(2.0));

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB _INBOX.req', $writes);
        self::assertStringContainsString('reply:hello', $writes);
    }

    /**
     * Extracts and JSON-decodes the body of the first PUB frame addressed to $inbox.
     *
     * @return array<string,mixed>
     */
    private function decodePublishedJson(FakeTransport $transport, string $inbox): array
    {
        foreach ($transport->writes as $write) {
            if (!str_starts_with($write, 'PUB ' . $inbox . ' ')) {
                continue;
            }

            $headerEnd = strpos($write, "\r\n");
            self::assertNotFalse($headerEnd);
            $body = substr($write, $headerEnd + 2);
            $body = rtrim($body, "\r\n");

            $decoded = json_decode($body, true);
            self::assertIsArray($decoded, 'PUB body to ' . $inbox . ' was not valid JSON: ' . $body);

            /** @var array<string,mixed> $decoded */
            return $decoded;
        }

        self::fail('No PUB frame addressed to ' . $inbox . ' was written.');
    }
}
