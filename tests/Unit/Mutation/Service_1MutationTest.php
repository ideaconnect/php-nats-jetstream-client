<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\ServiceEndpoint;
use IDCT\NATS\Services\ServiceError;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Targeted tests that kill Infection survivors in src/Services/Service.php (chunk 1).
 *
 * Each test pins the EXACT observable behavior a specific mutation would change: a thrown
 * constructor exception, the bytes written to FakeTransport, an observer-context key/value, an
 * accumulated counter, or the precise id length. All async work is driven by pumping
 * processIncoming() against pre-seeded frames — no sleeps, no real sockets.
 */
final class Service_1MutationTest extends \PHPUnit\Framework\TestCase
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

    // ---------------------------------------------------------------------------------------------
    // Line 78: semantic-version regex anchors.
    // ---------------------------------------------------------------------------------------------

    public function testVersionRegexRequiresLeadingAnchor(): void
    {
        // kills PregMatchRemoveCaret @ line 78
        // "x1.2.3" contains a valid semver SUFFIX. With the caret (^) the whole string must be a
        // semver, so it is rejected; dropping the caret would let the trailing "1.2.3" match.
        $client = $this->connectedClient(new FakeTransport($this->infoAndPong()));

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('semantic version');
        $client->service('svc', 'x1.2.3');
    }

    public function testVersionRegexRequiresTrailingAnchor(): void
    {
        // kills PregMatchRemoveDollar @ line 78
        // "1.2.3 " has trailing junk (a space). With the dollar ($) the match must reach end-of-string,
        // so it is rejected; dropping the dollar would accept the leading "1.2.3".
        $client = $this->connectedClient(new FakeTransport($this->infoAndPong()));

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('semantic version');
        $client->service('svc', '1.2.3 ');
    }

    // ---------------------------------------------------------------------------------------------
    // Line 82: id = bin2hex(random_bytes(8)) -> 16 hex chars.
    // ---------------------------------------------------------------------------------------------

    public function testServiceIdIsSixteenHexChars(): void
    {
        // kills IncrementInteger and DecrementInteger @ line 82
        // random_bytes(8) -> bin2hex -> exactly 16 hex chars. 7 bytes -> 14 chars, 9 bytes -> 18.
        $client = $this->connectedClient(new FakeTransport($this->infoAndPong()));
        $service = $client->service('svc', '1.0.0');

        $id = $service->statsSnapshot()['id'] ?? '';
        self::assertIsString($id);
        self::assertSame(16, strlen($id));
        self::assertMatchesRegularExpression('/^[0-9a-f]{16}$/', $id);
    }

    // ---------------------------------------------------------------------------------------------
    // Line 105: trim($subject) === '' guard.
    // ---------------------------------------------------------------------------------------------

    public function testWhitespaceOnlySubjectIsRejected(): void
    {
        // kills UnwrapTrim @ line 105
        // A whitespace-only subject ("   ") is empty after trim() and must be rejected. Without trim()
        // "   " !== '' so the guard would pass and a blank-ish subject would be registered.
        $client = $this->connectedClient(new FakeTransport($this->infoAndPong()));
        $service = $client->service('svc', '1.0.0');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('subject must not be empty');
        $service->addEndpoint('echo', '   ', static fn(NatsMessage $m): string => 'x');
    }

    // ---------------------------------------------------------------------------------------------
    // Line 218: validation gate (schema !== null && validator !== null).
    // ---------------------------------------------------------------------------------------------

    public function testValidationGateRequiresBothSchemaAndValidator(): void
    {
        // kills LogicalAnd @ line 218
        // Endpoint HAS a schema but NO validator. With && the validation block is skipped and the
        // handler runs normally. With || the block is entered and ($this->requestValidator)(...) is
        // called on a null validator -> TypeError escapes dispatch, so no normal reply is produced.
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('svc', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'OK:' . $m->payload, schema: ['type' => 'object']);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB _INBOX.req', $writes);
        self::assertStringContainsString('OK:hello', $writes);
        // The handler ran exactly once and produced no validation error.
        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame(0, $endpoint['num_errors'] ?? null);
    }

    // ---------------------------------------------------------------------------------------------
    // Validation-rejection path: lines 221 (Minus), 224 (Assignment/PlusEqual), 226, 236, 246.
    // ---------------------------------------------------------------------------------------------

    /**
     * Drives two schema-rejected requests and captures every observer event/context so the
     * validation-path mutations can be pinned precisely.
     *
     * @return array{events: list<array{event: string, context: array<string,mixed>}>, processingTime: int, errors: int}
     */
    private function runTwoRejectedRequests(): array
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            'HMSG svc.v 13 _INBOX.r1 ' . strlen("NATS/1.0\r\nX-Request-Id:corr-1\r\n\r\n") . ' ' . strlen("NATS/1.0\r\nX-Request-Id:corr-1\r\n\r\nhi") . "\r\nNATS/1.0\r\nX-Request-Id:corr-1\r\n\r\nhi\r\n",
            "MSG svc.v 13 _INBOX.r2 2\r\nho\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $events = [];
        $service = $client->service('val', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = ['event' => $event, 'context' => $context];
            })
            ->withRequestValidator(static fn(NatsMessage $message, array $schema): string => 'bad input')
            ->addEndpoint('v', 'svc.v', static fn(NatsMessage $m): string => 'never', schema: ['type' => 'object']);
        $service->start()->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];

        return [
            'events' => $events,
            'processingTime' => (int) ($endpoint['processing_time'] ?? -1),
            'errors' => (int) ($endpoint['num_errors'] ?? -1),
        ];
    }

    public function testValidationProcessingTimeIsAccumulatedDifferenceNotSum(): void
    {
        // kills Minus @ line 221
        // duration = max(0, hrtime() - started) is a tiny non-negative value. The Minus mutant turns
        // it into hrtime() + started (~2x the absolute clock), which is astronomically large.
        $result = $this->runTwoRejectedRequests();
        self::assertGreaterThanOrEqual(0, $result['processingTime']);
        self::assertLessThan(1_000_000_000_000, $result['processingTime']);
    }

    public function testValidationProcessingTimeAccumulatesAcrossRequests(): void
    {
        // kills Assignment (= vs +=) and PlusEqual (-= vs +=) @ line 224
        // Each request_end carries its own duration_ns; processing_time must equal their SUM.
        // '=' would leave only the last duration; '-=' would drive it negative.
        $result = $this->runTwoRejectedRequests();

        $sum = 0;
        $sawTwoEnds = 0;
        foreach ($result['events'] as $e) {
            if ($e['event'] === 'request_end') {
                self::assertArrayHasKey('duration_ns', $e['context']);
                $sum += (int) $e['context']['duration_ns'];
                $sawTwoEnds++;
            }
        }

        self::assertSame(2, $sawTwoEnds);
        self::assertSame($sum, $result['processingTime']);
    }

    public function testValidationRequestErrorContextCarriesValidationErrorCode(): void
    {
        // kills ArrayItemRemoval @ line 226
        // request_error context must carry code => 'VALIDATION_ERROR'.
        $result = $this->runTwoRejectedRequests();

        $errorContext = null;
        foreach ($result['events'] as $e) {
            if ($e['event'] === 'request_error') {
                $errorContext = $e['context'];
                break;
            }
        }

        self::assertNotNull($errorContext);
        self::assertArrayHasKey('code', $errorContext);
        self::assertSame('VALIDATION_ERROR', $errorContext['code']);
        self::assertSame('bad input', $errorContext['error'] ?? null);
    }

    public function testValidationRequestEndContextCarriesDurationKey(): void
    {
        // kills ArrayItemRemoval @ line 246
        // request_end (rejection path) context must include the duration_ns key.
        $result = $this->runTwoRejectedRequests();

        $endContext = null;
        foreach ($result['events'] as $e) {
            if ($e['event'] === 'request_end') {
                $endContext = $e['context'];
                break;
            }
        }

        self::assertNotNull($endContext);
        self::assertArrayHasKey('duration_ns', $endContext);
    }

    public function testValidationErrorReplyCarriesCorrelationIdFromHeader(): void
    {
        // kills Ternary @ line 236
        // The first rejected request carries X-Request-Id:corr-1. The error payload must embed it as
        // correlation_id (real: is_string ? id : null). The swapped ternary yields null and omits it.
        $header = "NATS/1.0\r\nX-Request-Id:corr-1\r\n\r\n";
        $merged = $header . 'hi';
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            'HMSG svc.v 13 _INBOX.r ' . strlen($header) . ' ' . strlen($merged) . "\r\n{$merged}\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('val', '1.0.0')
            ->withRequestValidator(static fn(NatsMessage $m, array $schema): string => 'bad input')
            ->addEndpoint('v', 'svc.v', static fn(NatsMessage $m): string => 'never', schema: ['type' => 'object']);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"code":"VALIDATION_ERROR"', $writes);
        self::assertStringContainsString('"correlation_id":"corr-1"', $writes);
    }

    // ---------------------------------------------------------------------------------------------
    // ServiceError handler path: lines 264 (ArrayItemRemoval/MethodCallRemoval), 265, 266.
    // ---------------------------------------------------------------------------------------------

    /**
     * @return array{events: list<array{event: string, context: array<string,mixed>}>}
     */
    private function runServiceErrorRequest(): array
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.e 13 _INBOX.r 2\r\ngo\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $events = [];
        $service = $client->service('svc', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = ['event' => $event, 'context' => $context];
            })
            ->addEndpoint('e', 'svc.e', static function (NatsMessage $m): never {
                throw new ServiceError('429', 'Rate limited');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        return ['events' => $events];
    }

    public function testServiceErrorEmitsRequestErrorEvent(): void
    {
        // kills MethodCallRemoval @ line 264
        // The ServiceError path must emit a request_error observer event.
        $events = array_column($this->runServiceErrorRequest()['events'], 'event');
        self::assertContains('request_error', $events);
    }

    public function testServiceErrorRequestErrorContextCarriesCodeAndError(): void
    {
        // kills ArrayItemRemoval @ line 264, ArrayItem @ line 265 (code), ArrayItem @ line 266 (error)
        // request_error context for a ServiceError must carry both the chosen code and description.
        $errorContext = null;
        foreach ($this->runServiceErrorRequest()['events'] as $e) {
            if ($e['event'] === 'request_error') {
                $errorContext = $e['context'];
                break;
            }
        }

        self::assertNotNull($errorContext);
        self::assertArrayHasKey('code', $errorContext);
        self::assertSame('429', $errorContext['code']);
        self::assertArrayHasKey('error', $errorContext);
        self::assertSame('Rate limited', $errorContext['error']);
    }

    // ---------------------------------------------------------------------------------------------
    // Generic handler-exception path: lines 283 (ArrayItemRemoval/MethodCallRemoval), 285,
    // 301 (Minus), 302 (Assignment), 304 (ArrayItemRemoval), 335 (Ternary).
    // ---------------------------------------------------------------------------------------------

    /**
     * Drives two throwing-handler requests; captures observer events and stats.
     *
     * @return array{events: list<array{event: string, context: array<string,mixed>}>, processingTime: int}
     */
    private function runTwoThrowingRequests(): array
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.e 13 _INBOX.r1 2\r\ng1\r\n",
            "MSG svc.e 13 _INBOX.r2 2\r\ng2\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $events = [];
        $service = $client->service('svc', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = ['event' => $event, 'context' => $context];
            })
            ->addEndpoint('e', 'svc.e', static function (NatsMessage $m): never {
                throw new \RuntimeException('boom-' . $m->payload);
            });
        $service->start()->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];

        return [
            'events' => $events,
            'processingTime' => (int) ($endpoint['processing_time'] ?? -1),
        ];
    }

    public function testHandlerErrorEmitsRequestErrorEvent(): void
    {
        // kills MethodCallRemoval @ line 283
        // The generic handler-exception path must emit a request_error event.
        $events = array_column($this->runTwoThrowingRequests()['events'], 'event');
        self::assertContains('request_error', $events);
    }

    public function testHandlerErrorRequestErrorContextCarriesCodeAndMessage(): void
    {
        // kills ArrayItemRemoval @ line 283 (code), ArrayItem @ line 285 (error)
        // request_error context for an uncaught handler exception carries HANDLER_ERROR plus the raw
        // message (server-side; the reply body is sanitized elsewhere).
        $errorContext = null;
        foreach ($this->runTwoThrowingRequests()['events'] as $e) {
            if ($e['event'] === 'request_error') {
                $errorContext = $e['context'];
                break;
            }
        }

        self::assertNotNull($errorContext);
        self::assertArrayHasKey('code', $errorContext);
        self::assertSame('HANDLER_ERROR', $errorContext['code']);
        self::assertArrayHasKey('error', $errorContext);
        self::assertSame('boom-g1', $errorContext['error']);
    }

    public function testHandlerPathProcessingTimeIsDifferenceNotSum(): void
    {
        // kills Minus @ line 301
        // duration in the handler finally must be hrtime() - started (tiny), not hrtime() + started.
        $result = $this->runTwoThrowingRequests();
        self::assertGreaterThanOrEqual(0, $result['processingTime']);
        self::assertLessThan(1_000_000_000_000, $result['processingTime']);
    }

    public function testHandlerPathProcessingTimeAccumulates(): void
    {
        // kills Assignment (= vs +=) @ line 302
        // processing_time must equal the SUM of per-request request_end duration_ns values.
        $result = $this->runTwoThrowingRequests();

        $sum = 0;
        $ends = 0;
        foreach ($result['events'] as $e) {
            if ($e['event'] === 'request_end') {
                self::assertArrayHasKey('duration_ns', $e['context']);
                $sum += (int) $e['context']['duration_ns'];
                $ends++;
            }
        }

        self::assertSame(2, $ends);
        self::assertSame($sum, $result['processingTime']);
    }

    public function testHandlerPathRequestEndContextCarriesDurationKey(): void
    {
        // kills ArrayItemRemoval @ line 304
        // request_end (handler path) context must include duration_ns.
        $endContext = null;
        foreach ($this->runTwoThrowingRequests()['events'] as $e) {
            if ($e['event'] === 'request_end') {
                $endContext = $e['context'];
                break;
            }
        }

        self::assertNotNull($endContext);
        self::assertArrayHasKey('duration_ns', $endContext);
    }

    // ---------------------------------------------------------------------------------------------
    // JSON-encode-failure path (#97): lines 322 (Increment), 324 (ArrayItemRemoval/MethodCallRemoval),
    // 326 (ArrayItem), 335 (Ternary).
    // ---------------------------------------------------------------------------------------------

    /**
     * Drives one request whose handler returns un-encodable (non-UTF-8) data, with a correlation id.
     *
     * @return array{events: list<array{event: string, context: array<string,mixed>}>, errors: int, writes: string}
     */
    private function runEncodeFailureRequest(): array
    {
        $header = "NATS/1.0\r\nX-Request-Id:enc-9\r\n\r\n";
        $merged = $header . 'hi';
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            'HMSG svc.bad 13 _INBOX.r ' . strlen($header) . ' ' . strlen($merged) . "\r\n{$merged}\r\n",
        ]);
        $client = $this->connectedClient($transport);

        $events = [];
        $service = $client->service('svc', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = ['event' => $event, 'context' => $context];
            })
            ->addEndpoint('bad', 'svc.bad', static fn(NatsMessage $m): array => ['data' => "\xff\xfe\xfa"]);
        $service->start()->await();

        $client->processIncoming()->await();

        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];

        return [
            'events' => $events,
            'errors' => (int) ($endpoint['num_errors'] ?? -1),
            'writes' => implode('', $transport->writes),
        ];
    }

    public function testEncodeFailureIncrementsErrorCounter(): void
    {
        // kills Increment (errors++ -> errors--) @ line 322
        $result = $this->runEncodeFailureRequest();
        self::assertSame(1, $result['errors']);
    }

    public function testEncodeFailureEmitsRequestErrorEvent(): void
    {
        // kills MethodCallRemoval @ line 324
        $events = array_column($this->runEncodeFailureRequest()['events'], 'event');
        self::assertContains('request_error', $events);
    }

    public function testEncodeFailureRequestErrorContextCarriesCodeAndError(): void
    {
        // kills ArrayItemRemoval @ line 324 (code), ArrayItem @ line 326 (error)
        $errorContext = null;
        foreach ($this->runEncodeFailureRequest()['events'] as $e) {
            if ($e['event'] === 'request_error') {
                $errorContext = $e['context'];
                break;
            }
        }

        self::assertNotNull($errorContext);
        self::assertArrayHasKey('code', $errorContext);
        self::assertSame('HANDLER_ERROR', $errorContext['code']);
        self::assertArrayHasKey('error', $errorContext);
        self::assertIsString($errorContext['error']);
        self::assertNotSame('', $errorContext['error']);
    }

    public function testEncodeFailureReplyCarriesCorrelationId(): void
    {
        // kills Ternary @ line 335
        // The controlled 500 reply on the encode-failure path must embed correlation_id from the header.
        $result = $this->runEncodeFailureRequest();
        self::assertStringContainsString('"code":"HANDLER_ERROR"', $result['writes']);
        self::assertStringContainsString('"correlation_id":"enc-9"', $result['writes']);
    }

    // ---------------------------------------------------------------------------------------------
    // start() rollback: lines 354 (Foreach_), 356 (MethodCallRemoval).
    // ---------------------------------------------------------------------------------------------

    public function testStartRollbackUnsubscribesAlreadySubscribedSids(): void
    {
        // kills Foreach_ @ line 354 and MethodCallRemoval @ line 356
        // A partial start() failure must roll back the SIDs subscribed so far by emitting UNSUB frames.
        // Iterating [] (Foreach_) or removing the unsubscribe call (MethodCallRemoval) emits no UNSUB.
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->connectedClient($transport);

        $service = $client->service('svc', '1.0.0')
            ->addEndpoint('good', 'svc.good', static fn(NatsMessage $m): string => 'ok')
            ->addEndpoint('bad', 'bad subject', static fn(NatsMessage $m): string => 'no');

        $writesBefore = count($transport->writes);

        try {
            $service->start()->await();
            self::fail('Expected start() to throw on the invalid subject');
        } catch (\Throwable) {
            // expected
        }

        $rollbackWrites = implode('', array_slice($transport->writes, $writesBefore));
        // The discovery subscriptions + the "good" endpoint were unsubscribed during rollback.
        self::assertStringContainsString('UNSUB ', $rollbackWrites);
    }

    // ---------------------------------------------------------------------------------------------
    // drain(): line 438 (MethodCallRemoval of flush()).
    // ---------------------------------------------------------------------------------------------

    public function testDrainFlushesWithExtraPing(): void
    {
        // kills MethodCallRemoval @ line 438
        // connect() emits one PING; drain()'s flush() must emit a SECOND PING. Removing flush() leaves
        // only the connect-time PING. Count PING frames to distinguish (>=2 real, ==1 mutant).
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "PONG\r\n", // answers the drain flush
        ]);
        $client = $this->connectedClient($transport);

        $service = $client->service('svc', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        $pingsBefore = substr_count(implode('', $transport->writes), "PING\r\n");
        $service->drain()->await();
        $pingsAfter = substr_count(implode('', $transport->writes), "PING\r\n");

        // drain() added exactly one flush PING on top of whatever was sent before.
        self::assertSame($pingsBefore + 1, $pingsAfter);
    }
}
