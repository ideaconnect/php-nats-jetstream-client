<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Mutation-killing tests for src/JetStream/JetStreamContext.php (chunk 6).
 *
 * Each test pins the exact observable behavior a surviving mutant would change:
 *   - the json_decode() nesting-depth boundary (511 / 512 / 513) on line 1924,
 *   - the malformed-response exception message + code on line 1926,
 *   - the API-error default code on line 1934,
 *   - the $JS.ACK reply-subject guard logic on lines 2002-2003.
 */
final class JetStreamContext_6MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * Builds a single MSG reply frame carrying $json as its payload.
     */
    private function reply(string $json): string
    {
        return sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($json), $json);
    }

    /**
     * Connects a client whose single API reply is $json.
     */
    private function connectedClientWithReply(string $json): NatsClient
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->reply($json),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Produces JSON nested exactly $levels deep: {"a":{"a":...{"a":1}...}}.
     * json_decode($depth) parses a structure iff $levels <= $depth.
     */
    private function nestedObjectJson(int $levels): string
    {
        return str_repeat('{"a":', $levels) . '1' . str_repeat('}', $levels);
    }

    /**
     * The real depth is 512, so a structure nested 511 levels parses successfully and the API call
     * returns normally. The DecrementInteger mutant (depth 511) would reject 511-deep JSON and throw a
     * "Malformed JetStream API response" error instead.
     */
    public function testDecodesJsonAtOneBelowTheDepthLimit(): void
    {
        // kills DecrementInteger @ line 1924 (512 -> 511)
        $client = $this->connectedClientWithReply($this->nestedObjectJson(511));

        // No 'success' key in the nested payload, so deleteStream() returns false WITHOUT throwing.
        // The mutant (depth 511) would throw a JetStreamException before returning.
        $result = $client->jetStream()->deleteStream('ORDERS')->await();

        self::assertFalse($result);
    }

    /**
     * The real depth is 512, so a structure nested 512 levels is REJECTED (one level too deep) and the
     * call throws "Malformed JetStream API response". The IncrementInteger mutant (depth 513) would
     * accept 512-deep JSON and return normally - no exception.
     */
    public function testRejectsJsonAtTheDepthLimit(): void
    {
        // kills IncrementInteger @ line 1924 (512 -> 513)
        $client = $this->connectedClientWithReply($this->nestedObjectJson(512));

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed JetStream API response');

        $client->jetStream()->deleteStream('ORDERS')->await();
    }

    /**
     * A non-JSON API reply is wrapped as a JetStreamException whose message is exactly
     * "Malformed JetStream API response: " . <JsonException message>, with code 0.
     *
     * This pins the line-1926 mutants:
     *   - Concat (operands swapped): message would no longer START with the prefix.
     *   - ConcatOperandRemoval (suffix dropped): message would no longer CONTAIN the json error text.
     *   - Decrement/IncrementInteger on the code: code would be -1 / 1 instead of 0.
     */
    public function testMalformedResponseWrappingMessageAndCode(): void
    {
        $client = $this->connectedClientWithReply('notjson');

        try {
            $client->jetStream()->deleteStream('ORDERS')->await();
            self::fail('Expected JetStreamException for malformed response');
        } catch (JetStreamException $e) {
            // kills Concat @ line 1926 (prefix must come first)
            self::assertStringStartsWith('Malformed JetStream API response: ', $e->getMessage());
            // kills ConcatOperandRemoval @ line 1926 (the JsonException detail must be appended)
            self::assertStringContainsString('Syntax error', $e->getMessage());
            // kills Decrement/IncrementInteger @ line 1926 (exception code is exactly 0)
            self::assertSame(0, $e->getCode());
        }
    }

    /**
     * When an API error payload carries a description but NO "code" field, the default code is 0. The
     * Decrement/Increment mutants on line 1934 would surface code -1 / 1 instead.
     */
    public function testApiErrorWithoutCodeDefaultsToZero(): void
    {
        // kills Decrement/IncrementInteger @ line 1934 ((int)($error['code'] ?? 0))
        $client = $this->connectedClientWithReply('{"error":{"description":"boom"}}');

        try {
            $client->jetStream()->deleteStream('ORDERS')->await();
            self::fail('Expected JetStreamException for API error');
        } catch (JetStreamException $e) {
            self::assertSame('boom', $e->getMessage());
            self::assertSame(0, $e->getCode());
        }
    }

    /**
     * A reply subject of the form $JS.<not-ACK>... must be treated as NOT a JetStream delivery, so
     * streamSequenceOf() returns null. With the LogicalOr mutant (|| -> &&) the guard only rejects when
     * BOTH tokens are wrong; here parts[0] == '$JS' (first check false) so the && guard would fall
     * through and return the sequence at index 5 (555) instead of null.
     */
    public function testRejectsJsReplyWhenSecondTokenIsNotAck(): void
    {
        // kills LogicalOr @ line 2002 (|| -> &&)
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $message = new NatsMessage(
            subject: 'orders.created',
            sid: 1,
            replyTo: '$JS.NOTACK.s.c.1.555.7.0.0', // 9 tokens, index 5 = 555
            payload: 'x',
            rawHeaders: null,
        );

        self::assertNull($js->streamSequenceOf($message));
    }

    /**
     * A reply subject whose first token is not "$JS" (but which otherwise has a valid 9-token ACK
     * shape) must yield null. The ReturnRemoval mutant on line 2003 drops "return null;", so the method
     * would fall through to the token-count match and return the sequence at index 5 (777) instead.
     */
    public function testReturnsNullWhenFirstTokenIsNotJs(): void
    {
        // kills ReturnRemoval @ line 2003 (guard must short-circuit with null)
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $message = new NatsMessage(
            subject: 'orders.created',
            sid: 1,
            replyTo: 'XX.ACK.s.c.1.777.7.0.0', // 9 tokens, parts[0] != '$JS', index 5 = 777
            payload: 'x',
            rawHeaders: null,
        );

        self::assertNull($js->streamSequenceOf($message));
    }
}
