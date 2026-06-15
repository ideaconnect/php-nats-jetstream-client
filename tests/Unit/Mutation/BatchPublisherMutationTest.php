<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\BatchPublisher;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for {@see BatchPublisher}. Each method pins the exact observable behaviour
 * that a surviving Infection mutant would change. See // kills <mutator> @ line N comments.
 */
final class BatchPublisherMutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * @param list<string> $reads
     */
    private function connectedClient(array $reads = []): NatsClient
    {
        $transport = new FakeTransport(array_merge([self::INFO, "PONG\r\n"], $reads));
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /** Builds a NATS MSG frame for sid with the given payload. */
    private static function msg(string $inbox, int $sid, string $payload): string
    {
        return sprintf("MSG %s %d %d\r\n%s\r\n", $inbox, $sid, strlen($payload), $payload);
    }

    /** JSON document whose deepest branch is nested $k array levels but with a top-level error object. */
    private static function deepErrorDoc(int $k, int $code, string $description): string
    {
        $nested = '1';
        for ($i = 0; $i < $k; ++$i) {
            $nested = '[' . $nested . ']';
        }

        return sprintf('{"error":{"code":%d,"description":"%s"},"pad":%s}', $code, $description, $nested);
    }

    // -----------------------------------------------------------------------------------------------
    // add() - MAX_MESSAGES overflow message (line 59)
    // -----------------------------------------------------------------------------------------------

    /**
     * The overflow exception message must be exactly "Atomic batch is limited to 1000 messages" - the
     * literal, the constant, and the suffix concatenated in that order.
     */
    public function testOverflowMessageIsExactConcatenation(): void
    {
        $batch = $this->connectedClient()->jetStream()->batch('overflow');

        $prop = new \ReflectionProperty($batch, 'messages');
        $prop->setValue($batch, array_fill(0, BatchPublisher::MAX_MESSAGES, [
            'subject' => 's',
            'payload' => 'p',
            'headers' => [],
        ]));

        try {
            $batch->add('orders.created', 'overflow');
            self::fail('Expected overflow to throw');
        } catch (JetStreamException $e) {
            // kills Concat @ 59 (operand reorder), kills ConcatOperandRemoval @ 59 (dropped operand)
            self::assertSame('Atomic batch is limited to 1000 messages', $e->getMessage());
        }
    }

    // -----------------------------------------------------------------------------------------------
    // commit() - the START request must use messages[0] (line 109)
    // -----------------------------------------------------------------------------------------------

    /**
     * The batch START (sequence 1) request must carry the FIRST staged message (index 0), not index 1.
     */
    public function testStartRequestUsesFirstStagedMessage(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":2,"batch":"b-start","count":2}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            // Zero-byte accept for the START request (sid 1).
            "MSG _INBOX.a 1 0\r\n\r\n",
            // Commit PubAck for the commit request (sid 2).
            self::msg('_INBOX.b', 2, $commitAck),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->batch('b-start')
            ->add('alpha.subject', 'AAA')
            ->add('beta.subject', 'BBB')
            ->commit()
            ->await();

        $startWrite = null;
        foreach ($transport->writes as $w) {
            if (str_contains($w, 'Nats-Batch-Sequence:1')) {
                $startWrite = $w;
                break;
            }
        }

        self::assertNotNull($startWrite, 'expected a sequence-1 start write');
        // kills IncrementInteger @ 109 (messages[0] -> messages[1]): start must carry message 0.
        self::assertStringStartsWith('HPUB alpha.subject _INBOX.', $startWrite);
        self::assertStringContainsString('AAA', $startWrite);
        self::assertStringNotContainsString('beta.subject', $startWrite);
    }

    // -----------------------------------------------------------------------------------------------
    // assertStartAccepted() - json_decode depth (line 155)
    // -----------------------------------------------------------------------------------------------

    /**
     * The start-reply parser uses depth 512. A start error object whose document needs exactly depth
     * 512 must still parse and surface the embedded error. A lower limit (511) would throw a
     * JsonException, get swallowed, and wrongly treat the rejection as ACCEPTED.
     */
    public function testStartDepth512ParsesEmbeddedErrorAndRejects(): void
    {
        // deepErrorDoc(510) => document needs limit >= 512 to parse (object+error+padding nesting).
        $startReject = self::deepErrorDoc(510, 400, 'depth-rejected');
        $commitAck = '{"stream":"ORDERS","seq":2,"batch":"b-d","count":2}';

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $startReject),
            // Only consumed if the start were (wrongly) treated as accepted.
            self::msg('_INBOX.b', 2, $commitAck),
        ]);

        try {
            $client->jetStream()->batch('b-d')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();
            self::fail('Expected the embedded start error to be parsed and thrown');
        } catch (JetStreamException $e) {
            // kills DecrementInteger @ 155 (512 -> 511): a too-shallow limit would swallow this error.
            self::assertSame('depth-rejected', $e->getMessage());
        }
    }

    /**
     * A start reply nested one level deeper than the depth-512 limit must FAIL to parse, be swallowed,
     * and be treated as ACCEPTED so the commit proceeds. A higher limit (513) would parse it, see the
     * embedded error, and wrongly reject.
     */
    public function testStartDepth513OverflowsAndIsTreatedAsAccepted(): void
    {
        // deepErrorDoc(511) => needs limit >= 513; at 512 it throws -> swallowed -> accepted.
        $startTooDeep = self::deepErrorDoc(511, 400, 'should-not-surface');
        $commitAck = '{"stream":"ORDERS","seq":2,"batch":"b-d2","count":2}';

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $startTooDeep),
            self::msg('_INBOX.b', 2, $commitAck),
        ]);

        $ack = $client->jetStream()->batch('b-d2')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->commit()
            ->await();

        // kills IncrementInteger @ 155 (512 -> 513): a deeper limit would parse and reject instead.
        self::assertSame(2, $ack->batchCount);
        self::assertSame('b-d2', $ack->batchId);
    }

    // -----------------------------------------------------------------------------------------------
    // assertStartAccepted() - start-rejection error code defaulting (line 166)
    // -----------------------------------------------------------------------------------------------

    /**
     * A start rejection with NO code field must default the exception code to 0.
     */
    public function testStartRejectionMissingCodeDefaultsToZero(): void
    {
        $startReject = '{"error":{"description":"no code field"}}';

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $startReject),
        ]);

        try {
            $client->jetStream()->batch('b-nc')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();
            self::fail('Expected start rejection to throw');
        } catch (JetStreamException $e) {
            // kills DecrementInteger @ 166 (0 -> -1) and IncrementInteger @ 166 (0 -> 1).
            self::assertSame(0, $e->getCode());
            self::assertSame('no code field', $e->getMessage());
        }
    }

    /**
     * A start rejection WITH a code field must use that code (not the default), so the ?? must read the
     * present value rather than short-circuiting on the default.
     */
    public function testStartRejectionUsesProvidedCode(): void
    {
        $startReject = '{"error":{"code":503,"description":"explicit code"}}';

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $startReject),
        ]);

        try {
            $client->jetStream()->batch('b-ec')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();
            self::fail('Expected start rejection to throw');
        } catch (JetStreamException $e) {
            // kills Coalesce @ 166 (error['code'] ?? 0  =>  0 ?? error['code']): mutant would force 0.
            self::assertSame(503, $e->getCode());
        }
    }

    // -----------------------------------------------------------------------------------------------
    // parseCommitAck() - json_decode depth (line 198)
    // -----------------------------------------------------------------------------------------------

    /**
     * The commit-ack parser uses depth 512. A commit error whose document needs exactly depth 512 must
     * parse and surface the embedded error description. A lower limit (511) would throw a JsonException
     * and surface the generic "Malformed atomic batch commit ack" message instead.
     */
    public function testCommitAckDepth512ParsesEmbeddedError(): void
    {
        // Single-message batch: the only request is the commit (sid 1).
        $commitErr = self::deepErrorDoc(510, 409, 'commit-depth-error');

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $commitErr),
        ]);

        try {
            $client->jetStream()->batch('b-cd')
                ->add('orders.created', 'a')
                ->commit()
                ->await();
            self::fail('Expected the embedded commit error to be parsed and thrown');
        } catch (JetStreamException $e) {
            // kills DecrementInteger @ 198 (512 -> 511): a too-shallow limit would report "Malformed".
            self::assertSame('commit-depth-error', $e->getMessage());
            self::assertSame(409, $e->getCode());
        }
    }

    /**
     * A commit ack nested one level past depth 512 must FAIL to parse and surface the generic
     * "Malformed atomic batch commit ack" message. A higher limit (513) would parse it and surface the
     * embedded error description instead.
     */
    public function testCommitAckDepth513OverflowsToMalformed(): void
    {
        $commitTooDeep = self::deepErrorDoc(511, 409, 'should-stay-hidden');

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $commitTooDeep),
        ]);

        try {
            $client->jetStream()->batch('b-cd2')
                ->add('orders.created', 'a')
                ->commit()
                ->await();
            self::fail('Expected a malformed-commit-ack error');
        } catch (JetStreamException $e) {
            // kills IncrementInteger @ 198 (512 -> 513): a deeper limit would surface the embedded error.
            self::assertStringStartsWith('Malformed atomic batch commit ack', $e->getMessage());
            self::assertStringNotContainsString('should-stay-hidden', $e->getMessage());
        }
    }

    // -----------------------------------------------------------------------------------------------
    // parseCommitAck() - malformed-ack message + code (line 200)
    // -----------------------------------------------------------------------------------------------

    /**
     * A non-JSON commit ack must throw with the exact message "Malformed atomic batch commit ack: "
     * followed by the JsonException text, and with code 0.
     */
    public function testMalformedCommitAckExactMessageAndCode(): void
    {
        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, 'not-json-at-all'),
        ]);

        try {
            $client->jetStream()->batch('b-mal')
                ->add('orders.created', 'a')
                ->commit()
                ->await();
            self::fail('Expected malformed commit ack to throw');
        } catch (JetStreamException $e) {
            // kills Concat @ 200 (operand reorder) and ConcatOperandRemoval @ 200 (dropped getMessage()).
            self::assertSame('Malformed atomic batch commit ack: Syntax error', $e->getMessage());
            // kills DecrementInteger @ 200 (0 -> -1) and IncrementInteger @ 200 (0 -> 1).
            self::assertSame(0, $e->getCode());
        }
    }

    // -----------------------------------------------------------------------------------------------
    // parseCommitAck() - embedded error description cast (line 206) + code defaulting (line 207)
    // -----------------------------------------------------------------------------------------------

    /**
     * A commit-ack error whose description is a JSON number must be cast to a string before becoming
     * the exception message. Without the (string) cast the int is passed to the string-typed exception
     * message parameter under strict_types and raises a TypeError instead of a JetStreamException.
     */
    public function testCommitAckNumericDescriptionIsStringified(): void
    {
        $errorAck = '{"error":{"description":12345,"code":7}}';

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $errorAck),
        ]);

        try {
            $client->jetStream()->batch('b-numdesc')
                ->add('orders.created', 'a')
                ->commit()
                ->await();
            self::fail('Expected commit ack error to throw');
        } catch (JetStreamException $e) {
            // kills CastString @ 206 ((string) description removed): mutant would TypeError, not throw this.
            self::assertSame('12345', $e->getMessage());
            self::assertSame(7, $e->getCode());
        }
    }

    /**
     * A commit-ack error with NO code field must default the exception code to 0.
     */
    public function testCommitAckErrorMissingCodeDefaultsToZero(): void
    {
        $errorAck = '{"error":{"description":"aborted, no code"}}';

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $errorAck),
        ]);

        try {
            $client->jetStream()->batch('b-ecnc')
                ->add('orders.created', 'a')
                ->commit()
                ->await();
            self::fail('Expected commit ack error to throw');
        } catch (JetStreamException $e) {
            // kills DecrementInteger @ 207 (0 -> -1) and IncrementInteger @ 207 (0 -> 1).
            self::assertSame(0, $e->getCode());
            self::assertSame('aborted, no code', $e->getMessage());
        }
    }

    /**
     * A commit-ack error WITH an integer code must use that code (not the default), so the ?? must read
     * the present value rather than short-circuiting on the default.
     */
    public function testCommitAckErrorUsesProvidedIntegerCode(): void
    {
        $errorAck = '{"error":{"code":409,"description":"conflict"}}';

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $errorAck),
        ]);

        try {
            $client->jetStream()->batch('b-ic')
                ->add('orders.created', 'a')
                ->commit()
                ->await();
            self::fail('Expected commit ack error to throw');
        } catch (JetStreamException $e) {
            // kills Coalesce @ 207 (error['code'] ?? 0  =>  0 ?? error['code']): mutant would force 0.
            self::assertSame(409, $e->getCode());
            self::assertSame('conflict', $e->getMessage());
        }
    }

    /**
     * A commit-ack error whose code is a numeric STRING must be cast to int before becoming the
     * exception code. Without the (int) cast the string is passed to the int-typed exception code
     * parameter under strict_types and raises a TypeError instead of a JetStreamException.
     */
    public function testCommitAckStringCodeIsCastToInt(): void
    {
        $errorAck = '{"error":{"code":"409","description":"string code"}}';

        $client = $this->connectedClient([
            self::msg('_INBOX.a', 1, $errorAck),
        ]);

        try {
            $client->jetStream()->batch('b-sc')
                ->add('orders.created', 'a')
                ->commit()
                ->await();
            self::fail('Expected commit ack error to throw');
        } catch (JetStreamException $e) {
            // kills CastInt @ 207 ((int) code removed): mutant would TypeError on the string code.
            self::assertSame(409, $e->getCode());
            self::assertSame('string code', $e->getMessage());
        }
    }
}
