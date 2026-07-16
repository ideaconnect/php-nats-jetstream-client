<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Models\ConsumerInfo;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Targeted mutation-killing tests for src/JetStream/JetStreamContext.php (chunk 2).
 *
 * Each test pins a specific observable behavior (return value, thrown exception code/message,
 * exact bytes on the wire, sid value, collection size) that a surviving mutant would change.
 */
final class JetStreamContext_2MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * @param list<string> $readQueue
     *
     * @return array{0: NatsClient, 1: FakeTransport}
     */
    private function connectedClient(array $readQueue): array
    {
        $transport = new FakeTransport(array_merge([self::INFO, "PONG\r\n"], $readQueue));
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        return [$client, $transport];
    }

    private static function msg(string $json, int $sid = 1): string
    {
        return sprintf("MSG _INBOX.x %d %d\r\n%s\r\n", $sid, strlen($json), $json);
    }

    private static function hmsg(string $headers, string $body = ''): string
    {
        return sprintf(
            "HMSG _INBOX.x 1 %d %d\r\n%s%s\r\n",
            strlen($headers),
            strlen($headers) + strlen($body),
            $headers,
            $body,
        );
    }

    /**
     * Post-#118 the request inbox is MUXED: request() no longer subscribes a fresh inbox per call;
     * instead ONE long-lived wildcard "<base>.*" (sid 1, established lazily on the first request)
     * serves every reply, and each request publishes reply-to "<base>.<token>" and routes by that
     * token. A pre-seeded "MSG _INBOX.x <sid>" / "HMSG _INBOX.x <sid>" therefore no longer reaches the
     * waiting request (its subject is not the request's random "<base>.<token>", so dispatchMuxReply
     * drops it and the request times out).
     *
     * This installs a dynamic responder that mirrors a real server: it learns the mux base from the
     * wildcard SUB, and for each request PUB/HPUB (a publish whose reply-to lives under that base) pops
     * the next reply frame and re-emits it on the CAPTURED reply-to with the mux sid (1). Frames are
     * delivered FIFO in the order requests are written, so a method that issues several requests (e.g.
     * listConsumers paging create->offset+=2->...) still gets its replies in sequence.
     *
     * @param list<string> $frames Pre-built MSG/HMSG reply frames; only their subject+sid are rewritten.
     */
    private function muxReplies(FakeTransport $transport, array $frames): void
    {
        $queue = $frames;
        $base = null;

        $transport->onWrite = static function (string $bytes) use (&$queue, &$base): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                // Learn the mux base from the wildcard registration "SUB <base>.* <sid>".
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1); // strip the trailing "*" -> "_INBOX.<hex>."
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            // PUB <subject> <replyTo> <len>  |  HPUB <subject> <replyTo> <hdrLen> <totLen>
            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                return [];
            }

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo)];
        };
    }

    /**
     * Rewrites a pre-built MSG/HMSG reply frame's subject and sid to the request's captured mux reply-to
     * and the mux sid (1), preserving the payload/header bytes and declared lengths verbatim so the
     * reply's content is delivered unchanged - only its addressing is corrected for the mux inbox.
     */
    private static function rewriteReplyFrame(string $frame, string $replyTo): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo; // subject
        $tokens[2] = '1';      // mux sid

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    /**
     * listConsumers paginates across multiple pages.
     *
     * kills DoWhile @ line 383 (while(false) would stop after page 1 -> 2 instead of 5 consumers)
     * kills NotIdentical @ line 394 (=== [] makes the loop body run at most once)
     * kills LogicalAndAllSubExprNegation @ line 394 (negated condition stops after page 1)
     * kills Assignment @ line 392 (offset= instead of += never reaches offset 4 on page 3)
     * kills PlusEqual @ line 392 (offset-= would request negative offsets, never 2/4)
     * kills ArrayItemRemoval @ line 384 (dropping ['offset'=>...] removes the offset key entirely)
     */
    public function testListConsumersPaginatesAccumulatingOffset(): void
    {
        $page = static fn(array $names): string => json_encode([
            'total' => 5,
            'consumers' => array_map(
                static fn(string $n): array => ['stream_name' => 'ORDERS', 'name' => $n, 'config' => ['durable_name' => $n]],
                $names,
            ),
        ], JSON_THROW_ON_ERROR);

        [$client, $transport] = $this->connectedClient([]);
        // Three paginated CONSUMER.LIST requests share the mux inbox; replies come back FIFO.
        $this->muxReplies($transport, [
            self::msg($page(['a', 'b'])),   // offset 0 -> 2 consumers
            self::msg($page(['c', 'd'])),   // offset 2 -> 2 consumers
            self::msg($page(['e'])),        // offset 4 -> 1 consumer (total 5 reached)
        ]);

        $consumers = $client->jetStream()->listConsumers('ORDERS')->await();

        // All five consumers collected across three pages: proves the loop ran more than once and
        // kept consuming until total was reached.
        self::assertCount(5, $consumers);
        self::assertSame(
            ['a', 'b', 'c', 'd', 'e'],
            array_map(static fn(ConsumerInfo $c): string => $c->name, $consumers),
        );

        $writes = implode('||', $transport->writes);
        // Offset is accumulated (0, 2, 4) and is present on every request.
        self::assertStringContainsString('"offset":0', $writes);
        self::assertStringContainsString('"offset":2', $writes);
        self::assertStringContainsString('"offset":4', $writes);
        // A subtractive offset (PlusEqual -> -=) would produce negative offsets.
        self::assertStringNotContainsString('"offset":-2', $writes);
    }

    /**
     * streamMessageFromResponse only base64-decodes data when it is actually a string.
     *
     * kills LogicalAnd @ line 455 (&& -> ||): with a non-string (int) data field, || would
     * short-circuit on isset and base64-decode the coerced int, yielding a garbage payload.
     */
    public function testStreamMessageIgnoresNonStringData(): void
    {
        // "1234" is a valid 4-char base64 token, so an || mutant would decode it to 3 garbage bytes.
        $json = '{"message":{"subject":"orders.x","data":1234}}';

        [$client, $transport] = $this->connectedClient([]);
        $this->muxReplies($transport, [self::msg($json)]);

        $message = $client->jetStream()->getStreamMessage('ORDERS', 1)->await();

        // Real code leaves payload empty because data is not a string.
        self::assertSame('', $message->payload);
        self::assertSame('orders.x', $message->subject);
    }

    /**
     * streamMessageFromResponse pins the synthetic sid to 0.
     *
     * kills DecrementInteger @ line 474 (sid: 0 -> -1)
     * kills IncrementInteger @ line 474 (sid: 0 -> 1)
     */
    public function testStreamMessageSidIsZero(): void
    {
        $json = '{"message":{"subject":"orders.x","data":"' . base64_encode('hi') . '"}}';

        [$client, $transport] = $this->connectedClient([]);
        $this->muxReplies($transport, [self::msg($json)]);

        $message = $client->jetStream()->getStreamMessage('ORDERS', 1)->await();

        self::assertSame(0, $message->sid);
        self::assertSame('hi', $message->payload);
    }

    /**
     * deleteMessage returns false when the server omits the success flag.
     *
     * kills FalseValue @ line 501 (?? false -> ?? true): a missing success key would flip to true.
     */
    public function testDeleteMessageReturnsFalseWhenSuccessAbsent(): void
    {
        [$client, $transport] = $this->connectedClient([]);
        $this->muxReplies($transport, [self::msg('{}')]);

        self::assertFalse($client->jetStream()->deleteMessage('ORDERS', 7)->await());
    }

    /**
     * A Direct Get against a stream with no DIRECT.GET responder surfaces the exact 503 message.
     *
     * kills Concat @ line 556 (operand reorder)
     * kills ConcatOperandRemoval @ line 556 (dropping the stream name or a literal)
     * (the exact-equality assertion fails for any reordering or dropped operand)
     */
    public function testDirectGetNoRespondersMessageIsExact(): void
    {
        // A no-responders reply is an HMSG carrying a "NATS/1.0 503" status line.
        [$client, $transport] = $this->connectedClient([]);
        $this->muxReplies($transport, [self::hmsg("NATS/1.0 503\r\n\r\n")]);

        try {
            $client->jetStream()->directGetLastMessageForSubject('ORDERS', 'orders.1')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(503, $e->getCode());
            self::assertSame(
                'JetStream Direct Get is unavailable on stream ORDERS'
                . ' (enable allow_direct on the stream, or use a server that supports Direct Get)',
                $e->getMessage(),
            );
        }
    }

    /**
     * directGet throws at the exact 400 status boundary.
     *
     * kills GreaterThanOrEqualTo @ line 570 (>= -> >): status exactly 400 must still throw with
     * code 400; the > mutant would skip the throw and fall through to the "unrecognized" guard.
     */
    public function testDirectGetThrowsAtStatus400Boundary(): void
    {
        [$client, $transport] = $this->connectedClient([]);
        $this->muxReplies($transport, [self::hmsg("NATS/1.0 400 Bad Request\r\n\r\n")]);

        try {
            $client->jetStream()->directGetLastMessageForSubject('ORDERS', 'orders.1')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // Real code: throws the status description with code 400.
            // > mutant: would instead throw "unrecognized response" with code 0.
            self::assertSame(400, $e->getCode());
            self::assertSame('Bad Request', $e->getMessage());
        }
    }

    /**
     * directGet accepts a response carrying Nats-Stream even when Nats-Sequence is absent.
     *
     * kills LogicalAnd @ line 578 (&& -> ||): the && guard only rejects when BOTH metadata headers
     * are missing; the || mutant would reject (throw) when EITHER is missing.
     * kills IncrementInteger @ line 586 (sid: 0 -> 1)
     * kills DecrementInteger @ line 586 (sid: 0 -> -1)
     */
    public function testDirectGetAcceptsResponseWithOnlyNatsStreamAndSidIsZero(): void
    {
        $headers = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.kept\r\n\r\n";
        [$client, $transport] = $this->connectedClient([]);
        $this->muxReplies($transport, [self::hmsg($headers, 'body')]);

        // Real (&&): Nats-Stream present, so it does NOT throw and returns the message.
        $message = $client->jetStream()->directGetLastMessageForSubject('ORDERS', 'orders.kept')->await();

        self::assertSame('orders.kept', $message->subject);
        self::assertSame('body', $message->payload);
        self::assertSame(0, $message->sid);
    }

    /**
     * directGetLastForSubjects rejects a wildcard subject with the exact guidance message.
     *
     * kills Concat @ line 615 (operand reorder)
     * kills ConcatOperandRemoval @ line 615 (dropping the subject or a literal)
     */
    public function testDirectGetLastForSubjectsWildcardMessageIsExact(): void
    {
        [$client] = $this->connectedClient([]);

        try {
            $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.*'])->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(
                'directGetLastForSubjects expects exact subjects; the wildcard "orders.*"'
                . ' would be truncated - use directGetBatch() with an explicit batch size instead',
                $e->getMessage(),
            );
        }
    }

    /**
     * directGetBatch throws at the exact 400 status boundary and propagates the description.
     *
     * kills GreaterThanOrEqualTo @ line 667 (>= -> >): status exactly 400 must be treated as an
     * error; the > mutant would treat it as a data message and return without throwing.
     * kills Coalesce @ line 670 ($headers['Description'] ?? '' -> '' ?? $headers['Description']):
     * the mutant always yields '' so the thrown message would fall back to the generic default.
     */
    public function testDirectGetBatchThrowsAtStatus400WithDescription(): void
    {
        [$client] = $this->connectedClient([
            self::hmsg("NATS/1.0 400 Boom\r\nStatus: 400\r\nDescription: Boom\r\n\r\n"),
            // A trailing 204 EOB so a > mutant (which treats 400 as data) still terminates fast.
            self::hmsg("NATS/1.0 204 EOB\r\n\r\n"),
        ]);

        try {
            $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10], 1000)->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(400, $e->getCode());
            // Real code surfaces the server Description; the coalesce mutant would surface the
            // generic 'JetStream direct get batch error' fallback instead.
            self::assertSame('Boom', $e->getMessage());
        }
    }

    /**
     * directGetBatch builds collected messages with sid 0.
     *
     * kills DecrementInteger @ line 679 (sid: 0 -> -1)
     */
    public function testDirectGetBatchMessageSidIsZero(): void
    {
        $headers = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\n\r\n";
        [$client] = $this->connectedClient([
            self::hmsg($headers, 'aaa'),
            self::hmsg("NATS/1.0 204 EOB\r\n\r\n"),
        ]);

        $messages = $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10], 1000)->await();

        self::assertCount(1, $messages);
        self::assertSame(0, $messages[0]->sid);
        self::assertSame('aaa', $messages[0]->payload);
    }
}
