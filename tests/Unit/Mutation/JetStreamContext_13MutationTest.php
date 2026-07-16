<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\Models\ConsumerInfo;
use IDCT\NATS\JetStream\Models\StreamInfo;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for src/JetStream/JetStreamContext.php (chunk 13).
 *
 * Focus areas of surviving mutants pinned here:
 *  - `(bool) (... ?? false)` casts on the success flag of delete APIs: the existing "defaults to false"
 *    tests exercise only the ABSENT case (false either way); these feed a NON-boolean `success` so the
 *    unwrapped mutant returns a non-bool from a `: bool` closure (strict_types) and blows up.
 *  - `array_values(array_filter($raw, 'is_array'))` in listStreams()/listConsumers(): the mutant that
 *    drops the is_array filter feeds a non-array element to `fromArray(array ...)`, a strict TypeError.
 *  - the `idleHeartbeatNs <= 0` guard on subscribeOrderedConsumer().
 *  - self::assertValidJsName() removals on methods that do NOT delegate to another validating call, so
 *    the removal is genuinely observable (an invalid name is otherwise accepted).
 *
 * Harness mirrors chunk 1: a FakeTransport onWrite responder learns the muxed request inbox (#118) and
 * replies FIFO on each request's captured reply-to. Fully in-process; no sockets, no sleeps.
 */
final class JetStreamContext_13MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * Connects a client and arms the muxed request-inbox responder (#118) with the given JSON replies.
     *
     * @param list<string> $muxFrames Pre-built MSG reply frames; only their subject+sid get rewritten.
     *
     * @return array{NatsClient, FakeTransport}
     */
    private function connect(array $muxFrames): array
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $this->muxReplies($transport, $muxFrames);

        return [$client, $transport];
    }

    private static function msg(string $json, string $inbox = '_INBOX.a', int $sid = 1): string
    {
        return sprintf("MSG %s %d %d\r\n%s\r\n", $inbox, $sid, strlen($json), $json);
    }

    /**
     * Installs a dynamic responder that mirrors a real NATS server against the muxed request inbox
     * (#118): it learns the mux base+sid from the wildcard "SUB <base>.* <sid>" frame, then for each
     * request PUB/HPUB whose reply-to lives under that base pops the next queued reply and re-emits it on
     * the CAPTURED reply-to with the mux sid. Frames are delivered FIFO in request order.
     *
     * @param list<string> $frames
     */
    private function muxReplies(FakeTransport $transport, array $frames): void
    {
        $queue = $frames;
        $base = null;
        $muxSid = 1;

        $transport->onWrite = static function (string $bytes) use (&$queue, &$base, &$muxSid): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                $parts = explode(' ', $head);
                $subject = $parts[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1); // strip trailing "*" -> "_INBOX.<hex>."
                    $muxSid = (int) ($parts[2] ?? 1);
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

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo, $muxSid)];
        };
    }

    /** Rewrites a reply frame's subject+sid onto the request's captured mux reply-to, bytes unchanged. */
    private static function rewriteReplyFrame(string $frame, string $replyTo, int $sid): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo;
        $tokens[2] = (string) $sid;

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    // ── (bool) casts on success flags ─────────────────────────────────────────────────────────────

    /**
     * kills CastBool @ 384 (deleteStream): `(bool) ($response['success'] ?? false)` -> unwrapped.
     * A JSON `success` of 1 (int) is cast to the boolean true by the real code; the unwrapped mutant
     * returns the raw int 1 from a `: bool` closure under strict_types, which is a TypeError (and even
     * absent that, `assertTrue` demands === true, not a truthy 1).
     */
    public function testDeleteStreamCastsNonBooleanSuccessToBool(): void
    {
        [$client] = $this->connect([self::msg('{"success":1}')]);

        self::assertTrue($client->jetStream()->deleteStream('ORDERS')->await());
    }

    /**
     * kills CastBool @ 549 (deleteMessage): same mechanism as deleteStream, on the STREAM.MSG.DELETE ack.
     */
    public function testDeleteMessageCastsNonBooleanSuccessToBool(): void
    {
        [$client] = $this->connect([self::msg('{"success":1}')]);

        self::assertTrue($client->jetStream()->deleteMessage('ORDERS', 5)->await());
    }

    /**
     * kills CastBool @ 1555 (deleteConsumer): same mechanism on the CONSUMER.DELETE ack.
     */
    public function testDeleteConsumerCastsNonBooleanSuccessToBool(): void
    {
        [$client] = $this->connect([self::msg('{"success":1}')]);

        self::assertTrue($client->jetStream()->deleteConsumer('ORDERS', 'PROC')->await());
    }

    // ── array_filter(is_array) in list mappers ────────────────────────────────────────────────────

    /**
     * kills UnwrapArrayFilter @ 416 (listStreams): the real code filters `$raw` down to array entries
     * before `array_map(fn (array $stream) => StreamInfo::fromArray($stream))`. The mutant drops the
     * filter, so a non-array element (123) reaches the `array`-typed callback -> strict TypeError. The
     * valid element still hydrates to a StreamInfo, so the real path returns exactly one.
     */
    public function testListStreamsDropsNonArrayStreamEntry(): void
    {
        [$client] = $this->connect([self::msg('{"streams":[{"config":{"name":"S1"}},123],"total":1}')]);

        $streams = $client->jetStream()->listStreams()->await();

        self::assertCount(1, $streams);
        self::assertInstanceOf(StreamInfo::class, $streams[0]);
        self::assertSame('S1', $streams[0]->name);
    }

    /**
     * kills UnwrapArrayFilter @ 435 (listConsumers): same is_array filter, on the CONSUMER.LIST page.
     * A non-array entry (99) would reach ConsumerInfo::fromArray(array ...) without the filter.
     */
    public function testListConsumersDropsNonArrayConsumerEntry(): void
    {
        [$client] = $this->connect([self::msg('{"consumers":[{"config":{"durable_name":"C1"}},99],"total":1}')]);

        $consumers = $client->jetStream()->listConsumers('ORDERS')->await();

        self::assertCount(1, $consumers);
        self::assertInstanceOf(ConsumerInfo::class, $consumers[0]);
        self::assertSame('C1', $consumers[0]->name);
    }

    // ── idleHeartbeatNs <= 0 guard on subscribeOrderedConsumer ────────────────────────────────────

    /**
     * kills LessThanOrEqualTo @ 1205 (`<= 0` -> `< 0`) AND LogicalAndAllSubExprNegation @ 1205 on the
     * ordered-consumer heartbeat guard. Zero is the boundary: the real guard rejects it, while both
     * mutants let idleHeartbeatNs=0 through and never throw synchronously.
     */
    public function testSubscribeOrderedConsumerRejectsZeroIdleHeartbeat(): void
    {
        [$client] = $this->connect([]);

        $this->expectException(\InvalidArgumentException::class);
        $client->jetStream()->subscribeOrderedConsumer('ORDERS', static function (): void {}, null, 0);
    }

    // ── self::assertValidJsName() removals on non-delegating methods ───────────────────────────────

    /**
     * kills MethodCallRemoval @ 1549 (deleteConsumer stream check). deleteConsumer issues its request
     * directly (no re-validating delegate), so removing the stream guard lets a dotted stream name
     * through to the wire and the pre-armed success reply resolves it - no rejection is thrown.
     */
    public function testDeleteConsumerRejectsInvalidStreamName(): void
    {
        [$client] = $this->connect([self::msg('{"success":true}')]);

        try {
            $client->jetStream()->deleteConsumer('bad.stream', 'PROC')->await();
            self::fail('expected an Invalid-stream-name rejection');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Invalid stream name', $e->getMessage());
        }
    }

    /**
     * kills MethodCallRemoval @ 1567 (pauseConsumer stream check).
     */
    public function testPauseConsumerRejectsInvalidStreamName(): void
    {
        [$client] = $this->connect([self::msg('{"success":true}')]);

        try {
            $client->jetStream()->pauseConsumer('bad.stream', 'PROC', '2026-01-01T00:00:00Z')->await();
            self::fail('expected an Invalid-stream-name rejection');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Invalid stream name', $e->getMessage());
        }
    }

    /**
     * kills MethodCallRemoval @ 1583 (resumeConsumer stream check).
     */
    public function testResumeConsumerRejectsInvalidStreamName(): void
    {
        [$client] = $this->connect([self::msg('{"success":true}')]);

        try {
            $client->jetStream()->resumeConsumer('bad.stream', 'PROC')->await();
            self::fail('expected an Invalid-stream-name rejection');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Invalid stream name', $e->getMessage());
        }
    }

    /**
     * kills MethodCallRemoval @ 1600 (unpinConsumer stream check). A valid group is supplied so the only
     * thing standing between a dotted stream name and the wire is the stream guard.
     */
    public function testUnpinConsumerRejectsInvalidStreamName(): void
    {
        [$client] = $this->connect([self::msg('{"success":true}')]);

        try {
            $client->jetStream()->unpinConsumer('bad.stream', 'PROC', 'g1')->await();
            self::fail('expected an Invalid-stream-name rejection');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Invalid stream name', $e->getMessage());
        }
    }

    /**
     * kills MethodCallRemoval @ 681 (directGetLastForSubjects stream check). With an EMPTY subject list
     * the coroutine returns [] before touching directGetBatch (whose own stream guard would otherwise
     * mask this removal), so removing the guard turns an invalid-stream rejection into a silent [].
     */
    public function testDirectGetLastForSubjectsRejectsInvalidStreamName(): void
    {
        [$client] = $this->connect([]);

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name');
        $client->jetStream()->directGetLastForSubjects('bad.stream', [])->await();
    }
}
