<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class BatchPublisherTest extends TestCase
{
    /**
     * Post-#118 the request inbox is MUXED: request()/requestWithHeaders() no longer subscribe a fresh
     * inbox per call; instead ONE long-lived wildcard "<base>.*" (sid 1) serves every reply, and each
     * request publishes reply-to "<base>.<token>" and routes by that token. A batch commit() funnels its
     * start/commit legs through request(), so a pre-seeded "MSG _INBOX.x <sid>" no longer reaches the
     * waiting request (its subject is not the request's random "<base>.<token>", so dispatchMuxReply drops
     * it and the request times out).
     *
     * This installs a dynamic responder that mirrors a real server: it learns the mux base from the
     * wildcard SUB, and for each request PUB/HPUB (a publish whose reply-to lives under that base) pops the
     * next reply frame and re-emits it on the CAPTURED reply-to with the mux sid (1). Frames are delivered
     * FIFO in the order requests are written, so a multi-message batch (start request then commit request)
     * gets its replies in sequence, while the intermediate fire-and-forget HPUBs (no reply-to) consume
     * nothing.
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
                // A plain publish, or a batch intermediate (fire-and-forget, no reply-to) - not a request.
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
     * A parseable pre-2.12 INFO version means the connected server cannot honor batch semantics:
     * commit() must fail the version pre-flight BEFORE anything reaches the wire. The reply-shape
     * detection (#130) fires only AFTER the start message is durably stored, which leaves one
     * orphan message from an all-or-nothing API (#152). The scripted plain PubAck reply must go
     * unconsumed: not a single batch frame may be written.
     */
    public function testCommitPreflightRejectsPre212ServerBeforeAnyWrite(): void
    {
        $plainPubAck = '{"stream":"ORDERS","seq":41}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.11.4","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // How a pre-2.12 server WOULD ack the start request if it were sent; the pre-flight
            // must abort before the request goes out, leaving this reply unread.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($plainPubAck), $plainPubAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b-old')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c');

        try {
            $batch->commit()->await();
            self::fail('expected UnsupportedFeatureException from the pre-2.12 version pre-flight');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_atomic', $e->feature);
            self::assertSame('2.12', $e->requiredVersion);
            self::assertSame('2.11.4', $e->serverVersion);
        }

        $batchWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
        ));
        self::assertCount(0, $batchWrites, 'the pre-flight must reject the batch before any message is published');
    }

    /**
     * The pre-flight parses lenient version strings: a two-segment "2.11" is below 2.12 and must
     * be rejected before any write - including for a single-message batch, whose plain PubAck a
     * pre-2.12 server would otherwise silently accept as a normal publish (#152).
     */
    public function testCommitPreflightRejectsTwoSegmentPre212Version(): void
    {
        $plainPubAck = '{"stream":"ORDERS","seq":7}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.11","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // The plain PubAck a pre-2.12 server WOULD return for the lone message; must stay unread.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($plainPubAck), $plainPubAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b-two-segment')->add('orders.created', 'only');

        try {
            $batch->commit()->await();
            self::fail('expected UnsupportedFeatureException from the two-segment version pre-flight');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('2.11', $e->serverVersion);
        }

        self::assertStringNotContainsString('Nats-Batch-Id:', implode('', $transport->writes));
    }

    /**
     * A 2.12 pre-release ("2.12.0-beta.1") DOES understand batch semantics: the pre-flight must
     * compare the numeric prefix (nats.go-style), not full semver precedence where a pre-release
     * orders BELOW its release - commit() proceeds and the batch commits normally (#152).
     */
    public function testCommitProceedsOnPrereleaseOf212(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":2,"batch":"b-beta","count":2}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0-beta.1","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Zero-byte ack to the batch-start request, then the commit PubAck (FIFO on the mux inbox).
            "MSG _INBOX.a 1 0\r\n\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->batch('b-beta')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->commit()
            ->await();

        self::assertSame(2, $ack->batchCount);
        self::assertSame('b-beta', $ack->batchId);
    }

    /**
     * An unparseable INFO version (proxy / custom build) must NOT trip the pre-flight: commit()
     * falls through to the reply-shape detection, which aborts on the plain PubAck to the start
     * request after exactly one write (#152 keeping #130 as defense in depth).
     */
    public function testCommitUnparseableVersionFallsThroughToReplyShapeDetection(): void
    {
        $plainPubAck = '{"stream":"ORDERS","seq":41}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"synadia-custom","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // The batch-incapable server acks the batch-start request with a NORMAL PubAck.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($plainPubAck), $plainPubAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b-unparseable')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c');

        try {
            $batch->commit()->await();
            self::fail('expected UnsupportedFeatureException for a plain-publish batch start ack');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_atomic', $e->feature);
            self::assertSame('synadia-custom', $e->serverVersion);
        }

        $batchWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
        ));
        self::assertCount(1, $batchWrites, 'only the start message may reach the wire before the abort');
    }

    /**
     * commit() on a NEVER-CONNECTED client: serverInfo() is null, so the version pre-flight must
     * null-safely skip the gate (unknown version falls through to reply-shape detection) and the
     * failure surfaces as the transport-level NatsException from the request - never as a PHP
     * Error from dereferencing the missing ServerInfo.
     */
    public function testCommitBeforeConnectSkipsVersionPreflightNullSafely(): void
    {
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 100), new FakeTransport());

        $batch = $client->jetStream()->batch('b-unconnected')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b');

        try {
            $batch->commit()->await();
            self::fail('an unconnected commit must fail at the wire, not succeed');
        } catch (\IDCT\NATS\Exception\NatsException $e) {
            // The failure is the request layer's, reached only because the null serverInfo()
            // pre-flight fell through instead of crashing on a null dereference.
            self::assertNotInstanceOf(UnsupportedFeatureException::class, $e);
        }
    }

    /**
     * Reply-shape abort with NO advertised server version (INFO omits `version`): the
     * UnsupportedFeatureException message must omit the version slot cleanly - no dangling space
     * from gluing an empty version onto "connected server".
     */
    public function testReplyShapeAbortMessageOmitsMissingServerVersionCleanly(): void
    {
        $plainPubAck = '{"stream":"ORDERS","seq":41}';

        $transport = new FakeTransport([
            // No "version" field: ServerInfo::version defaults to '' and the pre-flight cannot parse it.
            'INFO {"server_id":"S1","server_name":"n1","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // The batch-incapable server acks the batch-start request with a NORMAL PubAck.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($plainPubAck), $plainPubAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b-no-version')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b');

        try {
            $batch->commit()->await();
            self::fail('expected UnsupportedFeatureException for a plain-publish batch start ack');
        } catch (UnsupportedFeatureException $e) {
            // Exact wording: with no version, "connected server" is followed directly by "treated"
            // (single space) - an unknown version must not inject a blank version token.
            self::assertSame(
                'Atomic batch publish requires NATS server 2.12+ (connected server treated the batch as plain publishes)',
                $e->getMessage(),
            );
        }
    }

    /**
     * Reply-shape detection in a mixed-version cluster: the connected server advertises 2.12+ (so
     * the pre-flight passes) but the JS leader is older and answers the batch START with a normal
     * PubAck instead of ADR-50's zero-byte ack - it stored the message as a plain publish.
     * Continuing would store the whole "batch" message-by-message, silently breaking the
     * all-or-nothing guarantee: commit() must abort with UnsupportedFeatureException before
     * publishing the remaining messages (#130).
     */
    public function testCommitAbortsWhenBatchStartIsAcknowledgedAsPlainPublish(): void
    {
        $plainPubAck = '{"stream":"ORDERS","seq":41}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.1","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // The pre-2.12 JS leader acks the batch-start request with a NORMAL PubAck.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($plainPubAck), $plainPubAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b-old')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c');

        try {
            $batch->commit()->await();
            self::fail('expected UnsupportedFeatureException for a plain-publish batch start ack');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_atomic', $e->feature);
            self::assertSame('2.12', $e->requiredVersion);
            self::assertSame('2.12.1', $e->serverVersion);
        }

        // The batch aborted at the start: no intermediate or commit frames were published.
        $batchWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
        ));
        self::assertCount(1, $batchWrites, 'only the start message may reach the wire before the abort');
    }

    /**
     * Defense in depth for the commit leg: a multi-message commit acknowledged by a PubAck WITHOUT
     * the batch id/count means the server committed nothing as a batch - it stored messages
     * individually. commit() must throw UnsupportedFeatureException, not report success (#130).
     */
    public function testCommitRejectsMultiMessageAckWithoutBatchFields(): void
    {
        $plainCommitAck = '{"stream":"ORDERS","seq":42}';

        $transport = new FakeTransport([
            // Version 2.12+ so the #152 pre-flight passes and the commit-leg guard itself is
            // exercised (a pre-2.12 version would short-circuit this test before any write).
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.1","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Zero-byte ack to the batch-start request, then a plain PubAck (no batch/count) to the
            // commit request (FIFO on the mux inbox).
            "MSG _INBOX.a 1 0\r\n\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($plainCommitAck), $plainCommitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->batch('b-nofields')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();
            self::fail('a multi-message commit acked without batch fields must be rejected');
        } catch (UnsupportedFeatureException) {
            // Expected: the commit-leg guard fired, not the pre-flight - prove it saw the wire.
            $batchWrites = array_values(array_filter(
                $transport->writes,
                static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
            ));
            self::assertCount(2, $batchWrites, 'start and commit must both have reached the wire');
        }
    }

    /**
     * A single-message batch is trivially atomic, so a plain PubAck without batch fields is an
     * acceptable commit ack (the guard must not reject it) (#130).
     */
    public function testSingleMessageBatchAcceptsPlainPubAck(): void
    {
        $plainCommitAck = '{"stream":"ORDERS","seq":7}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($plainCommitAck), $plainCommitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->batch('b-single')
            ->add('orders.created', 'only')
            ->commit()
            ->await();

        self::assertSame(7, $ack->seq);
        self::assertNull($ack->batchCount);
    }

    /**
     * A no-responders reply to the commit request (a single-message batch sends only the commit
     * request) must surface as JetStreamException(503) - the same taxonomy jsRequest() produces -
     * not a bare NatsException, so a JetStream-disabled server / unbound subject is catchable as
     * JetStreamException like every other JS path (#161). The version pre-flight (2.12) is passed, so
     * the commit request reaches the wire.
     */
    public function testCommitNormalizesNoRespondersTo503(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // The lone commit request hits a subject with no responder -> 503 status reply.
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(503);
        $this->expectExceptionMessage('No JetStream responder');

        $client->jetStream()->batch('b-503')
            ->add('orders.created', 'only')
            ->commit()
            ->await();
    }

    /**
     * A no-responders reply to the batch-START request (a multi-message batch sends the start request
     * first) must likewise surface as JetStreamException(503), not a bare NatsException (#161). The
     * 503 normalization only touches the no-responders case, leaving the #130/#138/#152 reply-shape
     * and version-preflight semantics unchanged.
     */
    public function testStartRequestNormalizesNoRespondersTo503(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // The batch-start request hits a subject with no responder -> 503 status reply;
            // the batch aborts here, so the intermediate/commit messages are never sent.
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(503);
        $this->expectExceptionMessage('No JetStream responder');

        $client->jetStream()->batch('b-503-start')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->commit()
            ->await();
    }

    /**
     * Verifies a committed batch: the START (seq 1) is a request, the intermediate is fire-and-forget,
     * the final carries the commit marker as a request, all share one Nats-Batch-Id, and the commit
     * ack is parsed (issue #8, ADR-50).
     */
    public function testCommitSendsBatchHeadersAndParsesAck(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":3,"batch":"batch-xyz","count":3}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Zero-byte ack to the batch-start request, then the commit PubAck (FIFO on the mux inbox).
            "MSG _INBOX.a 1 0\r\n\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('batch-xyz');
        $ack = $batch
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c')
            ->commit()
            ->await();

        self::assertSame(3, $ack->batchCount);
        self::assertSame('batch-xyz', $ack->batchId);

        $batchWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
        ));

        self::assertCount(3, $batchWrites);

        foreach ($batchWrites as $write) {
            self::assertStringContainsString('Nats-Batch-Id:batch-xyz', $write);
        }

        // START (seq 1): a request (carries an inbox), no commit marker.
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $batchWrites[0]);
        self::assertStringContainsString('Nats-Batch-Sequence:1', $batchWrites[0]);
        self::assertStringNotContainsString('Nats-Batch-Commit', $batchWrites[0]);

        // Intermediate (seq 2): fire-and-forget (no inbox after the subject, just the byte counts).
        self::assertMatchesRegularExpression('/^HPUB orders\.created \d/', $batchWrites[1]);
        self::assertStringContainsString('Nats-Batch-Sequence:2', $batchWrites[1]);
        self::assertStringNotContainsString('Nats-Batch-Commit', $batchWrites[1]);

        // Commit (seq 3): a request carrying the commit marker.
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $batchWrites[2]);
        self::assertStringContainsString('Nats-Batch-Sequence:3', $batchWrites[2]);
        self::assertStringContainsString('Nats-Batch-Commit:1', $batchWrites[2]);
    }

    /**
     * The intermediates of a multi-message commit are fire-and-forget (nothing is awaited from the
     * server between them), so they must be coalesced into ONE transport write instead of one
     * awaited write per message (#138): a 5-message commit performs exactly 3 batch writes - the
     * start request, one write carrying the byte-identical HPUB frames for sequences 2-4 in order,
     * and the commit request.
     */
    public function testCommitCoalescesIntermediatesIntoSingleWrite(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":5,"batch":"b-coalesce","count":5}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Zero-byte ack to the batch-start request, then the commit PubAck (FIFO on the mux inbox).
            "MSG _INBOX.a 1 0\r\n\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->batch('b-coalesce')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c')
            ->add('orders.created', 'd')
            ->add('orders.created', 'e')
            ->commit()
            ->await();

        self::assertSame(5, $ack->batchCount);
        self::assertSame('b-coalesce', $ack->batchId);

        $batchWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
        ));

        // Exactly 3 transport writes carry batch frames: start, ONE coalesced intermediate
        // write, commit. Per-intermediate writes would make this 5.
        self::assertCount(3, $batchWrites);

        // START (seq 1): a request (carries an inbox), no commit marker.
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $batchWrites[0]);
        self::assertStringContainsString('Nats-Batch-Sequence:1', $batchWrites[0]);

        // The middle write is the byte-exact concatenation of the intermediate frames
        // (sequences 2-4, payloads b-d, no reply inbox), in staging order.
        $expected = '';
        foreach ([[2, 'b'], [3, 'c'], [4, 'd']] as [$sequence, $payload]) {
            $headerBlock = sprintf(
                "NATS/1.0\r\nNats-Batch-Id:b-coalesce\r\nNats-Batch-Sequence:%d\r\n\r\n",
                $sequence,
            );
            $expected .= sprintf(
                "HPUB orders.created %d %d\r\n%s%s\r\n",
                strlen($headerBlock),
                strlen($headerBlock) + strlen($payload),
                $headerBlock,
                $payload,
            );
        }
        self::assertSame($expected, $batchWrites[1]);

        // Commit (seq 5): a request carrying the commit marker.
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $batchWrites[2]);
        self::assertStringContainsString('Nats-Batch-Sequence:5', $batchWrites[2]);
        self::assertStringContainsString('Nats-Batch-Commit:1', $batchWrites[2]);
    }

    /**
     * Coalescing must not lose per-message max_payload enforcement (#138): an oversized
     * intermediate (headers + payload above the server's advertised max_payload) throws
     * ProtocolException BEFORE any intermediate reaches the wire - only the already-sent start
     * frame is written, and no commit is attempted.
     */
    public function testOversizedIntermediateThrowsBeforeAnyIntermediateWrite(): void
    {
        $transport = new FakeTransport([
            // The server advertises max_payload 256; enforceMaxPayload() reads this limit.
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":256,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Zero-byte ack to the batch-start request; no further replies - the commit
            // must never be sent.
            "MSG _INBOX.a 1 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b-toolarge')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            // 300-byte payload + batch headers exceeds max_payload 256.
            ->add('orders.created', str_repeat('x', 300))
            ->add('orders.created', 'd');

        try {
            $batch->commit()->await();
            self::fail('Expected the oversized intermediate to throw');
        } catch (ProtocolException $e) {
            self::assertStringContainsString('exceeds server max_payload', $e->getMessage());
        }

        // Only the start frame reached the wire: the whole intermediate block is validated
        // before any of it is written, so even the VALID intermediate (seq 2) was not sent.
        $batchWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
        ));
        self::assertCount(1, $batchWrites, 'only the start message may reach the wire');
        self::assertStringContainsString('Nats-Batch-Sequence:1', $batchWrites[0]);

        $allWrites = implode('', $transport->writes);
        self::assertStringNotContainsString('Nats-Batch-Sequence:2', $allWrites);
        self::assertStringNotContainsString('Nats-Batch-Commit', $allWrites);
    }

    /**
     * Verifies a batch-start rejection (error reply to the start request) aborts before the
     * intermediates/commit are sent (issue #8, ADR-50).
     */
    public function testCommitRejectedAtStart(): void
    {
        $startError = '{"error":{"code":400,"description":"atomic publish not enabled"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($startError), $startError),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('batch-rej')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c');

        try {
            $batch->commit()->await();
            self::fail('Expected the batch-start rejection to throw');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('atomic publish not enabled', $e->getMessage());
        }

        // Only the start message was written; no commit and no further sequence was sent.
        $allWrites = implode('', $transport->writes);
        self::assertStringNotContainsString('Nats-Batch-Commit', $allWrites);
        self::assertStringNotContainsString('Nats-Batch-Sequence:3', $allWrites);
    }

    /**
     * Verifies committing an empty batch is rejected.
     */
    public function testCommitEmptyBatchThrows(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Cannot commit an empty batch');

        $client->jetStream()->batch()->commit()->await();
    }

    /**
     * Verifies a too-long batch id is rejected.
     */
    public function testBatchRejectsOversizedId(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Batch id must be between 1 and 64 characters');

        $client->jetStream()->batch(str_repeat('x', 65));
    }

    /**
     * Verifies adding after commit is rejected.
     */
    public function testAddAfterCommitThrows(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":1,"batch":"b","count":1}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b');
        $batch->add('orders.created', 'a')->commit()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Cannot add to an already-committed batch');

        $batch->add('orders.created', 'b');
    }

    /**
     * Verifies an aborted batch (commit ack carries an error) surfaces as a JetStreamException.
     */
    public function testCommitAbortSurfacesError(): void
    {
        $errorAck = '{"error":{"code":400,"description":"batch consistency check failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorAck), $errorAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('batch consistency check failed');

        $client->jetStream()->batch('b')->add('orders.created', 'a')->commit()->await();
    }

    /**
     * Verifies that adding more than MAX_MESSAGES messages to a batch throws.
     */
    public function testAddExceedingMaxMessagesThrows(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('batch-overflow');

        // Fill the batch up to the maximum using reflection to avoid slow looping.
        $prop = new \ReflectionProperty($batch, 'messages');
        $prop->setValue($batch, array_fill(0, \IDCT\NATS\JetStream\BatchPublisher::MAX_MESSAGES, [
            'subject' => 's',
            'payload' => 'p',
            'headers' => [],
        ]));

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Atomic batch is limited to');

        $batch->add('orders.created', 'overflow');
    }

    /**
     * Verifies the count() method returns the number of staged messages.
     */
    public function testCountReturnsNumberOfStagedMessages(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('batch-count');

        self::assertSame(0, $batch->count());

        $batch->add('orders.created', 'a');
        self::assertSame(1, $batch->count());

        $batch->add('orders.created', 'b');
        self::assertSame(2, $batch->count());
    }

    /**
     * Verifies the batchId() method returns the batch id passed at construction.
     */
    public function testBatchIdReturnsConstructedId(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('my-explicit-id');

        self::assertSame('my-explicit-id', $batch->batchId());
    }

    /**
     * Verifies that calling commit() a second time on an already-committed batch throws.
     */
    public function testDoubleCommitThrows(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":1,"batch":"b2","count":1}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Reply for the first (and only) commit request.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b2');
        $batch->add('orders.created', 'a');

        // First commit must succeed.
        $ack = $batch->commit()->await();
        self::assertSame(1, $ack->batchCount);

        // Second commit must throw "Batch already committed".
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Batch already committed');

        $batch->commit()->await();
    }

    /**
     * commit() must release the staged payloads: they are dead weight afterwards (add() rejects a
     * committed batch), yet an app retaining the publisher (e.g. keyed by batchId() to correlate
     * acks) would pin up to MAX_MESSAGES full payloads for the object's lifetime (#133). The
     * retention itself is not observable through behavior, so the internal array is checked via
     * reflection; count() going to 0 is the public face of the release. The full 3-message wire
     * exchange must still happen (the release must not disturb what is sent).
     */
    public function testCommitReleasesStagedPayloads(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":3,"batch":"b-release","count":3}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Zero-byte ack to the batch-start request, then the commit PubAck (FIFO on the mux inbox).
            "MSG _INBOX.a 1 0\r\n\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $batch = $client->jetStream()->batch('b-release')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->add('orders.created', 'c');

        $ack = $batch->commit()->await();
        self::assertSame(3, $ack->batchCount);

        // All 3 staged messages reached the wire despite the release...
        $batchWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:b-release'),
        ));
        self::assertCount(3, $batchWrites);

        // ...and nothing is retained on the committed publisher.
        $prop = new \ReflectionProperty($batch, 'messages');
        self::assertSame([], $prop->getValue($batch), 'a committed batch must not retain its staged payloads');
        self::assertSame(0, $batch->count());
    }

    /**
     * Verifies that a non-JSON (but non-empty) reply to the batch-start request is treated as
     * accepted and publish continues (in assertStartAccepted).
     */
    public function testNonJsonStartReplyTreatedAsAccepted(): void
    {
        $commitAck = '{"stream":"ORDERS","seq":2,"batch":"batch-nonjson","count":2}';

        // The start reply is non-empty and non-JSON - the server replied with something unexpected
        // but not an error JSON; the client should treat it as accepted and continue.
        $nonJsonStart = 'OK';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Non-JSON reply to the start request (treated as accepted), then the commit PubAck
            // (FIFO on the mux inbox).
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($nonJsonStart), $nonJsonStart),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($commitAck), $commitAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->batch('batch-nonjson')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->commit()
            ->await();

        self::assertSame(2, $ack->batchCount);
        self::assertSame('batch-nonjson', $ack->batchId);
    }

    /**
     * Verifies that a malformed (non-JSON) commit ack throws a JetStreamException
     * (in parseCommitAck).
     */
    public function testMalformedCommitAckThrows(): void
    {
        // The server replies with something that is not valid JSON as the commit ack.
        $badAck = 'not-json-at-all';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Commit request reply (single-message batch - no start request).
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($badAck), $badAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed atomic batch commit ack');

        $client->jetStream()->batch('b-malformed')
            ->add('orders.created', 'a')
            ->commit()
            ->await();
    }
}
