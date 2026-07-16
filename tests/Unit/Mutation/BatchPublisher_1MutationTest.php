<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Second mutation-killing chunk for {@see \IDCT\NATS\JetStream\BatchPublisher}. Each method pins the
 * exact observable behaviour that one surviving Infection mutant would change, focused on the version
 * pre-flight (#152), the reply-shape detection (#130), and the diagnostic each failure surfaces. See
 * the per-method docblocks naming the exact mutant (line + mutator) killed.
 */
final class BatchPublisher_1MutationTest extends TestCase
{
    /** The FakeTransport backing the most recent {@see clientWithVersion()} client (write inspection). */
    private FakeTransport $transport;

    /**
     * Builds a connected client advertising the given INFO version whose muxed request inbox (#118)
     * echoes the supplied reply frames FIFO on each request's captured reply-to (see {@see muxReplies()}).
     * The backing transport is stored in {@see $transport} so a test can inspect what reached the wire.
     *
     * @param list<string> $reads Pre-built MSG/HMSG reply frames; only their subject+sid are rewritten.
     */
    private function clientWithVersion(string $version, array $reads = []): NatsClient
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"' . $version
            . '","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

        $this->transport = new FakeTransport([$info, "PONG\r\n"]);
        $this->muxReplies($this->transport, $reads);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 2_000), $this->transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Installs a dynamic mux responder (post-#118): it learns the wildcard "<base>.*" registration and,
     * for each request PUB/HPUB whose reply-to lives under that base, pops the next reply frame and
     * re-emits it on the CAPTURED reply-to with the mux sid (1). Frames are delivered FIFO in write order.
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
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1); // "_INBOX.<hex>."
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                return [];
            }

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo)];
        };
    }

    /**
     * Rewrites a pre-built MSG/HMSG reply frame's subject (token 1) to the request's captured mux
     * reply-to and its sid (token 2) to the mux sid (1), preserving the payload/header bytes verbatim.
     */
    private static function rewriteReplyFrame(string $frame, string $replyTo): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo;
        $tokens[2] = '1';

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    /** Builds a NATS MSG frame for sid with the given payload. */
    private static function msg(string $inbox, int $sid, string $payload): string
    {
        return sprintf("MSG %s %d %d\r\n%s\r\n", $inbox, $sid, strlen($payload), $payload);
    }

    /** Zero-byte batch-start accept frame (the ADR-50 accept ack). */
    private static function zeroAck(): string
    {
        return "MSG _INBOX.a 1 0\r\n\r\n";
    }

    /** Multi-message commit PubAck carrying the batch id and count. */
    private static function batchAck(string $batchId, int $count): string
    {
        $ack = sprintf('{"stream":"ORDERS","seq":%d,"batch":"%s","count":%d}', $count, $batchId, $count);

        return self::msg('_INBOX.b', 2, $ack);
    }

    /** Count of frames actually written that carry a Nats-Batch-Id (i.e. batch frames on the wire). */
    private static function batchWriteCount(FakeTransport $transport): int
    {
        return count(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'Nats-Batch-Id:'),
        ));
    }

    // -----------------------------------------------------------------------------------------------
    // commit() version pre-flight - major-version comparison (line 114)
    // -----------------------------------------------------------------------------------------------

    /**
     * Kills IncrementInteger @ 114 ($version[0] < 2  =>  $version[1] < 2). A server whose MAJOR
     * version is below 2 (here "1.99") cannot honor batch semantics: the pre-flight must reject it
     * before any byte is written. The mutant tests the MINOR instead - for [1, 99] that is 99 < 2
     * (false) and the second clause needs major === 2 (false), so it would wrongly PROCEED and
     * publish the batch. Pinned by: throws UnsupportedFeatureException naming "1.99" and NOTHING is
     * written (even though replies are scripted for the would-be proceed path).
     */
    public function testPreflightRejectsSubTwoMajorVersion(): void
    {
        // Scripted replies for the mutant's (wrong) proceed path, so it completes deterministically
        // instead of timing out - the clean pre-flight must consume none of them.
        $client = $this->clientWithVersion('1.99', [
            self::zeroAck(),
            self::batchAck('b-major', 2),
        ]);

        try {
            $client->jetStream()->batch('b-major')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();
            self::fail('a sub-2.0 major version must trip the pre-flight before any write');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('1.99', $e->serverVersion);
        }

        self::assertSame(0, self::batchWriteCount($this->transport), 'the pre-flight must publish nothing');
    }

    // -----------------------------------------------------------------------------------------------
    // versionComponents() - anchored numeric prefix (line 239) and capture group (line 243)
    // -----------------------------------------------------------------------------------------------

    /**
     * Kills PregMatchRemoveCaret @ 239 (/^v?(\d+).../  =>  /v?(\d+).../). The version regex is anchored:
     * a string that does NOT begin with a (optionally v-prefixed) number - a proxy/custom build like
     * "release-1.5" - has no parseable version, so the pre-flight is skipped and commit() proceeds.
     * Without the caret the mutant scans the whole string, finds the embedded "1.5", reads it as major 1,
     * and wrongly trips the pre-flight. Pinned by: commit() succeeds and returns the batch ack.
     */
    public function testUnanchoredVersionDoesNotTripPreflight(): void
    {
        $client = $this->clientWithVersion('release-1.5', [
            self::zeroAck(),
            self::batchAck('b-anchor', 2),
        ]);

        $ack = $client->jetStream()->batch('b-anchor')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->commit()
            ->await();

        self::assertSame(2, $ack->batchCount);
        self::assertSame('b-anchor', $ack->batchId);
    }

    /**
     * Kills DecrementInteger @ 243 ((int) $match[1]  =>  (int) $match[0]). The major version is capture
     * group 1, not the full match: for a v-prefixed "v2.12.0" the full match ($match[0]) is "v2.12",
     * which casts to int 0 (leading 'v' is non-numeric), while the capture ($match[1]) is "2". The
     * mutant therefore reads major 0 (< 2) and wrongly rejects a fully batch-capable 2.12 server.
     * Pinned by: commit() proceeds and returns the batch ack.
     */
    public function testVPrefixedVersionUsesCaptureGroupNotFullMatch(): void
    {
        $client = $this->clientWithVersion('v2.12.0', [
            self::zeroAck(),
            self::batchAck('b-vprefix', 2),
        ]);

        $ack = $client->jetStream()->batch('b-vprefix')
            ->add('orders.created', 'a')
            ->add('orders.created', 'b')
            ->commit()
            ->await();

        self::assertSame(2, $ack->batchCount);
        self::assertSame('b-vprefix', $ack->batchId);
    }

    // -----------------------------------------------------------------------------------------------
    // assertStartAccepted() - reply-shape detection (line 224)
    // -----------------------------------------------------------------------------------------------

    /**
     * Kills LogicalOr @ 224 (isset(stream) || isset(seq)  =>  isset(stream) && isset(seq)). A batch-start
     * answered by ANY normal-PubAck shape (here a reply carrying only "stream", no "seq") means the
     * server stored the start as a plain publish and does not understand batch headers - commit() must
     * abort. The mutant only aborts when BOTH fields are present, so a stream-only ack would slip
     * through and the "batch" would be stored message-by-message. Pinned by: throws
     * UnsupportedFeatureException on a stream-only start reply.
     */
    public function testStartRejectedWhenReplyCarriesStreamButNoSeq(): void
    {
        $client = $this->clientWithVersion('2.12.0', [
            self::msg('_INBOX.a', 1, '{"stream":"ORDERS"}'),
            // Only reached if the mutant wrongly accepts the start and proceeds to commit.
            self::batchAck('b-oronly', 2),
        ]);

        try {
            $client->jetStream()->batch('b-oronly')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();
            self::fail('a stream-only (no-seq) start ack must abort the batch');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_atomic', $e->feature);
        }
    }

    // -----------------------------------------------------------------------------------------------
    // atomicBatchUnsupported() - which diagnostic each failure path surfaces (lines 115, 254, 260)
    // -----------------------------------------------------------------------------------------------

    /**
     * The pre-flight failure carries the "nothing was published" wording (data is intact) and names the
     * connected server version. This pins the full message, killing several mutants at once:
     *   - TrueValue @ 115  (preflight: true => false): flips the wording to "treated ... as plain publishes".
     *   - Ternary  @ 260   (message branch swap): same flip on the preflight branch.
     *   - NotIdentical @ 254 (!== null => === null) and (!== '' => === ''): drops " 2.11.4" from the message.
     *   - LogicalAndAllSubExprNegation / LogicalAndNegation @ 254: likewise drop the version.
     *   - Ternary @ 254 (branch swap): likewise drops the version.
     */
    public function testPreflightMessageSaysNothingPublishedAndNamesVersion(): void
    {
        $client = $this->clientWithVersion('2.11.4');

        try {
            $client->jetStream()->batch('b-msg')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();
            self::fail('a pre-2.12 version must trip the pre-flight');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame(
                'Atomic batch publish requires NATS server 2.12+ (connected server 2.11.4; nothing was published)',
                $e->getMessage(),
            );
        }

        self::assertSame(0, self::batchWriteCount($this->transport), 'the pre-flight must publish nothing');
    }

    /**
     * Kills FalseValue @ 254... (the $preflight default) — precisely: FalseValue @ 251 (default
     * bool $preflight = false => true). The reply-shape detection calls atomicBatchUnsupported() with
     * NO argument, relying on the default false to produce the "treated the batch as plain publishes"
     * wording (the start message was already stored). Flipping the default to true would mislabel this
     * as "nothing was published", hiding that an orphan message exists (#152). Pinned by the exact
     * reply-shape message on an unparseable-version server whose start is acked as a plain publish.
     */
    public function testReplyShapeMessageSaysTreatedAsPlainPublishes(): void
    {
        // Unparseable version => pre-flight is skipped; the plain-PubAck start ack drives reply-shape.
        $client = $this->clientWithVersion('synadia-custom', [
            self::msg('_INBOX.a', 1, '{"stream":"ORDERS","seq":41}'),
        ]);

        try {
            $client->jetStream()->batch('b-shape')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();
            self::fail('a plain-PubAck start ack must abort via reply-shape detection');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame(
                'Atomic batch publish requires NATS server 2.12+ (connected server synadia-custom treated the batch as plain publishes)',
                $e->getMessage(),
            );
        }
    }
}
