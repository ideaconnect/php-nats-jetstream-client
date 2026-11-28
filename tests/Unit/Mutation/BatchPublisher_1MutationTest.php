<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use PHPUnit\Framework\TestCase;

/**
 * Second mutation-killing chunk for {@see \IDCT\NATS\JetStream\BatchPublisher}. Each method pins the
 * exact observable behaviour that one surviving Infection mutant would change, focused on the absence of
 * a version pre-flight (ADR-44, #272; up to 2.35.0 commit() had the #152 one), the reply-shape detection
 * (#130), and the diagnostic each failure surfaces. See the per-method docblocks naming the exact mutant killed.
 */
final class BatchPublisher_1MutationTest extends TestCase
{
    use OwnsTestResources;

    protected function tearDown(): void
    {
        // Every client and connection a test makes is registered as it is constructed (#183), weakly: the shutdown
        // closes what is left and checks that nothing goes on running into the next test.
        $this->releaseOwnedResources();
    }

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
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 2_000), $this->transport));
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
    // commit() sends the batch whatever version INFO reports (ADR-44, #272)
    // -----------------------------------------------------------------------------------------------

    /**
     * No INFO version decides: a 1.x major, an unanchored "release-1.5", a v-prefixed "v2.12.0" and a pre-2.12
     * "2.11.4" all get the batch on the wire, and it commits when the JetStream API that answers serves atomic
     * batches. Up to 2.35.0 the #152 pre-flight refused the 1.x and 2.11.4 ones before any write; a pre-flight
     * brought back (any version comparison in commit()) fails this test. Pinned by: every message is written and
     * commit() returns the batch ack.
     */
    public function testCommitIsSentWhateverVersionInfoReports(): void
    {
        foreach (['1.99', 'release-1.5', 'v2.12.0', '2.11.4'] as $version) {
            $client = $this->clientWithVersion($version, [
                self::zeroAck(),
                self::batchAck('b-any', 2),
            ]);

            $ack = $client->jetStream()->batch('b-any')
                ->add('orders.created', 'a')
                ->add('orders.created', 'b')
                ->commit()
                ->await();

            self::assertSame(2, $ack->batchCount, $version);
            self::assertSame('b-any', $ack->batchId, $version);
            self::assertSame(2, self::batchWriteCount($this->transport), 'both messages are written on ' . $version);
        }
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
    // atomicBatchUnsupported() - the diagnostic of the reply-shape detection
    // -----------------------------------------------------------------------------------------------

    /**
     * The reply-shape failure says the server treated the batch as plain publishes (the start message is already
     * stored) and reports the connected server's version, which is informational only: the answering server may be
     * another one (#272). This pins the full message on a server whose INFO reports 2.11.4, killing the mutants of
     * the version slot (NotIdentical / LogicalAnd / Ternary on `$serverVersion !== null && $serverVersion !== ''`),
     * which replace "2.11.4" with "unknown"; the "unknown" branch is pinned by
     * BatchPublisherTest::testReplyShapeAbortMessageOmitsMissingServerVersionCleanly().
     */
    public function testReplyShapeMessageSaysTreatedAsPlainPublishesAndReportsVersion(): void
    {
        $client = $this->clientWithVersion('2.11.4', [
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
                'Atomic batch publish requires NATS server 2.12+, and the server treated the batch as plain publishes (connected server reports 2.11.4)',
                $e->getMessage(),
            );
            self::assertSame('2.11.4', $e->serverVersion);
        }

        self::assertSame(1, self::batchWriteCount($this->transport), 'only the start message reaches the wire');
    }

    /**
     * The same wording for an unparseable INFO version: nothing about the string changes the outcome (#272).
     */
    public function testReplyShapeMessageForUnparseableVersion(): void
    {
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
                'Atomic batch publish requires NATS server 2.12+, and the server treated the batch as plain publishes (connected server reports synadia-custom)',
                $e->getMessage(),
            );
        }
    }
}
