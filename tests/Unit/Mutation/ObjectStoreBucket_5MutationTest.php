<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for src/JetStream/ObjectStore/ObjectStoreBucket.php (chunk 5).
 *
 * These pin the leader STREAM.MSG.GET fallback path reached from info() on a Direct Get 503:
 * info() -> fetchInfo(name, false) -> requestStreamMessage() -> decodeMetadataFromApiMessage().
 * Each test pins an exact observable behaviour (a written request body, an ObjectInfo field, or a
 * null return) that a specific surviving mutant would change. Frames are fed deterministically
 * through FakeTransport and the in-process Amp loop - no sockets, no sleeps, no wall-clock timing.
 * Source must NOT be modified.
 */
final class ObjectStoreBucket_5MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /** URL-safe base64 (with padding), matching the Object Store meta-subject encoding. */
    private function encodeName(string $name): string
    {
        return strtr(base64_encode($name), '+/', '-_');
    }

    private function digestOf(string $data): string
    {
        return 'SHA-256=' . strtr(base64_encode(hash('sha256', $data, true)), '+/', '-_');
    }

    private function connectedClient(FakeTransport $transport): NatsClient
    {
        // A small request timeout keeps a mis-wired reply from hanging on the 10 s default.
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Installs a dynamic mux-inbox responder (#118): learns the mux base from the wildcard SUB and, for
     * each request PUB/HPUB whose reply-to lives under that base, pops the next reply frame FIFO and
     * re-emits it on the CAPTURED reply-to with the mux sid (1). Frames are delivered in the order the
     * requests are written; every read here funnels through request() (info() Direct Get, then the
     * STREAM.MSG.GET fallback), so FIFO ordering maps each reply to its request unchanged.
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
     * and the mux sid (1), preserving the payload/header bytes and declared lengths verbatim.
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

    /** Direct Get status-only reply (HMSG), e.g. a 503 no-responders that triggers the fallback. */
    private function directStatusReply(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\nDescription: {$description}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    /** A MSG reply frame carrying a JSON body (the caller supplies the raw JSON string verbatim). */
    private function jsonReply(string $json): string
    {
        return sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($json), $json);
    }

    /**
     * Returns the single FakeTransport write frame that carries the given needle (or fails the test).
     */
    private function frameContaining(FakeTransport $transport, string $needle): string
    {
        $match = null;
        foreach ($transport->writes as $write) {
            if (str_contains($write, $needle)) {
                $match = $write;
            }
        }

        self::assertNotNull($match, 'No write frame containing ' . $needle);

        return $match;
    }

    /**
     * fetchInfo() must surface the STREAM.MSG.GET record's stream sequence as ObjectInfo::revision,
     * casting the "seq" field to int. When the server reports seq as a JSON STRING, the (int) cast is
     * load-bearing: ObjectInfo::fromArray()'s revision parameter is typed ?int and the call site runs
     * under strict_types, so passing the raw string "42" raises a TypeError instead of yielding an int.
     *
     * Kills: CastInt @ 746  ((int) $message['seq']  ->  $message['seq']).
     */
    public function testFetchInfoFallbackCastsStringSeqToIntRevision(): void
    {
        $enc = $this->encodeName('doc.txt');
        $metaJson = (string) json_encode([
            'name' => 'doc.txt', 'bucket' => 'assets', 'nuid' => 'n1', 'size' => 5,
            'chunks' => 1, 'digest' => $this->digestOf('hello'), 'deleted' => false,
        ], JSON_THROW_ON_ERROR);
        // seq is deliberately a STRING so the (int) cast is observable (else revision would be "42").
        $response = (string) json_encode([
            'message' => [
                'subject' => '$O.assets.M.' . $enc,
                'seq' => '42',
                'data' => base64_encode($metaJson),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 503, 'No Responders'), // info() Direct Get -> 503 -> fallback
            $this->jsonReply($response),                        // STREAM.MSG.GET success, seq="42"
        ]);

        $info = $this->connectedClient($transport)->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNotNull($info);
        // Real -> (int)"42" == 42; mutant keeps the string "42" and ObjectInfo::fromArray(?int) TypeErrors.
        self::assertSame(42, $info->revision);
    }

    /**
     * requestStreamMessage() must ask the leader for the LATEST record of the meta subject by sending a
     * {"last_by_subj":"$O.assets.M.<enc>"} body on the STREAM.MSG.GET request. Dropping the array item
     * would send an empty "[]" body, which asks for the wrong (or no) message. The assertion targets the
     * STREAM.MSG.GET frame specifically (the earlier Direct Get frame also embeds "last_by_subj", so a
     * whole-writes substring check would not catch this).
     *
     * Kills: ArrayItemRemoval @ 1145  (['last_by_subj' => $subject]  ->  []).
     */
    public function testRequestStreamMessageBodyCarriesLastBySubjFilter(): void
    {
        $enc = $this->encodeName('doc.txt');
        $metaJson = (string) json_encode([
            'name' => 'doc.txt', 'bucket' => 'assets', 'nuid' => 'n1', 'size' => 5,
            'chunks' => 1, 'digest' => $this->digestOf('hello'), 'deleted' => false,
        ], JSON_THROW_ON_ERROR);
        $response = (string) json_encode([
            'message' => [
                'subject' => '$O.assets.M.' . $enc,
                'seq' => 7,
                'data' => base64_encode($metaJson),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 503, 'No Responders'), // info() Direct Get -> 503 -> fallback
            $this->jsonReply($response),                        // STREAM.MSG.GET success
        ]);

        $info = $this->connectedClient($transport)->jetStream()->objectStore('assets')->info('doc.txt')->await();
        self::assertNotNull($info);
        self::assertSame('doc.txt', $info->name);

        // Isolate the STREAM.MSG.GET request frame; its body must be the last_by_subj filter, not "[]".
        $msgGet = $this->frameContaining($transport, '$JS.API.STREAM.MSG.GET.OBJ_assets');
        self::assertStringContainsString('"last_by_subj":"$O.assets.M.' . $enc . '"', $msgGet);
        self::assertStringNotContainsString("\r\n[]\r\n", $msgGet);
    }

    /**
     * decodeMetadataFromApiMessage() must coerce the STREAM.MSG.GET record's "data" field to string
     * before the empty/base64 handling. When the server reports "data" as a non-string (a JSON number),
     * the (string) cast lets the value flow through the base64/JSON guards and cleanly resolve to a
     * not-found (null) info(). Dropping the cast passes the raw int to base64_decode(string $s, ...),
     * which raises a TypeError under strict_types instead of returning null.
     *
     * Kills: CastString @ 1195  ((string) ($message['data'] ?? '')  ->  $message['data'] ?? '').
     */
    public function testFetchInfoCastsNonStringDataFieldAndReturnsNull(): void
    {
        $enc = $this->encodeName('doc.txt');
        // "data" is a JSON NUMBER, not a string, so only the (string) cast keeps the decode from crashing.
        $response = (string) json_encode([
            'message' => [
                'subject' => '$O.assets.M.' . $enc,
                'seq' => 9,
                'data' => 12345,
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 503, 'No Responders'), // info() Direct Get -> 503 -> fallback
            $this->jsonReply($response),                        // STREAM.MSG.GET with numeric data
        ]);

        $info = $this->connectedClient($transport)->jetStream()->objectStore('assets')->info('doc.txt')->await();

        // Real -> the numeric data base64-decodes to non-JSON -> metadata null -> info() null.
        // Mutant -> base64_decode(int) TypeErrors under strict_types instead of returning null.
        self::assertNull($info);
    }
}
