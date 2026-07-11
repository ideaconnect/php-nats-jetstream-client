<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;

/**
 * Streaming parser for NATS protocol frames read from transport.
 *
 * Control-line operations (MSG, HMSG, PING, PONG, INFO, +OK, -ERR) are matched case-sensitively as
 * the NATS server emits them - always upper-case per the protocol - so no case-folding is performed.
 */
final class ProtocolParser
{
    /**
     * Maximum length of a single control line (everything up to CRLF: INFO, MSG/HMSG headers, -ERR,
     * etc.). Real control lines are at most a few KB; this bounds an unterminated line so a peer
     * streaming bytes without a CRLF cannot grow the buffer to OOM. maxFrameSize only bounds MSG/HMSG
     * payloads, which are not parsed until their control line completes, so it does not cover this.
     */
    private const MAX_CONTROL_LINE_BYTES = 1048576; // 1 MiB

    /**
     * Default inbound MSG/HMSG bound used before the server's negotiated `max_payload` is known. The
     * connection raises this from INFO once connected so large messages on big-`max_payload` servers are
     * receivable (#94).
     */
    public const DEFAULT_MAX_FRAME_SIZE = 8 * 1024 * 1024; // 8 MiB

    private string $buffer = '';

    /** Maximum total frame size (headers + payload) accepted from the server. */
    private int $maxFrameSize;

    /**
     * Parsed-but-incomplete MSG/HMSG header awaiting its payload bytes. Remembering it means a
     * large payload arriving across many chunks is not re-scanned/re-parsed on every push; each
     * subsequent push only checks whether enough bytes have accumulated.
     *
     * @var array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int}|null
     */
    private ?array $pending = null;

    /**
     * Chunks received while $pending's payload is still incomplete. While a frame is pending no
     * frame boundary can be found before its required bytes arrive, so chunks accumulate here
     * and are joined into $buffer with a single implode once the frame is completable, instead
     * of growing $buffer with a `.=` copy per 8 KiB transport read (#140: ~2-3x constant-factor
     * overhead on multi-MB frames). Invariant: non-empty only while $pending !== null and still
     * short of its required bytes; always flushed into $buffer before the parse loop runs, so
     * the loop's offsets and the pending payloadOffset rebase never see these bytes.
     *
     * @var list<string>
     */
    private array $pendingChunks = [];

    /** Total byte length of $pendingChunks, tracked so completion is detected without joining. */
    private int $pendingChunksLength = 0;

    /**
     * Frames parsed by the current push() call, held on the instance rather than in a local so a
     * ProtocolException thrown while parsing a LATER frame of the same chunk cannot discard
     * already-parsed siblings: the finally-block resync has consumed their bytes, so dropping them
     * would lose them permanently - core NATS never resends, and a reconnect replays SUBs, not
     * missed messages (#147). Drained by push()'s normal return, by {@see takeParsedFrames()} at
     * the catch site after a throw, or - failing that - prepended to the next push()'s result.
     *
     * @var list<ProtocolFrame>
     */
    private array $parsedFrames = [];

    /**
     * Creates a parser for line and payload frames produced by the NATS server.
     *
     * @param int $maxFrameSize Maximum total MSG/HMSG bytes accepted per frame to limit memory usage.
     */
    public function __construct(int $maxFrameSize = self::DEFAULT_MAX_FRAME_SIZE)
    {
        $this->maxFrameSize = $maxFrameSize;
    }

    /**
     * Appends a raw socket chunk and emits all complete frames currently available.
     *
     * @return list<ProtocolFrame>
     */
    public function push(string $chunk): array
    {
        if ($chunk !== '') {
            if ($this->pending === null) {
                $this->buffer .= $chunk;
            } else {
                // A pending payload is incomplete (a pending frame only survives a push while
                // short of its required bytes), so no frame can complete before those bytes
                // arrive: collect chunks aside and join once instead of copying $buffer per push.
                $this->pendingChunks[] = $chunk;
                $this->pendingChunksLength += strlen($chunk);

                $required = $this->pending['payloadOffset'] + $this->pending['totalBytes'] + 2;
                if (strlen($this->buffer) + $this->pendingChunksLength < $required) {
                    // Still short: nothing is parseable, keep accumulating. Bounded by $required,
                    // which parseDataFrameHeader() already capped at maxFrameSize.
                    return [];
                }

                // Frame completable: materialize the buffer with a single join (one copy of every
                // byte) and let the loop below consume the frame plus anything after it.
                $this->buffer = implode('', [$this->buffer, ...$this->pendingChunks]);
                $this->pendingChunks = [];
                $this->pendingChunksLength = 0;
            }
        }

        $offset = 0;
        $bufferLength = strlen($this->buffer);

        // The buffer is trimmed by $offset in the finally even when a parse throws, and the offending
        // bytes are consumed (offset advanced past them) BEFORE each parse that can throw, so a
        // malformed frame cannot remain buffered and re-throw forever (resync instead of poison).
        try {
            while ($offset < $bufferLength) {
                if ($this->pending !== null) {
                    $required = $this->pending['payloadOffset'] + $this->pending['totalBytes'] + 2;
                    if ($bufferLength < $required) {
                        // Payload still incomplete; keep buffered bytes for the next chunk without
                        // re-scanning or re-parsing the control line.
                        break;
                    }

                    // Consume the frame's bytes and clear the pending header before/while building,
                    // so a malformed payload (bad trailing CRLF) cannot leave the bytes buffered.
                    try {
                        $this->parsedFrames[] = $this->buildPendingFrame();
                    } finally {
                        $offset = $required;
                        $this->pending = null;
                    }

                    continue;
                }

                $lineEndPos = strpos($this->buffer, "\r\n", $offset);
                if ($lineEndPos === false) {
                    // No complete control line yet. Bound the unterminated line so a peer that streams
                    // bytes without a CRLF cannot drive the buffer to unbounded growth (OOM).
                    if (($bufferLength - $offset) > self::MAX_CONTROL_LINE_BYTES) {
                        throw new ProtocolException('Control line exceeds maximum length without CRLF');
                    }

                    break;
                }

                $line = substr($this->buffer, $offset, $lineEndPos - $offset);
                $nextOffset = $lineEndPos + 2;

                // Operation verbs are matched case-insensitively and separated from their arguments by any
                // whitespace (space or tab), matching the NATS wire spec; argument separators within the
                // line are also whitespace-tolerant (see splitControlLine()). Real servers always send
                // upper-case verbs, so the case fold below only adds leniency - and is skipped when the
                // verb already matches a canonical form, avoiding a strtoupper() per frame (#140).
                $verb = substr($line, 0, strcspn($line, " \t"));
                if (
                    $verb !== 'MSG' && $verb !== 'HMSG' && $verb !== 'PING' && $verb !== 'PONG'
                    && $verb !== '+OK' && $verb !== '-ERR' && $verb !== 'INFO'
                ) {
                    $verb = strtoupper($verb);
                }

                if ($verb === 'MSG' || $verb === 'HMSG') {
                    // Consume the control line first so a malformed header resyncs past it on throw.
                    // The next loop iteration (or a later push) completes the payload.
                    $offset = $nextOffset;
                    $this->pending = $this->parseDataFrameHeader($verb, $line, $nextOffset);

                    continue;
                }

                // Consume the control line first so an unsupported/invalid line resyncs past it.
                $offset = $nextOffset;
                $this->parsedFrames[] = $this->parseControlFrame($verb, $line);
            }
        } finally {
            if ($offset > 0) {
                $this->buffer = substr($this->buffer, $offset);

                // Keep the pending payload offset relative to the trimmed buffer.
                if ($this->pending !== null) {
                    $this->pending['payloadOffset'] -= $offset;
                }
            }
        }

        return $this->takeParsedFrames();
    }

    /**
     * Drains frames that parsed successfully before a push() threw mid-chunk. The throw aborts
     * push() before its normal return, but the finally-block resync has already consumed those
     * frames' bytes, so the catch site must collect them here - a frame left undrained is only
     * deferred (the next push() returns it first), never dropped (#147).
     *
     * @return list<ProtocolFrame>
     */
    public function takeParsedFrames(): array
    {
        $frames = $this->parsedFrames;
        $this->parsedFrames = [];

        return $frames;
    }

    /**
     * Parses line-based control frames that do not carry trailing payload bytes. $verb is the
     * upper-cased leading operation token; the payload (INFO/-ERR) is read from the original $line so its
     * case is preserved.
     */
    private function parseControlFrame(string $verb, string $line): ProtocolFrame
    {
        if ($verb === 'INFO') {
            // substr past the 4-char "INFO" verb, then drop the single separating whitespace.
            return new ProtocolFrame(
                type: ProtocolFrameType::Info,
                infoPayload: ltrim(substr($line, 4)),
            );
        }

        if ($verb === '-ERR') {
            return new ProtocolFrame(
                type: ProtocolFrameType::Err,
                error: trim(substr($line, 4)),
            );
        }

        // PING/PONG/+OK carry no arguments - require the whole (case-insensitive) line to be the verb.
        // $verb is already upper-cased, so when the line equals it the fold is a proven no-op (#140).
        $normalized = $line === $verb ? $verb : strtoupper($line);
        if ($normalized === 'PING') {
            return new ProtocolFrame(type: ProtocolFrameType::Ping);
        }

        if ($normalized === 'PONG') {
            return new ProtocolFrame(type: ProtocolFrameType::Pong);
        }

        if ($normalized === '+OK') {
            return new ProtocolFrame(type: ProtocolFrameType::Ok);
        }

        throw new ProtocolException('Unsupported control frame: ' . $line);
    }

    /**
     * Parses a MSG or HMSG control line into a pending-frame descriptor and validates its size.
     *
     * @param int $payloadOffset Absolute offset into the current buffer where payload bytes begin.
     * @return array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int}
     */
    private function parseDataFrameHeader(string $verb, string $line, int $payloadOffset): array
    {
        if ($verb === 'HMSG') {
            return $this->parseHMsgHeader($line, $payloadOffset);
        }

        return $this->parseMsgHeader($line, $payloadOffset);
    }

    /**
     * Tokenizes a MSG/HMSG control line on whitespace.
     *
     * Fast path: real servers separate arguments with single spaces (see the leniency note in
     * push()), so a plain explode() covers virtually every frame without the per-message regex
     * cost (#140). The whitespace-tolerant preg_split fallback must engage whenever explode()
     * could disagree with it: an empty token (doubled/trailing spaces), a token count outside
     * the frame's valid range, or a token containing non-space whitespace (tab/LF/VT/FF/CR)
     * that the regex would treat as a separator - e.g. a tab can hide an extra field inside a
     * space-separated token and silently change which fields the tokens map to.
     *
     * @param int $minParts Minimum valid token count for the frame type (caller re-validates).
     * @param int $maxParts Maximum valid token count for the frame type (caller re-validates).
     * @return list<string>
     */
    private function splitControlLine(string $line, int $minParts, int $maxParts): array
    {
        $parts = explode(' ', $line);
        $count = count($parts);

        if ($count >= $minParts && $count <= $maxParts) {
            $canonical = true;
            foreach ($parts as $part) {
                if ($part === '' || strpbrk($part, "\t\n\v\f\r") !== false) {
                    $canonical = false;
                    break;
                }
            }

            if ($canonical) {
                return $parts;
            }
        }

        $parts = preg_split('/\s+/', $line);

        // preg_split() only returns false on a compile/backtrack error, which the constant
        // pattern cannot hit; an empty list fails the caller's count check exactly like the
        // previous `$parts === false ||` guard did.
        return $parts === false ? [] : $parts;
    }

    /**
     * @return array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int}
     */
    private function parseMsgHeader(string $line, int $payloadOffset): array
    {
        $parts = $this->splitControlLine($line, 4, 5);
        if (count($parts) < 4 || count($parts) > 5) {
            throw new ProtocolException('Invalid MSG frame line: ' . $line);
        }

        $sid = $this->parseUnsignedInt($parts[2], 'sid', $line);
        $size = $this->parseUnsignedInt($parts[count($parts) - 1], 'payload size', $line);
        if ($size > $this->maxFrameSize) {
            throw new ProtocolException('MSG frame payload size is invalid: ' . $size);
        }

        return [
            'type' => ProtocolFrameType::Msg,
            'subject' => $parts[1],
            'sid' => $sid,
            'replyTo' => count($parts) === 5 ? $parts[3] : null,
            'headerBytes' => null,
            'totalBytes' => $size,
            'payloadOffset' => $payloadOffset,
        ];
    }

    /**
     * @return array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int}
     */
    private function parseHMsgHeader(string $line, int $payloadOffset): array
    {
        $parts = $this->splitControlLine($line, 5, 6);
        if (count($parts) < 5 || count($parts) > 6) {
            throw new ProtocolException('Invalid HMSG frame line: ' . $line);
        }

        $sid = $this->parseUnsignedInt($parts[2], 'sid', $line);

        if (count($parts) === 6) {
            $replyTo = $parts[3];
            $headerBytes = $this->parseUnsignedInt($parts[4], 'header bytes', $line);
            $totalBytes = $this->parseUnsignedInt($parts[5], 'total bytes', $line);
        } else {
            $replyTo = null;
            $headerBytes = $this->parseUnsignedInt($parts[3], 'header bytes', $line);
            $totalBytes = $this->parseUnsignedInt($parts[4], 'total bytes', $line);
        }

        if ($totalBytes > $this->maxFrameSize) {
            throw new ProtocolException('HMSG frame payload size is invalid: ' . $totalBytes);
        }

        if ($headerBytes > $totalBytes) {
            throw new ProtocolException('HMSG header bytes exceed total bytes: ' . $line);
        }

        return [
            'type' => ProtocolFrameType::HMsg,
            'subject' => $parts[1],
            'sid' => $sid,
            'replyTo' => $replyTo,
            'headerBytes' => $headerBytes,
            'totalBytes' => $totalBytes,
            'payloadOffset' => $payloadOffset,
        ];
    }

    /**
     * Parses a required unsigned-integer token (sid / size / header bytes), rejecting non-numeric
     * or negative values instead of silently coercing them to 0 (which would misframe the stream).
     */
    private function parseUnsignedInt(string $value, string $field, string $line): int
    {
        if ($value === '' || !ctype_digit($value)) {
            throw new ProtocolException(sprintf('Invalid %s in frame line: %s', $field, $line));
        }

        // Reject values that would overflow a PHP int (which (int) silently saturates to PHP_INT_MAX),
        // so a bogus size/sid is a clear protocol error rather than a corrupted-but-accepted value.
        $maxLen = strlen((string) PHP_INT_MAX);
        if (strlen($value) > $maxLen || (strlen($value) === $maxLen && $value > (string) PHP_INT_MAX)) {
            throw new ProtocolException(sprintf('%s out of range in frame line: %s', $field, $line));
        }

        return (int) $value;
    }

    /**
     * Builds the frame for the currently buffered pending header once its payload is complete.
     */
    private function buildPendingFrame(): ProtocolFrame
    {
        /** @var array{type: ProtocolFrameType, subject: string, sid: int, replyTo: ?string, headerBytes: ?int, totalBytes: int, payloadOffset: int} $pending */
        $pending = $this->pending;

        $payload = substr($this->buffer, $pending['payloadOffset'], $pending['totalBytes']);
        $crlf = substr($this->buffer, $pending['payloadOffset'] + $pending['totalBytes'], 2);

        if ($crlf !== "\r\n") {
            $label = $pending['type'] === ProtocolFrameType::HMsg ? 'HMSG' : 'MSG';
            throw new ProtocolException($label . ' frame payload must be terminated by CRLF');
        }

        return new ProtocolFrame(
            type: $pending['type'],
            subject: $pending['subject'],
            sid: $pending['sid'],
            replyTo: $pending['replyTo'],
            payload: $payload,
            headerBytes: $pending['headerBytes'],
            totalBytes: $pending['type'] === ProtocolFrameType::HMsg ? $pending['totalBytes'] : null,
        );
    }
}
