<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Transport\CancellableDialTransportInterface;
use IDCT\NATS\Transport\TransportClosedException;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * A scripted server for reconnect tests, modelling the outages a client has to ride out.
 *
 * Every successful dial opens a fresh session ("epoch") that greets with INFO and answers PINGs with
 * PONGs, so a client (re)connects without pre-seeded read queues. The test then drives the outage:
 *   - dropConnection(): the live session dies like a reset socket - reads fail with EOF (pending ones
 *     too), writes fail;
 *   - silence(): the live session hangs - writes are accepted but nothing is answered any more, so
 *     only the heartbeat (maxPingsOut) can notice;
 *   - refuseDials() / acceptDials(): dials fail while refused, keeping a recovery in its backoff loop;
 *   - holdNextDial() / releaseDial(): the next dial parks until released, freezing a recovery mid-dial
 *     (a held dial that is released while dials are refused fails);
 *   - holdNextGreeting() / releaseGreeting(): the next session's greeting (its INFO) is held back until
 *     released, like one still on its way over a slow network: the client has a live socket but has not
 *     written its CONNECT yet;
 *   - {@see $requireConnectFirst}: a session whose first frame from the client is not CONNECT refuses the
 *     CONNECT that follows, as a server that requires authentication does;
 *   - {@see $rejectAuthentication}: the next handshakes are refused with an authorization -ERR;
 *   - {@see $closeDelay}: close() takes a while, like a TLS or WebSocket close handshake;
 *   - stallNextWriteContaining(): the next write containing a needle is held up, like a socket under
 *     backpressure;
 *   - failNextWriteContaining(): the next write containing a needle finds the socket dead - failing with
 *     a TransportClosedException, or with the error the test gives, such as the raw stream error a
 *     built-in transport's socket throws.
 *
 * Reads and writes belong to the session they started on, like a real socket's: one still pending
 * when that session ends fails, even if a new session is live by then - at once, like a real socket's,
 * unless the test asks for a write whose failure surfaces only later. Responses and PONGs held back by
 * {@see $responseDelay} and {@see $pongDelay} end with their session too.
 *
 * Reads block like an idle socket until a frame arrives, the session ends or the caller's cancellation
 * fires. A {@see $responder} answers published messages with raw frames; {@see replyFrame()} builds a
 * reply on the sid the client subscribed for the reply subject on the live session (an exact subject,
 * or a "<base>.*" wildcard such as the mux request inbox).
 */
final class ReconnectingTransport implements CancellableDialTransportInterface
{
    public const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /** @var list<string> Every dial attempt, refused ones included. */
    public array $connectCalls = [];

    /**
     * Dials the connection stopped (see {@see CancellableDialTransportInterface}): a held dial ends when the
     * cancellation the connection passed fires, as a built-in transport's dial does, and opens nothing.
     */
    public int $dialsCancelled = 0;

    /** @var list<array{epoch: int, bytes: string}> Every write a live session accepted, with its epoch. */
    public array $writes = [];

    /**
     * Answers each PUB/HPUB written to a live, answering session: called with the subject, the reply
     * subject (null when there is none) and the payload (without headers); returns raw frames to
     * deliver on that session.
     *
     * @var (\Closure(string, ?string, string): list<string>)|null
     */
    public ?\Closure $responder = null;

    /** Seconds the responder's frames are held back (a slow server); 0 delivers them at once. */
    public float $responseDelay = 0.0;

    /**
     * Seconds the PONGs answering PINGs after a session's handshake PING are held back (a slow server); 0
     * answers them at once. The handshake's own PONG always goes out at once.
     */
    public float $pongDelay = 0.0;

    /** Whether PINGs after a session's handshake PING are answered (the handshake's always is). */
    public bool $answerPings = true;

    /** Whether a session answers the client's CONNECT with an "Authorization Violation" -ERR. */
    public bool $rejectAuthentication = false;

    /**
     * Whether a session whose first frame from the client is not CONNECT answers the CONNECT that follows with an
     * "Authorization Violation" -ERR. nats-server with authorization configured does that to a connection whose
     * first protocol message is anything but CONNECT, and closes it.
     */
    public bool $requireConnectFirst = false;

    /**
     * Frames the next handshake's PONG carries right behind it, in the same read - the way a server can
     * coalesce an async INFO with it. One-shot.
     */
    public string $handshakeTrailer = '';

    /** Seconds close() takes before the session ends; 0 closes at once. */
    public float $closeDelay = 0.0;

    /**
     * Called with the bytes of each write a live session accepted, once it is recorded - for a test
     * that has to act at the exact moment a write went out.
     *
     * @var (\Closure(string): void)|null
     */
    public ?\Closure $afterWrite = null;

    /**
     * Called when a close() starts, before its {@see $closeDelay} - for a test that has to act while the
     * close is under way.
     *
     * @var (\Closure(): void)|null
     */
    public ?\Closure $beforeClose = null;

    /** One-shot: the next write containing this needle drops the session and fails. */
    private ?string $failingWriteNeedle = null;

    /** What the failing write throws; null for a TransportClosedException. */
    private ?\Throwable $failingWriteError = null;

    /**
     * One-shot write stalls (see {@see stallNextWriteContaining()}).
     *
     * @var array<string, array{seconds: float, outlivesSession: bool}>
     */
    private array $writeStalls = [];

    /**
     * Writes stalled on the live session that end with it, keyed by their timer: ending the session
     * releases them at once, and they then fail, like a real socket's pending write.
     *
     * @var array<string, DeferredFuture<null>>
     */
    private array $stalledWrites = [];

    /** Writes whose stall is still running, those that outlive their session included. */
    private int $writesStalled = 0;

    /** @var array<string, true> Timers of the live session's delayed responses: cancelled when it ends. */
    private array $delayedDeliveries = [];

    private int $epoch = -1;
    /** @var list<string> */
    private array $reads = [];
    private bool $sessionLive = false;
    private bool $silent = false;
    private bool $handshakePending = false;
    private bool $dialsRefused = false;
    private bool $holdNextDial = false;
    private bool $holdNextGreeting = false;
    /** Whether the live session's greeting is held back (see {@see holdNextGreeting()}). */
    private bool $greetingHeldBack = false;
    /** Whether the live session has had a frame from the client yet. */
    private bool $clientFrameSeen = false;
    /** Whether the live session's first frame from the client was not CONNECT (see {@see $requireConnectFirst}). */
    private bool $connectNotFirst = false;
    /** @var ?DeferredFuture<null> */
    private ?DeferredFuture $heldDial = null;
    /** @var DeferredFuture<null> Completed (and replaced) whenever parked reads must re-check. */
    private DeferredFuture $wake;
    /** @var array<string, int> Subject => sid of the SUBs written to the live session. */
    private array $sids = [];

    public function __construct()
    {
        $this->wake = new DeferredFuture();
    }

    public function connect(string $dsn, int $timeoutMs, ?Cancellation $cancellation = null): Future
    {
        return async(function () use ($dsn, $timeoutMs, $cancellation): void {
            $this->connectCalls[] = $dsn . '|' . $timeoutMs;
            $this->refuseDialIfRefusing();

            if ($this->holdNextDial) {
                $this->holdNextDial = false;
                /** @var DeferredFuture<null> $held */
                $held = new DeferredFuture();
                $this->heldDial = $held;
                // A referenced keep-alive stands in for a real dial's socket watcher, as for parked reads.
                $keepAlive = EventLoop::delay(30.0, static function (): void {});
                try {
                    $held->getFuture()->await($cancellation);
                } catch (CancelledException $stopped) {
                    $this->heldDial = null;
                    $this->dialsCancelled++;

                    throw $stopped;
                } finally {
                    EventLoop::cancel($keepAlive);
                }
                // Dials may have been refused while this one was held.
                $this->refuseDialIfRefusing();
            }

            $this->epoch++;
            $this->reads = [self::INFO];
            $this->sessionLive = true;
            $this->silent = false;
            $this->handshakePending = true;
            $this->sids = [];
            $this->greetingHeldBack = $this->holdNextGreeting;
            $this->holdNextGreeting = false;
            $this->clientFrameSeen = false;
            $this->connectNotFirst = false;
            $this->wakeReaders();
        });
    }

    public function upgradeTls(): Future
    {
        return async(static fn(): null => null);
    }

    public function write(string $bytes): Future
    {
        // Like AmpSocketTransport, a write to a socket already known to be gone fails at once.
        if (!$this->sessionLive) {
            return Future::error(new TransportClosedException('The connection is gone'));
        }

        $epoch = $this->epoch;

        return async(function () use ($bytes, $epoch): void {
            foreach ($this->writeStalls as $needle => $stall) {
                if (str_contains($bytes, $needle)) {
                    unset($this->writeStalls[$needle]);
                    $this->stall($stall['seconds'], $stall['outlivesSession']);
                    // The session may have ended - or been replaced - while the write was stalled.
                    $this->assertSessionLive($epoch);
                }
            }

            if ($this->failingWriteNeedle !== null && str_contains($bytes, $this->failingWriteNeedle)) {
                $this->failingWriteNeedle = null;
                $this->endSession();

                throw $this->failingWriteError ?? new TransportClosedException('The connection is gone');
            }

            $frames = self::parseClientFrames($bytes);

            $this->writes[] = ['epoch' => $epoch, 'bytes' => $bytes];

            if (!$this->clientFrameSeen && $frames !== []) {
                $this->clientFrameSeen = true;
                $this->connectNotFirst = $frames[0]['op'] !== 'CONNECT';
            }

            $pongs = [];
            $laterPongs = [];
            $responses = [];
            foreach ($frames as $frame) {
                if ($frame['op'] === 'PUB' || $frame['op'] === 'HPUB') {
                    $responses = [...$responses, ...$this->answer($frame)];
                } elseif ($frame['op'] === 'PING' && !$this->handshakePending) {
                    $laterPongs = [...$laterPongs, ...$this->answer($frame)];
                } else {
                    $pongs = [...$pongs, ...$this->answer($frame)];
                }
            }

            // Only the responder and later PONGs can be slow: the handshake's PONG goes out at once.
            $this->deliver($epoch, $pongs, 0.0);
            $this->deliver($epoch, $laterPongs, $this->pongDelay);
            $this->deliver($epoch, $responses, $this->responseDelay);

            if ($this->afterWrite !== null) {
                ($this->afterWrite)($bytes);
            }
        });
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        $epoch = $this->epoch;

        return async(function () use ($cancellation, $epoch): string {
            while (true) {
                if (!$this->sessionLive || $this->epoch !== $epoch) {
                    throw new TransportClosedException('Socket closed by peer (EOF)');
                }

                $chunk = $this->greetingHeldBack ? null : array_shift($this->reads);
                if ($chunk !== null) {
                    return $chunk;
                }

                // An idle socket: park until something arrives, the session ends, or the caller gives up.
                // A referenced keep-alive stands in for a real socket's readable watcher, so the
                // unreferenced timers behind every TimeoutCancellation still fire; it expires on its
                // own so a forgotten read cannot wedge the test process.
                $keepAlive = EventLoop::delay(30.0, static function (): void {});
                try {
                    $this->wake->getFuture()->await($cancellation);
                } finally {
                    EventLoop::cancel($keepAlive);
                }
            }
        });
    }

    public function close(): Future
    {
        return async(function (): void {
            if ($this->beforeClose !== null) {
                ($this->beforeClose)();
            }

            if ($this->closeDelay > 0.0) {
                delay($this->closeDelay);
            }

            $this->endSession();
        });
    }

    /** Kills the live session like a reset socket: reads fail with EOF (pending ones too), writes fail. */
    public function dropConnection(): void
    {
        $this->endSession();
    }

    /** Hangs the live session: writes are still accepted, but nothing is answered any more. */
    public function silence(): void
    {
        $this->silent = true;
    }

    public function refuseDials(): void
    {
        $this->dialsRefused = true;
    }

    public function acceptDials(): void
    {
        $this->dialsRefused = false;
    }

    /**
     * Makes the next write containing $needle take $seconds before it completes - a socket held up by
     * backpressure. One-shot: later writes containing it are not delayed.
     *
     * The stall ends early, failing the write, when its session ends - as a real socket's pending write
     * fails when the socket closes. With $outlivesSession it runs its full course anyway, and only then
     * fails: a transport whose close leaves a pending write to fail later, which TransportInterface allows.
     */
    public function stallNextWriteContaining(string $needle, float $seconds, bool $outlivesSession = false): void
    {
        $this->writeStalls[$needle] = ['seconds' => $seconds, 'outlivesSession' => $outlivesSession];
    }

    /** How many writes are stalled right now (see {@see stallNextWriteContaining()}). */
    public function writesStalled(): int
    {
        return $this->writesStalled;
    }

    /**
     * Makes the next write containing $needle find the socket dead: the session drops and the write fails, with
     * $error when given (a built-in transport passes on its socket's raw stream error, an
     * {@see \Amp\ByteStream\ClosedException} say), else with a TransportClosedException.
     */
    public function failNextWriteContaining(string $needle, ?\Throwable $error = null): void
    {
        $this->failingWriteNeedle = $needle;
        $this->failingWriteError = $error;
    }

    /** Parks the next dial until {@see releaseDial()}. */
    public function holdNextDial(): void
    {
        $this->holdNextDial = true;
    }

    public function releaseDial(): void
    {
        $held = $this->heldDial;
        $this->heldDial = null;
        $held?->complete();
    }

    /** Holds back the greeting of the next session until {@see releaseGreeting()}: nothing can be read before it. */
    public function holdNextGreeting(): void
    {
        $this->holdNextGreeting = true;
    }

    public function releaseGreeting(): void
    {
        $this->greetingHeldBack = false;
        $this->wakeReaders();
    }

    /** Whether a session is live whose greeting is held back (see {@see holdNextGreeting()}). */
    public function greetingHeld(): bool
    {
        return $this->sessionLive && $this->greetingHeldBack;
    }

    /** Delivers a raw frame on the live session (dropped when there is none). */
    public function pushFrame(string $frame): void
    {
        if ($this->sessionLive) {
            $this->reads[] = $frame;
            $this->wakeReaders();
        }
    }

    /** Whether a session is live: dialled, and not ended by a drop or a close since. */
    public function sessionLive(): bool
    {
        return $this->sessionLive;
    }

    /** The index of the latest session: 0 for the first dial, 1 after the first reconnect, ... */
    public function epoch(): int
    {
        return $this->epoch;
    }

    /**
     * The sid the client subscribed on the live session for $subject: an exact SUB, or a "<base>.*"
     * wildcard SUB covering it.
     */
    public function sidFor(string $subject): ?int
    {
        if (isset($this->sids[$subject])) {
            return $this->sids[$subject];
        }

        foreach ($this->sids as $subscribed => $sid) {
            if (!str_ends_with($subscribed, '.*')) {
                continue;
            }

            $base = substr($subscribed, 0, -1);
            if (str_starts_with($subject, $base) && !str_contains(substr($subject, strlen($base)), '.')) {
                return $sid;
            }
        }

        return null;
    }

    /**
     * A reply to $replyTo on the sid the client subscribed for it, or no frame when it did not.
     *
     * @return list<string>
     */
    public function replyFrame(string $replyTo, string $payload): array
    {
        $sid = $this->sidFor($replyTo);

        return $sid === null ? [] : [self::msgFrame($replyTo, $sid, $payload)];
    }

    /** A MSG frame for $sid, optionally carrying a reply subject (e.g. a JetStream ack subject). */
    public static function msgFrame(string $subject, int $sid, string $payload, ?string $reply = null): string
    {
        $head = $reply === null
            ? sprintf('MSG %s %d %d', $subject, $sid, strlen($payload))
            : sprintf('MSG %s %d %s %d', $subject, $sid, $reply, strlen($payload));

        return $head . "\r\n" . $payload . "\r\n";
    }

    /**
     * An HMSG frame for $sid, optionally carrying a reply subject. $headers is the whole header block,
     * "NATS/1.0\r\n" through the blank line that ends it.
     */
    public static function hmsgFrame(string $subject, int $sid, string $headers, string $payload, ?string $reply = null): string
    {
        $sizes = strlen($headers) . ' ' . (strlen($headers) + strlen($payload));
        $head = $reply === null
            ? sprintf('HMSG %s %d %s', $subject, $sid, $sizes)
            : sprintf('HMSG %s %d %s %s', $subject, $sid, $reply, $sizes);

        return $head . "\r\n" . $headers . $payload . "\r\n";
    }

    /**
     * The control lines of every frame the client wrote (optionally only in one epoch), e.g.
     * "SUB updates 2" or "PUB svc.echo _INBOX.abc.1 2", in wire order.
     *
     * @return list<string>
     */
    public function controlLines(?int $epoch = null): array
    {
        $lines = [];
        foreach ($this->writes as $write) {
            if ($epoch !== null && $write['epoch'] !== $epoch) {
                continue;
            }

            foreach (self::parseClientFrames($write['bytes']) as $frame) {
                $lines[] = $frame['line'];
            }
        }

        return $lines;
    }

    /**
     * Control lines (see {@see controlLines()}) that start with $prefix.
     *
     * @return list<string>
     */
    public function controlLinesStartingWith(string $prefix, ?int $epoch = null): array
    {
        return array_values(array_filter(
            $this->controlLines($epoch),
            static fn(string $line): bool => str_starts_with($line, $prefix),
        ));
    }

    /**
     * Answers one client frame on the live session.
     *
     * @param array{op: string, line: string, args: list<string>, payload: string} $frame
     * @return list<string>
     */
    private function answer(array $frame): array
    {
        switch ($frame['op']) {
            case 'CONNECT':
                return $this->rejectAuthentication || ($this->requireConnectFirst && $this->connectNotFirst)
                    ? ["-ERR 'Authorization Violation'\r\n"]
                    : [];
            case 'SUB':
                $sid = (int) $frame['args'][count($frame['args']) - 1];
                $this->sids[$frame['args'][0]] = $sid;

                return [];
            case 'UNSUB':
                if (count($frame['args']) === 1) {
                    $sid = (int) $frame['args'][0];
                    $this->sids = array_filter($this->sids, static fn(int $known): bool => $known !== $sid);
                }

                return [];
            case 'PING':
                $handshake = $this->handshakePending;
                $this->handshakePending = false;
                if ($this->silent || (!$handshake && !$this->answerPings)) {
                    return [];
                }

                $trailer = $handshake ? $this->handshakeTrailer : '';
                if ($handshake) {
                    $this->handshakeTrailer = '';
                }

                return ["PONG\r\n" . $trailer];
            case 'PUB':
            case 'HPUB':
                if ($this->silent || $this->responder === null) {
                    return [];
                }

                $args = $frame['args'];
                $withReply = count($args) === ($frame['op'] === 'PUB' ? 3 : 4);
                $replyTo = $withReply ? $args[1] : null;
                $body = $frame['op'] === 'HPUB'
                    ? substr($frame['payload'], (int) $args[count($args) - 2])
                    : $frame['payload'];

                return ($this->responder)($args[0], $replyTo, $body);
            default:
                return [];
        }
    }

    /**
     * Queues frames on the session they answer - after $delay seconds when positive - unless that
     * session has ended by then.
     *
     * @param list<string> $frames
     */
    private function deliver(int $epoch, array $frames, float $delay): void
    {
        if ($frames === []) {
            return;
        }

        $push = function () use ($epoch, $frames): void {
            if ($epoch !== $this->epoch || !$this->sessionLive) {
                return;
            }

            array_push($this->reads, ...$frames);
            $this->wakeReaders();
        };

        if ($delay > 0.0) {
            $timer = '';
            $timer = EventLoop::delay($delay, function () use (&$timer, $push): void {
                unset($this->delayedDeliveries[$timer]);
                $push();
            });
            $this->delayedDeliveries[$timer] = true;

            return;
        }

        $push();
    }

    /**
     * Holds the calling write for $seconds, or - unless it outlives its session - until the session ends.
     * The timer is referenced, like the writable watcher of a real socket's pending write.
     */
    private function stall(float $seconds, bool $outlivesSession): void
    {
        /** @var DeferredFuture<null> $released */
        $released = new DeferredFuture();
        $timer = EventLoop::delay($seconds, static function () use ($released): void {
            if (!$released->isComplete()) {
                $released->complete();
            }
        });
        if (!$outlivesSession) {
            $this->stalledWrites[$timer] = $released;
        }

        $this->writesStalled++;
        try {
            $released->getFuture()->await();
        } finally {
            $this->writesStalled--;
            EventLoop::cancel($timer);
            unset($this->stalledWrites[$timer]);
        }
    }

    /** @phpstan-impure Reads a flag the test flips while a dial is held. */
    private function refuseDialIfRefusing(): void
    {
        if ($this->dialsRefused) {
            throw new TransportClosedException('Connection refused');
        }
    }

    private function assertSessionLive(int $epoch): void
    {
        if (!$this->sessionLive || $this->epoch !== $epoch) {
            throw new TransportClosedException('The connection is gone');
        }
    }

    private function endSession(): void
    {
        $this->sessionLive = false;
        $this->reads = [];
        $this->wakeReaders();

        foreach ($this->stalledWrites as $timer => $released) {
            EventLoop::cancel($timer);
            if (!$released->isComplete()) {
                $released->complete();
            }
        }
        $this->stalledWrites = [];

        foreach (array_keys($this->delayedDeliveries) as $timer) {
            EventLoop::cancel($timer);
        }
        $this->delayedDeliveries = [];
    }

    private function wakeReaders(): void
    {
        $wake = $this->wake;
        $this->wake = new DeferredFuture();
        $wake->complete();
    }

    /**
     * Splits client bytes into frames: the control line, its operation and arguments, and - for
     * PUB/HPUB - the payload bytes (for HPUB, headers and body together).
     *
     * @return list<array{op: string, line: string, args: list<string>, payload: string}>
     */
    private static function parseClientFrames(string $bytes): array
    {
        $frames = [];
        $offset = 0;
        $length = strlen($bytes);

        while ($offset < $length) {
            $end = strpos($bytes, "\r\n", $offset);
            if ($end === false) {
                break;
            }

            $line = substr($bytes, $offset, $end - $offset);
            $offset = $end + 2;
            $parts = explode(' ', $line);
            $op = strtoupper($parts[0]);
            $args = array_slice($parts, 1);

            $payload = '';
            if (($op === 'PUB' || $op === 'HPUB') && $args !== []) {
                $size = (int) $args[count($args) - 1];
                $payload = substr($bytes, $offset, $size);
                $offset += $size + 2;
            }

            $frames[] = ['op' => $op, 'line' => $line, 'args' => $args, 'payload' => $payload];
        }

        return $frames;
    }
}
