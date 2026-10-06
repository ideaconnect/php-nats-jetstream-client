<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\TransportInterface;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * A server that enforces a subscription limit and subscribe permissions, for tests of the request mux inbox.
 *
 * Like nats-server, each session answers the client's frames in wire order:
 *   - a SUB beyond {@see $limit} subscriptions gets "-ERR 'maximum subscriptions exceeded'" and is not
 *     registered, and the connection stays open (nats-server's processSubEx);
 *   - a SUB to a subject under a {@see $denied} prefix gets "-ERR 'Permissions Violation for Subscription to
 *     "<subject>"'" and is not registered;
 *   - a PING gets a PONG in order with those -ERRs, so the PONG comes after the answer to every SUB written
 *     ahead of its PING;
 *   - an UNSUB frees its sid's slot, and one for a sid the session does not hold is ignored (processUnsub).
 * A {@see $responder} answers PUBs with raw frames, and {@see replyFrame()} builds a reply only on a
 * subscription the session holds, so nothing arrives on an inbox the server rejected.
 *
 * The test drives the rest:
 *   - dropConnection(): the session dies like a reset socket - reads fail with EOF, writes fail;
 *   - holdAnswers() / releaseAnswers(): the session's answers are held back, like a slow network;
 *   - {@see $answerPings}: while it is false, PINGs after the handshake's go unanswered;
 *   - stallNextWriteContaining(): the next write containing a needle takes a while, like a socket under
 *     backpressure, until its time is up or the test calls releaseStalledWrites(). Writes reach the session in
 *     the order they were made, so the writes made after it wait for it;
 *   - completeNextWriteContainingLate(): the next write containing a needle reaches the session and is answered at
 *     once, but its writer learns it is done only a while later, so a read in another fiber meets the answer first.
 *
 * Reads block like an idle socket until something arrives, the session ends or the caller's cancellation
 * fires (unless {@see $readsReturnWhenIdle} is set), and return everything that has arrived, like a socket
 * read that drains its buffer.
 */
final class SubscriptionLimitServer implements TransportInterface
{
    public const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /** How many reads in a row may find nothing while {@see $readsReturnWhenIdle} is set. */
    public const MAX_IDLE_READS = 1_000;

    /** How many subscriptions a session holds before it rejects a SUB. */
    public int $limit = PHP_INT_MAX;

    /** @var list<string> Subject prefixes the client may not subscribe to, such as "_INBOX.". */
    public array $denied = [];

    /**
     * Answers each PUB/HPUB: called with the subject, the reply subject (null when there is none) and the
     * payload (without headers); returns raw frames to deliver on the session.
     *
     * @var (\Closure(string, ?string, string): list<string>)|null
     */
    public ?\Closure $responder = null;

    /** Whether PINGs after a session's handshake PING are answered (the handshake's always is). */
    public bool $answerPings = true;

    /**
     * Whether a read that finds nothing returns '' at once, as FakeTransport's does, instead of waiting like an idle
     * socket. A caller that then reads again without sleeping never lets a timer run, so after
     * {@see MAX_IDLE_READS} such reads in a row a read fails, and the test fails instead of hanging.
     */
    public bool $readsReturnWhenIdle = false;

    /** @var list<array{epoch: int, bytes: string}> Every write a live session took, with its session. */
    public array $writes = [];

    private int $epoch = -1;
    private int $idleReads = 0;
    private bool $sessionLive = false;
    private bool $handshakePending = false;
    /** @var list<string> */
    private array $reads = [];
    /** @var array<int, string> Sid => subject of each subscription the live session holds. */
    private array $subscriptions = [];
    private bool $holdingAnswers = false;
    /** @var list<string> Answers held back since holdAnswers(). */
    private array $heldAnswers = [];
    /** @var array<string, array{seconds: float, completesAfterClose: bool}> One-shot write stalls by needle. */
    private array $writeStalls = [];
    /** @var array<string, float> One-shot late completions by needle: how long after it was answered the write ends. */
    private array $lateCompletions = [];
    /** @var array<string, DeferredFuture<null>> Stalled writes that end with the session, by their timer. */
    private array $stalledWrites = [];
    /** @var array<string, DeferredFuture<null>> Every stall still running, by its timer, whether it ends with the session or not. */
    private array $runningStalls = [];
    /** @var Future<void>|null The live session's latest write, which the next one waits for. */
    private ?Future $lastWrite = null;
    /** @var DeferredFuture<null> Completed (and replaced) whenever parked reads must re-check. */
    private DeferredFuture $wake;

    public function __construct()
    {
        $this->wake = new DeferredFuture();
    }

    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function (): void {
            $this->epoch++;
            $this->sessionLive = true;
            $this->handshakePending = true;
            $this->subscriptions = [];
            $this->lastWrite = null;
            $this->reads = [self::INFO];
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
        $previous = $this->lastWrite;

        $write = async(function () use ($bytes, $epoch, $previous): void {
            // Like bytes on a socket, a write reaches the session after the writes made before it, a stalled one
            // included.
            if ($previous !== null && !$previous->isComplete()) {
                try {
                    $previous->await();
                } catch (\Throwable) {
                    // Its own writer learns why it failed.
                }

                if (!$this->sessionLive || $this->epoch !== $epoch) {
                    throw new TransportClosedException('The connection is gone');
                }
            }

            foreach ($this->writeStalls as $needle => $stall) {
                if (!str_contains($bytes, $needle)) {
                    continue;
                }

                unset($this->writeStalls[$needle]);
                $this->stall($stall['seconds'], endsWithSession: !$stall['completesAfterClose']);
                if (!$this->sessionLive || $this->epoch !== $epoch) {
                    if ($stall['completesAfterClose']) {
                        // As far as the writer can tell, the bytes went out before the close; the server never got them.
                        return;
                    }

                    throw new TransportClosedException('The connection is gone');
                }
            }

            $this->writes[] = ['epoch' => $epoch, 'bytes' => $bytes];
            $answers = [];
            foreach (self::parseClientFrames($bytes) as $frame) {
                array_push($answers, ...$this->answer($frame));
            }

            if ($this->holdingAnswers) {
                array_push($this->heldAnswers, ...$answers);
            } elseif ($answers !== []) {
                array_push($this->reads, ...$answers);
                $this->wakeReaders();
            }

            foreach ($this->lateCompletions as $needle => $seconds) {
                if (str_contains($bytes, $needle)) {
                    unset($this->lateCompletions[$needle]);
                    delay($seconds);

                    break;
                }
            }
        });
        $this->lastWrite = $write;

        return $write;
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        $epoch = $this->epoch;

        return async(function () use ($cancellation, $epoch): string {
            while (true) {
                if (!$this->sessionLive || $this->epoch !== $epoch) {
                    throw new TransportClosedException('Socket closed by peer (EOF)');
                }

                if ($this->reads !== []) {
                    $chunk = implode('', $this->reads);
                    $this->reads = [];
                    $this->idleReads = 0;

                    return $chunk;
                }

                if ($this->readsReturnWhenIdle) {
                    if (++$this->idleReads > self::MAX_IDLE_READS) {
                        throw new \LogicException(sprintf('%d reads in a row found nothing: the reader never sleeps', self::MAX_IDLE_READS));
                    }

                    return '';
                }

                // An idle socket: park until something arrives, the session ends, or the caller gives up. A
                // referenced keep-alive stands in for a real socket's readable watcher, so the unreferenced timers
                // behind every TimeoutCancellation still fire; it expires on its own so a forgotten read cannot
                // wedge the test process.
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
            $this->endSession();
        });
    }

    /** Kills the live session like a reset socket: reads fail with EOF (pending ones too), writes fail. */
    public function dropConnection(): void
    {
        $this->endSession();
    }

    /** Delivers a raw frame on the live session at once, held answers or not (dropped when there is no session). */
    public function pushFrame(string $frame): void
    {
        if ($this->sessionLive) {
            $this->reads[] = $frame;
            $this->wakeReaders();
        }
    }

    /** From now on the session's answers to what the client writes are held back, until releaseAnswers(). */
    public function holdAnswers(): void
    {
        $this->holdingAnswers = true;
    }

    /** Delivers the answers held back since holdAnswers(), in order, and stops holding them. */
    public function releaseAnswers(): void
    {
        $this->holdingAnswers = false;
        $held = $this->heldAnswers;
        $this->heldAnswers = [];
        if ($held !== [] && $this->sessionLive) {
            array_push($this->reads, ...$held);
            $this->wakeReaders();
        }
    }

    /**
     * Makes the next write containing $needle take $seconds before the session gets it - a socket held up by
     * backpressure. One-shot.
     *
     * When the session ends meanwhile the write fails at once, as a real socket's pending write fails when the
     * socket closes. With $completesAfterClose it runs its full course instead and then completes, as if the
     * bytes had been handed over before the close, though the server never gets them.
     */
    public function stallNextWriteContaining(string $needle, float $seconds, bool $completesAfterClose = false): void
    {
        $this->writeStalls[$needle] = ['seconds' => $seconds, 'completesAfterClose' => $completesAfterClose];
    }

    /**
     * Makes the next write containing $needle end only $seconds after the session got it and answered it, as with a
     * transport that reports a write done late: a read in another fiber can meet the answer before the writer
     * resumes. One-shot.
     */
    public function completeNextWriteContainingLate(string $needle, float $seconds): void
    {
        $this->lateCompletions[$needle] = $seconds;
    }

    /**
     * Ends every stall still running now, as if its time were up. For a test that holds a write up, with a stall
     * longer than it lasts, until it has done what must happen while the write waits, rather than racing the
     * stall's timer.
     */
    public function releaseStalledWrites(): void
    {
        foreach ($this->runningStalls as $released) {
            if (!$released->isComplete()) {
                $released->complete();
            }
        }
    }

    /** The index of the latest session: 0 for the first dial, 1 after the first reconnect, ... */
    public function epoch(): int
    {
        return $this->epoch;
    }

    /**
     * The subscriptions the live session holds.
     *
     * @return array<int, string> Sid => subject.
     */
    public function heldSubscriptions(): array
    {
        return $this->subscriptions;
    }

    /**
     * A reply to $replyTo on the subscription the live session holds for it - an exact subject, or a "<base>.*"
     * wildcard such as the mux inbox - or no frame when it holds none.
     *
     * @return list<string>
     */
    public function replyFrame(string $replyTo, string $payload): array
    {
        foreach ($this->subscriptions as $sid => $subject) {
            $base = substr($subject, 0, -1);
            $coveredByWildcard = str_ends_with($subject, '.*')
                && str_starts_with($replyTo, $base)
                && !str_contains(substr($replyTo, strlen($base)), '.');
            if ($subject === $replyTo || $coveredByWildcard) {
                return [sprintf("MSG %s %d %d\r\n%s\r\n", $replyTo, $sid, strlen($payload), $payload)];
            }
        }

        return [];
    }

    /**
     * The control lines of the frames the client wrote that start with $prefix (optionally only in one
     * session), e.g. "SUB updates 2" or "PUB svc.echo _INBOX.abc.1 2", in wire order.
     *
     * @return list<string>
     */
    public function controlLines(string $prefix = '', ?int $epoch = null): array
    {
        $lines = [];
        foreach ($this->writes as $write) {
            if ($epoch !== null && $write['epoch'] !== $epoch) {
                continue;
            }

            foreach (self::parseClientFrames($write['bytes']) as $frame) {
                if (str_starts_with($frame['line'], $prefix)) {
                    $lines[] = $frame['line'];
                }
            }
        }

        return $lines;
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
            case 'PING':
                $handshake = $this->handshakePending;
                $this->handshakePending = false;

                return $handshake || $this->answerPings ? ["PONG\r\n"] : [];
            case 'SUB':
                $subject = $frame['args'][0];
                foreach ($this->denied as $prefix) {
                    if (str_starts_with($subject, $prefix)) {
                        return ["-ERR 'Permissions Violation for Subscription to \"" . $subject . "\"'\r\n"];
                    }
                }

                if (count($this->subscriptions) >= $this->limit) {
                    return ["-ERR 'maximum subscriptions exceeded'\r\n"];
                }

                $this->subscriptions[(int) $frame['args'][count($frame['args']) - 1]] = $subject;

                return [];
            case 'UNSUB':
                // With a max the subscription stays until it has delivered that many.
                if (count($frame['args']) === 1) {
                    unset($this->subscriptions[(int) $frame['args'][0]]);
                }

                return [];
            case 'PUB':
            case 'HPUB':
                if ($this->responder === null) {
                    return [];
                }

                $args = $frame['args'];
                $withReply = count($args) === ($frame['op'] === 'PUB' ? 3 : 4);
                $body = $frame['op'] === 'HPUB'
                    ? substr($frame['payload'], (int) $args[count($args) - 2])
                    : $frame['payload'];

                return ($this->responder)($args[0], $withReply ? $args[1] : null, $body);
            default:
                return [];
        }
    }

    /**
     * Holds the calling write for $seconds, or - when it ends with the session - until the session ends. The
     * timer is referenced, like the writable watcher of a real socket's pending write.
     */
    private function stall(float $seconds, bool $endsWithSession): void
    {
        /** @var DeferredFuture<null> $released */
        $released = new DeferredFuture();
        $timer = EventLoop::delay($seconds, static function () use ($released): void {
            if (!$released->isComplete()) {
                $released->complete();
            }
        });
        $this->runningStalls[$timer] = $released;
        if ($endsWithSession) {
            $this->stalledWrites[$timer] = $released;
        }

        try {
            $released->getFuture()->await();
        } finally {
            EventLoop::cancel($timer);
            unset($this->stalledWrites[$timer], $this->runningStalls[$timer]);
        }
    }

    private function endSession(): void
    {
        $this->sessionLive = false;
        $this->reads = [];
        $this->heldAnswers = [];
        $this->subscriptions = [];
        $this->wakeReaders();

        foreach ($this->stalledWrites as $released) {
            if (!$released->isComplete()) {
                $released->complete();
            }
        }
        $this->stalledWrites = [];
    }

    private function wakeReaders(): void
    {
        $wake = $this->wake;
        $this->wake = new DeferredFuture();
        $wake->complete();
    }

    /**
     * Splits client bytes into frames: the control line, its operation and arguments, and - for PUB/HPUB - the
     * payload bytes (for HPUB, headers and body together).
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
