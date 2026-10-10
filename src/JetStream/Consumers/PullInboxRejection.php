<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use Amp\Cancellation;
use Amp\DeferredCancellation;

/**
 * The server's rejection of the reply inbox of one run of the pipelined pull engine
 * ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}), as the inbox's rejection handler records it
 * ({@see \IDCT\NATS\Core\NatsClient::subscribeGuarded()}): a permissions violation naming the inbox, which a server
 * sends mid-run when a configuration reload withdraws the subscribe permission, or the connection's subscription limit
 * met before the inbox was confirmed, a reconnect's replay of it included (#175). Each run has its own, as it has its
 * own inbox.
 *
 * Whichever fiber's read meets the -ERR records it, inside the -ERR's dispatch: the engine's pump read, the handler's
 * own read, an application's processIncoming() loop, the heartbeat, or the replay of a reconnect another fiber runs.
 * {@see reject()} keeps the first error and then fires the run's wake-up ({@see wakeUp()}), with no suspension in
 * between, which the pump read and the idle backoff wait with, as they wait with the wake-ups of the iterator's stop()
 * and drain() (#181): a rejection recorded while the engine waits ends that wait at once, rather than at the earliest
 * pull's deadline, its expiry plus a second (#206). The engine looks at {@see error()} again after every wait of its
 * own that can let another fiber run, its handler's and onError's included, before every pull it issues, and the guard
 * of a pull's publication before every attempt to write it, a retry after a recovery included, so that no pull goes
 * out on an inbox the run knows the server rejected.
 *
 * The error is read through the state of the wake-up, by a method static analysis treats as impure, as the client
 * drain's request is ({@see PullPipelineDrainParticipant::isHandOverRequested()}): another fiber records it during
 * any await of the engine, so the engine's later checks of it are not taken as settled by its earlier ones.
 *
 * @internal Not part of the supported public API.
 */
final class PullInboxRejection
{
    /** The -ERR text the server rejected the inbox with: the first one, kept for the rest of the run. */
    private ?string $error = null;

    /** Fired once the rejection is recorded, and only then while the run lasts: Amp fires it on destruction, after the run. */
    private readonly DeferredCancellation $wakeUp;

    public function __construct()
    {
        $this->wakeUp = new DeferredCancellation();
    }

    /**
     * Records the server's rejection of the inbox, $error being the -ERR's text: the first one is kept, and the run's
     * wake-up fired. Does not suspend, since it runs inside the dispatch of the -ERR: the wake-up's callbacks are
     * queued, not run.
     */
    public function reject(string $error): void
    {
        $this->error ??= $error;
        $this->wakeUp->cancel();
    }

    /**
     * The -ERR text the server rejected the inbox with, or null while it has not.
     *
     * @phpstan-impure Another fiber's read records the rejection while the engine awaits.
     */
    public function error(): ?string
    {
        return $this->wakeUp->isCancelled() ? $this->error : null;
    }

    /**
     * The run's wake-up for the rejection: fired once it is recorded, so that a wait composed with it ends at once. As
     * for the run's other wake-ups ({@see PullPipelineControl::stopInterruption()}), compose it only while it has not
     * fired: once it has, the engine sees the rejection before its next wait and ends the run.
     */
    public function wakeUp(): Cancellation
    {
        return $this->wakeUp->getCancellation();
    }
}
