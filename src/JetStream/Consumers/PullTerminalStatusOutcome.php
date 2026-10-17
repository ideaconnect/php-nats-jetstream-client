<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

/**
 * What an infinite run of the pipelined pull engine ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()})
 * does once it has met a terminal pull status (a 409 Consumer Deleted, a server error, ...) and closed its inbox to new
 * deliveries (#211): the lifecycle rules apply again, in their order, since that wait can let another fiber run. The
 * retire phase meets such a status on an empty retire; the hand-over after a frame that ended the connection meets one
 * on a pull that brought no message (#214). Both go the same way.
 *
 * @internal For the pull consumer engine; not part of the supported API.
 */
enum PullTerminalStatusOutcome
{
    /**
     * A stop(), or a close that discards what the connection received, came first: the run returns the count it has
     * delivered, with no onError and no hand-over of what it still holds.
     */
    case ReturnTheCount;

    /**
     * The client's drain() asked for the hand-over, or the server rejected the run's inbox (#206): the top of the loop
     * hands the rest over and returns the count, or throws the rejection. onError was not called.
     */
    case BackToTheTop;

    /**
     * onError, when set, got the status, once: the run hands over what it still holds, the overflow included, and
     * ends. No path pulls again.
     */
    case EndAfterTheHandOver;
}
