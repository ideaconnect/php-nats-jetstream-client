<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

/**
 * What an infinite run of the pipelined pull engine ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()})
 * received that no pull in flight had room for (#187). The engine's count of its pulls decides how many requests it
 * issues, not whether a message the server sent the run reaches the handler: the server can hold more of the run's
 * requests than the run has in flight. A server that outlived the connection serves the requests the run issued before
 * a reconnect once the reconnect subscribes the run's inbox again, and a pull written from the reconnect buffer reaches
 * the new connection after the run counted it as lost. Such a message carries no token to tell its request by, so the
 * router gives it to the oldest pull still open, and what none of them has room for comes here, where it used to be
 * dropped as a straggler: gone on a consumer without acks or with max_deliver 1, delivered again after the ack wait
 * otherwise.
 *
 * The router appends here, in the order messages arrive, and once this holds a message every later one joins it here
 * too, even one a pull in flight has room for, until the engine has handed it all over: a newer message cannot reach
 * the handler ahead of an older one through a pull. The engine hands it over right after the pulls it retires, which
 * hold older messages, and before it issues another pull or reads on; the delivery takes what arrives while the handler
 * runs as well, until the buffer is empty. Its messages count in the run's total, not against any pull's batch, and a
 * delivery of them does to the run's idle state and its group pin what a pull's does. A finite run (setIterations())
 * has none: it keeps its exact count, and a message past its last pull's batch is dropped, as fetchBatch() drops one.
 *
 * @internal Not part of the supported public API.
 */
final class PullOverflow extends PullMessageBuffer {}
