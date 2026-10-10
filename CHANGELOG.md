# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).

Each entry is tagged so the version impact is clear:

- `[bugfix]` - corrects wrong behavior. Bump the patch version.
- `[feature]` - adds new, backward-compatible capability. Bump the minor version.
- `[docs]` - documentation only, no code change. No release needed (or patch).
- `[bc-break]` - changes existing behavior in a way that can break callers. Bump the major version.

Note on flags: a `[bc-break]` that only corrects an evident bug is treated as a
`[bugfix]`, not a real break, even though observable behavior changes.

## [2.24.5] - 2026-10-10

### Upgrade notes

- The library now requires `amphp/amp ^3.1.3`, up from `^3.1` (#200). 3.1.3 is a patch release with the same
  requirements as 3.1.2 (`php >=8.1`, `revolt/event-loop ^1 || ^0.2`). An application whose lock holds amp 3.1.0 to
  3.1.2 gets this release and amp 3.1.3 with `composer update`, or with
  `composer update idct/php-nats-jetstream-client --with-all-dependencies` (`-W`). Without `-W`, updating this package
  alone keeps the locked amp, and Composer stays on 2.24.4 without an error.

### Fixed

- [bugfix] Under `amphp/amp` 3.1.0 to 3.1.2, which `composer.json` still allowed, a request with waiting for a reconnect
  disabled lost a reply that came in the same read as a server's lame-duck `INFO` (#200). Before 3.1.3,
  `CompositeCancellation::isRequested()` and `throwIfRequested()` reported a part that had fired only one event-loop tick
  later. With `waitForReconnect: false` the read that met the lame-duck `INFO` asks such a composite whether the
  request's wake-up has fired - that is, whether its reply came in the chunk - and the request then returns the reply.
  Under 3.1.2 it was told no, so the request failed with `Connection is not open`, its reply delivered and dropped,
  every time the reply came behind the `INFO` (reconnect on, a cluster to fail over to). `requestMany()` and
  `fetchBatch()` returned what they had received, and a `SubscriptionQueue` poll kept its message for the next poll.
  amp 3.1.3 asks the composite's parts directly, and the library now requires it. Two existing tests, which CI ran under
  the newest amp only, pin this and fail under 3.1.2:
  `OperationReadLameDuckFailoverTest::testWithWaitingDisabledAnOperationWhoseReadBringsTheLameDuckInfoFailsAtOnce`
  (the reply case) and `OperationReadReconnectLifecycleTest::testAnOperationParkedOnTheReconnectItStartedDoesNotSpin`,
  whose look counter a 3.1.2 composite never asked.

## [2.24.4] - 2026-10-10

### Upgrade notes

- Requests now work while the client's `drain()` delivers (#213), which withdraws the 2.24.0 advice to ack with `ack()`
  in a handler that may run during a drain: `ackSync()`, a JetStream publish, a Key/Value read or write, `request()`
  and `requestMany()` made by a handler the drain runs - a pull consumer run's in its hand-over, or a core
  subscription's with a message the drain delivers - now get their replies, also when the client has made no request
  before. The drain keeps the shared reply inbox subscribed until its delivery phase is over, subscribes it for a
  first request, and unsubscribes it at the end, so its `UNSUB` now follows the requests on the wire instead of
  coming first. The round trips count against the drain's one budget (`requestTimeoutMs`).
- The delivery phase ends once everything is delivered, or at the drain's deadline, and every request still waiting
  then fails with `Connection is not open`, whichever fiber made it: a request of another fiber is taken while the
  drain delivers, but the drain does not wait for its reply, which can come too late although the server has acted on
  it; give a JetStream publish made there a `msgId`. A request made after that phase, after a `disconnect()` that
  interrupts the drain, or from the listener of the drain's deadline report is still refused, unsent. Subscriptions,
  and what needs one of its own (`fetchBatch()`, `fetchNext()`, a pull consumer's new pulls, batched Direct Get,
  Key/Value `keys()` and `history()`, an Object Store download of more than one chunk), stay refused while Draining.
- A drain with a connection now hands nothing more over once its deadline has passed: no handler gets another message,
  also not the first of a new delivery pass, which a final backlog pass used to hand over past the deadline to a
  handler whose request-based ack would then have been refused; the "drain deadline exceeded" report counts that
  message as well. A drain that found no connection to drain keeps its final backlog pass as before.
- `disconnect()` now ends the requests in flight as it begins, wherever they wait, with `Connection is not open`,
  rather than once the close of the transport fails their reads; a request made while the close is under way is not
  sent.
- The request that subscribes the reply inbox does so within its own timeout and cancellation (#194): a request whose
  inbox's `SUB` meets a dead socket and a reconnect that outlasts its timeout now fails with `Request timed out for
  subject <subject> while waiting for the reply inbox to be set up`, where it used to wait up to another
  `requestTimeoutMs` and fail with `Subscribe to "_INBOX.<inbox>.*" timed out waiting for the connection to be
  re-established`.

### Fixed

- [bugfix] A request made while the client's `drain()` delivered failed at once with `Connection is not open`, so a
  pull consumer handler that acked with `ackSync()` processed every message the drain handed over twice (#213). Since
  2.24.0 the drain hands the messages a pull consumer run holds to its handler, but it unsubscribed the shared reply
  inbox before delivering anything, and the connection refused every request while Draining: the handler processed
  the message, its `ackSync()` failed, nothing reached the server, and the server delivered the message again after
  `ack_wait`, to be processed a second time. Measured on nats-server 2.12.15 with the iterator's defaults, a producer at
  20 messages a second and the worker drained 4.5 s in: 29 of 87 messages were processed twice. A handler that made a
  request as part of its work (a JetStream publish, a Key/Value write) failed on its first message instead, and a core
  subscription's handler whose message the drain's flush delivered could not make a request either. The drain now
  keeps the reply inbox through its delivery phase and releases it afterwards, lets a request subscribe it when the
  client has none, and takes requests through the bounded request writers while it delivers. One request lifetime per
  connection, ended by the application's close, bounds every wait of every request: the end of the delivery phase -
  everything delivered, or the deadline, which a timer enforces wherever the drain is - seals delivery, counts what is
  left for the deadline report, and only then ends the requests still waiting, so that a run whose handler a woken
  request lets go on cannot hide what it held, and the drain still closes within its budget. A write that fails while
  the drain holds the connection is neither retried nor followed by a reconnect.
- [bugfix] The request that subscribed the shared reply inbox did so without a budget, so a `SUB` held up by
  backpressure, or the reconnect its failed write started, could keep the request waiting past its own timeout and
  its caller's cancellation (#194). The set-up now runs within the request's budget, through the bounded writer of the
  guarded inboxes; a request that joined the set-up is no longer failed with the other request's timeout when that
  request stops waiting: the abandoned `SUB` is followed by an `UNSUB` once it is out, and the joiner subscribes the
  inbox itself within its own budget. The `UNSUB`s owed for muxes dropped before are taken in the step that writes
  them, so a set-up that never writes leaves them owed for the next one.

## [2.24.3] - 2026-10-10

### Upgrade notes

- An infinite `PullConsumerIterator::handle()` run (no `setIterations()`) now hands its handler every message the
  server sends it, also more than its pulls in flight asked for (#187). After a reconnect the server can hold more of
  the run's pull requests than the run counts: a server that outlived the connection serves the requests the run
  issued before the reconnect once the reconnect has subscribed the run's inbox again, and a pull written during the
  outage goes out from the reconnect buffer afterwards. Those messages used to be dropped; they now reach the handler
  in arrival order, behind what the run's pulls hold, and count in the result of `handle()`, so that across a
  reconnect a handler can get more messages than `setBatching()` times `setDepth()`. The run issues no new pull while
  it holds such messages. A terminal pull status (`409 Consumer Deleted`, say) now also hands over what the pulls
  behind the one it ended hold before `handle()` resolves, where those messages used to be left unacked. `stop()`,
  `disconnect()` and a `drain()` whose budget runs out still leave the rest undelivered, and a finite run keeps its
  exact count and still drops a message past its last pull's batch.

### Fixed

- [bugfix] An infinite pull consumer run dropped messages it had received when the connection reconnected (#187).
  The engine behind `PullConsumerIterator::handle()` ends its pulls in flight once the connection's reconnect count
  moves, so that it pulls again on the new connection at once (#120), and it dropped them with what they held: a
  message the reconnect's own delivery gave a pull while a lower subscription's handler held the engine's read up,
  and the answer to a pull the engine had issued on the new connection before it saw a reconnect that the handler's
  own `ack()` ran. It also dropped as a straggler every message none of its pulls in flight had room for, which a
  server that outlived the connection sends for the requests the run issued before the reconnect, or wrote from the
  reconnect buffer during it. The handler never got such a message; the server delivered it again only after
  `ack_wait`, or never on a consumer with `ack_policy: none` or `max_deliver: 1`. Measured on nats-server 2.12.15 with
  the worker's socket closed while the server stayed up, an infinite run of batch 1 and depth 1 on a consumer with
  `ack_policy: none`: after the reconnect the consumer held two waiting requests, the worker received m-1 and m-2, and
  the handler got only m-1, the consumer's `ack_floor` at 2. The run now ends its pulls instead of dropping them: what
  they received goes to the handler in issue order before any pull goes out, a pull that received nothing leaves
  without its status being classified, and what no pull has room for is held for the handler in arrival order, behind
  the pulls, wherever the run goes on or ends (the iterator's and the client's `drain()`, a failure, a terminal
  status, a frame that ended the connection), and counted in the drain's deadline report. The run checks the
  reconnect count again after every wait, the handler's included, so that a status the old connection brought a pull
  still to be retired neither ends the run through `onError` nor drops a group's pin, and a pull issued after a
  reconnect the handler ran is not ended with the old ones; it never does while the client's `drain()` is under way,
  which still gets what the pulls held. The iterator's `drain()` across a reconnect now delivers what the pulls in
  flight had received. Found by the review of #178 (2.14.0), and of #210 (2.23.0).

## [2.24.2] - 2026-10-09

### Upgrade notes

- Batch fetch setup now draws on `expiresMs + 1000` ms, including inbox SUB, failed-write
  recovery, PUB and collection. An empty fetch whose own budget expires reports the usual
  `JetStreamException('No messages received within timeout', 408)` instead of a global subscribe
  timeout. A longer fetch may wait past `requestTimeoutMs`. Direct Get includes setup in its
  initial no-progress interval and renews that interval when replies arrive, even during a pending
  PUB; the multi-subject helper keeps a separate interval for each chunk (#188).
- A delayed pull is shortened to fit the remaining local budget with a 100 ms response margin.
  If its requested heartbeat no longer fits half that expiry, that attempt omits the heartbeat
  and its local heartbeat-miss check. Already-started transport writes remain uncancellable.

### Fixed

- [bugfix] Bound `fetchBatch()`, `fetchNext()`, `directGetBatch()` and `directGetLastForSubjects()`
  setup and failed PUB recovery by their own budgets, including inline write backpressure.
  Batch requests do not enter the reconnect buffer; a retry checks its inbox and remaining budget.
  Completed replies wake a pending publication, and late failed writes recover without resending (#188).
- [bugfix] Release timed-out or rejected batch inboxes locally before waiting for wire cleanup.
  An abandoned SUB that completes late is followed by UNSUB on its own connection; cleanup of a
  replayed inbox during a slow Reconnected listener also releases the server subscription. An
  UNSUB stalled by backpressure cannot hold the batch result beyond its budget (#188).

## [2.24.1] - 2026-10-09

### Upgrade notes

- A request whose PUB/HPUB write discovers a lost connection now honors its existing timeout,
  caller cancellation and `waitForReconnect: false` while the connection recovers (#215).
  `requestMany()` throws `TimeoutException` when its budget expires before the send completes;
  after a successful send, its collection timeout still returns the replies collected, possibly none.
  A transport write already in progress can finish after cancellation or timeout; neither guarantees
  that the server did not act on the request.

### Fixed

- [bugfix] Keep `request()`, `requestWithHeaders()` and `requestMany()` PUB/HPUB writes and their
  single recover-and-retry within the request's original budget, including inline socket backpressure.
  Recovery continues independently after the request ends; expired requests are neither buffered
  nor retried later. Each attempt rechecks the budget, connection state and shared reply inbox before
  writing, so an inbox rejected or dropped during recovery cannot receive a retry (#215).

## [2.24.0] - 2026-10-09

### Upgrade notes

- A JetStream pull consumer run, `PullConsumerIterator::handle()`, now follows the connection's own rule when the
  application closes the connection (#207): the client's `drain()` hands over what the run's pulls hold, and
  `disconnect()` discards it, wherever the run is. Once its flush is done, `drain()` asks each run to hand every pull in
  flight to the handler, in order, while the connection is Draining, so that the handler's acks still go out, and waits
  for that, the handler included, within its budget (`requestTimeoutMs`); the run then ends and `handle()` resolves with
  its count, as after the iterator's `drain()`, where it used to fail with `Connection is not open`, the handler having
  got none of those messages. A `drain()` can therefore take as long as such a handler, up to its budget, before it
  closes the connection; when the budget runs out first, the rest is discarded and counted in its
  `drain deadline exceeded` report, and `handle()` still resolves with what was handed over, unless the run was writing
  a pull as the drain closed the connection, which fails it with that write's error. Only the acks the handler publishes
  go out during that hand-over (`ack()`, `nak()`, `term()`, `inProgress()`): the connection refuses requests while it is
  Draining, so `ackSync()`, a JetStream publish or a Key/Value write fails there at once with `Connection is not open`;
  a handler that may run during a drain acks with `ack()`. A handler, an `onError` or an `errorListener` that awaits the
  client's `drain()` while the run calls it holds that drain for its whole budget, as a core subscription's handler that
  awaits it does, since the drain waits for the very run that waits for it, and the rest of the pull is then discarded:
  stop or drain the iterator there and drain the client once `handle()` has resolved, or call the client's `drain()`
  from another fiber or without awaiting it. While the client drains, a run issues no new pull. A `disconnect()`, from
  the handler or from another fiber, now also ends the hand-over of a pull the run retires, before the next message,
  where the rest of the pull used to reach the handler after the close had returned, every ack failing: on a consumer
  with acks that rest is delivered again after `ack_wait` and processed once, rather than twice, and on a consumer
  without acks (`ack_policy: none`) or with `max_deliver: 1` it is lost, as the hand-over before a run fails loses it
  since 2.22.0. `handle()` still fails after a `disconnect()`, with `Connection is not open` or the error of the read or
  write that met the close, except where the client's `drain()` had already asked the run for its hand-over, or where a
  finite run has no pull left to issue: it then resolves with its count. A worker that drains the iterator and awaits
  `handle()` before it closes the connection sees no change.

### Changed

- `[bugfix]` `disconnect()` discards what a pull consumer run's pulls hold also while the run hands over a pull it
  retires (#207), as it discards it in the hand-over before a run fails since 2.22.0 and as it discards what the
  connection itself has received and not delivered (nats.go `Close()` parity): the handler gets no message after the
  close, whether the handler or another fiber called `disconnect()`, and the rest of the pull stays unacked. The retire
  phase used to hand a full pull over whole, the handler running on the Closed connection after `disconnect()` had
  returned, every ack failing, so that with explicit acks those messages were processed and then delivered again. Every
  delivery of a run now checks the same signal before each message, the connection's own
  (`NatsConnection::isDiscardingUndelivered()`, `@internal`): a `disconnect()` under way or done, or a `drain()` once
  its budget has run out (from its `drain deadline exceeded` report on, also while it closes the connection), discards;
  a `drain()` within its budget hands over. A pull whose messages the close discards still counts as one that brought
  messages, so that the run goes on to meet the close and `handle()` fails as before, rather than end as if the stream
  were exhausted. Measured on nats-server 2.12.15 with `ack_wait` 3 s and a run of batch 2 and depth 1 whose pull holds
  m-1 and m-2, the handler awaiting `disconnect()` on m-1: in 2.23.0 m-2 reached the handler on the Closed connection,
  its ack failing, and with explicit acks the server delivered it again after `ack_wait` (`num_delivered` 2); with
  `ack_policy: none` it was processed after `disconnect()` had returned. Now m-2 is not handed over: with explicit acks
  it is delivered again after `ack_wait`, once, and with `ack_policy: none` it is lost (`ack_floor` 2).

### Fixed

- `[bugfix]` The client's `drain()` dropped what a pull consumer run's pulls held, and did not wait for a handler that
  was handing a pull over (#207). The engine behind `PullConsumerIterator::handle()` hands a pull's messages to the
  handler only when the pull completes (its batch full, a status from the server, or its deadline), and the drain
  delivered only the connection's own queues: it unsubscribed the run's inbox, read until the `PONG` of its flush and
  closed the connection under the run, whose read then failed and handed nothing over, since the drain had set the close
  intent (#197), and `handle()` threw `Connection is not open`. A worker that drained its client on `SIGTERM`, without
  draining the iterator first, so lost those messages on a consumer without acks and got them again after `ack_wait`
  otherwise; a pull that a message filled during the drain's flush was handed over while the drain closed the
  connection, the later messages reaching the handler on the Closed connection with their acks failing; and a run that
  went on while the drain flushed wrote its next pull onto the inbox the drain had unsubscribed. The run is now a drain
  participant (`DrainParticipant`, `@internal`), registered once its inbox is subscribed: once its flush is done, the
  drain asks each run to hand every pull in flight over, in issue order, while the connection is Draining, the request
  waking a run that waits in its pump read or its idle backoff, and waits for those hand-overs within its single budget,
  the `drain deadline exceeded` report counting what a run still holds when the budget runs out, which the run then
  discards, also while the drain closes the connection, rather than hand it over. The run then ends with its count, and
  it issues no pull while the connection is Draining. A drain without a connection to drain, or whose flush a
  `disconnect()` cut short, asks no run, whose read then fails and discards what it holds, as before; a drain that first
  waited for a reconnect still gets what the pulls held before the connection dropped. Measured on nats-server 2.12.15
  with `ack_wait` 3 s, the stream holding m-1 and m-2, and the client's `drain()` called 1 s into an infinite run of
  batch 3 and depth 1: in 2.23.0 the handler got nothing and `handle()` threw `Connection is not open`; with
  `ack_policy: none` the consumer then had both as delivered and acknowledged (`ack_floor` 2), the messages gone, and
  with explicit acks both came again after `ack_wait` (`num_delivered` 2). The handler now gets m-1 and m-2 while the
  connection is Draining, `drain()` returns once that is done, and `handle()` returns 2; with explicit acks both acks
  reach the server (`ack_floor` 2, `num_ack_pending` 0) and nothing comes again.

## [2.23.0] - 2026-10-09

### Upgrade notes

- An infinite JetStream pull consumer run, `PullConsumerIterator::handle()` without `setIterations()`, with reconnect on
  and `waitForReconnect` enabled (the defaults), now goes on after a fatal `-ERR` (`Stale Connection`,
  `User Authentication Expired`, ...) or a server `PING` whose `PONG` the socket would not take, once the reconnect has
  reopened the connection, as it goes on after a lost connection (#210). The frame's error goes to the error listener
  and the logger, and `handle()` no longer throws it: a worker that restarted its consumer when `handle()` failed with
  `Server sent error frame: 'Stale Connection'` finds the run still going. What the pulls held when the connection ended
  reaches the handler as part of the run and counts in the result of `handle()`, and a handler that throws there ends
  the run with its own exception, where 2.22.0 reported it and threw the frame's error. A finite run still ends with the
  frame's error, and so does an infinite one with reconnect off or `waitForReconnect: false`, whose connection the
  application is closing, or whose reconnect gave up, or whose new server refused the credentials, while the read that
  met the frame was still waiting for it. A reconnect that gives up only after that read stopped waiting, as one against
  a server that stays down does with the default options (ten attempts whose backoffs alone add up to about 43 s, where
  that read waits at most the run's expiry plus one second), ends the run as after a lost connection: the frame's error
  goes to the error listener first, and `handle()` then throws `Reconnect attempts exhausted`.

### Fixed

- `[bugfix]` An infinite pull consumer run ended on a fatal `-ERR` or a `PONG` the socket would not take even when the
  reconnect had reopened the connection, where it goes on after an EOF (#210). The pump read that met such a frame
  recovered the connection and threw the frame's error once the reconnect was done (#171), and the engine took that as
  the end of the run: it handed over what its pulls held (#197) and `handle()` failed with
  `Server sent error frame: 'Stale Connection'` while the connection was Open on the new socket, so a worker stopped
  consuming until something started it again. A server sends `Stale Connection` to a client that left its `PING`s
  unanswered for a few intervals (a handler that blocks the event loop with synchronous work, a paused process), and
  `User Authentication Expired` when a user JWT expires, which `jwtProvider` renews on every reconnect, so every expiry
  ended such a consumer. Such a run now goes on, as after a lost connection: the frame's error goes to the error
  listener and the logger, since nothing else says why the server closed the connection; what the pulls held goes to the
  handler in issue order as a delivery of the run, counted in its total, with `stop()` and a close of the application's
  ending it and a pinned group's pin captured from it; and the run pulls again on the new connection, once the reconnect
  is done, or, when the read's own wait ended first, once its next read has waited for the reconnect. A finite run,
  reconnect off, `waitForReconnect: false`, a reconnect that gave up while the read was still waiting for it and a close
  of the application's still end the run with the frame's error, and so does any failure that leaves the connection
  open; a reconnect that gives up later ends the run as after an EOF, with `Reconnect attempts exhausted`. Measured on
  nats-server 2.12 with `ping_interval: "1s"` and `ping_max: 1`: an infinite run of batch 1 and depth 1 whose handler
  blocked the process for 3.5 s on m-1 (`usleep()`), the client's own heartbeat off. The server logged
  `Stale Client Connection - Closing`; in 2.22.0, `handle()` threw `Server sent error frame: 'Stale Connection'` right
  after the `Reconnected` event, 3.5 s into the run, with `ack_policy` explicit and none alike, and m-2 and m-3,
  published once the client had reconnected, were never consumed (`num_pending` 2). The run now reports that error to
  the error listener, gets m-2 and m-3 on the new connection, and `handle()` returns 3 when the handler stops it on m-3
  (`num_pending` 0, `num_ack_pending` 0 with explicit acks). With the process blocked by a timer while the run's pull
  (batch 3) held m-1, 2.22.0 handed m-1 over before it threw; the run now hands it over as part of the run and goes on
  to m-2 and m-3. A handler that acks m-1 only after blocking still loses that ack to the closed socket, as before: the
  server redelivers m-1 after `ack_wait`.

## [2.22.0] - 2026-10-08

### Upgrade notes

- A JetStream pull consumer run, `PullConsumerIterator::handle()`, that ends with an error now runs the handler for the
  messages its pulls had received before it throws (#197): when its read fails because the connection is going (lost
  with `waitForReconnect: false`, a reconnect that gave up, reconnect off, a fatal `-ERR` whether or not the reconnect
  reopened the connection), when a pull's write fails, when the server rejects its reply inbox, or when its read fails
  for a reason `handlerErrorsFailOperations` or `slowConsumerErrorsFailOperations` makes the read's own. `handle()`
  still throws the same error, once the handler has run. On a closed connection the handler's ack fails; a handler that
  throws during that delivery, as one that lets its failed ack out does, ends it, and its exception goes to the error
  listener and the logger instead of out of `handle()`. A `stop()` still leaves those messages unhandled, and so does
  closing the connection yourself, with `disconnect()` or the client's `drain()`, which discards them as before: drain
  the iterator and await `handle()` before you close the connection. A handler that throws at any other time still ends
  the run at once with its own exception.

### Fixed

- `[bugfix]` A pull consumer run that ended with a failure of its own read or of a pull's write dropped the messages its
  pulls in flight had received (#197). The engine behind `PullConsumerIterator::handle()` hands a pull's messages to the
  handler only when it retires the pull (its batch full, a status from the server, or its deadline), so a pull holds
  messages the server counted as delivered and the handler has not seen, with `setBatching(10)` on a stream that
  trickles most of the time. A run whose read failed with the connection going, which it does with
  `waitForReconnect: false` once the connection is lost (#178) or failed over from a server in lame duck mode (#191),
  when the reconnect gives up, with reconnect off and on a fatal `-ERR`, ended with that error and dropped them, finite
  and infinite runs alike, and so did a run whose pull's write failed while an earlier pull held messages
  (depth above 1), whose reply inbox the server rejected, or whose read failed for a reason
  `handlerErrorsFailOperations` or `slowConsumerErrorsFailOperations` makes its own: the server redelivered those
  messages only after `ack_wait`, and never under `ack_policy: none` or `max_deliver: 1`. Such a run now hands the
  handler what its pulls received, in issue order, with `stop()` checked before each message as when it retires a pull,
  and then throws that error unchanged, as `fetchBatch()` returns its partial batch since 2.21.0. A run the application
  ends by closing the connection, with `disconnect()` or the client's `drain()`, still hands nothing over: the close
  discards those messages, as `disconnect()` discards what the connection received and has not delivered, rather than
  run the handler after the close has returned, with every ack failing, and a close made during that delivery ends it as
  a `stop()` does. That covers only the hand-over before the run fails: a pull the run retires normally is still handed
  over whole, as before, also when the handler or another fiber closes the connection meanwhile, since the connection's
  `drain()` lets acks out while it is Draining. A handler that throws during that delivery ends it and is reported to
  the error listener and the logger, the run's own failure being the one thrown, and a message read while the handler
  runs goes to a pull still to be handed over, when one is left open, rather than being dropped with the buffer being
  handed over; with none left it is dropped, unacked, and comes again only after `ack_wait`, or never under
  `ack_policy: none` or `max_deliver: 1`. Measured on nats-server 2.12 with reconnect off: a run of batch 3 and depth 1
  whose pull had received the stream's two messages, the client's TCP connection then cut through a proxy while the
  server stayed up. On a consumer with `ack_policy: none` the handler never saw them, and the server had them as
  delivered and acknowledged (`ack_floor` 2, nothing pending): they were gone. With explicit acks they came back only as
  redeliveries after `ack_wait` (`num_delivered` 2). The handler now gets both before `handle()` throws
  `Reconnect is disabled`. With explicit acks its acks fail on the closed connection and the server redelivers both
  after `ack_wait`: a handler that catches the failed ack gets both, and one that lets it out, as a bare
  `$js->ack($msg)->await()` does, gets the first, the failure going to the error listener. A `disconnect()` or `drain()`
  of the connection while the pull held them handed nothing over, in 2.21.0 and now alike, the messages then lost or
  redelivered as above. A graceful server shutdown was not affected: the server answers the waiting pull with
  `409 Server Shutdown` before it closes the connection, which retires the pull. Over the unit suite's scripted server
  the same holds with waiting disabled, a reconnect that gave up, a lame-duck failover with waiting disabled, and a
  fatal `-ERR` whose reconnect reopened the connection. An infinite run that goes on through a reconnect still discards
  what its pulls held at the reconnect (#187).

## [2.21.0] - 2026-10-08

### Upgrade notes

- One of the library's operations whose own read brings a server's lame-duck `INFO` (a `request()` or
  `requestMany()`, a `SubscriptionQueue` poll, a JetStream fetch or pull consumer, Key/Value `keys()` or `history()`,
  a `flush()` or `rtt()` waiting for its `PONG`) no longer waits for the whole failover: it waits within its own
  timeout, and with `waitForReconnect: false` fails at once with `Connection is not open` while the failover runs on,
  unless what it waits for came in the same read (#191). Your own `processIncoming()` and `readIncoming()`, a serving
  loop's read and the heartbeat still run the failover inline. The messages read with the `INFO`, those ahead of it
  included, are now delivered by the operation's read, with its rules for a handler that throws or a full queue,
  before the failover leaves the connection: unless a handler, or the error listener a report of that read goes to,
  awaits, ahead of the failover's `Disconnected` event. What such a handler starts runs once the failover has begun,
  since a publish and an operation each run in a fiber of their own: a publish is buffered and goes to the new server
  once the subscriptions are replayed, and an operation waits for the failover within its timeout, or with
  `waitForReconnect: false` fails at once with `Connection is not open`. Both used to run on the new server directly,
  the failover having run first.
- A `requestMany()` or a JetStream `fetchBatch()` whose read fails because the connection is going (lost or being failed
  over with `waitForReconnect: false`, a reconnect that gave up, or reconnect off) now returns what it has received
  instead of throwing; it throws only when it has received nothing. Code that took that exception to mean that nothing
  was delivered now gets the partial result.

### Fixed

- `[bugfix]` A `requestMany()` or a JetStream `fetchBatch()` whose read fails because the connection is going returns
  the replies or the partial batch it has received, as `requestMany()` does when the connection closes and
  `fetchBatch()` when the heartbeats stop, rather than throw and lose them: a fetched message the server counted as
  delivered was redelivered only after the ack wait, or never on a consumer without acks. Such a read fails with
  `waitForReconnect: false` once the connection is lost (#178) or is being failed over from a server in lame duck mode
  (#191), with any setting when the reconnect gives up, and with reconnect off when the connection drops, the
  configuration of symfony-nats-messenger, whose `get()` fetches. With nothing received, or for a failure that leaves
  the connection open (a full queue, a handler's own), they still throw. Up to 2.20.0 a lame-duck failover ran inline
  and the collection went on once it was over, though a `fetchBatch()` whose failover gave up threw at its next read all
  the same.
- `[bugfix]` An operation whose own read brought a lame-duck `INFO` overran its deadline by the whole failover (#191).
  The failover (#47) ran inline in the dispatch of the `INFO`, in the read's own fiber, and nothing bounded it: the
  dials, the handshake, the subscription replay and any backoff between attempts. The read now starts the failover in a
  fiber of its own and waits for it only within the operation's deadline and wake-up, as a read that meets a lost
  connection does since 2.14.0 (#178): the operation times out at its deadline, or returns what a delivery brings
  meanwhile, and the failover carries on, reports its failure through the error listener, as the inline failover does,
  and announces the new connection when it is over. Only a read whose chunk holds bytes behind the `INFO` that fail to
  parse still runs the failover inline, as it recovers the corrupt stream right after. The rest of the chunk the `INFO`
  came in is still the leaving server's (#182), now from the moment the failover is started: its `PING` and `INFO` are
  dropped, its `-ERR` is reported, and its `PONG` still completes the leaving connection's slot, so a `flush()` whose
  `PONG` came behind the `INFO` succeeds, with `waitForReconnect: false` as well. Verified on nats-server 2.12 with two
  servers, the second paused so that its dial hung until `connectTimeoutMs` (1 s), and the first put into lame duck mode
  with `SIGUSR2`: a `request()` with a 300 ms timeout whose read brought the `INFO` returned after 3.29 s, once the
  failover had reached the second server; it now times out after 0.30 s with the failover under way, and the second
  server's connection is announced 3.17 s after the `LameDuck` event, as before.
- `[bugfix]` A lame-duck failover no longer fails over a connection that a reconnect opened while a `DiscoveredServers`
  or `LameDuck` listener of the `INFO` was suspended (#191). It took the connection that was current once the listeners
  had returned: a `LameDuck` listener that awaited a `flush()` on a connection that had just died let the flush's failed
  write reconnect, and the failover then failed the new connection over as well, a second reconnect for nothing. It now
  fails over only the connection the `INFO` came on, whether it runs inline (your own read, a serving loop's, the
  heartbeat's) or in a fiber of its own (an operation's read).

## [2.20.0] - 2026-10-08

### Upgrade notes

- Your own read, `processIncoming()` or `readIncoming()`, now first continues the subscriptions whose delivery an
  earlier read stopped at a handler that threw, before it reads the socket (#186): their later messages are
  delivered with nothing new on the wire, and when one of those handlers throws too, the read throws its exception
  without reading. A `processIncoming()` that used to block on an idle socket after a handler failure now runs those
  handlers, or throws their next failure, at once. The read then goes on as before, and its result still counts only
  what it read from the socket. It continues what any read stopped that way, an operation's read under
  `handlerErrorsFailOperations: true` included; the reads of the library's operations, a serving loop, the
  heartbeat, a reconnect and a drain are unchanged.
- A catch-and-continue loop whose connection drops now delivers that remainder, and throws its next failure, before
  it notices the drop. Before, with reconnect off those messages were discarded when the connection closed ("parsed
  inbound message(s) were discarded undelivered"), and with reconnect on they were delivered after the reconnect,
  their failures reported to the error listener rather than thrown. The cancellation you pass bounds the read's
  waits, not that delivery: a read whose cancellation has already fired still continues the remainder, then throws
  its `CancelledException`.

### Fixed

- `[bugfix]` The rest of a subscription whose handler threw in your own read waited in the client's memory until the
  server sent anything (#186). Since 2.13.0 (#177) such a read delivers the other subscriptions' messages and leaves
  the failing subscription's later messages queued for the next pass, but your next `processIncoming()` or
  `readIncoming()` went straight to the socket and delivered them only once a chunk arrived: on an otherwise idle
  connection the server's next `PING`, by default two minutes away, a `disconnect()` meanwhile discarding them, and
  the subscription's next failure reached the application just as late. Your own read now delivers that remainder
  before it reads, in sid order, and then any that a read made in one of those handlers, or another fiber's
  delivery, stops meanwhile, with its own rules: a handler that throws there is thrown, one failure per read, and a
  second failing subscription is reported. Otherwise the read goes on to read as before, and its result still counts
  what it read. Only what a handler failure stopped is continued: the later subscriptions of a delivery held up in
  another fiber's handler still wait for that delivery, unless a handler failure had already stopped one of them,
  which is continued whole, in order, the messages that delivery brought for it included; a subscription whose
  delivery is under way in another fiber is left to it; and a `disconnect()` already under way when the read starts
  still discards what is queued, while a pass already running when a `disconnect()` begins goes on to the other
  stopped subscriptions, as any pass under way does. When a handler that runs there awaits while the connection
  drops, the read waits for the reconnect before it touches the new socket, as a serving loop's read does. Verified
  on nats-server 2.12 with a catch-and-continue loop and one raw write carrying `slow:s1`, `s2`, `s3` and `jobs:j1`:
  `s2` and `s3` reached their handler 1 ms after the write, where on 2.19.0 they waited 3.0 s, until the next
  publish.

## [2.19.0] - 2026-10-07

### Fixed

- `[bugfix]` A JetStream pull fetch (`fetchBatch()`, `fetchNext()`), a pull consumer run (`PullConsumerIterator::handle()`)
  or a batched Direct Get (`directGetBatch()`, `directGetLastForSubjects()`) whose reply-inbox `SUB` the server rejected
  for the connection's subscription limit learnt of it only when its own read met the `-ERR 'maximum subscriptions
  exceeded'` (#175). When another fiber's read took it - the heartbeat's while the `SUB` write was held up, an
  application's `processIncoming()` loop's - the `-ERR`, which names no subject, was reported to the error listener and
  nothing else happened: the fetch waited out its expiry plus a second and reported an empty pull (`JetStreamException`
  408, `No messages received within timeout`), which callers treat as no messages; the Direct Get batch reported a
  stalled batch after the same wait; the pull engine, whose rejection handler the connection called only for an `-ERR`
  naming its subject, retired pull after pull at their deadlines. Measured through symfony-nats-messenger on 2.10.3: an
  empty batch after 1.6 s, the connection open, the `-ERR` only on the listener. The inboxes now get the shared reply
  inbox's rule, generalised (`NatsConnection::subscribeGuarded()`, `@internal`, exposed on `NatsClient`): a `PING` is
  written behind the inbox's `SUB` in the same write, and the inbox counts as held by the server once that `PING`'s
  `PONG` or a delivery on it arrives; a limit `-ERR` read before that, by whichever fiber, drops the inbox, writes its
  `UNSUB` (the `-ERR` may have been another `SUB`'s, and the server then still holds the inbox) and tells the operation,
  which fails at once: the fetch and the Direct Get batch with a `ConnectionException` naming the inbox and the limit
  (`JetStream pull fetch failed: the server may have rejected its reply-inbox subscription "_INBOX.JS.FETCH.<nuid>"
  because the connection is at its subscription limit (maximum subscriptions exceeded). A retry subscribes a new one.`),
  the pull engine through its fail-fast latch (#167), whose `JetStreamException` now says the limit when that is the
  cause. A reconnect replays such an inbox with a `PING` behind it again, with the same consequence, also when the
  inbox's own `SUB` write failed and the reconnect it started replays it: the subscribe then reports the rejection,
  not a closed connection, and the replay writes the inbox's `UNSUB`. As for the shared reply inbox, several inboxes
  unconfirmed at once - two fetches, a fetch and the reply inbox - cannot be told apart by the `-ERR`, so each is
  treated as rejected and each operation fails; an operation whose own read meets the `-ERR` still fails with the
  server's error, as before; a `subscribe()` of the application's beyond the limit behaves as before. A permissions
  violation naming a fetch's or a Direct Get's inbox now fails the call at once as well, quoting it, where the call
  waited out its deadline. `markSubscriptionUnbounded()` and `onSubscriptionRejected()` leave a sid that is no longer
  registered alone, so a rejection read between the `SUB` write and those calls leaves nothing behind. Callers that
  catch only `JetStreamException` around a fetch or a Direct Get batch, treating an empty result as routine, now see a
  `ConnectionException` at the subscription limit, on a connection that stays open.
  Verified on nats-server 2.12 with `max_subscriptions: 3`: with an application loop reading and the `SUB` write held
  up for 0.5 s, the fetch failed as soon as the write completed (0.50 s after it was issued) and a retry with a free
  slot got the stored message, where it reported an empty pull after 2.50 s.

## [2.18.0] - 2026-10-07

### Fixed

- `[bugfix]` A `PING`, `PONG`, `INFO`, `-ERR` or reply-inbox `MSG` read in the same chunk as a lame-duck `INFO`,
  behind it, was handled on the connection the lame-duck failover had just opened (#182). The failover (#47) runs
  inline in the dispatch of the `INFO`, and the dispatch then went on with the rest of the chunk as if nothing had
  happened: the old server's `PING` was answered with a `PONG` written on the new connection; a stale `INFO`
  overwrote the new server's info, and a second lame-duck `INFO` started a second failover, since the lame-duck flag
  had been reset for the new connection; a `PONG` completed the new connection's oldest PONG slot - the fence behind
  the replayed reply-inbox `SUB` - and so confirmed the replayed inbox before the new server had answered anything,
  as a late reply of the old server on that inbox did; an `-ERR` was applied to the new connection (`maximum
  subscriptions exceeded` dropped the replayed, still unconfirmed inbox, a permissions violation naming a subject
  fired the replayed subscription's rejection handler) and failed the read with `Server sent error frame` although
  the failover had succeeded. The dispatch now checks before each frame whether the chunk's connection is gone,
  replaced or ended, and handles the rest of the chunk for the connection it was read on: its messages are still
  queued for their subscriptions, which the failover replayed, though a late reply on the request inbox no longer
  counts as the new server's confirmation of the replayed inbox; its `PING`, `PONG` and `INFO` are dropped; its
  `-ERR` is reported through the error listener (`The server the connection left sent an error frame: ...`) and
  neither applied nor thrown. When the connection ends instead - the failover failed, or the `LameDuck` listener
  closed the connection itself - the chunk's `PING` is not answered on the closed transport either: the read returns
  what it read, and the next one fails with the closed connection, instead of the read failing with the PONG write's
  error. Nothing changes without a failover (reconnect disabled, or a pool of one): the chunk is handled as before.
  Found by the review of the read wake-up (#174, 2.12.0).

### Testing & CI

- `[docs]` `OperationReadReconnectLifecycleTest::testAnOperationParkedOnTheReconnectItStartedDoesNotSpin` no longer
  bounds the CPU time of the second an operation spends parked on the reconnect its read started, which depended on
  the environment: under Xdebug with coverage on a busy CI runner the second's own cost (three refused dials with
  their exceptions and listener calls, GC) reached the 0.15 s bound, while the same seeds passed elsewhere, and a 1 ms
  poll cost less than the bound on a fast machine. It counts how often the operation looks for its result instead,
  which a spin would do about a thousand times in that second and a parked operation not at all: the poll's buffer is
  swapped for a new `CountingBuffer` test double, which counts how often `next()` checks it, and the request gets a
  `CountingCancellation` as its cancellation. A process-wide count of the callbacks the event loop registers in that
  second was tried as well and dropped: the reconnect attempts of clients that earlier tests leave behind register
  their 500 handshake polls in a burst. The socket-read and dial counts stay. Dev-only, no library change.
- `[docs]` `ReadWakeupLifecycleTest::testTwoPollsOfOneQueueWithOneMessage` no longer bounds the CPU time of the 2 s
  that two polls of one queue spend on one message, the poll the delivery wakes finding nothing and waiting on to its
  timeout, which depended on the environment the same way: in the Infection initial run for 2.17.0, under Xdebug with
  coverage on the shared runner, the wait's own cost reached 0.82 s against the 0.5 s bound, while the four unit jobs
  of the same run passed it. It counts how often the two polls check the queue's buffer instead, with the same
  `CountingBuffer` in place of the buffer the polls share: six when no wait ends at once (one check each as the polls
  start, two each when the message comes), and two per pause of a millisecond, close to a thousand or more over the
  wait, for a woken poll whose next wait ended at once again and again, which the socket-read count does not see when
  the read returns before it reads. The bound is 60; the other assertions stay. Dev-only, no library change.

## [2.17.0] - 2026-10-07

### Fixed

- `[bugfix]` A `PullConsumerIterator::stop()` or `drain()` from another fiber - a signal handler's timer, a supervisor -
  while the pull engine waited on the socket for its in-flight pulls, or in its idle backoff, was seen only when that
  wait ended on its own (#181): at the earliest pull's deadline, its expiry plus a second (4 s with the default 3 s
  expiry, 31 s with a 30 s one), or at the end of the backoff, up to 500 ms. A call from inside the handler was
  unaffected. Each `handle()` run now gives the engine two one-shot wake-ups, one per flag, which `stop()` and `drain()`
  fire right after setting their flag; the engine's pump read and idle backoff wait with each wake-up while it has not
  fired, so the wait ends at once and the flag is seen at the top of the loop. `stop()` ends the run at once, with the
  pull inbox released as on every exit, and a `stop()` that lands while a pull of a generation is written ends the
  generation there: no further pull is written for a run about to end. `drain()` wakes the engine once, so it stops
  issuing pulls at once and, with nothing in flight, returns at once; the pulls in flight still complete, or reach
  their deadline, and their messages are delivered first, as before. A fired wake-up is never composed into a later
  wait, whatever fired it, so nothing spins: a latched drain does not end the reads that follow it; a run started
  again gets fresh wake-ups, so a stop in an earlier run cannot end a wait of the next; and a run still active when
  `handle()` is called again on the same iterator, its wake-ups replaced and fired by Amp as they are destructed, goes
  on as before #181, waiting to its pulls' deadlines until the shared flags end it. Start the next run only once the
  previous run's future has resolved: the runs share the stop/drain flags, so a `handle()` while a run is active
  clears a `stop()` that run has not seen yet. The internal `PullPipelineControl` carries the wake-ups as
  `stopInterruption()` and `drainInterruption()`, optional in its constructor.

## [2.16.0] - 2026-10-07

### Fixed

- `[bugfix]` A `request()` or `requestMany()` whose read was parked on the socket when another fiber's read brought the
  `-ERR` that drops or rejects the shared reply inbox - `maximum subscriptions exceeded` before the server was known to
  hold the inbox, or a permissions violation naming it - noticed the drop only with the server's next bytes or at its
  deadline (#180). The client drops the inbox inline in the dispatch of that `-ERR`, and since 2.12.0 a request's read is
  woken by its own reply alone, so nothing ended the read; the shape arises when the other fiber's dispatch is held up
  ahead of the `-ERR`, as it is while it awaits the write of the PONG for a server PING in the same chunk (socket
  backpressure): the read slot is free by then, the request's read takes it, and the `-ERR` drops the inbox under it. A
  request that looked while another operation's read - a poll's, a flush's - held the socket, and so waited at its own
  wait for the read slot rather than in a read, noticed the drop only once that read ended. The connection now fires
  every waiting request's wake-up when it drops or rejects the inbox, whichever fiber's read meets the `-ERR`, and the
  wake-up ends every wait the request makes, its read and its wait for the read slot alike: the request looks again at
  once, failing with the dropped-inbox error or the permissions error, returning a reply delivered by then, or, for a
  `requestMany()` with replies collected, returning them. A terminal close while a request waits for its reply ends the
  request at its next look with `Connection is not open`, as a read still waiting fails on its own: this is the exit of a
  request a rejection woke right before an error listener answered the `-ERR` with `disconnect()`, which resets the
  rejection latch; such a request would otherwise have looked every millisecond until its deadline and ended with a
  timeout. Nothing else is woken: polls, fetches, consumers and your own reads are not involved.

## [2.15.0] - 2026-10-07

### Upgrade notes

- An operation that reads the socket while it waits for a result of its own - a `SubscriptionQueue` poll,
  `fetchBatch()`/`fetchNext()`, `directGetBatch()`, the pull consumer, or Key/Value `keys()`/`history()` - now
  takes what is already queued for its own subscription before it reads or waits for another fiber's read (#179).
  This reverses the 2.12.0 note that "an operation gets its message once the in-order delivery reaches it, so a
  handler ahead of it that outlasts the operation's deadline still makes it time out": an operation now returns at
  once what an earlier or a concurrent read has already queued for it, even while a handler ahead of it, in another
  fiber, is still running. In particular this is what lets a user handler poll a `SubscriptionQueue` of its own for
  a message delivered in the same chunk: the handler's own message is ahead of the queue's in that chunk, the
  queue's message is queued behind the handler's delivery, and the poll now takes it from inside the handler instead
  of reading the socket and timing out. The reorder is only ever of the operation's own subscription ahead of
  another subscription's message queued with it; order within a subscription, and between the other subscriptions,
  is unchanged, and NATS guarantees order only within a subscription. `request()` and `requestMany()`, whose replies
  share one inbox, are unchanged, and so are your own `processIncoming()` and `readIncoming()`.
- The take is only ever of what is ALREADY queued when the operation looks, so two shapes are unchanged by it. A
  poll whose own read brings the chunk (no other fiber reads) still runs any handler ahead of its message in that
  read's own delivery pass, and returns the message once that handler returns, however long it takes: nothing is
  queued for the poll until the pass reaches it. And while a reconnect is under way the operation still waits for it
  first, within its own timeout, as before 2.13.0: the take sits next to the in-order delivery of what a read left
  queued, after the wait for a reconnect, so a message already queued for the operation is taken once the connection
  is back, not during the outage. (The take was placed there, rather than ahead of the reconnect wait, to keep the
  reconnect path exactly as 2.13.0 shipped it; a message an operation received before the drop is delivered during
  the outage only when another fiber's delivery of it is still under way, as 2.13.0's #174 note describes.) An
  overflow of the operation's own subscription met by the take fails the operation, not the read that queued the
  chunk; a poll concurrent with its own `unsubscribe()` may still return a message the connection received before
  the `UNSUB` went out.

### Fixed

- `[bugfix]` A handler that polled a `SubscriptionQueue` of its own for a message delivered in the same chunk waited
  out its whole timeout, and the message reached the queue only once the handler returned (#179). The chunk held the
  handler's own message ahead of the queue's; the in-order delivery was suspended inside the handler, so the queued
  message was not delivered until the handler returned, and the poll read the socket with it already queued and
  returned `null` (or `[]`, or a fetch's expiry) at its deadline. The same shape without a handler - an operation
  whose message was queued behind a delivery held up in another fiber's awaiting handler - got it, since 2.12.0, only
  when that delivery reached it. An operation's read now takes what is already queued for its own subscription first,
  before it reads or waits for another fiber's read, delivering that one subscription with the read's own rules (its
  own overflow or failing handler still fails it) and returning so the operation looks again; the other
  subscriptions' queued messages stay in order for the delivery under way. It is skipped while that subscription's own
  delivery is under way further up the stack, and while a `disconnect()` is closing the connection.

## [2.14.0] - 2026-10-07

### Upgrade notes

- One of the library's operations whose own read is the first to notice a lost connection - a `request()` or
  `requestMany()`, a `SubscriptionQueue` poll, `fetchBatch()`, `fetchNext()` or `directGetBatch()`, a pull
  consumer, Key/Value `keys()` or `history()`: every operation that reads for a result of its own - no longer waits
  for the whole reconnect it starts: it waits within its own timeout, as it does for a reconnect another fiber
  runs, and with `waitForReconnect: false` it fails at once with `Connection is not open`, the read's error as its
  previous (#178). A `flush()`, `rtt()` or `drainSubscription()` whose own read is the first to notice fails with
  `Connection lost before the server answered the PING` as soon as the reconnect's first attempt ends the old
  connection's `PONG`s, or at once with `Connection is not open` when waiting is disabled, and
  `drainSubscription()` then reports that and removes the subscription while the reconnect runs on. Code that
  relied on such an operation returning only once the connection was back - polling in a loop and taking the next
  successful result for the reconnect, say - now sees it time out or fail during the outage, with the reconnect
  still under way; the connection events say when the connection is back, and with waiting enabled the next
  operation waits for the reconnect within its own timeout, as before. A reconnect that gives up while nothing
  waits for it any more is announced by its `Closed` event and the log: an operation that timed out before the
  reconnect gave up does not see `Reconnect attempts exhausted`, and the exception of a `drainSubscription()` flush
  that used to report it now reports the lost `PONG`. With `waitForReconnect: false` the reconnect such an
  operation started advances only while something awaits on the event loop, as one the heartbeat starts always
  did: a synchronous application that only ever issues operations failing on the spot has to keep awaiting
  something for the connection to come back. Your own `processIncoming()` and `readIncoming()`, a serving loop's
  read and the heartbeat still run the reconnect themselves and wait for all of it.

### Fixed

- `[bugfix]` An operation whose own read was the one on the socket when the connection dropped waited for the whole
  reconnect, however long it took, before it returned (#178). `readChunk()`'s failed-read path ran the reconnect
  inline in the operation's fiber, so neither the operation's deadline nor its wake-up (#174) could end the wait:
  with the server down for 2 s and a 1 s deadline, `SubscriptionQueue::next()` and `request()` returned after 2 s,
  although a delivery still under way had brought their result 50 ms into the outage, and a `fetchBatch()` or a
  pull consumer's read did the same. On a real server, where a poll's read is often the only one on the socket, a
  `next()` with a 1 s timeout returned only once the server was back. Such a read now starts the reconnect in a
  fiber of its own, like a failed control write does, and waits for it within the operation's deadline and wake-up:
  the operation returns what a delivery brings during the outage, or times out at its deadline, and the reconnect
  carries on, announces the new connection once it is over and fails on its own terms when it gives up. With
  `waitForReconnect: false` the operation fails at once with `Connection is not open`, the read's error as its
  previous, as one whose control write noticed the loss does, instead of waiting for the reconnect first. A
  reconnect another fiber already runs is still joined within the same wait, your own read still runs the reconnect
  itself, and a listener called during the reconnect is still refused an operation that would wait on it.

## [2.13.0] - 2026-10-07

### Upgrade notes

- Your own read, `processIncoming()` or `readIncoming()`, now delivers the messages it brought for the other
  subscriptions before it throws a handler's exception; only the failing subscription's own later messages stay
  queued, until the next read that receives anything (or a serving loop's read) continues with them: on an
  otherwise idle connection that is the server's next `PING`, by default two minutes away. Under
  `handlerErrorsFailOperations: true` an operation's read does the same before it fails the operation, and a
  handler's `CancelledException` no longer ends the operation's wait when the operation's own result arrived in
  the same read behind it: the result is delivered first and the operation completes with it. A second
  subscription whose handler throws in the same read has its exception passed to the `errorListener` and logged
  at error level, since one read throws one exception; it used to be thrown by the next read that received
  bytes, so without an `errorListener` or a logger it is now seen nowhere. Code that relied on nothing behind a
  throwing handler being delivered until it read again now sees those handlers run within the read that throws,
  and a handler behind the failing one that awaits, or an `errorListener` that awaits while a second failure of
  the same read is reported, delays the exception by as long.

### Fixed

- `[bugfix]` A message left queued by a handler that threw in another fiber's read waited for the next read
  that received bytes (#177). The application's `processIncoming()` read a chunk holding a message whose handler
  threw and, behind it, the message an operation waited for; the delivery stopped at the failing handler, and
  nothing delivered the rest until the server sent more, so `SubscriptionQueue::next()`, `fetchAll()`,
  `request()`, `requestMany()` and `fetchBatch()` waited out their whole deadline with their result queued the
  whole time, and the pipelined pull consumer re-pulled at every expiry with its message queued the whole time;
  on a real server, which pings only every two minutes, that is the whole deadline every time. A read that
  throws a handler's exception now delivers everything else it queued first: the other subscriptions' messages,
  in sid order, so the operation's wake-up fires as usual and it returns its result at once. The failing
  subscription's later messages still stay queued, in order, for the next read that receives anything: your own
  read then throws again if the next handler throws, so each failure of that subscription reaches you one read
  at a time, and an operation's or a background read that delivers them reports the failure instead. A serving
  loop's read, as `Service::run()` makes, still delivers what an earlier or a concurrent read has queued and not
  reached: the later sids of a delivery that another fiber's handler holds up, awaiting, and the rest of a
  subscription whose handler threw.

## [2.12.0] - 2026-10-06

### Upgrade notes

- **Custom transports.** `TransportInterface::readLine()` must keep every byte it has not returned when its
  cancellation ends it, a partial frame included: the connection cancels a read not only at a deadline but also to
  wake an operation whose result another fiber delivered meanwhile, and then reads on (#174). The built-in transports
  do, and the rule always held in practice, since every non-blocking poll and the heartbeat cancel their reads after
  a short bound. A transport that lost bytes on cancellation would now corrupt the stream in normal operation.

### Fixed

- `[bugfix]` An operation that reads the socket while it waits for a result of its own, a `SubscriptionQueue` poll,
  `fetchBatch()`/`fetchNext()`, `directGetBatch()`, the pull consumer or Key/Value `keys()`/`history()`, got a result
  that another fiber's read delivered only with the server's next bytes or at its deadline when that delivery reached
  it while the operation's read was already waiting (#174). That happened in three ways: the delivery was still under
  way when the read started, held up in the handler of another subscription that awaits an HTTP or database call, say,
  or in the write of the PONG a server PING ahead of the message is owed; it came in the few event-loop hops between
  the operation's check and the start of its read; or it came with the reconnect the read waited for. On a real server
  the first costs the whole deadline, the server pinging only every two minutes, and the third about two seconds. An
  operation's read now ends without reading as soon as anything is delivered to the operation's subscription,
  wherever the read waits: on the socket, whose read it cancels, behind another fiber's read, or for a reconnect; the
  operation then looks again. Application reads, `processIncoming()` and `readIncoming()`, are unchanged, and so is
  the order of deliveries: an operation gets its message once the in-order delivery reaches it, so a handler ahead of
  it that outlasts the operation's deadline still makes it time out.
- `[bugfix]` `request()` and `requestMany()` got a reply that another fiber's read delivered only with the server's
  next bytes or at their timeout in the same three cases (#174). Their replies come on the one reply inbox every
  request shares, so their read ends on their own reply instead, as the waiter that takes it fires: another request's
  reply does not end it. A request cancelled right after its reply arrived throws the caller's own cancellation, with
  its reason.
- `[bugfix]` `flush()`, `rtt()`, the flushes of `drain()` and `drainSubscription()` and the confirmation of the reply
  inbox end their read as soon as their PONG is in, whichever fiber's read took it, also when that read's dispatch
  had to wait before it reached the PONG, for the write of the PONG it owed the server for a PING ahead of it, say
  (#174). They used to wait for the server's next bytes or their deadline. A flush whose PING went out before the
  connection dropped fails with `Connection lost before the server answered the PING` as soon as the reconnect's
  first attempt clears the pong slots, rather than once the reconnect is over or at the flush's deadline.
- `[bugfix]` An operation's read that waited for a reconnect looks again before it reads the new socket, whether or
  not the reconnect delivered anything to it (#174). The pipelined pull consumer, which re-issues the pulls the old
  server forgot once it sees the reconnect, used to read the new socket first and re-pull only at the lost pull's
  deadline, its expiry plus a second.
- `[bugfix]` `SubscriptionQueue::next()` with a timeout, and `fetchAll()` with a limit and a timeout, returned a
  message that another fiber's read delivered while they paused between reads (an application's
  `processIncoming()` loop, say) only once the timeout ran out (#174). They started another read before they
  looked at the queue, and that read waited on the socket, with the message already there, for the server's
  next bytes or the whole timeout. They now take what arrived before they read again. `next()` had this shape
  since 1.0.0, `fetchAll()` since 2.0.0.
- `[bugfix]` `SubscriptionQueue::fetchAll()` also takes what such a read delivered during its pause when that
  does not complete the call, so its own next read has the queue's whole buffer (#174). When that delivery had
  filled the queue, the messages the next read brought overflowed it: they or the earlier ones were dropped, or
  under `SlowConsumerPolicy::Error` the call failed with a `SlowConsumerException`.
- `[bugfix]` The pipelined pull engine behind `PullConsumerIterator::handle()` handed over a batch that another
  fiber's read delivered while the engine waited for the write of a pull request (an application's
  `processIncoming()` loop, say, with the socket under backpressure) only at that pull's deadline, its expiry
  plus 1 s, or with the server's next bytes. It looked at its oldest pull only before it issued pulls, and
  then read with that pull's batch already there. It now looks again before it reads. The review of #174
  found this; the engine had the same shape since it arrived in 2.7.0.

### Testing & CI

- `[docs]` The E2E job runs the unit suite once instead of twice, and the Docker fixtures do less background
  work (#176). The job no longer runs the unit suite inside `composer test:e2e`: its coverage step runs it right
  after, together with the integration suite, and the Unit + Static jobs run it on PHP 8.2 to 8.5. The skipped
  pass took about 3 minutes and gave the timing flakes in #176 one more place to hit. `scripts/run-tests-e2e.sh`
  gained `SKIP_UNIT_TESTS` for this; without it the local flow is unchanged. The compose servers no longer
  run with debug and trace logging (`-DV`), which wrote every inbound payload through Docker's log pipeline
  and was never read, and their healthchecks probe every 30 s instead of every 2 s (nothing reads the health
  status; readiness comes from `scripts/wait-for-nats-services.sh`). That script now also waits for the
  WebSocket server (`nats-ws`, monitoring port 18229), which it had left out. The unused `workflow_dispatch`
  input `integration-repeat-count` is gone: no job read it. Dev-only, no library change.
- `[docs]` Timing-bound tests no longer depend on how fast the runner is or how late a timer fires (#176).
  About 100 unit tests and 11 integration tests bounded elapsed time close to what they measured, or relied on
  a timer firing in time, so a busy CI runner could fail them although the code was fine. Several did; the rest
  were found before they could, by checking how much room each bound left. They now decide by the order
  of events or by which of two timers fires first, measure from the event that matters (a dial is stopped by its
  own refused attempt, through a new `StopsDialOnRefusalConnector` test double, and timed from that stop), count
  idle sleeps instead of timing chunked reads (a new `LoopTickCountingTransport`
  test double), or keep their bounds far from what the broken behavior takes. Each changed test still fails when
  its bug is put back. The integration suite's recoverable client no longer gives a connection up after one late
  PONG, which made a busy NATS container add a reconnect. Two `WaitForReconnectTest` tests that could no longer
  fail since 2.10.0 were reworked so they can. Dev-only, no library change.

## [2.11.0] - 2026-10-05

### Upgrade notes

- A subscription handler that throws while an operation's read delivers to it no longer fails that operation:
  the exception is passed to the error listener and logged at error level, and the operation completes. With
  no `errorListener` and no logger it is visible nowhere: register an `errorListener`, or set
  `handlerErrorsFailOperations: true` to have operations fail as before. Code that caught another
  subscription's handler exception from `request()`, `flush()`, `rtt()`, a JetStream call or a
  `SubscriptionQueue` poll now gets the operation's result instead. `processIncoming()` and `readIncoming()`
  still throw it.

### Added

- `[feature]` `NatsOptions::$handlerErrorsFailOperations` (default `false`): set `true` to have operations fail
  as before the first change below: a subscription handler that throws fails whichever operation's read
  delivered to it, and a handler's `CancelledException` can end an operation's wait early. `Service::run()`
  and the background reads report it either way, and the second change below applies either way.

### Changed

- `[bugfix]` An operation that reads the socket while it waits for a result of its own no longer fails with
  another subscription's handler exception (#173). It used to throw the exception of whichever handler its
  read delivered to, even when its own result had arrived: a `request()` made inside a service endpoint failed
  with it, so the endpoint answered its requester with a `HANDLER_ERROR` reply and kept the other handler's
  message as its `last_error`; a JetStream publish could fail after the server had stored the message, so a
  retry stored it twice unless it carried the same message id within the stream's duplicate window; and the
  messages behind the failing one waited for another read. A handler that threw
  a `CancelledException` made a `request()` take it for the end of its own wait: the request read on with its
  reply left undelivered and timed out. Now the failure is reported through the error listener, logged at
  error level, the rest of the read is delivered, and the operation completes. This covers `request()`,
  `requestMany()` and what is built on them (JetStream publish, `ackSync()`, stream and consumer management,
  Key/Value and Object Store calls), `flush()`, `rtt()`, `fetchBatch()`/`fetchNext()`, `directGetBatch()`,
  pull consumers (`consumePipelined()`), Key/Value `keys()` and `history()`, and `SubscriptionQueue` polling,
  as well as a request waiting for its reply inbox to be confirmed. A handler of the operation's own
  subscription that throws still fails it; that is only ever a subscription the library makes for the
  operation, such as a `SubscriptionQueue`'s or a fetch's. `processIncoming()` / `readIncoming()`, the reads an
  application makes itself, still throw. An `-ERR` that fails a read but leaves the connection open, such as
  `maximum subscriptions exceeded`, still fails the operation whose read meets it: it names no subscription,
  and it is often the answer to what the operation itself sent. `handlerErrorsFailOperations` restores the old behavior. The README's new Handler Failures
  section describes the whole behavior.
- `[bugfix]` The delivery after a reconnect stopped at the first handler that threw: it reported the failure
  and left the messages behind it queued, for every subscription. An operation whose own read ran the
  reconnect, such as a `SubscriptionQueue` poll or a fetch, then waited for the server's next bytes for a result
  that had already arrived: a poll returned nothing once its timeout ended, and a fetch discarded what was
  queued for it when it unsubscribed. The delivery now delivers the rest as well, reporting any other handler
  that throws, whatever `handlerErrorsFailOperations` says (#173).

## [2.10.3] - 2026-10-05

### Upgrade notes

- An `-ERR` the server keeps the connection open for no longer ends the connection: `maximum subscriptions
  exceeded`, `Invalid Publish Subject` and any `Permissions Violation` other than `... for Publish to ...` and
  `... for Subscription to ...`, which the client still only reports. The read that brought it fails with it
  at once, as in 2.10.0, and no `Disconnected`, `Reconnected` or `Closed` follows, so the 2.10.1 note now
  holds only for an `-ERR` the server closes the connection after (`Stale Connection`, `Authorization
  Violation`, ...). The heartbeat, a reconnect's replay, a service's `run()` and the flushes of `drain()`,
  `drainSubscription()` and a service's `drain()` report it to the error listener instead and carry on:
  watch that listener, not `Closed`, for these errors.
- After such an `-ERR` the flushes of `drain()` and `drainSubscription()` read on to their `PONG`, so a
  server that then goes silent makes `drain()` wait out its budget, where it used to end at the `-ERR`. When
  the server closes the connection after it instead, `drainSubscription()`'s flush runs the reconnect itself,
  and with reconnect on the call returns only once that reconnect ends, past `requestTimeoutMs` if need be;
  2.10.1 and 2.10.2 waited for the reconnect only within that budget.
- Once the server rejects the shared reply inbox of `request()` and `requestMany()` for the subscription
  limit, requests fail fast until a slot is free, and the connection stays open: one already sent whose own
  read meets the `-ERR` fails with it, any other with a `ConnectionException` saying `request/reply failed:
  the server may have rejected the shared reply-inbox subscription ...`. Each request subscribes the inbox
  again and is sent only once the server has taken it, so every attempt costs a round trip and nothing
  throttles them: a caller that retries should pause between attempts.
- The write that subscribes the reply inbox now ends with a PING (`SUB _INBOX.<inbox>.* <sid>\r\nPING\r\n`).
  A transport double that answers only a write that is exactly `PING\r\n`, as one scripted for `flush()`
  often does, leaves it unanswered, so the `PONG` it sends for the next `flush()`, `rtt()` or `drain()`
  answers that PING instead, and the call times out. Such a double should answer every PING.
- A reconnect announces the new connection (`Reconnected`, or `Connected` when it completed a failed first
  `connect()`) once it is over. Operations that waited for it, a joined `connect()` and a `drain()`
  included, go on without waiting for the listener, and `disconnect()` and `drain()` no longer wait for one
  still running: once they have closed the connection, what it calls fails with `Connection is not open`. An
  operation the listener calls that finds the new connection gone reconnects again: the listener hears
  `Disconnected` and `Reconnected` (or `Closed`) while that call is still running, the `Reconnected`
  delivered from the event loop, so handlers can get the new connection's messages first. Do not hold a lock
  across an await on the same connection in a listener: a call nested in it may need that lock.
- A `drain()` that waited for a reconnect can therefore close the connection before a `publish()` whose
  failed write ran that reconnect is retried, while the listener, or a logger that suspends as it records
  the `Reconnected`, is still busy. That publish then fails with the error of its write and is not sent,
  where the retry used to come first; a built-in transport's raw stream error is wrapped in a
  `TransportClosedException`.
- The listener hears of a connection only while it is still the open one, so a `Reconnected` call always
  finds the connection Open. One closed, being closed, replaced or lost before the listener was told it
  opened is not announced, and when it was lost, neither is its loss, though a `Closed` still is. The logger
  still records every transition.
- With reconnect off, the `Closed` event of a lost connection now carries the error that ended it and is
  logged at warning level instead of info, so a failed read is logged twice: as the read's error and with
  the `Closed` event.
- A service's `run()` now reports what its reads used to swallow, to the error listener and at error level:
  another subscription's handler that throws, once per failure (a handler failing on every message is logged
  for every message), and an `-ERR` the server keeps the connection open for. A handler's
  `CancelledException` no longer stops the service: only `run()`'s own timeout or cancellation does, or a
  connection closed for good. A handler awaiting the cancellation passed to `run()` reports its
  `CancelledException` when that cancellation stops the service.
- `NatsHeaders::fromWireBlock()` and `fromWireBlockMulti()` declare their keys as `int|string`, since a header
  name that is a decimal integer, such as `1`, comes back as an int key. Static analysis now reports code
  that hands such a key to a function taking a string, and code that passes the whole map where string keys
  are declared, such as Symfony Messenger's `SerializerInterface::decode()`: cast such keys to string there.
  `NatsHeaders::get()` takes the map as it is.
- A first `INFO` that is valid JSON but not an object (`INFO 1`, `INFO [1]`) fails the connect as broken JSON
  does, saying `INFO payload is not a JSON object`.

### Fixed

- `[bugfix]` Since 2.10.1 an `-ERR` the server keeps the connection open for ended the connection, unless the
  client only reports it, as it does `Invalid Subject`, `Permissions Violation for Publish to ...` and `...
  for Subscription to ...`. So `maximum subscriptions exceeded`, `Invalid Publish Subject` and any other
  `Permissions Violation`, such as `... for Publish with Reply of ...`, closed a healthy connection, or with
  reconnect on reconnected it; after a rejected SUB, every reconnect attempt replayed that SUB and failed
  again, until the reconnect gave up and closed the connection. Such an `-ERR` fails the read that brought
  it, as before 2.10.1, and leaves the connection open (when an account's subscription limit is lowered
  below what a client holds, the server closes the connection after `maximum subscriptions exceeded` anyway,
  and the EOF ends it). Only an `-ERR` the server closes the connection after (`Stale Connection`,
  `Authorization Violation`, `Maximum Payload Violation`, ...) and a `PONG` the socket would not take end the
  connection, and nothing else a frame raises does. Such a failure also outranks any other met in the same
  read, so a fatal `-ERR` read together with a rejected SUB's still ends the connection.
- `[bugfix]` A reconnect whose replayed SUB the server rejected for exceeding the maximum subscriptions
  failed, and so did every attempt after it, until the reconnect gave up and closed the connection. Any
  failure of the replay that does not end the connection is now reported to the error listener instead, and
  the reconnect completes; one that ends it still fails the attempt. A rejection read ahead of a line that
  does not parse is reported as well, though that attempt fails on the parse error. A rejection that arrives
  after the replay's short poll fails the read that brings it instead, and the connection stays open.
- `[bugfix]` The flushes of `drain()` and `drainSubscription()` ended at an `-ERR` the server keeps the
  connection open for, and the messages the server sent behind it, up to the `PONG`, were lost without a
  count: `drain()` closed the socket with them unread and reported only the `-ERR`, and `drainSubscription()`
  removed the subscription, so a later read dropped them as an unknown sid's. A long-standing bug, in 2.10.0
  as well; on 2.10.1 and 2.10.2 `drainSubscription()` also ended the connection, since such an `-ERR` did
  there. Both flushes now report such an `-ERR` to the error listener and read on to their `PONG` within the
  same budget, also when a line that does not parse follows the `-ERR` in the same read. A fatal `-ERR` and a
  `PONG` the socket would not take still end the flush at once, and `flush()` and `rtt()` still fail with
  such an `-ERR`.
- `[bugfix]` A `drain()` waited out its whole budget (`requestTimeoutMs`, 10 s by default) against a healthy
  server while another fiber read the connection, such as an application's `processIncoming()` loop or a
  service's `run()`: that fiber's read took the `PONG` of the drain's flush before the flush first read, and
  the flush then waited for that fiber's next read, on a socket with nothing more to come. Every flush - that
  of `drain()`, `flush()`, `rtt()`, `drainSubscription()` or a service's `drain()` - also had a narrower
  window: another fiber's read could take the `PONG` just before the flush's own read started, which then
  waited on the idle socket until the budget ran out, and the call returned only then, `rtt()` reporting that
  wait as the round trip. A `drain()` issued right after a `request()`, in the same tick, met it against a
  server that answered at once. A flush now looks for its `PONG` before every read, and its read looks again
  before it takes the socket.
- `[bugfix]` The shared reply inbox of `request()` and `requestMany()` was recorded only once the write of
  its SUB had returned. When that write found the socket dead, the reconnect it started replayed the SUB,
  and a permissions rejection read during that replay missed the latch of 2.7.1 (#167): the request and
  every later one waited out their timeouts instead of failing at once with the permissions error. And when
  a terminal close came while the write was under way and the write completed all the same (a narrow race,
  since the shipped transports fail such a write when the socket closes), the closed connection's inbox
  stayed recorded: after a new `connect()` every request was sent with a reply subject nobody held there
  and timed out. The inbox is now recorded before its SUB is written, so whichever read meets the server's
  answer finds it. A request joins a set-up still under way instead of trusting the recorded inbox, and a
  set-up whose write fails without the connection coming back in time is rolled back. A terminal close
  during the set-up fails the request without sending it (with `Connection was closed while the reply inbox
  was being set up` when the write completed anyway), and a request after a new `connect()` neither waits
  for a set-up left from the closed connection nor reuses its inbox.
- `[bugfix]` The shared reply inbox of `request()` and `requestMany()` stayed dead once the server rejected
  its SUB with `maximum subscriptions exceeded`, an `-ERR` that names no subject and leaves the connection
  open. In 2.10.0 the request that met the rejection failed with it, and every later one was sent with a
  reply subject nobody held and waited out its whole timeout (`requestMany()` returned nothing), even after
  a slot freed up, until the connection closed for good. 2.10.1 and 2.10.2 ended the connection on that
  `-ERR` instead, and with reconnect on every attempt replayed the inbox's SUB into the same limit until the
  reconnect gave up. The SUB now goes out with a PING right behind it, and the inbox counts as confirmed
  once that PING's PONG arrives, which the server sends only after its answer to the SUB, or once a reply
  arrives on it. That `-ERR` arriving before then drops the inbox: the requests waiting on it fail at once,
  a `requestMany()` that has collected replies returns them, and the next request subscribes a new inbox,
  in the same write as an UNSUB of the dropped one. Until a new inbox is confirmed, a request waits for that
  before it is sent, so while the limit holds requests fail fast without reaching the responder, and once a
  slot is free they work again, with no reconnect. A reconnect whose replay of the inbox is rejected works
  the same way, and a confirmed inbox stays in place when another subscription is rejected: the requests in
  flight on it are not failed with the dropped-inbox error, though one whose own read brings that `-ERR`
  fails with it, as any read does. Another subscription's `-ERR` that arrives while the inbox's SUB still
  waits behind earlier writes, or before its PONG, drops an inbox the server takes after all; the next
  request's UNSUB gives its slot back.
- `[bugfix]` A request whose reply inbox was still being subscribed when a `drain()` began was sent all the
  same, after the drain's UNSUB of that inbox: the responder ran it, though no reply could come back, and the
  request failed with `Connection is not open` once the drain closed the connection. A `drain()` that begins
  while a request subscribes the inbox, or waits for the server to take it, now fails the request with
  `Connection is not open` without sending it, as one issued during the drain does.
- `[bugfix]` An async `INFO` whose payload is valid JSON but not an object was not treated as malformed.
  `INFO 1` failed the read with a `TypeError`, which 2.10.1 and 2.10.2 also took for a connection failure
  and ended the connection with, and `INFO [1]` replaced the server info with defaults (no server id, no
  headers support), after which every publish with headers was refused. Such an `INFO` is now reported to
  the error listener as malformed, as broken JSON is, and the last server info is kept. A first `INFO` like
  that fails the connect as broken JSON does, saying `INFO payload is not a JSON object`, where a number
  failed it with a `ConnectionException` wrapping a `TypeError` and an array connected with the default info.
- `[bugfix]` An operation called from a `Reconnected` listener - or from the `Connected` listener of a failed
  initial connect that a reconnect completed - that found the new connection gone already joined the
  reconnect that had announced it, which was still waiting for the listener to return. A read meeting EOF or
  a fatal `-ERR`, a `flush()` or a `subscribe()` waited out its timeout; a `publish()`, a `request()`, a
  lame-duck failover and a read without a cancellation never returned. The connection then stayed Open on the
  dead socket, and a failure another fiber or the heartbeat noticed while the listener ran was lost the same
  way, the heartbeat stopping for good. The new connection is now announced once the reconnect is over, as a
  direct connect already was: such an operation reconnects again, as it would anywhere else, and an
  announcement made while another `Connected` or `Reconnected` listener call runs is delivered from the
  event loop, so that the calls do not nest one level deeper with every connection a server drops. A
  `publish()` whose failed write ran the reconnect is retried by the state it then finds: buffered behind a
  reconnect or lame-duck failover the listener left in flight (it used to fail, its frame lost, or go to the
  connection the server was about to close); written to the connection a `drain()` is still flushing; or,
  when the connection was closed meanwhile, failing with the error of its write, a built-in transport's raw
  stream error wrapped in a `TransportClosedException` ("Transport is not connected").
- `[bugfix]` A `drain()` that waited for a reconnect also waited for a slow `Reconnected` listener, past its
  budget. A supervisor that reconnects on the `Closed` of a `drain()` called from that listener was refused
  with `Recovery was aborted before the connection opened`, the reconnect still waiting for the listener; it
  now opens a new connection, as documented. And a close the listener makes takes over what the reconnect
  read, even while the close is still under way as the listener returns: `drain()` delivers it and
  `disconnect()` discards it, where it used to be delivered during the close. A read that receives anything
  during the close still delivers it with what it received, as any read does.
- `[bugfix]` A `connect()` that joined a failed first dial waited for the `Connected` listener of the
  reconnect that completed the dial, so a listener waiting for that `connect()` waited out its own timeout;
  it now resumes when the listener starts, as on a direct connect. When that listener lets the new
  connection die and returns with the next reconnect in flight, the `connect()` that dialled now fails with
  `Connect was aborted before the connection opened` while those that joined it succeed, as on a direct
  connect, where both used to report success on the dead socket.
- `[bugfix]` With reconnect off, a connection the heartbeat gave up on (unanswered PINGs, a failed PING
  write, the socket closing or breaking during its read) closed without saying why: `Reconnect is disabled`
  and its cause (#172) reached only an operation that joined the recovery. The `Closed` event now carries
  the cause, so that the connection listener and the log learn it. The reason for unanswered PINGs reads
  "the last PING" when `maxPingsOut` is 1, and says that the heartbeat allows no unanswered PING when it is 0.
- `[bugfix]` A service's `run()` swallowed the exception of a handler that threw while its loop read, another
  subscription's on the same connection: nothing reported it, the loop backed off 20 ms, and the messages that
  read brought behind the failing one, for any subscription, stayed queued until the server sent something
  else. A request read behind it waited up to the heartbeat interval, and was lost when the service stopped
  first, and a subscription failing on two or more messages per read starved the service's endpoints for
  good. A handler that threw a `CancelledException` stopped the service, silently. Such a failure is now
  reported to the error listener, the rest of the read is delivered, and the loop reads on without backing
  off, as `drain()` treats a throwing handler. An `-ERR` the server keeps the connection open for is reported
  the same way, also one read ahead of a line that does not parse; the loop used to swallow it too. The
  loop's read also delivers what another read left queued - another fiber's that stopped at a throwing
  handler, or the delivery after a reconnect - instead of leaving it for the server's next bytes, except
  while a `disconnect()` is closing the connection, which discards it. A read that receives anything during
  the close, the loop's or any other, still delivers it with what it received. A known limitation remains:
  the read of an operation, such as a `request()` an endpoint handler makes, still fails with another
  subscription's handler exception, and the endpoint then answers its requester with a `HANDLER_ERROR` reply.
- `[bugfix]` A service's `drain()` swallowed what its flush met, such as another subscription's handler that
  threw, and returned before the `PONG` that confirms the server has processed the `UNSUB`s. The flush now
  reports it to the error listener and reads on to that `PONG`.
- `[bugfix]` A service endpoint's request validator that threw escaped the endpoint into the read that
  delivered the request: the requester got no reply, the endpoint counted no error, and the read failed with
  the exception, which a service's `run()` swallowed. Such a validator is now answered like a handler that
  throws: a `HANDLER_ERROR` reply without the exception's text, an error counted with the text as
  `last_error`, and `request_error` and `request_end` for the observers. A `ServiceError` it throws is sent as
  chosen, as a handler's is.
- `[bugfix]` A request header whose name is a decimal integer, such as `1`, which any requester can send,
  made a service endpoint throw a `TypeError` into the read that delivered the request, and the request went
  unanswered, whenever the endpoint had an observer or its handler threw: `NatsHeaders::fromWireBlock()`
  returns such a name as an int key, as PHP stores such array keys, and the endpoint lowercased it under
  strict types. The endpoint now answers such a request like any other. `fromWireBlock()` and
  `fromWireBlockMulti()` now declare their keys as `int|string`, so static analysis reports such a call in
  your code too.

## [2.10.2] - 2026-10-04

### Upgrade notes

- With reconnect off, `Reconnect is disabled` now carries the error that ended the connection as its
  previous exception (`getPrevious()`). Its message and code are unchanged.

### Fixed

- `[bugfix]` With reconnect off, `Reconnect is disabled` did not say why the connection ended (#172), so
  the operator saw what reads like a configuration problem and had nothing to go on. The error that ended
  the connection is now chained as its previous exception (`getPrevious()`): the socket error of a failed
  read or write, the `ProtocolException` of a stream that could not be parsed, a server `-ERR` such as
  `Stale Connection`, or why the heartbeat gave up (`The server did not answer the last 2 PINGs`, its PING
  write failing, or the socket closing or breaking during its read). That holds for the operation that
  failed and for any operation that joined the recovery. The message is unchanged, so code that matches it
  keeps working.

## [2.10.1] - 2026-10-03

### Upgrade notes

- After a fatal `-ERR` from the server, the connection is no longer left Open: with reconnect off it is
  Closed by the time the read that met the `-ERR` throws, and with reconnect on that read first waits for
  the reconnect within its own timeout (a `processIncoming()` without a cancellation waits for the whole
  reconnect, as it already did after a failed read). The read still throws the same
  `ConnectionException` (`Server sent error frame: ...`). The same holds for a server PING whose PONG the
  socket would not take; that read still throws the socket's own error.

### Fixed

- `[bugfix]` A fatal `-ERR` from the server - the error it sends right before it closes the connection,
  such as `Stale Connection` for a client that stopped answering its pings - failed the operation that
  read it but left the connection Open (#171). The next operation then wrote into the socket the server had
  closed and failed as well: with reconnect off with `Reconnect is disabled`, with reconnect on only after
  waiting out its whole timeout, since its request had gone to the dead socket. A synchronous application
  that sat idle for a few minutes - between requests in a worker-mode runtime, say - met this on its next
  two calls. Such a frame, and a PONG the socket would not take, now ends the connection like a failed
  read: it reconnects, or with reconnect off closes for good, and the read that met it fails with the
  server's error once the connection is Closed or, with reconnect on, once the reconnect is done or the
  read's own timeout runs out (not waiting at all with `waitForReconnect: false`). The heartbeat's read
  does the same. A full subscription queue still does not end the connection.

## [2.10.0] - 2026-09-30

### Upgrade notes

- A `subscribe()`, `flush()` or `rtt()` whose write finds the socket dead no longer throws the socket's own
  error (for example `Amp\ByteStream\StreamException`): it starts the reconnect and waits for it within
  its own timeout, as it waits for a reconnect already in flight. `subscribe()` then returns; `flush()`
  and `rtt()` throw a `ConnectionException` (`Connection lost before the server answered the PING`). A wait
  that runs out throws a `TimeoutException`, and with reconnect off they throw a `ConnectionException`
  (`Reconnect is disabled`). Code that caught the transport's exception there should catch those instead.
- `unsubscribe()` no longer throws when its write finds the socket dead.
- `disconnect()` and `drain()` now return only once the reconnect or `connect()` they stopped has ended.
  With the built-in transports that is at once; a custom transport that implements only
  `TransportInterface` can hold them up to `connectTimeoutMs`. Implement `CancellableDialTransportInterface`
  to let a close stop its dial.

### Added

- `[feature]` `CancellableDialTransportInterface`, a transport whose dial can be stopped: its `connect()`
  takes a cancellation that fires when the application closes the connection while a connect or a
  reconnect is dialling. Both built-in transports implement it. A custom transport that implements only
  `TransportInterface` works as before, and a close waits for its dial to end, up to `connectTimeoutMs`.

### Fixed

- `[bugfix]` A `subscribe()`, `flush()` or `rtt()` whose write found the socket dead failed with the
  socket's own error (for example `Amp\ByteStream\StreamException` "Broken pipe") and left the connection
  Open on that socket, so every operation that wrote a control frame failed the same way until the
  heartbeat noticed. A JetStream fetch subscribes its inbox first: after a server restart the application
  had not seen yet, a fetch threw "Broken pipe", and a consumer built on it exited. Such a write is now a
  connection failure, as a failed publish write already was. The connection reconnects, and the operation
  waits for it within its own timeout, the way it waits for a reconnect already in flight; a
  `subscribe()` then runs on the new connection, while a `flush()` or `rtt()` fails anyway, with
  `Connection lost before the server answered the PING`, since what it was to confirm went to the dead
  connection. With reconnect off the connection closes for good and the operation fails with
  `Reconnect is disabled`; with `waitForReconnect: false` it fails at once with `Connection is not open`.
  `unsubscribe()` no longer throws on a dead socket either - the server dropped the subscription with the
  connection - as it already did not on a connection that is not open, and the next operation that needs
  the socket reconnects.

- `[bugfix]` `disconnect()` or `drain()` during a reconnect that was dialling left that reconnect running
  after they returned: they cut its backoff short but not its dial, which with Amp's retry pauses takes
  some 6 s against a refused port. A `connect()` issued after the close met the stopped reconnect still
  winding down and failed with `Recovery was aborted before the connection opened`. In a synchronous
  application the reconnect never got the event-loop time to end, so every later `connect()` failed that
  way and the connection could not be reopened until the process restarted (found while reviewing
  symfony-nats-messenger PR #45, where a transport's `close()` then broke every later dispatch). A close
  now stops the dial - Amp's retry pauses included, for a transport that implements
  `CancellableDialTransportInterface` - and waits, bounded by `connectTimeoutMs`, for the reconnect or
  `connect()` it stopped to end before it returns, so a `connect()` issued after it dials afresh. A
  `connect()` racing the close still fails at once. A reconnect attempt that finds a close came while it
  was closing the previous socket no longer dials.
- `[bugfix]` A reconnect that a close stopped delivered the messages queued on the connection on its way
  out: after `disconnect()`, which discards them, or beside `drain()`'s own delivery and past the rules it
  keeps. It now leaves them to the close.

## [2.9.0] - 2026-09-30

### Upgrade notes

- An operation whose wait for a reconnect runs out (see `waitForReconnect` below) fails with a
  `TimeoutException`, for example `Subscribe to "orders" timed out waiting for the connection to be
  re-established`, where it used to fail at once with `Connection is not open`. `TimeoutException` is not
  a `ConnectionException`, so a `catch (ConnectionException)` around `subscribe()`, `flush()`, `rtt()`,
  `request()`, `requestWithHeaders()` or `requestMany()` no longer catches that case.
- `processIncoming()` and `readIncoming()` without a `Cancellation` now wait for a reconnect in flight to
  end, and for another fiber's socket read to end, instead of returning or failing at once. Pass a
  `Cancellation` to bound them.
- `SubscriptionQueue::fetch()`, and `next()` without a timeout, return `null` during a reconnect instead of
  throwing `Connection is not open`; `next()` and `fetchAll()` with a timeout wait for the reconnect within
  it. A reconnect that gives up still throws its error.
- `drain()` now emits `ConnectionEvent::Closed`, so a listener that reconnects on every `Closed` now also
  reconnects after a graceful `drain()`.
- Under `SlowConsumerPolicy::Error`, an overflow that an operation's read runs into is reported through
  the error listener, logged at error level, instead of failing that operation. With no `errorListener`
  and no logger it is visible nowhere: register an `errorListener`, or set
  `slowConsumerErrorsFailOperations: true` to have operations fail as before.

### Added

- `[feature]` `NatsOptions::$waitForReconnect` (default `true`). While a reconnect is in flight,
  `request()`, `requestWithHeaders()`, `requestMany()`, `subscribe()`, `flush()`, `rtt()`, `drain()`,
  `drainSubscription()` and the reads - `processIncoming()` / `readIncoming()` and `SubscriptionQueue`
  polling - wait for it instead of failing at once with `Connection is not open`, then run on the new
  connection. An operation waits within its own timeout; a read within the `Cancellation` passed to it,
  and without one until the reconnect ends. A reconnect that gives up surfaces its own error (e.g.
  `Reconnect attempts exhausted`). Operations still fail at once when no reconnect is in flight, and
  when called from a connection/error listener while the reconnect is in flight (it runs inside the
  reconnect, which waits for it). Set `false` to fail
  fast again, except that `drain()` then closes the connection and throws, and `drainSubscription()`
  delivers what already arrived and removes the subscription.
- `[feature]` `SlowConsumerException`, thrown under `SlowConsumerPolicy::Error` when a message arrives
  for a subscription whose queue is full, carries the subscription's `sid`. It extends
  `ConnectionException`, which the overflow was thrown as before, with the same message, so existing
  handlers still catch it; a caller can now tell a subscriber that fell behind from a failed connection.
- `[feature]` `NatsOptions::$slowConsumerErrorsFailOperations` (default `false`): set `true` to keep the
  behavior from before the change below, where under `SlowConsumerPolicy::Error` an overflow of any
  subscription fails whichever operation's read ran into it. An overflow of a `SubscriptionQueue`'s
  polling buffer that is thrown is then also reported, as it was before. `Service::run()` reports
  overflows either way.

### Changed

- `[bugfix]` Under `SlowConsumerPolicy::Error`, an operation that reads the socket while it waits for a
  result of its own no longer fails with another subscription's overflow. It used to throw the overflow
  (a `ConnectionException`) of whichever subscription's queue its read happened to overflow: a `request()`
  failed although its reply had arrived (a retry could repeat the work, a JetStream publish could be
  stored twice), a fetch discarded the messages it had collected (redelivered late, or lost with
  `max_deliver=1`), and which operation failed was down to chance. Now the overflow is reported through
  the error listener and the operation completes. This covers `request()`, `requestMany()` and what is
  built on them (JetStream publish, `ackSync()`, stream and consumer management, Key/Value and Object
  Store calls), `flush()`, `rtt()`, `fetchBatch()`/`fetchNext()`, `directGetBatch()`, pull consumers
  (`consumePipelined()`), Key/Value `keys()` and `history()`, and `SubscriptionQueue` polling;
  `Service::run()`, which used to swallow the overflow without reporting it, now reports it. An overflow
  of the operation's own subscription (a `SubscriptionQueue`'s) still fails it, once the rest of the read
  is delivered, and `processIncoming()` / `readIncoming()`, the reads an application makes itself, still
  throw. `slowConsumerErrorsFailOperations` restores the old behavior. A reported overflow is logged at
  error level. The README's new Slow Consumers section describes the whole behavior.
- `[bugfix]` The same now holds for a `SubscriptionQueue`'s own polling buffer, which fills when the
  application does not poll the queue: under `SlowConsumerPolicy::Error` its overflow is a
  `SlowConsumerException` naming the queue's subscription - it was a plain `NatsException`, so a
  `catch (ConnectionException)` now catches it too - thrown to the application's own read and reported
  when another operation or a background read runs into it. It used to be thrown from whichever read
  delivered the message, so a `request()` failed although its reply had arrived, and it cut that
  delivery short: the messages behind it, for other subscriptions too - an operation's reply among them -
  waited for the next read, and `drain()` discarded them at its deadline. They are now delivered in the
  same pass. It used to be reported as well as thrown, logged at debug level; now each overflow reaches
  the application once, and a reported one is logged at error level like any other overflow under
  `Error` (with `slowConsumerErrorsFailOperations`, one that is thrown is still reported as well).
- `[bugfix]` `drain()` now emits `ConnectionEvent::Closed` when it closes the connection - once, after
  the drain is over, so a `Closed` listener can `connect()` again straight away. It used to close the
  connection without any event; nats.go likewise calls its closed handler after `Drain()`. When
  something else closed the connection during the drain - a reconnect that gave up, or `disconnect()` -
  that path's `Closed` is the only one. A listener that reconnects on every `Closed` now also does so
  after `drain()`, as it already did after `disconnect()`. Every close is now announced once:
  `disconnect()` of a connection already closed and announced (after `drain()`, after a reconnect that
  gave up, a second `disconnect()`) closes it again without a second `Closed` event. The same holds when
  a `disconnect()` - or a `drain()` running out of time - comes while a connect or reconnect that gave up
  is still closing the socket (a TLS or WebSocket close takes a while): that path's `Closed`, with its
  error, is the only one. A `disconnect()` there used to announce the close a second time.

### Fixed

- `[bugfix]` A synchronous application could leave a reconnect stalled until the process restarted.
  A reconnect only advances while something waits on the event loop, and awaiting an operation that
  fails at once never lets the loop run its dials and backoff timers. So when the heartbeat started a
  reconnect in the background (a hung server, a dropped connection noticed between operations), a
  queue worker or daemon whose operations all failed that way stayed `Connecting` and failed every
  operation from then on. With `waitForReconnect` the first operation waits and so drives the
  reconnect. A publish buffered during a reconnect now also yields one event-loop tick, so a process that
  only publishes moves the reconnect along, one tick per publish.
- `[bugfix]` A request, pull fetch or `processIncoming()` whose socket read failed - or brought a corrupt
  chunk - while another fiber (typically the heartbeat) was already reconnecting waited for the whole
  reconnect, ignoring its own timeout - up to the full backoff schedule during a long outage. It now
  gives up at its own deadline while the reconnect carries on.
- `[bugfix]` One timeout now covers a whole request: the request timeout (and `requestMany()`'s total
  timeout) starts when the request is issued, so the wait for a reconnect, the reply-inbox set-up and the
  publish all count against it, and the reply wait gets what is left. It used to start only after the
  publish. A request with a
  non-positive timeout is now rejected before anything is published.
- `[bugfix]` `flush()` and `rtt()` honor the single `requestTimeoutMs` budget they document: the PING
  write and the PONG wait used to get a full budget each, so a backpressured flush could take twice
  as long. `rtt()` does not count a wait for a reconnect as round-trip time.
- `[bugfix]` `drain()` during a reconnect threw `Connection is not open` without closing anything, so
  the reconnect could reopen - with every subscription replayed - the connection the application was
  shutting down, and the publishes buffered during the outage were lost when it fell back to
  `disconnect()`. `drain()` now waits for the reconnect within its budget, so those publishes are
  flushed before the drain. If the budget runs out it stops the reconnect at once, bounds its backlog
  pass by the same budget, and reports the buffered publishes it discards through the error listener;
  `connect()` is refused (`Cannot connect: drain in progress`) until the drain is over, because the
  drain's teardown would close a connection opened meanwhile. When it cannot wait (`waitForReconnect`
  disabled, or called from a connection/error listener) it closes the connection, reports what it
  discards - the buffered publishes and the messages received but not yet delivered - and throws
  `Cannot drain while reconnecting: the connection was closed instead`, matching nats.go's `Drain()`.
- `[bugfix]` `drainSubscription()` could leave the subscription alive on the server. When the
  connection was lost during its flush, the reconnect re-subscribed the sid and `drainSubscription()`
  then removed it only locally: the server kept delivering to a sid nothing handles, and for a queue
  group that member silently took its share of the group's messages. A subscription being drained is
  no longer re-subscribed.
- `[bugfix]` A subscription removed while a reconnect was between re-subscribing and going live -
  `unsubscribe()` and `drainSubscription()` can then only remove it locally - stayed subscribed on the
  server, with the same effect. It is now unsubscribed on the new connection before that goes live, and
  so is an auto-unsubscribe max armed in that window, which used to leave the server delivering past
  the max.
- `[bugfix]` `drainSubscription()` while reconnecting, or during `drain()`, dropped the messages already
  received for the subscription without delivering or reporting them. While reconnecting it now waits for
  the reconnect within its budget, like `flush()`, and drains on the new connection - so it also delivers
  what the server sends on a subscription the reconnect had already re-subscribed before its UNSUB lands;
  when it cannot wait (`waitForReconnect` disabled, a listener inside the reconnect, the budget spent) it
  delivers what arrived. During a `drain()` it leaves the subscription to the drain, whose flush may still
  bring messages for it: removing it at once dropped those. Its UNSUB write is bounded by its budget like
  `drain()`'s writes; a peer that stopped reading held it forever. A handler that throws during it is
  reported through the error listener and delivery goes on, like in `drain()`: a handler failing inside
  its flush was swallowed without a trace,
  and one failing in its final delivery made it throw and keep the subscription. When a delivery for the
  subscription is already under way - a handler draining its own subscription included - that delivery
  now hands over the messages queued behind it and then removes the subscription; the handler that
  drained itself used to lose them. An UNSUB that fails because the connection just dropped no longer
  makes it throw and keep the subscription: it still delivers and removes it. A second call for a sid
  that is already being drained now resolves at once, instead of sending a second UNSUB.
- `[bugfix]` `disconnect()` during a reconnect let the reconnect run on until its current backoff delay
  ended, so a `connect()` right after the close joined it and failed with `Recovery was aborted before
  the connection opened`, and a close during the backoff after the last attempt was reported as
  `Reconnect attempts exhausted` with a second `Closed` event. Closing now cuts the backoff short, so a
  reconnect waiting between attempts stops at once (an attempt already dialling finishes first), and it
  stops without reporting exhaustion - also when its attempt then fails authentication, which used to
  announce the close a second time. A `connect()` issued while such a reconnect is still winding down
  now fails at once instead of waiting for it: it could never open the connection, and when the
  `connect()` came from a `Closed` listener that the reconnect itself was waiting on, the wait never
  ended.
- `[bugfix]` `disconnect()` while `connect()` was still dialling, or retrying a failed first dial
  (`retryOnFailedInitialConnect`), was overridden: the dial went `Open` behind the announced close, with
  close intent still set, so nothing would recover that connection later. `connect()` now fails with
  `Connect was aborted before the connection opened`, and the retries' backoff is cut short. A
  `disconnect()` during the last retry, whose dial then failed too, no longer makes `connect()` report
  that dial's error and announce the close a second time.
- `[bugfix]` A read, write or heartbeat that failed on a connection the application had since closed and
  reopened - for example from an error listener that reconnects - started a reconnect of the new,
  healthy connection. Failures are now tied to the connection they happened on.
- `[bugfix]` `drain()` whose connection died while it waited for its flush `PONG` read the dead socket
  until its budget ran out; it now ends the flush at once. A server `PING` answered during a drain is
  bounded by the drain budget like the drain's own writes, so a stalled socket cannot hold it past it.
- `[bugfix]` A publish issued while `disconnect()` was closing the connection during a reconnect was
  accepted into the reconnect buffer and reported success, then discarded silently. Publishes are
  refused once a close is under way.
- `[bugfix]` A reconnect the server refused to authenticate discarded the publishes buffered during the
  outage without reporting them. It now reports them as an exhausted reconnect does (`Reconnect failed
  authentication: N bytes of buffered publishes were discarded`).
- `[bugfix]` A logger that threw while an exhausted reconnect reported the publishes it discarded
  aborted the cleanup: the connection kept its subscriptions and no `Closed` event was emitted. A
  logger that threw on a reconnect's "attempt failed" line ended the reconnect, leaving the connection
  `Connecting` for good, and one that threw while a lifecycle event was logged kept that event from the
  connection listener. A logger that threw on a failed read skipped the reconnect: the connection stayed
  `Open` on a dead socket and every later read failed with the logger's exception. And one that threw on
  an error the heartbeat's own read picked up (a fatal `-ERR`, a corrupt stream) escaped into the event
  loop. All of these are now contained.
- `[bugfix]` Under `SlowConsumerPolicy::Error`, a subscriber that could not keep up made the connection
  close for good on its next reconnect. With its queue full - nothing delivers while a reconnect runs -
  the first message the server sent after the reconnect re-subscribed it overflowed the queue, and the
  overflow failed the attempt like a fatal server error; every attempt failed the same way until the
  reconnect gave up, closed the connection and discarded the queued messages. The overflow now drops
  that message and is reported through the error listener, as the policy says, and the reconnect carries
  on; a fatal `-ERR` in the same read still fails the attempt. An overflow among the frames read just
  before a corrupt one was not reported at all; it is now.
- `[bugfix]` The flushes of `drain()` and `drainSubscription()` ended early when one of their reads
  overflowed a subscription's queue under `SlowConsumerPolicy::Error`, or when a handler threw while they
  delivered, and the messages still in flight were then lost without a trace: `drain()` closed the socket
  on them, and for `drainSubscription()` they arrived after the subscription was removed. The overflow or
  the handler's failure is reported through the error listener and the flush reads on to its `PONG`.
- `[bugfix]` Under `SlowConsumerPolicy::Error`, the application's own read that met a subscription's
  overflow and then a fatal `-ERR` threw the overflow and only reported the `-ERR`, so the caller took a
  failed connection for a subscriber that fell behind. The `-ERR` is now thrown and the overflow reported.
- `[bugfix]` Under `SlowConsumerPolicy::Error`, a read that ended in a corrupt frame could lose an
  overflow: a second overflow in that read replaced the first, which was then neither thrown nor
  reported - as did the failure of a connection that could not recover from that frame. Each overflow now
  reaches the application once.
- `[bugfix]` Under `SlowConsumerPolicy::Error`, an error listener that reacted to an overflow by reading on
  the connection - `drainSubscription($error->sid)`, say - could have messages delivered out of order: the
  overflow was reported before the rest of its read was queued, and the listener's read delivered the
  next read's messages ahead of it. An overflow is now reported once its read is queued.
- `[bugfix]` A handler that threw while the heartbeat's own read delivered was swallowed without a trace,
  and the messages behind it waited for the next read. A handler that threw while a reconnect's handshake
  delivered (the server had sent a frame right behind its `PONG`) failed that reconnect attempt. Both are
  now reported through the error listener, and delivery goes on.
- `[bugfix]` `SubscriptionQueue::fetchAll()` that failed - on an overflow of its own subscription under
  `SlowConsumerPolicy::Error`, say - lost the messages it had already taken from the queue's buffer:
  neither returned nor counted. They are now put back, in order, for the next call.
- `[bugfix]` `subscribeQueue()` threw when the messages that arrived before its queue existed did not fit
  in it under `SlowConsumerPolicy::Error`: the rest of those messages were lost, and the subscription stayed
  registered, feeding a queue nobody held. The overflow is now counted and reported, and the queue is
  returned.
- `[bugfix]` Key/Value `keys()` and `history()` could return a cut-short list as if it were complete (under
  the default `DropOldest`), or fail (under `Error`), when one read brought more records than
  `maxPendingMessagesPerSubscription` - possible once `readChunkSizeBytes` is raised. Their replay
  subscriptions are now exempt from that bound, like the JetStream fetch inboxes: the listing bounds
  them.
- `[bugfix]` Under `SlowConsumerPolicy::DropOldest` and `DropNewest`, a message the policy discarded was
  reported in the middle of the read that brought it, as were a recoverable `-ERR` and a malformed async
  `INFO`: an error listener that read on the connection then had the next read's messages delivered ahead
  of the rest of that one. They are now reported once the read has queued everything it read, like the
  `Error` overflows. A logger that threw on one of these reports failed the read - under `DropOldest` after
  the oldest message was gone but before the new one was queued, so that one was lost too - and so did one
  that threw on a `SubscriptionQueue`'s own drop report. A logger that throws on any report is now
  contained, and the error listener still gets the report: a handler's failure after a reconnect and a
  JetStream push consumer's status used to be kept from it.
- `[bugfix]` A `processIncoming()` or `readIncoming()` loop could freeze the process at 100% CPU. When
  another fiber held the socket read - a request waiting for its reply, or the heartbeat reading the
  answer to its `PING` (a handler that awaits something is enough to let it start) - the call returned `0`
  at once without handing the event loop control, so no timer or socket read ran again, that other read's
  included. It now waits for that read to finish, bounded by its cancellation, and then returns `0`. A
  loop over `SubscriptionQueue::fetch()`, or `next()` without a timeout, spun the same way and is fixed
  with it.
- `[bugfix]` A request made by a handler of a message that arrived during a reconnect timed out, and
  `drain()` called from a `Reconnected` listener waited out its whole budget before closing. The read whose
  failure started the reconnect held the socket read until the reconnect was over, so nothing called from
  inside the reconnect could read. That read now lets go of the socket before it reconnects.
- `[bugfix]` A reconnect with a large `maxReconnectAttempts` crashed after about 60 attempts instead of
  backing off at `reconnectMaxDelayMs`: `reconnectDelayMs * 2^(attempt - 1)` left the int range, and
  casting the out-of-range float gave a garbage, possibly negative, delay that `delay()` rejected
  ("Delay must be greater than or equal to zero"; PHP 8.5 also warns that the float is not
  representable as an int). The delay is now capped before the cast.

## [2.8.0] - 2026-08-08

### Added

- `[feature]` `JetStreamContext::stopOrderedConsumer(int $sid): Future` stops an ordered consumer,
  or a KV / Object Store watch, even after automatic recreates rotated its internal subscription id,
  and deletes the server-side ephemeral consumer instead of waiting for it to expire. A plain
  `unsubscribe($sid)` only ever worked until the first recreate, so this is now the documented way
  to stop any of them.
- `[feature]` `KeyValueBucket::bind(): Future` resolves a mirrored bucket's read and write prefixes
  from `STREAM.INFO`. It is required on any handle that did not itself run `create()` (including a
  fresh `keyValue()` handle in the same process) before reads and write-through work correctly.
- `[feature]` `NatsHeaders::get(array $headers, string $name): ?string` looks a header up
  case-insensitively, preferring an exact-case match. Publishers differ in how they canonicalize
  header names (nats.go canonicalizes on read), so an exact-case array lookup could silently miss.
- `[feature]` `ObjectStoreBucket::watch()` gained an `exactName` parameter for watching an object
  whose own name contains `*` or `>`, and `ObjectStoreWatchOptions` gained an `idleHeartbeat`
  argument to tune the watch's heartbeat interval.
- `[feature]` `JetStreamContext::subscribeOrderedConsumer()` gained `consumerOverrides` (extra
  consumer configuration merged into the created instance) and `onConsumerCreated` (invoked once
  with the initial instance's `ConsumerInfo`, for example to read `num_pending`).

### Deprecated

- `[docs]` `WebSocketFrameCodec::unmask()` is deprecated. Masked server-to-client frames are now a
  terminal RFC 6455 violation, so the helper has no production caller left; harnesses that decode
  client-written frames should use `decode(..., allowMasked: true)`. It still works and is still
  covered by tests, and will be removed no earlier than the next major release.

### Fixed

- `[bugfix]` Ordered consumers (and the KV/OS watches riding on them): the disconnect-collision
  deferral introduced in this release cycle now clears the heartbeat watchdog's miss latch before
  returning. Without it, a WATCHDOG-triggered recreate (silent/reaped consumer) whose attempts all
  burned during a reconnect window left the latch set forever - no frame can arrive on the old
  inbox to clear it (the consumer was already dead and the recreate deleted it) - so every
  post-reconnect watchdog tick early-returned and the watch stalled permanently with zero signal.
  The watchdog now genuinely re-fires two idle intervals after the connection is Open again, as
  the deferral always promised. The deferral additionally rewinds the adopt-before-await dispatch
  state (consumer name, deliver inbox, expected sequence) to the pre-episode instance - when the
  episode's initial delete never took effect server-side, the surviving old consumer's
  post-reconnect frames pass the name filter and resume in-order delivery immediately instead of
  silently feeding the watchdog until the next idle heartbeat. The rewind runs ONLY when the
  episode delivered nothing: a candidate whose create succeeded with a lost reply may already
  have replayed frames to the handler, and rewinding then would re-admit the survivor's copies of
  stream sequences the handler already saw (duplicate ordered deliveries) - such an episode keeps
  the adopted state and falls back to the exactly-once filtered-until-heartbeat recovery. And the
  deferral decision is
  LATCHED, not sampled: any attempt observing the connection away from Open marks the episode as a
  disconnect collision, so a reconnect completing during the orphan-reap awaits can no longer flip
  it into a spurious terminal "recreate failed" teardown.

- `[bugfix]` WebSocket transport hardening to RFC 6455/7692 strictness: the handshake now requires
  `Upgrade: websocket` / a `Connection` token list containing `Upgrade`, rejects extension
  responses that were never offered (an unsolicited `permessage-deflate` used to silently flip
  compression ON, deflating the CONNECT into a server that never negotiated it), and validates
  permessage-deflate parameters against the offered no-context-takeover pair - accepting a
  server-volunteered `server_max_window_bits` of 8..15 per RFC 7692 7.1.2.1 (in the token or
  quoted-string spelling, RFC 6455 9.1), since the 15-bit raw inflater decodes any smaller-window
  stream; the value-less spelling, out-of-range values, and leading zeroes are rejected per the
  section 7 grammar. Frame-level: masked
  server-to-client frames, fragmented/oversized control frames (a fragmented PING used to splice
  its continuation into an in-progress data message - silent payload corruption), and RSV1 without
  negotiated compression are now terminal protocol violations - all deferred via the #115 pattern,
  so data frames decoded from the same read are delivered BEFORE the violation surfaces (an
  oversized declared length, corrupt deflate payload, or fragment-bound overflow previously threw
  mid-batch and discarded them). The server must also echo `server_no_context_takeover` when
  accepting compression (RFC 7692 7.1.1.1) - this per-message-inflate codec cannot decode context
  takeover - and a one-shot deferred violation surfacing on the heartbeat timer's read now
  recovers the connection instead of being silently swallowed, reported through the guarded #150
  emit path so a throwing user-supplied PSR-3 logger can neither skip the recovery nor escape into
  the event-loop timer.
  Note the contract change on the public `WebSocketFrameCodec::decode()`: it no longer THROWS on a
  strictness violation - it reports it via a new by-ref `$terminal` out-param and returns the valid
  frames parsed before it - and MASKED frames are rejected by default (RFC 6455 5.1 covers
  server-to-client frames); pass `allowMasked: true` to decode client-written frames (test
  harnesses, server-side use). `WebSocketFrameCodec::unmask()` is now `@deprecated` (production-dead
  under the masked-frame rejection; `decode(..., allowMasked: true)` serves the same audience) but
  retained - removing a public helper would be a bc-break.

- `[bugfix]` A permission-rejected pull reply inbox (`_INBOX.JS.PULL.<nuid>.*`) now fails
  `handle()` fast with a clear error naming the wildcard to grant, instead of polling forever with
  zero signal (every retire was a silent client-side deadline) - #167's fail-fast generalized via a
  per-sid subscription-rejection callback (`NatsClient::onSubscriptionRejected()`).

- `[bugfix]` Caller-owned push consumers now perform the ADR-9 heartbeat gap check: each idle
  heartbeat's `Nats-Last-Consumer` is compared against the locally delivered consumer sequence and
  a mismatch is surfaced once per gap episode via the error listener AND logger (nats.go
  ErrConsumerSequenceMismatch parity). Previously heartbeats kept the silence watchdog quiet while
  being withheld from the handler, so an ack-none / max_deliver=1 gap was permanent, invisible loss.
  The check also detects a server-side consumer REPLACEMENT (nats.go any-inequality parity): a
  heartbeat reporting `Nats-Last-Consumer` BELOW the session's tracked max surfaces one "consumer
  appears to have been replaced (sequence regressed)" error and rebases the tracker to the reported
  value, and a delivered consumer sequence below the tracked max rebases it silently - so gap
  detection follows the replacement instance instead of being masked by the stale high-water mark
  until it passed the old maximum.

- `[bugfix]` JetStream read resilience: `directGetLastForSubjects()` treats an all-miss `multi_last`
  chunk's lone 404 as "no matches" (ADR-31) instead of discarding every other chunk's results (the
  KV/OS batched enumerations could throw spuriously when keys were purged mid-enumeration), and the
  KV `getAll()` / ObjectStore `list()` per-subject lookups fall back to the leader STREAM.MSG.GET
  path on a Direct Get 503 (allow_direct-disabled interop buckets) exactly like `get()`/`info()`.
  The `list()` fallback queries the leader by the enumerated meta subject VERBATIM, so records
  stored under non-canonical (unpadded base64url) name tokens by other clients are not silently
  dropped from the fallback listing.

- `[bugfix]` Object Store links and watch patterns: `addLink()`/`addBucketLink()` refuse any name
  already held by a NON-LINK record - live or deleted tombstone alike (the exact nats.go
  `ErrObjectAlreadyExists` guard shape; overwriting a live object silently stranded its chunks
  forever, and allowing the tombstone diverged cross-client on shared buckets) - and `addLink()`
  rejects deleted and link-to-link targets (nats.go parity); `watch()` base64url-encodes an
  exact-name pattern into the meta-subject filter and rejects wildcard patterns loudly (they can
  never match encoded name tokens - previously a wildcard watch subscribed successfully and
  observed nothing), with a new `exactName: true` parameter to watch an object whose name itself
  contains `*` or `>` (the pattern is then always encoded).

- `[bugfix]` Headers interop: `NatsHeaders::toWireBlock()` emits values VERBATIM (the silent trim
  mutated signature/checksum-carrying values; nats.go writes caller bytes untouched - the decoder
  keeps its trim as an inbound tolerance), and the new case-insensitive `NatsHeaders::get()`
  accessor bridges lookups against non-canonicalizing publishers.

- `[bugfix]` Services/observability polish: endpoint names are validated against the ADR-32 token
  rules at registration (previously advertised verbatim to conformant tooling);
  `BasicJsonSchemaValidator` rejects non-empty JSON lists for `"type":"object"`; and JetStream
  client-level errors (`emitClientError()` - e.g. a terminally dead ordered consumer) now reach the
  PSR-3 logger as well as the error listener, so a logger-only configuration is no longer blind.

- `[bugfix]` KV and Object Store watches are now LOSSLESS: both are rebuilt on the ordered-consumer
  machinery (nats.go watcher parity), gaining consumer-sequence gap detection with automatic
  recreate-from-last-revision+1 (a slow-consumer drop or reconnect window is REPLAYED instead of
  becoming a silent permanent gap - ack-none watch deliveries are never redelivered on their own),
  flow control, and watchdog-driven recreation of a silent/reaped consumer (which previously
  surfaced an error at best - or, for the Object Store watch, hung forever with no signal: it never
  requested an idle heartbeat at all; it now defaults one like KV, tunable via
  `ObjectStoreWatchOptions::$idleHeartbeat`). A recreate before the first delivery re-applies the
  watch's initial deliver policy, so a `new`/`last_per_subject` watch never replays from sequence 1.
  `subscribeOrderedConsumer()` gained `consumerOverrides` and `onConsumerCreated` parameters, and
  the new `JetStreamContext::stopOrderedConsumer(int $sid)` stops an ordered consumer / watch even
  after recreates rotated its internal sid (a plain `unsubscribe()` only ever worked until the
  first rotation). A watch stopped via the legacy plain `unsubscribe($sid)` no longer strands its
  internal stop-registry entry forever: the watchdog's self-cancel tick releases the entry and
  best-effort deletes the server-side ephemeral (only the CURRENT timer runs this cleanup - a
  rotated-out old timer cannot release a consumer that lives on under a new sid). The cleanup
  latches the stop FIRST, exactly like `stopOrderedConsumer()` - a recreate parked in its awaits
  when the tick fires tears its fresh instance down instead of installing a consumer whose stop
  handle is already gone (which would have been permanently unstoppable). And a
  `stopOrderedConsumer()` racing an in-flight recreate whose remaining create attempts then all
  fail no longer emits a spurious terminal "recreate failed" error for the deliberately stopped
  consumer - the exhausted episode releases the never-adopted fresh inbox and returns silently.

- `[bugfix]` KV buckets created with `sources` now attach the mandatory ADR-57 subject transforms
  (`{src: "$KV.<src>.>", dest: "$KV.<bucket>.>"}`), so sourced entries are re-subjected into the
  new bucket's prefix and visible to get/getAll/keys/watch - previously they were copied under the
  origin's subjects where every read path was blind to them (invisible data). Caller-supplied
  transforms pass through verbatim (full-control sources, e.g. non-KV streams; the name is then not
  `KV_`-prefixed). Mirror buckets now set `mirror_direct: true` and write THROUGH to the origin's
  prefix (nats.go `putPre` parity) instead of publishing to their own subject that no stream
  ingests (503); cross-domain mirrors (`domain` shorthand → `external: {api: "$JS.<domain>.API"}`)
  route writes via the external API prefix and reads via the origin prefix. The new
  `KeyValueBucket::bind()` resolves the same prefixes from STREAM.INFO for handles attached to
  mirror buckets created elsewhere - note this includes a FRESH `keyValue()` handle in the same
  process (each handle is independent; only the instance that ran `create()` is auto-resolved).
  Mirror read/write prefixes are applied only AFTER the stream create is confirmed server-side -
  and, symmetrically, the stale-prefix reset in `create()`/`bind()` also happens only after the
  server confirmed the new configuration, atomically with the re-apply. So a failed mirror create
  can neither leave a fresh handle misdirecting writes at the origin's subjects NOR blind a
  handle whose mirror the server confirmed earlier (a failed re-create leaves the last confirmed
  routing intact), and no suspension window exists in which concurrent readers see half-reset
  prefixes.
  Also note the name-mapping rules: the `bucket` alias is `KV_`-prefixed UNCONDITIONALLY (it
  explicitly declares a KV bucket, so a bucket legitimately named `KV_x` maps to its own backing
  stream `KV_KV_x`), while a transform-less explicit source `name`, bare-string entries, and
  mirror `name`s are `KV_`-prefixed only when not already (nats.go stream-name idempotence) - the
  previous used-as-is behavior produced invisible data; supply `subject_transforms` to source a
  non-KV stream by its verbatim name.

- `[bugfix]` Pipelined Object Store uploads can no longer store a silently corrupted object: the
  ADR-21 503 retry inside `publish()` could re-order a retried chunk BEHIND later accepted chunks,
  and `put()`/`putStream()` then published the meta record and reported success for an object whose
  stream-order reassembly no longer matches its digest (every later read failed with a digest
  mismatch). The acked stream sequences are now verified strictly increasing in chunk order; a
  permuted upload aborts before the meta publish with a clear retryable error, and one seq-less
  ack (a defensive case no real server produces) skips only its own neighbor pairs instead of
  disabling the check for all remaining chunks. Any failed/aborted
  upload additionally purges the partial NUID's chunks (nats.go `purgePartial` parity) - previously
  they were orphaned in the stream forever, since no meta record ever referenced them.

- `[bugfix]` The infinite pipelined pull engine no longer treats a transient 503 (no JetStream API
  responder - server restarting, JS still wiring up after a reconnect, a leader election window) as
  terminal. Previously the worker stopped PERMANENTLY, and with no `onError` configured `handle()`
  resolved normally with the processed count - indistinguishable from a clean drain while messages
  piled up in the stream. 503 is now routine-with-backoff (nats.go `Consume()` parity; finite/fetch
  semantics unchanged); the first 503 of a streak fires `onError` once as an operator signal,
  re-armed by the next delivery OR by any routine non-503 retire (a 404/408/non-terminal 409 or a
  client-side deadline proves the JS API answers again) - i.e. one `onError` per no-responders
  episode, so a later outage on an idle, never-delivering stream is still reported.

- `[bugfix]` The `fetchBatch()` and `directGetBatch()` reply inboxes are now slow-consumer-exempt
  like the mux (#118) and pull-pipeline (#120) inboxes. A burst of small replies above the
  per-subscription pending cap (default 1024) arriving within one read chunk was silently
  DropOldest-discarded: a fetch returned with its head missing (permanently lost on a
  `max_deliver: 1` consumer - the server counted every message as delivered), and a Direct Get
  batch returned a truncated result presented as complete (its replies are never redelivered and
  the 204 end-of-batch marker still arrived). Memory stays bounded by the requested batch size.

- `[bugfix]` `drain()` and `flush()` can no longer hang forever on a backpressure-stalled peer:
  transport writes suspend indefinitely when the send buffer is full (they cannot be cancelled),
  and `drain()` cancels the heartbeat FIRST - removing the only escalation that could break such a
  wedge - so its documented ~`requestTimeoutMs` bound was unenforceable. Drain writes are now
  bounded by the drain budget (a wedged write is abandoned; the teardown's socket close fails it
  out) and any drain write failure falls through to teardown, so `drain()` always reaches Closed.
  Note the contract change: `drain()` no longer THROWS on a dead-socket write failure - it reports
  it via the error listener and still closes cleanly (previously it threw and left the connection
  stranded in Draining). The teardown closes the transport BEST-EFFORT like every other terminal
  path, so a custom transport whose `close()` throws on an already-broken socket cannot re-strand
  Draining either. Handler publishes issued while `drain()` delivers backlog are bounded by the
  drain's REMAINING budget (not a fresh full `requestTimeoutMs` each), and a delivery pass stops
  delivering past its head message once the budget is exhausted, reporting the dropped remainder
  via the loud "drain deadline exceeded" error - previously K backlog messages whose handlers each
  published against a wedged transport could extend `drain()` to ~K x `requestTimeoutMs`.
  `flush()`'s PING write is bounded by the request timeout and surfaces a `TimeoutException` on a
  write-side wedge. Implemented at the connection layer - `TransportInterface` is unchanged, so
  custom transports are unaffected.

- `[bugfix]` The WebSocket transport's ping-answer (pong) and RFC 6455 Close-echo writes are no
  longer inline in the read fiber: control answers are held as data SLOTS drained by a single
  writer fiber - a latest-pong slot where a newer ping's payload REPLACES a queued-but-unsent pong
  (RFC 6455 5.5.3: only the most recent ping needs answering) and a dedicated OP_CLOSE echo slot
  that is always flushed (RFC 6455 5.5.1 makes the echo mandatory; first Close wins), giving
  constant memory with no cap that could drop answers. Inline in the read fiber, a pong write that
  THREW (peer died right after coalescing `[MSG][PING]` into one read) discarded the
  already-decoded MSG bytes from the same read - consumed from the buffer, never delivered, never
  resent - and a pong write that SUSPENDED on a backpressure-stalled peer parked the entire read
  path behind an outbound control frame (the same suspension class as the
  `WebSocketTransport::close()` wedge fixed below). A coalesced ping flood now collapses to one
  pong answering the NEWEST ping, and the Close echo goes out even behind 16+ coalesced pings.

- `[bugfix]` Every `$SRV` discovery response (`PING`/`INFO`/`STATS`/`SCHEMA`) now serializes empty
  `metadata` maps as JSON objects (`{}`) instead of arrays (`[]`), service-level and per-endpoint.
  ADR-32 types metadata as `map[string]string`, and Go-based consumers (`nats micro ls`, nats.go
  micro) hard-fail unmarshalling `[]` into a map - rejecting the whole discovery response, so a
  metadata-less service (the default) was invisible to standard tooling. `statsSnapshot()` still
  returns plain PHP arrays; the conversion happens only at the wire boundary.

- `[bugfix]` Service reply-publish failures can no longer escape the endpoint callback into the
  shared dispatch loop (which aborted delivery for every subscription on the connection): the
  schema-validation error reply is guarded; a requester-controlled non-UTF-8 correlation header no
  longer makes the JSON error reply throw at encode time (the correlation id is omitted instead -
  this was remotely triggerable); connection-level reply failures are recorded on the endpoint and
  swallowed; and a `ServiceError` code containing CR/LF is collapsed to one line like the
  description instead of blowing up the header build. A request whose handler errored and whose
  error-reply publish then ALSO failed counts ONE error (previously two, letting `$SRV.STATS`
  report `num_errors > num_requests`) and no longer emits a late duplicate `request_error`
  observer event with a hardcoded code contradicting the handler's own; the reply-publish failure
  is counted as the request's error only when the handler itself succeeded, and the endpoint's
  `last_error` records it in both cases.

- `[bugfix]` `WebSocketTransport::close()` can no longer deadlock the client on a backpressure-stalled
  peer. The best-effort RFC 6455 Close frame was written inline before the socket close; Amp's socket
  `write()` SUSPENDS the calling fiber (it does not throw) while the write buffer is full - e.g. a peer
  holding the TCP window at zero - so the `try/catch` never engaged, `close()` parked forever before
  reaching the socket close, and since that close is the only thing that errors pending writes out,
  every recovery/disconnect/drain path awaiting `transport->close()` wedged permanently (the heartbeat
  had already self-cancelled once state left Open, so nothing remained to break the cycle). The Close
  frame now goes out on its own fiber awaited with a 0.25 s bound: a responsive socket still sends it
  before closing, and a wedged write is abandoned - the socket close then fails it out, waking its
  fiber. Any other writes still queued behind a stalled buffer at that point are failed out the same
  way (loudly, via their write futures) instead of being waited on - `disconnect()` is the documented
  lossy path and `drain()` has already flushed and PONG-confirmed before it closes the transport.
  `readLine()` now also pins its socket in a local for the whole read loop, so a reader resuming
  inside the bounded close window cannot deref the already-nulled socket property. Plain-TCP
  `AmpSocketTransport` was never affected (its `close()` does not write).

- `[bugfix]` The pipelined pull engine no longer treats a single immediately-answered empty pull as proof
  of idleness when `setDepth()` > 1 (#169). Retires run in issue order, so under `no_wait` with steady
  traffic below `depth*batch` a tail pull that raced an already-drained stream would 404 right after a
  delivering head pull - and that lone empty latched the idle drain, injecting a periodic backoff pause
  and clamping the next generation to depth 1 (the effective pipeline oscillated depth->1->depth with
  ~10 ms stalls despite continuously delivering). The engine now counts a ROLLING streak of consecutive
  empty retires across pulls (reset by any delivery, surviving the engine's continuous refill) and only
  latches the idle drain once the streak spans one full pipeline width (>= `setDepth()`), so a
  delivering pipeline keeps its full depth while a genuinely idle stream still accumulates to the latch
  and backs off exactly as before. `setDepth(1)` (and finite `setIterations()` mode) keep the original
  latch-on-first-empty behavior unchanged.

## [2.7.1] - 2026-07-16

### Fixed

- `[bugfix]` `request()`/`requestMany()` now surface a clear, catchable `ConnectionException` when the
  shared mux reply-inbox subscription `_INBOX.<inbox>.*` is rejected by server permissions, instead of
  hanging every request to a silent timeout (#167, a follow-up to #118's muxed request inbox). An account
  without subscribe permission for the reply-inbox wildcard (e.g. `_INBOX.>`) previously saw all
  request/reply calls time out with no explanation; the connection now detects the async permission
  `-ERR`, drops the dead mux state (so a reconnect does not replay the rejected wildcard SUB), and fails
  requests fast with a message naming the wildcard permission required. The in-flight request that
  triggers the rejection also fails fast rather than waiting out its full timeout.
- `[bugfix]` Pull consumers with a priority group under the `overflow` or `prioritized` policy now honor
  `setDepth()` instead of pulling strictly serially. Those policies never emit a `Nats-Pin-Id`, so the
  "grouped-and-unpinned" serialization guard (meant to let a `pinned_client` group capture its pin before
  fanning out) previously held for the whole run and silently disabled pipelining. A grouped run now pulls
  serially only until its first delivery, then fans out to the configured depth if no pin was captured.
  Conversely, a `pinned_client` group that captured a pin and then LOST it (a `423` stale-pin cleared the
  pin mid-run) now RE-SERIALIZES its pin re-capture - pulling one at a time until it re-pins - instead of
  racing pin-less pulls at full depth, matching the bootstrap behavior (#170).
- `[bugfix]` `requestMany()` no longer discards already-collected replies if the mux reply inbox is
  permission-rejected mid-collection (e.g. a reconnect re-SUB is rejected after some replies arrived): it
  returns the partial batch it has, and surfaces the clear permission error only when nothing was collected.

## [2.7.0] - 2026-07-15

### Changed

- `[feature]` Pull consumers now PIPELINE their pull requests (#120). `PullConsumerIterator::handle()` no
  longer creates a fresh inbox + SUB, publishes one pull, waits, and UNSUBs per pull. It opens ONE
  long-lived pull inbox subscription `_INBOX.JS.PULL.<base>.*` for the whole run and keeps several pulls
  in flight at once (see `setDepth()`, default 2), issuing the next pull as soon as one retires - so there
  is no inter-batch round-trip stall and no SUB/UNSUB churn per pull. The consumer's throughput ceiling
  rises from ~1 batch per RTT to overlapping pulls (a large gain on higher-latency links). Status replies
  route by a per-pull token, while delivered messages (which the server sends on their original subject)
  are matched to the oldest in-flight pull in order; the shared pull inbox is exempt from the slow-consumer
  drop so a slow handler never loses a buffered message. All existing semantics are preserved:
  `stop()`/`drain()`, the escalating
  idle backoff, finite `setIterations()` (still serial and stop-on-any-error), pinned priority groups
  (`setGroup()`), 423 stale-pin re-pull, terminal-status `onError`, and reconnect survival. The
  single-shot `fetchBatch()`/`fetchNext()` primitives are unchanged.
- `[feature]` The default pull batch size is now **100** (was 1) and a new `setDepth()` controls pull
  overlap (default 2). `setBatching(1)` still works and still pipelines. A consumer that relied on the
  old batch=1 default now fetches up to 100 messages per pull - size it against your `MaxAckPending` and
  `ack_wait`; call `setBatching(1)` to restore one-message pulls.
- Internal: consolidated duplicated boilerplate across the connection, JetStream, and transport layers
  into shared helpers/traits (#111) - the ObjectStore digest and meta-record builders, the JetStream
  offset-pagination loop, the connect-handshake poll loop, the API-error decode, the Direct Get meta
  decode (unifying guards that had drifted between `info()`/`list()`), and the client TLS-context
  assembly. No public API or wire-format change; behavior is unchanged.

## [2.6.0] - 2026-07-15

### Added

- `[feature]` `NatsOptions::$readChunkSizeBytes` (default 128 KiB, up from Amp's 8 KiB) caps the bytes a
  single transport socket read may return; both the TCP and WebSocket transports apply it via the Amp
  socket's chunk size after connecting. Large inbound payloads (e.g. an ObjectStore download) now arrive
  in far fewer chunks, dividing the per-chunk syscall + fiber-spawn + parser-push overhead ~8-32x. It
  raises only the MAXIMUM a read may return - the socket still returns just what is available, so
  small-message behavior is unchanged (#119).
- `[feature]` `NatsClient::readIncoming()` / `NatsConnection::readIncoming()` expose one read-and-dispatch
  cycle as an `IncomingChunkResult` (the frame count PLUS whether the read consumed bytes off the wire),
  the progress signal the wait loops use to decide whether to yield. `processIncoming()` is unchanged - it
  remains the frame-count-only view of the same cycle (#119).

### Changed

- `[feature]` KV `keys()`/`listKeys()` no longer download every value just to list names (#110). They now
  enumerate via a `last_per_subject` + `headers_only` ephemeral consumer (the metaOnly watch path nats.go's
  `Keys()` uses), so only the `KV-Operation`/`Nats-Sequence` headers return and DEL/PURGE tombstones are
  filtered by header alone - no value body crosses the wire. Previously `keys()` was `array_keys(getAll())`,
  which issued one full-value Direct Get per key. The returned set of names is unchanged (deleted keys
  excluded); the order (stream-sequence order of each key's latest record) remains unspecified, as before.
  Measured against a 100-key bucket with 4 KiB values: value bytes read dropped from ~423 KiB to ~13 KiB
  (headers only) and the 100 per-key Direct Get requests became a single consumer.
- `[feature]` KV `getAll()` and Object Store `list()` fetch every key/object with ONE batched multi_last
  Direct Get (ADR-31) instead of one Direct Get request per subject, on servers that support it (#110).
  The batched path is version-gated via the new `JetStreamContext::supportsBatchedDirectGet()` (NATS 2.11+);
  older or unparseable-version servers fall back to the unchanged per-subject Direct Get fan-out. The
  subject list is split into chunks bounded by the server's 1024-result cap and by the negotiated
  `max_payload`, so buckets with more than ~1024 keys are enumerated across several batched requests
  instead of a single oversized one the server would reject. Returned data, tombstone/deleted filtering,
  and 404/missing handling are identical on both paths. Measured against a 100-key bucket, `getAll()`
  issued 1 Direct Get request instead of 100 (the value bytes it must transfer are unchanged, since it
  returns the values).
- `[feature]` The read-path wait loops (`flush()`, `drain()`, `request()`, `requestMany()`,
  `JetStreamContext` pull-fetch and direct-get-batch, `KeyValueBucket::history()` and `keys()`,
  `SubscriptionQueue`, `Service::run()`) no longer pay a 1 ms idle sleep on partial-frame progress. A frame spanning N socket
  chunks previously cost ~N ms (~125 ms/MB at the 8 KiB default; ObjectStore downloads were capped near
  8 MB/s) because each partial read reported 0 frames and every loop treated 0 frames as "idle" and slept
  1 ms - even when the rest of the payload was already in the kernel buffer. The loops now sleep 1 ms ONLY
  on a genuinely idle read (an empty read, or another fiber owning the socket) and loop immediately when
  bytes were consumed but no full frame completed yet. Delivery/enumeration is byte-for-byte identical;
  each loop's deadline/cancellation bound, the #117 pong-slot flush semantics, #147 frame retention, #148
  Connecting-window reads, and #135 parking are preserved, and a truly empty socket still yields the 1 ms
  so the event loop advances and deadlines fire (#119).
- `[feature]` `request()`/`requestMany()` now share ONE long-lived muxed reply inbox per connection
  instead of creating a fresh inbox subscription (SUB + UNSUB) for every call (#118). A single wildcard
  subscription `_INBOX.<base>.*` is established lazily on the first request; each request publishes its
  reply-to as `_INBOX.<base>.<token>` and the reply is routed back by that per-request token. This
  removes two control-plane frames and a server-side interest change per request - a large throughput win
  for JetStream, which funnels every API call, KV, and Object Store operation through `request()`. Reply
  delivery, timeouts, no-responders (503) handling, cancellation, and `requestMany` semantics are
  unchanged. A delayed or duplicate reply for a completed/timed-out request is discarded by token (it can
  never reach a later request), and the shared mux queue is exempt from the slow-consumer drop so a reply
  is never dropped. NOTE (permission requirement): the mux inbox subscribes to a wildcard under the inbox
  prefix, so an account permissioned to *subscribe* only to exact `_INBOX.<...>` subjects (not the
  `_INBOX.>`/`_INBOX.*` wildcard) will have the mux SUB rejected where per-request exact-subject inboxes
  previously succeeded. Because the client does not block on the SUB's server acknowledgement, that
  rejection arrives as an asynchronous `-ERR` (delivered to the error listener), after which every
  `request()`/`requestMany()` on the connection times out (the server has no interest on the wildcard) -
  a silent regression relative to the pre-mux exact-inbox path for such accounts. Grant `_INBOX.>`
  subscribe permission (the same requirement as nats.go's muxed responder) to use this client's
  request/reply on a restricted account.

## [2.5.4] - 2026-07-13

### Fixed

- `[bugfix]` Ordered-consumer recreate no longer loses the replay under load (a regression in the
  #122 deliver-inbox rotation, shipped in 2.5.3). The rotation adopted the new instance (its
  client-chosen name and the reset expected-sequence) only AFTER the CONSUMER.CREATE reply, but the
  server can begin delivering the replay on the rotated inbox the instant the consumer exists - and
  those frames are dispatched during the create request's own read-pump, before that reply is
  processed. The early replay frames were then filtered out by the not-yet-adopted consumer name and
  a later one tripped a spurious gap, cascading into a recreate storm in which only the pre-gap
  message was ever delivered (observed as an intermittent, load-dependent failure of the
  ordered-consumer dropped-delivery recovery). The name is chosen client-side, so the instance is now
  adopted BEFORE the create await; a new deterministic test pins a replay frame arriving ahead of the
  create reply, and the recovery now passes reliably under sustained CPU load (#122).

## [2.5.3] - 2026-07-12

### Fixed

- `[bugfix]` The reconnect-buffer flush no longer defers the `Open` flip indefinitely under sustained
  publish pressure (#165, a #148 follow-up). Since #148 recovery stays `Connecting` until
  `flushReconnectBuffer()` fully drains, and the flush loops so publishes buffered mid-flush still go
  out in order before `Open`. A fiber publishing continuously re-filled the buffer during each flush
  write's suspension, so the loop kept iterating and the connection could stay `Connecting` for the
  whole outage window - `subscribe()`/`request()`/`flush()`/`rtt()`/`processIncoming()` all threw
  "Connection is not open" and `Reconnected` never fired (nats.go converges here by holding the
  connection mutex during the pending flush; PHP fibers have no equivalent implicit exclusion). The
  flush is now bounded: after `RECONNECT_FLUSH_MAX_PASSES` drain passes it SEALS the buffer, so late
  publishers park on a flush-done gate (then write directly once `Open`, or fail loudly once `Closed`)
  instead of appending, the remaining bytes drain in one final pass, and `Open` flips within a bounded
  number of passes. Wire ordering is unchanged: the already-buffered frames are written before the
  flip, so per-publisher order and buffered-before-direct (#148) still hold, and #123 retain-on-flush-
  failure / loud-exhaustion semantics are untouched.
- `[bugfix]` A WebSocket server that delivered final data frames and then gracefully closed in the
  same read no longer loses those messages (#115). `WebSocketTransport::processFrames()` threw
  `TransportClosedException` the instant it hit the `OP_CLOSE` frame, discarding the payload of every
  data frame earlier in the same batch - already consumed out of the read buffer by the by-reference
  `WebSocketFrameCodec::decode()`, so those NATS messages were gone (silently lost on core NATS;
  recovered only via redelivery on JetStream). The transport now returns that accumulated data from
  `readLine()` first and defers the close to the next `readLine()` via a `pendingClose` flag,
  including on the large-frame spill path (#164) so a spilled frame's bytes are returned too. The RFC
  6455 Close echo (#161) is still written when the close is first seen. Relatedly, two RFC 6455 5.4
  protocol violations in the same handler now fail loudly with a `ProtocolException` instead of
  silently degrading: a new data frame arriving mid-fragmentation (previously overwrote the partial
  message) and an orphan continuation frame with no fragmented message in progress (previously
  dropped). Both violations DEFER the same way as the close: any valid data decoded earlier in the
  same batch is returned from `readLine()` first, and the `ProtocolException` is surfaced on the next
  `readLine()` call - the connection still fails loudly, just after the already-decoded messages are
  delivered, with no silent drop.
- `[bugfix]` The WebSocket permessage-deflate inflate path now caps the decompressed size, closing a
  decompression-bomb OOM vector (#121). `WebSocketFrameCodec::inflate()` fed the whole compressed
  payload to a single `inflate_add()` with no output bound, so a tiny hostile frame could inflate to
  gigabytes and OOM-spike the client - inconsistent with the file's existing max-frame-size / #89
  threat model. It now inflates the input in bounded slices (DEFLATE's ~1032:1 maximum ratio caps
  each call's output) and throws a `ProtocolException` the moment the accumulated output exceeds the
  cap, keeping peak allocation bounded. Legitimate payloads within the cap inflate byte-identically.
- `[bugfix]` Ordered consumers no longer orphan a live server-side ephemeral on a recreate, and a
  non-current heartbeat can no longer drive a recreate storm (#122). Each recreate now ROTATES the
  deliver inbox: it generates a fresh deliver subject, subscribes it (a new sid), points the new
  `CONSUMER.CREATE` at that subject, and unsubscribes the old inbox. An orphan left by a lost
  `CONSUMER.CREATE` reply (a timeout / connection loss where the create may still have SUCCEEDED
  server-side) therefore delivers to the OLD inbox that the client no longer subscribes to, so neither
  its data frames NOR its plain idle heartbeats reach the tail-gap check - the recreate storm #122
  targets cannot happen - and dropping the client's interest lets the orphan's server-side
  `inactive_threshold` reap it, so the leak self-heals. Because only the current consumer's frames
  arrive on the current inbox, the tail-gap check is inherently scoped without parsing control-frame
  subjects. Each recreate still chooses the consumer name client-side and best-effort-deletes any
  orphaned prior attempt as a faster-cleanup nice-to-have (mirroring the #151 tolerate-any-failure
  delete), and on a TERMINAL recreate failure the deliver subscription is torn down so "dead" is
  actually dead. The #113 watchdog (re-armed on the rotated sid) / `recreateInFlight` guard,
  #114/#116 recreate retry, and #155 ack-metadata tolerance are all preserved.
- `[bugfix]` A non-100 status control frame (e.g. `409 Consumer Deleted`, `404`, `408`, `503`) on a
  push deliver subject is no longer forwarded to the user handler as if it were a message (#121).
  `handlePushControlMessage()` only intercepted status 100 (idle heartbeat / flow control); other
  status frames fell through and were delivered as empty "data". They are now intercepted for every
  push subscription: an ordered consumer treats such a terminal status as a gone consumer and
  recreates from the last in-order point, while a caller-owned push consumer surfaces a terminal
  (4xx/5xx) status through the error listener as a descriptive `JetStreamException` instead of
  silently dropping it. Status-0 data messages (including those carrying user headers) are unaffected.
- `[bugfix]` A JetStream publish ack that carries neither an `error` nor a `stream` is now rejected
  with a `JetStreamException` instead of being accepted as a bogus `PubAck('', 0)` success (#121),
  matching nats.go, which rejects an empty-stream ack as invalid.
- `[bugfix]` `directGetBatch()` and `KeyValueBucket::history()` no longer silently return a truncated
  prefix when their bounded wait elapses before completion (#121). A Direct Get batch that never
  receives its end-of-batch marker (204 / `Nats-Num-Pending: 0`), or a history replay that never
  catches up (`num_pending` never reaches 0), now throws a `JetStreamException` reporting the
  incomplete result rather than returning whatever partial data was collected as if it were complete.
  Both bounds are PROGRESS-BASED (reset on each inbound frame, mirroring #153's missed-heartbeat
  approach), so a healthy-but-slow large replay that keeps making progress no longer throws - only a
  genuinely stalled server (no progress for the interval) does. `history()` gained an optional
  per-call progress-timeout argument (defaulting to the previous bound).

### Documentation

- `[docs]` `NatsConnection::publish()` now documents its at-least-once, nats.go-parity semantics while
  reconnecting (#121): a publish issued during a reconnect is buffered and reports success immediately
  - before the frame reaches any server - and if the reconnect ultimately exhausts every attempt the
  buffered frames are discarded, with the loss signalled only out-of-band via the connection-level
  `Closed` event and the "Reconnect exhausted: N bytes ... discarded" async error (#123), never
  through the returned `Future`. The related write-order note - a publish whose direct write fails is
  re-sent AFTER the frames concurrent publishers buffered during the same outage - is confirmed as the
  intended buffered-before-direct ordering (#148/#165), not a defect: the re-send is deliberately not
  seeded into the flush so recovery success stays independent of the failing frame (#145) and the
  post-recovery delivery drain runs before the retry (#144); per-publisher order is preserved. No
  behavior change.

## [2.5.2] - 2026-07-12

### Fixed

- `[bugfix]` KV, Object Store, and atomic-batch header requests now share the same no-responders
  exception taxonomy as every other JetStream path (#161). `KeyValueBucket::publishWithHeadersAck`
  (every KV put/update/delete with headers), `ObjectStoreBucket::publishMeta` (the meta-record
  rollup publish), and `BatchPublisher`'s start and commit requests called `requestWithHeaders()`
  directly, so on a JetStream-disabled server or an unbound subject they surfaced a bare
  `NatsException('No responders...')` instead of the `JetStreamException(503)` that
  `JetStreamContext::jsRequest()`/`publish()` produce - a caller catching `JetStreamException`
  missed it. The normalization is extracted into a shared `JetStreamRequest` helper that all four
  sites (including `jsRequest()`) now funnel through. Only the no-responders case is normalized; the
  batch start/commit reply-shape detection and version pre-flight (#130/#138/#152) are unchanged.
- `[bugfix]` A FAILED initial `connect()` that recovers via `recoverConnection()` (reconnect enabled)
  now emits `Connected` for the first-ever successful handshake instead of `Disconnected` then
  `Reconnected`, and no longer bumps the reconnect count for what is really an initial connect (#161).
  A process whose very first dial needed one retry previously observed `Disconnected -> Reconnected`
  for a connection that was never up and never saw `Connected`, so listener state machines keyed on
  `Connected` (metrics, readiness gates) never fired. The connection state machine (#144/#145/#148)
  is untouched: only which lifecycle event fires on the initial-connect recovery path changed,
  gated on a new "has ever been open" flag set by `markConnectionOpen()`.
- `[bugfix]` On a server-initiated WebSocket Close frame the transport now writes an echo Close frame
  (mirroring the received status code) before surfacing `TransportClosedException`, per RFC 6455
  section 5.5.1 (#161). Previously the client threw without echoing, which strict intermediaries and
  servers treat as an abnormal closure (1006). The echo is best-effort (write failures are ignored -
  the socket may already be gone) and the `TransportClosedException` the connection layer relies on
  to reconnect is unchanged.
- `[bugfix]` The per-inbound-chunk pending-message drain no longer scans every live subscription
  (#162), a performance regression introduced by #139. To avoid a per-message `SplQueue` alloc/free,
  #139 began keeping each subscription's queue allocated but EMPTY for the subscription's lifetime;
  `drainAllPending()` then iterated `array_keys($this->pendingMessages)` after every inbound chunk, so
  the drain scan became O(all live subscriptions) plus a fresh `array_keys()` copy of every sid - paid
  on every chunk INCLUDING each message-free heartbeat self-read (measured ~213 us/chunk at 10,000
  idle subscriptions, ~0 in v2.4.0). The drain now iterates a dirty set of only the sids whose queue
  actually holds a message: a sid is recorded when a message is enqueued to it and removed once its
  queue drains empty, so the per-chunk cost is O(sids-with-backlog) and a message-free chunk returns
  in O(1) with no allocation. #139's no-per-message-realloc win is preserved (the empty queue objects
  are still retained; only the scan set changed), and delivery is byte-for-byte unchanged: per-sid
  FIFO holds, and cross-sid delivery keeps the ascending-sid (registration) order of the old full-map
  scan. The `dispatchingSids` re-entrancy guard (#112/#156), auto-unsubscribe completion (#112), and
  `drain()`'s bounded backlog round-up (#149/#150) are unaffected - `hasUndeliveredDrainBacklog()` now
  keys on the same dirty set, staying consistent with what the drain iterates.
- `[bugfix]` Restore inbound hot-path throughput lost in #140 and drop the extra per-chunk read
  fiber (#163). First, `ProtocolParser::splitControlLine()` tokenized MSG/HMSG control lines with an
  `explode(' ')` + per-token canonical scan fast path that measured slower per line than the
  `preg_split('/\s+/')` it replaced on a PCRE-JIT build (roughly 5-7% on typical single-space lines
  and larger on non-canonical lines; JIT is on by default since PHP 7.3 and this package requires
  8.2+): the canonical scan cost more than the JIT-compiled regex on typical single-space lines, and
  any non-canonical line then ran `preg_split()` anyway. The split reverts
  to `preg_split('/\s+/')`, which is byte-identical to the fast path for every input (the fast path
  already fell back to this exact call off the canonical path), so the whitespace tolerance is
  unchanged - multi-space, tab, and other whitespace runs still separate fields, pinned by the
  differential fuzz and the tokenization tests - and only the speed changes. Second,
  `AmpSocketTransport::readLine()` wrapped the socket read in its own `async()`, so each inbound
  chunk spawned a SECOND fiber on top of `processIncoming()`'s read fiber; the read now runs inline
  in the caller's fiber and returns an already-resolved future (mirroring the write path #136),
  removing one fiber and future allocation per chunk. Read cancellation (a bounded read's timeout
  still surfaces as `CancelledException`), EOF-to-`TransportClosedException`, and the empty-string
  no-socket poll all still surface through the returned future exactly as before.
- `[bugfix]` Frames the server coalesces BEHIND the handshake PONG in one TCP segment are no longer
  dropped, and a frame left partly buffered at the handshake boundary no longer corrupts the stream
  (#157). `awaitInitialPong()` returned the moment it saw the PONG, discarding every frame the parser
  had already extracted after it in the same batch - an async INFO with `connect_urls`, a lame-duck
  notice, an -ERR - so discovered cluster peers vanished with no trace. Separately, `connectOnce()`
  then replaced the parser wholesale to couple the frame bound to the negotiated `max_payload`; if
  the handshake segment ended mid-frame, those buffered bytes were thrown away, the next read resumed
  at an arbitrary offset, parsed a bogus control line, and raised a spurious `ProtocolException` that
  forced an unnecessary reconnect. `awaitInitialPong()` now hands the frames parsed behind the PONG
  back to `connectOnce()`, which dispatches them through the normal enqueue/deliver path (an INFO
  updates the discovered-server pool; a MSG reaches its handler); and the post-handshake bound change
  is applied in place on the same parser (`ProtocolParser::setMaxFrameSize()`) instead of replacing
  it, so a partial trailing frame completes cleanly on the next read. The connect-start parser reset
  (#125) is preserved, so no framing state leaks across a reconnect. A lame-duck INFO coalesced behind
  a RECONNECT PONG, now dispatched during `connectOnce()`, would ask to reconnect while a recovery was
  already running on the same fiber; that re-entrant request is guarded to a no-op so the in-flight
  recovery still completes instead of deadlocking on its own future.
- `[bugfix]` Frame-dispatch errors and a discarded inbound backlog can no longer vanish silently
  (#158), closing three gaps in tension with the "a drop must never be silent" principle (#134).
  First, the per-chunk `finally { drainAllPending(); }` in `processIncoming()` could REPLACE the
  in-flight dispatch exception: when a fatal frame (a server -ERR, a PONG-write failure) had thrown
  and a handler then threw while the enqueued backlog drained, PHP propagated the finally's exception
  and swallowed the fatal one, so the connection continued as if the server never errored. The drain
  is now contained so a handler failure during it is routed to the error listener and the primary
  dispatch exception still reaches the caller's escalation path. Second, `dispatchFrames()` kept only
  the FIRST failure (rethrown after the loop) and dropped the second and later frame failures from the
  same chunk without a trace; each suppressed failure is now emitted through the error listener (the
  first is still rethrown). Third, a terminal close reached from reconnect exhaustion, an auth abort,
  or reconnect being disabled cleared the parsed-but-undelivered INBOUND backlog silently, asymmetric
  with the loud OUTBOUND reconnect-buffer discard (#123); such a close now emits an error naming the
  count of inbound messages being discarded before releasing state (`drain()`'s own bounded-deadline
  discard (#149) and `disconnect()`'s documented nats.go `Close()` parity path are unchanged).
- `[bugfix]` `drain()` no longer silently discards the backlog of a subscription whose handler is
  suspended mid-dispatch (#149). When a handler awaited inside the dispatch loop while messages
  were still queued for its sid, the `dispatchingSids` re-entrancy guard made drain()'s final
  delivery skip that sid, `releaseRuntimeState()` then cleared the subscription registry, and the
  resumed dispatch loop broke on the missing subscription - dropping the remainder on the
  documented lossless path. drain() now waits for every in-flight dispatch to finish and every
  sid's queue to empty before releasing state (nats.go `Drain()` waits for per-subscription
  delivery to complete before closing). The wait is bounded by a single overall drain budget
  computed once at entry - one deadline covers BOTH the flush-wait and the backlog-wait phases, so
  total drain time cannot exceed ~`requestTimeoutMs` (the earlier fix added a second sequential
  deadline that roughly doubled worst-case drain latency). When a handler stays suspended PAST that
  deadline so the remaining buffered messages cannot be delivered, drain() no longer drops them
  silently: it counts the still-buffered messages and emits an error naming that count ("drain
  deadline exceeded: N buffered message(s) were not delivered before close") through the error
  listener before closing, so the loss is always observable (mirrors the #123/#134 observable-drop
  principle).
- `[bugfix]` `drain()` no longer breaks when a handler publishes during the drain (#150). A
  JetStream ack, a `Service` reply, or `NatsMessage::respond()` invoked from a handler while the
  connection is `Draining` previously threw `ConnectionException` ("Connection is not open"): the
  publish path only wrote to the socket when `Open` and only buffered while a reconnect was in
  flight. The unguarded final backlog delivery then propagated that exception out of drain(), so
  `releaseRuntimeState()`/`transport->close()` never ran - the connection stranded in `Draining`
  with the socket open and the delivered-but-unacked message was redelivered by the server. Now a
  publish during `Draining` writes straight to the still-live socket (nats.go drains by publishing
  then closing), and drain's final delivery is contained per pass so a handler exception is routed
  to the error listener while drain() always reaches `Closed`. A publish after the connection has
  `Closed` still throws (#146); pong-slot correlation (#117) and auto-unsubscribe counting (#112)
  are preserved.

- `[bugfix]` JetStream push/ordered subscriptions now run an idle-heartbeat watchdog, so a consumer
  that stops delivering is no longer silently dead forever (#113). An ordered consumer, and any push
  consumer the caller created with `idle_heartbeat`, receives a status-100 heartbeat at least every
  interval while it is alive; previously nothing noticed when those heartbeats STOPPED, so a
  consumer reaped after an `inactive_threshold` lapse, a `mem_storage` R1 ordered-consumer restart,
  or an interest gap left the client holding a live core subscription to a deliver inbox no consumer
  would ever publish to again - no data, no heartbeat, no error, forever. The sequence-gap logic
  could never catch this because it only runs when a frame arrives. A monotonic watchdog now fires
  when no frame (data, heartbeat, or flow-control) has arrived for two heartbeat intervals: for an
  ordered consumer it triggers the same recreate path the gap logic uses (resuming from the last
  in-order point via `opt_start_seq`), matching nats.go's `ErrConsumerNotActive` monitor; for a
  caller-owned push consumer, which the library cannot recreate, it surfaces a descriptive
  "not active" error through the error listener. The watchdog rearms on every inbound frame (so a
  quiet-but-alive consumer is never falsely recreated), fires at most once per silence episode (no
  recreate/error storm; a failed recreate falls back to the existing bounded-retry + error-listener
  path), and holds the connection weakly and self-cancels the moment its subscription is torn down,
  so it never leaks a timer or roots an abandoned connection (mirroring the #126 ping timer).
  `subscribeOrderedConsumer()` gains an optional `idleHeartbeatNs` argument to tune the interval, and
  KV watchers now request a default idle heartbeat too (tunable via the new
  `KeyWatchOptions::$idleHeartbeat`), so a silent or reaped watch surfaces a "not active" error instead
  of hanging forever - previously the default KV watch requested no idle heartbeat, so total silence
  was indistinguishable from an idle stream and no watchdog armed. The ordered-consumer recreate is
  serialized by an in-flight guard so the dispatch-handler (sequence-gap / tail-gap) and watchdog-timer
  paths cannot both drive a recreate at once and orphan a transient ephemeral consumer, and a
  successful recreate clears the miss latch and rebases the silence clock so the watchdog re-arms for
  the new consumer - a replacement reaped again before its first heartbeat is recovered rather than
  leaving the watchdog wedged. The watchdog also rebases (and neither fires nor cancels) while the
  connection is mid-reconnect, so it survives a transient reconnect.
- `[bugfix]` `flush()`, `drain()`, `drainSubscription()`, and `rtt()` now correlate PONGs to
  their PINGs through a FIFO slot queue (nats.go `nc.pongs` parity) instead of shared booleans
  that ANY PONG cleared (#117). Previously a stale PONG - one answering an earlier heartbeat
  PING whose bounded self-read timed out without consuming it, or a previously timed-out flush's
  PING - satisfied the wait immediately, so `flush()` returned before the server had processed
  the writes issued after that older PING, and `drain()`/`drainSubscription()` closed or dropped
  state with in-flight MSGs still unread: silent loss on the documented lossless path. A
  concurrent flush timing out also cleared the shared flag, releasing sibling flushes that had
  seen zero pongs. Now every outbound PING (heartbeats included, as placeholder slots) occupies
  one queue position, the PONG handler completes the oldest slot (TCP preserves PING/PONG
  order), a timed-out flush leaves its slot queued so its late PONG cannot release a later
  waiter, and every epoch end (reconnect handshake, terminal close) errors out all parked slots
  so a flush caught mid-reconnect fails fast with `ConnectionException` instead of idling out
  its deadline against the new socket. The `maxPingsOut` liveness watchdog still resets on any
  PONG.
- `[bugfix]` Pull-consumer robustness (#153): the 409 pull statuses "Message Size Exceeds
  MaxBytes" and "Batch Completed" are now classified as pull-completion statuses instead of
  terminal errors, so an infinite `consume()` loop with `setMaxBytes()` survives an oversized
  pending head message and keeps pulling (nats.go excludes `ErrMaxBytesExceeded`/
  `ErrBatchCompleted` from terminal handling); genuinely terminal 409s (Consumer Deleted,
  Consumer is push based) still stop the loop. Infinite mode also paces immediately answered
  empty pulls - a `setNoWait(true)` loop against an idle consumer used to busy-poll the server
  with an unthrottled 404/re-pull storm, and the non-terminal 409s re-pulled just as hot. Each
  consecutive empty window now backs off with an escalating delay (10ms doubling, capped at
  500ms, reset on delivery), settling an idle consumer at about 2 pulls per second.
- `[bugfix]` Pull idle heartbeats (#153): `fetchBatch()`/`fetchNext()` now validate the ADR-13
  rule `idle_heartbeat <= 50% of expires` client-side and reject violations (and non-positive
  values) with a clear `InvalidArgumentException` instead of forwarding a value the server
  refuses. When idle heartbeats are requested, the fetch loop now tracks frame arrivals on the
  reply inbox (heartbeats included) and fails fast with a "missed idle heartbeats"
  `JetStreamException` once two heartbeat intervals pass in silence (nats.go `ErrNoHeartbeat`
  parity) - previously status-100 frames were discarded untracked and a dead server/route left
  the fetch waiting out the full `expires`+grace deadline. A partial batch collected before the
  silence is still returned.
- `[bugfix]` A reconnect now stays in `Connecting` until the subscription replay and the
  reconnect-buffer flush have completed, and flips `Open` (arming the ping timer) only then -
  nats.go RECONNECTING parity. Previously `connectOnce()` flipped `Open` before the replay ran,
  with three consequences (#148): a publish from a concurrent fiber during the replay wrote
  straight to the wire ahead of the buffered earlier publishes, inverting per-publisher ordering
  and breaking JetStream `Nats-Expected-Last-Subject-Sequence` chains; a replay-leg failure left
  `state = Open` plus an armed ping timer on a dead socket for the whole backoff window, so user
  publishes surfaced spurious write errors instead of buffering; and the replay's own poll reads
  could collide with a user read admitted by the premature `Open` (Amp `PendingReadError`),
  aborting an otherwise-successful attempt. Publishes issued during the replay window keep
  buffering and the flush now drains in a loop, so frames appended mid-flush still go out - in
  publish order - before the connection opens. The heartbeat self-read additionally re-checks the
  state after its PING write, so a tick that raced into a recovery cannot read against the
  recovery's socket (#148).
- `[bugfix]` A message handler throwing during the post-recovery delivery drain no longer closes
  a healthy, fully recovered connection: the exception used to escape `recoverConnection()` into
  its callers' failure handling, so the heartbeat paths (`pingTimerTick()` maxPingsOut escalation
  and `consumeHeartbeatResponse()` peer-closed recovery) flipped a SUCCESSFULLY recovered
  connection to Closed on a live socket - with no Closed event and no runtime-state release -
  and `publish()`'s write-failure retry surfaced an unrelated handler exception for a frame that
  was neither written nor buffered. Handler exceptions from that drain are now contained inside
  `recoverConnection()` and reported through the error listener (nats.go parity: handler errors
  during post-reconnect delivery are async errors, not connection failures); genuine recovery
  failures (exhaustion, auth, reconnect disabled) still throw unchanged (#144).
- `[bugfix]` `connect()` no longer races an in-flight recovery (nats.go `conn.mu` parity). Calling
  `connect()` while a recovery was mid-flight (backoff, dial, or handshake) started a second
  concurrent `connectOnce()` chain against the same transport and parser; the recovery loop's next
  attempt then closed the healthy socket the user's `connect()` had just established and replayed
  only the pre-outage subscriptions, silently losing every subscription created on the new epoch
  (a runtime repro observed 4 dials for one outage). `connect()` now joins the in-flight recovery
  and shares its outcome, a concurrent `connect()` awaits the first dial instead of dialing in
  parallel, and `connect()` during `drain()` throws `ConnectionException`
  (`Cannot connect: drain in progress`) instead of dialing into the teardown. Re-entry semantics:
  a `connect()` called from a connection/error listener throws `ConnectionException` instead of
  joining - the listener runs inside the connecting/recovery fiber, so awaiting the join there
  could never complete (a permanent deadlock, including when the terminal `Closed` event is emitted
  by a failed initial connect); schedule supervision reconnects with `Revolt\EventLoop::queue()`
  and do not await the scheduled connect from inside the listener. The in-flight `connect()`
  deferred is now settled (and cleared) before every synchronous lifecycle emission
  (`Connected`/`Closed`), so it is never pending while a listener runs - closing the deadlock at its
  source in addition to the fiber guard. A dial that ends without the connection Open (a recovery or
  coalesced connect aborted by a concurrent `disconnect()`/`drain()`) throws `ConnectionException`
  ("aborted before the connection opened") - for the OWNER `connect()` (whose owned recovery was
  aborted mid-flight) exactly as for joiners, so an aborted owner no longer resolves as success on a
  Closed connection. The reverse direction is guarded too: `recoverConnection()` ignores recovery
  requests from stale failure continuations (a write/read that suspended before a terminal close and
  resumed failing later) while a user `connect()` is dialing - but that guard now also requires the
  state not be Open, so a genuine live-epoch failure while a `Connected` listener is still parked
  starts a recovery instead of being swallowed onto a dead socket. The `closing = false` reset also
  moved onto the fresh-dial path only, so a `connect()` racing a concurrent `disconnect()` can no
  longer disarm the user's close intent and let the recovery re-open a connection the user just
  closed - close intent wins and the connection stays Closed. A manual `connect()` after a
  terminal close (exhaustion, reconnect disabled, auth failure, user close) still starts a clean
  epoch exactly as before (#145).
- `[bugfix]` Ordered consumer: a `TimeoutException` or `ConnectionException` from the best-effort
  `deleteConsumer()` leg of a recreate (sequence-gap or heartbeat tail-gap recovery) no longer
  bypasses the create-retry loop and permanently silences the consumer. Those exceptions extend
  `NatsException`, not `JetStreamException`, so the delete-leg catch missed them and the failure
  fell straight into the outer containment - the terminal "recreate failed" error was emitted with
  zero create attempts made, and no consumer, heartbeat, or message would ever arrive on the
  deliver inbox again. The delete leg now tolerates any failure (a timed-out delete may well have
  succeeded server-side), so control always proceeds to the create attempts and the terminal error
  is emitted only when the create leg itself is exhausted (#151).
- `[bugfix]` The reconnect-disabled terminal path in `performRecovery()` now closes the transport
  best-effort and releases runtime state, matching every other terminal transition to Closed
  (the #127/#133 invariant). Previously the socket stayed pinned open and
  `subscriptions`/`subscriptionMeta`/`pendingMessages` (handler closures and payload bytes)
  survived the close, so a later manual `connect()` could deliver frames carrying the dead
  epoch's sids to stale handlers. The `Reconnect is disabled` exception and Closed-event
  semantics are unchanged. `bufferFrame()` additionally refuses publishes once the state is
  Closed, so a publish racing a terminal path's transport-close await fails loudly with
  `Connection is not open` instead of buffering bytes the state release would silently
  discard (#146).
- `[bugfix]` A parse failure no longer silently discards valid sibling frames from the same
  chunk: `ProtocolParser::push()` retains frames parsed before a mid-chunk `ProtocolException`
  (drained via `takeParsedFrames()`, or prepended to the next `push()` result), and every read
  path that can hit one - `processIncoming()`, the heartbeat self-read, and the reconnect
  subscription-replay poll - now delivers them to their handlers instead of dropping them: their
  bytes are already consumed, and core NATS never resends, so they were permanently lost. The
  `ProtocolException` surfaces through the error listener on each of those paths instead of
  vanishing, and on `processIncoming()` the error emission and the recovery both run even when a
  handler throws while the recovered frames are delivered (the handler's exception still
  propagates afterwards, matching #128's containment semantics) (#147).
- `[bugfix]` `BatchPublisher::commit()` now pre-flights the INFO-advertised server version and
  throws `UnsupportedFeatureException` BEFORE anything reaches the wire when the connected server
  is parseably older than 2.12 (#152). Previously the pre-2.12 detection (#130) fired only on the
  reply to the batch START request - by then the old server had already durably stored the start
  message as a plain publish, leaving one orphan message in the stream on the "nothing" path of an
  all-or-nothing API (and a silent plain publish for a single-message batch). Version parsing is
  numeric-prefix (nats.go-style): pre-releases such as `2.12.0-beta.1` count as 2.12 and proceed;
  unparseable versions (proxies, custom builds) and mixed-version clusters where the JS leader is
  older than the connected server still fall through to the reply-shape detection as defense in
  depth. Also documents that `BatchPublisher::MAX_MESSAGES = 1000` is ADR-50's server DEFAULT
  batch limit, not a protocol constant - the server's error reply stays authoritative for the
  configured limit.
- `[bugfix]` JetStream errors: the API error envelope's stable `err_code` (ADR-1) is now parsed at
  every envelope decode site and exposed via the new `JetStreamException::getErrCode()` accessor
  (null when the envelope carried none or the error is client-side). Error-kind discrimination now
  matches `err_code` first - `createOrUpdateStream()` detects "stream name already in use" by 10058
  and KV `createKey()` detects "wrong last sequence" by 10071 - falling back to description
  substrings only when `err_code` is absent (old servers), so server rewording no longer breaks the
  create-or-update and exclusive-create semantics. The previous KV check compared `getCode()` to
  10071 and could never match a real server rejection (those carry the HTTP-like 400). The synthetic
  "Key already exists" exception now carries 400 in `getCode()` and 10071 in `getErrCode()` instead
  of minting an API err_code into the transport-code slot (#154).
- `[bugfix]` `$JS.ACK` reply-subject parsing now tolerates trailing tokens:
  `JsMessageMetadata::fromMessage()` and `extractStreamSequence()` accept the expanded v2 form
  with >= 11 tokens
  instead of exactly 11 or 12, anchoring field offsets from the front and ignoring everything
  after index 10 - nats.go parser parity, whose comment warns the parser must not be strict about
  trailing tokens because servers may append them (the 12th, a random suffix, was itself a later
  addition). A future server appending a 13th token no longer nulls out every delivery's metadata.
  The exact 9-token v1 form is unchanged. Additionally, the ordered consumer's null-metadata path
  is no longer a silent trapdoor: a reply subject that claims the `$JS.ACK` form but cannot be
  parsed used to route the message to the handler with BOTH the consumer-sequence gap check and
  the stale-consumer filter bypassed and zero errors emitted - the entire ordering guarantee
  evaporated quietly. Such a delivery now surfaces a descriptive `JetStreamException` through the
  error listener - once per consumer instance, re-armed on recreate, so an unparseable stream
  cannot become an error storm - and the message is still delivered best-effort (at-most-once,
  ordering unverified), matching the previous delivery behavior. Parse failures deliberately do
  not trigger a recreate: the replacement consumer would produce the same unparseable form, and
  the tolerant parser makes the branch nearly unreachable. Absent or plain (non-ack) reply
  subjects keep the silent best-effort path (#155).
- `[bugfix]` WebSocket inbound performance for large frames (#164): a single WebSocket frame larger
  than 65 535 bytes (which must carry a 64-bit length) used to grow `$readBuffer` with a
  payload-sized `.=` copy per 8 KiB socket read - superlinear, the #140 issue the TCP path already
  fixed. Such a frame is now sized from its header (`WebSocketFrameCodec::frameRequiredBytes()`) and
  its remaining reads accumulate in a chunk list joined exactly once by a new spanning-consume path,
  so each payload byte is copied a bounded number of times regardless of how many reads it spans;
  fragmented-message continuation payloads likewise collect in a list joined once at the final
  fragment. Frames that fit a 7-bit or 16-bit length (<= 65 535 bytes, including every fragmented
  message's per-frame payloads) are inherently bounded and keep the pre-#164 `.=` + batch-decode
  path byte-for-byte - they are never even sized - so the small-frame and fragmented paths are
  unchanged. Measured through `readLine()` fed exact 8 KiB reads by an in-memory socket (medians of
  21 interleaved before/after process rounds, min-of-5 each, one pinned core, WSL2/PHP 8.5): a single
  10 MB frame drops ~22 ms to ~7 ms (~3x); a 10 MB message fragmented into 8 KiB continuation frames
  (~20 ms) and a flood of 100k 200-byte frames (~190 ms) stay at parity (within run-to-run noise).
  Outbound masking is unchanged - measured allocator-bound, with no stable win on PHP 8.5 (dropping
  the `substr` trim was within noise at 1 MB and slower at 8 KiB; PHP-level 8-byte-word masking was
  ~15x slower than the native whole-string XOR). Wire behavior is unchanged, pinned by the existing
  decode, reassembly and fragment-bound tests plus new multi-chunk, partial-next-frame,
  ping-between-fragments and read-boundary torture pins (a 64-bit-length frame and a large masked
  frame in one-byte reads, and continuation-frame headers split across reads).
- `[bugfix]` Auto-unsubscribe armed with `max <= already-delivered` while a backlog was still
  queued no longer over-delivers exactly one message past the cap (#156). `drainPendingForSid()`
  enforced the delivery cap only AFTER delivering each message, so when `unsubscribe(sid, max)` was
  armed with `max` at or below the current delivered count while messages were still queued,
  `completeAutoUnsubIfSatisfied()` deferred (backlog not yet drained) and the next drain dequeued and
  delivered one more message before the post-delivery check fired - contradicting the #112 contract
  that the handler is never invoked more than `max` times. The drain loop now checks the cap at the
  TOP of the loop, before dequeuing/delivering, and drops the sid without invoking the handler once
  more when `delivered >= max` (nats.go `AutoUnsubscribe` gates delivery, not the aftermath). The
  existing post-delivery check is preserved for the #112 backlog-flush case where `max > delivered`
  on entry.
- `[bugfix]` `SlowConsumerPolicy::Error`'s overflow drop is now always observable, and its
  auto-unsubscribe accounting is documented and self-consistent (#159). The overflowing message is
  still lost - core NATS does not resend it - but the loss was surfaced inconsistently: the
  polling-queue (`SubscriptionQueue`) variant threw WITHOUT counting the drop through
  `droppedCount()`/the error listener, so it did not match the observable-drop contract the
  DropOldest/DropNewest paths honour (#134). The polling-queue Error variant now counts the drop via
  `droppedCount()` and reports it through the error listener before it throws. Accounting is
  deliberately unchanged: the dropped message still counts toward the auto-unsub max at intake
  (`receivedCounts`), exactly like DropOldest/DropNewest and exactly as the server counts a message
  the moment it writes it (#112). A client-side drop must NOT roll that count back - doing so would
  leave `receivedCounts` short of the max forever, so `completeAutoUnsubIfSatisfied()` would never
  fire and the subscription would leak (the #112 invariant). The documented, tested semantics: an
  Error-policy auto-unsub can therefore complete having delivered fewer than `max` messages, with each
  overflow surfaced loudly rather than lost silently. On the push (handler) path the overflow is
  surfaced exactly once - the thrown `ConnectionException` is rethrown to the caller (or, for a second
  frame in the same chunk, reported through the error listener) by `dispatchFrames()` (#158); the
  connection layer no longer also emits that same exception, which would have reported it twice. The
  per-subscription pending bound is message-COUNT based only; there is no byte-based bound (nats.go's
  pending limits are both count- and byte-based).
- `[bugfix]` `requestMany()` no longer returns more than `maxResponses` when replies coalesce into a
  single TCP chunk (#160). The inbox collector appended every reply unconditionally while the wait
  loop enforced the cap only between reads, so one `processIncoming()` dispatching several replies
  could return more than requested. The collector now caps at `maxResponses`, dropping replies past
  the limit; stall/total-deadline semantics for under-cap collections are unchanged.

## [2.5.1] - 2026-07-11

### Fixed

- `[bugfix]` Static analysis: `JsMessageMetadata::fromMessage()` rewritten with literal token
  offsets per `count()` branch so PHPStan 2.2.5's stricter array-shape inference can prove every
  access (the shared base-offset arithmetic tripped `offsetAccess.notFound` and failed CI's
  `Unit + Static` jobs; CI resolves dependencies fresh and picked up 2.2.5 while the local gate
  ran an older vendor). Byte-identical parsing behavior, pinned by the existing 9/11/12-token
  ack-subject tests.

## [2.5.0] - 2026-07-11

### Added

- `[feature]` `NatsOptions::$pingIntervalSeconds` now accepts `int|float`, so sub-second heartbeat
  intervals (e.g. `0.05`) are expressible; integer values keep working unchanged (backward
  compatible for every existing caller) and `0` still disables the heartbeat. The underlying
  timer (`EventLoop::repeat()`) and the heartbeat read budget already operate on floats, so no
  runtime behavior changes for existing configurations. Motivation (#142): the integer floor made
  1 s the minimum observable interval, forcing every ping-timer unit test to wall-clock-sleep past
  it (~10 s per unit-suite run); those tests now run 50 ms intervals with the same deterministic
  assertions.

### Changed

- `[bugfix]` Concurrent requests no longer poll: a request waiting while another fiber owns the
  socket read used to wake every 1ms (allocating a Future per wakeup), so N concurrent requests
  burned O(N x 1000/s) wakeups - a KV `getAll()` on a large bucket became CPU-bound. Waiters now
  park on their reply or on a read-slot-release signal and one of them takes over the read pump
  when it frees; `requestMany()` waiters additionally wake per delivery so stall detection is
  unchanged. Measured with 200 concurrent requests idling 300ms against a live server: CPU time
  dropped from 425ms (a full core for the whole window) to 92ms (#135).
- `[bugfix]` The outbound hot path no longer stacks 2-3 `async()` fiber hops per message (#136):
  transport `write()` runs inline in the caller's fiber and returns an already-resolved future
  (failures still surface through the future, never as a synchronous throw, so the #124 error
  contract is unchanged), and the JetStream ack/nak/term/inProgress helper returns the publish
  future directly instead of wrapping a third fiber around a replyTo null-check. Publish and
  request-target subjects are also memoized after first validation (bounded at 512 entries, full
  reset at the cap) so repeat publishes skip the regex + per-token scan; per-request reply inboxes
  are never cached. Measured against a live server: 50k serial small publishes went from ~4.5s
  wall / ~4.4s CPU (~11k msg/s) to ~3.9s / ~3.8s (~13k msg/s), and a 5k serial JetStream
  fetch-and-ack loop from ~1.0s wall / ~0.9s CPU (~4.9k acks/s) to ~0.5s / ~0.5s (~9.2k acks/s).
- `[bugfix]` Reconnect subscription replay no longer has a ~5ms x N-subscriptions latency floor
  (#137): `resubscribeAll()` used to issue an awaited SUB write (plus the optional #112 UNSUB
  re-arm) and then a ~5ms drain poll per sid - with verbose off the server sends nothing after a
  successful SUB, so the poll always ate its full timeout serially inside the reconnect critical
  section (publishes buffering, no dispatch; 500 subscriptions added ~2.5s of blackout). The
  replay is now coalesced into O(1) transport writes - one buffer with every SUB (+UNSUB re-arm)
  frame, byte-identical on the wire and in order - followed by a single bounded drain poll, so
  prompt `-ERR` responses still abort the attempt exactly as before.
- `[bugfix]` `BatchPublisher::commit()` no longer sends each intermediate batch message as its own
  awaited write (#138): per ADR-50 only the start and commit legs are request/reply, yet every
  intermediate paid ~1 syscall + 2 fiber hops, so a max-size 1000-message commit burned ~998
  unnecessary syscalls of pure overhead in the library's designated atomic bulk API. The
  intermediates are now coalesced into bounded segments (at most 512 KiB per segment, so a
  1000 x 1MB batch never concatenates into a ~1GB string) and each segment goes out as ONE
  transport write - byte-identical HPUB frames, same order, same batch headers. Per-message
  subject and `max_payload` validation now runs for the WHOLE block before any intermediate hits
  the wire, and the start/commit request legs (including the #130 pre-2.12 guards) are unchanged.
  Measured against a live 2.12 server: a 1000-message commit (128-byte payloads) dropped from
  ~55ms to ~21ms median wall time.
- `[bugfix]` JetStream push deliveries (push consumers, ordered consumers, KV watches) no longer
  pay a per-message `async()` hop and duplicate parsing (#139): the push control-frame check runs
  synchronously in the dispatch fiber instead of spawning a fiber per message (a Future allocation,
  an event-loop hop, and at least one added tick of delivery latency each) and skips header parsing
  entirely for header-less data messages - the overwhelmingly common delivery; the rare
  flow-control/stalled acks are still sent, byte-identical. The ordered consumer reads
  `Nats-Last-Consumer` from the control frame's already-parsed headers instead of re-parsing the
  block, NATS header blocks are split with `explode("\r\n")` instead of `preg_split` (same pieces,
  cheaper), and a subscription's pending-message queue is now reused across drains instead of being
  freed when emptied and reallocated on the next delivery. Measured with an ordered consumer
  receiving 20k small messages from a live server (window includes the 20k publishes): median wall
  2.20s -> 1.95s and CPU 2.06s -> 1.80s, ~9.1k -> ~10.3k msg/s end to end.
- `[bugfix]` Inbound micro-overheads roundup (#140), behavior-identical: MSG/HMSG control lines are
  tokenized with a plain `explode(' ')` fast path instead of `preg_split('/\s+/')` per message
  (~5-10% of a core at 100k msg/s), falling back to the regex whenever the space-split result shows
  an empty token, a wrong token count, or embedded non-space whitespace - so tab/multi-space
  leniency is preserved bit-for-bit; the per-frame `strtoupper()` verb fold is skipped when the verb
  already matches a canonical upper-case form. While a large MSG/HMSG payload is incomplete,
  subsequent socket chunks accumulate in a list joined once on completion instead of growing the
  buffer with a copy per 8 KiB read (measured ~2-3x constant-factor overhead on multi-MB frames).
  Service endpoints no longer parse request headers (observer context / correlation id) on every
  request: the context is resolved lazily and memoized, so a service with no observers and a
  successful handler skips the parse entirely, while observer events and error-reply correlation
  ids are unchanged.

### Fixed

- `[bugfix]` `SubscriptionQueue` slow-consumer drops are no longer silent (#134): an overflow under
  `DropOldest`/`DropNewest` now reports through the client's `errorListener` and logger with the
  same "Slow consumer on sid ..." debug-level signal the connection layer already emits for its own
  queue, and a new monotonic `SubscriptionQueue::droppedCount()` lets polling consumers detect
  delivery gaps. This matters because for `subscribeQueue()` consumers the connection queue drains
  into this second-level queue on every `processIncoming()` cycle - so this is where real overflow
  lands, and it previously produced no signal anywhere. The `Error` policy is unchanged (it already
  throws).
- `[bugfix]` Resource-release roundup from the July review (#133): `keyValue()`/`objectStore()`
  no longer memoize bucket wrappers - the per-name cache had no eviction (not even
  `deleteBucket()`), so a long-lived client touching many bucket names (e.g. one per tenant)
  retained one wrapper per name forever; the wrappers are all-readonly value objects, so a fresh
  instance per call is behavior-equivalent. Every terminal connect/reconnect failure (failed
  initial connect, exhausted or auth-aborted recovery) now closes the transport socket
  best-effort, mirroring `disconnect()` - previously the last failed attempt's socket stayed
  pinned by the transport until the client object was GC'd. `BatchPublisher::commit()` releases
  the staged payloads once the batch is sent - a retained committed publisher (e.g. kept keyed by
  `batchId()` to correlate acks) previously pinned up to 1000 full payloads for its lifetime; as
  a consequence, `count()` now returns 0 after `commit()` (it previously kept reporting the
  staged total).
- `[bugfix]` `fetchBatch()`/`fetchNext()` no longer silently drop unrecognized `$pull` fields: an
  unknown key now throws a `JetStreamException` naming the offending key and the supported set, so
  a typo (or a field this client does not implement) can no longer make the caller believe the
  option took effect - a bug-driven behavior change treated as a bugfix. The ADR-13 `idle_heartbeat`
  field (nanoseconds) is now an accepted pull-request field and reaches the wire (the fetch loop
  already absorbs the resulting status-100 heartbeat frames). Ordered consumers additionally pin
  `num_replicas: 1` (ADR-17 / nats.go `ordered.go` parity), so an interest-retention stream's
  replica count is no longer inherited by the ephemeral ordered consumer (#132).
- `[bugfix]` Spec-conformance corrections from the July review (#132): KV `create()` defaults now
  include `deny_delete: true` and `discard: new` (ADR-8 / nats.go `CreateKeyValue` parity; both
  stay user-overridable, `discard: new` expects NATS server 2.7.2+), so bucket revision history can
  no longer be deleted out from under other tooling and a full bucket rejects writes instead of
  silently evicting old keys. KV key validation is tightened to the ADR-8 rules (charset
  `[-/_=.a-zA-Z0-9]`, reserved `_kv` prefix rejected), so entries written from PHP can no longer be
  unreadable via nats.go/nats.java/the `nats` CLI. Publishing with headers against a server whose
  INFO advertises `"headers": false` now fails client-side with a clear `ConnectionException`
  (nats.go `ErrHeadersNotSupported` parity) instead of the server killing the connection on an
  unknown HPUB operation. The services framework `started` timestamp is now generated in UTC, so
  its RFC3339 `Z` suffix is truthful on non-UTC hosts (ADR-32) and `nats micro` shows correct
  uptimes.
- `[bugfix]` JetStream stream and consumer (durable) names are now validated client-side before
  being interpolated into a `$JS.API.*` subject, rejecting empty names and names containing
  spaces, tabs, CR/LF, `.`, `*`, `>`, `/` or `\` (nats.go `checkStreamName` / `checkConsumerName`
  parity). Previously a dotted name silently changed which API endpoint the request hit:
  `createConsumer('S', 'a.b')` was routed by the server as the filtered-create form (consumer
  "a", filter "b"), `getConsumer()`/`deleteConsumer()` for that name hit no API route at all and
  surfaced a misleading 503 "subject is not bound to a stream", and `directGetStreamMessage()` on
  a dotted stream name could be routed as `DIRECT.GET.<stream>.<last_by_subject>` and silently
  return data from a SIBLING stream (#131).
- `[bugfix]` Atomic batch publish no longer degrades silently to non-atomic storage on servers
  without batch support (pre-2.12): such a server treats `Nats-Batch-*` headers as opaque and
  acknowledges the batch start/commit as plain publishes, and `commit()` previously reported
  success while the "batch" had been stored message-by-message. The batch start now requires
  ADR-50's zero-byte ack (a normal PubAck aborts with `UnsupportedFeatureException` carrying the
  server version, before the remaining messages are published), and a multi-message commit ack
  must carry the batch id/count. The README "Server Version Requirements" section and the
  `JetStreamContext::batch()` docblock now describe when `UnsupportedFeatureException` can
  actually fire per feature class (#130).
- `[bugfix]` `NatsClient::subscribeQueue()` no longer silently drops messages delivered between
  the SUB hitting the wire and the `SubscriptionQueue` object being constructed: the subscription
  handler is registered before the SUB write (so the sid is immediately routable, and a concurrent
  read or the heartbeat self-read can dispatch for it while `subscribeQueue()` is still suspended),
  but the handler discarded anything arriving before the queue existed. Early deliveries are now
  buffered and replayed into the queue (through the normal cap and slow-consumer policy) (#129).
- `[bugfix]` An exception while handling one inbound frame no longer discards the frames already
  parsed from the same chunk. The parser has consumed the bytes, so an undispatched trailing frame
  was silently and permanently lost (core NATS does not resend) - e.g. a slow-consumer overflow on
  one subscription (Error policy) destroyed messages for healthy sibling subscriptions delivered
  in the same TCP chunk, and a failing PONG reply destroyed the messages behind the server PING.
  Dispatch is now contained per frame: every frame is handled, buffered deliveries are drained,
  and the first failure surfaces afterwards. The heartbeat self-read additionally reports a fatal
  frame (e.g. a server `-ERR`) through the `errorListener` instead of swallowing it whole (#128).
- `[bugfix]` Every terminal transition to `Closed` now releases per-connection runtime state
  (subscription registry and handler closures, queued messages, counters, parser bytes, reconnect
  buffer) - previously only user `disconnect()`/`drain()` did. An exhausted reconnect or a
  terminal auth failure left everything referenced; worse, calling `connect()` again on the same
  instance silently believed it was subscribed (nothing was re-SUBbed) and a later automatic
  recovery would resurrect the dead epoch's sids as ghost subscriptions, duplicating deliveries
  into stale handler closures. Subscriptions now never survive a terminal close: re-`connect()`
  starts from a clean slate and the application re-creates its subscriptions (nats.go parity,
  documented on `connect()`) (#127).
- `[bugfix]` A `NatsConnection` abandoned without `disconnect()`/`drain()` is now garbage-collectable:
  the ping timer's repeat closure previously bound `$this` strongly, so the event loop rooted the
  whole connection graph forever - the open socket, every subscription handler closure, and all
  buffers leaked per abandoned client, and the zombie timer kept PINGing and even delivering
  messages to abandoned handlers (stealing queue-group deliveries from live workers). The timer
  now holds the connection through a `WeakReference` and cancels itself once the application drops
  its last reference, and a new destructor cancels the heartbeat and closes the socket
  best-effort (#126).
- `[bugfix]` The reconnect handshake now starts from a clean protocol parser. Previously the new
  connection's INFO/PONG bytes were fed into the parser state left by the dead connection; after a
  drop mid-message-payload the pending frame swallowed each attempt's INFO as phantom payload
  bytes, so every reconnect attempt failed with "Expected INFO during connect" and the client
  closed permanently against a healthy server once the attempt budget was exhausted (#125).
- `[bugfix]` Transport `write()` on a closed/never-connected socket now throws
  `TransportClosedException` instead of silently succeeding (both TCP and WebSocket transports).
  Previously a publish, JetStream ACK, or flow-control reply racing a reconnect (or a concurrent
  `disconnect()`) could hit the nulled socket and report success while sending nothing - a silent
  message loss. The connection now also leaves the `Open` state before recovery's first await, so
  publishes issued while the dead socket is being torn down are routed into the reconnect buffer
  and replayed after the new handshake instead of racing the closing socket (#124).
- `[bugfix]` A reconnect-flush failure no longer silently destroys publishes accepted during the
  reconnect window: `flushReconnectBuffer()` cleared the buffer before awaiting the write, so a
  socket failure during the flush left the next (successful) attempt with nothing to replay while
  every affected `publish()` had already reported success. The buffer is now cleared only after
  the flush write succeeds (a partially transmitted flush may duplicate frames on the retry -
  duplication beats loss, matching nats.go pending-buffer semantics), and exhausting reconnect
  attempts with a non-empty buffer reports the abandoned bytes through the `errorListener` and
  clears the buffer so a later manual `connect()` cannot replay frames from a dead epoch (#123).
- `[bugfix]` `unsubscribe($sid, $max)` (auto-unsubscribe) sent `UNSUB <sid> <max>` but dropped the local
  handler immediately, so every message the server legitimately kept delivering up to the max was
  silently discarded. The handler now stays registered until `$max` total messages have been received
  (nats.go `AutoUnsubscribe` parity), a reconnect re-arms the server with the remaining allowance, and
  reaching the max removes the subscription locally (#112). The accounting is anchored to messages
  **received** (not delivered), so a message dropped by the slow-consumer policy still advances toward
  the max exactly as the server counts it - without this, under the default `DropOldest` policy the
  subscription could stall below its max forever (a permanent leak) and a reconnect would re-arm and
  over-deliver live messages past the intended max. Handler delivery is separately capped at the max so
  a batched-in or replayed extra frame is never over-delivered, and arming while a reconnect is in
  flight now defers to recovery instead of destroying the subscription (which would have silently lost
  the remaining armed deliveries).
- `[bugfix]` Subscription state no longer leaks on failure paths: `unsubscribe()` releases local state
  even when the connection is not open or the UNSUB write fails (previously it threw first, leaking the
  entry and its handler closure, and `resubscribeAll()` would revive dead inboxes as ghost
  subscriptions), and `subscribe()` rolls its registry entry back when the SUB write fails.
  `unsubscribe()` on a connection that is not open now cleans up silently instead of throwing
  `ConnectionException` - a bug-driven behavior change treated as a bugfix (#116).
- `[bugfix]` A failed ordered-consumer recreate (after a sequence gap) is no longer silently swallowed:
  the create is retried up to 3 times with backoff and a terminal failure is surfaced through the
  configured `errorListener`, so the application learns the consumer went permanently silent instead of
  waiting on dead air forever (#114). Adds `NatsClient::options()` exposing the client's runtime options.

### Testing & CI

- `[docs]` New nightly mutation-testing workflow (`mutation-nightly.yml`, 03:17 UTC daily +
  manual dispatch): re-scores the whole unit-covered tree against the 90% MSI gate every day and
  uploads the Infection logs as artifacts, catching mutation-score drift between pushes. The
  `composer infection` script now disables Composer's 300 s process timeout, which killed any
  full mutation run mid-flight (the full sweep takes ~30 minutes; current score: 91% covered MSI
  over 5234 mutants at 100% mutation coverage).
- `[docs]` Test-suite hygiene roundup from the July review (#142), dev-only: deleted
  `testDirectGetBatchDelaysOnZeroFrames`, whose assertions could not fail for its stated purpose
  (the pacing delay was never observed) while burning ~1 s per run - the sibling
  `testDirectGetBatchReturnsEmptyArrayOnTimeout` keeps the empty/timeout path covered; test
  comments no longer pin production line numbers (several were already stale) and reference the
  method or branch by name instead - deliberate per-mutant line pins in `tests/Unit/Mutation`
  are exempt; the ping-timer unit tests use fractional 50 ms intervals instead of ~10 s of
  wall-clock sleeps (see the `pingIntervalSeconds` entry above); the behat exception steps
  compare via `is_a()` (instanceof semantics) instead of strict class-string equality, so
  introducing a more precise exception subclass no longer breaks scenarios.
- `[docs]` The reconnect path is now exercised against a live server (#141): a new
  `SeveringTransport` test decorator over the real `AmpSocketTransport` force-closes the live TCP
  socket mid-session, and two new integration tests (severing mid-idle and mid-traffic) assert the
  client observes the genuine EOF, reconnects, replays its subscriptions with a real SUB, and
  delivers post-reconnect traffic published from a second independent client. The four scripted
  "reconnect" tests that lived in the integration suite but never contacted the fixture (they ran
  only against injected fakes, so they were skipped by the local unit gate) were relocated to
  `tests/Unit/NatsConnectionTest.php`; one of them duplicated an existing unit test
  (`testProcessIncomingReconnectsAndResubscribesAfterReadFailure` covers the identical FlakyTransport
  script with stronger assertions) and was deleted instead. Dev-only - no runtime/library change.

### Documentation

- `[docs]` README fixes: the feature table now shows the real named arguments for KV tombstones
  (`delete/purge(..., tombstoneTtl:)` - the documented `ttl:` threw "Unknown named parameter"),
  and the Schedule FQCN in the scheduling note renders with single backslashes (the doubled
  `IDCT\\NATS\\...` form inside a code span displayed literally and broke copy-paste) (#143).
- `[docs]` `disconnect()` and plain `unsubscribe()` docblocks (connection and client facade) plus
  the README drain section now state that locally queued, undelivered messages are discarded
  (intentional nats.go `Close()`/`Unsubscribe()` parity) and name `drain()`/`drainSubscription()`
  as the lossless teardown paths (#134). No behavior change.
- `[docs]` The README "NATS Server Version Requirements" section now states the real server floor:
  core NATS works against any server, but all JetStream consumer helpers use the 2.9+ named
  `CONSUMER.CREATE` API with no fallback to the legacy `DURABLE.CREATE` form, so consumer
  management requires NATS 2.9+ (documented in the feature table; pre-2.9 servers fail with a
  generic 503, not an `UnsupportedFeatureException`) (#132).

## [2.4.1] - 2026-06-15

### Testing & CI

- `[docs]` Added mutation testing with [Infection](https://infection.github.io/) (`composer infection`,
  `scripts/run-mutation.sh`, `infection.json5`). 517 new unit tests under `tests/Unit/Mutation/` raised the
  suite's mutation score (Covered MSI) from **75% to 93%**, killing ~870 previously-surviving mutants. CI
  now enforces a **strict mutation gate** (a dedicated `mutation` job that fails the build below 90% MSI) -
  a quality bar on top of line coverage that catches assertions which pass but don't actually pin behavior.
  Mutation runs against the fast `unit` testsuite (no Docker); the remaining ~6% are mutants verified to be
  equivalent (no observable behavioral difference), documented in each test's reasoning rather than chased
  with meaningless assertions. Dev-only - no runtime/library change. **PHP 8.2 support is unchanged:**
  Infection requires PHP 8.3+ and is intentionally not in `require-dev`, so `composer install` still works
  on PHP 8.2; the `mutation` CI job runs on PHP 8.3 and installs Infection itself.

### Documentation

- `[docs]` Added a "PHP Support Policy" section to the README: the library follows PHP's official release
  schedule, so PHP 8.2 support will be dropped by the end of 2026 (when PHP 8.2 reaches end-of-life),
  raising the minimum to PHP 8.3 at that point.
- `[docs]` Pointed the "Made in the EU" badge at the renamed `ideaconnect/made-in-the-eu` repository.
- `[docs]` Replaced AI-style typography in the README, CHANGELOG, TESTS.md, and PHP docblocks/comments
  with plain ASCII: em/en dashes become `-`, the ellipsis character becomes `...`, and arrow / `x` for the
  arrow and multiplication signs. Functional non-ASCII (the `µs` duration unit, `©`, `§`) is left intact.

## [2.4.0] - 2026-06-14

### Added

- `[feature]` Protocol parser now recognizes operation verbs case-insensitively and accepts any whitespace
  (space or tab) between a verb and its arguments, aligning with the NATS wire spec. Real servers always
  send upper-case verbs, so this only adds leniency; argument/payload bytes are preserved verbatim. Resolves
  the long-standing README TODO (which has been removed).

### Fixed

- `[bugfix]` JetStream: `createStream()` no longer rejects an empty `subjects` list when a non-empty
  `sources` configuration is provided. A pure aggregate/sourcing stream legitimately has no subjects of its
  own (the server allows it); the client previously only exempted `mirror`, so creating a sources-only
  aggregate stream failed with "Stream subjects must not be empty...".
- `[bugfix]` Connection: a malformed async `INFO` frame is no longer allowed to throw out of the core
  `processIncoming()` read loop. Previously a non-JSON async INFO (corruption in flight, or a non-conformant
  server push) raised an uncaught `JsonException` that aborted the read cycle and skipped delivery of the
  `MSG` frames parsed from the same chunk. The runtime INFO decode is now contained (the bad update is
  skipped and surfaced via the error listener), mirroring the dispatch-containment principle from #97.
  Handshake INFO is still validated strictly and fails the connect on bad JSON.
- `[bugfix]` WebSocket transport: the frame decoder no longer re-slices the entire remaining receive buffer
  once per frame. A single read carrying many coalesced frames is now decoded in O(total bytes) instead of
  O(frames x bytes) by advancing a cursor and trimming once, improving throughput under bursty high-fanout
  traffic. Behavior (including the "leave an incomplete trailing frame buffered" contract) is unchanged.

### Documentation

- `[docs]` Added `TESTS.md` - a catalogue of every unit, integration, and Behat test with a one-line
  description of what it verifies - linked from the README's test baseline section.
- `[docs]` Added an `examples/` directory: one runnable, self-contained script per README example (42
  files), plus `scripts/run-examples.sh` which runs them all against dockerized NATS and reports
  pass/skip/fail - a gate that keeps the README examples honest. Linked from the README Usage section.
- `[docs]` Distributed Counter example now creates its backing stream with `allow_direct: true` (required
  because `counterValue()` reads via Direct Get); without it the documented example threw "no responders
  for $JS.API.DIRECT.GET". Prose updated accordingly.
- `[docs]` Each `examples/*.php` script now opens with a file-level intro docblock describing exactly what
  it does and which README section it mirrors.
- `[docs]` Every example heading in the README now carries a "Runnable example" pointer linking to the
  matching `examples/*.php` script, so each documented feature is one click from a runnable, verified file.
- `[docs]` `scripts/run-examples.sh` now defaults `NATS_NKEY_SEED` to the dev seed trusted by
  `build/nats/nkey.conf`, so `auth-standalone-nkey.php` runs as a real functional test in the dev stack
  instead of self-skipping. With this, all 42 examples pass against the full dockerized stack.
- `[docs]` CI now runs every example as a required gate (a dedicated `examples` job in
  `.github/workflows/ci.yml` that boots the full dockerized stack and runs `scripts/run-examples.sh`).
  The runner gained an `EXAMPLES_STRICT` mode (used by CI) that treats a skipped example as a failure, so
  the build fails unless every example actually executes and passes.

## [2.3.0] - 2026-06-13

### Security

- **Credential exposure via a configured `tlsContext` (#95).** Versions before 2.3.0 could transmit the
  CONNECT frame - which carries the configured credentials (token / user-password / JWT signature / NKey
  signature) - in **cleartext** when a `NatsOptions::$tlsContext` was supplied but `tlsRequired` was off,
  the DSN used the `nats://` scheme, and the server's INFO did not advertise `tls_required`. The TLS-required
  check ignored `tlsContext`, so the upgrade and the cleartext fail-safe were both skipped. Fixed: a
  configured `tlsContext` now forces the TLS upgrade (and fails fast if TLS cannot be established).
  **Upgrading is recommended for anyone using the `tlsContext` escape hatch.** See the Fixed entry below.

### Added

- `[feature]` Object Store: `ObjectStoreBucket::watch()` now accepts an optional `ObjectStoreWatchOptions`
  to select the delivery policy, mirroring the KeyValue watch matrix and the reference ObjectStore.Watch.
  With no options (`null`) the watcher stays updates-only (`deliver_policy=new`, unchanged). Passing an
  `ObjectStoreWatchOptions` instance opts into "snapshot then follow" - replay the current metadata of
  every existing object first, then live updates (`last_per_subject`, the reference default) - or full
  history (`includeHistory`) / explicit updates-only (`updatesOnly`). (#98)
- `[feature]` Services: a declared endpoint `schema` is now also surfaced in the standard `$SRV.INFO`
  response endpoint entries. ADR-32 stabilizes only PING/INFO/STATS, so spec-conformant tooling (nats CLI
  micro, nats.go) never queries the non-spec `$SRV.SCHEMA` verb; carrying the schema in INFO makes it
  discoverable. The `$SRV.SCHEMA` verb is retained for backward compatibility. (#101)

### Fixed

- `[bugfix]` Object Store: an object stored with empty/default metadata is now readable by the official
  NATS clients. Empty `metadata` was serialized as a JSON array (`"metadata":[]`), which the Go client
  rejects with "object-store meta information invalid" because it expects a `map`; the field is now omitted
  when empty (matching `omitempty`), restoring interoperability with the `nats` CLI / nats.go for the
  common default-metadata case. Verified live against the `nats` CLI. (#109)
- `[bugfix]` KeyValue: `watch()`'s `onCaughtUp` (end-of-initial-data) signal now fires on an empty or
  no-match bucket. Previously it could only fire from a delivered message reporting `num_pending = 0`, so
  with nothing to deliver it never fired and a caller blocking on it hung forever. The signal is now also
  derived from the created consumer's `num_pending` and fires immediately when the consumer starts with
  nothing pending. (#99)
- `[bugfix]` Protocol: the inbound MSG/HMSG frame bound is now coupled to the server's negotiated
  `max_payload` instead of a fixed 8 MiB. On a server with a raised `max_payload` (e.g. 16/32/64 MiB), a
  legitimately large message larger than 8 MiB was rejected as an oversized frame - throwing a
  `ProtocolException` that the connection turned into a reconnect, so the message was effectively
  undeliverable. The parser bound is raised from INFO (`max_payload` + a header-block margin, never below
  the historical 8 MiB), with a generous 64 MiB fallback when `max_payload` is unknown. (#94)
- `[bugfix]` Services: the endpoint success path no longer lets a `json_encode` failure escape the shared
  dispatch loop. A handler returning a value that cannot be JSON-encoded (binary / non-UTF-8 data, NAN/INF)
  previously threw a `JsonException` out of the subscription callback, aborting delivery for every
  subscription on the connection. The response publish is now guarded: an encode failure is recorded and
  answered with a controlled `HANDLER_ERROR`/500 reply (mirroring the handler-exception path), so one
  endpoint returning binary data can no longer take down the whole client's dispatch. (#97)
- `[bugfix]` KeyValue: `history()` no longer uses the throwing `messageMetadata()` path. A delivery
  lacking a parseable `$JS.ACK` reply subject (a control / non-conformant frame) is now skipped instead of
  throwing out of the shared dispatch loop - which would tear down delivery for every subscription on the
  connection (the same class fixed for `watch()` in #90) - and is no longer recorded as a bogus history
  entry. (#96)
- `[bugfix]` TLS: a configured `NatsOptions::$tlsContext` now correctly forces the TLS upgrade, matching
  its documented "treated as TLS-required" contract. Previously `requiresTls()` ignored `tlsContext`, so
  a `tlsContext`-only configuration over a `nats://` DSN to a server that did not advertise `tls_required`
  connected in plaintext and wrote CONNECT (carrying credentials) in cleartext. The credentials fail-safe
  now also covers this path, so a `tlsContext` whose handshake cannot establish TLS fails fast instead of
  leaking credentials. (#95)
- `[bugfix]` WebSocket: a corrupt permessage-deflate frame no longer emits an uncaught native `E_WARNING`
  from `inflate_add()`/`deflate_add()` before the typed `ProtocolException`. The warning is now suppressed
  (the return-value check already raises `ProtocolException`), so apps that promote warnings to exceptions
  get the intended `ProtocolException` instead of a generic `ErrorException` leaking from the codec. (#100)

### Documentation

- `[docs]` README "Reconnect Behavior" no longer wrongly states that publishes during reconnect are lost.
  It now documents the outbound reconnect buffer (publishes are buffered up to `reconnectBufferSize`,
  default 8 MiB, and flushed on reconnect; rejected only when the buffer is full, buffering is disabled, or
  the connection is closed/not reconnecting), with test citations. (#102)
- `[docs]` README "Configuration Option Mapping" table now lists the 11 previously-omitted `NatsOptions`
  fields - `connectionListener`, `errorListener`, `jwtProvider`, `tokenProvider`, `reconnectBufferSize`,
  `tlsContext`, `randomizeServers`, `retryOnFailedInitialConnect`, `webSocketHeaders`,
  `webSocketCompression`, `logger` - with types/defaults. `NatsOptionsTest::testDefaultsMatchDocumentedValues`
  now asserts these defaults too, keeping the table's "asserted by" claim accurate. (#103)
- `[docs]` README: new "WebSocket Transport" section (with an Index entry) showing how to wire
  `WebSocketTransport`, the `ws://` / `wss://` expectations, and the `webSocketHeaders` /
  `webSocketCompression` options. (#104)
- `[docs]` README: the Observability note now documents the typed `connectionListener` /
  `errorListener` closures, not just the PSR-3 logger. (#105)
- `[docs]` README: added a standalone-NKey authentication example (nkey + nonceSigner, no JWT) to the
  Authentication Options block. (#106)
- `[docs]` PHPDoc: `KeyWatchOptions` and `KeyValueBucket::watch()` now make clear that the
  last-per-subject "snapshot then follow" default applies only when a `KeyWatchOptions` instance is
  supplied; `watch()` called with `$options = null` is updates-only and replays nothing. (#107)
- `[docs]` PHPDoc: `ObjectInfo::$digest` is no longer described as "Server-provided" - it is the content
  digest recorded by the writing client and verified on read. (#108)
- `[docs]` Added a runnable performance baseline script (`scripts/benchmark.php`, request/reply + publish
  throughput) and a sample-results table in the README's Performance section.

## [2.2.0] - 2026-06-10

### Added

- `[feature]` Server-version awareness for version-gated features. Each feature's minimum NATS version
  is documented (PHPDoc `Requires NATS X.Y+` notes + a compatibility table in the README) and exposed
  programmatically via the new `IDCT\NATS\JetStream\FeatureSupport` registry
  (`FeatureSupport::requiredVersion('allow_atomic')` -> `"2.12"`).
- `[feature]` New `IDCT\NATS\Exception\UnsupportedFeatureException` (a subclass of `JetStreamException`).
  When a JetStream request fails because the connected server is too old for a feature (the server
  rejects the config field with `unknown field "X"`), the client now raises this typed exception
  carrying the feature, the required version, and the server's reported version - instead of an opaque
  error. The detection is **reactive** (derived from the server's own response on failure); there is no
  per-request version probe. `JetStreamException` is no longer `final` so it can be specialized
  (existing `catch (JetStreamException)` handlers are unaffected).

## [2.1.1] - 2026-06-10

Verification pass for the 2.1.0 roadmap features against a live NATS 2.12.9 server.

### Fixed

- `[bugfix]` Atomic batch publish (#8): the stream-config field is **`allow_atomic`** - the server
  rejects the previously-documented `allow_atomic_publish` with `unknown field`. Corrected the
  `batch()`/`BatchPublisher` docblocks (the `BatchPublisher` code itself was already correct and is now
  verified end-to-end: a 3-message batch commits 3/3 with the `batch`/`count` ack parsed).

### Added

- Live integration tests for atomic batch publish (#8) and batched/multi Direct Get (#13), and a
  connection-level regression test for the fragmented-INFO handshake (#2, the `trim($chunk)` bug fixed
  in v1.0.1 previously had no test). Full integration suite (76 tests) passes against NATS 2.12.9.

## [2.1.0] - 2026-06-10

NATS 2.11/2.12 client feature support (roadmap milestone, GitHub issues #4-#14). All changes are
backward compatible (new optional parameters / new methods); the one behavior change (#5 delete
markers) is bug-driven and flagged `[bugfix]`, so this is a minor release.

### Changed

- `[bugfix]` Honor JetStream subject delete-markers (`Nats-Marker-Reason`: MaxAge/Remove/Purge, ADR-43,
  issue #5). A server-written delete-marker is now treated as a tombstone rather than a live value:
  `KeyValueBucket::get()` returns a `PURGE` entry with a null value (was an empty-string `PUT`),
  `getAll()` omits the key, `watch()` emits a tombstone, and `ObjectStoreBucket::watch()`/`info()` skip
  the marker. **Behavior change** (flagged `bc-break` on the issue, but bug-driven so versioned as a
  bugfix): only reachable when a stream has `subject_delete_marker_ttl` set, which this client now also
  forwards as a create option.

### Added

- `[feature]` Batched / multi Direct Get (ADR-31, issue #13). New `directGetBatch()` collects a
  multi-response Direct Get stream (terminated by a 204 EOB or `Nats-Num-Pending: 0`), and
  `directGetLastForSubjects()` fetches the latest message for many subjects in one request via
  `multi_last`. Additive - the existing per-subject bulk paths (`getAll()`/`list()`) are unchanged
  pending live verification on a 2.11+ server.
- `[feature]` Pull-consumer priority groups and richer pull options (ADR-42, issue #7).
  `fetchBatch()`/`fetchNext()` accept a `$pull` array (`group`, `id`, `min_pending`,
  `min_ack_pending`, `priority`, `max_bytes`, `no_wait`); `PullConsumerIterator` gains
  `setGroup()`/`setPriority()`/`setMinPending()`/`setMinAckPending()`/`setMaxBytes()`/`setNoWait()`
  and transparently captures the `Nats-Pin-Id` and re-pins on a 423 stale-pin status. New
  `unpinConsumer()` (CONSUMER.UNPIN) and `pinIdOf()`; consumer-create validates `priority_groups`/
  `priority_policy`.
- `[feature]` Atomic (all-or-nothing) batch publish (ADR-50, issue #8). `JetStreamContext::batch()`
  returns a `BatchPublisher`: `add()` stages messages and `commit()` sends them with a shared
  `Nats-Batch-Id`, an incrementing `Nats-Batch-Sequence`, and `Nats-Batch-Commit: 1` on the final
  message, returning a single PubAck exposing the committed `batchCount`/`batchId`. Capped at 1000
  messages; an aborted batch surfaces as a `JetStreamException`. Requires `allow_atomic_publish` on
  the stream.
- `[feature]` Multi-subject consumer filters (issue #10, NATS 2.10+). The consumer-create methods now
  accept a `filter_subjects` array (via options), validated client-side and mutually exclusive with the
  singular filter subject (combining the two is rejected with a clear error instead of an opaque server
  rejection).
- `[feature]` Distributed counter CRDT (ADR-49, issue #9). `JetStreamContext::incrementCounter()`
  publishes a `Nats-Incr` delta (signed/unsigned integer string) and returns the new total;
  `counterValue()` reads the current value via Direct Get ("0" when absent). Values are handled as
  strings (decoded with `JSON_BIGINT_AS_STRING`) so arbitrary-precision counters are not truncated.
  The target stream must be created with `allow_msg_counter` enabled.
- `[feature]` `JetStreamContext::publish()` now accepts optional message headers - a generic
  `array $headers`, a `$msgId` (`Nats-Msg-Id`) for server-side de-duplication within the stream's
  `duplicate_window` (issue #11), and a per-message `$ttl` (`Nats-TTL`; requires `allow_msg_ttl` on
  the stream - issue #4). `KeyValueBucket::put()` takes an optional per-key `$ttl`, and
  `delete()`/`purge()` take an optional tombstone TTL. TTL values (integer seconds, a Go duration
  string, or "never") are validated client-side via the new `MessageTtl` helper.
- `[feature]` Recurring and cron scheduled publishing (ADR-51, issue #6). `Schedule::every()` builds
  an `@every <interval>` expression (from an integer number of seconds or a Go-style duration string)
  and `Schedule::cron()` validates/returns a 6-field (seconds-resolution) cron expression.
  `Schedule::predefined()` returns a predefined alias (`@daily`, `@hourly`, ...).
  `JetStreamContext::publishScheduled()` now accepts `@at` (with `Z` or a numeric RFC3339 offset),
  `@every`, cron, and the predefined aliases (previously only `@at` with `Z`) and emits the optional
  `Nats-Schedule-Source`, `Nats-Schedule-Time-Zone` (cron/alias only, rejected otherwise), and
  `Nats-Schedule-Rollup: sub` headers alongside the existing
  `Nats-Schedule`/`-Target`/`-TTL`. The target stream must be created with `allow_msg_schedules`
  enabled (e.g. `createStream(..., ['allow_msg_schedules' => true])`).

## [2.0.0] - 2026-06-07

Findings from a deep review against a live NATS 2.12 server (README correctness,
real-server behavior, bugs, and performance). Object Store interoperability with
the `nats` CLI, idle-connection heartbeat survival, and request-timeout recovery
were all verified working and are unchanged.

### Fixed

- `[feature]` Added `flush()` (on `NatsClient`/`NatsConnection`): sends a PING and waits for the
  server's PONG, confirming the server has processed everything written so far (e.g. a SUBSCRIBE
  before publishing a dependent request). Bounded by the request timeout.
- `[feature]` Service endpoints accept optional per-endpoint `metadata` (`addEndpoint(..., metadata:)`),
  advertised in the `$SRV.INFO` response per the NATS micro spec.
- `[bugfix]` The protocol parser now rejects a size/sid token that would overflow a PHP int (which
  `(int)` silently saturates to `PHP_INT_MAX`) as a `ProtocolException`.
- `[bugfix]` `NkeySeedSigner` now zeroes the raw seed and key-pair buffers (`sodium_memzero`) once the
  Ed25519 key is derived; `ProtocolCodec` fails fast if a configured nkey does not match the seed
  signer's public key. The service `started` timestamp now carries sub-second precision.
- `[bugfix]` `ObjectStoreBucket::putStream()` no longer recopies the buffer tail per chunk (O(n^2)
  for a producer block much larger than `chunkSize`); it advances a read offset and compacts once per
  block. The constructor now rejects a non-positive `chunkSize` (which made `put()`/`putStream()` loop
  forever) with a `JetStreamException`.
- `[bugfix]` Object Store `info()`/`get()`/`list()` now populate `ObjectInfo::revision` from the
  record's stream sequence (the `Nats-Sequence` Direct Get header, or the `seq` of the STREAM.MSG.GET
  fallback) instead of always leaving it null.
- `[bugfix]` KeyValue/Object Store bucket names are now validated (`^[A-Za-z0-9_-]+$`); a name with
  dots or wildcards would otherwise mis-scope the backing stream subjects.
- `[bugfix]` JetStream `publish()`/`publishScheduled()` now translate a no-responders reply into a
  `JetStreamException` (code 503) - e.g. publishing to a subject not bound to any stream - so a
  `catch (JetStreamException)` no longer misses it as a bare `NatsException`.
- `[bugfix]` `drain()` now always closes the socket and clears state even if a fatal frame surfaces
  mid-flush, instead of escaping the flush loop and leaving the connection wedged in `Draining`.
- `[bugfix]` Reconnect no longer deadlocks when a subscription handler publishes during recovery.
  Subscription-replay (`drainImmediateServerFrames`) previously delivered buffered messages to user
  callbacks while still inside the reconnect critical section; a callback that published and hit a
  write failure re-entered `recoverConnection()` and awaited the in-progress reconnect, hanging the
  recovery fiber. Buffered messages are now delivered after recovery completes (outside the critical
  section), so such a callback starts a fresh recovery instead of deadlocking.
- `[bugfix]` Microservice endpoint error replies now carry the NATS micro-spec `Nats-Service-Error`
  and `Nats-Service-Error-Code` reply headers (400 for validation, 500 for handler errors), so a
  generic client (Go `micro`, `nats` CLI) detects the failure by header instead of treating the
  header-less JSON error body as success. The description is collapsed to a single line so a crafted
  message cannot break header framing; the JSON error body is unchanged.
- `[bugfix]` KeyValue `getAll()` now paginates the STREAM.INFO subjects map (via `offset`) instead
  of reading a single page, so a bucket with more keys than the server's subjects-map cap is no longer
  silently truncated (mirroring Object Store `list()`), and it now throws on a STREAM.INFO API error
  instead of swallowing it into an empty result.
- `[bugfix]` Single-record Direct Get reads now fall back to the leader `STREAM.MSG.GET` path
  when Direct Get is unavailable (a stream with `allow_direct` disabled, or an older server). The
  no-responders error is translated to a clear `JetStreamException` (code 503); KeyValue `get()` and
  Object Store `info()`/`get()` (single-chunk fast path) then retry on the leader, so reads keep
  working on interop buckets (e.g. created by the `nats` CLI without `allow_direct`) instead of
  surfacing an opaque error.
- `[bugfix]` Ordered-consumer gap detection and `streamSequenceOf()` (used for KV/Object Store
  revision) now parse the 11-token domain-qualified ACK reply subject
  (`$JS.ACK.<domain>.<account>.<stream>...` without the trailing random token), not just the 9- and
  12-token forms. Previously the sequence fell through to null on JetStream-domain/leaf deployments,
  silently disabling gap detection and revision tracking there.
- `[bugfix]` The plaintext-credentials fail-safe now also covers the handshake-first TLS path. The
  guard that refuses to write CONNECT (which carries jwt/sig/nkey/user/pass/token) over a still-plain
  socket was gated on the non-handshake-first branch, so `tlsHandshakeFirst=true` combined with no TLS
  materials (and a `nats://` DSN) while the server's INFO advertised `tls_required` could leak
  credentials in cleartext. The fail-fast now runs whenever TLS is required and the handshake did not
  establish it, regardless of `tlsHandshakeFirst`.
- `[bugfix]` A JetStream flow-control STALL heartbeat is now answered. The server leaves the message
  reply empty for a stall and puts the flow-control reply subject in the `Nats-Consumer-Stalled`
  header value; the client previously detected the stall but published to the empty reply, so the
  ack never reached the server and a throttled ordered/flow-controlled consumer could stall
  indefinitely with no error surfaced. The normal `$JS.FC.` flow-control-request reply path is
  unchanged.
- `[bugfix]` Subscription dispatch is now non-reentrant per SID. If a handler awaits on the
  connection (e.g. an ordered consumer recreating itself during gap recovery), a heartbeat tick
  or a nested `request()` self-pump could previously re-enter the per-SID drain and deliver that
  subscription's next message on top of the still-suspended handler - corrupting ordered-consumer
  recovery state (stale by-reference sequence/consumer-name -> duplicate `deleteConsumer`/recreate)
  and causing overlapping/duplicate delivery. Delivery for a SID in flight is now deferred until
  the suspended handler returns (FIFO preserved); other SIDs stay deliverable so nested requests
  still complete.
- `[bugfix]` The CONNECT frame now advertises the resolved client library version (from the
  installed Composer package, with a constant fallback) instead of the stale hardcoded
  `0.1.0-dev`, so server `connz`/monitoring attributes traffic to the correct version.
- `[bugfix]` Header publishes (`publishWithHeaders()`, KV/Object Store metadata writes) now
  build and CR/LF-validate the header wire block once and reuse it for sizing and each write
  attempt, instead of re-running `toWireBlock()` two or three times per publish.
- `[bugfix]` The per-chunk subscription drain no longer rescans every subscription that has
  ever received a message: a drained (or undeliverable) per-SID queue is now released, so the
  drain stays proportional to the subscriptions with pending messages. This also fixed a latent
  coupling where the request UNSUB cleanup was gated on the (now-released) pending queue.
- `[bugfix]` Object Store `get()`/`getToCallback()` now fetch a single-chunk object with one
  Direct Get on its chunk subject, instead of creating, pulling from, and deleting a transient
  ephemeral consumer - turning the common small-object download from 4 round-trips into 1 (plus
  the metadata read). Multi-chunk objects still use the batched pull-consumer path.
- `[bugfix]` Object Store `put()` and `delete()` now run the previous-revision lookup
  concurrently with the chunk upload / tombstone publish, awaiting it only just before the
  best-effort chunk purge it feeds. Previously the lookup was a serial round-trip on the
  critical path before the first byte was written, roughly doubling small-object write
  latency. The lookup is issued at the same coroutine depth as the upload, so request
  ordering stays deterministic.
- `[bugfix]` Ephemeral push consumers (KeyValue/Object Store `watch()`, ordered consumer)
  now set an `inactive_threshold`, so the server reaps them once the subscription ends
  instead of leaking server-side consumers when a long-running app re-subscribes. An active
  subscription keeps the consumer alive; callers may override the threshold.
- `[bugfix]` `SubscriptionQueue` now bounds its polling backlog with
  `maxPendingMessagesPerSubscription` and the configured slow-consumer policy. Previously
  the connection's per-chunk drain emptied its (capped) queue into the unbounded polling
  queue, so a queue consumed slower than it was fed could grow until OOM.
- `[bugfix]` Object Store `list()` now paginates the meta-subject enumeration (via the
  STREAM.INFO `offset`) instead of reading a single page, so a bucket with more objects
  than the server's subjects-map cap is no longer silently truncated. The loop terminates
  on the first empty/duplicate page, so it is safe even against a server that ignores
  `offset`.
- `[bugfix]` `NatsHeaders::toWireBlock()` now rejects an empty/blank header name or one
  containing whitespace or a colon (previously only CR/LF were rejected, so a bad name
  silently produced a malformed/mutated block), and trims surrounding whitespace from
  values so they round-trip symmetrically with decode (which already trims).
- `[bugfix]` Object Store digest verification now compares the decoded digest *bytes*
  (tolerating missing base64url padding) with `hash_equals()`, instead of a string compare
  that spuriously rejected a byte-identical object whose metadata used unpadded base64url
  (some non-Go clients).
- `[bugfix]` KeyValue `get()`/`update()`/`delete()`/`purge()`/`getAll()` now wrap a
  malformed (non-JSON) reply in a `JetStreamException` instead of leaking a raw
  `JsonException`, consistent with `put()` and the rest of the API.
- `[bugfix]` The protocol parser now bounds an unterminated control line (no CRLF) to 1 MiB
  and raises a `ProtocolException` instead of buffering it without limit. `maxFrameSize`
  only bounded MSG/HMSG payloads (parsed after their control line completes), so a peer
  streaming bytes without a CRLF could drive the client to OOM.
- `[bugfix]` `Service::start()` is now atomic: if a subscribe fails partway, it rolls back
  the subscriptions already made and rethrows, instead of leaving the service
  half-initialized with the idempotency guard then masking a retried `start()` as a no-op.
  A separate `started` flag tracks completion.
- `[bugfix]` Microservice request observers now receive the terminal `request_end` event
  on the schema-validation rejection path too (previously only `request_start` ->
  `request_error` fired), so observer spans/timers/gauges are not leaked for rejected
  (often hostile) traffic.
- `[bugfix]` `NatsClient::service()` now validates the service name
  (`^[A-Za-z0-9_-]+$`) and requires a semantic version, failing fast instead of crashing
  `start()` mid-loop or over-subscribing to discovery subjects when the name contains a
  dot/space/wildcard.
- `[bugfix]` `request()` (and every JetStream/KV/Object Store call built on it) no longer
  throws a spurious `TimeoutException` when the reply is delivered in the same event-loop
  tick the deadline fires. The wait loop now checks for completion before the deadline, so
  a reply that lands as the timeout expires is returned instead of discarded.
- `[bugfix]` Ordered-consumer gap recovery now contains a failed consumer recreate
  (pruned/deleted stream, leadership change, transient timeout) instead of throwing out of
  the shared subscription dispatch loop and aborting delivery for every other subscription
  on the connection.
- `[bugfix]` `PullConsumerIterator` infinite mode (`setIterations(null)`) now survives a
  transient `409` (`Exceeded MaxAckPending`, `Leadership Change`, `Server Shutdown`,
  `Exceeded MaxWaiting`) and keeps polling, instead of treating every non-404/408 status
  as terminal and silently exiting forever. A terminal `409 Consumer Deleted` still stops
  the loop, and finite mode is unchanged.
- `[bugfix]` `drain()` no longer busy-spins (100% CPU) or hangs when the server never
  sends the flush PONG. The flush loop now yields between empty reads so its deadline can
  fire; previously a synchronous 0-frame read starved the event loop, so the
  `TimeoutCancellation` could never fire and `drain()` never returned.
- `[bugfix]` `drain()` no longer resurrects the connection on a read failure mid-flush. A
  peer close during drain previously triggered `recoverConnection()` - reconnecting and
  re-SUBscribing the very subscriptions `drain()` had just removed (and possibly
  re-delivering messages). `processIncoming()` now skips recovery while the connection is
  `Draining` and treats the read failure as end-of-flush.
- `[bugfix]` `CredentialsParser` now parses real `nsc`-generated `.creds` files. The
  marker regex required exactly five dashes on both the BEGIN and END lines, but the
  NATS toolchain emits five dashes on BEGIN and **six** on END, so
  `CredentialsParser::fromFile()` threw `Credentials file does not contain a NATS USER
  JWT block` on essentially every genuine credentials file - making the documented
  JWT-via-`.creds` auth path unusable. Both markers now accept five-or-more dashes.
- `[bugfix]` Object Store now stores a 0-byte object with `chunks=0` and publishes no
  chunk message, matching the official Object Store layout; previously it wrote one
  empty chunk and recorded `chunks=1`. `get()` of an empty object also returns
  immediately instead of blocking until the download batch expiry waiting for a chunk
  that never arrives.
- `[bugfix]` Object Store `get()` and `getToCallback()` now return `null` for a deleted
  (tombstoned) object, consistent with a missing object and the official not-found
  semantics; the tombstone metadata remains observable via `info()`. Previously `get()`
  returned an `ObjectData` with `null` data and `getToCallback()` returned the
  `ObjectInfo`.
- `[bugfix]` Microservice handler errors no longer leak the raw exception message to the
  requester: the reply carries a generic `Internal server error` under the
  `HANDLER_ERROR` code, while the full detail stays server-side (endpoint `lastError`,
  `$SRV.STATS`, and the `request_error` observer event).
- `[bugfix]` Service `$SRV.STATS` no longer emits the non-spec `requests`/`errors`
  aliases; only the spec-compliant `num_requests`/`num_errors` remain.
- `[bugfix]` Connections now disable Nagle's algorithm (`TCP_NODELAY`). NATS is a
  small-message request/reply protocol, and Nagle combined with delayed ACKs added
  roughly 40 ms of latency per round trip; local request/reply throughput improved
  about 16x in a single-process benchmark (~22 to ~365 req/s) after this change.
- `[bugfix]` Ordered consumers (`subscribeOrderedConsumer()`) now deliver in order,
  gap-free, and without duplicates. Gap detection was based on the stream sequence,
  which is non-contiguous for a filtered consumer, so every filtered delivery looked
  like a gap; and on a gap the out-of-order message was forwarded and the expected
  sequence advanced past it, causing duplicate/out-of-order delivery and a cascading
  consumer delete+recreate storm. Detection now uses the JetStream consumer
  (delivery) sequence, the out-of-order message is discarded, and the consumer is
  recreated from the last in-order stream sequence (resuming from the next available
  message if the restart point was pruned).
- `[bugfix]` `NatsOptions` now rejects genuinely-invalid configuration at construction
  (non-positive `connectTimeoutMs`/`requestTimeoutMs`, `maxPendingMessagesPerSubscription`
  below 1, and negative reconnect/`maxPingsOut` values) with an `InvalidArgumentException`,
  instead of misbehaving later. Legitimate edge values stay valid: `pingIntervalSeconds`
  `<= 0` disables the heartbeat, `maxPingsOut` 0 is allowed, and an empty `servers` list
  falls back to the default - so this is input validation, not a breaking change.
- `[bugfix]` KeyValue keys with a leading, trailing, or consecutive dot (which produce a
  malformed `$KV.<bucket>.<key>` subject) are now rejected up front; dots, colons and
  slashes elsewhere in a key remain valid.
- `[bugfix]` Object Store `put()` now pipelines chunk publishes in bounded in-flight
  windows instead of awaiting one PubAck round-trip per chunk, so large-object uploads
  are no longer strictly round-trip-bound. PUB frames are written to the single
  connection in chunk order, so stream order (and download reassembly) is preserved.
- `[bugfix]` Single-record reads - KeyValue `get()` and Object Store `info()` (and the
  metadata read behind `get()`/`getToCallback()`) - now use the Direct Get API (served by
  any replica) instead of leader-only `STREAM.MSG.GET`, consistent with `getAll()`/`list()`.
  On clustered/replicated streams this stops concentrating reads on the stream leader. (The
  internal put/delete cleanup lookup stays on `STREAM.MSG.GET` for deterministic ordering.)
- `[bugfix]` KeyValue `getAll()` and Object Store `list()` now read the latest record
  per key/object via the Direct Get API issued concurrently, instead of N+1 sequential
  leader-only `STREAM.MSG.GET` reads. For large buckets this stops hammering the stream
  leader and collapses O(keys) serial round-trips into roughly one round-trip of
  wall-clock. (Concurrent request/reply on a single connection is covered by a new
  integration test.)
- `[bugfix]` `publish()` and `publishScheduled()` now wrap a malformed (non-JSON)
  acknowledgment in a `JetStreamException` instead of leaking a raw `JsonException`,
  consistent with the other JetStream API calls.
- `[bugfix]` Direct Get now rejects an unrecognized response (no status line and no
  `Nats-Stream`/`Nats-Sequence` headers) with a `JetStreamException` instead of
  returning a garbage body, guarding against a non-conformant server/proxy.
- `[bugfix]` KeyValue `watch()` now delivers updates through a JetStream push consumer
  (`deliver_policy=new`, ack-free) so each entry carries its `revision` (the stream
  sequence). Previously it used a plain core subscription and always reported
  `revision=null`, so a watcher could never feed an entry back into `update()`/CAS.
  Live-updates-only semantics are unchanged.
- `[feature]` `JetStreamContext::streamSequenceOf()` returns the stream sequence of a
  JetStream-delivered message (from its `$JS.ACK` reply).
- `[feature]` `ObjectStoreBucket::putStream()` uploads an object from a producer callback
  without holding the whole payload in memory (the streaming counterpart to
  `getToCallback()`): blocks of any size are re-chunked to `chunkSize`, published in bounded
  in-flight windows, and the SHA-256 digest is computed incrementally.
- `[feature]` Object Store `watch()` now delivers updates through a JetStream push consumer
  (consistent with KeyValue `watch()`) and exposes each update's stream sequence via the new
  `ObjectInfo::$revision` field. Previously it used a plain core subscription that carried no
  sequence/revision. Live-updates-only semantics (`deliver_policy=new`, ack-free) are
  unchanged; `$revision` is `null` on `ObjectInfo`s from `get()`/`info()`/`list()`.
- `[bugfix]` `Service::stop()` now tolerates a closed/lost connection: it unsubscribes
  each endpoint best-effort and always clears its subscription state, instead of
  aborting on the first failure (which leaked the remaining subscriptions and broke a
  later `start()` restart).
- `[bugfix]` `Service::addEndpoint()` now rejects a duplicate subject with an
  `InvalidArgumentException` instead of silently overwriting the earlier endpoint and
  handler (which also under-reported them in INFO/SCHEMA/STATS).
- `[bugfix]` `Service::run()` now stops when the connection is unrecoverable (closed
  for good) and backs off interruptibly, instead of busy-spinning at ~50 Hz silently
  swallowing the error.
- `[bugfix]` The microservice discovery handler now swallows an encode/publish failure
  (e.g. invalid-UTF-8 metadata) instead of throwing out of the shared dispatch loop,
  which would abort delivery of buffered frames for other subscriptions.
- `[feature]` `NatsClient::state()` exposes the current connection state.
- `[feature]` `SubscriptionQueue::unsubscribe()` / `close()` cancel the queue's own
  subscription (convenience for `$client->unsubscribe($queue->sid)`).
- `[feature]` `AmpSocketTransport` now accepts `nats://` DSNs directly (self-normalizing
  to `tcp://`), so the transport is usable standalone, not only via the connection layer.
- `[bugfix]` Object Store downloads now use a no-ack (`ack_policy=none`) consumer. The
  read-only download previously used an explicit-ack consumer and acked each chunk; on
  a slow link an ack stalling past `ack_wait` triggered redelivery, which re-hashed a
  chunk and produced a spurious digest mismatch.
- `[bugfix]` Object Store downloads now fail on a truncated transfer (fewer chunks than
  the metadata declares) via a digest-independent completeness check, instead of
  silently returning a partial object when the metadata carries no digest.
- `[bugfix]` Object Store `watch()` now tolerates a malformed metadata payload (skips
  it) instead of throwing out of the dispatch loop, which would abort delivery of
  buffered frames for other subscriptions.
- `[bugfix]` `listStreams()` and `listConsumers()` now paginate through the JetStream
  LIST API (`offset`/`total`). Previously they read only the first page, silently
  truncating accounts with more than the server page size (256) of streams, or a
  stream with more than 256 consumers.
- `[bugfix]` `PullConsumerIterator` infinite mode (`setIterations(null)`) now keeps
  polling past routine empty windows (404/408) instead of terminating on the first
  idle gap, so a long-running worker is no longer killed by a quiet period. Terminal
  errors (e.g. 409 consumer deleted) still stop the loop, and finite mode is unchanged.
- `[bugfix]` The heartbeat watchdog now resets the outstanding-ping counter only when
  an actual PONG is received, not on any inbound bytes. Previously a server that
  stopped answering PINGs but kept trickling data (or a proxy replaying buffered data)
  never tripped `maxPingsOut`, defeating dead-link detection on busy connections.
- `[bugfix]` `drain()` now waits for the server's PONG (bounded by a deadline) before
  closing, instead of bailing on a transient partial/empty read. A larger message
  split across socket reads no longer cuts the flush short and drops in-flight
  deliveries.
- `[bugfix]` `SubscriptionQueue::fetchAll()` no longer returns early on a transient
  empty read (e.g. the heartbeat self-read briefly owning the socket) while its
  configured timeout window still has time remaining.
- `[bugfix]` `recoverConnection()` now coalesces concurrent reconnect attempts. A
  suspended ping-timer callback resuming while the read path already began recovering
  can no longer launch a second reconnect that races on the parser, state, and socket.
- `[bugfix]` The protocol parser now rejects malformed frames instead of silently
  misframing the stream: non-numeric or negative MSG/HMSG sizes and sids, and HMSG
  header bytes exceeding total bytes, raise a `ProtocolException`. A parse failure now
  resyncs past the offending bytes instead of leaving them buffered to re-throw on
  every subsequent read, and `processIncoming()` treats an unparseable stream as a
  transport failure (reconnect) rather than letting the exception escape the read loop.
- `[bugfix]` The client no longer transmits credentials in plaintext to a TLS-required
  server. When the server advertises `tls_required` (or the option/`tls://` scheme
  requires TLS) but no TLS materials were configured at connect time, the previous
  code performed a no-op "upgrade" and then wrote CONNECT (token/user/pass/JWT/sig)
  over the still-plaintext socket, hanging until the handshake deadline. The client
  now fails fast with a clear error and never writes CONNECT before TLS is active.
- `[bugfix]` A graceful peer close (socket EOF) now triggers reconnect from the read
  path. `readLine()` previously collapsed the EOF `null` into an empty string, which
  the connection treated as "no data this tick", so a server restart / idle-timeout /
  load-balancer reap left the client believing it was connected to a dead socket
  (never recovering when pings are disabled, recovering only after ~90s otherwise).
  Transports now signal EOF via a `TransportClosedException`, which `processIncoming()`
  and the heartbeat self-read escalate to `recoverConnection()`.
- `[bugfix]` `SubscriptionQueue::fetch()`, `next()` (with no/zero/negative timeout),
  and `fetchAll()` (with no timeout) no longer block the calling fiber forever on an
  idle subject against a real socket. Each now bounds its single poll with a small
  cancellation, honoring the documented non-blocking contract.
- `[bugfix]` `Service::run()` now passes its cancellation into `processIncoming()`,
  not only the outer `await()`. Previously a timed/cancelled run loop left the idle
  socket read running detached, wedging the shared connection (every later read
  short-circuited and the heartbeat stalled). The read is now torn down on cancel.
- `[bugfix]` `getStreamMessage()` no longer returns an empty payload when the stored
  body is the single character `0`. The decoded body was passed through a falsy
  fallback, so a legitimate `"0"` payload was replaced with an empty string.
- `[bugfix]` `getStreamMessage()` now preserves headers stored with the message.
  Previously the stored header block was dropped and the returned message had no
  headers.
- `[bugfix]` The heartbeat keep-alive read now delivers any application message it
  happens to read while consuming the server reply, instead of leaving it buffered
  until the next manual read. This removes a rare case where a reply could be
  delayed until its own timeout.
- `[bugfix]` Microservice endpoints now default to a shared queue group (`q`) so
  multiple instances of the same service load-balance requests, matching the NATS
  micro specification. Previously every instance handled every request, which
  duplicated side effects and work across instances. **Behavior change:** with more
  than one instance, each request is now handled by exactly one of them. Pass `null`
  or `''` as the endpoint queue group to opt out and fan out to all instances.
  (Reclassified from a breaking change to a bugfix because the previous behavior
  defeated the framework's scaling model.)

### Changed

- `[feature]` Faster Object Store downloads: object chunks are pulled in bounded
  batches rather than one request/reply round-trip per chunk, which significantly
  reduces latency for large, multi-chunk objects while keeping peak memory bounded.
  Digest verification, in-order delivery, the chunk-by-chunk `getToCallback()`
  contract, and `nats` CLI interoperability are unchanged.

### Documentation

- `[docs]` Corrected the Performance Benchmark Recipe, which previously stalled after
  roughly 50 requests because the responder consumed only one transport chunk per
  request. The recipe now drives the responder with a single continuous read loop,
  and the `processIncoming()` single-chunk semantics are spelled out.
- `[docs]` Corrected the Scheduled Publish example. It now creates the backing stream
  with `allow_msg_schedules` (and `allow_msg_ttl` when a schedule TTL is used) so the
  example runs as written; without those flags the server rejects the publish.
- `[docs]` Renamed the "Stream Message Direct Get" section to "Stream Message Get" and
  clarified that `getStreamMessage()` uses the standard stream message get API (not
  the JetStream direct-get API) and preserves the stored body and headers.
