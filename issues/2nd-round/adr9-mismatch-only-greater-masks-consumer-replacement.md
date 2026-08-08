# ADR-9 gap check only fires on Nats-Last-Consumer GREATER than tracked max — a server-side consumer replacement resets cseq and masks real gaps until the old high-water mark is passed

- **Status:** OPEN (filed 2026-08-08, second-round review; adversarially verified)
- **Severity:** minor
- **Type:** lost-message detection gap / nats.go parity deviation
- **Area:** JetStream push-consumer dispatch (round-1 ADR-9 fix follow-up)
- **Where:** `src/JetStream/JetStreamContext.php:1192` (strictly-greater signal condition),
  `:1217-1221` (monotonic-max tracker + only re-arm site)

## Problem

`callerOwnedPushDispatch` signals a consumer-sequence mismatch only when the heartbeat's
`Nats-Last-Consumer` is strictly **greater** than the tracked max:

```php
if ($sawDeliveryThisSession && $lastDelivered !== null
    && $lastDelivered > $deliveredConsumerSeq && !$gapSignaled)
```

and `$deliveredConsumerSeq` is a monotonic max (`:1217` advances it only on
`consumerSequence > $deliveredConsumerSeq`, which is also the only `$gapSignaled` re-arm site).
nats.go's `checkForSequenceMismatch` fires `ErrConsumerSequenceMismatch` on **any** inequality.
After a server-side consumer replacement (delete + recreate with the same name/deliver subject —
resets cseq to 1), deliveries at cseq 1..N (N < old max) neither advance the tracker nor re-arm
the signal, and heartbeats report `Nats-Last-Consumer` below the stale high-water mark, so `>`
never fires: the replacement itself is never surfaced, and **real local gaps in the new consumer
instance are masked** until its sequence exceeds the old maximum — by which point tracker and
heartbeat re-converge and the episode is unreportable.

## Failure scenario

An ack-none durable push consumer delivered up to cseq 500 this session. An operator deletes and
recreates the consumer with the same config; the new instance starts at cseq 1. The client then
locally drops deliveries 10–50 (slow-consumer window). Heartbeats report `Nats-Last-Consumer` = 50
< 500 → no mismatch is ever signaled — permanent silent loss in exactly the ack-none configuration
the round-1 fix (`issues/push-consumer-no-adr9-gap-detection.md`) targets.

## Suggested fix

Detect the reset: when `$lastDelivered` (or a delivered cseq) is materially **below**
`$deliveredConsumerSeq`, rebase the tracker to the new instance (reset
`$deliveredConsumerSeq`/`$sawDeliveryThisSession`, optionally surface the mismatch once as
nats.go does for any `ldseq != dseq`), so gap detection re-arms against the replacement consumer
instead of the stale high-water mark.
