# Deferral rewind can deliver duplicate messages when the episode's candidate delivered frames before the disconnect

- **Status:** FIXED (2026-08-08) — the deferral snapshots `$lastStreamSeq` and performs the adoption rewind
  ONLY when the episode delivered nothing. An episode whose candidate already replayed frames to
  the handler keeps its adopted state and falls back to the exactly-once filtered-until-heartbeat
  recovery, so a surviving old consumer can no longer re-deliver stream sequences the handler
  already saw. Pinned by the candidate-delivered arm added to the deferral rewind tests, verified
  red with the rewind made unconditional.
- **Severity:** minor
- **Type:** duplicate deliveries on the ordered path (introduced by the 2nd-round rewind fix;
  pre-fix this sub-case was exactly-once)
- **Area:** JetStream ordered consumers — disconnect-collision deferral rewind
- **Where:** `src/JetStream/JetStreamContext.php:1644-1647` (rewind restores
  consumerName/deliver/expectedConsumerSeq but not `$lastStreamSeq`), `:1434-1447` (snapshot
  without lastStreamSeq), `:1773-1788` (dispatch checks name + consumerSequence only — no
  stream-sequence dedup before `$handler()`)

## Problem

The rewind restores the pre-episode adoption state but neither rewinds `$lastStreamSeq` nor adds
stream-sequence dedup. In the sub-case where the episode's candidate consumer WAS created and
delivered frames to the user handler before the connection dropped (create reply lost, frames
dispatched during the create's own read-pump), a surviving old consumer re-delivers those same
stream sequences after reconnect: they pass the restored name filter and the restored strict
cseq equality, so the handler receives them a second time. Pre-fix, these survivor frames were
name-filtered and the next watchdog recreate resumed exactly-once from `lastStreamSeq+1`.

## Failure scenario

Watchdog episode at stream seq n (expected cseq E): the initial delete of ORD_old times out with
no server-side effect; attempt 1's create succeeds but its reply is lost — before the connection
drops, the candidate replays k messages (stream seqs n+1..n+k) which reach the handler. Attempts
exhaust while not Open; the deferral rewinds cseq tracking to E and keeps the old inbox. After
reconnect the surviving ORD_old resumes at its own cseq E carrying stream seq n+1: the handler
receives stream seqs n+1..n+k a SECOND time — k duplicate KV/OS watch notifications.

## Suggested fix

Snapshot `$lastStreamSeq` alongside the other pre-episode state and only rewind the adoption when
the episode delivered nothing (`$lastStreamSeq` unchanged); when candidate deliveries advanced
it, skip the rewind — keeping the pre-fix filtered-until-heartbeat recovery, which is
exactly-once. (Alternative: keep the rewind but drop survivor frames with
`streamSequence <= $lastStreamSeq` before `$handler()` — a no-op on every normal path.)
