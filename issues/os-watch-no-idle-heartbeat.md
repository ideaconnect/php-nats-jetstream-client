# ObjectStore watch requests no idle heartbeat — a dead watch hangs forever with no error

- **Status:** FIXED (2026-08-07) — `ObjectStoreBucket::watch()` now defaults
  `idle_heartbeat` to the new `WATCH_IDLE_HEARTBEAT_NS` (5 s, KV parity) so the #113 watchdog arms,
  and `ObjectStoreWatchOptions` gained an `$idleHeartbeat` override. Confirmed by
  `ObjectStoreBucketTest::testWatchRequestsDefaultIdleHeartbeat` (fails pre-fix) and
  `testWatchSilentConsumerSurfacesNotActiveError` (silent consumer surfaces a "not active" error).
- **Severity:** major
- **Type:** blocked flow (silent hang)
- **Area:** ObjectStore watch
- **Where:** `src/JetStream/ObjectStore/ObjectStoreBucket.php:847-848` (and
  `ObjectStoreWatchOptions::toConsumerConfig()`), contrast `src/JetStream/KeyValue/KeyValueBucket.php:405-409`

## Problem

The KV watch force-defaults an idle heartbeat so `subscribeEphemeralPushConsumer` arms the
missed-heartbeat watchdog (#113):

```php
// KeyValueBucket.php:409
$consumerOptions['idle_heartbeat'] ??= self::WATCH_IDLE_HEARTBEAT_NS;
```

The Object Store watch path never sets `idle_heartbeat` — neither the null-options default
(`['deliver_policy' => 'new', 'ack_policy' => 'none']`) nor `ObjectStoreWatchOptions`. With no
heartbeat requested, `idleHeartbeatOf()` returns null and **no watchdog is armed**
(`JetStreamContext.php:2784+`).

## Failure scenario

1. OS watch established (ephemeral push consumer).
2. The server restarts (ephemeral consumers do not survive), or the consumer is reaped by
   `inactive_threshold`.
3. Total silence forever: the watcher never receives another `ObjectInfo` and no error is
   surfaced anywhere. Indistinguishable from an idle bucket.

This is exactly the condition #113 fixed for KV; the fix was not applied to the Object Store
watch.

## Suggested fix

Mirror the KV line in `ObjectStoreBucket::watch()`:
`$consumerOptions['idle_heartbeat'] ??= <watch heartbeat ns>;` (heartbeats are status-100 control
frames and are already filtered before delivery, so watchers see no behavioral change).
