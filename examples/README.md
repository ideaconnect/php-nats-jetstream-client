# Examples

One runnable, self-contained script per README example. Each connects, exercises the feature end-to-end,
prints an `OK ...` line, and cleans up the streams/buckets/services it created (so they are safely
re-runnable). Running them is how we verify the README stays true - see [`scripts/run-examples.sh`](../scripts/run-examples.sh).

## Running

Core examples need only a JetStream-enabled server:

```bash
docker compose up -d nats
NATS_URL=nats://127.0.0.1:14222 php examples/publish-subscribe.php
```

Run the whole set (reports pass / skip / fail; non-zero exit on any failure):

```bash
docker compose up -d            # full stack: token/userpass/jwt/tls/nkey/ws variants too
composer fixture:jwt            # JWT + creds fixtures (auth-jwt-nkey, auth-credentials-file)
NATS_URL=nats://127.0.0.1:14222 bash scripts/run-examples.sh
```

CI runs this same set as a required gate (the `examples` job) with `EXAMPLES_STRICT=1`, which treats a
skipped example as a failure - so every example must actually execute and pass.

Every base example reads `NATS_URL` (default `nats://127.0.0.1:4222`). The auth/WebSocket examples read
their own variant-server env vars (`NATS_TOKEN_URL`, `NATS_USERPASS_URL`, `NATS_JWT_URL`, `NATS_NKEY_URL`,
`NATS_TLS_URL`, `NATS_WS_URL`), each defaulting to the dev docker-compose port.

## Core messaging

- `publish-subscribe.php` - publish a message and receive it on a subscription.
- `request-reply.php` - request/reply round-trip with a self-hosted responder.
- `request-many.php` - scatter-gather: collect replies from several responders.
- `queue-group-subscribe.php` - queue-group (load-balanced) subscription.
- `polling-subscribe.php` - pull messages with `SubscriptionQueue` (`fetch`/`next`/`fetchAll`).
- `headers-and-server-info.php` - publish/read message headers and read server info.
- `connection-stats-rtt.php` - connection statistics and round-trip-time measurement.
- `graceful-drain.php` - `drain()` flushes in-flight messages before closing.

## JetStream - consumers

- `jetstream-stream-and-consumer.php` - create a stream + durable consumer and publish.
- `stream-update-and-consumer-info.php` - update a stream and read consumer info.
- `pull-consumer-fetch-ack.php` - pull fetch + `ack()` / `ackSync()`.
- `pull-consumer-nak-term.php` - `inProgress` / `nak` / `nakWithDelay` / `term` redelivery control.
- `push-consumer-durable.php` - durable push consumer delivery.
- `ephemeral-consumers.php` - ephemeral pull and push consumers.
- `ordered-consumer.php` - ordered consumer (gap-free in-order delivery).
- `consumer-pause-resume.php` - pause a consumer until a future instant, then resume.
- `consumer-list.php` - list a stream's consumers.
- `fetch-batch.php` - fetch a batch of messages in one pull.
- `pull-consumer-batching-iteration.php` - the fluent `pullConsumer()` batching iterator.
- `pull-consumer-priority-groups.php` - pull priority groups (pinned-client policy).

## JetStream - streams, messages & data

- `stream-purge-and-list.php` - purge by subject filter / fully, and list streams.
- `stream-message-get.php` - fetch a stored message by sequence.
- `jetstream-direct-get.php` - Direct Get by sequence and last-by-subject.
- `atomic-batch-publish.php` - atomic batch publish (`$js->batch()`).
- `scheduled-publish.php` - schedule a delayed publish (`@at`).
- `distributed-counter.php` - CRDT distributed counter (`incrementCounter`/`counterValue`).
- `typed-stream-configuration.php` - typed `StreamConfiguration` / consumer builders.
- `stream-mirroring-and-sourcing.php` - mirror and multi-source aggregate streams.
- `republish-and-subject-transform.php` - republish and subject-transform stream options.

## KeyValue & Object Store

- `keyvalue-bucket.php` - KV put/get/update/watch.
- `object-store-bucket.php` - Object Store put/get/info/list/delete.
- `object-store-streaming-to-callback.php` - stream an object to a callback chunk-by-chunk.
- `object-store-streaming-upload.php` - stream a large object up in chunks (`putStream`).

## Services (NATS micro)

- `services-framework.php` - service with endpoints and grouped endpoints, plus discovery.
- `services-schema-discovery.php` - endpoint schema validation + observers + `$SRV.SCHEMA`.

## Authentication & transports

These need the matching variant server from `docker compose up -d`:

- `auth-token.php` - token auth (`nats-token`, default `:14223`).
- `auth-userpass.php` - username/password auth (`nats-userpass`, default `:14224`).
- `auth-jwt-nkey.php` - JWT + NKey auth (`nats-jwt`, default `:14227`; needs `composer fixture:jwt`).
- `auth-standalone-nkey.php` - standalone NKey challenge auth (`nats-nkey`, default `:14226`). Run via
  `scripts/run-examples.sh` (it exports the dev seed trusted by `build/nats/nkey.conf`); run directly it
  skips unless `NATS_NKEY_SEED` is set to a seed the server trusts.
- `auth-tls.php` - mutual TLS (`nats-tls`, default `:14225`; uses `build/tls/*` fixtures).
- `auth-credentials-file.php` - `.creds` file auth (needs `composer fixture:jwt`).
- `websocket-transport.php` - NATS over WebSocket (`nats-ws`, default `ws://...:14229`).
