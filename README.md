# Vault: a privacy-compliant commerce backend

Go microservices for an order → payment flow. User PII is encrypted per user, every PII access is written to a
tamper-evident audit log, and deleting a user makes that user's data unreadable everywhere (crypto-shredding).

```
Client → gateway (Hertz: JWT + roles, rate limit, request IDs)
            │ RPC
   ┌────────┼─────────────┐
 order     payment        user
 (outbox)  (idempotency)  (envelope-encrypted PII, crypto-shredding, audit log)
   │
   ▼ Kafka (Redpanda), keyed by user_id
 pipeline → redact/tokenize PII → analytics_events   (DLQ on poison messages)

Observability: OpenTelemetry traces → Jaeger · Prometheus metrics + alerts → Grafana
```

## Run it

```bash
make up && make topics      # Postgres, Redpanda, Jaeger, Prometheus, Grafana
make run                    # order + payment + user + gateway + pipeline (Ctrl-C stops all)

TOKEN=$(go run ./cmd/tokengen)
curl -i -X POST localhost:8080/v1/checkout \
  -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: my-first-order" \
  -H 'Content-Type: application/json' -d '{"amount_cents": 1999}'

make test                   # unit + integration tests (needs `make up`)
make load                   # k6 load test (needs `make run`)
```

### API

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/checkout` | Needs `Authorization: Bearer` and `Idempotency-Key`. 201 PAID, 402 declined, 200 + `Idempotent-Replayed: true` on retry, 422 key reused with a different amount, 429 rate limited, 503 retry with the same key |
| GET | `/v1/orders/:id` | 404 for orders that aren't yours |
| PUT | `/v1/me/profile` | `{"email", "address"}`, stored encrypted. 409 if the email belongs to another account |
| GET | `/v1/me/profile` | Decrypts and audits the read |
| DELETE | `/v1/me` | Crypto-shreds the account: 204, then every copy of the PII is unreadable |
| GET | `/v1/support/users/:id/profile?reason=…` | Needs a `support` role token (`tokengen -role support`) and a reason; audited with both |

Internal only (admin ports): `/healthz`, `/readyz` and `/metrics` on every service (gateway 8090, order 8081,
payment 8082, user 8083, pipeline 8084), plus `GET localhost:8083/audit/verify`, which walks the audit hash chain.

## Observability

| Tool | URL | What it shows |
|---|---|---|
| Grafana | http://localhost:3000 | The **Vault** dashboard (home page): checkout health, RPC latency and failures, payment provider, outbox and pipeline, PII access, Go runtime |
| Jaeger | http://localhost:16686 | One trace per request, across gateway → order → payment → Kafka → pipeline, including every SQL query |
| Prometheus | http://localhost:9090/alerts | Alert rules: service down, 5xx rate, checkout p99, outbox stuck, dead letters, audit chain broken, payment provider errors |

- **Traces cross Kafka.** The relay publishes events later, from a background loop, so the outbox row stores the trace context of the request that wrote it and the relay sends it as Kafka headers. The pipeline's span joins the original checkout's trace.
- **Logs link to traces.** Log lines written with a request context carry `trace_id` and `span_id`.
- **RED metrics everywhere:** rate, errors and duration for every HTTP route and RPC method, plus business metrics (checkout outcomes, payment provider latency, outbox backlog and age, analytics freshness, PII accesses by action and actor type).

## Design decisions
- **Idempotent checkout, end to end:** orders are unique on `(user_id, idempotency_key)`, a payment's key is its order ID (one charge per order), and status updates are no-ops when already applied. A client can retry any failure with the same key.
- **"Reserve, then call out" for payments:** a PENDING row is inserted first (only one caller can win), then the PSP is called outside any transaction. A crash leaves the row PENDING; after a lease expires, the next retry re-asks the idempotent PSP.
- **404 instead of 403 for other users' orders:** callers can't probe which order IDs exist.
- **Per-user token-bucket rate limit, in memory:** simple and fast, but with N gateway replicas each user gets N x the limit. A shared limit would need Redis.
- **Outbox instead of writing to the DB and Kafka separately:** writing to both can fail halfway and leave them inconsistent. If Kafka is down, checkout still works; events wait in the outbox and are published when it recovers (tested: stop Redpanda, check out, start it, and the events arrive).
- **One relay publishes at a time (Postgres advisory lock):** with parallel relays, a user's `order.paid` could reach Kafka before their `order.created`. A single publisher keeps per-user order; standby instances take over automatically if it dies, because the lock is released with its transaction.
- **Permanent vs transient failures in the consumer:** a malformed message goes to the DLQ with its reason and source offset, so it can't block the partition. A DB outage is retried with exponential backoff and jitter instead, because dead-lettering during an outage would dump every message. Offsets are committed only after the batch is stored.
- **Allowlist, not blocklist, for analytics:** the pipeline copies only named fields. A new PII field added to the event later is dropped by default.
- **HMAC tokens, not plain hashes:** anyone can hash a list of known user IDs and match a plain SHA-256. Without the key, they can't.
- **At-least-once delivery + idempotent consumers (PK on event_id):** this gives the effect of exactly-once processing.
- **Partition key = user_id:** events for one user stay in order.
- **Envelope encryption, one data key per user:** a master key (KMS in production) wraps each user's data key; the data key encrypts their email and address with AES-256-GCM. PII is encrypted before it leaves the user service, so the database, the outbox, Kafka and every backup only hold ciphertext.
- **Crypto-shredding for deletion:** `DELETE /v1/me` destroys the user's data key. Copies of their ciphertext already in Kafka, consumer databases or backups become permanently unreadable, without having to find and scrub each one. Orders stay (financial records have retention requirements) but hold no PII.
- **Ciphertext bound to (user, field):** GCM's associated data includes the user ID and field name, so a ciphertext copied into another user's row or another column fails to decrypt instead of showing the wrong person's data.
- **Blind index for email:** an HMAC of the normalized email enforces one account per email without decrypting every row.
- **Hash-chained audit log, written in the same transaction as the PII access:** every read, write and delete records actor and reason. If the audit write fails, the access fails ("no audit, no access"). Editing, deleting or reordering any entry breaks the chain, and `/audit/verify` reports the first bad entry.
- **Support access is allowed, but never silent:** it needs a staff role and a reason, both of which are audited.
- **Business errors aren't failures:** RPC metrics split outcomes into `ok`, `business_error` (not found, key reused: expected answers) and `error` (the service failing). Alerts fire only on `error`, so a client sending bad requests doesn't page anyone.
- **Route patterns as metric labels, never raw paths:** `/v1/orders/:id`, not `/v1/orders/<uuid>`. One time series per order ID would grow without bound and take down Prometheus.
- **Latency buckets densest around the SLO:** percentiles are interpolated within a histogram bucket, so p99 is only as precise as the bucket it falls in. Counters also start at zero for every known label, so the first event registers as an increase.

## Known limitations
- All services share one Postgres database for local dev; in production each service would own its own database.
- A payment stuck PENDING is only resolved when the client retries. A background reconciliation job would fix this without waiting for the client.
- The pipeline processes one record at a time, about 1,900 events/sec (measured draining a 33k backlog). Batch inserts or one goroutine per partition would raise that.
- One active relay caps outbox throughput at a single publisher. Fine at this scale; beyond that, partition the outbox by user and run one relay per shard.
- `user_keys` lives in the same Postgres as everything else. A database backup therefore contains the keys, and restoring an old backup would bring back a deleted user's key. In production the keys go in a separate store (a KMS, or a key database with short backup retention), so data backups never include them.
- Every PII read unwraps the data key. With a real KMS that's a network call, so you'd cache unwrapped keys briefly in memory, trading a small exposure window for latency.
- The audit chain is serialized by one lock, so it's a single point of contention. Sharding chains (per region or per tenant) would scale it. Anyone with write access to the whole table could also rewrite the whole chain; anchoring the head hash in a separate system prevents that.
- One outbox row for a Kafka topic that doesn't exist blocks the relay, because Kafka rejects the whole batch (this happened during development before `user.events` existed). A fix: move rows that fail permanently to a dead-letter table, the same way the pipeline handles poison messages.
- On Go 1.27, `sonic` (fast JSON) falls back to `encoding/json` (see the startup warning). Pinning the toolchain to Go 1.26 would restore it.

- Every request is traced (100% sampling). At production volume you'd sample a small fraction and keep every trace with an error.
- The user service re-verifies the whole audit chain every minute, so the check gets slower as the log grows. Verifying incrementally from the last checkpoint would fix that.

## Roadmap
- Load-test baseline, profiling with pprof, and a benchmark table
- Circuit breakers, and chaos tests under load (`make chaos`)
- CI with Postgres and Redpanda, so integration tests run on every push
