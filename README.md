# Vault

A small payments backend I built to learn how checkout systems stay correct when things fail, and how to handle
user data carefully. It's five Go services that talk over RPC and Kafka, backed by Postgres.

What it does:

- Checkout is safe to retry. A double click, a timeout or a crash halfway through never charges anyone twice.
- Order events go through Kafka into an analytics table, with user IDs swapped for tokens.
- Emails and addresses are encrypted with a separate key per user. Deleting an account destroys that key, so every
  copy of the user's data (including ones already sitting in Kafka or in backups) becomes unreadable.
- Every read of personal data is recorded in an audit log that detects tampering.

Built with Go, CloudWeGo (Kitex and Hertz), PostgreSQL, Kafka (Redpanda), Docker Compose, OpenTelemetry,
Prometheus and Grafana.

![Grafana dashboard during a chaos test](docs/dashboard.png)

The dashboard during a chaos test, with Postgres frozen, Kafka down and the payment service killed, all under load.

## How it fits together

```
client
  |
gateway  (HTTP: auth, rate limiting)
  |
  +-- order service    ---> outbox ---> Kafka ---> pipeline ---> analytics table
  +-- payment service
  +-- user service     (encrypted profiles, audit log)
```

The gateway handles checkout by creating an order, charging it, then marking it paid or failed. Each step can be
retried safely. Order events are written to an outbox table in the same transaction as the order, then a
background relay publishes them to Kafka.

## Running it locally

You need Go and Docker.

```bash
make up && make topics   # Postgres, Kafka, Jaeger, Prometheus, Grafana
make run                 # starts the five services (Ctrl-C stops them)
```

Then, in another terminal:

```bash
TOKEN=$(go run ./cmd/tokengen)
curl -i -X POST localhost:8080/v1/checkout \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: order-1" \
  -H 'Content-Type: application/json' \
  -d '{"amount_cents": 1999}'
```

Run the same command again and you get the same order back, with `Idempotent-Replayed: true`.

Grafana is at http://localhost:3000 and Jaeger (traces) is at http://localhost:16686.

## API

| Endpoint | What it does |
|---|---|
| `POST /v1/checkout` | Create and pay for an order. Needs a token and an `Idempotency-Key` header. |
| `GET /v1/orders/:id` | Get one of your orders. |
| `PUT /v1/me/profile` | Save your email and address (stored encrypted). |
| `GET /v1/me/profile` | Read your profile. |
| `DELETE /v1/me` | Delete your account. |
| `GET /v1/support/users/:id/profile?reason=...` | Support staff only. Needs a reason, which gets logged. |

## Testing

```bash
make test    # unit and integration tests (needs make up)
make e2e     # end-to-end checks against the running services
make chaos   # breaks things under load, then checks the data
```

The chaos test sends 300 checkouts a second for 100 seconds while it freezes Postgres, stops Kafka and kills the
payment service. In the latest run that was about 27,700 orders with 0 charged twice and 0 events lost. Checkout
kept working the whole time Kafka was down, because events waited in the outbox until it came back.

CI runs everything against real Postgres and Kafka on every push, then starts the services and runs the
end-to-end checks.

## Performance

Under load, checkout slowed down past about 1,000 requests a second. Profiling showed the Go services mostly idle
and Postgres mostly waiting to flush its write-ahead log, so the real limit was the number of commits. A third of
them came from the analytics pipeline, which saved one event per commit.

Batching the pipeline's writes and changing how the outbox cleans up after itself cut commits per checkout by
about 17%, and analytics went from falling up to 5 minutes behind to under half a second. Checkout latency looked
better too, but on a laptop the run-to-run noise was bigger than the difference, so I'm not counting that.

Method and full numbers are in [docs/benchmarks.md](docs/benchmarks.md).

## Things that broke along the way

Most of these only showed up under load or during the chaos test:

- The analytics pipeline never exited when stopped, so old copies kept piling up in the Kafka consumer group.
- After the Docker VM restarted, the database clock was 24 seconds behind the services. The payment code compared
  the two clocks, so every in-progress charge looked stale and got retried. It now only uses the database's clock.
- Kitex's built-in retry quietly dropped error responses, which crashed the gateway. I replaced it with my own
  retry logic, capped so it can't pile onto an outage.
- Jaeger ran out of memory when every request was traced at ~1,000 requests a second.
- The circuit breaker kept failing requests for about 9 seconds after the payment service was already back.

## More detail

- [docs/design.md](docs/design.md): why things are built the way they are, and known limitations
- [docs/benchmarks.md](docs/benchmarks.md): how performance was measured

## What's next

- A reconciliation job for checkouts that were charged but never marked paid
- A chaos test where a service gets slow instead of going down
