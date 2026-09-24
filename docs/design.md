# Design notes

Why things are built the way they are, grouped by area. Known limitations are at the end.

## Checkout and payments

Every step of checkout can be repeated without side effects. Orders are unique per user and idempotency key, so
retrying a checkout returns the original order instead of creating a new one. The payment for an order uses the
order ID as its key, so an order can only be charged once. Marking an order paid twice does nothing the second
time. If a client gets a 503, it can retry with the same key and the checkout picks up where it stopped.

Payments reserve before they call out. The payment service first inserts a PENDING row, which only one caller can
win, and only then calls the payment provider, outside any database transaction so a slow provider never holds
locks. If the service crashes between the call and saving the result, the row stays PENDING. After a 10 second
lease, the next retry asks the provider again with the same key, which is safe because the provider is
idempotent too. The lease is checked with the database's own clock (`now() - created_at`), never the service's,
because the two can drift apart.

Other users' orders return 404 rather than 403, so nobody can probe which order IDs exist.

## Events

The order service never writes to Postgres and Kafka separately, because one could succeed and the other fail. It
writes the event to an outbox table in the same transaction as the order, and a relay publishes outbox rows to
Kafka afterwards. If Kafka is down, checkout still works and events wait in the table.

Only one relay publishes at a time (a Postgres advisory lock), which keeps each user's events in order. Events are
keyed by user ID, so a user's events land in the same partition. Published rows are deleted rather than marked as
published, since an update rewrites the whole row and write volume turned out to be the bottleneck.

Delivery is at least once. The pipeline commits its Kafka offsets only after the batch is saved, and the
analytics table ignores event IDs it has already seen, so duplicates are harmless. A malformed message goes to a
dead-letter topic with the reason attached, so it can't block everything behind it. A database outage is retried
with backoff instead, because dead-lettering every message during an outage would make a mess.

The pipeline copies an allowlist of fields into analytics, so a new personal field added to events later is
dropped by default. User IDs are replaced with an HMAC token: analysts can still count unique buyers, but can't
match tokens back to people without the key (a plain hash of a user ID can be matched by hashing guesses).

## Personal data

Each user gets their own data key, which encrypts their email and address with AES-256-GCM. The data keys are
themselves encrypted with a master key, which would live in a KMS in production. Personal data is encrypted
before it leaves the user service, so the database, Kafka and backups only ever hold ciphertext.

Deleting an account destroys the user's data key. Copies of their encrypted data elsewhere (Kafka, backups,
other services) become unreadable without having to find each one. Orders are kept, since financial records
have retention rules, but they don't contain personal data.

Each ciphertext is tied to its user and field, so encrypted data copied into another user's row, or from one
column to another, fails to decrypt instead of showing the wrong person's data. Emails are looked up through an
HMAC of the normalized address, which enforces one account per email without decrypting anything.

Every read, write and delete of personal data adds an audit entry recording who did it and why, in the same
transaction as the access itself. If the audit entry can't be written, the access fails. Each entry includes the
hash of the previous one, so editing, deleting or reordering entries breaks the chain, and `/audit/verify` on the
user service's admin port reports where. Support staff can view a customer's profile, but only with a reason,
and that reason is logged.

## Reliability

Every RPC has a 2 second timeout. The gateway retries timeouts up to twice, which is only safe because every call
is idempotent. Retries are limited by a budget of roughly 10% of traffic, so during an outage they stop instead of
piling more load onto a struggling service. In the chaos test this halved worst-case latency compared with
Kitex's built-in retry.

Each RPC method has a circuit breaker. Once half of the recent calls fail, calls fail immediately instead of
waiting for the timeout, and a test call goes through every 2 seconds to check for recovery.

Services shut down gracefully but with a deadline. A service that can't open its health check port refuses to
start, rather than running invisibly.

## Observability

Traces follow a request through the gateway, RPC calls, SQL queries and Kafka. Because the relay publishes events
later from a background loop, the outbox stores the original request's trace ID and passes it along in the
Kafka message, so the pipeline's work shows up in the same trace as the checkout that caused it. Log lines
include the trace ID.

Each service reports request rate, errors and latency, plus business numbers like checkout outcomes, outbox
backlog and analytics freshness. RPC metrics separate expected errors (not found, key reused) from real failures,
and alerts only look at real failures. Metrics are labelled by route pattern (`/v1/orders/:id`), never the actual
path, since a label per order ID would grow forever.

## Known limitations

- All services share one Postgres database locally. In production each would have its own.
- An order that was charged but never marked paid stays PENDING until the client retries. A reconciliation job
  would fix this.
- The encryption keys live in the same database as the data, so a backup contains both. Restoring an old backup
  would bring back a deleted user's key. In production the keys would be stored separately.
- The audit log goes through a single lock, which limits throughput. Someone with full write access to the table
  could also rewrite the whole chain. Saving the latest hash somewhere else would catch that.
- The first requests into a full outage can still wait about 4 seconds (a timeout plus one retry) before the retry
  budget runs out.
- One relay publishes all events, and one pipeline consumer processes them. Both would need sharding at larger
  scale.
- The user service re-checks the entire audit chain every minute, which gets slower as the log grows.
- If an outbox row points at a Kafka topic that doesn't exist, it blocks the relay.
- On Go 1.27 the fast JSON library falls back to the standard one. CI uses Go 1.26, where it works.
