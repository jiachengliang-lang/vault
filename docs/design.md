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

A checkout cut off part way (charged but not marked paid, or mid-charge) is finished by a client retry with the same
key. For clients that never retry, a reconciler (`cmd/reconcile`, every 5 minutes on AWS) does the same thing: for
each order PENDING for over 2 minutes it marks it from the payment's result, or retries a charge left PENDING with
the order's original key, going through the order and payment services so the same rules and events apply. An
order with no payment at all is left an hour for the client to come back, then marked FAILED.

## Events

The order service never writes to Postgres and Kafka separately, because one could succeed and the other fail. It
writes the event to an outbox table in the same transaction as the order, and a relay publishes outbox rows to
Kafka afterwards. If Kafka is down, checkout still works and events wait in the table.

Each publish attempt has a 10-second limit. Without one, the Kafka client retries forever while Kafka is down,
so the relay hung silently with its transaction open: nothing logged a failure and the backlog metrics froze. A
Kafka outage test on AWS found that. Now each attempt fails, is logged, and the next one retries. A batch that timed
out may have been partly delivered; sending it again is safe, since consumers ignore event IDs they've seen.

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
themselves encrypted with a master key. On AWS that's a KMS key: it never leaves KMS, every unwrap is a request
CloudTrail records, and only the user service's role may use it. (The services share one EC2 host, so the user
service runs as its own ECS task to get its own role.) Locally the master key comes from an environment variable
behind the same interface. Personal data is encrypted before it leaves the user service, so the database, Kafka
and backups only ever hold ciphertext.

The wrapped data keys are kept apart from the data. On AWS they're in DynamoDB, so a backup of the database
contains no keys, and restoring an old one can't bring back the key of a user who's since been deleted.
Point-in-time recovery is off for that table for the same reason: with it on, a deleted key could be restored
for 35 days. Locally the keys are in a `user_keys` table, for convenience.

Deleting an account destroys the user's data key. Copies of their encrypted data elsewhere (Kafka, backups,
other services) become unreadable without having to find each one. The key is destroyed first, before the
database transaction that removes the profile and writes the audit entry: it's the step that protects the user,
and if the rest fails, a retry finishes it. Orders are kept, since financial records
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
- Since keys moved out of the transaction, a profile update racing an account deletion can leave behind a row
  encrypted under the destroyed key. It can't be read (reads treat a missing key as "not found"), and the next
  update for that user replaces it.
- The audit log goes through a single lock, which limits throughput. Someone with full write access to the table
  could also rewrite the whole chain. Saving the latest hash somewhere else would catch that.
- The first requests into a full outage can still wait about 4 seconds (a timeout plus one retry) before the retry
  budget runs out.
- One relay publishes all events, and one pipeline consumer processes them. Both would need sharding at larger
  scale.
- The user service re-checks the entire audit chain every minute, which gets slower as the log grows.
- If an outbox row points at a Kafka topic that doesn't exist, it blocks the relay.
- On Go 1.27 the fast JSON library falls back to the standard one. CI uses Go 1.26, where it works.
