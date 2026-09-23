-- Starting schema. Read every line: you will be asked "why is this column/constraint here?"

-- ---------- user service: PII is never stored in plaintext ----------
CREATE TABLE user_keys (
    user_id     UUID PRIMARY KEY,
    wrapped_dek BYTEA NOT NULL,          -- per-user data key, encrypted by the master key (envelope encryption)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Deleting a row here = crypto-shredding: every ciphertext for that user, anywhere, becomes unreadable.

CREATE TABLE users (
    user_id     UUID PRIMARY KEY,
    email_enc   BYTEA NOT NULL,          -- AES-GCM ciphertext under the user's DEK
    email_hash  BYTEA NOT NULL UNIQUE,   -- HMAC of email, for lookups without decrypting
    address_enc BYTEA,
    region      TEXT NOT NULL DEFAULT 'US',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- order service ----------
CREATE TABLE orders (
    order_id        UUID PRIMARY KEY,
    user_id         UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL CHECK (amount_cents > 0),
    status          TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING -> PAID | FAILED
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- keys are scoped per user: two users picking the same key must not collide
    UNIQUE (user_id, idempotency_key)
);

-- Transactional outbox: written in the SAME transaction as the order row,
-- then a relay publishes it to Kafka and deletes it. Solves the dual-write problem.
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,          -- partition key (user_id) -> per-user ordering
    payload      JSONB NOT NULL,
    headers      JSONB NOT NULL DEFAULT '{}', -- trace context of the request that wrote the event
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- payment service ----------
CREATE TABLE payments (
    payment_id      UUID PRIMARY KEY,
    order_id        UUID NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE, -- gateway uses the order_id: one charge per order, retries never double-charge
    amount_cents    BIGINT NOT NULL,
    status          TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- audit log: append-only, hash-chained (tamper-evident) ----------
CREATE TABLE audit_log (
    seq        BIGSERIAL PRIMARY KEY,
    actor      TEXT NOT NULL,            -- which service / operator
    action     TEXT NOT NULL,            -- e.g. READ_PII, DELETE_USER
    subject_id UUID NOT NULL,            -- whose data
    reason     TEXT NOT NULL,
    prev_hash  BYTEA NOT NULL,
    hash       BYTEA NOT NULL,           -- sha256(prev_hash || seq || actor || action || subject_id || reason || ts)
    ts         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- analytics sink: de-identified only (pipeline writes here) ----------
CREATE TABLE analytics_events (
    event_id     UUID PRIMARY KEY,       -- PK makes consumer writes idempotent under at-least-once delivery
    user_token   TEXT NOT NULL,          -- tokenized user id, never raw PII
    event_type   TEXT NOT NULL,
    amount_cents BIGINT,
    ts           TIMESTAMPTZ NOT NULL
);
