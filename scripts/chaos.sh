#!/usr/bin/env bash
# Chaos drill: run `make load` in another terminal, then run this.
# Expect: error rate spikes, circuit breaker opens, service recovers, no double charges, no lost events.
set -euo pipefail

echo "1) Pausing Postgres for 10s (simulates DB stall)..."
docker compose pause postgres; sleep 10; docker compose unpause postgres

echo "2) Killing the broker for 15s (simulates Kafka outage; outbox should buffer)..."
docker compose stop redpanda; sleep 15; docker compose start redpanda

echo "Done. Verify: SELECT count(*) FROM outbox WHERE published_at IS NULL;  -> should drain to 0"
echo "       SELECT idempotency_key, count(*) FROM payments GROUP BY 1 HAVING count(*) > 1;  -> should be empty"
