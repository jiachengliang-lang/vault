#!/usr/bin/env bash
# Chaos test: steady checkout load while breaking things, then check that nothing was lost,
# duplicated or left inconsistent. Needs `make up` and `make run` in another terminal.
#
# Timeline (RATE checkouts/s for 100 s):
#   t=15s  freeze Postgres for 10 s     every service's queries hang
#   t=40s  stop Kafka for 15 s          checkout should keep working; events wait in the outbox
#   t=70s  kill the payment service     restarted 10 s later (outside `make run`: stop it with pkill)
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=${RATE:-300}
q() { docker compose exec -T postgres psql -U vault -d vault -qAtc "$1"; }

bin/tokengen -n 500 > loadtest/tokens.json 2>/dev/null
T0=$(q "SELECT now()")
START=$(date +%s)
at() { local wait=$(( $1 - ($(date +%s) - START) )); (( wait > 0 )) && sleep "$wait"; return 0; }

RATE=$RATE DURATION=100s k6 run -q --summary-export=/tmp/vault-chaos.json loadtest/checkout.js >/dev/null 2>&1 &
K6=$!
at 15; echo "t=15s  freezing Postgres for 10 s";  docker compose pause postgres >/dev/null; sleep 10; docker compose unpause postgres >/dev/null
at 40; echo "t=40s  stopping Kafka for 15 s";     docker compose stop redpanda >/dev/null;  sleep 15; docker compose start redpanda >/dev/null
at 70; echo "t=70s  killing the payment service"; pkill -f "^bin/payment$" || true;        sleep 10
nohup bin/payment > /tmp/vault-payment.log 2>&1 &
echo "t=80s  payment service restarted"
wait $K6 || true

echo; echo "== What clients saw, per 10 s window (from gateway metrics)"
END=$(date +%s)
sleep 10 # let Prometheus scrape the last window
curl -s -G localhost:9090/api/v1/query_range --data-urlencode "start=$START" --data-urlencode "end=$END" --data-urlencode "step=10" \
  --data-urlencode 'query=sum by (code) (increase(http_requests_total{route="/v1/checkout"}[10s]))' |
python3 -c '
import json, sys
series = {r["metric"]["code"]: {int(t): float(v) for t, v in r["values"]} for r in json.load(sys.stdin)["data"]["result"]}
times = sorted(t for t in {t for s in series.values() for t in s} if any(series[c].get(t, 0) for c in series))
if times:
    t0 = times[0]
    codes = sorted(series)
    print("  t      " + "  ".join(f"{c:>6}" for c in codes))
    for t in times:
        print(f"  {t - t0:>4}s  " + "  ".join(f"{series[c].get(t, 0):>6.0f}" for c in codes))'
curl -s -G localhost:9090/api/v1/query --data-urlencode 'query=sum by (reason) (increase(gateway_upstream_failures_total[3m]))' |
  python3 -c 'import json,sys; print("  upstream failures by reason:", {r["metric"]["reason"]: round(float(r["value"][1])) for r in json.load(sys.stdin)["data"]["result"]})'

echo; echo "== Waiting for the outbox and pipeline to drain"
for _ in $(seq 1 60); do [ "$(q "SELECT count(*) FROM outbox")" = 0 ] && break; sleep 2; done
sleep 5

echo; echo "== Consistency checks (orders created since the test started)"
check() { # name, SQL returning a count, expected
  local got; got=$(q "$2")
  if [ "$got" = "$3" ]; then echo "  PASS  $1 ($got)"; else echo "  FAIL  $1: got $got, want $3"; fi
}
report() { echo "  INFO  $1: $(q "$2")"; }
W="o.created_at >= '$T0'"
report "orders created" "SELECT count(*) FROM orders o WHERE $W"
report "orders by status" "SELECT string_agg(status || '=' || n, ', ') FROM (SELECT status, count(*) n FROM orders o WHERE $W GROUP BY 1) x"
check "orders charged more than once" "SELECT count(*) FROM (SELECT p.order_id FROM payments p JOIN orders o USING (order_id) WHERE $W GROUP BY 1 HAVING count(*) > 1) x" 0
check "PAID orders without a successful payment" "SELECT count(*) FROM orders o WHERE $W AND o.status = 'PAID' AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.order_id AND p.status = 'SUCCEEDED')" 0
check "events stuck in the outbox" "SELECT count(*) FROM outbox" 0
check "order.created events missing from analytics" "SELECT (SELECT count(*) FROM orders o WHERE $W) - (SELECT count(*) FROM analytics_events WHERE event_type = 'order.created' AND ts >= '$T0')" 0
check "order.paid events missing from analytics" "SELECT (SELECT count(*) FROM orders o WHERE $W AND status = 'PAID') - (SELECT count(*) FROM analytics_events WHERE event_type = 'order.paid' AND ts >= '$T0')" 0
# A client that gets a 503 is told to retry with the same key; k6 doesn't. These are checkouts
# abandoned mid-way, which a retry (or a reconciliation job) would finish.
report "charged, but order still PENDING (would be finished by a client retry)" "SELECT count(*) FROM orders o JOIN payments p USING (order_id) WHERE $W AND o.status = 'PENDING' AND p.status = 'SUCCEEDED'"
report "payments stuck PENDING (would be finished by a client retry)" "SELECT count(*) FROM payments p JOIN orders o USING (order_id) WHERE $W AND p.status = 'PENDING'"
