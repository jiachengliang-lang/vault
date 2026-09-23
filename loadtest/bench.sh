#!/usr/bin/env bash
# Runs the checkout load test at increasing fixed rates and prints one table row per rate:
# achieved throughput, latency percentiles, error rate, Postgres commits and WAL bytes per checkout
# (everything the system wrote, pipeline included), and analytics freshness. Needs `make up` and `make run`.
#
#   loadtest/bench.sh 100 200 400 600 800
set -euo pipefail
cd "$(dirname "$0")"
export COMPOSE_FILE="$(cd .. && pwd)/docker-compose.yml"
DURATION=${DURATION:-30s}
OUT=$(mktemp -d)

pg_counters() { # total commits and WAL bytes written, straight from Postgres
  docker compose exec -T postgres psql -U vault -d vault -Atc \
    "SELECT (SELECT xact_commit FROM pg_stat_database WHERE datname = 'vault') || ' ' || (SELECT wal_bytes FROM pg_stat_wal)"
}

freshness() { # p99 time from an order event happening to it landing in analytics, over the run
  curl -s -G localhost:9090/api/v1/query --data-urlencode "query=histogram_quantile(0.99, sum by (le) (increase(pipeline_event_end_to_end_seconds_bucket[$1])))" |
    python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "nan")'
}

printf "| target rps | achieved rps | p50 ms | p95 ms | p99 ms | errors | commits / checkout | WAL KB / checkout | analytics freshness p99 |\n|---|---|---|---|---|---|---|---|---|\n"
for rate in "${@:-100 200 400}"; do
  pg_before=$(pg_counters)
  RATE=$rate DURATION=$DURATION k6 run -q --summary-export="$OUT/$rate.json" checkout.js >/dev/null 2>&1 || true
  sleep 6 # let the pipeline finish the run's events and Prometheus scrape
  pg_after=$(pg_counters)
  fresh=$(freshness "${DURATION%s}s")
  python3 - "$OUT/$rate.json" "$rate" "$pg_before" "$pg_after" "$fresh" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))["metrics"]
d, reqs, fails = s["http_req_duration"], s["http_reqs"], s["http_req_failed"]
n = reqs["count"]
(c0, w0), (c1, w1) = (map(float, a.split()) for a in sys.argv[3:5])
commits, wal_kb = (c1 - c0) / max(n, 1), (w1 - w0) / max(n, 1) / 1024
fresh = float(sys.argv[5])
fresh_s = "n/a" if fresh != fresh else (f"> 1800 s" if fresh >= 1800 else f"{fresh:.2f} s")
print(f"| {sys.argv[2]} | {reqs['rate']:.0f} | {d['p(50)']:.1f} | {d['p(95)']:.1f} | {d['p(99)']:.1f} | {fails['value']*100:.2f}% | {commits:.2f} | {wal_kb:.2f} | {fresh_s} |")
PY
  sleep 5 # let the system settle between rates
done
