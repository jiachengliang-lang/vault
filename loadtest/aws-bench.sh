#!/usr/bin/env bash
# Runs the checkout load test against Vault on AWS at increasing fixed rates, and prints one
# Markdown table row per rate: achieved throughput, latency percentiles, errors, and the peak
# CPU of the ECS host and of the database during that run, from CloudWatch, which shows which
# one gives out first. Needs the AWS CLI, k6, Go, and a gateway that lets this machine in.
#
#   JWT_SECRET=... loadtest/aws-bench.sh 50 100 200
#
# Latency here includes the network between the load generator and AWS. The first line printed
# is that round trip on its own (a /healthz request does no work), to tell the two apart.
set -euo pipefail
cd "$(dirname "$0")"
: "${JWT_SECRET:?needed to mint test tokens}"
DURATION=${DURATION:-60s}
OUT=${OUT:-$(mktemp -d)}

output() {
  aws cloudformation describe-stacks --stack-name Vault \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
CLUSTER=${CLUSTER:-$(output ClusterName)}
DB=${DB:-$(output DbInstance)}
if [ -z "$CLUSTER" ] || [ -z "$DB" ] || [ "$CLUSTER" = None ] || [ "$DB" = None ]; then
  echo "Couldn't find the cluster or database in the Vault stack's outputs; is it deployed?" >&2
  exit 1
fi
export GATEWAY=${GATEWAY:-$(../infra/scripts/gateway-url.sh)}

# 500 users, so even 1,000 checkouts a second is ~2 per user, under the per-user rate limit.
(cd .. && go run ./cmd/tokengen -n 500) > tokens.json

# Time from sending the request to the first byte back, on an open connection: k6 reuses
# connections, so the connection setup curl pays on each call would overstate it.
rtt=$(for _ in $(seq 1 30); do curl -s -o /dev/null -w '%{time_starttransfer} %{time_pretransfer}\n' "$GATEWAY/healthz"; done |
  awk '{print $1 - $2}' | sort -n | awk '{t[NR] = $1} END {printf "%.1f", t[int((NR + 1) / 2)] * 1000}')
echo "Gateway: $GATEWAY. Network round trip, median of 30 /healthz requests: ${rtt} ms."
echo

# Peak of the 1-minute averages over a run. ECS reports the cluster's CPU use as a share of the
# host's; RDS reports the database's. The window is widened to whole minutes, since a run shorter
# than a minute can fall between two of CloudWatch's minute marks.
peak_cpu() { # namespace dimension-name dimension-value start-epoch end-epoch
  aws cloudwatch get-metric-statistics --namespace "$1" --metric-name CPUUtilization \
    --dimensions "Name=$2,Value=$3" --start-time $(($4 / 60 * 60)) --end-time $(($5 / 60 * 60 + 60)) \
    --period 60 --statistics Average --query 'max(Datapoints[].Average)' --output text
}

runs=()
for rate in "$@"; do
  start=$(date +%s)
  # A run that misses the latency SLO exits non-zero; it's still a result, so keep going.
  RATE=$rate DURATION=$DURATION k6 run -q --summary-export="$OUT/$rate.json" checkout.js >/dev/null 2>&1 || true
  runs+=("$rate $start $(date +%s)")
  sleep 20 # let the system settle between rates
done

# CloudWatch's 1-minute data arrives a couple of minutes late.
sleep 150

printf "| target/s | achieved/s | p50 ms | p95 ms | p99 ms | errors | host CPU peak | DB CPU peak |\n"
printf "|---|---|---|---|---|---|---|---|\n"
for run in "${runs[@]}"; do
  read -r rate start end <<<"$run"
  host=$(peak_cpu AWS/ECS ClusterName "$CLUSTER" "$start" "$end")
  db=$(peak_cpu AWS/RDS DBInstanceIdentifier "$DB" "$start" "$end")
  python3 - "$OUT/$rate.json" "$rate" "$host" "$db" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))["metrics"]
d, reqs, fails = s["http_req_duration"], s["http_reqs"], s["http_req_failed"]
pct = lambda v: "n/a" if v in ("None", "") else f"{float(v):.0f}%"
print(f"| {sys.argv[2]} | {reqs['rate']:.0f} | {d['p(50)']:.1f} | {d['p(95)']:.1f} | {d['p(99)']:.1f} "
      f"| {fails['value'] * 100:.2f}% | {pct(sys.argv[3])} | {pct(sys.argv[4])} |")
PY
done
