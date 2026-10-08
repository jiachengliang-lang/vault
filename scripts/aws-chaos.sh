#!/usr/bin/env bash
# Chaos test on AWS: steady checkout load while breaking things, then check in the database that
# nothing was charged twice, lost, or left inconsistent. scripts/chaos.sh is the local version.
# Needs the AWS CLI, k6, Go, and a gateway that lets this machine in (ADMIN_CIDR).
#
# Timeline, RATE checkouts/s for 7 minutes:
#   t=30s   reboot the database          every query fails until it's back (a minute or two)
#   t=210s  stop the App task            gateway, order, payment and pipeline die mid-request;
#                                        ECS starts a new one
#   t=300s  stop the User task           profiles fail until ECS replaces it; checkout carries on
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=${RATE:-100}
DURATION=${DURATION:-420s}
OUT=$(mktemp -d)

output() {
  aws cloudformation describe-stacks --stack-name Vault \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
CLUSTER=$(output ClusterName)
DB=$(output DbInstance)
service() { aws ecs list-services --cluster "$CLUSTER" --query 'serviceArns' --output text | tr '\t' '\n' | grep "$1"; }
stop_task() { # service-name-fragment reason
  local task
  task=$(aws ecs list-tasks --cluster "$CLUSTER" --service-name "$(service "$1")" --query 'taskArns[0]' --output text)
  aws ecs stop-task --cluster "$CLUSTER" --task "$task" --reason "$2" --query 'task.lastStatus' --output text >/dev/null
}

export GATEWAY=${GATEWAY:-$(infra/scripts/gateway-url.sh)}
JWT_SECRET=$(aws secretsmanager get-secret-value --secret-id vault/app --query SecretString --output text |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["JWT_SECRET"])')
export JWT_SECRET
go run ./cmd/tokengen -n 500 > loadtest/tokens.json 2>/dev/null

T0=$(date -u +%FT%TZ)
START=$(date +%s)
at() { local wait=$(( $1 - ($(date +%s) - START) )); (( wait > 0 )) && sleep "$wait"; return 0; }
log() { echo "t=$(( $(date +%s) - START ))s  $*"; }

# Every request's status and time go to a file, to show what clients saw through each failure.
RATE=$RATE DURATION=$DURATION k6 run -q --out json="$OUT/k6.json" --summary-export="$OUT/summary.json" \
  loadtest/checkout.js >/dev/null 2>&1 &
K6=$!

at 30;  log "rebooting the database"
aws rds reboot-db-instance --db-instance-identifier "$DB" --query 'DBInstance.DBInstanceStatus' --output text >/dev/null
aws rds wait db-instance-available --db-instance-identifier "$DB"
log "database is back"

at 210; log "stopping the App task (gateway, order, payment, pipeline)"
stop_task AppService "chaos test"
at 300; log "stopping the User task"
stop_task UserService "chaos test"
wait $K6 || true
log "load finished"

echo; echo "== What clients saw, per 15 s (000 = no response: the gateway itself was down)"
python3 - "$OUT/k6.json" <<'PY'
import collections, datetime, json, sys
windows = collections.defaultdict(collections.Counter)
for line in open(sys.argv[1]):
    p = json.loads(line)
    if p.get("type") != "Point" or p.get("metric") != "http_reqs":
        continue
    t = datetime.datetime.fromisoformat(p["data"]["time"]).timestamp()
    windows[int(t // 15 * 15)][p["data"]["tags"].get("status", "000")] += 1
t0 = min(windows)
codes = sorted({c for w in windows.values() for c in w})
print("  t      " + "  ".join(f"{c:>6}" for c in codes))
for t in sorted(windows):
    print(f"  {t - t0:>4}s  " + "  ".join(f"{windows[t].get(c, 0):>6}" for c in codes))
PY

echo; echo "== Waiting for the outbox to drain"
for _ in $(seq 1 10); do [ "$(scripts/aws-sql.sh "SELECT count(*) FROM outbox")" = 0 ] && break; sleep 10; done

echo; echo "== Consistency checks (orders created since the test started)"
W="o.created_at >= '$T0'"
scripts/aws-sql.sh "
SELECT 'orders created|' || count(*) FROM orders o WHERE $W;
SELECT 'orders by status|' || coalesce(string_agg(status || '=' || n, ', '), '') FROM (SELECT status, count(*) n FROM orders o WHERE $W GROUP BY 1) x;
SELECT 'check|orders charged more than once|0|' || count(*) FROM (SELECT p.order_id FROM payments p JOIN orders o USING (order_id) WHERE $W GROUP BY 1 HAVING count(*) > 1) x;
SELECT 'check|PAID orders without a successful payment|0|' || count(*) FROM orders o WHERE $W AND o.status = 'PAID' AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.order_id AND p.status = 'SUCCEEDED');
SELECT 'check|events stuck in the outbox|0|' || count(*) FROM outbox;
SELECT 'check|order.created events missing from analytics|0|' || ((SELECT count(*) FROM orders o WHERE $W) - (SELECT count(*) FROM analytics_events WHERE event_type = 'order.created' AND ts >= '$T0'));
SELECT 'check|order.paid events missing from analytics|0|' || ((SELECT count(*) FROM orders o WHERE $W AND status = 'PAID') - (SELECT count(*) FROM analytics_events WHERE event_type = 'order.paid' AND ts >= '$T0'));
SELECT 'charged, but order still PENDING (a client retry or a reconciliation job would finish it)|' || count(*) FROM orders o JOIN payments p USING (order_id) WHERE $W AND o.status = 'PENDING' AND p.status = 'SUCCEEDED';
SELECT 'payments stuck PENDING (a client retry would finish it)|' || count(*) FROM payments p JOIN orders o USING (order_id) WHERE $W AND p.status = 'PENDING';
" | python3 -c '
import sys
for line in sys.stdin:
    parts = line.rstrip("\n").split("|")
    if parts[0] == "check":
        _, name, want, got = parts
        verdict, expected = ("PASS", "") if got == want else ("FAIL", ", want " + want)
        print(f"  {verdict}  {name}: {got}{expected}")
    elif len(parts) == 2:
        print(f"  INFO  {parts[0]}: {parts[1]}")'
