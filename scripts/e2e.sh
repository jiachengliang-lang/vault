#!/usr/bin/env bash
# End-to-end smoke test against running services (`make up`, `make topics`, `make run`).
# Exercises the public API the way a client would, then checks analytics, the audit chain and
# metrics. Exits non-zero on the first failure.
set -euo pipefail
cd "$(dirname "$0")/.."
GW=${GATEWAY:-http://localhost:8080}
pass=0
ok()   { echo "  PASS  $1"; pass=$((pass + 1)); }
fail() { echo "  FAIL  $1"; exit 1; }
json() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }
q()    { docker compose exec -T postgres psql -U vault -d vault -qAtc "$1"; }
# call METHOD PATH TOKEN [BODY] [KEY] -> sets $code, $body, $headers
call() {
  local args=(-s -X "$1" "$GW$2" -D /tmp/vault-e2e-headers -o /tmp/vault-e2e-body -w '%{http_code}')
  [ -n "${3:-}" ] && args+=(-H "Authorization: Bearer $3")
  [ -n "${4:-}" ] && args+=(-H 'Content-Type: application/json' -d "$4")
  [ -n "${5:-}" ] && args+=(-H "Idempotency-Key: $5")
  code=$(curl "${args[@]}"); body=$(cat /tmp/vault-e2e-body); headers=$(cat /tmp/vault-e2e-headers)
}
expect() { [ "$code" = "$2" ] && ok "$1 ($code)" || fail "$1: got $code, want $2: $body"; }

echo "== Health"
for port in 8090 8081 8082 8083 8084; do
  for _ in $(seq 1 30); do curl -sf "localhost:$port/readyz" >/dev/null && break; sleep 1; done
  curl -sf "localhost:$port/readyz" >/dev/null && ok "admin :$port ready" || fail "admin :$port not ready"
done

TOKEN=$(go run ./cmd/tokengen 2>/dev/null)
STAFF=$(go run ./cmd/tokengen -role support 2>/dev/null)
KEY="e2e-$(date +%s)-$RANDOM"
T0=$(q "SELECT now()")

echo "== Checkout"
call POST /v1/checkout "$TOKEN" '{"amount_cents": 1999}' "$KEY"; expect "new checkout is created" 201
ORDER=$(echo "$body" | json '["order_id"]')
[ "$(echo "$body" | json '["status"]')" = PAID ] && ok "order is PAID" || fail "order not PAID: $body"
call POST /v1/checkout "$TOKEN" '{"amount_cents": 1999}' "$KEY"; expect "retry with the same key" 200
[ "$(echo "$body" | json '["order_id"]')" = "$ORDER" ] && ok "retry returns the same order" || fail "retry created a new order"
grep -qi "idempotent-replayed: true" <<<"$headers" && ok "retry is marked Idempotent-Replayed" || fail "missing Idempotent-Replayed"
call POST /v1/checkout "$TOKEN" '{"amount_cents": 5}' "$KEY";          expect "same key, different amount" 422
call POST /v1/checkout "$TOKEN" '{"amount_cents": 2000000}' "$KEY-big"; expect "declined card" 402
call POST /v1/checkout "" '{"amount_cents": 1999}' "$KEY-anon";        expect "no token" 401
call GET "/v1/orders/$ORDER" "$TOKEN";                                  expect "owner reads the order" 200
call GET "/v1/orders/$ORDER" "$STAFF";                                  expect "someone else's order" 404
[ "$(q "SELECT count(*) FROM payments WHERE order_id = '$ORDER'")" = 1 ] && ok "charged exactly once" || fail "payment count != 1"

echo "== Profile and privacy"
EMAIL="e2e-$RANDOM-$(date +%s)@example.com"
USER_ID=$(python3 -c "import base64,json,sys; p=sys.argv[1].split('.')[1]; print(json.loads(base64.urlsafe_b64decode(p+'=='))['sub'])" "$TOKEN")
call PUT /v1/me/profile "$TOKEN" "{\"email\":\"$EMAIL\",\"address\":\"Berkeley, CA\"}"; expect "save profile" 200
[ "$(q "SELECT count(*) FROM users WHERE user_id = '$USER_ID' AND position(convert_to('$EMAIL','UTF8') in email_enc) = 0")" = 1 ] \
  && ok "email is encrypted at rest" || fail "plaintext email in the database"
call GET /v1/me/profile "$TOKEN"; expect "read profile" 200
call GET "/v1/support/users/$USER_ID/profile" "$STAFF";                       expect "support access without a reason" 400
call GET "/v1/support/users/$USER_ID/profile?reason=e2e-ticket-1234" "$STAFF"; expect "support access with a reason" 200
call GET "/v1/support/users/$USER_ID/profile?reason=e2e-ticket-1234" "$TOKEN"; expect "customer on the support endpoint" 403
call DELETE /v1/me "$TOKEN";      expect "delete account" 204
call GET /v1/me/profile "$TOKEN"; expect "profile after delete" 404
[ "$(q "SELECT count(*) FROM user_keys WHERE user_id = '$USER_ID'")" = 0 ] && ok "data key destroyed" || fail "data key still present"
call DELETE /v1/me "$TOKEN";      expect "second delete is idempotent" 204
[ "$(q "SELECT string_agg(action, ',' ORDER BY seq) FROM audit_log WHERE subject_id = '$USER_ID'")" = "WRITE_PII,READ_PII,READ_PII,DELETE_USER" ] \
  && ok "every PII access is audited" || fail "audit trail: $(q "SELECT string_agg(action, ',' ORDER BY seq) FROM audit_log WHERE subject_id = '$USER_ID'")"
[ "$(curl -s localhost:8083/audit/verify | json '["ok"]')" = True ] && ok "audit chain verifies" || fail "audit chain broken"

echo "== Events"
for _ in $(seq 1 30); do
  n=$(q "SELECT count(*) FROM analytics_events WHERE ts >= '$T0' AND amount_cents = 1999")
  [ "$n" -ge 2 ] && break; sleep 1
done
[ "$n" -ge 2 ] && ok "order.created and order.paid reached analytics via Kafka" || fail "analytics has $n events"
[ "$(q "SELECT count(*) FROM analytics_events WHERE ts >= '$T0' AND user_token = '$USER_ID'")" = 0 ] \
  && ok "analytics holds no raw user IDs" || fail "raw user ID in analytics"

echo "== Metrics"
# Save the output before grepping: with pipefail, `curl | grep -q` can fail after a match, because
# grep exits early and curl gets SIGPIPE writing the rest.
gw=$(curl -s localhost:8090/metrics); order=$(curl -s localhost:8081/metrics)
grep -q '^checkout_outcomes_total{outcome="paid"} [1-9]' <<<"$gw" && ok "gateway exports checkout metrics" || fail "no checkout metrics"
grep -q '^rpc_server_requests_total' <<<"$order" && ok "order service exports RPC metrics" || fail "no RPC metrics"

echo; echo "All $pass checks passed."
