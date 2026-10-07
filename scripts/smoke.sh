#!/usr/bin/env bash
# Checks a deployed Vault through its public API only, so it works from anywhere: the deploy
# workflow runs it after every deploy and rolls back if it fails. (e2e.sh goes deeper, but needs
# the local database and admin ports.) Needs JWT_SECRET to mint test tokens.
set -euo pipefail
cd "$(dirname "$0")/.."
GW=${GATEWAY:?set GATEWAY, e.g. http://1.2.3.4:8080}
pass=0
ok()   { echo "  PASS  $1"; pass=$((pass + 1)); }
fail() { echo "  FAIL  $1"; exit 1; }
json() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }
# call METHOD PATH TOKEN [BODY] [KEY] -> sets $code, $body, $headers
call() {
  local args=(-s -m 10 -X "$1" "$GW$2" -D /tmp/vault-smoke-headers -o /tmp/vault-smoke-body -w '%{http_code}')
  [ -n "${3:-}" ] && args+=(-H "Authorization: Bearer $3")
  [ -n "${4:-}" ] && args+=(-H 'Content-Type: application/json' -d "$4")
  [ -n "${5:-}" ] && args+=(-H "Idempotency-Key: $5")
  code=$(curl "${args[@]}" || echo 000); body=$(cat /tmp/vault-smoke-body 2>/dev/null || true)
  headers=$(cat /tmp/vault-smoke-headers 2>/dev/null || true)
}
expect() { [ "$code" = "$2" ] && ok "$1 ($code)" || fail "$1: got $code, want $2: $body"; }

echo "== Up"
# A fresh deploy can take a minute to start every container.
for _ in $(seq 1 60); do curl -sf -m 5 "$GW/healthz" >/dev/null && break; sleep 3; done
call GET /healthz; expect "gateway answers" 200

TOKEN=$(go run ./cmd/tokengen 2>/dev/null)
KEY="smoke-$(date +%s)-$RANDOM"

echo "== Checkout"
call POST /v1/checkout "" '{"amount_cents": 1999}' "$KEY"; expect "no token is rejected" 401
call POST /v1/checkout "$TOKEN" '{"amount_cents": 1999}' "$KEY"; expect "checkout is created" 201
ORDER=$(echo "$body" | json '["order_id"]')
[ "$(echo "$body" | json '["status"]')" = PAID ] && ok "order is PAID" || fail "order not PAID: $body"
call POST /v1/checkout "$TOKEN" '{"amount_cents": 1999}' "$KEY"; expect "retry with the same key" 200
[ "$(echo "$body" | json '["order_id"]')" = "$ORDER" ] && ok "retry returns the same order" || fail "retry created a new order"
grep -qi "idempotent-replayed: true" <<<"$headers" && ok "retry is marked Idempotent-Replayed" || fail "missing Idempotent-Replayed"

echo "== Profile"
EMAIL="smoke-$RANDOM@example.com"
call PUT /v1/me/profile "$TOKEN" "{\"email\": \"$EMAIL\", \"address\": \"somewhere\"}"; expect "profile is saved" 200
call GET /v1/me/profile "$TOKEN"; expect "profile is read" 200
[ "$(echo "$body" | json '["email"]')" = "$EMAIL" ] && ok "profile round-trips" || fail "wrong profile: $body"
call DELETE /v1/me "$TOKEN"; expect "account is deleted" 204
call GET /v1/me/profile "$TOKEN"; expect "deleted profile is gone" 404

echo; echo "All $pass checks passed."
