#!/usr/bin/env bash
# Restarts all services from fresh binaries and waits until the pipeline has caught up and
# rejoined its consumer group, so every benchmark starts from the same state.
set -euo pipefail
cd "$(dirname "$0")/.."
LOGS=${LOGS:-/tmp/vault-logs}; mkdir -p "$LOGS"
pkill -f "(^|/)bin/(order|payment|user|gateway|pipeline)$" || true
sleep 2
go build -o bin/ ./cmd/...
for s in order payment user gateway pipeline; do nohup bin/$s > "$LOGS/$s.log" 2>&1 & done
until [ "$(docker compose exec -T redpanda rpk group describe analytics-pipeline 2>/dev/null | awk '/^STATE/{s=$2} /TOTAL-LAG/{l=$2} END{print s"/"l}')" = "Stable/0" ]; do sleep 2; done
sleep 3
echo "services up, pipeline caught up (TRACE_SAMPLE_RATIO=${TRACE_SAMPLE_RATIO:-1})"
