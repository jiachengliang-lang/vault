#!/usr/bin/env bash
# Runs SQL against Vault's database on AWS and prints the rows, fields separated by |.
# The database only accepts connections from inside the VPC, so this starts psql as a one-off
# ECS task on the host and reads its output back from CloudWatch Logs. Takes ~20 seconds.
#
#   scripts/aws-sql.sh "SELECT status, count(*) FROM orders GROUP BY 1"
#   scripts/aws-sql.sh < checks.sql
set -euo pipefail
sql=${1:-$(cat)}

output() {
  aws cloudformation describe-stacks --stack-name Vault \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
cluster=$(output ClusterName)
log_group=$(output LogGroup)
overrides=$(python3 -c 'import json,sys; print(json.dumps({"containerOverrides": [{"name": "sql", "command": ["-c", sys.argv[1]]}]}))' "$sql")

task=$(aws ecs run-task --cluster "$cluster" --task-definition "$(output SqlTaskDefinition)" \
  --capacity-provider-strategy "capacityProvider=$(output CapacityProvider),weight=1" \
  --overrides "$overrides" --query 'tasks[0].taskArn' --output text)
aws ecs wait tasks-stopped --cluster "$cluster" --tasks "$task"

code=$(aws ecs describe-tasks --cluster "$cluster" --tasks "$task" --query 'tasks[0].containers[0].exitCode' --output text)
# Log lines can lag the task stopping by a few seconds.
for _ in 1 2 3 4 5; do
  rows=$(aws logs get-log-events --log-group-name "$log_group" --log-stream-name "sql/sql/${task##*/}" \
    --start-from-head --query 'events[].message' --output text 2>/dev/null | tr '\t' '\n') && [ -n "$rows" ] && break
  sleep 2
done
printf '%s\n' "${rows:-}"
if [ "$code" != 0 ]; then
  echo "psql exited with $code" >&2
  exit 1
fi
