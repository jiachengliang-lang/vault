#!/usr/bin/env bash
# Creates the app's keys in Secrets Manager once. They live outside the CDK stack on purpose:
# `cdk destroy` shouldn't throw away the keys, and keys must never appear in code or templates.
set -euo pipefail
NAME=${SECRET_NAME:-vault/app}

if aws secretsmanager describe-secret --secret-id "$NAME" >/dev/null 2>&1; then
  echo "$NAME already exists, leaving it alone"
  exit 0
fi

# MASTER_KEY must be base64 of exactly 32 bytes (AES-256); the others are HMAC/JWT keys.
secret=$(python3 -c '
import base64, json, os
print(json.dumps({
    "MASTER_KEY": base64.b64encode(os.urandom(32)).decode(),
    "JWT_SECRET": base64.b64encode(os.urandom(32)).decode(),
    "BLIND_INDEX_KEY": base64.b64encode(os.urandom(32)).decode(),
    "TOKEN_KEY": base64.b64encode(os.urandom(32)).decode(),
}))')
aws secretsmanager create-secret --name "$NAME" --description "Vault application keys" \
  --secret-string "$secret" --query ARN --output text
