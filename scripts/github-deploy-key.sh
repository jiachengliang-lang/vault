#!/usr/bin/env bash
# Creates a fresh access key for the vault-github-deploy user and stores it as GitHub Actions
# secrets, then deletes the user's older keys. Run it once after `cdk deploy VaultCi`, and again
# whenever you want to rotate the key. The key goes straight from AWS to GitHub; it's never printed.
set -euo pipefail
USER_NAME=vault-github-deploy

old=$(aws iam list-access-keys --user-name "$USER_NAME" --query 'AccessKeyMetadata[].AccessKeyId' --output text)
key=$(aws iam create-access-key --user-name "$USER_NAME" --output json)
python3 -c 'import json,sys; print(json.load(sys.stdin)["AccessKey"]["AccessKeyId"], end="")' <<<"$key" | gh secret set AWS_ACCESS_KEY_ID
python3 -c 'import json,sys; print(json.load(sys.stdin)["AccessKey"]["SecretAccessKey"], end="")' <<<"$key" | gh secret set AWS_SECRET_ACCESS_KEY
unset key

for k in $old; do aws iam delete-access-key --user-name "$USER_NAME" --access-key-id "$k"; done
echo "GitHub now has a new key for $USER_NAME; older keys deleted: ${old:-none}"
