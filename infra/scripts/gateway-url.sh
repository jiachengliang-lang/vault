#!/usr/bin/env bash
# Prints the gateway's URL. The host gets a new public IP whenever it's replaced, so look it up.
set -euo pipefail
group=$(aws cloudformation describe-stacks --stack-name Vault \
  --query "Stacks[0].Outputs[?OutputKey=='HostGroup'].OutputValue" --output text)
ip=$(aws ec2 describe-instances \
  --filters "Name=tag:aws:autoscaling:groupName,Values=$group" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "http://$ip:8080"
