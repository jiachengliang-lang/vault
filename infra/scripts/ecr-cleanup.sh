#!/usr/bin/env bash
# Keeps only the 10 most recent images (the last 5 deploys: app + migrate) in the repository
# `cdk bootstrap` created for container images. CDK tags every image with its content hash, so the
# bootstrap's own rule, which only expires untagged images after a year, never removes anything,
# and every deploy left ~150 MB behind. Deleting old images is safe: deploying an older version
# rebuilds and pushes any image that's missing.
#
# The repository belongs to the CDKToolkit stack, so re-running `cdk bootstrap` resets this policy.
# Run this script again afterwards.
set -euo pipefail
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REPO=cdk-hnb659fds-container-assets-$ACCOUNT-${AWS_REGION:-us-east-2}

aws ecr put-lifecycle-policy --repository-name "$REPO" --lifecycle-policy-text '{
  "rules": [
    {
      "rulePriority": 1,
      "description": "Untagged images should not exist, but expire any older than one year",
      "selection": {"tagStatus": "untagged", "countType": "sinceImagePushed", "countUnit": "days", "countNumber": 365},
      "action": {"type": "expire"}
    },
    {
      "rulePriority": 2,
      "description": "Keep the 10 most recent images (the last 5 deploys)",
      "selection": {"tagStatus": "any", "countType": "imageCountMoreThan", "countNumber": 10},
      "action": {"type": "expire"}
    }
  ]
}' --query repositoryName --output text
