#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { AlertsStack } from '../lib/alerts-stack';
import { CiStack } from '../lib/ci-stack';
import { VaultStack } from '../lib/vault-stack';

const app = new cdk.App();
// The account only allows us-east-2.
const env = { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-east-2' };

// Who can reach the gateway, comma-separated: `make aws-up` passes your current IP, the deploy
// workflow passes yours and its own. With none, the gateway is closed to everyone.
const allowedCidrs = String(app.node.tryGetContext('allowedCidrs') ?? '')
  .split(',')
  .map((c) => c.trim())
  .filter(Boolean);

new VaultStack(app, 'Vault', { env, allowedCidrs, appSecretName: 'vault/app' });

// Deployed once, and left in place: it's only an IAM user, which costs nothing.
new CiStack(app, 'VaultCi', { env });

// Also left in place: the topic alarms email through. Deploy with -c alarmEmail=you@example.com
// and click the link AWS sends.
new AlertsStack(app, 'VaultAlerts', { env, email: app.node.tryGetContext('alarmEmail') });
