#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { VaultStack } from '../lib/vault-stack';

const app = new cdk.App();

// Only this address range can reach the gateway. `make aws-up` passes your current IP.
const allowedCidr = app.node.tryGetContext('allowedCidr');
if (!allowedCidr) {
  throw new Error('pass -c allowedCidr=<your ip>/32 (or run `make aws-up`)');
}

new VaultStack(app, 'Vault', {
  // The account only allows us-east-2.
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: 'us-east-2' },
  allowedCidr,
  appSecretName: 'vault/app',
});
