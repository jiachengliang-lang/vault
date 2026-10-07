import { Stack, StackProps } from 'aws-cdk-lib/core';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

/**
 * The IAM user GitHub Actions deploys as.
 *
 * Keyless sign-in (GitHub OIDC) would be better, but this account's organization doesn't allow
 * creating OIDC providers. So the workflow uses an access key for this user, kept in GitHub's
 * encrypted secrets. scripts/github-deploy-key.sh creates or rotates it and hands it straight to
 * GitHub without printing it.
 */
export class CiStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const user = new iam.User(this, 'DeployUser', { userName: 'vault-github-deploy' });

    // The deploy itself goes through the roles `cdk bootstrap` created, like a deploy from a laptop.
    user.addToPolicy(
      new iam.PolicyStatement({
        actions: ['sts:AssumeRole'],
        resources: [`arn:aws:iam::${this.account}:role/cdk-hnb659fds-*`],
      }),
    );
    // What the workflow does around the deploy: check whether Vault is up, find the gateway's
    // address, and mint test tokens for the smoke test.
    user.addToPolicy(
      new iam.PolicyStatement({
        actions: ['cloudformation:DescribeStacks'],
        resources: [`arn:aws:cloudformation:${this.region}:${this.account}:stack/Vault/*`],
      }),
    );
    user.addToPolicy(new iam.PolicyStatement({ actions: ['ec2:DescribeInstances'], resources: ['*'] }));
    user.addToPolicy(
      new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        resources: [`arn:aws:secretsmanager:${this.region}:${this.account}:secret:vault/app-*`],
      }),
    );
  }
}
