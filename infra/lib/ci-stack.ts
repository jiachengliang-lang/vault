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
    // What the workflows do around a deploy: check whether Vault is up, find the gateway's
    // address, and mint test tokens for the smoke and load tests.
    user.addToPolicy(
      new iam.PolicyStatement({
        actions: ['cloudformation:DescribeStacks'],
        resources: [`arn:aws:cloudformation:${this.region}:${this.account}:stack/Vault/*`],
      }),
    );
    user.addToPolicy(new iam.PolicyStatement({ actions: ['ec2:DescribeInstances'], resources: ['*'] }));
    // The load test workflow: let the runner's address reach the gateway for the length of the
    // test (only on Vault's own security groups), and read CPU metrics to find the bottleneck.
    user.addToPolicy(
      new iam.PolicyStatement({
        actions: ['ec2:AuthorizeSecurityGroupIngress', 'ec2:RevokeSecurityGroupIngress'],
        resources: [`arn:aws:ec2:${this.region}:${this.account}:security-group/*`],
        conditions: { StringEquals: { 'aws:ResourceTag/aws:cloudformation:stack-name': 'Vault' } },
      }),
    );
    user.addToPolicy(new iam.PolicyStatement({ actions: ['cloudwatch:GetMetricStatistics'], resources: ['*'] }));
    user.addToPolicy(
      new iam.PolicyStatement({
        actions: ['secretsmanager:GetSecretValue'],
        resources: [`arn:aws:secretsmanager:${this.region}:${this.account}:secret:vault/app-*`],
      }),
    );
  }
}
