import * as cdk from 'aws-cdk-lib/core';
import { Template } from 'aws-cdk-lib/assertions';
import { CiStack } from '../lib/ci-stack';

const template = Template.fromStack(
  new CiStack(new cdk.App(), 'Ci', { env: { account: '111111111111', region: 'us-east-2' } }),
);

test('the deploy user gets only what the workflow uses', () => {
  const actions = Object.values(template.findResources('AWS::IAM::Policy'))
    .flatMap((p) => p.Properties.PolicyDocument.Statement)
    .flatMap((st: { Action: string | string[] }) => st.Action)
    .sort();
  expect(actions).toEqual([
    'cloudformation:DescribeStacks',
    'cloudwatch:GetMetricStatistics',
    'ec2:AuthorizeSecurityGroupIngress',
    'ec2:DescribeInstances',
    'ec2:RevokeSecurityGroupIngress',
    'secretsmanager:GetSecretValue',
    'sts:AssumeRole',
  ]);
});

test("it can only open Vault's own security groups", () => {
  const statements = Object.values(template.findResources('AWS::IAM::Policy')).flatMap(
    (p) => p.Properties.PolicyDocument.Statement,
  );
  const sg = statements.find((st: { Action: string[] }) => [st.Action].flat().includes('ec2:AuthorizeSecurityGroupIngress'));
  expect(sg.Condition).toEqual({ StringEquals: { 'aws:ResourceTag/aws:cloudformation:stack-name': 'Vault' } });
});

test('no access key in the template, where CloudFormation would keep a copy', () => {
  template.resourceCountIs('AWS::IAM::AccessKey', 0);
});
