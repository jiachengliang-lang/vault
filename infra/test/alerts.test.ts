import * as cdk from 'aws-cdk-lib/core';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { AlertsStack } from '../lib/alerts-stack';

const template = Template.fromStack(
  new AlertsStack(new cdk.App(), 'Alerts', {
    env: { account: '111111111111', region: 'us-east-2' },
    email: 'someone@example.com',
  }),
);

test('recovery emails only for ALARM to OK, not for a new alarm getting its first data', () => {
  template.hasResourceProperties('AWS::Events::Rule', {
    EventPattern: {
      source: ['aws.cloudwatch'],
      'detail-type': ['CloudWatch Alarm State Change'],
      detail: {
        alarmName: [{ prefix: 'vault-' }],
        previousState: { value: ['ALARM'] },
        state: { value: ['OK'] },
      },
    },
  });
});

// Giving the topic an access policy replaces the default one, so both publishers must be listed:
// without the CloudWatch statement, alarms would silently stop emailing.
test('both CloudWatch alarms and EventBridge can publish to the topic', () => {
  template.hasResourceProperties('AWS::SNS::TopicPolicy', {
    PolicyDocument: {
      Statement: Match.arrayWith([
        Match.objectLike({ Action: 'sns:Publish', Principal: { Service: 'cloudwatch.amazonaws.com' } }),
        Match.objectLike({ Action: 'sns:Publish', Principal: { Service: 'events.amazonaws.com' } }),
      ]),
    },
  });
});
