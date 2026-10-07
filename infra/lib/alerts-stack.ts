import { Stack, StackProps } from 'aws-cdk-lib/core';
import * as events from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import { Construct } from 'constructs';

export const ALARM_TOPIC_NAME = 'vault-alarms';

export interface AlertsStackProps extends StackProps {
  /** Where alarm emails go. AWS sends a confirmation link first. */
  readonly email?: string;
}

/**
 * Where Vault's alarms send notifications. It's a stack of its own, deployed once and left up,
 * because an email subscription has to be confirmed by clicking a link: if the topic came and
 * went with Vault, every work session would start with another confirmation email.
 */
export class AlertsStack extends Stack {
  constructor(scope: Construct, id: string, props: AlertsStackProps) {
    super(scope, id, props);
    const topic = new sns.Topic(this, 'Alarms', { topicName: ALARM_TOPIC_NAME, displayName: 'Vault alarms' });
    if (props.email) {
      topic.addSubscription(new subs.EmailSubscription(props.email));
    }
    // The EventBridge target below gives the topic an access policy, which replaces the default
    // one CloudWatch alarms relied on to publish. So allow this account's alarms explicitly.
    topic.addToResourcePolicy(
      new iam.PolicyStatement({
        actions: ['sns:Publish'],
        principals: [new iam.ServicePrincipal('cloudwatch.amazonaws.com')],
        resources: [topic.topicArn],
        conditions: {
          StringEquals: { 'aws:SourceAccount': this.account },
          ArnLike: { 'aws:SourceArn': `arn:aws:cloudwatch:${this.region}:${this.account}:alarm:vault-*` },
        },
      }),
    );

    // Recovery emails. An alarm's own OK action fires on every move to OK, including each new
    // alarm's first data after a fresh deploy, which meant eight "OK" emails every session. This
    // rule only matches a real recovery: ALARM to OK.
    new events.Rule(this, 'Recovered', {
      description: 'Email when a Vault alarm recovers (ALARM to OK only)',
      eventPattern: {
        source: ['aws.cloudwatch'],
        detailType: ['CloudWatch Alarm State Change'],
        detail: {
          alarmName: [{ prefix: 'vault-' }],
          previousState: { value: ['ALARM'] },
          state: { value: ['OK'] },
        },
      },
      targets: [
        new targets.SnsTopic(topic, {
          message: events.RuleTargetInput.fromText(
            `RECOVERED: ${events.EventField.fromPath('$.detail.alarmName')} is back to OK at ` +
              `${events.EventField.fromPath('$.time')}. ${events.EventField.fromPath('$.detail.state.reason')}`,
          ),
        }),
      ],
    });
  }
}
