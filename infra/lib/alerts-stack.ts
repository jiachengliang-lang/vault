import { Stack, StackProps } from 'aws-cdk-lib/core';
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
  }
}
