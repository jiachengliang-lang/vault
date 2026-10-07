import * as path from 'node:path';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib/core';
import * as autoscaling from 'aws-cdk-lib/aws-autoscaling';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cw_actions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sns from 'aws-cdk-lib/aws-sns';
import { Construct } from 'constructs';
import { ALARM_TOPIC_NAME } from './alerts-stack';

const RUNBOOK_URL = 'https://github.com/jiachengliang-lang/vault/blob/main/docs/runbook.md';

export interface VaultStackProps extends StackProps {
  /** Who can reach the gateway, e.g. ["203.0.113.7/32"]. */
  readonly allowedCidrs: string[];
  /** Secrets Manager secret holding the app keys (see scripts/create-secrets.sh). */
  readonly appSecretName: string;
}

/**
 * Vault on AWS, sized to stay inside the free plan: one small EC2 host runs every container
 * (the five services and Redpanda) with host networking, next to a single-AZ Postgres.
 * There's no NAT gateway or load balancer, the two things that would cost money around the clock.
 */
export class VaultStack extends Stack {
  constructor(scope: Construct, id: string, props: VaultStackProps) {
    super(scope, id, props);
    const repoRoot = path.join(__dirname, '..', '..');

    // Public subnets for the host (it pulls images straight from ECR, so no NAT gateway),
    // isolated subnets for the database, which has no route to the internet at all.
    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [
        { name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
        { name: 'db', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 },
      ],
    });

    // Only the gateway's port is reachable, and only from allowedCidrs. No SSH: shell access goes
    // through Session Manager, which needs no open port.
    const hostSg = new ec2.SecurityGroup(this, 'HostSg', { vpc, description: 'ECS host' });
    for (const cidr of props.allowedCidrs) {
      hostSg.addIngressRule(ec2.Peer.ipv4(cidr), ec2.Port.tcp(8080), 'Vault gateway');
    }
    const hostRole = new iam.Role(this, 'HostRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore')],
    });
    const host = new autoscaling.AutoScalingGroup(this, 'Host', {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      minCapacity: 1,
      maxCapacity: 1,
      launchTemplate: new ec2.LaunchTemplate(this, 'HostTemplate', {
        instanceType: new ec2.InstanceType('t4g.small'), // free-tier eligible, 2 GiB, ARM like the Mac that builds the image
        machineImage: ecs.EcsOptimizedImage.amazonLinux2023(ecs.AmiHardwareType.ARM),
        userData: ec2.UserData.forLinux(),
        role: hostRole,
        securityGroup: hostSg,
        associatePublicIpAddress: true,
        requireImdsv2: true, // containers can't use the old metadata API to grab the host's credentials
      }),
    });
    // Keep the host's route to the internet until the host itself is gone. Otherwise `cdk destroy`
    // deletes the routes first (nothing else depends on them), the ECS agent loses contact, ECS
    // can never confirm the tasks stopped, and deleting the services hangs.
    host.node.addDependency(...vpc.publicSubnets.map((s) => s.internetConnectivityEstablished));
    const cluster = new ecs.Cluster(this, 'Cluster', { vpc });
    const capacity = new ecs.AsgCapacityProvider(this, 'HostCapacity', {
      autoScalingGroup: host,
      enableManagedTerminationProtection: false, // lets `cdk destroy` remove the host without waiting
    });
    cluster.addAsgCapacityProvider(capacity);
    // Services place tasks through the capacity provider, so a task waits for the host instead of
    // failing placement. The first deploy failed that way: ECS tried to start Kafka before the
    // host existed, and the circuit breaker rolled everything back.
    // Each service also depends on the host and the cluster (which includes the capacity provider
    // link), so on `cdk destroy` the services go first. Without that, deleting the link failed with
    // "capacity provider is in use" and the teardown had to be run twice.
    const serviceDefaults = {
      cluster,
      desiredCount: 1,
      capacityProviderStrategies: [{ capacityProvider: capacity.capacityProviderName, weight: 1 }],
      // One host and host networking: the old task has to stop before the new one can take the ports.
      minHealthyPercent: 0,
      maxHealthyPercent: 100,
      circuitBreaker: { rollback: true },
    };

    const db = new rds.DatabaseInstance(this, 'Db', {
      engine: rds.DatabaseInstanceEngine.postgres({ version: rds.PostgresEngineVersion.VER_16 }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE4_GRAVITON, ec2.InstanceSize.MICRO),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      credentials: rds.Credentials.fromGeneratedSecret('vault'),
      databaseName: 'vault',
      allocatedStorage: 20,
      storageType: rds.StorageType.GP3,
      storageEncrypted: true,
      multiAz: false,
      publiclyAccessible: false,
      backupRetention: Duration.days(1),
      // The whole stack is torn down between work sessions, so skip the final snapshot.
      deletionProtection: false,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    db.connections.allowDefaultPortFrom(host, 'ECS host');

    const logGroup = new logs.LogGroup(this, 'Logs', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const logging = (prefix: string) => ecs.LogDrivers.awsLogs({ logGroup, streamPrefix: prefix });

    // ---------- Kafka (Redpanda) ----------
    // Its own service, so redeploying the app doesn't restart the broker.
    const redpandaImage = ecs.ContainerImage.fromRegistry('docker.redpanda.com/redpandadata/redpanda:latest');
    const kafkaTask = new ecs.Ec2TaskDefinition(this, 'KafkaTask', { networkMode: ecs.NetworkMode.HOST });
    const redpanda = kafkaTask.addContainer('redpanda', {
      image: redpandaImage,
      command: [
        'redpanda', 'start', '--mode', 'dev-container', '--smp', '1', '--memory', '512M',
        '--kafka-addr', '0.0.0.0:9092', '--advertise-kafka-addr', 'localhost:9092',
        // Redpanda's defaults (8081, 8082) are the order and payment admin ports, and with host
        // networking everything shares one set of ports.
        '--schema-registry-addr', '0.0.0.0:18081', '--pandaproxy-addr', '0.0.0.0:18082',
      ],
      memoryReservationMiB: 700,
      logging: logging('kafka'),
      healthCheck: {
        command: ['CMD-SHELL', "rpk cluster health | grep -q 'Healthy:.*true'"],
        interval: Duration.seconds(10),
        retries: 6,
        startPeriod: Duration.seconds(30),
      },
    });
    // Same as `make topics`. The relay stalls on a topic that doesn't exist, so create them up front.
    const topics = kafkaTask.addContainer('topics', {
      image: redpandaImage,
      essential: false,
      entryPoint: ['/bin/bash', '-c'],
      command: [
        'rpk topic create order.events payment.events order.events.dlq user.events -p 6 -X brokers=localhost:9092 || true',
      ],
      memoryReservationMiB: 64,
      logging: logging('kafka'),
    });
    topics.addContainerDependencies({ container: redpanda, condition: ecs.ContainerDependencyCondition.HEALTHY });

    const kafka = new ecs.Ec2Service(this, 'Kafka', { ...serviceDefaults, taskDefinition: kafkaTask });
    kafka.node.addDependency(host, cluster);

    // ---------- the five services, plus the schema migration that runs before them ----------
    const appImage = ecs.ContainerImage.fromAsset(repoRoot, { platform: Platform.LINUX_ARM64 });
    const migrateImage = ecs.ContainerImage.fromAsset(path.join(repoRoot, 'db'), { platform: Platform.LINUX_ARM64 });
    const appSecret = secretsmanager.Secret.fromSecretNameV2(this, 'AppSecret', props.appSecretName);
    const appKey = (field: string) => ecs.Secret.fromSecretsManager(appSecret, field);

    // pgx and psql both read PGHOST/PGUSER/PGPASSWORD, so the password never has to be pasted
    // into a connection string.
    const dbEnv = { PGHOST: db.instanceEndpoint.hostname, PGDATABASE: 'vault', PGSSLMODE: 'require' };
    const dbSecrets = {
      PGUSER: ecs.Secret.fromSecretsManager(db.secret!, 'username'),
      PGPASSWORD: ecs.Secret.fromSecretsManager(db.secret!, 'password'),
    };

    // ---------- users' keys: master key in KMS, wrapped data keys in DynamoDB ----------
    // The master key never leaves KMS; every unwrap is an API call that CloudTrail records.
    const masterKey = new kms.Key(this, 'MasterKey', {
      description: 'Vault master key: wraps each user\'s data key',
      enableKeyRotation: true,
      removalPolicy: RemovalPolicy.DESTROY,
      pendingWindow: Duration.days(7), // the shortest AWS allows
    });
    // Wrapped data keys live here rather than in Postgres, so a database backup holds no keys,
    // and restoring one can't bring back the key of a user who was deleted.
    const keysTable = new dynamodb.TableV2(this, 'UserKeys', {
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      // Off on purpose: point-in-time recovery would keep a deleted user's key restorable for
      // 35 days, undoing crypto-shredding for that long.
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: false },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const common = {
      // Host, user and password come from the PG* variables above.
      DATABASE_URL: 'postgres:///vault?sslmode=require',
      KAFKA_BROKERS: 'localhost:9092',
      // No trace collector yet; sampling nothing keeps the exporter quiet.
      TRACE_SAMPLE_RATIO: '0',
      ...dbEnv,
    };
    // A task with its services, each started after the schema migration has run. Permissions
    // belong to a task, so a service that needs access no other service should have gets its own.
    const newTask = (id: string, services: { name: string; env?: Record<string, string>; secrets: Record<string, ecs.Secret> }[]) => {
      const task = new ecs.Ec2TaskDefinition(this, `${id}Task`, { networkMode: ecs.NetworkMode.HOST });
      const migrate = task.addContainer('migrate', {
        image: migrateImage,
        essential: false,
        environment: dbEnv,
        secrets: dbSecrets,
        memoryReservationMiB: 64,
        logging: logging('migrate'),
      });
      for (const s of services) {
        const c = task.addContainer(s.name, {
          image: appImage,
          command: [`/app/${s.name}`],
          environment: { ...common, ...s.env },
          secrets: s.secrets,
          memoryReservationMiB: 64,
          logging: logging(s.name),
        });
        c.addContainerDependencies({ container: migrate, condition: ecs.ContainerDependencyCondition.SUCCESS });
      }
      const svc = new ecs.Ec2Service(this, id, { ...serviceDefaults, taskDefinition: task });
      svc.node.addDependency(host, cluster);
      return { task, service: svc };
    };

    const app = newTask('App', [
      { name: 'gateway', secrets: { JWT_SECRET: appKey('JWT_SECRET') } },
      { name: 'order', secrets: dbSecrets },
      { name: 'payment', secrets: dbSecrets },
      { name: 'pipeline', secrets: { ...dbSecrets, TOKEN_KEY: appKey('TOKEN_KEY') } },
    ]);
    // Only the user service can use the master key and the key table: a bug or breach in the
    // gateway, order or payment service can't decrypt anyone's personal data.
    const user = newTask('User', [
      {
        name: 'user',
        env: { KMS_KEY_ID: masterKey.keyArn, KEYS_TABLE: keysTable.tableName, AWS_REGION: this.region },
        secrets: { ...dbSecrets, BLIND_INDEX_KEY: appKey('BLIND_INDEX_KEY') },
      },
    ]);
    masterKey.grantEncryptDecrypt(user.task.taskRole);
    keysTable.grantReadWriteData(user.task.taskRole);

    // ---------- alarms ----------
    // Each one is a symptom someone would notice, and each has a section in docs/runbook.md saying
    // what it means and what to do. They email whoever subscribed to the vault-alarms topic
    // (AlertsStack), on the way into ALARM and again on recovery.
    const topic = sns.Topic.fromTopicArn(
      this,
      'AlarmTopic',
      `arn:aws:sns:${this.region}:${this.account}:${ALARM_TOPIC_NAME}`,
    );
    const runbook = (anchor: string) => `${RUNBOOK_URL}#${anchor}`;
    const notify = (alarm: cloudwatch.Alarm) => {
      alarm.addAlarmAction(new cw_actions.SnsAction(topic));
      alarm.addOkAction(new cw_actions.SnsAction(topic));
    };

    // A service with no running task. ECS reports LiveTaskCount every minute while a service
    // exists, so no data at all also counts as down. Three minutes, so the gap while a deploy
    // swaps one task for the next doesn't page anyone.
    const services: [string, ecs.Ec2Service][] = [['App', app.service], ['User', user.service], ['Kafka', kafka]];
    for (const [name, svc] of services) {
      notify(
        new cloudwatch.Alarm(this, `${name}Down`, {
          alarmName: `vault-${name.toLowerCase()}-down`,
          alarmDescription: `The ${name} service has had no running task for 3 minutes. ${runbook('service-down')}`,
          metric: new cloudwatch.Metric({
            namespace: 'AWS/ECS',
            metricName: 'LiveTaskCount',
            dimensionsMap: { ClusterName: cluster.clusterName, ServiceName: svc.serviceName },
            statistic: 'Minimum',
            period: Duration.minutes(1),
          }),
          comparisonOperator: cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
          threshold: 1,
          evaluationPeriods: 3,
          treatMissingData: cloudwatch.TreatMissingData.BREACHING,
        }),
      );
    }

    // The services write JSON logs. Metric filters turn the lines worth alerting on into counts.
    const logCount = (id: string, pattern: string) =>
      new logs.MetricFilter(this, `${id}Filter`, {
        logGroup,
        filterPattern: logs.FilterPattern.literal(pattern),
        metricNamespace: 'Vault',
        metricName: id,
        metricValue: '1',
      }).metric({ statistic: 'Sum', period: Duration.minutes(1) });

    // An alarm fires when the count is above threshold, over a window of `minutes`.
    const logAlarms: { id: string; pattern: string; threshold: number; minutes: number; description: string; anchor: string }[] = [
      {
        id: 'GatewayErrors',
        pattern: '{ $.msg = "request failed" }',
        threshold: 10,
        minutes: 5,
        description: 'More than 10 requests failed with a 5xx in 5 minutes.',
        anchor: 'gateway-errors',
      },
      {
        id: 'SlowCheckouts',
        pattern: '{ $.msg = "slow request" && $.route = "/v1/checkout" }',
        threshold: 10,
        minutes: 5,
        description: 'More than 10 checkouts took over 500 ms in 5 minutes.',
        anchor: 'slow-checkouts',
      },
      {
        id: 'OutboxStuck',
        pattern: '{ $.msg = "outbox relay: publish failed, will retry" }',
        threshold: 0,
        minutes: 3,
        description: 'Events have failed to reach Kafka for 3 minutes in a row; they are waiting in the outbox.',
        anchor: 'outbox-stuck',
      },
      {
        id: 'AuditChainBroken',
        pattern: '{ $.msg = "AUDIT CHAIN BROKEN" }',
        threshold: 0,
        minutes: 1,
        description: 'The audit log failed verification: an entry was edited, deleted or reordered.',
        anchor: 'audit-chain-broken',
      },
    ];
    for (const a of logAlarms) {
      const count = logCount(a.id, a.pattern);
      notify(
        new cloudwatch.Alarm(this, a.id, {
          alarmName: `vault-${a.anchor}`,
          alarmDescription: `${a.description} ${runbook(a.anchor)}`,
          // OutboxStuck must hold every minute (Kafka still down); the others sum a window.
          ...(a.id === 'OutboxStuck'
            ? { metric: count, evaluationPeriods: a.minutes, datapointsToAlarm: a.minutes }
            : { metric: count.with({ period: Duration.minutes(a.minutes) }), evaluationPeriods: 1 }),
          comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
          threshold: a.threshold,
          // No matching lines means nothing went wrong.
          treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
        }),
      );
    }

    notify(
      new cloudwatch.Alarm(this, 'DbStorageLow', {
        alarmName: 'vault-db-storage-low',
        alarmDescription: `The database has less than 2 GiB of disk left. ${runbook('db-storage-low')}`,
        metric: db.metricFreeStorageSpace({ statistic: 'Minimum', period: Duration.minutes(5) }),
        comparisonOperator: cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
        threshold: 2 * 1024 ** 3,
        evaluationPeriods: 1,
      }),
    );

    new CfnOutput(this, 'HostGroup', {
      value: host.autoScalingGroupName,
      description: 'Auto Scaling group of the ECS host (scripts/gateway-url.sh finds its public IP)',
    });
    new CfnOutput(this, 'LogGroup', { value: logGroup.logGroupName });
  }
}
