import * as cdk from 'aws-cdk-lib/core';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { VaultStack } from '../lib/vault-stack';

// These pin down the choices that keep the stack free and closed to the internet,
// so a later change can't quietly undo them.
const template = Template.fromStack(
  new VaultStack(new cdk.App(), 'Test', {
    env: { account: '111111111111', region: 'us-east-2' },
    allowedCidrs: ['203.0.113.7/32'],
    appSecretName: 'vault/app',
  }),
);

test('no NAT gateway or load balancer', () => {
  template.resourceCountIs('AWS::EC2::NatGateway', 0);
  template.resourceCountIs('AWS::ElasticLoadBalancingV2::LoadBalancer', 0);
});

test('database is small, single-AZ, encrypted and not public', () => {
  template.hasResourceProperties('AWS::RDS::DBInstance', {
    DBInstanceClass: 'db.t4g.micro',
    MultiAZ: false,
    PubliclyAccessible: false,
    StorageEncrypted: true,
  });
});

test('only the gateway port is open, and only to allowedCidrs', () => {
  const ingress = Object.values(template.findResources('AWS::EC2::SecurityGroup'))
    .flatMap((sg) => sg.Properties.SecurityGroupIngress ?? []);
  expect(ingress).toEqual([
    expect.objectContaining({ CidrIp: '203.0.113.7/32', FromPort: 8080, ToPort: 8080 }),
  ]);
});

test('one free-tier host with IMDSv2', () => {
  template.hasResourceProperties('AWS::EC2::LaunchTemplate', {
    LaunchTemplateData: Match.objectLike({
      InstanceType: 't4g.small',
      MetadataOptions: Match.objectLike({ HttpTokens: 'required' }),
    }),
  });
  template.hasResourceProperties('AWS::AutoScaling::AutoScalingGroup', { MinSize: '1', MaxSize: '1' });
});

test('services wait for the host instead of failing placement', () => {
  const services = template.findResources('AWS::ECS::Service');
  const groupId = Object.keys(template.findResources('AWS::AutoScaling::AutoScalingGroup'))[0];
  const associationId = Object.keys(template.findResources('AWS::ECS::ClusterCapacityProviderAssociations'))[0];
  for (const svc of Object.values(services)) {
    expect(svc.Properties.CapacityProviderStrategy).toHaveLength(1);
    expect(svc.Properties.LaunchType).toBeUndefined();
    expect(svc.DependsOn).toContain(groupId);
    // ...and is deleted before the cluster's capacity provider link, so one `cdk destroy` is enough.
    expect(svc.DependsOn).toContain(associationId);
  }
});

test('the host keeps its internet route until it is deleted', () => {
  const [group] = Object.values(template.findResources('AWS::AutoScaling::AutoScalingGroup'));
  const routes = Object.entries(template.findResources('AWS::EC2::Route'))
    .filter(([, r]) => r.Properties.GatewayId)
    .map(([id]) => id);
  expect(routes.length).toBeGreaterThan(0);
  for (const id of routes) expect(group.DependsOn).toContain(id);
});

test('only the user service can use the master key and the key table', () => {
  const tasks = template.findResources('AWS::ECS::TaskDefinition');
  const roleOf = (container: string) => {
    const [task] = Object.values(tasks).filter((t) =>
      t.Properties.ContainerDefinitions.some((c: { Name: string }) => c.Name === container),
    );
    return task.Properties.TaskRoleArn['Fn::GetAtt'][0];
  };
  const policies = Object.values(template.findResources('AWS::IAM::Policy'));
  const actionsFor = (role: string) =>
    policies
      .filter((p) => p.Properties.Roles.some((r: { Ref: string }) => r.Ref === role))
      .flatMap((p) => p.Properties.PolicyDocument.Statement.flatMap((st: { Action: string | string[] }) => st.Action));

  expect(actionsFor(roleOf('user'))).toEqual(expect.arrayContaining(['kms:Decrypt', 'kms:Encrypt', 'dynamodb:PutItem']));
  for (const other of ['gateway', 'order', 'payment', 'pipeline']) {
    expect(roleOf(other)).not.toEqual(roleOf('user'));
    expect(actionsFor(roleOf(other)).join(' ')).not.toMatch(/kms:|dynamodb:/);
  }
});

test('user service uses KMS, not a master key from Secrets Manager', () => {
  const containers = Object.values(template.findResources('AWS::ECS::TaskDefinition')).flatMap(
    (t) => t.Properties.ContainerDefinitions,
  );
  const user = containers.find((c: { Name: string }) => c.Name === 'user');
  expect(user.Environment.map((e: { Name: string }) => e.Name)).toEqual(expect.arrayContaining(['KMS_KEY_ID', 'KEYS_TABLE']));
  for (const c of containers) {
    expect((c.Secrets ?? []).map((e: { Name: string }) => e.Name)).not.toContain('MASTER_KEY');
  }
});

test('key table has no point-in-time recovery, so deleted keys stay deleted', () => {
  template.hasResourceProperties('AWS::DynamoDB::GlobalTable', {
    Replicas: [Match.objectLike({ PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: false } })],
  });
});

test("Kafka's data survives the task restarting", () => {
  const [kafkaTask] = Object.values(template.findResources('AWS::ECS::TaskDefinition')).filter((t) =>
    t.Properties.ContainerDefinitions.some((c: { Name: string }) => c.Name === 'redpanda'),
  );
  expect(kafkaTask.Properties.Volumes).toEqual([
    expect.objectContaining({ DockerVolumeConfiguration: expect.objectContaining({ Scope: 'shared', Autoprovision: true }) }),
  ]);
  const redpanda = kafkaTask.Properties.ContainerDefinitions.find((c: { Name: string }) => c.Name === 'redpanda');
  expect(redpanda.MountPoints).toEqual([expect.objectContaining({ ContainerPath: '/var/lib/redpanda/data' })]);
});

// pgx's default pool is max(4, cores), so 4 on the 2-vCPU host; that capped checkout throughput.
test('every service sets its own Postgres pool size', () => {
  const containers = Object.values(template.findResources('AWS::ECS::TaskDefinition')).flatMap(
    (t) => t.Properties.ContainerDefinitions,
  );
  const urls = containers.flatMap((c: { Environment?: { Name: string; Value: string }[] }) =>
    (c.Environment ?? []).filter((e) => e.Name === 'DATABASE_URL').map((e) => e.Value),
  );
  expect(urls.length).toBeGreaterThan(0);
  for (const u of urls) expect(u).toMatch(/pool_max_conns=\d+/);
});
