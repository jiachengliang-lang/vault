import * as fs from 'node:fs';
import * as path from 'node:path';
import * as cdk from 'aws-cdk-lib/core';
import { Template } from 'aws-cdk-lib/assertions';
import { VaultStack } from '../lib/vault-stack';

const template = Template.fromStack(
  new VaultStack(new cdk.App(), 'Test', {
    env: { account: '111111111111', region: 'us-east-2' },
    allowedCidrs: ['203.0.113.7/32'],
    appSecretName: 'vault/app',
  }),
);
const alarms = Object.values(template.findResources('AWS::CloudWatch::Alarm'));
const repo = path.join(__dirname, '..', '..');

test('stays within the 10 alarms CloudWatch gives for free', () => {
  expect(alarms.length).toBeGreaterThan(0);
  expect(alarms.length).toBeLessThanOrEqual(10);
});

test('every alarm emails on the way into ALARM', () => {
  for (const a of alarms) {
    expect(JSON.stringify(a.Properties.AlarmActions)).toContain(':vault-alarms');
  }
});

// Recovery emails come from AlertsStack's rule instead (see alerts.test.ts), which skips an
// alarm's first move to OK after a fresh deploy.
test('no OK actions on the alarms themselves', () => {
  for (const a of alarms) {
    expect(a.Properties.OKActions ?? []).toEqual([]);
  }
});

test('every alarm links to a runbook section that exists', () => {
  const runbook = fs.readFileSync(path.join(repo, 'docs', 'runbook.md'), 'utf8');
  // GitHub's anchor for "## Service down" is #service-down.
  const anchors = new Set(
    [...runbook.matchAll(/^## (.+)$/gm)].map((m) => m[1].toLowerCase().replace(/[^a-z0-9 -]/g, '').replace(/ /g, '-')),
  );
  for (const a of alarms) {
    const [, anchor] = a.Properties.AlarmDescription.match(/runbook\.md#([a-z0-9-]+)/) ?? [];
    expect(anchors).toContain(anchor);
  }
});

// A log-based alarm matches on the exact message text. If the Go code's message changes, the
// alarm would silently never fire again, so check each message still appears in the source.
test('every log message an alarm matches on is still logged by the services', () => {
  const source = ['internal', 'cmd']
    .flatMap((dir) => fs.readdirSync(path.join(repo, dir), { recursive: true, encoding: 'utf8' }).map((f) => path.join(dir, f)))
    .filter((f) => f.endsWith('.go') && !f.endsWith('_test.go'))
    .map((f) => fs.readFileSync(path.join(repo, f), 'utf8'))
    .join('\n');
  const filters = Object.values(template.findResources('AWS::Logs::MetricFilter'));
  expect(filters.length).toBeGreaterThan(0);
  for (const f of filters) {
    const [, msg] = f.Properties.FilterPattern.match(/\$\.msg = "([^"]+)"/) ?? [];
    expect(source).toContain(`"${msg}"`);
  }
});

// ECS reports nothing (not 0) for a service with no tasks; without FILL the alarm took 11 minutes.
test('service-down alarms treat a missing task count as 0', () => {
  const down = alarms.filter((a) => /-down$/.test(a.Properties.AlarmName));
  expect(down).toHaveLength(3);
  for (const a of down) {
    expect(a.Properties.Metrics.map((m: { Expression?: string }) => m.Expression)).toContain('FILL(tasks, 0)');
  }
});
