# Runbook

What to do when a Vault alarm emails you. Each alarm's description links to its section here. Commands assume the
AWS CLI is signed in to the account, from the repo root.

Alarms email on the way into ALARM, and a `RECOVERED` email follows when one goes from ALARM back to OK. (Recovery
emails come from an EventBridge rule in the `VaultAlerts` stack rather than the alarms' own OK actions, which would
also fire as each new alarm gets its first data after a fresh deploy.)

## Any alarm: first look

```bash
LG=$(aws cloudformation describe-stacks --stack-name Vault --query "Stacks[0].Outputs[?OutputKey=='LogGroup'].OutputValue" --output text)
# Errors and warnings from every service in the last 15 minutes, newest last
aws logs tail "$LG" --since 15m --filter-pattern '{ $.level = "ERROR" || $.level = "WARN" }'
# What's running
CLUSTER=$(aws ecs list-clusters --query 'clusterArns[0]' --output text)
aws ecs describe-services --cluster "$CLUSTER" --services $(aws ecs list-services --cluster "$CLUSTER" --query serviceArns --output text) \
  --query 'services[].[serviceName,runningCount,desiredCount,events[0].message]' --output table
```

Was there a deploy just before it started? Check the deploy workflow's runs on GitHub. If the smoke test passed but
things broke anyway, run the deploy workflow on the `deployed` tag's commit to go back, then investigate.

## Service down

**Means:** one of the ECS services (App, User or Kafka) has had no running task for 3 minutes. Expect the email
about 5 minutes in: ECS's task counts reach CloudWatch a couple of minutes late. App down means the
API is down. User down means checkout still works but profiles don't. Kafka down means checkout still works and
events wait in the outbox ([Outbox stuck](#outbox-stuck) will follow).

**Check:**
- The service's latest events (`describe-services` above). Look for tasks that start and stop in a loop.
- Why the last task stopped:
  ```bash
  aws ecs describe-tasks --cluster "$CLUSTER" --tasks $(aws ecs list-tasks --cluster "$CLUSTER" --desired-status STOPPED --query 'taskArns[0]' --output text) \
    --query 'tasks[0].[stoppedReason,containers[].[name,exitCode,reason]]'
  ```
- That service's logs just before it stopped: `aws logs tail "$LG" --since 30m --log-stream-name-prefix <container>`.
  A `startup failed` line names the dependency it couldn't reach (database, Kafka, KMS).
- No tasks at all and no events: the host may be gone or disconnected. Check the Auto Scaling group has one healthy
  instance (`make aws-url` fails if there's none) and the container instance shows `agentConnected: true`.

**Fix:** a bad deploy goes back to the last good commit (above). A crash loop on a dependency gets fixed at the
dependency. A replaced host starts its tasks again by itself within a few minutes.

## Gateway errors

**Means:** more than 10 API requests returned a 5xx in 5 minutes. Clients see failures; checkout clients are told to
retry with the same Idempotency-Key, which is safe.

**Check:**
```bash
aws logs tail "$LG" --since 15m --filter-pattern '{ $.msg = "request failed" || $.msg = "upstream call failed" }'
```
`route` shows which endpoint, `upstream call failed` shows which downstream call and why: `circuit_open` means the
gateway is failing fast because that service kept failing, `timeout` means it's slow or unreachable.

**Fix:** follow it to the failing service and treat that as [Service down](#service-down) or a database problem.
Errors that started with a deploy: roll back.

## Slow checkouts

**Means:** more than 10 checkouts took over 500 ms in 5 minutes.

**Check:**
```bash
aws logs tail "$LG" --since 15m --filter-pattern '{ $.msg = "slow request" }'
aws cloudwatch get-metric-statistics --namespace AWS/RDS --metric-name CPUUtilization --dimensions Name=DBInstanceIdentifier,Value=<db id> \
  --start-time $(date -u -v-1H +%FT%TZ) --end-time $(date -u +%FT%TZ) --period 300 --statistics Maximum
```
Locally the bottleneck was Postgres commits (see [benchmarks](benchmarks.md)). On the t4g.micro database, also
check CPU credits (`CPUCreditBalance` near 0 means it's being throttled). Slow payment provider calls show up as
`charge failed` lines from the payment service.

**Fix:** if it's load, it passes when the load does; the rate limiter caps each user. If the database is out of
CPU credits, it recovers as credits build back up, or move to a larger instance class.

## Outbox stuck

**Means:** the order service's outbox relay has failed to publish to Kafka every minute for 3 minutes. Checkout
still works: events wait in the `outbox` table and go out once Kafka is back, in order. Analytics falls behind, and
the pipeline logs a `fetch error` every few seconds while it can't reach Kafka.

Each publish attempt gives up after 10 seconds and logs a warning, which is what this alarm counts. (Before that
limit, the relay waited on Kafka forever and logged nothing, so an outage on AWS went unalerted.) It only fires
while there are events waiting: with no checkouts, there's nothing to fail, and only
[Service down](#service-down) for Kafka tells you.

**Check:** is Kafka up ([Service down](#service-down) for the Kafka service)? The relay's warnings say why
publishing failed:
```bash
aws logs tail "$LG" --since 15m --filter-pattern '{ $.msg = "outbox relay: publish failed, will retry" }'
```
`UNKNOWN_TOPIC_OR_PARTITION` means a topic is missing: the Kafka task's `topics` container creates them at start
and its log says what happened. Kafka's data is in a Docker volume on the host, so it survives the task
restarting. A replaced host starts Kafka empty: a single `UNKNOWN_TOPIC_ID` warning right after is producers
noticing the recreated topics, and the retry succeeds.

**Fix:** bring Kafka back. Nothing to replay by hand; the relay catches up by itself. Watch the warnings stop.

## Audit chain broken

**Means:** the user service's minute-by-minute check of the audit log found an entry that was edited, deleted or
reordered. Nothing in the code changes past entries, so treat it as someone with database access tampering with the
record of who read personal data.

**Check:** the log line has the sequence number and what's wrong:
```bash
aws logs tail "$LG" --since 1h --filter-pattern '{ $.msg = "AUDIT CHAIN BROKEN" }'
```

**Fix:** don't "repair" the chain: that destroys the evidence. Snapshot the database, find who had write access
(RDS master credentials in Secrets Manager, and who read them, in CloudTrail), and rotate the credentials.

## Db storage low

**Means:** under 2 GiB of the database's 20 GiB disk is left. When it runs out, every write fails, including
checkout.

**Check:** which tables are big. Usually `analytics_events` (it only grows) or `outbox` (events not reaching
Kafka, see [Outbox stuck](#outbox-stuck)).

**Fix:** if the outbox is backed up, fix Kafka and it drains. Otherwise raise `allocatedStorage` in
`infra/lib/vault-stack.ts` and deploy; RDS grows the disk without downtime.
