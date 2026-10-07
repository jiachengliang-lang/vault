# Vault on AWS

The AWS setup for Vault, written with the AWS CDK in TypeScript. `lib/vault-stack.ts` is the whole thing.

It's sized to cost almost nothing: one small ARM EC2 host runs every container (the five services and Redpanda)
through ECS, next to a single-AZ Postgres on RDS. There's no NAT gateway or load balancer. Only the gateway's port
is open, and only to the IP that deployed it.

```bash
make aws-secrets   # once: create the app keys in Secrets Manager
make aws-up        # deploy (about 15 minutes, mostly the database)
make aws-url       # print the gateway URL
make aws-down      # delete everything except the keys
```

The first deploy to an account also needs `npx cdk bootstrap` (from this folder).

`npm test` checks the choices that keep it cheap and closed: no NAT gateway or load balancer, a small private
encrypted database, and only the gateway port open.
