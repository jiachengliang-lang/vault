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

## Deploying from GitHub

`.github/workflows/deploy.yml` runs after CI passes on `main`. If Vault is up, it deploys the new version,
runs `scripts/smoke.sh` against it through the public API, and on success moves the `deployed` tag to that commit.
If the smoke test fails, it deploys the `deployed` commit again. A deploy that fails outright doesn't need that:
CloudFormation rolls the stack back, and the ECS circuit breaker rolls back containers that keep crashing.

Vault is usually down to save money, so a push to `main` only deploys when it's already up. **Run workflow** on
the deploy workflow brings it up or tears it down.

GitHub signs in as the `vault-github-deploy` IAM user from the `VaultCi` stack, with an access key kept in GitHub's
encrypted secrets. (Keyless sign-in through GitHub's OIDC tokens would be better, but this account's organization
doesn't allow creating OIDC providers.) Set up once, from the repo root:

```bash
(cd infra && npx cdk deploy VaultCi)
scripts/github-deploy-key.sh       # creates the key and stores it in GitHub; run again to rotate
gh variable set ADMIN_CIDR --body "$(curl -s https://checkip.amazonaws.com)/32"   # optional: your IP
```

`npm test` checks the choices that keep it cheap and closed: no NAT gateway or load balancer, a small private
encrypted database, and only the gateway port open.
