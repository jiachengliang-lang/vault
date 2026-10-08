# Benchmarks

## Setup

Everything runs on one laptop (Apple M5, 10 cores, 16 GB): the five services, Postgres, Kafka, Jaeger,
Prometheus, Grafana and the load generator. The absolute numbers only mean something on this machine, so the
useful part is the before and after comparison.

`loadtest/checkout.js` sends checkouts at a fixed rate. I avoided the usual "N users, each waits for a response
before sending the next" setup, because that sends less traffic exactly when the server slows down and makes
latency look better than it is. Traces were sampled at 10% for these runs.

`loadtest/bench.sh` runs the test at several rates and also records, per checkout, how many Postgres commits and
how much write-ahead log the whole system produced, plus how far behind analytics was.

## Finding the bottleneck

Checkout latency climbed past 200 ms at around 1,000 requests a second. The Go services weren't busy: the order
service used under one CPU core, almost all of it waiting on the network. Sampling what Postgres was doing showed
it spent about 90% of its time waiting to flush the write-ahead log, which every commit has to do. So the limit
was commits per second.

A third of all commits came from the analytics pipeline, which saved each event in its own transaction. That was
also why analytics fell minutes behind under load.

## Changes

1. The pipeline now saves each batch from Kafka (up to 500 events) in a single insert and commit.
2. The outbox relay deletes rows after publishing them, instead of updating each one and cleaning up later.

## Results

Median of 3 runs per version, alternating old and new, 30 seconds per rate.

| Requests/s | Version | p99 latency | Errors | Commits per checkout | Analytics delay (p99) |
|---|---|---|---|---|---|
| 800 | before | 929 ms | 0% | 4.78 | 29.7 s |
| 800 | after | 613 ms | 0% | 3.97 | 0.25 s |
| 1,000 | before | 1,237 ms | 0% | 4.58 | 288 s |
| 1,000 | after | 767 ms | 0% | 3.95 | 0.25 s |
| 1,200 | before | 3,967 ms | 10.4% | 4.47 | 296 s |
| 1,200 | after | 5,691 ms | 6.6% | 3.88 | 0.37 s |

Commits per checkout dropped about 17% and analytics delay dropped from minutes to well under a second, and both
held in every single run. The latency medians improved at 800 and 1,000 requests a second, but identical runs
varied by up to 6x on this laptop and both versions were overloaded at 1,200, so I don't treat the latency
numbers as a real result.

## Reproducing

```bash
make up && make topics
make run          # in one terminal
make bench        # in another
```

## On AWS

The same checkout test against the AWS deployment: one t4g.small host (2 vCPUs, 2 GiB) running all the services
and Redpanda, and a db.t4g.micro Postgres. The load comes from a GitHub runner (`.github/workflows/loadtest.yml`),
60 seconds per rate. Its network round trip to the gateway, measured on its own, was 17–19 ms, which is included in
every latency below. CPU figures are the peak 1-minute average from CloudWatch. Neither machine ran low on CPU
credits, so these aren't burst numbers.

### A pool sized by the CPU count

| Requests/s | Achieved | p50 | p95 | p99 | Errors | Host CPU | Database CPU |
|---|---|---|---|---|---|---|---|
| 50 | 50 | 56 ms | 67 ms | 121 ms | 0% | 8% | 12% |
| 100 | 100 | 56 ms | 62 ms | 151 ms | 0% | 15% | 15% |
| 200 | 200 | 57 ms | 198 ms | 359 ms | 0% | 22% | 24% |
| 400 | 366 | 2,018 ms | 3,955 ms | 4,847 ms | 48.5% | 25% | 35% |

At 400 a second, neither machine was busy and the database never had more than 11 connections open. The gateway's
logs showed calls to the order service hitting the 2-second RPC timeout, then its circuit breaker opening; the
median of 2,018 ms is that timeout. pgx sizes its connection pool from the number of CPUs, with a minimum of 4. That's
10 connections on the laptop, but 4 on the 2-vCPU host, so order requests were queueing for a connection. The services
now ask for 16 each (`pool_max_conns` in `DATABASE_URL`), 64 in all, under the database's limit of 79.

### After

| Requests/s | Achieved | p50 | p95 | p99 | Errors | Host CPU | Database CPU |
|---|---|---|---|---|---|---|---|
| 200 | 200 | 59 ms | 70 ms | 117 ms | 0% | 27% | 23% |
| 400 | 400 | 61 ms | 83 ms | 166 ms | 0% | 33% | 47% |
| 600 | 586 | 65 ms | 795 ms | 1,942 ms | 0% | 64% | 53% |
| 800 | 496 | 1,345 ms | 3,379 ms | 4,060 ms | 24% | 45% | 60% |

400 a second now holds with a p99 of 166 ms. Past about 600 the order service's database calls time out again, this
time with 35 connections in use and both machines much busier. Of the median 60 ms, about 18 ms is the network and
20 ms is the fake payment provider's built-in delay.
