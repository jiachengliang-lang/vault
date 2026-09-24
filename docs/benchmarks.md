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
