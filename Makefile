.PHONY: up down logs psql topics build run test e2e lint load bench chaos

up:        ## start infra (postgres, redpanda, jaeger, prometheus, grafana)
	docker compose up -d

down:      ## stop infra and wipe volumes
	docker compose down -v

logs:
	docker compose logs -f

psql:
	docker compose exec postgres psql -U vault -d vault

topics:    ## create Kafka topics (partitioned by user_id key)
	docker compose exec redpanda rpk topic create order.events payment.events order.events.dlq user.events -p 6 || true # already exists is fine

build:
	go build -o bin/ ./cmd/...

run: build ## start all services locally (Ctrl-C stops all)
	trap 'kill 0' INT TERM; bin/order & bin/payment & bin/user & bin/gateway & bin/pipeline & wait

test:
	go test ./... -race -count=1

e2e:       ## end-to-end smoke test against running services (needs `make run`)
	./scripts/e2e.sh

lint:
	go vet ./...

load: build ## k6 load test against the gateway (run `make run` in another terminal first)
	bin/tokengen -n 500 > loadtest/tokens.json
	k6 run loadtest/checkout.js

bench: build ## latency at increasing fixed rates (run `make run` in another terminal first)
	bin/tokengen -n 500 > loadtest/tokens.json
	loadtest/bench.sh 100 200 400 600 800

chaos: build ## break Postgres, Kafka and payment under load, then check consistency (needs `make run`)
	./scripts/chaos.sh
