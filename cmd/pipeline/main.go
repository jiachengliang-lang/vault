// Pipeline: consumes order events, de-identifies them, and writes them to analytics_events.
// Poison messages go to the dead-letter topic.
package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"

	"vault/internal/order"
	"vault/internal/pipeline"
	"vault/internal/platform"
)

const (
	consumerGroup = "analytics-pipeline"
	dlqTopic      = order.TopicOrderEvents + ".dlq"
)

func main() {
	log := platform.NewLogger("pipeline")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := platform.NewPool(ctx, platform.Env("DATABASE_URL", platform.DefaultDatabaseURL))
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(platform.Env("KAFKA_BROKERS", platform.DefaultKafkaBrokers), ",")...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(order.TopicOrderEvents),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // a brand-new group starts from the beginning
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	admin := platform.StartAdmin(platform.Env("ADMIN_ADDR", ":8084"), func(ctx context.Context) error {
		return errors.Join(pool.Ping(ctx), cl.Ping(ctx))
	})
	defer admin.Shutdown(context.Background())

	key := platform.Env("TOKEN_KEY", "dev-token-key-change-me")
	if _, ok := os.LookupEnv("TOKEN_KEY"); !ok {
		log.Warn("TOKEN_KEY not set, using the dev default")
	}
	p := pipeline.NewProcessor(
		pipeline.NewTokenizer([]byte(key)),
		pipeline.NewPostgresSink(pool),
		pipeline.NewKafkaDLQ(cl, dlqTopic),
	)
	log.Info("pipeline starting", "group", consumerGroup, "topic", order.TopicOrderEvents)
	pipeline.Consume(ctx, cl, p)
	log.Info("pipeline stopped")
}
