// User service: stores encrypted PII, crypto-shreds on delete, and keeps the hash-chained audit log.
// Kitex RPC on RPC_ADDR; health checks and GET /audit/verify on the internal ADMIN_ADDR.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"github.com/twmb/franz-go/pkg/kgo"

	"vault/internal/outbox"
	"vault/internal/platform"
	"vault/internal/user"
	"vault/kitex_gen/user/userservice"
)

func main() {
	log := platform.NewLogger("user")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// MASTER_KEY: base64 of 32 random bytes (openssl rand -base64 32). In production this is a KMS key.
	masterKey := sha256.Sum256([]byte("dev-master-key-change-me"))
	key := masterKey[:]
	if v, ok := os.LookupEnv("MASTER_KEY"); ok {
		var err error
		if key, err = base64.StdEncoding.DecodeString(v); err != nil {
			log.Error("MASTER_KEY must be base64", "err", err)
			os.Exit(1)
		}
	} else {
		log.Warn("MASTER_KEY not set, using the dev default")
	}
	keys, err := user.NewLocalKeyWrapper(key)
	if err != nil {
		log.Error("master key", "err", err)
		os.Exit(1)
	}
	index := user.NewBlindIndex([]byte(platform.Env("BLIND_INDEX_KEY", "dev-blind-index-key-change-me")))

	pool, err := platform.NewPool(ctx, platform.Env("DATABASE_URL", platform.DefaultDatabaseURL))
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	store := user.NewStore(pool, keys, index)

	admin := platform.StartAdmin(platform.Env("ADMIN_ADDR", ":8083"), pool.Ping, platform.Route{
		Pattern: "GET /audit/verify",
		Handler: func(w http.ResponseWriter, r *http.Request) {
			res, err := store.Verify(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if !res.OK {
				w.WriteHeader(http.StatusConflict)
			}
			json.NewEncoder(w).Encode(map[string]any{
				"ok": res.OK, "entries": res.Entries, "broken_at_seq": res.BrokenAt,
				"problem": res.Problem, "head_hash": hex.EncodeToString(res.HeadHash),
			})
		},
	})
	defer admin.Shutdown(context.Background())

	// Each service with an outbox runs a relay; the shared advisory lock keeps one active at a time.
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(platform.Env("KAFKA_BROKERS", platform.DefaultKafkaBrokers), ",")...))
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer producer.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		outbox.NewRelay(pool, producer).Run(ctx)
	}()

	addr, err := net.ResolveTCPAddr("tcp", platform.Env("RPC_ADDR", ":8883"))
	if err != nil {
		log.Error("bad RPC_ADDR", "err", err)
		os.Exit(1)
	}
	svr := userservice.NewServer(
		user.NewHandler(store),
		server.WithServiceAddr(addr),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: "user"}),
	)
	log.Info("user service starting", "rpc", addr.String())
	if err := svr.Run(); err != nil {
		log.Error("server stopped", "err", err)
	}
	cancel()
	wg.Wait()
}
