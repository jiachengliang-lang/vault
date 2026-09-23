// tokengen prints a dev JWT for calling the gateway:
//
//	go run ./cmd/tokengen                 # random user
//	go run ./cmd/tokengen -user <uuid>    # specific user
//	go run ./cmd/tokengen -n 500          # JSON array of 500 tokens for distinct users (load tests)
//	go run ./cmd/tokengen -role support   # support-staff token
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	"vault/internal/gateway"
	"vault/internal/platform"
)

func main() {
	user := flag.String("user", "", "user UUID (random if empty)")
	ttl := flag.Duration("ttl", 24*time.Hour, "token lifetime")
	n := flag.Int("n", 0, "print a JSON array of n tokens for n random users")
	role := flag.String("role", "", `staff role, e.g. "support"`)
	flag.Parse()
	secret := []byte(platform.Env("JWT_SECRET", "dev-secret-change-me"))

	if *n > 0 {
		tokens := make([]string, *n)
		for i := range tokens {
			var err error
			if tokens[i], err = gateway.NewToken(secret, uuid.New(), *ttl); err != nil {
				log.Fatal(err)
			}
		}
		json.NewEncoder(os.Stdout).Encode(tokens)
		return
	}

	id := uuid.New()
	if *user != "" {
		var err error
		if id, err = uuid.Parse(*user); err != nil {
			log.Fatalf("-user: %v", err)
		}
	}
	token, err := gateway.NewTokenWithRole(secret, id, *role, *ttl)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(token)
}
