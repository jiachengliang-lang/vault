package user

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"vault/internal/platform"
)

// testDynamoKeyStore points at DynamoDB Local from `make up`, creating the table on first use.
func testDynamoKeyStore(t *testing.T) *DynamoKeyStore {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-2"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(platform.Env("DYNAMODB_ENDPOINT", "http://localhost:8000"))
	})
	const table = "vault_user_keys_test"
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(table),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("user_id"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("user_id"), KeyType: types.KeyTypeHash}},
	})
	var exists *types.ResourceInUseException
	if err != nil && !errors.As(err, &exists) {
		platform.SkipUnlessRequired(t, "dynamodb local unavailable, run `make up` first: %v", err)
	}
	return NewDynamoKeyStore(client, table)
}

// Both stores must behave the same, since the user service runs on either.
func TestKeyStores(t *testing.T) {
	stores := map[string]func(*testing.T) KeyStore{
		"postgres": func(t *testing.T) KeyStore { return NewPostgresKeyStore(platform.TestPool(t)) },
		"dynamodb": func(t *testing.T) KeyStore { return testDynamoKeyStore(t) },
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			ks := open(t)
			ctx := context.Background()
			id := uuid.New()

			if _, err := ks.Get(ctx, id); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get before create: got %v, want ErrNotFound", err)
			}
			got, err := ks.CreateIfAbsent(ctx, id, []byte("first"))
			if err != nil || string(got) != "first" {
				t.Fatalf("create: got %q, %v", got, err)
			}
			// A later create must not replace the key, or data already encrypted under it is lost.
			got, err = ks.CreateIfAbsent(ctx, id, []byte("second"))
			if err != nil || string(got) != "first" {
				t.Fatalf("second create: got %q, %v; want the original key", got, err)
			}
			if had, err := ks.Delete(ctx, id); err != nil || !had {
				t.Fatalf("delete: got %v, %v", had, err)
			}
			if had, err := ks.Delete(ctx, id); err != nil || had {
				t.Fatalf("second delete: got %v, %v; want false", had, err)
			}
			if _, err := ks.Get(ctx, id); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
			}
		})

		t.Run(name+"/concurrent first writes agree", func(t *testing.T) {
			ks := open(t)
			id := uuid.New()
			results := make([][]byte, 10)
			var wg sync.WaitGroup
			for i := range results {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, err := ks.CreateIfAbsent(context.Background(), id, []byte{byte(i)})
					if err != nil {
						t.Error(err)
					}
					results[i] = got
				}()
			}
			wg.Wait()
			for _, r := range results[1:] {
				if !bytes.Equal(r, results[0]) {
					t.Fatalf("writers got different keys: %v", results)
				}
			}
		})
	}
}

// With keys in DynamoDB, the database holds no keys at all, so a backup of it can't bring a
// deleted user's data back. Shredding still works end to end.
func TestDeleteCryptoShredsWithKeysOutsideTheDatabase(t *testing.T) {
	keys, err := NewLocalKeyWrapper(bytes.Repeat([]byte{7}, keySize))
	if err != nil {
		t.Fatal(err)
	}
	pool := platform.TestPool(t)
	s := NewStore(pool, keys, testDynamoKeyStore(t), NewBlindIndex([]byte("test-index-key")))
	ctx := context.Background()
	p := Profile{UserID: uuid.New(), Email: uniqueEmail(), Address: "somewhere"}

	if err := s.Upsert(ctx, p, self); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, p.UserID, self); err != nil || got != p {
		t.Fatalf("got %+v, %v; want %+v", got, err, p)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_keys WHERE user_id = $1`, p.UserID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("key was written to Postgres, where database backups would keep it")
	}
	var emailEnc []byte
	if err := pool.QueryRow(ctx, `SELECT email_enc FROM users WHERE user_id = $1`, p.UserID).Scan(&emailEnc); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(ctx, p.UserID, self); err != nil {
		t.Fatal(err)
	}
	if _, err := s.decryptWithStoredKey(ctx, p.UserID, "email", emailEnc); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete, a saved copy of the ciphertext must be unreadable: got %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT 1 FROM users WHERE user_id = $1`, p.UserID).Scan(&n); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("user row should be gone: %v", err)
	}
}

// If the key is destroyed but the rest of Delete fails (say the database is down), the user is
// already unreadable, and a retry finishes the job instead of reporting "not found".
func TestDeleteRetryAfterPartialFailure(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	p := Profile{UserID: uuid.New(), Email: uniqueEmail()}
	if err := s.Upsert(ctx, p, self); err != nil {
		t.Fatal(err)
	}
	// Simulate the first attempt dying right after the key was destroyed.
	if _, err := s.keyStore.Delete(ctx, p.UserID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, p.UserID, self); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get with the key gone: got %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, p.UserID, self); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if err := s.Delete(ctx, p.UserID, self); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete after cleanup: got %v, want ErrNotFound", err)
	}
}
