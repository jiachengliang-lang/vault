package user

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"vault/internal/platform"
)

var self = Accessor{Actor: "test", Reason: "unit test"}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	keys, err := NewLocalKeyWrapper(bytes.Repeat([]byte{7}, keySize))
	if err != nil {
		t.Fatal(err)
	}
	return NewStore(platform.TestPool(t), keys, NewBlindIndex([]byte("test-index-key")))
}

func uniqueEmail() string {
	return uuid.NewString()[:8] + "@example.com"
}

func TestProfileRoundTripAndNoPlaintextAtRest(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	p := Profile{UserID: uuid.New(), Email: uniqueEmail(), Address: "2150 Shattuck Ave, Berkeley"}

	if err := s.Upsert(ctx, p, self); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, p.UserID, self)
	if err != nil || got != p {
		t.Fatalf("got %+v, %v; want %+v", got, err, p)
	}

	// What an attacker with a DB dump sees:
	var emailEnc, addressEnc []byte
	if err := s.db.QueryRow(ctx, `SELECT email_enc, address_enc FROM users WHERE user_id = $1`, p.UserID).
		Scan(&emailEnc, &addressEnc); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(emailEnc, []byte(p.Email)) || bytes.Contains(addressEnc, []byte("Shattuck")) {
		t.Fatal("plaintext PII found in the database")
	}
}

// The headline test: after deletion, ciphertext that already escaped into other systems
// (here, the outbox event that was published to Kafka) can no longer be decrypted.
func TestDeleteCryptoShredsEveryCopy(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	p := Profile{UserID: uuid.New(), Email: uniqueEmail(), Address: "somewhere"}
	if err := s.Upsert(ctx, p, self); err != nil {
		t.Fatal(err)
	}

	// Grab the copy that went to Kafka, as a downstream consumer or a backup would hold it.
	var payload []byte
	err := s.db.QueryRow(ctx, `
		SELECT payload FROM outbox
		WHERE topic = $1 AND payload->>'user_id' = $2 AND payload->>'type' = 'user.profile_updated'`,
		TopicUserEvents, p.UserID.String()).Scan(&payload)
	if err != nil {
		t.Fatal(err)
	}
	var ev profileEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(p.Email)) {
		t.Fatal("event published to Kafka contains plaintext email")
	}
	if pt, err := s.decryptWithStoredKey(ctx, p.UserID, "email", ev.EmailEnc); err != nil || string(pt) != p.Email {
		t.Fatalf("before delete, the escaped copy should still decrypt: %q, %v", pt, err)
	}

	if err := s.Delete(ctx, p.UserID, self); err != nil {
		t.Fatal(err)
	}

	if _, err := s.decryptWithStoredKey(ctx, p.UserID, "email", ev.EmailEnc); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete, the escaped copy must be unreadable: got %v", err)
	}
	if _, err := s.Get(ctx, p.UserID, self); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, p.UserID, self); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: got %v, want ErrNotFound", err)
	}
}

func TestEmailIsUniqueAcrossUsers(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	email := uniqueEmail()
	if err := s.Upsert(ctx, Profile{UserID: uuid.New(), Email: email}, self); err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(ctx, Profile{UserID: uuid.New(), Email: email}, self); !errors.Is(err, ErrEmailTaken) {
		t.Fatalf("got %v, want ErrEmailTaken", err)
	}
}

func TestInvalidEmailRejected(t *testing.T) {
	s := newTestStore(t)
	for _, email := range []string{"not-an-email", "Alice <a@b.com>", ""} {
		if err := s.Upsert(context.Background(), Profile{UserID: uuid.New(), Email: email}, self); !errors.Is(err, ErrInvalidEmail) {
			t.Errorf("%q: got %v, want ErrInvalidEmail", email, err)
		}
	}
}

// Every PII operation leaves an audit entry naming the actor and reason.
func TestEveryPIIAccessIsAudited(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id := uuid.New()
	support := Accessor{Actor: "support:agent-7", Reason: "ticket-4821 address change"}

	if err := s.Upsert(ctx, Profile{UserID: id, Email: uniqueEmail()}, self); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, id, support); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, id, self); err != nil {
		t.Fatal(err)
	}

	rows, err := s.db.Query(ctx, `SELECT action, actor, reason FROM audit_log WHERE subject_id = $1 ORDER BY seq`, id)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var action, actor, reason string
		if err := rows.Scan(&action, &actor, &reason); err != nil {
			t.Fatal(err)
		}
		got = append(got, action+"|"+actor+"|"+reason)
	}
	want := []string{
		"WRITE_PII|test|unit test",
		"READ_PII|support:agent-7|ticket-4821 address change",
		"DELETE_USER|test|unit test",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestAuditChainStaysValidUnderConcurrency(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id := uuid.New()
	if err := s.Upsert(ctx, Profile{UserID: id, Email: uniqueEmail()}, self); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Get(ctx, id, self); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	res, err := s.Verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK {
		t.Fatalf("chain broken at seq %d: %s", res.BrokenAt, res.Problem)
	}
}

// Tampering is done inside a transaction that's rolled back, so the real chain is untouched.
func TestAuditTamperingIsDetected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for range 3 {
		if err := s.Upsert(ctx, Profile{UserID: uuid.New(), Email: uniqueEmail()}, self); err != nil {
			t.Fatal(err)
		}
	}

	cases := map[string]string{
		"edit a reason":    `UPDATE audit_log SET reason = 'nothing to see here' WHERE seq = (SELECT max(seq) - 1 FROM audit_log)`,
		"delete an entry":  `DELETE FROM audit_log WHERE seq = (SELECT max(seq) - 1 FROM audit_log)`,
		"change the actor": `UPDATE audit_log SET actor = 'someone-else' WHERE seq = (SELECT max(seq) - 2 FROM audit_log)`,
	}
	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			tx, err := s.db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, tamper); err != nil {
				t.Fatal(err)
			}
			res, err := VerifyAudit(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}
			if res.OK {
				t.Fatal("tampering went undetected")
			}
			t.Logf("detected at seq %d: %s", res.BrokenAt, res.Problem)
		})
	}
}
