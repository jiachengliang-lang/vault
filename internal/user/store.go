package user

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"vault/internal/outbox"
)

const TopicUserEvents = "user.events"

var (
	ErrNotFound     = errors.New("user not found")
	ErrEmailTaken   = errors.New("email already belongs to another account")
	ErrInvalidEmail = errors.New("invalid email address")
)

type Profile struct {
	UserID  uuid.UUID
	Email   string
	Address string
}

// Accessor is who is touching PII and why. Both go into the audit log.
type Accessor struct {
	Actor  string
	Reason string
}

// profileEvent is published when a profile changes. The PII inside stays encrypted, so Kafka,
// its retention, and every downstream copy only ever hold ciphertext tied to the user's DEK.
type profileEvent struct {
	EventID    uuid.UUID `json:"event_id"`
	Type       string    `json:"type"` // user.profile_updated | user.deleted
	UserID     uuid.UUID `json:"user_id"`
	EmailEnc   []byte    `json:"email_enc,omitempty"` // base64 in JSON
	AddressEnc []byte    `json:"address_enc,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

type Store struct {
	db       *pgxpool.Pool
	keys     KeyWrapper
	keyStore KeyStore
	index    BlindIndex
}

func NewStore(db *pgxpool.Pool, keys KeyWrapper, keyStore KeyStore, index BlindIndex) *Store {
	return &Store{db: db, keys: keys, keyStore: keyStore, index: index}
}

// Upsert creates or replaces a user's profile. The PII is encrypted before it leaves this process;
// the database, its backups, and the Kafka event only ever see ciphertext.
func (s *Store) Upsert(ctx context.Context, p Profile, who Accessor) error {
	addr, err := mail.ParseAddress(p.Email)
	if err != nil || addr.Address != p.Email {
		return ErrInvalidEmail
	}

	// The key is created before the transaction, since it lives in another store. If the
	// transaction then fails, the key is left unused and the next attempt picks it up.
	dek, err := s.getOrCreateDEK(ctx, p.UserID)
	if err != nil {
		return err
	}
	emailEnc, err := encryptField(dek, p.UserID.String(), "email", []byte(p.Email))
	if err != nil {
		return err
	}
	addressEnc, err := encryptField(dek, p.UserID.String(), "address", []byte(p.Address))
	if err != nil {
		return err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
		INSERT INTO users (user_id, email_enc, email_hash, address_enc)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id) DO UPDATE
		SET email_enc = EXCLUDED.email_enc, email_hash = EXCLUDED.email_hash, address_enc = EXCLUDED.address_enc`,
		p.UserID, emailEnc, s.index.Email(p.Email), addressEnc)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation on email_hash
		return ErrEmailTaken
	}
	if err != nil {
		return fmt.Errorf("upsert user: %w", err)
	}

	if err := appendAudit(ctx, tx, who.Actor, ActionWritePII, p.UserID, who.Reason); err != nil {
		return err
	}
	err = outbox.Write(ctx, tx, TopicUserEvents, p.UserID.String(), profileEvent{
		EventID: uuid.New(), Type: "user.profile_updated", UserID: p.UserID,
		EmailEnc: emailEnc, AddressEnc: addressEnc, OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Get decrypts a user's profile. The read is audited in the same transaction:
// if the audit entry can't be written, the caller gets an error, not the data.
func (s *Store) Get(ctx context.Context, userID uuid.UUID, who Accessor) (Profile, error) {
	// Fetch the key before opening the transaction. The other way round, each Get holds a pooled
	// connection while waiting for another one to read the key (in the Postgres key store), and
	// enough concurrent Gets deadlock the pool. A user with no key has been shredded, or never
	// existed: either way, not found.
	dek, err := s.dek(ctx, userID)
	if err != nil {
		return Profile{}, err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Profile{}, err
	}
	defer tx.Rollback(ctx)

	var emailEnc, addressEnc []byte
	err = tx.QueryRow(ctx, `SELECT email_enc, address_enc FROM users WHERE user_id = $1`, userID).
		Scan(&emailEnc, &addressEnc)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, err
	}
	email, err := decryptField(dek, userID.String(), "email", emailEnc)
	if err != nil {
		return Profile{}, fmt.Errorf("email: %w", err)
	}
	address, err := decryptField(dek, userID.String(), "address", addressEnc)
	if err != nil {
		return Profile{}, fmt.Errorf("address: %w", err)
	}

	if err := appendAudit(ctx, tx, who.Actor, ActionReadPII, userID, who.Reason); err != nil {
		return Profile{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Profile{}, err
	}
	return Profile{UserID: userID, Email: string(email), Address: string(address)}, nil
}

// Delete crypto-shreds a user: it destroys their data key. Every ciphertext encrypted under it,
// in this table, in Kafka, in the outbox, in backups of those, becomes permanently unreadable,
// without having to find and scrub each copy.
//
// Orders stay (financial records have legal retention periods) but hold no PII. The audit log keeps
// the user ID and the fact of deletion, which is what a regulator asks for.
//
// The key goes first, outside the transaction, because it's the step that actually protects the
// user. If the rest then fails, the data is already unreadable, and a retry finishes the cleanup
// and writes the audit entry.
func (s *Store) Delete(ctx context.Context, userID uuid.UUID, who Accessor) error {
	hadKey, err := s.keyStore.Delete(ctx, userID)
	if err != nil {
		return fmt.Errorf("delete key: %w", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `DELETE FROM users WHERE user_id = $1`, userID)
	if err != nil {
		return err
	}
	if !hadKey && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if err := appendAudit(ctx, tx, who.Actor, ActionDeleteUser, userID, who.Reason); err != nil {
		return err
	}
	// Tells downstream services to drop anything they derived from this user.
	err = outbox.Write(ctx, tx, TopicUserEvents, userID.String(), profileEvent{
		EventID: uuid.New(), Type: "user.deleted", UserID: userID, OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Verify checks the audit chain.
func (s *Store) Verify(ctx context.Context) (VerifyResult, error) {
	return VerifyAudit(ctx, s.db)
}

// dek returns the user's unwrapped data key, or ErrNotFound if they have none.
func (s *Store) dek(ctx context.Context, userID uuid.UUID) ([]byte, error) {
	wrapped, err := s.keyStore.Get(ctx, userID)
	if err != nil {
		return nil, err
	}
	dek, err := s.keys.Unwrap(wrapped)
	if err != nil {
		return nil, fmt.Errorf("unwrap key: %w", err)
	}
	return dek, nil
}

// getOrCreateDEK returns the user's data key, creating one on first use. Looking first saves a
// call to the key wrapper (a KMS request on AWS) on every update after the first.
func (s *Store) getOrCreateDEK(ctx context.Context, userID uuid.UUID) ([]byte, error) {
	dek, err := s.dek(ctx, userID)
	if !errors.Is(err, ErrNotFound) {
		return dek, err
	}
	fresh, err := newDEK()
	if err != nil {
		return nil, err
	}
	wrappedFresh, err := s.keys.Wrap(fresh)
	if err != nil {
		return nil, err
	}
	wrapped, err := s.keyStore.CreateIfAbsent(ctx, userID, wrappedFresh)
	if err != nil {
		return nil, fmt.Errorf("store key: %w", err)
	}
	return s.keys.Unwrap(wrapped)
}

// decryptWithStoredKey decrypts a ciphertext found anywhere (e.g. an old Kafka event) using the
// user's current key. After Delete it returns ErrNotFound: there is no key left to try.
func (s *Store) decryptWithStoredKey(ctx context.Context, userID uuid.UUID, field string, ct []byte) ([]byte, error) {
	dek, err := s.dek(ctx, userID)
	if err != nil {
		return nil, err
	}
	return decryptField(dek, userID.String(), field, ct)
}
