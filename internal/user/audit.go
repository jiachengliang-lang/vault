package user

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	auditEntries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "audit_entries_total",
		Help: "PII accesses recorded in the audit log, by action and actor type (user, support).",
	}, []string{"action", "actor_type"})
	chainValid = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "audit_chain_valid",
		Help: "1 if the last audit chain verification passed, 0 if it found tampering.",
	})
)

const (
	ActionReadPII    = "READ_PII"
	ActionWritePII   = "WRITE_PII"
	ActionDeleteUser = "DELETE_USER"

	auditLockID = 7_700_002
)

// Counters start at 0 for every known label set. Prometheus can't compute an increase for a series
// whose first sample is already 1, so without this the first support access would never show up.
func init() {
	for _, action := range []string{ActionReadPII, ActionWritePII, ActionDeleteUser} {
		for _, actor := range []string{"user", "support"} {
			auditEntries.WithLabelValues(action, actor)
		}
	}
}

// AuditEntry records who touched whose PII, and why.
type AuditEntry struct {
	Seq       int64
	Actor     string
	Action    string
	SubjectID uuid.UUID
	Reason    string
	TS        time.Time
	PrevHash  []byte
	Hash      []byte
}

// The audit log is a hash chain: each entry's hash covers its own fields plus the previous
// entry's hash. Editing, deleting or inserting a row in the middle changes that row's hash,
// which no longer matches the next row's prev_hash, so Verify pinpoints where the chain breaks.
//
// Limitation: someone with write access to the whole table could rewrite every hash from the
// edit onward. The fix is anchoring: periodically copy the head hash somewhere they can't write
// (a separate system, a WORM bucket, a signed checkpoint). Verify returns the head for that purpose.

var genesisHash = make([]byte, sha256.Size)

// computeHash length-prefixes every variable field, so ("ab", "c") and ("a", "bc")
// can't produce the same bytes and therefore the same hash.
func computeHash(prev []byte, seq int64, actor, action string, subject uuid.UUID, reason string, ts time.Time) []byte {
	h := sha256.New()
	h.Write(prev)
	binary.Write(h, binary.BigEndian, seq)
	for _, s := range []string{actor, action, subject.String(), reason} {
		binary.Write(h, binary.BigEndian, uint32(len(s)))
		h.Write([]byte(s))
	}
	binary.Write(h, binary.BigEndian, ts.UnixMicro())
	return h.Sum(nil)
}

// appendAudit adds an entry inside the caller's transaction. The PII operation and its audit
// record commit together or not at all: if the audit write fails, the read or delete fails too.
// "No audit, no access."
//
// Entries are serialized with an advisory lock, since each needs the previous hash. That makes the
// log a single point of contention: fine here, and a known tradeoff (shard chains per tenant to scale).
func appendAudit(ctx context.Context, tx pgx.Tx, actor, action string, subject uuid.UUID, reason string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, auditLockID); err != nil {
		return err
	}
	prev := genesisHash
	err := tx.QueryRow(ctx, `SELECT hash FROM audit_log ORDER BY seq DESC LIMIT 1`).Scan(&prev)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read audit head: %w", err)
	}
	var seq int64
	if err := tx.QueryRow(ctx, `SELECT nextval(pg_get_serial_sequence('audit_log', 'seq'))`).Scan(&seq); err != nil {
		return err
	}
	// Postgres stores microseconds; truncate so the hash we compute matches what we read back.
	ts := time.Now().UTC().Truncate(time.Microsecond)
	hash := computeHash(prev, seq, actor, action, subject, reason, ts)
	_, err = tx.Exec(ctx, `
		INSERT INTO audit_log (seq, actor, action, subject_id, reason, prev_hash, hash, ts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		seq, actor, action, subject, reason, prev, hash, ts)
	if err != nil {
		return fmt.Errorf("write audit entry: %w", err)
	}
	actorType, _, _ := strings.Cut(actor, ":")
	auditEntries.WithLabelValues(action, actorType).Inc()
	return nil
}

// VerifyResult reports whether the chain is intact. BrokenAt is the first bad seq, or 0.
type VerifyResult struct {
	OK       bool
	Entries  int
	BrokenAt int64
	Problem  string
	HeadHash []byte
}

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// VerifyAudit walks the whole chain and recomputes every hash.
func VerifyAudit(ctx context.Context, q Querier) (res VerifyResult, err error) {
	defer func() {
		switch {
		case err != nil:
		case res.OK:
			chainValid.Set(1)
		default:
			chainValid.Set(0)
		}
	}()
	rows, err := q.Query(ctx, `
		SELECT seq, actor, action, subject_id, reason, ts, prev_hash, hash
		FROM audit_log ORDER BY seq`)
	if err != nil {
		return VerifyResult{}, err
	}
	defer rows.Close()

	res = VerifyResult{OK: true, HeadHash: genesisHash}
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.Seq, &e.Actor, &e.Action, &e.SubjectID, &e.Reason, &e.TS, &e.PrevHash, &e.Hash); err != nil {
			return VerifyResult{}, err
		}
		res.Entries++
		switch {
		case string(e.PrevHash) != string(res.HeadHash):
			res.OK, res.BrokenAt, res.Problem = false, e.Seq, "prev_hash doesn't match the previous entry (row deleted, inserted or reordered)"
		case string(e.Hash) != string(computeHash(e.PrevHash, e.Seq, e.Actor, e.Action, e.SubjectID, e.Reason, e.TS)):
			res.OK, res.BrokenAt, res.Problem = false, e.Seq, "hash doesn't match the entry's contents (row edited)"
		}
		if !res.OK {
			return res, nil
		}
		res.HeadHash = e.Hash
	}
	return res, rows.Err()
}
