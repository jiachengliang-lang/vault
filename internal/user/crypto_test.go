package user

import (
	"bytes"
	"errors"
	"testing"
)

func testDEK(t *testing.T) []byte {
	t.Helper()
	dek, err := newDEK()
	if err != nil {
		t.Fatal(err)
	}
	return dek
}

func TestFieldRoundTrip(t *testing.T) {
	dek := testDEK(t)
	ct, err := encryptField(dek, "u1", "email", []byte("alice@example.com"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ct, []byte("alice")) {
		t.Fatal("ciphertext contains plaintext")
	}
	pt, err := decryptField(dek, "u1", "email", ct)
	if err != nil || string(pt) != "alice@example.com" {
		t.Fatalf("got %q, %v", pt, err)
	}
}

func TestSamePlaintextEncryptsDifferently(t *testing.T) {
	dek := testDEK(t)
	a, _ := encryptField(dek, "u1", "email", []byte("alice@example.com"))
	b, _ := encryptField(dek, "u1", "email", []byte("alice@example.com"))
	if bytes.Equal(a, b) {
		t.Fatal("random nonces must make repeated encryptions differ, or equal values are visible as equal ciphertexts")
	}
}

// Associated data binds a ciphertext to its user and field.
func TestCiphertextCannotBeMovedToAnotherUserOrField(t *testing.T) {
	dek := testDEK(t)
	ct, _ := encryptField(dek, "u1", "email", []byte("alice@example.com"))

	if _, err := decryptField(dek, "u2", "email", ct); !errors.Is(err, ErrDecrypt) {
		t.Errorf("pasted into another user's row: got %v, want ErrDecrypt", err)
	}
	if _, err := decryptField(dek, "u1", "address", ct); !errors.Is(err, ErrDecrypt) {
		t.Errorf("pasted into another column: got %v, want ErrDecrypt", err)
	}
}

func TestTamperingIsDetected(t *testing.T) {
	dek := testDEK(t)
	ct, _ := encryptField(dek, "u1", "email", []byte("alice@example.com"))
	ct[len(ct)-1] ^= 1
	if _, err := decryptField(dek, "u1", "email", ct); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("flipped one bit: got %v, want ErrDecrypt", err)
	}
}

func TestWrongMasterKeyCannotUnwrap(t *testing.T) {
	k1, _ := NewLocalKeyWrapper(testDEK(t))
	k2, _ := NewLocalKeyWrapper(testDEK(t))
	wrapped, err := k1.Wrap(testDEK(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k2.Unwrap(wrapped); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("got %v, want ErrDecrypt", err)
	}
}

func TestBlindIndexNormalizesAndNeedsKey(t *testing.T) {
	idx := NewBlindIndex([]byte("k"))
	if !bytes.Equal(idx.Email("Alice@Example.com "), idx.Email("alice@example.com")) {
		t.Error("case and whitespace variants must map to the same index")
	}
	if bytes.Equal(idx.Email("alice@example.com"), NewBlindIndex([]byte("other")).Email("alice@example.com")) {
		t.Error("a different key must give a different index")
	}
}
