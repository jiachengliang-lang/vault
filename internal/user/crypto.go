// Package user stores user PII encrypted under a per-user key, so deleting that key
// (crypto-shredding) makes every copy of the user's data unreadable at once.
package user

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
)

// Envelope encryption, two layers:
//
//	master key (KEK) --wraps--> per-user data key (DEK) --encrypts--> email, address
//
// The DEK is stored only in wrapped form. In production the master key lives in a KMS/HSM and never
// leaves it; here it comes from an env var behind the same interface, so swapping in a KMS is one type.

const keySize = 32 // AES-256

var ErrDecrypt = errors.New("decryption failed: wrong key, or ciphertext was tampered with")

// KeyWrapper protects data keys with a master key. A cloud KMS implements the same two operations.
type KeyWrapper interface {
	Wrap(dek []byte) ([]byte, error)
	Unwrap(wrapped []byte) ([]byte, error)
}

// LocalKeyWrapper wraps DEKs with AES-GCM under a master key held in memory. Dev only.
type LocalKeyWrapper struct {
	aead cipher.AEAD
}

func NewLocalKeyWrapper(masterKey []byte) (*LocalKeyWrapper, error) {
	aead, err := newAEAD(masterKey)
	if err != nil {
		return nil, fmt.Errorf("master key: %w", err)
	}
	return &LocalKeyWrapper{aead: aead}, nil
}

func (w *LocalKeyWrapper) Wrap(dek []byte) ([]byte, error) {
	return seal(w.aead, dek, []byte("dek"))
}

func (w *LocalKeyWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	return open(w.aead, wrapped, []byte("dek"))
}

func newDEK() ([]byte, error) {
	dek := make([]byte, keySize)
	_, err := rand.Read(dek)
	return dek, err
}

// encryptField encrypts one PII field under the user's DEK.
//
// The associated data binds each ciphertext to (user, field). AES-GCM authenticates it, so a
// ciphertext copied into another user's row, or from the address column into the email column,
// fails to decrypt instead of silently showing the wrong person's data.
func encryptField(dek []byte, userID, field string, plaintext []byte) ([]byte, error) {
	aead, err := newAEAD(dek)
	if err != nil {
		return nil, err
	}
	return seal(aead, plaintext, fieldAAD(userID, field))
}

func decryptField(dek []byte, userID, field string, ciphertext []byte) ([]byte, error) {
	aead, err := newAEAD(dek)
	if err != nil {
		return nil, err
	}
	return open(aead, ciphertext, fieldAAD(userID, field))
}

func fieldAAD(userID, field string) []byte {
	return []byte("user:" + userID + "|field:" + field)
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != keySize {
		return nil, fmt.Errorf("key must be %d bytes, got %d", keySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// seal returns nonce || ciphertext. A fresh random nonce per message is essential:
// reusing a GCM nonce under the same key leaks the XOR of the plaintexts and breaks authentication.
func seal(aead cipher.AEAD, plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, aead.NonceSize(), aead.NonceSize()+len(plaintext)+aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plaintext, aad), nil
}

func open(aead cipher.AEAD, sealed, aad []byte) ([]byte, error) {
	if len(sealed) < aead.NonceSize() {
		return nil, ErrDecrypt
	}
	nonce, ct := sealed[:aead.NonceSize()], sealed[aead.NonceSize():]
	pt, err := aead.Open(nil, nonce, ct, aad)
	if err != nil {
		return nil, ErrDecrypt
	}
	return pt, nil
}

// BlindIndex lets us enforce "one account per email" and look users up by email without
// decrypting every row. It's a keyed hash of the normalized email, for the same reason the
// pipeline uses HMAC: a plain hash of an email can be reversed by hashing a list of known emails.
type BlindIndex struct {
	key []byte
}

func NewBlindIndex(key []byte) BlindIndex {
	return BlindIndex{key: key}
}

func (b BlindIndex) Email(email string) []byte {
	mac := hmac.New(sha256.New, b.key)
	mac.Write([]byte(strings.ToLower(strings.TrimSpace(email))))
	return mac.Sum(nil)
}
