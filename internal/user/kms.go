package user

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// kmsAPI is the part of the KMS client this file uses, so tests can stand in for it.
type kmsAPI interface {
	Encrypt(ctx context.Context, in *kms.EncryptInput, opts ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(ctx context.Context, in *kms.DecryptInput, opts ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

// KMSKeyWrapper wraps data keys with a master key that never leaves AWS KMS. The service only
// ever holds a data key in memory; stealing its config or the database gets an attacker nothing
// they can unwrap, and every unwrap shows up in CloudTrail.
type KMSKeyWrapper struct {
	client kmsAPI
	keyID  string
}

func NewKMSKeyWrapper(client kmsAPI, keyID string) *KMSKeyWrapper {
	return &KMSKeyWrapper{client: client, keyID: keyID}
}

// The encryption context does for KMS what "dek" does as AAD in LocalKeyWrapper: a blob only
// decrypts when the same context is given, so ciphertext made for some other purpose under this
// key can't be passed off as a data key.
var dekContext = map[string]string{"purpose": "vault-user-dek"}

// KeyWrapper has no context parameter, so each call gets its own deadline.
const kmsTimeout = 2 * time.Second

func (w *KMSKeyWrapper) Wrap(dek []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), kmsTimeout)
	defer cancel()
	out, err := w.client.Encrypt(ctx, &kms.EncryptInput{
		KeyId: &w.keyID, Plaintext: dek, EncryptionContext: dekContext,
	})
	if err != nil {
		return nil, fmt.Errorf("kms encrypt: %w", err)
	}
	return out.CiphertextBlob, nil
}

func (w *KMSKeyWrapper) Unwrap(wrapped []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), kmsTimeout)
	defer cancel()
	// KMS can tell which key a blob belongs to, but naming it means a blob from any other key
	// is rejected rather than decrypted with whatever key this role happens to be allowed to use.
	out, err := w.client.Decrypt(ctx, &kms.DecryptInput{
		KeyId: &w.keyID, CiphertextBlob: wrapped, EncryptionContext: dekContext,
	})
	if err != nil {
		return nil, fmt.Errorf("kms decrypt: %w", err)
	}
	return out.Plaintext, nil
}
