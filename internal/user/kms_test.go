package user

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// fakeKMS acts like KMS for one key: a blob only decrypts under the key and context it was
// encrypted with.
type fakeKMS struct {
	keyID string
	blobs map[string]sealedBlob
}

type sealedBlob struct {
	plaintext []byte
	context   map[string]string
}

func (f *fakeKMS) Encrypt(_ context.Context, in *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	if *in.KeyId != f.keyID {
		return nil, errors.New("no such key")
	}
	blob := "blob-" + string(rune('a'+len(f.blobs)))
	f.blobs[blob] = sealedBlob{plaintext: in.Plaintext, context: maps.Clone(in.EncryptionContext)}
	return &kms.EncryptOutput{CiphertextBlob: []byte(blob)}, nil
}

func (f *fakeKMS) Decrypt(_ context.Context, in *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	b, ok := f.blobs[string(in.CiphertextBlob)]
	if !ok || in.KeyId == nil || *in.KeyId != f.keyID || !maps.Equal(b.context, in.EncryptionContext) {
		return nil, errors.New("InvalidCiphertextException")
	}
	return &kms.DecryptOutput{Plaintext: b.plaintext}, nil
}

func TestKMSKeyWrapper(t *testing.T) {
	fake := &fakeKMS{keyID: "arn:aws:kms:us-east-2:111111111111:key/test", blobs: map[string]sealedBlob{}}
	w := NewKMSKeyWrapper(fake, fake.keyID)
	dek := testDEK(t)

	wrapped, err := w.Wrap(dek)
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.Unwrap(wrapped)
	if err != nil || string(got) != string(dek) {
		t.Fatalf("round trip: got %x, %v", got, err)
	}

	// The same blob, decrypted without our context, must fail: that's what stops other ciphertext
	// under this key from being used as a data key.
	if _, err := fake.Decrypt(context.Background(), &kms.DecryptInput{KeyId: &fake.keyID, CiphertextBlob: wrapped}); err == nil {
		t.Fatal("blob decrypted without the encryption context")
	}
	// A wrapper configured with a different key refuses the blob rather than trying it.
	other := NewKMSKeyWrapper(fake, "arn:aws:kms:us-east-2:111111111111:key/other")
	if _, err := other.Unwrap(wrapped); err == nil {
		t.Fatal("blob decrypted under a different key id")
	}
}
