package user

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// KeyStore holds each user's wrapped data key.
//
// It's kept apart from the encrypted data so that a backup of the database holds no keys: with
// keys in the same database, restoring an old backup would bring back a deleted user's key, and
// with it their data. Locally it's the user_keys table, for convenience; on AWS it's DynamoDB.
type KeyStore interface {
	// CreateIfAbsent stores wrapped unless the user already has a key, and returns whichever key
	// ends up stored. Two concurrent first writes therefore agree on one key.
	CreateIfAbsent(ctx context.Context, userID uuid.UUID, wrapped []byte) ([]byte, error)
	// Get returns the user's wrapped key, or ErrNotFound.
	Get(ctx context.Context, userID uuid.UUID) ([]byte, error)
	// Delete destroys the user's key and reports whether there was one.
	Delete(ctx context.Context, userID uuid.UUID) (bool, error)
}

// PostgresKeyStore keeps keys in the user_keys table. Local development only.
type PostgresKeyStore struct {
	db *pgxpool.Pool
}

func NewPostgresKeyStore(db *pgxpool.Pool) *PostgresKeyStore {
	return &PostgresKeyStore{db: db}
}

func (s *PostgresKeyStore) CreateIfAbsent(ctx context.Context, userID uuid.UUID, wrapped []byte) ([]byte, error) {
	if _, err := s.db.Exec(ctx, `INSERT INTO user_keys (user_id, wrapped_dek) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING`,
		userID, wrapped); err != nil {
		return nil, err
	}
	return s.Get(ctx, userID)
}

func (s *PostgresKeyStore) Get(ctx context.Context, userID uuid.UUID) ([]byte, error) {
	var wrapped []byte
	err := s.db.QueryRow(ctx, `SELECT wrapped_dek FROM user_keys WHERE user_id = $1`, userID).Scan(&wrapped)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return wrapped, err
}

func (s *PostgresKeyStore) Delete(ctx context.Context, userID uuid.UUID) (bool, error) {
	tag, err := s.db.Exec(ctx, `DELETE FROM user_keys WHERE user_id = $1`, userID)
	return tag.RowsAffected() > 0, err
}

// dynamoAPI is the part of the DynamoDB client this file uses.
type dynamoAPI interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// DynamoKeyStore keeps keys in a DynamoDB table keyed by user_id.
//
// Point-in-time recovery stays off on that table on purpose. With it on, a deleted key could be
// restored for 35 days, which would undo crypto-shredding for that long.
type DynamoKeyStore struct {
	client dynamoAPI
	table  string
}

func NewDynamoKeyStore(client dynamoAPI, table string) *DynamoKeyStore {
	return &DynamoKeyStore{client: client, table: table}
}

func dynamoKey(userID uuid.UUID) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"user_id": &types.AttributeValueMemberS{Value: userID.String()}}
}

func (s *DynamoKeyStore) CreateIfAbsent(ctx context.Context, userID uuid.UUID, wrapped []byte) ([]byte, error) {
	item := dynamoKey(userID)
	item["wrapped_dek"] = &types.AttributeValueMemberB{Value: wrapped}
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &s.table,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(user_id)"),
	})
	var exists *types.ConditionalCheckFailedException
	if errors.As(err, &exists) {
		return s.Get(ctx, userID) // someone else's key won
	}
	if err != nil {
		return nil, err
	}
	return wrapped, nil
}

func (s *DynamoKeyStore) Get(ctx context.Context, userID uuid.UUID) ([]byte, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.table,
		Key:       dynamoKey(userID),
		// A read right after a delete must not see the old key.
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	b, ok := out.Item["wrapped_dek"].(*types.AttributeValueMemberB)
	if !ok {
		return nil, ErrNotFound
	}
	return b.Value, nil
}

func (s *DynamoKeyStore) Delete(ctx context.Context, userID uuid.UUID) (bool, error) {
	out, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:    &s.table,
		Key:          dynamoKey(userID),
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		return false, err
	}
	return len(out.Attributes) > 0, nil
}
