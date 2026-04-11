package pgdynamo

import (
	"context"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func newClient(store *storage.Store) *dynamodb.Client {
	handler := NewHandler(store)
	transport := &pgTransport{handler: handler}

	return dynamodb.New(dynamodb.Options{
		Region:           "us-east-1",
		Credentials:      credentials.NewStaticCredentialsProvider("fake", "fake", "fake"),
		HTTPClient:       transport,
		BaseEndpoint:     aws.String("http://localhost:8000"),
		RetryMaxAttempts: 1,
	})
}

// New creates a *dynamodb.Client backed by PostgreSQL.
// The returned client can be used exactly like a real DynamoDB client.
func New(ctx context.Context, pgConnString string) (*dynamodb.Client, error) {
	store, err := storage.NewStore(ctx, pgConnString)
	if err != nil {
		return nil, err
	}
	return newClient(store), nil
}

// NewWithCleanup creates a *dynamodb.Client backed by PostgreSQL
// and returns a cleanup function that closes the database connection.
func NewWithCleanup(ctx context.Context, pgConnString string) (*dynamodb.Client, func(), error) {
	store, err := storage.NewStore(ctx, pgConnString)
	if err != nil {
		return nil, nil, err
	}
	return newClient(store), store.Close, nil
}

// NewWithConfig creates a *dynamodb.Client with custom connection pool settings.
func NewWithConfig(ctx context.Context, cfg storage.StoreConfig) (*dynamodb.Client, func(), error) {
	store, err := storage.NewStoreWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return newClient(store), store.Close, nil
}
