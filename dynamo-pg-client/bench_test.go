package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo"
	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func setupBench(b *testing.B) (*dynamodb.Client, func()) {
	b.Helper()
	ctx := context.Background()
	client, cleanup, err := pgdynamo.NewWithCleanup(ctx, getConnString())
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	return client, cleanup
}

func setupBenchCached(b *testing.B) (*dynamodb.Client, func()) {
	b.Helper()
	ctx := context.Background()
	client, cleanup, err := pgdynamo.NewWithConfig(ctx, storage.StoreConfig{
		ConnString:      getConnString(),
		EnableMetaCache: true,
	})
	if err != nil {
		b.Fatalf("failed to create client: %v", err)
	}
	return client, cleanup
}

func createBenchTable(b *testing.B, client *dynamodb.Client, tableName string) {
	b.Helper()
	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		b.Fatalf("CreateTable: %v", err)
	}
}

// BenchmarkPutItem measures single item write throughput.
func BenchmarkPutItem(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_put"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("user#%d", i)},
				"SK":   &types.AttributeValueMemberS{Value: "profile"},
				"Name": &types.AttributeValueMemberS{Value: "Alice"},
				"Age":  &types.AttributeValueMemberN{Value: "30"},
			},
		})
		if err != nil {
			b.Fatalf("PutItem: %v", err)
		}
	}
}

// BenchmarkGetItem measures single item read throughput.
func BenchmarkGetItem(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_get"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	// Seed a single item
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "user#1"},
			"SK":   &types.AttributeValueMemberS{Value: "profile"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"},
			"Age":  &types.AttributeValueMemberN{Value: "30"},
		},
	})
	if err != nil {
		b.Fatalf("seed PutItem: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "user#1"},
				"SK": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		if err != nil {
			b.Fatalf("GetItem: %v", err)
		}
	}
}

// BenchmarkQuery measures query throughput with SK range condition.
func BenchmarkQuery(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_query"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	// Seed 100 items under the same PK
	for i := 0; i < 100; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "user#1"},
				"SK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("event#%03d", i)},
				"Data": &types.AttributeValueMemberS{Value: "payload"},
			},
		})
		if err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     &types.AttributeValueMemberS{Value: "user#1"},
				":prefix": &types.AttributeValueMemberS{Value: "event#0"},
			},
		})
		if err != nil {
			b.Fatalf("Query: %v", err)
		}
	}
}

// BenchmarkQueryWithFilter measures query with filter expression.
func BenchmarkQueryWithFilter(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_query_filter"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":     &types.AttributeValueMemberS{Value: "user#1"},
				"SK":     &types.AttributeValueMemberS{Value: fmt.Sprintf("item#%03d", i)},
				"Status": &types.AttributeValueMemberS{Value: fmt.Sprintf("status_%d", i%3)},
			},
		})
		if err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("PK = :pk"),
			FilterExpression:       aws.String("#s = :status"),
			ExpressionAttributeNames: map[string]string{
				"#s": "Status",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     &types.AttributeValueMemberS{Value: "user#1"},
				":status": &types.AttributeValueMemberS{Value: "status_0"},
			},
		})
		if err != nil {
			b.Fatalf("Query: %v", err)
		}
	}
}

// BenchmarkScan measures full table scan throughput.
func BenchmarkScan(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_scan"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%d", i)},
				"SK":   &types.AttributeValueMemberS{Value: "data"},
				"Data": &types.AttributeValueMemberS{Value: "payload"},
			},
		})
		if err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			b.Fatalf("Scan: %v", err)
		}
	}
}

// BenchmarkDeleteItem measures single item delete throughput.
func BenchmarkDeleteItem(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_delete"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	// Pre-seed items to delete
	for i := 0; i < b.N; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%d", i)},
				"SK": &types.AttributeValueMemberS{Value: "data"},
			},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%d", i)},
				"SK": &types.AttributeValueMemberS{Value: "data"},
			},
		})
		if err != nil {
			b.Fatalf("DeleteItem: %v", err)
		}
	}
}

// BenchmarkBatchWriteItem measures batch write throughput (25 items per batch).
func BenchmarkBatchWriteItem(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_batch_write"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		requests := make([]types.WriteRequest, 25)
		for j := 0; j < 25; j++ {
			requests[j] = types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"PK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%d_%d", i, j)},
						"SK":   &types.AttributeValueMemberS{Value: "data"},
						"Data": &types.AttributeValueMemberS{Value: "payload"},
					},
				},
			}
		}
		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: requests,
			},
		})
		if err != nil {
			b.Fatalf("BatchWriteItem: %v", err)
		}
	}
}

// BenchmarkUpdateItem measures single item update throughput.
func BenchmarkUpdateItem(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_update"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	// Seed one item to update repeatedly
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":      &types.AttributeValueMemberS{Value: "user#1"},
			"SK":      &types.AttributeValueMemberS{Value: "profile"},
			"Counter": &types.AttributeValueMemberN{Value: "0"},
		},
	})
	if err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "user#1"},
				"SK": &types.AttributeValueMemberS{Value: "profile"},
			},
			UpdateExpression: aws.String("SET Counter = Counter + :inc"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":inc": &types.AttributeValueMemberN{Value: "1"},
			},
		})
		if err != nil {
			b.Fatalf("UpdateItem: %v", err)
		}
	}
}

// BenchmarkTransactWriteItems measures transactional write throughput.
func BenchmarkTransactWriteItems(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_transact"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items := make([]types.TransactWriteItem, 5)
		for j := 0; j < 5; j++ {
			items[j] = types.TransactWriteItem{
				Put: &types.Put{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"PK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("tx#%d_%d", i, j)},
						"SK":   &types.AttributeValueMemberS{Value: "data"},
						"Data": &types.AttributeValueMemberS{Value: "payload"},
					},
				},
			}
		}
		_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: items,
		})
		if err != nil {
			b.Fatalf("TransactWriteItems: %v", err)
		}
	}
}

// BenchmarkPutItemLargeItem measures write throughput with a larger item (many attributes).
func BenchmarkPutItemLargeItem(b *testing.B) {
	client, cleanup := setupBench(b)
	defer cleanup()

	tableName := "bench_put_large"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	// Build a large item with 50 attributes
	item := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: "user#1"},
		"SK": &types.AttributeValueMemberS{Value: "profile"},
	}
	for j := 0; j < 50; j++ {
		item[fmt.Sprintf("Attr%d", j)] = &types.AttributeValueMemberS{
			Value: fmt.Sprintf("value_%d_with_some_padding_to_make_it_longer", j),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item["PK"] = &types.AttributeValueMemberS{Value: fmt.Sprintf("user#%d", i)}
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		if err != nil {
			b.Fatalf("PutItem: %v", err)
		}
	}
}

// --- Cached variants to measure metadata caching impact ---

// BenchmarkPutItemCached measures PutItem with metadata caching enabled.
func BenchmarkPutItemCached(b *testing.B) {
	client, cleanup := setupBenchCached(b)
	defer cleanup()

	tableName := "bench_put_cached"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("user#%d", i)},
				"SK":   &types.AttributeValueMemberS{Value: "profile"},
				"Name": &types.AttributeValueMemberS{Value: "Alice"},
				"Age":  &types.AttributeValueMemberN{Value: "30"},
			},
		})
		if err != nil {
			b.Fatalf("PutItem: %v", err)
		}
	}
}

// BenchmarkGetItemCached measures GetItem with metadata caching enabled.
func BenchmarkGetItemCached(b *testing.B) {
	client, cleanup := setupBenchCached(b)
	defer cleanup()

	tableName := "bench_get_cached"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "user#1"},
			"SK":   &types.AttributeValueMemberS{Value: "profile"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"},
			"Age":  &types.AttributeValueMemberN{Value: "30"},
		},
	})
	if err != nil {
		b.Fatalf("seed PutItem: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "user#1"},
				"SK": &types.AttributeValueMemberS{Value: "profile"},
			},
		})
		if err != nil {
			b.Fatalf("GetItem: %v", err)
		}
	}
}

// BenchmarkQueryCached measures Query with metadata caching enabled.
func BenchmarkQueryCached(b *testing.B) {
	client, cleanup := setupBenchCached(b)
	defer cleanup()

	tableName := "bench_query_cached"
	createBenchTable(b, client, tableName)
	defer func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "user#1"},
				"SK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("event#%03d", i)},
				"Data": &types.AttributeValueMemberS{Value: "payload"},
			},
		})
		if err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     &types.AttributeValueMemberS{Value: "user#1"},
				":prefix": &types.AttributeValueMemberS{Value: "event#0"},
			},
		})
		if err != nil {
			b.Fatalf("Query: %v", err)
		}
	}
}
