package main

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestQueryBeginsWithSK(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_begins_with_sk"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	items := []struct{ pk, sk string }{
		{"u1", "ORDER#2024-01"}, {"u1", "ORDER#2024-02"}, {"u1", "ORDER#2025-01"},
		{"u1", "PROFILE"}, {"u1", "SETTINGS"},
	}
	for _, it := range items {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: it.pk},
				"SK": &types.AttributeValueMemberS{Value: it.sk},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "u1"},
			":prefix": &types.AttributeValueMemberS{Value: "ORDER#2024"},
		},
	})
	if err != nil {
		t.Fatalf("Query begins_with: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("expected 2 ORDER#2024 items, got %d", out.Count)
	}
}

func TestQueryBetweenSK(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_between_sk"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 10; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("item#%02d", i)},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND SK BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":lo": &types.AttributeValueMemberS{Value: "item#03"},
			":hi": &types.AttributeValueMemberS{Value: "item#07"},
		},
	})
	if err != nil {
		t.Fatalf("Query BETWEEN: %v", err)
	}
	if out.Count != 5 {
		t.Fatalf("expected 5, got %d", out.Count)
	}
}

func TestQueryEmptyResult(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_empty"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "nonexistent"},
		},
	})
	if err != nil {
		t.Fatalf("Query empty: %v", err)
	}
	if out.Count != 0 {
		t.Fatalf("expected 0, got %d", out.Count)
	}
	if len(out.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(out.Items))
	}
	if out.LastEvaluatedKey != nil {
		t.Fatal("expected no LastEvaluatedKey")
	}
}

func TestScanEmptyTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_empty"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableName)})
	if err != nil {
		t.Fatalf("Scan empty: %v", err)
	}
	if out.Count != 0 {
		t.Fatalf("expected 0, got %d", out.Count)
	}
	if len(out.Items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(out.Items))
	}
}

func TestPutItemOverwrite(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_put_overwrite"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "p1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"},
			"Age":  &types.AttributeValueMemberN{Value: "30"},
		},
	})

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "p1"},
			"SK":    &types.AttributeValueMemberS{Value: "s1"},
			"Name":  &types.AttributeValueMemberS{Value: "Bob"},
			"Email": &types.AttributeValueMemberS{Value: "bob@test.com"},
		},
	})

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if out.Item["Name"].(*types.AttributeValueMemberS).Value != "Bob" {
		t.Fatal("expected Name=Bob after overwrite")
	}
	// PutItem replaces entirely — Age should be gone
	if _, ok := out.Item["Age"]; ok {
		t.Fatal("Age should not exist after PutItem overwrite")
	}
}

func TestDeleteItemNonExisting(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_delete_nonexisting"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	out, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "nope"},
			"SK": &types.AttributeValueMemberS{Value: "nope"},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		t.Fatalf("DeleteItem non-existing: %v", err)
	}
	if len(out.Attributes) != 0 {
		t.Fatalf("expected no attributes, got %d", len(out.Attributes))
	}
}

func TestPutItemAllAttributeTypes(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_all_types"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":     &types.AttributeValueMemberS{Value: "p1"},
			"SK":     &types.AttributeValueMemberS{Value: "s1"},
			"String": &types.AttributeValueMemberS{Value: "hello"},
			"Number": &types.AttributeValueMemberN{Value: "42.5"},
			"Binary": &types.AttributeValueMemberB{Value: []byte("binary data")},
			"Bool":   &types.AttributeValueMemberBOOL{Value: true},
			"Null":   &types.AttributeValueMemberNULL{Value: true},
			"StrSet": &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}},
			"NumSet": &types.AttributeValueMemberNS{Value: []string{"1", "2", "3"}},
			"BinSet": &types.AttributeValueMemberBS{Value: [][]byte{[]byte("x"), []byte("y")}},
			"List": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "item1"},
				&types.AttributeValueMemberN{Value: "100"},
				&types.AttributeValueMemberBOOL{Value: false},
			}},
			"Map": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"nested": &types.AttributeValueMemberS{Value: "value"},
				"num":    &types.AttributeValueMemberN{Value: "99"},
				"flag":   &types.AttributeValueMemberBOOL{Value: true},
			}},
		},
	})
	if err != nil {
		t.Fatalf("PutItem all types: %v", err)
	}

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}

	if out.Item["String"].(*types.AttributeValueMemberS).Value != "hello" {
		t.Fatal("String mismatch")
	}
	if out.Item["Number"].(*types.AttributeValueMemberN).Value != "42.5" {
		t.Fatal("Number mismatch")
	}
	if out.Item["Bool"].(*types.AttributeValueMemberBOOL).Value != true {
		t.Fatal("Bool mismatch")
	}
	if out.Item["Null"].(*types.AttributeValueMemberNULL).Value != true {
		t.Fatal("Null mismatch")
	}
	if len(out.Item["StrSet"].(*types.AttributeValueMemberSS).Value) != 3 {
		t.Fatal("StrSet length mismatch")
	}
	if len(out.Item["NumSet"].(*types.AttributeValueMemberNS).Value) != 3 {
		t.Fatal("NumSet length mismatch")
	}
	if len(out.Item["List"].(*types.AttributeValueMemberL).Value) != 3 {
		t.Fatal("List length mismatch")
	}
	m := out.Item["Map"].(*types.AttributeValueMemberM).Value
	if len(m) != 3 {
		t.Fatal("Map length mismatch")
	}
	if m["nested"].(*types.AttributeValueMemberS).Value != "value" {
		t.Fatal("Map nested value mismatch")
	}
}

func TestUpdateItemArithmetic(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_arithmetic"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "p1"},
			"SK":    &types.AttributeValueMemberS{Value: "s1"},
			"Count": &types.AttributeValueMemberN{Value: "10"},
		},
	})

	// Increment
	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression:         aws.String("SET #c = #c + :inc"),
		ExpressionAttributeNames: map[string]string{"#c": "Count"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc": &types.AttributeValueMemberN{Value: "5"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("increment: %v", err)
	}
	if out.Attributes["Count"].(*types.AttributeValueMemberN).Value != "15" {
		t.Fatalf("expected 15, got %s", out.Attributes["Count"].(*types.AttributeValueMemberN).Value)
	}

	// Decrement
	out, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression:         aws.String("SET #c = #c - :dec"),
		ExpressionAttributeNames: map[string]string{"#c": "Count"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":dec": &types.AttributeValueMemberN{Value: "3"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("decrement: %v", err)
	}
	if out.Attributes["Count"].(*types.AttributeValueMemberN).Value != "12" {
		t.Fatalf("expected 12, got %s", out.Attributes["Count"].(*types.AttributeValueMemberN).Value)
	}
}

func TestBatchGetItemPartialResults(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_batch_get_partial"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p2"}, "SK": &types.AttributeValueMemberS{Value: "s2"},
		},
	})

	out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				Keys: []map[string]types.AttributeValue{
					{"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"}},
					{"PK": &types.AttributeValueMemberS{Value: "p2"}, "SK": &types.AttributeValueMemberS{Value: "s2"}},
					{"PK": &types.AttributeValueMemberS{Value: "p3"}, "SK": &types.AttributeValueMemberS{Value: "s3"}}, // missing
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("BatchGetItem: %v", err)
	}
	if len(out.Responses[tableName]) != 2 {
		t.Fatalf("expected 2 results, got %d", len(out.Responses[tableName]))
	}
}

func TestBatchWriteMixedPutsAndDeletes(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_batch_mixed"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 3; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("p%d", i)},
				"SK": &types.AttributeValueMemberS{Value: "s1"},
			},
		})
	}

	_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: {
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
				}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "p4"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
				}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "p5"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
				}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("BatchWriteItem mixed: %v", err)
	}

	scan, _ := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableName)})
	if scan.Count != 4 {
		t.Fatalf("expected 4 items, got %d", scan.Count)
	}

	get, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if get.Item != nil {
		t.Fatal("p1 should be deleted")
	}
}

func TestBatchWriteLargeCount(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_batch_large"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	requests := make([]types.WriteRequest, 25)
	for i := 0; i < 25; i++ {
		requests[i] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("user#%02d", i)},
					"SK": &types.AttributeValueMemberS{Value: "profile"},
				},
			},
		}
	}

	_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{tableName: requests},
	})
	if err != nil {
		t.Fatalf("BatchWriteItem 25: %v", err)
	}

	scan, _ := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableName)})
	if scan.Count != 25 {
		t.Fatalf("expected 25, got %d", scan.Count)
	}
}

func TestNumericPKTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_numeric_pk"
	defer deleteTestTable(t, client, tableName)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: types.ScalarAttributeTypeN},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"ID":   &types.AttributeValueMemberN{Value: "42"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"},
		},
	})

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberN{Value: "42"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if out.Item["Name"].(*types.AttributeValueMemberS).Value != "Alice" {
		t.Fatal("expected Name=Alice")
	}
}

func TestQueryDescending(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_desc"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for _, sk := range []string{"a", "b", "c", "d", "e"} {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: sk},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
		ScanIndexForward: aws.Bool(false),
	})
	if err != nil {
		t.Fatalf("Query descending: %v", err)
	}
	firstSK := out.Items[0]["SK"].(*types.AttributeValueMemberS).Value
	lastSK := out.Items[4]["SK"].(*types.AttributeValueMemberS).Value
	if firstSK != "e" || lastSK != "a" {
		t.Fatalf("expected e..a, got %s..%s", firstSK, lastSK)
	}
}

func TestQueryWithFilterAndPagination(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_filter_page"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 10; i++ {
		status := "active"
		if i%2 == 0 {
			status = "inactive"
		}
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":     &types.AttributeValueMemberS{Value: "p1"},
				"SK":     &types.AttributeValueMemberS{Value: fmt.Sprintf("item#%02d", i)},
				"Status": &types.AttributeValueMemberS{Value: status},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("#s = :active"),
		ExpressionAttributeNames: map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"}, ":active": &types.AttributeValueMemberS{Value: "active"},
		},
		Limit: aws.Int32(5),
	})
	if err != nil {
		t.Fatalf("Query filter+page: %v", err)
	}
	if out.ScannedCount != 5 {
		t.Fatalf("expected ScannedCount=5, got %d", out.ScannedCount)
	}
	for _, item := range out.Items {
		if item["Status"].(*types.AttributeValueMemberS).Value != "active" {
			t.Fatal("filtered item should be active")
		}
	}
	if out.LastEvaluatedKey == nil {
		t.Fatal("expected LastEvaluatedKey")
	}
}

func TestUpdateItemMultipleSETClauses(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_multi_set"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"}, "Age": &types.AttributeValueMemberN{Value: "25"},
		},
	})

	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression:         aws.String("SET #n = :name, #a = :age, Email = :email"),
		ExpressionAttributeNames: map[string]string{"#n": "Name", "#a": "Age"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":name": &types.AttributeValueMemberS{Value: "Bob"}, ":age": &types.AttributeValueMemberN{Value: "30"},
			":email": &types.AttributeValueMemberS{Value: "bob@test.com"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("UpdateItem multi SET: %v", err)
	}
	if out.Attributes["Name"].(*types.AttributeValueMemberS).Value != "Bob" {
		t.Fatal("Name mismatch")
	}
	if out.Attributes["Age"].(*types.AttributeValueMemberN).Value != "30" {
		t.Fatal("Age mismatch")
	}
	if out.Attributes["Email"].(*types.AttributeValueMemberS).Value != "bob@test.com" {
		t.Fatal("Email mismatch")
	}
}

func TestUpdateItemADD(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_add_num"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Score": &types.AttributeValueMemberN{Value: "100"},
		},
	})

	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression: aws.String("ADD Score :inc"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc": &types.AttributeValueMemberN{Value: "50"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("ADD: %v", err)
	}
	if out.Attributes["Score"].(*types.AttributeValueMemberN).Value != "150" {
		t.Fatalf("expected 150, got %s", out.Attributes["Score"].(*types.AttributeValueMemberN).Value)
	}
}

func TestUpdateItemREMOVE(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_remove_attr"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"}, "Temp": &types.AttributeValueMemberS{Value: "x"},
		},
	})

	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
		UpdateExpression: aws.String("REMOVE Temp"),
		ReturnValues:     types.ReturnValueAllNew,
	})
	if err != nil {
		t.Fatalf("REMOVE: %v", err)
	}
	if _, ok := out.Attributes["Temp"]; ok {
		t.Fatal("Temp should be removed")
	}
	if out.Attributes["Name"].(*types.AttributeValueMemberS).Value != "Alice" {
		t.Fatal("Name should still exist")
	}
}

func TestConditionExpressionAttributeExists(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_cond_attr_exists"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Insert-if-not-exists pattern
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// Should fail on duplicate
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err == nil {
		t.Fatal("expected ConditionalCheckFailedException")
	}

	// Update-only-if-exists pattern
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "updated"},
		},
		ConditionExpression: aws.String("attribute_exists(PK)"),
	})
	if err != nil {
		t.Fatalf("update-if-exists: %v", err)
	}
}

func TestGSIQueryWithFilter(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_gsi_filter"
	defer deleteTestTable(t, client, tableName)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("GSI1PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("GSI1SK"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String("GSI1"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("GSI1SK"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for i := 1; i <= 6; i++ {
		status := "active"
		if i%2 == 0 {
			status = "inactive"
		}
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("u%d", i)}, "SK": &types.AttributeValueMemberS{Value: "profile"},
				"GSI1PK": &types.AttributeValueMemberS{Value: "ORG#1"}, "GSI1SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("u%d", i)},
				"Status": &types.AttributeValueMemberS{Value: status},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(tableName), IndexName: aws.String("GSI1"),
		KeyConditionExpression:   aws.String("GSI1PK = :pk"),
		FilterExpression:         aws.String("#s = :active"),
		ExpressionAttributeNames: map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "ORG#1"}, ":active": &types.AttributeValueMemberS{Value: "active"},
		},
	})
	if err != nil {
		t.Fatalf("GSI query with filter: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3, got %d", out.Count)
	}
}

func TestScanMultiplePartitions(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_multi_pk"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		for j := 1; j <= 3; j++ {
			_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk#%d", i)},
					"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("sk#%d", j)},
				},
			})
		}
	}

	out, _ := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableName)})
	if out.Count != 15 {
		t.Fatalf("expected 15, got %d", out.Count)
	}
}

func TestScanPaginationComplete(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_page_full"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 7; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("s%d", i)},
			},
		})
	}

	var allItems []map[string]types.AttributeValue
	var lastKey map[string]types.AttributeValue
	pages := 0
	for {
		pages++
		out, _ := client.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(tableName), Limit: aws.Int32(3), ExclusiveStartKey: lastKey,
		})
		allItems = append(allItems, out.Items...)
		lastKey = out.LastEvaluatedKey
		if lastKey == nil {
			break
		}
		if pages > 10 {
			t.Fatal("too many pages")
		}
	}
	if len(allItems) != 7 {
		t.Fatalf("expected 7, got %d", len(allItems))
	}
	if pages != 3 {
		t.Fatalf("expected 3 pages, got %d", pages)
	}
}

func TestTransactWriteConditionCheck(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_condcheck"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Status": &types.AttributeValueMemberS{Value: "active"},
		},
	})

	// Should pass — Status=active
	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{ConditionCheck: &types.ConditionCheck{
				TableName: aws.String(tableName),
				Key:       map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"}},
				ConditionExpression: aws.String("#s = :v"),
				ExpressionAttributeNames: map[string]string{"#s": "Status"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: "active"}},
			}},
			{Put: &types.Put{
				TableName: aws.String(tableName),
				Item:      map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "p2"}, "SK": &types.AttributeValueMemberS{Value: "s1"}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("TransactWrite pass: %v", err)
	}

	// Change status then retry — should fail
	_, _ = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key:       map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"}},
		UpdateExpression: aws.String("SET #s = :v"),
		ExpressionAttributeNames: map[string]string{"#s": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: "inactive"}},
	})

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{ConditionCheck: &types.ConditionCheck{
				TableName: aws.String(tableName),
				Key:       map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"}},
				ConditionExpression: aws.String("#s = :v"),
				ExpressionAttributeNames: map[string]string{"#s": "Status"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: "active"}},
			}},
			{Put: &types.Put{
				TableName: aws.String(tableName),
				Item:      map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "p3"}, "SK": &types.AttributeValueMemberS{Value: "s1"}},
			}},
		},
	})
	if err == nil {
		t.Fatal("expected TransactionCanceledException")
	}
}

func TestGetItemWithProjection(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_getitem_proj"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"}, "Email": &types.AttributeValueMemberS{Value: "a@t.com"},
			"Age": &types.AttributeValueMemberN{Value: "30"},
		},
	})

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
		ProjectionExpression: aws.String("Name, Age"),
	})
	if err != nil {
		t.Fatalf("GetItem projection: %v", err)
	}
	if _, ok := out.Item["Name"]; !ok {
		t.Fatal("Name should be projected")
	}
	if _, ok := out.Item["Email"]; ok {
		t.Fatal("Email should NOT be projected")
	}
	if _, ok := out.Item["PK"]; !ok {
		t.Fatal("PK key always included")
	}
}

func TestMultipleGSIsOnSameTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_multi_gsi"
	defer deleteTestTable(t, client, tableName)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("Email"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("City"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{IndexName: aws.String("EmailIndex"), KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("Email"), KeyType: types.KeyTypeHash}}, Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll}},
			{IndexName: aws.String("CityIndex"), KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("City"), KeyType: types.KeyTypeHash}}, Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll}},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for _, it := range []map[string]types.AttributeValue{
		{"PK": &types.AttributeValueMemberS{Value: "u1"}, "SK": &types.AttributeValueMemberS{Value: "p"}, "Email": &types.AttributeValueMemberS{Value: "a@t.com"}, "City": &types.AttributeValueMemberS{Value: "NYC"}},
		{"PK": &types.AttributeValueMemberS{Value: "u2"}, "SK": &types.AttributeValueMemberS{Value: "p"}, "Email": &types.AttributeValueMemberS{Value: "b@t.com"}, "City": &types.AttributeValueMemberS{Value: "NYC"}},
		{"PK": &types.AttributeValueMemberS{Value: "u3"}, "SK": &types.AttributeValueMemberS{Value: "p"}, "Email": &types.AttributeValueMemberS{Value: "c@t.com"}, "City": &types.AttributeValueMemberS{Value: "LA"}},
	} {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(tableName), Item: it})
	}

	outEmail, _ := client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(tableName), IndexName: aws.String("EmailIndex"),
		KeyConditionExpression: aws.String("Email = :e"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":e": &types.AttributeValueMemberS{Value: "a@t.com"}},
	})
	if outEmail.Count != 1 {
		t.Fatalf("EmailIndex: expected 1, got %d", outEmail.Count)
	}

	outCity, _ := client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(tableName), IndexName: aws.String("CityIndex"),
		KeyConditionExpression: aws.String("City = :c"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":c": &types.AttributeValueMemberS{Value: "NYC"}},
	})
	if outCity.Count != 2 {
		t.Fatalf("CityIndex: expected 2, got %d", outCity.Count)
	}
}

func TestTransactGetItemsMixed(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_get_mixed"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})

	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{TableName: aws.String(tableName), Key: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"}}}},
			{Get: &types.Get{TableName: aws.String(tableName), Key: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "x"}, "SK": &types.AttributeValueMemberS{Value: "x"}}}},
		},
	})
	if err != nil {
		t.Fatalf("TransactGetItems: %v", err)
	}
	if out.Responses[0].Item == nil {
		t.Fatal("first should have item")
	}
	if len(out.Responses[1].Item) != 0 {
		t.Fatal("second should be empty")
	}
}

func TestQuerySKGreaterThanDescending(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_gt_desc"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("sk#%d", i)},
			},
		})
	}

	out, _ := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND SK > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"}, ":sk": &types.AttributeValueMemberS{Value: "sk#2"},
		},
		ScanIndexForward: aws.Bool(false),
	})
	if out.Count != 3 {
		t.Fatalf("expected 3, got %d", out.Count)
	}
	if out.Items[0]["SK"].(*types.AttributeValueMemberS).Value != "sk#5" {
		t.Fatal("first should be sk#5 (descending)")
	}
}

func TestUpdateItemUpdatedOldReturnValues(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_updated_old"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"}, "Score": &types.AttributeValueMemberN{Value: "100"},
		},
	})

	out, _ := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"}},
		UpdateExpression:         aws.String("SET #n = :name"),
		ExpressionAttributeNames: map[string]string{"#n": "Name"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":name": &types.AttributeValueMemberS{Value: "Bob"}},
		ReturnValues: types.ReturnValueUpdatedOld,
	})
	if out.Attributes["Name"].(*types.AttributeValueMemberS).Value != "Alice" {
		t.Fatal("expected old Name=Alice")
	}
	if _, ok := out.Attributes["Score"]; ok {
		t.Fatal("Score not updated, should not be in UPDATED_OLD")
	}
}

func TestUpdateItemUpdatedNewReturnValues(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_updated_new"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"}, "Score": &types.AttributeValueMemberN{Value: "100"},
		},
	})

	out, _ := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"}},
		UpdateExpression:         aws.String("SET #n = :name"),
		ExpressionAttributeNames: map[string]string{"#n": "Name"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":name": &types.AttributeValueMemberS{Value: "Bob"}},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	if out.Attributes["Name"].(*types.AttributeValueMemberS).Value != "Bob" {
		t.Fatal("expected new Name=Bob")
	}
	if _, ok := out.Attributes["Score"]; ok {
		t.Fatal("Score not updated, should not be in UPDATED_NEW")
	}
}

func TestCreateTableWithLSIAndQuery(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_lsi_query2"
	defer deleteTestTable(t, client, tableName)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("CreatedAt"), AttributeType: types.ScalarAttributeTypeS},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{{
			IndexName: aws.String("CreatedAtIndex"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("CreatedAt"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for _, it := range []map[string]types.AttributeValue{
		{"PK": &types.AttributeValueMemberS{Value: "u1"}, "SK": &types.AttributeValueMemberS{Value: "p1"}, "CreatedAt": &types.AttributeValueMemberS{Value: "2024-01-15"}},
		{"PK": &types.AttributeValueMemberS{Value: "u1"}, "SK": &types.AttributeValueMemberS{Value: "p2"}, "CreatedAt": &types.AttributeValueMemberS{Value: "2024-03-20"}},
		{"PK": &types.AttributeValueMemberS{Value: "u1"}, "SK": &types.AttributeValueMemberS{Value: "p3"}, "CreatedAt": &types.AttributeValueMemberS{Value: "2024-02-10"}},
	} {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(tableName), Item: it})
	}

	out, _ := client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(tableName), IndexName: aws.String("CreatedAtIndex"),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "u1"}},
		ScanIndexForward: aws.Bool(true),
	})
	if out.Count != 3 {
		t.Fatalf("expected 3, got %d", out.Count)
	}
	dates := make([]string, 3)
	for i, item := range out.Items {
		dates[i] = item["CreatedAt"].(*types.AttributeValueMemberS).Value
	}
	if !sort.StringsAreSorted(dates) {
		t.Fatalf("expected sorted, got %v", dates)
	}
}

func TestScanWithFilterOnNumber(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_filter_num"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("p%d", i)}, "SK": &types.AttributeValueMemberS{Value: "s1"},
				"Score": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i*10)},
			},
		})
	}

	out, _ := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName), FilterExpression: aws.String("Score >= :min"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":min": &types.AttributeValueMemberN{Value: "30"}},
	})
	if out.Count != 3 {
		t.Fatalf("expected 3, got %d", out.Count)
	}
}

func TestAttributeValueMarshalerRoundTrip(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_marshaler_rt"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	type Record struct {
		PK       string            `dynamodbav:"PK"`
		SK       string            `dynamodbav:"SK"`
		Tags     []string          `dynamodbav:"Tags,stringset"`
		Scores   map[string]int    `dynamodbav:"Scores"`
		Metadata map[string]string `dynamodbav:"Metadata"`
		Active   bool              `dynamodbav:"Active"`
	}

	original := Record{
		PK: "user#1", SK: "profile", Tags: []string{"admin", "editor"},
		Scores: map[string]int{"math": 95, "science": 88},
		Metadata: map[string]string{"region": "us-east-1"}, Active: true,
	}

	item, _ := attributevalue.MarshalMap(original)
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(tableName), Item: item})

	out, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "user#1"}, "SK": &types.AttributeValueMemberS{Value: "profile"},
		},
	})

	var retrieved Record
	if err := attributevalue.UnmarshalMap(out.Item, &retrieved); err != nil {
		t.Fatalf("UnmarshalMap: %v", err)
	}
	if retrieved.PK != original.PK || retrieved.SK != original.SK {
		t.Fatal("keys mismatch")
	}
	if retrieved.Active != original.Active {
		t.Fatal("Active mismatch")
	}
	if len(retrieved.Tags) != len(original.Tags) {
		t.Fatal("Tags length mismatch")
	}
	if retrieved.Scores["math"] != 95 {
		t.Fatal("Scores mismatch")
	}
	if retrieved.Metadata["region"] != "us-east-1" {
		t.Fatal("Metadata mismatch")
	}
}

func TestScanWithFilterContainsOnList(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_contains_list"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "go"}, &types.AttributeValueMemberS{Value: "postgres"},
			}},
		},
	})
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p2"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "python"}, &types.AttributeValueMemberS{Value: "mysql"},
			}},
		},
	})

	out, _ := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName), FilterExpression: aws.String("contains(Tags, :tag)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":tag": &types.AttributeValueMemberS{Value: "go"}},
	})
	if out.Count != 1 {
		t.Fatalf("expected 1, got %d", out.Count)
	}
}

func TestQueryWithExpressionAttributeNames(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_expr_names"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
			"Status": &types.AttributeValueMemberS{Value: "active"},
		},
	})
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"}, "SK": &types.AttributeValueMemberS{Value: "s2"},
			"Status": &types.AttributeValueMemberS{Value: "inactive"},
		},
	})

	out, _ := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("#status = :val"),
		ExpressionAttributeNames: map[string]string{"#status": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"}, ":val": &types.AttributeValueMemberS{Value: "active"},
		},
	})
	if out.Count != 1 {
		t.Fatalf("expected 1, got %d", out.Count)
	}
}
