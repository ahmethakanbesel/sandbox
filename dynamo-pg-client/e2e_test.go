package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func getConnString() string {
	if cs := os.Getenv("PGDYNAMO_CONN"); cs != "" {
		return cs
	}
	return "postgres://dynamo:dynamo@localhost:5433/dynamo?sslmode=disable"
}

func setupClient(t *testing.T) (*dynamodb.Client, func()) {
	t.Helper()
	ctx := context.Background()
	client, cleanup, err := pgdynamo.NewWithCleanup(ctx, getConnString())
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client, cleanup
}

func createTestTable(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
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
		t.Fatalf("CreateTable failed: %v", err)
	}
}

func deleteTestTable(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
	ctx := context.Background()
	_, _ = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
}

// ===== TABLE OPERATIONS =====

func TestCreateAndDescribeTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_create_describe"
	defer deleteTestTable(t, client, tableName)

	out, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
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
		t.Fatalf("CreateTable: %v", err)
	}
	if *out.TableDescription.TableName != tableName {
		t.Fatalf("expected table name %s, got %s", tableName, *out.TableDescription.TableName)
	}
	if out.TableDescription.TableStatus != types.TableStatusActive {
		t.Fatalf("expected ACTIVE, got %s", out.TableDescription.TableStatus)
	}

	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if *desc.Table.TableName != tableName {
		t.Fatalf("describe: expected %s, got %s", tableName, *desc.Table.TableName)
	}
}

func TestCreateDuplicateTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_duplicate_table"
	defer deleteTestTable(t, client, tableName)

	createTestTable(t, client, tableName)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err == nil {
		t.Fatal("expected error for duplicate table, got nil")
	}
}

func TestListTables(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	tables := []string{"test_list_a", "test_list_b", "test_list_c"}
	for _, tn := range tables {
		defer deleteTestTable(t, client, tn)
		createTestTable(t, client, tn)
	}

	out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	found := 0
	for _, name := range out.TableNames {
		for _, tn := range tables {
			if name == tn {
				found++
			}
		}
	}
	if found != 3 {
		t.Fatalf("expected 3 tables, found %d in %v", found, out.TableNames)
	}
}

func TestDeleteTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_delete_table"

	createTestTable(t, client, tableName)

	out, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("DeleteTable: %v", err)
	}
	if out.TableDescription.TableStatus != types.TableStatusDeleting {
		t.Fatalf("expected DELETING status, got %s", out.TableDescription.TableStatus)
	}

	// Verify table is gone
	_, err = client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		t.Fatal("expected error describing deleted table")
	}
}

// ===== ITEM CRUD OPERATIONS =====

func TestPutAndGetItem(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_put_get"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

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
		t.Fatalf("PutItem: %v", err)
	}

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "user#1"},
			"SK": &types.AttributeValueMemberS{Value: "profile"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if out.Item == nil {
		t.Fatal("expected item, got nil")
	}
	name := out.Item["Name"].(*types.AttributeValueMemberS).Value
	if name != "Alice" {
		t.Fatalf("expected Alice, got %s", name)
	}
	age := out.Item["Age"].(*types.AttributeValueMemberN).Value
	if age != "30" {
		t.Fatalf("expected 30, got %s", age)
	}
}

func TestGetItemNotFound(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_get_missing"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "nonexistent"},
			"SK": &types.AttributeValueMemberS{Value: "none"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if out.Item != nil {
		t.Fatalf("expected nil item, got %v", out.Item)
	}
}

func TestPutItemReturnAllOld(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_put_return_old"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// First put
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "key1"},
			"SK":   &types.AttributeValueMemberS{Value: "sort1"},
			"Data": &types.AttributeValueMemberS{Value: "original"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	// Overwrite with ALL_OLD
	out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:    aws.String(tableName),
		ReturnValues: types.ReturnValueAllOld,
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "key1"},
			"SK":   &types.AttributeValueMemberS{Value: "sort1"},
			"Data": &types.AttributeValueMemberS{Value: "updated"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem ALL_OLD: %v", err)
	}
	if out.Attributes == nil {
		t.Fatal("expected old attributes, got nil")
	}
	oldData := out.Attributes["Data"].(*types.AttributeValueMemberS).Value
	if oldData != "original" {
		t.Fatalf("expected original, got %s", oldData)
	}
}

func TestDeleteItem(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_delete_item"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "del1"},
			"SK":   &types.AttributeValueMemberS{Value: "sort"},
			"Data": &types.AttributeValueMemberS{Value: "to-delete"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	delOut, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:    aws.String(tableName),
		ReturnValues: types.ReturnValueAllOld,
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "del1"},
			"SK": &types.AttributeValueMemberS{Value: "sort"},
		},
	})
	if err != nil {
		t.Fatalf("DeleteItem: %v", err)
	}
	if delOut.Attributes == nil {
		t.Fatal("expected returned attributes")
	}

	// Verify gone
	get, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "del1"},
			"SK": &types.AttributeValueMemberS{Value: "sort"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem after delete: %v", err)
	}
	if get.Item != nil {
		t.Fatal("item should be deleted")
	}
}

// ===== UPDATE ITEM =====

func TestUpdateItemSetAndRemove(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_item"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Put initial item
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":      &types.AttributeValueMemberS{Value: "u1"},
			"SK":      &types.AttributeValueMemberS{Value: "s1"},
			"Name":    &types.AttributeValueMemberS{Value: "Alice"},
			"Counter": &types.AttributeValueMemberN{Value: "10"},
			"Temp":    &types.AttributeValueMemberS{Value: "remove-me"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	// Update: SET Name, increment Counter, REMOVE Temp
	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		ReturnValues:     types.ReturnValueAllNew,
		UpdateExpression: aws.String("SET #n = :name, Counter = Counter + :inc REMOVE Temp"),
		ExpressionAttributeNames: map[string]string{
			"#n": "Name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":name": &types.AttributeValueMemberS{Value: "Bob"},
			":inc":  &types.AttributeValueMemberN{Value: "5"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "u1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}

	name := out.Attributes["Name"].(*types.AttributeValueMemberS).Value
	if name != "Bob" {
		t.Fatalf("expected Bob, got %s", name)
	}
	counter := out.Attributes["Counter"].(*types.AttributeValueMemberN).Value
	if counter != "15" {
		t.Fatalf("expected 15, got %s", counter)
	}
	if _, exists := out.Attributes["Temp"]; exists {
		t.Fatal("Temp should have been removed")
	}
}

func TestUpdateItemIfNotExists(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_ifnotexists"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Update a non-existent item using if_not_exists
	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		ReturnValues:     types.ReturnValueAllNew,
		UpdateExpression: aws.String("SET #c = if_not_exists(#c, :zero) + :inc"),
		ExpressionAttributeNames: map[string]string{
			"#c": "Counter",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":zero": &types.AttributeValueMemberN{Value: "0"},
			":inc":  &types.AttributeValueMemberN{Value: "1"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "new-item"},
			"SK": &types.AttributeValueMemberS{Value: "counter"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}

	counter := out.Attributes["Counter"].(*types.AttributeValueMemberN).Value
	if counter != "1" {
		t.Fatalf("expected 1, got %s", counter)
	}
}

func TestUpdateItemReturnValues(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_return_values"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "rv1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Old"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	// UPDATED_OLD
	oldOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		ReturnValues:     types.ReturnValueUpdatedOld,
		UpdateExpression: aws.String("SET #n = :name"),
		ExpressionAttributeNames: map[string]string{
			"#n": "Name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":name": &types.AttributeValueMemberS{Value: "New"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "rv1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem UPDATED_OLD: %v", err)
	}
	if oldOut.Attributes["Name"].(*types.AttributeValueMemberS).Value != "Old" {
		t.Fatalf("expected Old for UPDATED_OLD")
	}

	// UPDATED_NEW
	newOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		ReturnValues:     types.ReturnValueUpdatedNew,
		UpdateExpression: aws.String("SET #n = :name"),
		ExpressionAttributeNames: map[string]string{
			"#n": "Name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":name": &types.AttributeValueMemberS{Value: "Newer"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "rv1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem UPDATED_NEW: %v", err)
	}
	if newOut.Attributes["Name"].(*types.AttributeValueMemberS).Value != "Newer" {
		t.Fatalf("expected Newer for UPDATED_NEW")
	}
}

// ===== CONDITION EXPRESSIONS =====

func TestPutItemConditionExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_put_condition"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Put with condition attribute_not_exists — should succeed on new item
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "cond1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Data": &types.AttributeValueMemberS{Value: "first"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem with condition (new): %v", err)
	}

	// Same put again should fail — item already exists
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "cond1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Data": &types.AttributeValueMemberS{Value: "second"},
		},
	})
	if err == nil {
		t.Fatal("expected ConditionalCheckFailedException, got nil")
	}
}

func TestDeleteItemConditionExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_delete_condition"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":     &types.AttributeValueMemberS{Value: "dc1"},
			"SK":     &types.AttributeValueMemberS{Value: "s1"},
			"Status": &types.AttributeValueMemberS{Value: "active"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	// Delete with wrong condition
	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("#s = :val"),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "inactive"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "dc1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err == nil {
		t.Fatal("expected ConditionalCheckFailedException")
	}

	// Delete with correct condition
	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("#s = :val"),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "active"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "dc1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("DeleteItem with correct condition: %v", err)
	}
}

func TestUpdateItemConditionExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_condition"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":      &types.AttributeValueMemberS{Value: "uc1"},
			"SK":      &types.AttributeValueMemberS{Value: "s1"},
			"Version": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	// Update with wrong version (should fail)
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("Version = :v"),
		UpdateExpression:    aws.String("SET Version = :newv"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v":    &types.AttributeValueMemberN{Value: "99"},
			":newv": &types.AttributeValueMemberN{Value: "2"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "uc1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err == nil {
		t.Fatal("expected ConditionalCheckFailedException")
	}

	// Update with correct version (should succeed)
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(tableName),
		ConditionExpression: aws.String("Version = :v"),
		UpdateExpression:    aws.String("SET Version = :newv"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v":    &types.AttributeValueMemberN{Value: "1"},
			":newv": &types.AttributeValueMemberN{Value: "2"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "uc1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem with correct condition: %v", err)
	}
}

// ===== QUERY =====

func TestQueryBasic(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_basic"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Insert items
	for i := 1; i <= 5; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "user#1"},
				"SK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("order#%03d", i)},
				"Data": &types.AttributeValueMemberS{Value: fmt.Sprintf("order-%d", i)},
			},
		})
		if err != nil {
			t.Fatalf("PutItem: %v", err)
		}
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "user#1"},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if out.Count != 5 {
		t.Fatalf("expected 5 items, got %d", out.Count)
	}
}

func TestQueryWithSKCondition(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_sk"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("sk#%03d", i)},
			},
		})
	}

	// begins_with
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "p1"},
			":prefix": &types.AttributeValueMemberS{Value: "sk#00"},
		},
	})
	if err != nil {
		t.Fatalf("Query begins_with: %v", err)
	}
	if out.Count != 5 {
		t.Fatalf("expected 5 items with begins_with sk#00, got %d", out.Count)
	}

	// BETWEEN
	out, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND SK BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":lo": &types.AttributeValueMemberS{Value: "sk#002"},
			":hi": &types.AttributeValueMemberS{Value: "sk#004"},
		},
	})
	if err != nil {
		t.Fatalf("Query BETWEEN: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items in BETWEEN range, got %d", out.Count)
	}
}

func TestQueryWithFilterExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_filter"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":     &types.AttributeValueMemberS{Value: "p1"},
				"SK":     &types.AttributeValueMemberS{Value: fmt.Sprintf("s%d", i)},
				"Status": &types.AttributeValueMemberS{Value: map[bool]string{true: "active", false: "inactive"}[i%2 == 0]},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("#s = :status"),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "p1"},
			":status": &types.AttributeValueMemberS{Value: "active"},
		},
	})
	if err != nil {
		t.Fatalf("Query with filter: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("expected 2 active items, got %d", out.Count)
	}
}

func TestQueryScanForward(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_order"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for _, sk := range []string{"a", "b", "c"} {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: sk},
			},
		})
	}

	// Descending order
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		ScanIndexForward:       aws.Bool(false),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
	})
	if err != nil {
		t.Fatalf("Query DESC: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items, got %d", out.Count)
	}
	firstSK := out.Items[0]["SK"].(*types.AttributeValueMemberS).Value
	if firstSK != "c" {
		t.Fatalf("expected first SK to be 'c' (descending), got %s", firstSK)
	}
}

func TestQueryPagination(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_pagination"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 10; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("sk#%03d", i)},
			},
		})
	}

	var allItems []map[string]types.AttributeValue
	var startKey map[string]types.AttributeValue
	pages := 0

	for {
		out, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("PK = :pk"),
			Limit:                 aws.Int32(3),
			ExclusiveStartKey:     startKey,
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "p1"},
			},
		})
		if err != nil {
			t.Fatalf("Query page %d: %v", pages, err)
		}
		pages++
		allItems = append(allItems, out.Items...)
		startKey = out.LastEvaluatedKey
		if startKey == nil {
			break
		}
	}

	if len(allItems) != 10 {
		t.Fatalf("expected 10 total items across pages, got %d", len(allItems))
	}
	if pages < 3 {
		t.Fatalf("expected at least 3 pages with limit 3, got %d", pages)
	}
}

func TestQuerySelectCount(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_count"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("s%d", i)},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		Select:                 types.SelectCount,
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
	})
	if err != nil {
		t.Fatalf("Query COUNT: %v", err)
	}
	if out.Count != 5 {
		t.Fatalf("expected count 5, got %d", out.Count)
	}
}

// ===== SCAN =====

func TestScanBasic(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_basic"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("pk%d", i)},
				"SK":   &types.AttributeValueMemberS{Value: "s1"},
				"Data": &types.AttributeValueMemberS{Value: fmt.Sprintf("data-%d", i)},
			},
		})
	}

	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if out.Count != 5 {
		t.Fatalf("expected 5 items, got %d", out.Count)
	}
}

func TestScanWithFilter(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_filter"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 6; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":  &types.AttributeValueMemberS{Value: fmt.Sprintf("pk%d", i)},
				"SK":  &types.AttributeValueMemberS{Value: "s1"},
				"Age": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i*10)},
			},
		})
	}

	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		FilterExpression: aws.String("Age >= :minAge"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":minAge": &types.AttributeValueMemberN{Value: "40"},
		},
	})
	if err != nil {
		t.Fatalf("Scan with filter: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items with age >= 40, got %d", out.Count)
	}
}

func TestScanPagination(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_pagination"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 8; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk%03d", i)},
				"SK": &types.AttributeValueMemberS{Value: "s1"},
			},
		})
	}

	var total int32
	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			Limit:             aws.Int32(3),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		total += out.Count
		startKey = out.LastEvaluatedKey
		if startKey == nil {
			break
		}
	}
	if total != 8 {
		t.Fatalf("expected 8 total items, got %d", total)
	}
}

// ===== PROJECTION EXPRESSION =====

func TestProjectionExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_projection"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "p1"},
			"SK":    &types.AttributeValueMemberS{Value: "s1"},
			"Name":  &types.AttributeValueMemberS{Value: "Alice"},
			"Email": &types.AttributeValueMemberS{Value: "alice@example.com"},
			"Phone": &types.AttributeValueMemberS{Value: "555-1234"},
		},
	})

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(tableName),
		ProjectionExpression: aws.String("#n, Email"),
		ExpressionAttributeNames: map[string]string{
			"#n": "Name",
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem with projection: %v", err)
	}
	if _, ok := out.Item["Name"]; !ok {
		t.Fatal("Name should be in projection")
	}
	if _, ok := out.Item["Email"]; !ok {
		t.Fatal("Email should be in projection")
	}
	if _, ok := out.Item["Phone"]; ok {
		t.Fatal("Phone should NOT be in projection")
	}
	// Keys always included
	if _, ok := out.Item["PK"]; !ok {
		t.Fatal("PK should always be included")
	}
}

// ===== BATCH OPERATIONS =====

func TestBatchWriteAndGetItem(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_batch_ops"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Batch write
	writeRequests := make([]types.WriteRequest, 5)
	for i := 0; i < 5; i++ {
		writeRequests[i] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"PK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("batch#%d", i)},
					"SK":   &types.AttributeValueMemberS{Value: "s1"},
					"Data": &types.AttributeValueMemberS{Value: fmt.Sprintf("val-%d", i)},
				},
			},
		}
	}

	bwOut, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeRequests,
		},
	})
	if err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}
	if len(bwOut.UnprocessedItems) != 0 {
		t.Fatalf("expected 0 unprocessed items, got %d", len(bwOut.UnprocessedItems))
	}

	// Batch get
	keys := make([]map[string]types.AttributeValue, 5)
	for i := 0; i < 5; i++ {
		keys[i] = map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("batch#%d", i)},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		}
	}

	bgOut, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {Keys: keys},
		},
	})
	if err != nil {
		t.Fatalf("BatchGetItem: %v", err)
	}
	items := bgOut.Responses[tableName]
	if len(items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(items))
	}
}

func TestBatchWriteDelete(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_batch_delete"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Put items first
	for i := 0; i < 3; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("bd#%d", i)},
				"SK": &types.AttributeValueMemberS{Value: "s1"},
			},
		})
	}

	// Batch delete
	_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: {
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "bd#0"},
					"SK": &types.AttributeValueMemberS{Value: "s1"},
				}}},
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "bd#1"},
					"SK": &types.AttributeValueMemberS{Value: "s1"},
				}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("BatchWriteItem delete: %v", err)
	}

	// Verify only bd#2 remains
	out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableName)})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 remaining item, got %d", out.Count)
	}
}

// ===== TRANSACTIONS =====

func TestTransactWriteItems(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_write"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"PK":   &types.AttributeValueMemberS{Value: "tw#1"},
						"SK":   &types.AttributeValueMemberS{Value: "s1"},
						"Data": &types.AttributeValueMemberS{Value: "txn-data-1"},
					},
				},
			},
			{
				Put: &types.Put{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"PK":   &types.AttributeValueMemberS{Value: "tw#2"},
						"SK":   &types.AttributeValueMemberS{Value: "s1"},
						"Data": &types.AttributeValueMemberS{Value: "txn-data-2"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("TransactWriteItems: %v", err)
	}

	// Verify both items exist
	for _, pk := range []string{"tw#1", "tw#2"} {
		get, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: pk},
				"SK": &types.AttributeValueMemberS{Value: "s1"},
			},
		})
		if err != nil {
			t.Fatalf("GetItem %s: %v", pk, err)
		}
		if get.Item == nil {
			t.Fatalf("expected item %s to exist", pk)
		}
	}
}

func TestTransactWriteConditionFail(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_write_fail"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Put an existing item
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "existing"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})

	// Transaction with a condition check that should fail
	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: "new-item"},
						"SK": &types.AttributeValueMemberS{Value: "s1"},
					},
				},
			},
			{
				ConditionCheck: &types.ConditionCheck{
					TableName:           aws.String(tableName),
					ConditionExpression: aws.String("attribute_not_exists(PK)"),
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: "existing"},
						"SK": &types.AttributeValueMemberS{Value: "s1"},
					},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected TransactionCanceledException")
	}

	// The new-item should NOT have been written (transaction rollback)
	get, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "new-item"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if get.Item != nil {
		t.Fatal("transaction should have rolled back, new-item should not exist")
	}
}

func TestTransactWriteWithUpdate(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_update"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Initial item
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":      &types.AttributeValueMemberS{Value: "counter"},
			"SK":      &types.AttributeValueMemberS{Value: "s1"},
			"Counter": &types.AttributeValueMemberN{Value: "10"},
		},
	})

	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName:        aws.String(tableName),
					UpdateExpression: aws.String("SET Counter = Counter + :inc"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":inc": &types.AttributeValueMemberN{Value: "5"},
					},
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: "counter"},
						"SK": &types.AttributeValueMemberS{Value: "s1"},
					},
				},
			},
			{
				Delete: &types.Delete{
					TableName: aws.String(tableName),
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: "nonexistent"},
						"SK": &types.AttributeValueMemberS{Value: "s1"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("TransactWriteItems: %v", err)
	}

	get, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "counter"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	counter := get.Item["Counter"].(*types.AttributeValueMemberN).Value
	if counter != "15" {
		t.Fatalf("expected 15, got %s", counter)
	}
}

func TestTransactGetItems(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_get"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 3; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: fmt.Sprintf("tg#%d", i)},
				"SK":   &types.AttributeValueMemberS{Value: "s1"},
				"Data": &types.AttributeValueMemberS{Value: fmt.Sprintf("data-%d", i)},
			},
		})
	}

	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: []types.TransactGetItem{
			{Get: &types.Get{
				TableName: aws.String(tableName),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "tg#1"},
					"SK": &types.AttributeValueMemberS{Value: "s1"},
				},
			}},
			{Get: &types.Get{
				TableName: aws.String(tableName),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "tg#2"},
					"SK": &types.AttributeValueMemberS{Value: "s1"},
				},
			}},
			{Get: &types.Get{
				TableName: aws.String(tableName),
				Key: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "tg#3"},
					"SK": &types.AttributeValueMemberS{Value: "s1"},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("TransactGetItems: %v", err)
	}
	if len(out.Responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(out.Responses))
	}
	for i, resp := range out.Responses {
		if resp.Item == nil {
			t.Fatalf("response %d: expected item", i)
		}
	}
}

// ===== GSI SUPPORT =====

func TestGSIQueryBasic(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_gsi_query"
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
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("GSI1SK"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable with GSI: %v", err)
	}

	// Insert items with GSI attributes
	items := []map[string]types.AttributeValue{
		{
			"PK": &types.AttributeValueMemberS{Value: "user#1"},
			"SK": &types.AttributeValueMemberS{Value: "profile"},
			"GSI1PK": &types.AttributeValueMemberS{Value: "org#acme"},
			"GSI1SK": &types.AttributeValueMemberS{Value: "user#1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"},
		},
		{
			"PK": &types.AttributeValueMemberS{Value: "user#2"},
			"SK": &types.AttributeValueMemberS{Value: "profile"},
			"GSI1PK": &types.AttributeValueMemberS{Value: "org#acme"},
			"GSI1SK": &types.AttributeValueMemberS{Value: "user#2"},
			"Name": &types.AttributeValueMemberS{Value: "Bob"},
		},
		{
			"PK": &types.AttributeValueMemberS{Value: "user#3"},
			"SK": &types.AttributeValueMemberS{Value: "profile"},
			"GSI1PK": &types.AttributeValueMemberS{Value: "org#other"},
			"GSI1SK": &types.AttributeValueMemberS{Value: "user#3"},
			"Name": &types.AttributeValueMemberS{Value: "Carol"},
		},
	}

	for _, item := range items {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		if err != nil {
			t.Fatalf("PutItem: %v", err)
		}
	}

	// Query the GSI
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "org#acme"},
		},
	})
	if err != nil {
		t.Fatalf("Query GSI: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("expected 2 items from GSI query, got %d", out.Count)
	}

	// Verify names
	names := make([]string, len(out.Items))
	for i, item := range out.Items {
		names[i] = item["Name"].(*types.AttributeValueMemberS).Value
	}
	sort.Strings(names)
	if names[0] != "Alice" || names[1] != "Bob" {
		t.Fatalf("expected [Alice, Bob], got %v", names)
	}
}

func TestGSIDescribeTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_gsi_describe"
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
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("MyGSI"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable with GSI: %v", err)
	}

	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if len(desc.Table.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("expected 1 GSI in describe, got %d", len(desc.Table.GlobalSecondaryIndexes))
	}
	gsi := desc.Table.GlobalSecondaryIndexes[0]
	if *gsi.IndexName != "MyGSI" {
		t.Fatalf("expected GSI name MyGSI, got %s", *gsi.IndexName)
	}
}

// ===== COMPLEX EXPRESSION TESTS =====

func TestFilterContains(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_filter_contains"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "p1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Tags": &types.AttributeValueMemberS{Value: "go,python,rust"},
		},
	})
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "p1"},
			"SK":   &types.AttributeValueMemberS{Value: "s2"},
			"Tags": &types.AttributeValueMemberS{Value: "java,c++"},
		},
	})

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("contains(Tags, :lang)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":   &types.AttributeValueMemberS{Value: "p1"},
			":lang": &types.AttributeValueMemberS{Value: "python"},
		},
	})
	if err != nil {
		t.Fatalf("Query with contains: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item with python, got %d", out.Count)
	}
}

func TestFilterAndOr(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_filter_andor"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":     &types.AttributeValueMemberS{Value: "p1"},
				"SK":     &types.AttributeValueMemberS{Value: fmt.Sprintf("s%d", i)},
				"Score":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i*10)},
				"Active": &types.AttributeValueMemberBOOL{Value: i%2 == 0},
			},
		})
	}

	// Score > 20 AND Active = true -> items 4 (40, true)
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("Score > :score AND Active = :active"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "p1"},
			":score":  &types.AttributeValueMemberN{Value: "20"},
			":active": &types.AttributeValueMemberBOOL{Value: true},
		},
	})
	if err != nil {
		t.Fatalf("Query with AND: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item (score>20 AND active), got %d", out.Count)
	}
}

// ===== NUMERIC SORT KEY =====

func TestNumericSortKey(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_numeric_sk"
	defer deleteTestTable(t, client, tableName)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeN},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Insert items with numeric SK
	for _, sk := range []string{"1", "2", "10", "20", "100"} {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberN{Value: sk},
			},
		})
	}

	// Query SK > 5 should return 10, 20, 100
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND SK > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":sk": &types.AttributeValueMemberN{Value: "5"},
		},
	})
	if err != nil {
		t.Fatalf("Query numeric SK: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items > 5, got %d", out.Count)
	}
}

// ===== ADD/DELETE SET OPERATIONS =====

func TestUpdateItemAddToSet(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_add_set"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "p1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Tags": &types.AttributeValueMemberSS{Value: []string{"go", "rust"}},
		},
	})

	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		UpdateExpression: aws.String("ADD Tags :newTags"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newTags": &types.AttributeValueMemberSS{Value: []string{"python", "go"}},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem ADD: %v", err)
	}

	get, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})

	tags := get.Item["Tags"].(*types.AttributeValueMemberSS).Value
	sort.Strings(tags)
	if len(tags) != 3 || tags[0] != "go" || tags[1] != "python" || tags[2] != "rust" {
		t.Fatalf("expected [go, python, rust], got %v", tags)
	}
}

// ===== ATTRIBUTEVALUE MARSHALING =====

func TestAttributeValueMarshalUnmarshal(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_av_types"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	type TestItem struct {
		PK     string            `dynamodbav:"PK"`
		SK     string            `dynamodbav:"SK"`
		Name   string            `dynamodbav:"Name"`
		Age    int               `dynamodbav:"Age"`
		Active bool              `dynamodbav:"Active"`
		Tags   []string          `dynamodbav:"Tags"`
		Meta   map[string]string `dynamodbav:"Meta"`
	}

	item := TestItem{
		PK: "marshal#1", SK: "s1",
		Name: "Alice", Age: 30, Active: true,
		Tags: []string{"a", "b"},
		Meta: map[string]string{"role": "admin"},
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		t.Fatalf("MarshalMap: %v", err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      av,
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	get, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "marshal#1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}

	var result TestItem
	err = attributevalue.UnmarshalMap(get.Item, &result)
	if err != nil {
		t.Fatalf("UnmarshalMap: %v", err)
	}
	if result.Name != "Alice" {
		t.Fatalf("expected Alice, got %s", result.Name)
	}
	if result.Age != 30 {
		t.Fatalf("expected 30, got %d", result.Age)
	}
	if !result.Active {
		t.Fatal("expected Active=true")
	}
	if len(result.Tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(result.Tags))
	}
}

// ===== STUB OPERATIONS =====

func TestDescribeTimeToLive(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_ttl"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	out, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("DescribeTimeToLive: %v", err)
	}
	if out.TimeToLiveDescription.TimeToLiveStatus != types.TimeToLiveStatusDisabled {
		t.Fatalf("expected DISABLED, got %s", out.TimeToLiveDescription.TimeToLiveStatus)
	}
}

func TestDescribeEndpoints(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	out, err := client.DescribeEndpoints(ctx, &dynamodb.DescribeEndpointsInput{})
	if err != nil {
		t.Fatalf("DescribeEndpoints: %v", err)
	}
	if len(out.Endpoints) == 0 {
		t.Fatal("expected at least 1 endpoint")
	}
}

func TestListTagsOfResource(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	out, err := client.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:local:000000000000:table/test"),
	})
	if err != nil {
		t.Fatalf("ListTagsOfResource: %v", err)
	}
	if out.Tags == nil {
		t.Fatal("expected non-nil tags slice")
	}
}

func TestTagAndUntagResource(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.TagResource(ctx, &dynamodb.TagResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:local:000000000000:table/test"),
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
		},
	})
	if err != nil {
		t.Fatalf("TagResource: %v", err)
	}

	_, err = client.UntagResource(ctx, &dynamodb.UntagResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:local:000000000000:table/test"),
		TagKeys:     []string{"env"},
	})
	if err != nil {
		t.Fatalf("UntagResource: %v", err)
	}
}

func TestDescribeLimits(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	out, err := client.DescribeLimits(ctx, &dynamodb.DescribeLimitsInput{})
	if err != nil {
		t.Fatalf("DescribeLimits: %v", err)
	}
	if *out.AccountMaxReadCapacityUnits == 0 {
		t.Fatal("expected non-zero account max read capacity")
	}
}

// ===== HASH-ONLY TABLE (no sort key) =====

func TestHashOnlyTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_hash_only"
	defer deleteTestTable(t, client, tableName)

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"ID":   &types.AttributeValueMemberS{Value: "item1"},
			"Data": &types.AttributeValueMemberS{Value: "hello"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	get, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{Value: "item1"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if get.Item["Data"].(*types.AttributeValueMemberS).Value != "hello" {
		t.Fatal("unexpected data")
	}

	// Scan hash-only table
	scan, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if scan.Count != 1 {
		t.Fatalf("expected 1, got %d", scan.Count)
	}
}

// ===== ADDITIONAL COVERAGE TESTS =====

func TestScanSelectCount(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_count"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("pk%d", i)},
				"SK": &types.AttributeValueMemberS{Value: "s1"},
			},
		})
	}

	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		Select:    types.SelectCount,
	})
	if err != nil {
		t.Fatalf("Scan COUNT: %v", err)
	}
	if out.Count != 5 {
		t.Fatalf("expected 5, got %d", out.Count)
	}
	if len(out.Items) != 0 {
		t.Fatalf("expected 0 items for COUNT select, got %d", len(out.Items))
	}
}

func TestScanWithProjection(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_projection"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "p1"},
			"SK":    &types.AttributeValueMemberS{Value: "s1"},
			"Name":  &types.AttributeValueMemberS{Value: "Alice"},
			"Email": &types.AttributeValueMemberS{Value: "alice@example.com"},
		},
	})

	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:            aws.String(tableName),
		ProjectionExpression: aws.String("Name"),
	})
	if err != nil {
		t.Fatalf("Scan with projection: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1, got %d", out.Count)
	}
	item := out.Items[0]
	if _, ok := item["Name"]; !ok {
		t.Fatal("Name should be in projection")
	}
	if _, ok := item["Email"]; ok {
		t.Fatal("Email should NOT be in projection")
	}
}

func TestUpdateItemReturnAllOld(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_all_old"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "u1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Old"},
		},
	})

	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		ReturnValues:     types.ReturnValueAllOld,
		UpdateExpression: aws.String("SET #n = :name"),
		ExpressionAttributeNames: map[string]string{
			"#n": "Name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":name": &types.AttributeValueMemberS{Value: "New"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "u1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem ALL_OLD: %v", err)
	}
	if out.Attributes == nil {
		t.Fatal("expected old attributes")
	}
	name := out.Attributes["Name"].(*types.AttributeValueMemberS).Value
	if name != "Old" {
		t.Fatalf("expected Old, got %s", name)
	}
}

func TestQuerySKLessThan(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_sk_lt"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("sk#%03d", i)},
			},
		})
	}

	// SK < sk#003 should return sk#001, sk#002
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND SK < :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":sk": &types.AttributeValueMemberS{Value: "sk#003"},
		},
	})
	if err != nil {
		t.Fatalf("Query SK <: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("expected 2 items with SK < sk#003, got %d", out.Count)
	}
}

func TestQuerySKLessThanOrEqual(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_sk_lte"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("sk#%03d", i)},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND SK <= :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":sk": &types.AttributeValueMemberS{Value: "sk#003"},
		},
	})
	if err != nil {
		t.Fatalf("Query SK <=: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items with SK <= sk#003, got %d", out.Count)
	}
}

func TestQuerySKGreaterThanOrEqual(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_sk_gte"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("sk#%03d", i)},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND SK >= :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":sk": &types.AttributeValueMemberS{Value: "sk#003"},
		},
	})
	if err != nil {
		t.Fatalf("Query SK >=: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items with SK >= sk#003, got %d", out.Count)
	}
}

func TestQuerySKEqual(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_sk_eq"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 3; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: "p1"},
				"SK": &types.AttributeValueMemberS{Value: fmt.Sprintf("sk#%03d", i)},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND SK = :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":sk": &types.AttributeValueMemberS{Value: "sk#002"},
		},
	})
	if err != nil {
		t.Fatalf("Query SK =: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item with SK = sk#002, got %d", out.Count)
	}
}

func TestUpdateTimeToLive(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_ttl"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	out, err := client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("TTL"),
			Enabled:       aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatalf("UpdateTimeToLive: %v", err)
	}
	if out.TimeToLiveSpecification == nil {
		t.Fatal("expected non-nil TimeToLiveSpecification")
	}
}

func TestDescribeContinuousBackups(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_continuous_backups"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	out, err := client.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("DescribeContinuousBackups: %v", err)
	}
	if out.ContinuousBackupsDescription == nil {
		t.Fatal("expected non-nil ContinuousBackupsDescription")
	}
}

func TestUpdateTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_table"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	out, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("UpdateTable: %v", err)
	}
	if *out.TableDescription.TableName != tableName {
		t.Fatalf("expected %s, got %s", tableName, *out.TableDescription.TableName)
	}
}

func TestUpdateItemDeleteFromSet(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_delete_set"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "p1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Tags": &types.AttributeValueMemberSS{Value: []string{"go", "rust", "python"}},
		},
	})

	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		UpdateExpression: aws.String("DELETE Tags :vals"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":vals": &types.AttributeValueMemberSS{Value: []string{"rust"}},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem DELETE: %v", err)
	}

	get, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	tags := get.Item["Tags"].(*types.AttributeValueMemberSS).Value
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags after DELETE, got %d: %v", len(tags), tags)
	}
}

func TestUpdateItemListAppend(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_list_append"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
			"Items": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "first"},
			}},
		},
	})

	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		ReturnValues:     types.ReturnValueAllNew,
		UpdateExpression: aws.String("SET Items = list_append(Items, :new)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":new": &types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "second"},
			}},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem list_append: %v", err)
	}
	items := out.Attributes["Items"].(*types.AttributeValueMemberL).Value
	if len(items) != 2 {
		t.Fatalf("expected 2 items after list_append, got %d", len(items))
	}
}

func TestFilterNOTExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_filter_not"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 4; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":     &types.AttributeValueMemberS{Value: "p1"},
				"SK":     &types.AttributeValueMemberS{Value: fmt.Sprintf("s%d", i)},
				"Status": &types.AttributeValueMemberS{Value: map[bool]string{true: "active", false: "inactive"}[i <= 2]},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("NOT #s = :status"),
		ExpressionAttributeNames: map[string]string{
			"#s": "Status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "p1"},
			":status": &types.AttributeValueMemberS{Value: "active"},
		},
	})
	if err != nil {
		t.Fatalf("Query with NOT: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("expected 2 inactive items, got %d", out.Count)
	}
}

func TestFilterBETWEENExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_filter_between"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for i := 1; i <= 5; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":    &types.AttributeValueMemberS{Value: "p1"},
				"SK":    &types.AttributeValueMemberS{Value: fmt.Sprintf("s%d", i)},
				"Score": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i*10)},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("Score BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":lo": &types.AttributeValueMemberN{Value: "20"},
			":hi": &types.AttributeValueMemberN{Value: "40"},
		},
	})
	if err != nil {
		t.Fatalf("Query with BETWEEN: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items with score 20-40, got %d", out.Count)
	}
}

func TestFilterINExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_filter_in"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	for _, name := range []string{"Alice", "Bob", "Carol", "Dave"} {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":   &types.AttributeValueMemberS{Value: "p1"},
				"SK":   &types.AttributeValueMemberS{Value: name},
				"Name": &types.AttributeValueMemberS{Value: name},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("Name IN (:v1, :v2)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":v1": &types.AttributeValueMemberS{Value: "Alice"},
			":v2": &types.AttributeValueMemberS{Value: "Carol"},
		},
	})
	if err != nil {
		t.Fatalf("Query with IN: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("expected 2 items, got %d", out.Count)
	}
}

func TestFilterSizeExpression(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_filter_size"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "p1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"},
		},
	})
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "p1"},
			"SK":   &types.AttributeValueMemberS{Value: "s2"},
			"Name": &types.AttributeValueMemberS{Value: "Bartholomew"},
		},
	})

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("size(#n) > :sz"),
		ExpressionAttributeNames: map[string]string{
			"#n": "Name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
			":sz": &types.AttributeValueMemberN{Value: "5"},
		},
	})
	if err != nil {
		t.Fatalf("Query with size: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item with name > 5 chars, got %d", out.Count)
	}
}

func TestGSIKeysOnlyProjection(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_gsi_keys_only"
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
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeKeysOnly,
				},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":     &types.AttributeValueMemberS{Value: "user#1"},
			"SK":     &types.AttributeValueMemberS{Value: "profile"},
			"GSI1PK": &types.AttributeValueMemberS{Value: "org#1"},
			"Name":   &types.AttributeValueMemberS{Value: "Alice"},
			"Email":  &types.AttributeValueMemberS{Value: "alice@example.com"},
		},
	})

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "org#1"},
		},
	})
	if err != nil {
		t.Fatalf("Query GSI KEYS_ONLY: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item, got %d", out.Count)
	}
	// KEYS_ONLY should only have PK, SK, GSI1PK
	item := out.Items[0]
	if _, ok := item["PK"]; !ok {
		t.Fatal("PK should be included")
	}
	if _, ok := item["GSI1PK"]; !ok {
		t.Fatal("GSI1PK should be included")
	}
}

func TestGSIIncludeProjection(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_gsi_include"
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
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{
					ProjectionType:   types.ProjectionTypeInclude,
					NonKeyAttributes: []string{"Name"},
				},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":     &types.AttributeValueMemberS{Value: "user#1"},
			"SK":     &types.AttributeValueMemberS{Value: "profile"},
			"GSI1PK": &types.AttributeValueMemberS{Value: "org#1"},
			"Name":   &types.AttributeValueMemberS{Value: "Alice"},
			"Email":  &types.AttributeValueMemberS{Value: "alice@example.com"},
		},
	})

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "org#1"},
		},
	})
	if err != nil {
		t.Fatalf("Query GSI INCLUDE: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item, got %d", out.Count)
	}
	item := out.Items[0]
	if _, ok := item["Name"]; !ok {
		t.Fatal("Name should be included in INCLUDE projection")
	}
}

func TestGSIScan(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_gsi_scan"
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
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for i := 1; i <= 3; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":     &types.AttributeValueMemberS{Value: fmt.Sprintf("user#%d", i)},
				"SK":     &types.AttributeValueMemberS{Value: "profile"},
				"GSI1PK": &types.AttributeValueMemberS{Value: "org#1"},
			},
		})
	}

	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		IndexName: aws.String("GSI1"),
	})
	if err != nil {
		t.Fatalf("Scan GSI: %v", err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items from GSI scan, got %d", out.Count)
	}
}

func TestLSIQuery(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_lsi_query"
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
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("LSI1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("CreatedAt"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("CreateTable with LSI: %v", err)
	}

	for i := 1; i <= 3; i++ {
		_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"PK":        &types.AttributeValueMemberS{Value: "user#1"},
				"SK":        &types.AttributeValueMemberS{Value: fmt.Sprintf("item#%d", i)},
				"CreatedAt": &types.AttributeValueMemberS{Value: fmt.Sprintf("2024-01-%02d", i)},
				"Data":      &types.AttributeValueMemberS{Value: fmt.Sprintf("data-%d", i)},
			},
		})
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("LSI1"),
		KeyConditionExpression: aws.String("PK = :pk AND CreatedAt >= :date"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":   &types.AttributeValueMemberS{Value: "user#1"},
			":date": &types.AttributeValueMemberS{Value: "2024-01-02"},
		},
	})
	if err != nil {
		t.Fatalf("Query LSI: %v", err)
	}
	if out.Count != 2 {
		t.Fatalf("expected 2 items from LSI query, got %d", out.Count)
	}
}

func TestListTablesPagination(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	tables := []string{"test_listp_a", "test_listp_b", "test_listp_c", "test_listp_d"}
	for _, tn := range tables {
		defer deleteTestTable(t, client, tn)
		createTestTable(t, client, tn)
	}

	// List with limit
	out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
		Limit: aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(out.TableNames) > 2 {
		t.Fatalf("expected at most 2 tables, got %d", len(out.TableNames))
	}

	// If there's a LastEvaluatedTableName, do another page
	if out.LastEvaluatedTableName != nil {
		out2, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
			Limit:                   aws.Int32(2),
			ExclusiveStartTableName: out.LastEvaluatedTableName,
		})
		if err != nil {
			t.Fatalf("ListTables page 2: %v", err)
		}
		if len(out2.TableNames) == 0 {
			t.Fatal("expected more tables on page 2")
		}
	}
}

func TestDeleteNonexistentTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String("nonexistent_table_xyz"),
	})
	if err == nil {
		t.Fatal("expected error for deleting nonexistent table")
	}
}

func TestDescribeNonexistentTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String("nonexistent_table_xyz"),
	})
	if err == nil {
		t.Fatal("expected error for describing nonexistent table")
	}
}

func TestQueryNonexistentTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("nonexistent_table_xyz"),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
	})
	if err == nil {
		t.Fatal("expected error for querying nonexistent table")
	}
}

func TestScanNonexistentTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String("nonexistent_table_xyz"),
	})
	if err == nil {
		t.Fatal("expected error for scanning nonexistent table")
	}
}

func TestPutItemNonexistentTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("nonexistent_table_xyz"),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
		},
	})
	if err == nil {
		t.Fatal("expected error for putting item in nonexistent table")
	}
}

func TestUpdateItemNonexistentTable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String("nonexistent_table_xyz"),
		UpdateExpression: aws.String("SET Name = :val"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "test"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err == nil {
		t.Fatal("expected error for updating item in nonexistent table")
	}
}

func TestQueryNonexistentIndex(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_bad_index"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("NonexistentIndex"),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
	})
	if err == nil {
		t.Fatal("expected error for querying nonexistent index")
	}
}

func TestScanNonexistentIndex(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_scan_bad_index"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		IndexName: aws.String("NonexistentIndex"),
	})
	if err == nil {
		t.Fatal("expected error for scanning nonexistent index")
	}
}

func TestUpdateItemNoneReturnValues(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_none_return"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":   &types.AttributeValueMemberS{Value: "p1"},
			"SK":   &types.AttributeValueMemberS{Value: "s1"},
			"Name": &types.AttributeValueMemberS{Value: "Alice"},
		},
	})

	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		ReturnValues:     types.ReturnValueNone,
		UpdateExpression: aws.String("SET Name = :val"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "Bob"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem NONE: %v", err)
	}
	if len(out.Attributes) > 0 {
		t.Fatal("expected no attributes for NONE return value")
	}
}

func TestUpdateItemCreateNewItem(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_update_new_item"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// UpdateItem on non-existent item should create it
	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(tableName),
		ReturnValues:     types.ReturnValueAllNew,
		UpdateExpression: aws.String("SET Name = :val"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "New"},
		},
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "new-pk"},
			"SK": &types.AttributeValueMemberS{Value: "new-sk"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}
	name := out.Attributes["Name"].(*types.AttributeValueMemberS).Value
	if name != "New" {
		t.Fatalf("expected New, got %s", name)
	}

	// Verify the item was created
	get, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "new-pk"},
			"SK": &types.AttributeValueMemberS{Value: "new-sk"},
		},
	})
	if get.Item == nil {
		t.Fatal("item should have been created by UpdateItem")
	}
}

func TestFilterAttributeExistsE2E(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_filter_attr_exists"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "p1"},
			"SK":    &types.AttributeValueMemberS{Value: "s1"},
			"Email": &types.AttributeValueMemberS{Value: "a@b.com"},
		},
	})
	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "p1"},
			"SK": &types.AttributeValueMemberS{Value: "s2"},
		},
	})

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		FilterExpression:       aws.String("attribute_exists(Email)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
	})
	if err != nil {
		t.Fatalf("Query with attribute_exists: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item with Email, got %d", out.Count)
	}
}

func TestTransactWriteWithConditionOnPut(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_put_cond"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	// Put with condition that should succeed
	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName:           aws.String(tableName),
					ConditionExpression: aws.String("attribute_not_exists(PK)"),
					Item: map[string]types.AttributeValue{
						"PK":   &types.AttributeValueMemberS{Value: "tp#1"},
						"SK":   &types.AttributeValueMemberS{Value: "s1"},
						"Data": &types.AttributeValueMemberS{Value: "val"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("TransactWriteItems put with condition: %v", err)
	}

	// Same put with condition should fail
	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName:           aws.String(tableName),
					ConditionExpression: aws.String("attribute_not_exists(PK)"),
					Item: map[string]types.AttributeValue{
						"PK":   &types.AttributeValueMemberS{Value: "tp#1"},
						"SK":   &types.AttributeValueMemberS{Value: "s1"},
						"Data": &types.AttributeValueMemberS{Value: "val2"},
					},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected TransactionCanceledException")
	}
}

func TestTransactWriteDeleteWithCondition(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_del_cond"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":     &types.AttributeValueMemberS{Value: "td#1"},
			"SK":     &types.AttributeValueMemberS{Value: "s1"},
			"Status": &types.AttributeValueMemberS{Value: "active"},
		},
	})

	// Delete with correct condition
	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Delete: &types.Delete{
					TableName:           aws.String(tableName),
					ConditionExpression: aws.String("#s = :val"),
					ExpressionAttributeNames: map[string]string{
						"#s": "Status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":val": &types.AttributeValueMemberS{Value: "active"},
					},
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: "td#1"},
						"SK": &types.AttributeValueMemberS{Value: "s1"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("TransactWriteItems delete with condition: %v", err)
	}

	// Verify deleted
	get, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "td#1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	if get.Item != nil {
		t.Fatal("expected item to be deleted")
	}
}

func TestTransactWriteUpdateWithCondition(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_transact_upd_cond"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":      &types.AttributeValueMemberS{Value: "tu#1"},
			"SK":      &types.AttributeValueMemberS{Value: "s1"},
			"Version": &types.AttributeValueMemberN{Value: "1"},
		},
	})

	// Update with condition
	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName:           aws.String(tableName),
					ConditionExpression: aws.String("Version = :v"),
					UpdateExpression:    aws.String("SET Version = :newv"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":v":    &types.AttributeValueMemberN{Value: "1"},
						":newv": &types.AttributeValueMemberN{Value: "2"},
					},
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: "tu#1"},
						"SK": &types.AttributeValueMemberS{Value: "s1"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("TransactWriteItems update with condition: %v", err)
	}

	// Verify update
	get, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "tu#1"},
			"SK": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	v := get.Item["Version"].(*types.AttributeValueMemberN).Value
	if v != "2" {
		t.Fatalf("expected version 2, got %s", v)
	}
}

func TestDeleteItemFromGSITable(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_delete_gsi"
	defer deleteTestTable(t, client, tableName)

	_, _ = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("GSI1PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":     &types.AttributeValueMemberS{Value: "u1"},
			"SK":     &types.AttributeValueMemberS{Value: "profile"},
			"GSI1PK": &types.AttributeValueMemberS{Value: "org#1"},
		},
	})

	// Verify GSI has the item
	gsiOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "org#1"},
		},
	})
	if err != nil {
		t.Fatalf("Query GSI before delete: %v", err)
	}
	if gsiOut.Count != 1 {
		t.Fatalf("expected 1 in GSI before delete, got %d", gsiOut.Count)
	}

	// Delete the item
	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "u1"},
			"SK": &types.AttributeValueMemberS{Value: "profile"},
		},
	})
	if err != nil {
		t.Fatalf("DeleteItem: %v", err)
	}

	// Verify GSI entry is removed
	gsiOut2, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "org#1"},
		},
	})
	if err != nil {
		t.Fatalf("Query GSI after delete: %v", err)
	}
	if gsiOut2.Count != 0 {
		t.Fatalf("expected 0 in GSI after delete, got %d", gsiOut2.Count)
	}
}

func TestBatchWriteWithGSI(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_batch_gsi"
	defer deleteTestTable(t, client, tableName)

	_, _ = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("GSI1PK"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("GSI1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("GSI1PK"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	// Batch write items with GSI attributes
	_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: {
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "bg#1"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
					"GSI1PK": &types.AttributeValueMemberS{Value: "org#1"},
				}}},
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
					"PK": &types.AttributeValueMemberS{Value: "bg#2"}, "SK": &types.AttributeValueMemberS{Value: "s1"},
					"GSI1PK": &types.AttributeValueMemberS{Value: "org#1"},
				}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}

	// Verify GSI has items
	gsiOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("GSI1"),
		KeyConditionExpression: aws.String("GSI1PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "org#1"},
		},
	})
	if err != nil {
		t.Fatalf("Query GSI: %v", err)
	}
	if gsiOut.Count != 2 {
		t.Fatalf("expected 2 in GSI, got %d", gsiOut.Count)
	}
}

func TestQueryWithProjection(t *testing.T) {
	client, cleanup := setupClient(t)
	defer cleanup()
	ctx := context.Background()
	tableName := "test_query_projection"
	defer deleteTestTable(t, client, tableName)
	createTestTable(t, client, tableName)

	_, _ = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"PK":    &types.AttributeValueMemberS{Value: "p1"},
			"SK":    &types.AttributeValueMemberS{Value: "s1"},
			"Name":  &types.AttributeValueMemberS{Value: "Alice"},
			"Email": &types.AttributeValueMemberS{Value: "alice@test.com"},
			"Phone": &types.AttributeValueMemberS{Value: "555"},
		},
	})

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		ProjectionExpression:   aws.String("Name"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
	})
	if err != nil {
		t.Fatalf("Query with projection: %v", err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1, got %d", out.Count)
	}
	item := out.Items[0]
	if _, ok := item["Name"]; !ok {
		t.Fatal("Name should be in projection")
	}
	if _, ok := item["Email"]; ok {
		t.Fatal("Email should NOT be in projection")
	}
}
