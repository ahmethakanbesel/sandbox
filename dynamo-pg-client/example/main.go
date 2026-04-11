package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type User struct {
	PK    string `dynamodbav:"PK"`
	SK    string `dynamodbav:"SK"`
	Name  string `dynamodbav:"Name"`
	Email string `dynamodbav:"Email"`
	Age   int    `dynamodbav:"Age"`
}

func main() {
	ctx := context.Background()

	connStr := os.Getenv("PG_CONN")
	if connStr == "" {
		connStr = "postgres://dynamo:dynamo@localhost:5433/dynamo?sslmode=disable"
	}

	client, cleanup, err := pgdynamo.NewWithCleanup(ctx, connStr)
	if err != nil {
		log.Fatal("failed to create client:", err)
	}
	defer cleanup()

	tableName := "Users"

	// --- CreateTable ---
	fmt.Println("=== CreateTable ===")
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
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
		log.Fatal("CreateTable:", err)
	}
	fmt.Println("Table created successfully")

	// --- DescribeTable ---
	fmt.Println("\n=== DescribeTable ===")
	descOut, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		log.Fatal("DescribeTable:", err)
	}
	fmt.Printf("Table: %s, Status: %s\n", *descOut.Table.TableName, descOut.Table.TableStatus)

	// --- ListTables ---
	fmt.Println("\n=== ListTables ===")
	listOut, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		log.Fatal("ListTables:", err)
	}
	fmt.Printf("Tables: %v\n", listOut.TableNames)

	// --- PutItem (insert several users) ---
	fmt.Println("\n=== PutItem ===")
	users := []User{
		{PK: "USER#1", SK: "PROFILE", Name: "Alice", Email: "alice@example.com", Age: 30},
		{PK: "USER#1", SK: "ORDER#001", Name: "Alice", Email: "alice@example.com", Age: 30},
		{PK: "USER#1", SK: "ORDER#002", Name: "Alice", Email: "alice@example.com", Age: 30},
		{PK: "USER#2", SK: "PROFILE", Name: "Bob", Email: "bob@example.com", Age: 25},
		{PK: "USER#2", SK: "ORDER#001", Name: "Bob", Email: "bob@example.com", Age: 25},
		{PK: "USER#3", SK: "PROFILE", Name: "Charlie", Email: "charlie@example.com", Age: 35},
	}

	for _, u := range users {
		item, err := attributevalue.MarshalMap(u)
		if err != nil {
			log.Fatal("MarshalMap:", err)
		}
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		if err != nil {
			log.Fatal("PutItem:", err)
		}
		fmt.Printf("Put: PK=%s SK=%s\n", u.PK, u.SK)
	}

	// --- GetItem ---
	fmt.Println("\n=== GetItem ===")
	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#1"},
			"SK": &types.AttributeValueMemberS{Value: "PROFILE"},
		},
	})
	if err != nil {
		log.Fatal("GetItem:", err)
	}
	var got User
	if err := attributevalue.UnmarshalMap(getOut.Item, &got); err != nil {
		log.Fatal("UnmarshalMap:", err)
	}
	fmt.Printf("Got: %+v\n", got)

	// --- Query (all items for USER#1) ---
	fmt.Println("\n=== Query (all USER#1 items) ===")
	queryOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "USER#1"},
		},
	})
	if err != nil {
		log.Fatal("Query:", err)
	}
	fmt.Printf("Found %d items (scanned %d)\n", queryOut.Count, queryOut.ScannedCount)
	for _, item := range queryOut.Items {
		var u User
		_ = attributevalue.UnmarshalMap(item, &u)
		fmt.Printf("  PK=%s SK=%s Name=%s\n", u.PK, u.SK, u.Name)
	}

	// --- Query (USER#1 orders only, using begins_with) ---
	fmt.Println("\n=== Query (USER#1 orders with begins_with) ===")
	queryOut2, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":     &types.AttributeValueMemberS{Value: "USER#1"},
			":prefix": &types.AttributeValueMemberS{Value: "ORDER#"},
		},
	})
	if err != nil {
		log.Fatal("Query begins_with:", err)
	}
	fmt.Printf("Found %d orders\n", queryOut2.Count)
	for _, item := range queryOut2.Items {
		var u User
		_ = attributevalue.UnmarshalMap(item, &u)
		fmt.Printf("  PK=%s SK=%s\n", u.PK, u.SK)
	}

	// --- Query with Limit and pagination ---
	fmt.Println("\n=== Query (USER#1 with Limit=1, paginated) ===")
	var lastKey map[string]types.AttributeValue
	page := 0
	for {
		page++
		input := &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("PK = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "USER#1"},
			},
			Limit:             aws.Int32(1),
			ExclusiveStartKey: lastKey,
		}
		out, err := client.Query(ctx, input)
		if err != nil {
			log.Fatal("Query paginated:", err)
		}
		for _, item := range out.Items {
			var u User
			_ = attributevalue.UnmarshalMap(item, &u)
			fmt.Printf("  Page %d: PK=%s SK=%s\n", page, u.PK, u.SK)
		}
		lastKey = out.LastEvaluatedKey
		if lastKey == nil {
			break
		}
	}

	// --- Scan ---
	fmt.Println("\n=== Scan (all items) ===")
	scanOut, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		log.Fatal("Scan:", err)
	}
	fmt.Printf("Scanned %d items\n", scanOut.Count)
	for _, item := range scanOut.Items {
		var u User
		_ = attributevalue.UnmarshalMap(item, &u)
		fmt.Printf("  PK=%s SK=%s Name=%s\n", u.PK, u.SK, u.Name)
	}

	// --- Scan with FilterExpression ---
	fmt.Println("\n=== Scan (filter: Age > 28) ===")
	scanOut2, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		FilterExpression: aws.String("Age > :minAge"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":minAge": &types.AttributeValueMemberN{Value: "28"},
		},
	})
	if err != nil {
		log.Fatal("Scan filtered:", err)
	}
	fmt.Printf("Filtered: %d items (scanned %d)\n", scanOut2.Count, scanOut2.ScannedCount)
	for _, item := range scanOut2.Items {
		var u User
		_ = attributevalue.UnmarshalMap(item, &u)
		fmt.Printf("  PK=%s SK=%s Name=%s Age=%d\n", u.PK, u.SK, u.Name, u.Age)
	}

	// --- DeleteItem with ReturnValues ---
	fmt.Println("\n=== DeleteItem (USER#3/PROFILE with ALL_OLD) ===")
	delOut, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#3"},
			"SK": &types.AttributeValueMemberS{Value: "PROFILE"},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		log.Fatal("DeleteItem:", err)
	}
	if delOut.Attributes != nil {
		var deleted User
		_ = attributevalue.UnmarshalMap(delOut.Attributes, &deleted)
		fmt.Printf("Deleted: %+v\n", deleted)
	}

	// --- DeleteTable ---
	fmt.Println("\n=== DeleteTable ===")
	_, err = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		log.Fatal("DeleteTable:", err)
	}
	fmt.Println("Table deleted successfully")

	fmt.Println("\nAll operations completed successfully!")
}
