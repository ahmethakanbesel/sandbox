# dynamo-pg-client

Use the standard AWS Go SDK v2 `dynamodb.Client` with PostgreSQL as the storage backend. No real DynamoDB needed.

The translation happens entirely in-process — a custom `HTTPClient` intercepts AWS SDK requests, translates DynamoDB operations to PostgreSQL queries, and returns properly formatted responses. No HTTP server, no network hop.

## Installation

```bash
go get github.com/ahmethakanbesel/dynamo-pg-client
```

Requires Go 1.21+ and PostgreSQL 14+.

## Quick Start

Start PostgreSQL:

```bash
docker compose up -d
```

Use the client exactly like a real DynamoDB client:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func main() {
    ctx := context.Background()
    client, cleanup, err := pgdynamo.NewWithCleanup(ctx,
        "postgres://dynamo:dynamo@localhost:5433/dynamo?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer cleanup()

    // Create a table
    _, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
        TableName: aws.String("Users"),
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
        log.Fatal(err)
    }

    // Put an item
    item, _ := attributevalue.MarshalMap(map[string]any{
        "PK": "USER#1", "SK": "PROFILE", "Name": "Alice", "Age": 30,
    })
    _, err = client.PutItem(ctx, &dynamodb.PutItemInput{
        TableName: aws.String("Users"),
        Item:      item,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Query items
    out, err := client.Query(ctx, &dynamodb.QueryInput{
        TableName:              aws.String("Users"),
        KeyConditionExpression: aws.String("PK = :pk"),
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":pk": &types.AttributeValueMemberS{Value: "USER#1"},
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Found %d items\n", out.Count)
}
```

## Supported Operations

### Table Operations
| Operation | Notes |
|-----------|-------|
| CreateTable | Partition key, sort key, GSI, LSI |
| DeleteTable | |
| DescribeTable | |
| ListTables | Paginated |
| UpdateTable | Add/remove GSIs |

### Item Operations
| Operation | Notes |
|-----------|-------|
| PutItem | ConditionExpression, ReturnValues (ALL_OLD) |
| GetItem | ProjectionExpression, ConsistentRead |
| DeleteItem | ConditionExpression, ReturnValues (ALL_OLD) |
| UpdateItem | SET, ADD, REMOVE, DELETE actions; ReturnValues |

### Query & Scan
| Operation | Notes |
|-----------|-------|
| Query | KeyConditionExpression, FilterExpression, Limit, pagination, ScanIndexForward, Select=COUNT |
| Scan | FilterExpression, Limit, pagination |

### Batch & Transaction Operations
| Operation | Notes |
|-----------|-------|
| BatchWriteItem | Put and Delete, up to 25 items |
| BatchGetItem | Up to 100 keys |
| TransactWriteItems | Put, Delete, Update, ConditionCheck |
| TransactGetItems | Up to 100 items |

### Expression Support
- **KeyConditionExpression**: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`
- **FilterExpression**: comparisons, `begins_with`, `contains`, `attribute_exists`, `attribute_not_exists`, `attribute_type`, `size`, `IN`, `BETWEEN`, `AND`, `OR`, `NOT`
- **ConditionExpression**: same operators as FilterExpression
- **UpdateExpression**: `SET` (with `if_not_exists`, `list_append`), `ADD`, `REMOVE`, `DELETE`
- **ProjectionExpression**: attribute selection
- **ExpressionAttributeNames** and **ExpressionAttributeValues**

### Secondary Indexes
- **Global Secondary Indexes (GSI)**: created with table or added via UpdateTable
- **Local Secondary Indexes (LSI)**: created with table
- Projection types: ALL, KEYS_ONLY, INCLUDE

## Configuration

### Basic

```go
client, err := pgdynamo.New(ctx, "postgres://user:pass@host:5432/db?sslmode=disable")
```

### With Cleanup

```go
client, cleanup, err := pgdynamo.NewWithCleanup(ctx, connString)
defer cleanup() // closes the connection pool
```

### With Connection Pool Settings

```go
client, cleanup, err := pgdynamo.NewWithConfig(ctx, storage.StoreConfig{
    ConnString:      "postgres://user:pass@host:5432/db",
    MaxConns:        20,
    MinConns:        5,
    MaxConnLifetime: 30 * time.Minute,
    MaxConnIdleTime: 5 * time.Minute,
    EnableMetaCache: true, // caches table metadata in-memory
})
```

## How It Works

```
Your code → dynamodb.Client → AWS SDK middleware → pgTransport.Do()
                                                        ↓
                                                  Parse X-Amz-Target header
                                                  Parse JSON request body
                                                  Route to operation handler
                                                  Execute PostgreSQL queries
                                                  Return http.Response
```

Each DynamoDB table maps to a PostgreSQL table:

```sql
CREATE TABLE "dyn_Users" (
    pk         TEXT NOT NULL,
    sk         TEXT NOT NULL DEFAULT '',
    attributes JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (pk, sk)
);
```

- Partition and sort keys are stored as `TEXT` columns with a composite primary key
- All item attributes are stored in a `JSONB` column preserving DynamoDB's type descriptors (`{"Name": {"S": "Alice"}, "Age": {"N": "30"}}`)
- A GIN index on `attributes` and a hash index on `pk` are created automatically
- Secondary indexes (GSI/LSI) are backed by separate PostgreSQL tables with their own PK/SK columns

## Use Cases

- **Local development and testing** without a DynamoDB instance or AWS credentials
- **CI/CD pipelines** with PostgreSQL instead of DynamoDB Local (Java-based)
- **Integration tests** that need a real database but not real DynamoDB

## Limitations

- This is a compatibility layer, not a production DynamoDB replacement
- No provisioned throughput or capacity management
- No DynamoDB Streams
- No TTL enforcement (metadata tracked but items not auto-deleted)
- No parallel Scan (Segment/TotalSegments)
- FilterExpression is evaluated in Go after fetching rows (matching DynamoDB's Limit-before-filter semantics) rather than pushed down to SQL

## Running Tests

```bash
# Start PostgreSQL
docker compose up -d

# Run all tests
make test

# Run only e2e tests
make test-e2e

# Run only unit tests
make test-unit

# Run benchmarks
make bench
```

## License

MIT
