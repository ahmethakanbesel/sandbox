package pgdynamo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	json "github.com/goccy/go-json"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/operations"
	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

// Handler routes DynamoDB operations to the appropriate handler function.
type Handler struct {
	deps *operations.Deps
}

// NewHandler creates a new Handler with the given storage.
func NewHandler(store *storage.Store) *Handler {
	return &Handler{
		deps: &operations.Deps{Store: store},
	}
}

// HandleOperation dispatches a DynamoDB operation by name.
func (h *Handler) HandleOperation(ctx context.Context, operation string, body json.RawMessage) ([]byte, error) {
	var result any
	var err error

	switch operation {
	case "CreateTable":
		result, err = operations.HandleCreateTable(ctx, h.deps, body)
	case "DeleteTable":
		result, err = operations.HandleDeleteTable(ctx, h.deps, body)
	case "DescribeTable":
		result, err = operations.HandleDescribeTable(ctx, h.deps, body)
	case "ListTables":
		result, err = operations.HandleListTables(ctx, h.deps, body)
	case "PutItem":
		result, err = operations.HandlePutItem(ctx, h.deps, body)
	case "GetItem":
		result, err = operations.HandleGetItem(ctx, h.deps, body)
	case "DeleteItem":
		result, err = operations.HandleDeleteItem(ctx, h.deps, body)
	case "Query":
		result, err = operations.HandleQuery(ctx, h.deps, body)
	case "Scan":
		result, err = operations.HandleScan(ctx, h.deps, body)
	case "UpdateItem":
		result, err = operations.HandleUpdateItem(ctx, h.deps, body)
	case "TransactWriteItems":
		result, err = operations.HandleTransactWriteItems(ctx, h.deps, body)
	case "TransactGetItems":
		result, err = operations.HandleTransactGetItems(ctx, h.deps, body)
	case "BatchWriteItem":
		result, err = operations.HandleBatchWriteItem(ctx, h.deps, body)
	case "BatchGetItem":
		result, err = operations.HandleBatchGetItem(ctx, h.deps, body)
	case "UpdateTable":
		result, err = operations.HandleUpdateTable(ctx, h.deps, body)
	case "DescribeEndpoints":
		result = map[string]any{"Endpoints": []map[string]any{{
			"Address":            "dynamodb.local",
			"CachePeriodInMinutes": 1440,
		}}}
	case "DescribeTimeToLive":
		result, err = operations.HandleDescribeTimeToLive(ctx, h.deps, body)
	case "UpdateTimeToLive":
		result, err = operations.HandleUpdateTimeToLive(ctx, h.deps, body)
	case "TagResource", "UntagResource":
		result = map[string]any{}
	case "ListTagsOfResource":
		result = map[string]any{"Tags": []any{}}
	case "DescribeLimits":
		result = map[string]any{
			"AccountMaxReadCapacityUnits":  80000,
			"AccountMaxWriteCapacityUnits": 80000,
			"TableMaxReadCapacityUnits":    40000,
			"TableMaxWriteCapacityUnits":   40000,
		}
	case "DescribeContinuousBackups":
		result, err = operations.HandleDescribeContinuousBackups(ctx, h.deps, body)
	default:
		return nil, &DynamoError{
			Code:       "UnknownOperationException",
			Message:    fmt.Sprintf("Operation %s is not supported", operation),
			StatusCode: 400,
		}
	}

	if err != nil {
		return nil, classifyError(err)
	}

	return json.Marshal(result)
}

// classifyError converts generic errors to DynamoError if they contain known patterns.
func classifyError(err error) error {
	var de *DynamoError
	if errors.As(err, &de) {
		return err
	}

	msg := err.Error()
	if strings.HasPrefix(msg, "ResourceNotFoundException:") {
		return ErrResourceNotFound(strings.TrimPrefix(msg, "ResourceNotFoundException: "))
	}
	if strings.HasPrefix(msg, "ResourceInUseException:") {
		return ErrResourceInUse(strings.TrimPrefix(msg, "ResourceInUseException: "))
	}
	if strings.HasPrefix(msg, "ValidationException:") {
		return ErrValidation(strings.TrimPrefix(msg, "ValidationException: "))
	}
	if strings.HasPrefix(msg, "ConditionalCheckFailedException:") {
		return ErrConditionalCheckFailed(strings.TrimPrefix(msg, "ConditionalCheckFailedException: "))
	}
	if strings.HasPrefix(msg, "TransactionCanceledException:") {
		return &DynamoError{
			Code:       "TransactionCanceledException",
			Message:    strings.TrimPrefix(msg, "TransactionCanceledException: "),
			StatusCode: 400,
		}
	}
	return &DynamoError{
		Code:       "InternalServerError",
		Message:    msg,
		StatusCode: 500,
	}
}
