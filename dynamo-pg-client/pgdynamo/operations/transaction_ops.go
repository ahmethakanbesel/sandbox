package operations

import (
	"context"
	json "github.com/goccy/go-json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/expr"
	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

// transactWriteRequest represents the DynamoDB TransactWriteItems request body.
type transactWriteRequest struct {
	TransactItems []transactWriteItem `json:"TransactItems"`
}

type transactWriteItem struct {
	Put            *transactPut            `json:"Put"`
	Delete         *transactDelete         `json:"Delete"`
	Update         *transactUpdate         `json:"Update"`
	ConditionCheck *transactConditionCheck `json:"ConditionCheck"`
}

type transactPut struct {
	TableName                 string                            `json:"TableName"`
	Item                      storage.Item                      `json:"Item"`
	ConditionExpression       string                            `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string                 `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]storage.AttributeValue `json:"ExpressionAttributeValues"`
}

type transactDelete struct {
	TableName                 string                            `json:"TableName"`
	Key                       storage.Item                      `json:"Key"`
	ConditionExpression       string                            `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string                 `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]storage.AttributeValue `json:"ExpressionAttributeValues"`
}

type transactUpdate struct {
	TableName                 string                            `json:"TableName"`
	Key                       storage.Item                      `json:"Key"`
	UpdateExpression          string                            `json:"UpdateExpression"`
	ConditionExpression       string                            `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string                 `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]storage.AttributeValue `json:"ExpressionAttributeValues"`
}

type transactConditionCheck struct {
	TableName                 string                            `json:"TableName"`
	Key                       storage.Item                      `json:"Key"`
	ConditionExpression       string                            `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string                 `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]storage.AttributeValue `json:"ExpressionAttributeValues"`
}

// transactGetRequest represents the DynamoDB TransactGetItems request body.
type transactGetRequest struct {
	TransactItems []transactGetItem `json:"TransactItems"`
}

type transactGetItem struct {
	Get *transactGet `json:"Get"`
}

type transactGet struct {
	TableName                string            `json:"TableName"`
	Key                      storage.Item      `json:"Key"`
	ProjectionExpression     string            `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
}

// HandleTransactWriteItems implements the DynamoDB TransactWriteItems operation.
// All writes are performed atomically within a single PostgreSQL transaction.
// If any condition expression fails, the entire transaction is rolled back and
// a TransactionCanceledException is returned with per-item cancellation reasons.
func HandleTransactWriteItems(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req transactWriteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}

	if len(req.TransactItems) == 0 {
		return nil, fmt.Errorf("ValidationException: TransactItems is required and must not be empty")
	}

	tx, err := deps.Store.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("InternalServerError: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is expected to fail

	cancelReasons := make([]string, len(req.TransactItems))
	canceled := false

	for i, item := range req.TransactItems {
		reason, err := executeTransactWriteItem(ctx, deps, tx, item)
		if err != nil {
			return nil, err
		}
		if reason != "" {
			cancelReasons[i] = reason
			canceled = true
		} else {
			cancelReasons[i] = "None"
		}
	}

	if canceled {
		return nil, fmt.Errorf(
			"TransactionCanceledException: Transaction canceled, please refer cancellation reasons for specific reasons [%s]",
			strings.Join(cancelReasons, ", "),
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("InternalServerError: %w", err)
	}

	return map[string]any{}, nil
}

// executeTransactWriteItem processes a single item within a TransactWriteItems call.
// It returns a cancellation reason string if a condition check fails, or an empty
// string on success. Non-condition errors are returned as the error value.
func executeTransactWriteItem(ctx context.Context, deps *Deps, tx pgx.Tx, item transactWriteItem) (string, error) {
	switch {
	case item.Put != nil:
		return executeTransactPut(ctx, deps, tx, item.Put)
	case item.Delete != nil:
		return executeTransactDelete(ctx, deps, tx, item.Delete)
	case item.Update != nil:
		return executeTransactUpdate(ctx, deps, tx, item.Update)
	case item.ConditionCheck != nil:
		return executeTransactConditionCheck(ctx, deps, tx, item.ConditionCheck)
	default:
		return "", fmt.Errorf("ValidationException: TransactItem must contain exactly one operation")
	}
}

func executeTransactPut(ctx context.Context, deps *Deps, tx pgx.Tx, put *transactPut) (string, error) {
	meta, err := resolveTableMetaTx(ctx, deps, tx, put.TableName)
	if err != nil {
		return "", err
	}

	key := extractKeyFromItem(put.Item, meta)

	if put.ConditionExpression != "" {
		reason, err := checkConditionTx(ctx, deps, tx, put.TableName, meta, key,
			put.ConditionExpression, put.ExpressionAttributeNames, put.ExpressionAttributeValues)
		if err != nil {
			return "", err
		}
		if reason != "" {
			return reason, nil
		}
	}

	if _, err := deps.Store.PutItemTx(ctx, tx, put.TableName, meta, put.Item, false); err != nil {
		return "", err
	}
	return "", nil
}

func executeTransactDelete(ctx context.Context, deps *Deps, tx pgx.Tx, del *transactDelete) (string, error) {
	meta, err := resolveTableMetaTx(ctx, deps, tx, del.TableName)
	if err != nil {
		return "", err
	}

	if del.ConditionExpression != "" {
		reason, err := checkConditionTx(ctx, deps, tx, del.TableName, meta, del.Key,
			del.ConditionExpression, del.ExpressionAttributeNames, del.ExpressionAttributeValues)
		if err != nil {
			return "", err
		}
		if reason != "" {
			return reason, nil
		}
	}

	if _, err := deps.Store.DeleteItemTx(ctx, tx, del.TableName, meta, del.Key, false); err != nil {
		return "", err
	}
	return "", nil
}

func executeTransactUpdate(ctx context.Context, deps *Deps, tx pgx.Tx, upd *transactUpdate) (string, error) {
	meta, err := resolveTableMetaTx(ctx, deps, tx, upd.TableName)
	if err != nil {
		return "", err
	}

	actions, err := expr.ParseUpdateExpression(upd.UpdateExpression, upd.ExpressionAttributeNames, upd.ExpressionAttributeValues)
	if err != nil {
		return "", err
	}

	existingItem, err := deps.Store.GetItemTx(ctx, tx, upd.TableName, meta, upd.Key)
	if err != nil {
		return "", err
	}

	if upd.ConditionExpression != "" {
		ok, err := expr.EvaluateCondition(upd.ConditionExpression, upd.ExpressionAttributeNames, upd.ExpressionAttributeValues, existingItem)
		if err != nil {
			return "", err
		}
		if !ok {
			return "ConditionalCheckFailed", nil
		}
	}

	baseItem := existingItem
	if baseItem == nil {
		baseItem = storage.Item{}
	}
	for k, v := range upd.Key {
		baseItem[k] = v
	}

	newItem := expr.ApplyUpdateActions(baseItem, actions, upd.ExpressionAttributeValues)
	for k, v := range upd.Key {
		newItem[k] = v
	}

	if _, err := deps.Store.UpdateItemTx(ctx, tx, upd.TableName, meta, upd.Key, newItem, false); err != nil {
		return "", err
	}
	return "", nil
}

func executeTransactConditionCheck(ctx context.Context, deps *Deps, tx pgx.Tx, cc *transactConditionCheck) (string, error) {
	meta, err := resolveTableMetaTx(ctx, deps, tx, cc.TableName)
	if err != nil {
		return "", err
	}

	return checkConditionTx(ctx, deps, tx, cc.TableName, meta, cc.Key,
		cc.ConditionExpression, cc.ExpressionAttributeNames, cc.ExpressionAttributeValues)
}

// HandleTransactGetItems implements the DynamoDB TransactGetItems operation.
// All reads are performed within a single PostgreSQL transaction for consistency.
func HandleTransactGetItems(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req transactGetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}

	if len(req.TransactItems) == 0 {
		return nil, fmt.Errorf("ValidationException: TransactItems is required and must not be empty")
	}

	tx, err := deps.Store.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("InternalServerError: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is expected to fail

	responses := make([]map[string]any, len(req.TransactItems))

	for i, ti := range req.TransactItems {
		if ti.Get == nil {
			return nil, fmt.Errorf("ValidationException: TransactGetItem must contain a Get operation")
		}

		meta, err := resolveTableMetaTx(ctx, deps, tx, ti.Get.TableName)
		if err != nil {
			return nil, err
		}

		item, err := deps.Store.GetItemTx(ctx, tx, ti.Get.TableName, meta, ti.Get.Key)
		if err != nil {
			return nil, err
		}

		entry := map[string]any{}
		if item != nil {
			if ti.Get.ProjectionExpression != "" {
				projections := parseProjectionExpression(ti.Get.ProjectionExpression, ti.Get.ExpressionAttributeNames)
				item = storage.ProjectItem(item, projections, meta.PKName, meta.SKName)
			}
			entry["Item"] = item
		}
		responses[i] = entry
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("InternalServerError: %w", err)
	}

	return map[string]any{"Responses": responses}, nil
}

// resolveTableMetaTx fetches and validates table metadata within a transaction,
// returning an error if the table does not exist.
func resolveTableMetaTx(ctx context.Context, deps *Deps, tx pgx.Tx, tableName string) (*storage.TableMeta, error) {
	meta, err := deps.Store.GetTableMetaTx(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, fmt.Errorf("ResourceNotFoundException: Requested resource not found: Table: %s not found", tableName)
	}
	return meta, nil
}

// checkConditionTx evaluates a condition expression against the existing item within
// a transaction. Returns "ConditionalCheckFailed" if the condition is not met, or an
// empty string if it passes.
func checkConditionTx(
	ctx context.Context,
	deps *Deps,
	tx pgx.Tx,
	tableName string,
	meta *storage.TableMeta,
	key storage.Item,
	conditionExpr string,
	names map[string]string,
	values map[string]storage.AttributeValue,
) (string, error) {
	existingItem, err := deps.Store.GetItemTx(ctx, tx, tableName, meta, key)
	if err != nil {
		return "", err
	}

	ok, err := expr.EvaluateCondition(conditionExpr, names, values, existingItem)
	if err != nil {
		return "", err
	}
	if !ok {
		return "ConditionalCheckFailed", nil
	}
	return "", nil
}
