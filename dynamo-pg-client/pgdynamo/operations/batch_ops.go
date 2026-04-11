package operations

import (
	"context"
	json "github.com/goccy/go-json"
	"fmt"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

func HandleBatchWriteItem(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		RequestItems map[string][]struct {
			PutRequest *struct {
				Item storage.Item `json:"Item"`
			} `json:"PutRequest"`
			DeleteRequest *struct {
				Key storage.Item `json:"Key"`
			} `json:"DeleteRequest"`
		} `json:"RequestItems"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}

	if len(req.RequestItems) == 0 {
		return nil, fmt.Errorf("ValidationException: RequestItems is required")
	}

	for tableName, requests := range req.RequestItems {
		meta, err := deps.Store.GetTableMeta(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, fmt.Errorf("ResourceNotFoundException: Requested resource not found: Table: %s not found", tableName)
		}

		// Separate puts and deletes for batch processing
		var putItems []storage.Item
		var deleteKeys []storage.Item
		for _, r := range requests {
			switch {
			case r.PutRequest != nil:
				putItems = append(putItems, r.PutRequest.Item)
			case r.DeleteRequest != nil:
				deleteKeys = append(deleteKeys, r.DeleteRequest.Key)
			default:
				return nil, fmt.Errorf("ValidationException: each write request must contain a PutRequest or DeleteRequest")
			}
		}

		if len(putItems) > 0 {
			if err := deps.Store.BatchPutItems(ctx, tableName, meta, putItems); err != nil {
				return nil, err
			}
		}
		if len(deleteKeys) > 0 {
			if err := deps.Store.BatchDeleteItems(ctx, tableName, meta, deleteKeys); err != nil {
				return nil, err
			}
		}
	}

	resp := map[string]any{
		"UnprocessedItems": map[string]any{},
	}
	return resp, nil
}

func HandleBatchGetItem(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		RequestItems map[string]struct {
			Keys                     []storage.Item    `json:"Keys"`
			ProjectionExpression     string            `json:"ProjectionExpression"`
			ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
			ConsistentRead           bool              `json:"ConsistentRead"`
		} `json:"RequestItems"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}

	if len(req.RequestItems) == 0 {
		return nil, fmt.Errorf("ValidationException: RequestItems is required")
	}

	responses := make(map[string][]storage.Item, len(req.RequestItems))

	for tableName, tableReq := range req.RequestItems {
		meta, err := deps.Store.GetTableMeta(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, fmt.Errorf("ResourceNotFoundException: Requested resource not found: Table: %s not found", tableName)
		}

		var projections []string
		if tableReq.ProjectionExpression != "" {
			projections = parseProjectionExpression(tableReq.ProjectionExpression, tableReq.ExpressionAttributeNames)
		}

		items := make([]storage.Item, 0, len(tableReq.Keys))
		for _, key := range tableReq.Keys {
			item, err := deps.Store.GetItem(ctx, tableName, meta, key)
			if err != nil {
				return nil, err
			}
			if item == nil {
				continue
			}
			if len(projections) > 0 {
				item = storage.ProjectItem(item, projections, meta.PKName, meta.SKName)
			}
			items = append(items, item)
		}

		responses[tableName] = items
	}

	resp := map[string]any{
		"Responses":       responses,
		"UnprocessedKeys": map[string]any{},
	}
	return resp, nil
}
