package operations

import (
	"context"
	json "github.com/goccy/go-json"
	"fmt"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/expr"
	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

func HandleScan(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName                 string                            `json:"TableName"`
		IndexName                 string                            `json:"IndexName"`
		FilterExpression          string                            `json:"FilterExpression"`
		ProjectionExpression      string                            `json:"ProjectionExpression"`
		ExpressionAttributeNames  map[string]string                 `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]storage.AttributeValue `json:"ExpressionAttributeValues"`
		Limit                     int                               `json:"Limit"`
		ExclusiveStartKey         storage.Item                      `json:"ExclusiveStartKey"`
		Select                    string                            `json:"Select"`
		ConsistentRead            bool                              `json:"ConsistentRead"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}

	meta, err := deps.Store.GetTableMeta(ctx, req.TableName)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, fmt.Errorf("ResourceNotFoundException: Requested resource not found: Table: %s not found", req.TableName)
	}

	// Resolve effective key names for pagination
	pkName, skName, pkType, skType := meta.PKName, meta.SKName, meta.PKType, meta.SKType
	if req.IndexName != "" {
		idx := findIndex(meta, req.IndexName)
		if idx == nil {
			return nil, fmt.Errorf("ValidationException: The table does not have the specified index: %s", req.IndexName)
		}
		pkName, skName, pkType, skType = idx.PKName, idx.SKName, idx.PKType, idx.SKType
	}

	// Parse filter expression
	var filterFn func(storage.Item) bool
	if req.FilterExpression != "" {
		fr, err := expr.ParseFilterExpression(
			req.FilterExpression,
			req.ExpressionAttributeNames,
			req.ExpressionAttributeValues,
		)
		if err != nil {
			return nil, err
		}
		filterFn = fr.Func
	}

	// Extract pagination cursor
	var startPK, startSK string
	if req.ExclusiveStartKey != nil {
		startPK, _ = storage.ExtractKeyValue(req.ExclusiveStartKey, pkName, pkType)
		if skName != "" {
			startSK, _ = storage.ExtractKeyValue(req.ExclusiveStartKey, skName, skType)
		}
	}

	params := storage.ScanParams{
		TableName:        req.TableName,
		IndexName:        req.IndexName,
		Limit:            req.Limit,
		ExclusiveStartPK: startPK,
		ExclusiveStartSK: startSK,
		SKType:           skType,
		SelectCount:      req.Select == "COUNT",
	}

	result, err := deps.Store.Scan(ctx, params)
	if err != nil {
		return nil, err
	}

	// Apply filter in Go (DynamoDB semantics)
	if filterFn != nil {
		var filtered []storage.Item
		for _, item := range result.Items {
			if filterFn(item) {
				filtered = append(filtered, item)
			}
		}
		result.Items = filtered
		result.Count = len(filtered)
	}

	// Apply projection
	if req.ProjectionExpression != "" {
		projections := parseProjectionExpression(req.ProjectionExpression, req.ExpressionAttributeNames)
		for i, item := range result.Items {
			result.Items[i] = storage.ProjectItem(item, projections, pkName, skName)
		}
	}

	resp := map[string]any{
		"Count":        result.Count,
		"ScannedCount": result.ScannedCount,
	}

	if req.Select != "COUNT" {
		items := result.Items
		if items == nil {
			items = []storage.Item{}
		}
		resp["Items"] = items
	}

	if result.LastEvaluatedPK != "" {
		lastKey := storage.Item{
			pkName: storage.BuildKeyAttributeValue(result.LastEvaluatedPK, pkType),
		}
		if skName != "" {
			lastKey[skName] = storage.BuildKeyAttributeValue(result.LastEvaluatedSK, skType)
		}
		resp["LastEvaluatedKey"] = lastKey
	}

	return resp, nil
}
