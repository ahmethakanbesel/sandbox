package operations

import (
	"context"
	json "github.com/goccy/go-json"
	"fmt"
	"strings"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/expr"
	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

func HandlePutItem(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName                 string                            `json:"TableName"`
		Item                      storage.Item                      `json:"Item"`
		ReturnValues              string                            `json:"ReturnValues"`
		ConditionExpression       string                            `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string                 `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]storage.AttributeValue `json:"ExpressionAttributeValues"`
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

	// Evaluate condition if present
	if req.ConditionExpression != "" {
		existingItem, err := deps.Store.GetItem(ctx, req.TableName, meta, extractKeyFromItem(req.Item, meta))
		if err != nil {
			return nil, err
		}
		ok, err := expr.EvaluateCondition(req.ConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues, existingItem)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("ConditionalCheckFailedException: The conditional request failed")
		}
	}

	returnOld := req.ReturnValues == "ALL_OLD"
	oldItem, err := deps.Store.PutItem(ctx, req.TableName, meta, req.Item, returnOld)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{}
	if returnOld && oldItem != nil {
		resp["Attributes"] = oldItem
	}
	return resp, nil
}

func HandleGetItem(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName                string            `json:"TableName"`
		Key                      storage.Item      `json:"Key"`
		ProjectionExpression     string            `json:"ProjectionExpression"`
		ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
		ConsistentRead           bool              `json:"ConsistentRead"`
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

	item, err := deps.Store.GetItem(ctx, req.TableName, meta, req.Key)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{}
	if item != nil {
		if req.ProjectionExpression != "" {
			projections := parseProjectionExpression(req.ProjectionExpression, req.ExpressionAttributeNames)
			item = storage.ProjectItem(item, projections, meta.PKName, meta.SKName)
		}
		resp["Item"] = item
	}
	return resp, nil
}

func HandleDeleteItem(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName                 string                            `json:"TableName"`
		Key                       storage.Item                      `json:"Key"`
		ReturnValues              string                            `json:"ReturnValues"`
		ConditionExpression       string                            `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string                 `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]storage.AttributeValue `json:"ExpressionAttributeValues"`
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

	// Evaluate condition if present
	if req.ConditionExpression != "" {
		existingItem, err := deps.Store.GetItem(ctx, req.TableName, meta, req.Key)
		if err != nil {
			return nil, err
		}
		ok, err := expr.EvaluateCondition(req.ConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues, existingItem)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("ConditionalCheckFailedException: The conditional request failed")
		}
	}

	returnOld := req.ReturnValues == "ALL_OLD"
	oldItem, err := deps.Store.DeleteItem(ctx, req.TableName, meta, req.Key, returnOld)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{}
	if returnOld && oldItem != nil {
		resp["Attributes"] = oldItem
	}
	return resp, nil
}

func HandleUpdateItem(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName                 string                            `json:"TableName"`
		Key                       storage.Item                      `json:"Key"`
		UpdateExpression          string                            `json:"UpdateExpression"`
		ConditionExpression       string                            `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string                 `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]storage.AttributeValue `json:"ExpressionAttributeValues"`
		ReturnValues              string                            `json:"ReturnValues"`
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

	// Parse update expression
	actions, err := expr.ParseUpdateExpression(req.UpdateExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}

	// Get existing item
	existingItem, err := deps.Store.GetItem(ctx, req.TableName, meta, req.Key)
	if err != nil {
		return nil, err
	}

	// Evaluate condition if present
	if req.ConditionExpression != "" {
		ok, err := expr.EvaluateCondition(req.ConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues, existingItem)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("ConditionalCheckFailedException: The conditional request failed")
		}
	}

	// Apply update actions
	var baseItem storage.Item
	if existingItem != nil {
		baseItem = existingItem
	} else {
		baseItem = storage.Item{}
	}

	// Ensure keys are in the base item
	for k, v := range req.Key {
		baseItem[k] = v
	}

	newItem := expr.ApplyUpdateActions(baseItem, actions, req.ExpressionAttributeValues)

	// Ensure keys remain in the new item
	for k, v := range req.Key {
		newItem[k] = v
	}

	returnOld := req.ReturnValues == "ALL_OLD"
	_, err = deps.Store.UpdateItem(ctx, req.TableName, meta, req.Key, newItem, returnOld)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{}
	switch req.ReturnValues {
	case "ALL_OLD":
		if existingItem != nil {
			resp["Attributes"] = existingItem
		}
	case "ALL_NEW":
		resp["Attributes"] = newItem
	case "UPDATED_OLD":
		if existingItem != nil {
			updated := make(storage.Item)
			for _, a := range actions {
				if v, ok := existingItem[a.Path]; ok {
					updated[a.Path] = v
				}
			}
			resp["Attributes"] = updated
		}
	case "UPDATED_NEW":
		updated := make(storage.Item)
		for _, a := range actions {
			if v, ok := newItem[a.Path]; ok {
				updated[a.Path] = v
			}
		}
		resp["Attributes"] = updated
	}

	return resp, nil
}

func extractKeyFromItem(item storage.Item, meta *storage.TableMeta) storage.Item {
	key := storage.Item{}
	if v, ok := item[meta.PKName]; ok {
		key[meta.PKName] = v
	}
	if meta.SKName != "" {
		if v, ok := item[meta.SKName]; ok {
			key[meta.SKName] = v
		}
	}
	return key
}

func parseProjectionExpression(expression string, names map[string]string) []string {
	resolved := expr.ResolveNames(expression, names)
	parts := strings.Split(resolved, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
