package operations

import (
	"context"
	json "github.com/goccy/go-json"
	"fmt"
	"time"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

// Deps provides shared dependencies to operation handlers.
type Deps struct {
	Store *storage.Store
}

type keySchemaEntry struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"`
}

type attrDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"`
}

type gsiInput struct {
	IndexName  string           `json:"IndexName"`
	KeySchema  []keySchemaEntry `json:"KeySchema"`
	Projection struct {
		ProjectionType   string   `json:"ProjectionType"`
		NonKeyAttributes []string `json:"NonKeyAttributes"`
	} `json:"Projection"`
}

func HandleCreateTable(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName                string           `json:"TableName"`
		KeySchema                []keySchemaEntry  `json:"KeySchema"`
		AttributeDefinitions     []attrDefinition  `json:"AttributeDefinitions"`
		GlobalSecondaryIndexes   []gsiInput        `json:"GlobalSecondaryIndexes"`
		LocalSecondaryIndexes    []gsiInput        `json:"LocalSecondaryIndexes"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}

	if req.TableName == "" {
		return nil, fmt.Errorf("ValidationException: TableName is required")
	}

	// Build attribute type lookup
	attrTypes := make(map[string]string)
	for _, ad := range req.AttributeDefinitions {
		attrTypes[ad.AttributeName] = ad.AttributeType
	}

	meta := storage.TableMeta{
		TableName:   req.TableName,
		TableStatus: "ACTIVE",
	}

	for _, ks := range req.KeySchema {
		switch ks.KeyType {
		case "HASH":
			meta.PKName = ks.AttributeName
			meta.PKType = attrTypes[ks.AttributeName]
		case "RANGE":
			meta.SKName = ks.AttributeName
			meta.SKType = attrTypes[ks.AttributeName]
		}
	}

	if meta.PKName == "" || meta.PKType == "" {
		return nil, fmt.Errorf("ValidationException: HASH key is required")
	}

	// Parse GSIs
	for _, gsi := range req.GlobalSecondaryIndexes {
		idx := storage.IndexMeta{
			IndexName:      gsi.IndexName,
			IndexType:      "GSI",
			ProjectionType: gsi.Projection.ProjectionType,
			NonKeyAttrs:    gsi.Projection.NonKeyAttributes,
		}
		if idx.ProjectionType == "" {
			idx.ProjectionType = "ALL"
		}
		for _, ks := range gsi.KeySchema {
			switch ks.KeyType {
			case "HASH":
				idx.PKName = ks.AttributeName
				idx.PKType = attrTypes[ks.AttributeName]
			case "RANGE":
				idx.SKName = ks.AttributeName
				idx.SKType = attrTypes[ks.AttributeName]
			}
		}
		meta.GSIs = append(meta.GSIs, idx)
	}

	// Parse LSIs
	for _, lsi := range req.LocalSecondaryIndexes {
		idx := storage.IndexMeta{
			IndexName:      lsi.IndexName,
			IndexType:      "LSI",
			ProjectionType: lsi.Projection.ProjectionType,
			NonKeyAttrs:    lsi.Projection.NonKeyAttributes,
		}
		if idx.ProjectionType == "" {
			idx.ProjectionType = "ALL"
		}
		for _, ks := range lsi.KeySchema {
			switch ks.KeyType {
			case "HASH":
				idx.PKName = ks.AttributeName
				idx.PKType = attrTypes[ks.AttributeName]
			case "RANGE":
				idx.SKName = ks.AttributeName
				idx.SKType = attrTypes[ks.AttributeName]
			}
		}
		meta.LSIs = append(meta.LSIs, idx)
	}

	if err := deps.Store.CreateTable(ctx, meta); err != nil {
		if contains(err.Error(), "already exists") {
			return nil, fmt.Errorf("ResourceInUseException: Table already exists: %s", req.TableName)
		}
		return nil, err
	}

	// Build response
	keySchema := buildKeySchema(meta.PKName, meta.SKName)
	now := float64(time.Now().Unix())
	tableDesc := map[string]any{
		"TableName":              req.TableName,
		"TableStatus":            "ACTIVE",
		"KeySchema":              keySchema,
		"AttributeDefinitions":   req.AttributeDefinitions,
		"TableArn":               "arn:aws:dynamodb:local:000000000000:table/" + req.TableName,
		"CreationDateTime":       now,
		"ItemCount":              0,
		"TableSizeBytes":         0,
		"ProvisionedThroughput": map[string]any{
			"ReadCapacityUnits":  5,
			"WriteCapacityUnits": 5,
		},
	}

	if len(meta.GSIs) > 0 {
		tableDesc["GlobalSecondaryIndexes"] = buildGSIDescriptions(meta.GSIs, req.TableName, attrTypes)
	}
	if len(meta.LSIs) > 0 {
		tableDesc["LocalSecondaryIndexes"] = buildLSIDescriptions(meta.LSIs, attrTypes)
	}

	return map[string]any{"TableDescription": tableDesc}, nil
}

func HandleDeleteTable(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName string `json:"TableName"`
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

	if err := deps.Store.DeleteTable(ctx, req.TableName); err != nil {
		return nil, err
	}

	tableDesc := map[string]any{
		"TableName":            req.TableName,
		"TableStatus":          "DELETING",
		"KeySchema":            buildKeySchema(meta.PKName, meta.SKName),
		"AttributeDefinitions": buildAttrDefs(meta),
		"TableArn":             "arn:aws:dynamodb:local:000000000000:table/" + req.TableName,
		"ItemCount":            0,
		"TableSizeBytes":       0,
	}
	return map[string]any{"TableDescription": tableDesc}, nil
}

func HandleDescribeTable(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName string `json:"TableName"`
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

	attrTypes := buildAttrTypeMap(meta)
	tableDesc := map[string]any{
		"TableName":              req.TableName,
		"TableStatus":            meta.TableStatus,
		"KeySchema":              buildKeySchema(meta.PKName, meta.SKName),
		"AttributeDefinitions":   buildAttrDefs(meta),
		"TableArn":               "arn:aws:dynamodb:local:000000000000:table/" + req.TableName,
		"CreationDateTime":       meta.CreatedAt,
		"ItemCount":              0,
		"TableSizeBytes":         0,
		"ProvisionedThroughput": map[string]any{
			"ReadCapacityUnits":  5,
			"WriteCapacityUnits": 5,
		},
	}

	if len(meta.GSIs) > 0 {
		tableDesc["GlobalSecondaryIndexes"] = buildGSIDescriptions(meta.GSIs, req.TableName, attrTypes)
	}
	if len(meta.LSIs) > 0 {
		tableDesc["LocalSecondaryIndexes"] = buildLSIDescriptions(meta.LSIs, attrTypes)
	}

	return map[string]any{"Table": tableDesc}, nil
}

func HandleListTables(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		ExclusiveStartTableName string `json:"ExclusiveStartTableName"`
		Limit                   int    `json:"Limit"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}

	names, lastEval, err := deps.Store.ListTables(ctx, req.ExclusiveStartTableName, req.Limit)
	if err != nil {
		return nil, err
	}
	if names == nil {
		names = []string{}
	}

	resp := map[string]any{
		"TableNames": names,
	}
	if lastEval != "" {
		resp["LastEvaluatedTableName"] = lastEval
	}
	return resp, nil
}

// HandleUpdateTable is a stub that returns current table description.
func HandleUpdateTable(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName string `json:"TableName"`
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
	attrTypes := buildAttrTypeMap(meta)
	tableDesc := map[string]any{
		"TableName":   req.TableName,
		"TableStatus": meta.TableStatus,
		"KeySchema":   buildKeySchema(meta.PKName, meta.SKName),
		"TableArn":    "arn:aws:dynamodb:local:000000000000:table/" + req.TableName,
	}
	if len(meta.GSIs) > 0 {
		tableDesc["GlobalSecondaryIndexes"] = buildGSIDescriptions(meta.GSIs, req.TableName, attrTypes)
	}
	return map[string]any{"TableDescription": tableDesc}, nil
}

// HandleDescribeTimeToLive returns a stub TTL description (disabled).
func HandleDescribeTimeToLive(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}
	return map[string]any{
		"TimeToLiveDescription": map[string]any{
			"TimeToLiveStatus": "DISABLED",
		},
	}, nil
}

// HandleUpdateTimeToLive is a stub that acknowledges the request.
func HandleUpdateTimeToLive(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName               string `json:"TableName"`
		TimeToLiveSpecification struct {
			AttributeName string `json:"AttributeName"`
			Enabled       bool   `json:"Enabled"`
		} `json:"TimeToLiveSpecification"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}
	status := "DISABLED"
	if req.TimeToLiveSpecification.Enabled {
		status = "ENABLED"
	}
	return map[string]any{
		"TimeToLiveSpecification": map[string]any{
			"AttributeName": req.TimeToLiveSpecification.AttributeName,
			"Enabled":       req.TimeToLiveSpecification.Enabled,
			"TimeToLiveStatus": status,
		},
	}, nil
}

// HandleDescribeContinuousBackups returns a stub backup description.
func HandleDescribeContinuousBackups(ctx context.Context, deps *Deps, body json.RawMessage) (any, error) {
	var req struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("ValidationException: %w", err)
	}
	return map[string]any{
		"ContinuousBackupsDescription": map[string]any{
			"ContinuousBackupsStatus": "DISABLED",
			"PointInTimeRecoveryDescription": map[string]any{
				"PointInTimeRecoveryStatus": "DISABLED",
			},
		},
	}, nil
}

// --- Helper functions ---

func buildKeySchema(pkName, skName string) []map[string]string {
	ks := []map[string]string{
		{"AttributeName": pkName, "KeyType": "HASH"},
	}
	if skName != "" {
		ks = append(ks, map[string]string{"AttributeName": skName, "KeyType": "RANGE"})
	}
	return ks
}

func buildAttrDefs(meta *storage.TableMeta) []map[string]string {
	defs := []map[string]string{
		{"AttributeName": meta.PKName, "AttributeType": meta.PKType},
	}
	if meta.SKName != "" {
		defs = append(defs, map[string]string{"AttributeName": meta.SKName, "AttributeType": meta.SKType})
	}
	// Include GSI/LSI key attributes
	seen := map[string]bool{meta.PKName: true, meta.SKName: true}
	for _, idx := range append(meta.GSIs, meta.LSIs...) {
		if !seen[idx.PKName] && idx.PKType != "" {
			defs = append(defs, map[string]string{"AttributeName": idx.PKName, "AttributeType": idx.PKType})
			seen[idx.PKName] = true
		}
		if idx.SKName != "" && !seen[idx.SKName] && idx.SKType != "" {
			defs = append(defs, map[string]string{"AttributeName": idx.SKName, "AttributeType": idx.SKType})
			seen[idx.SKName] = true
		}
	}
	return defs
}

func buildAttrTypeMap(meta *storage.TableMeta) map[string]string {
	m := map[string]string{meta.PKName: meta.PKType}
	if meta.SKName != "" {
		m[meta.SKName] = meta.SKType
	}
	for _, idx := range append(meta.GSIs, meta.LSIs...) {
		m[idx.PKName] = idx.PKType
		if idx.SKName != "" {
			m[idx.SKName] = idx.SKType
		}
	}
	return m
}

func buildGSIDescriptions(gsis []storage.IndexMeta, tableName string, attrTypes map[string]string) []map[string]any {
	result := make([]map[string]any, 0, len(gsis))
	for _, gsi := range gsis {
		desc := map[string]any{
			"IndexName":      gsi.IndexName,
			"IndexStatus":    "ACTIVE",
			"KeySchema":      buildKeySchema(gsi.PKName, gsi.SKName),
			"IndexArn":       "arn:aws:dynamodb:local:000000000000:table/" + tableName + "/index/" + gsi.IndexName,
			"ItemCount":      0,
			"IndexSizeBytes": 0,
			"Projection": map[string]any{
				"ProjectionType": gsi.ProjectionType,
			},
			"ProvisionedThroughput": map[string]any{
				"ReadCapacityUnits":  5,
				"WriteCapacityUnits": 5,
			},
		}
		if len(gsi.NonKeyAttrs) > 0 {
			desc["Projection"].(map[string]any)["NonKeyAttributes"] = gsi.NonKeyAttrs
		}
		result = append(result, desc)
	}
	return result
}

func buildLSIDescriptions(lsis []storage.IndexMeta, attrTypes map[string]string) []map[string]any {
	result := make([]map[string]any, 0, len(lsis))
	for _, lsi := range lsis {
		desc := map[string]any{
			"IndexName":      lsi.IndexName,
			"KeySchema":      buildKeySchema(lsi.PKName, lsi.SKName),
			"ItemCount":      0,
			"IndexSizeBytes": 0,
			"Projection": map[string]any{
				"ProjectionType": lsi.ProjectionType,
			},
		}
		if len(lsi.NonKeyAttrs) > 0 {
			desc["Projection"].(map[string]any)["NonKeyAttributes"] = lsi.NonKeyAttrs
		}
		result = append(result, desc)
	}
	return result
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
