package storage

import (
	json "github.com/goccy/go-json"
	"fmt"
)

// Item represents a DynamoDB item as a map of attribute name → typed value.
// Example: {"Name": {"S": "Alice"}, "Age": {"N": "30"}}
type Item map[string]AttributeValue

// AttributeValue is the DynamoDB typed JSON wrapper.
// Only one key should be set (S, N, B, BOOL, NULL, L, M, SS, NS, BS).
type AttributeValue map[string]any

// ExtractKeyValue pulls the scalar string representation of a key attribute from an item.
// For S: returns the string directly.
// For N: returns the numeric string.
// For B: returns the base64-encoded string.
func ExtractKeyValue(item Item, attrName, attrType string) (string, error) {
	av, ok := item[attrName]
	if !ok {
		return "", fmt.Errorf("missing key attribute %q", attrName)
	}
	raw, ok := av[attrType]
	if !ok {
		return "", fmt.Errorf("attribute %q is not of type %s", attrName, attrType)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("attribute %q type %s value is not a string", attrName, attrType)
	}
	return s, nil
}

// BuildKeyAttributeValue constructs a DynamoDB typed attribute value for a key.
func BuildKeyAttributeValue(value, attrType string) AttributeValue {
	return AttributeValue{attrType: value}
}

// ItemFromJSON unmarshals a JSON object into an Item.
func ItemFromJSON(data json.RawMessage) (Item, error) {
	var item Item
	if err := json.Unmarshal(data, &item); err != nil {
		return nil, fmt.Errorf("unmarshal item: %w", err)
	}
	return item, nil
}

// ItemToJSON marshals an Item to JSON bytes.
func ItemToJSON(item Item) ([]byte, error) {
	return json.Marshal(item)
}

// ProjectItem returns a new Item containing only the specified attribute names.
// Key attributes (pk/sk) are always included.
func ProjectItem(item Item, projections []string, pkName, skName string) Item {
	if len(projections) == 0 {
		return item
	}
	wanted := make(map[string]bool, len(projections)+2)
	wanted[pkName] = true
	if skName != "" {
		wanted[skName] = true
	}
	for _, p := range projections {
		wanted[p] = true
	}
	result := make(Item, len(wanted))
	for k, v := range item {
		if wanted[k] {
			result[k] = v
		}
	}
	return result
}
