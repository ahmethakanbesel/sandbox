package storage

import (
	json "github.com/goccy/go-json"
	"testing"
)

func TestItemFromJSON(t *testing.T) {
	raw := json.RawMessage(`{"PK":{"S":"pk1"},"SK":{"S":"sk1"},"Name":{"S":"Alice"}}`)
	item, err := ItemFromJSON(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if item["PK"]["S"] != "pk1" {
		t.Fatalf("expected pk1, got %v", item["PK"]["S"])
	}
	if item["Name"]["S"] != "Alice" {
		t.Fatalf("expected Alice, got %v", item["Name"]["S"])
	}
}

func TestItemFromJSONInvalid(t *testing.T) {
	raw := json.RawMessage(`invalid json`)
	_, err := ItemFromJSON(raw)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestItemToJSON(t *testing.T) {
	item := Item{
		"PK":   {"S": "pk1"},
		"Name": {"S": "Alice"},
		"Age":  {"N": "30"},
	}
	data, err := ItemToJSON(item)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Parse back to verify
	var result Item
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result["PK"]["S"] != "pk1" {
		t.Fatalf("expected pk1, got %v", result["PK"]["S"])
	}
}

func TestItemFromJSONRoundTrip(t *testing.T) {
	original := Item{
		"PK":     {"S": "user#1"},
		"SK":     {"S": "profile"},
		"Name":   {"S": "Alice"},
		"Age":    {"N": "30"},
		"Active": {"BOOL": true},
	}
	data, err := ItemToJSON(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	parsed, err := ItemFromJSON(json.RawMessage(data))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed["PK"]["S"] != "user#1" {
		t.Fatalf("expected user#1, got %v", parsed["PK"]["S"])
	}
	if parsed["Name"]["S"] != "Alice" {
		t.Fatalf("expected Alice, got %v", parsed["Name"]["S"])
	}
}

func TestProjectItemEmptySkName(t *testing.T) {
	item := Item{
		"PK":   {"S": "pk1"},
		"Name": {"S": "Alice"},
		"Age":  {"N": "30"},
	}
	result := ProjectItem(item, []string{"Name"}, "PK", "")
	if _, ok := result["PK"]; !ok {
		t.Fatal("PK should always be included")
	}
	if _, ok := result["Name"]; !ok {
		t.Fatal("Name should be in projection")
	}
	if _, ok := result["Age"]; ok {
		t.Fatal("Age should not be in projection")
	}
}

func TestExtractKeyValueNonStringUnderlying(t *testing.T) {
	item := Item{
		"PK": {"S": 42}, // wrong: S should be a string, but underlying is int
	}
	_, err := ExtractKeyValue(item, "PK", "S")
	if err == nil {
		t.Fatal("expected error for non-string underlying value")
	}
}
