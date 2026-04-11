package storage

import (
	"fmt"
	"testing"

	json "github.com/goccy/go-json"
)

func BenchmarkItemToJSON(b *testing.B) {
	item := Item{
		"PK":   {"S": "user#1"},
		"SK":   {"S": "profile"},
		"Name": {"S": "Alice"},
		"Age":  {"N": "30"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ItemToJSON(item)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkItemFromJSON(b *testing.B) {
	raw := json.RawMessage(`{"PK":{"S":"user#1"},"SK":{"S":"profile"},"Name":{"S":"Alice"},"Age":{"N":"30"}}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ItemFromJSON(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkItemToJSONLarge(b *testing.B) {
	item := Item{
		"PK": {"S": "user#1"},
		"SK": {"S": "profile"},
	}
	for j := 0; j < 50; j++ {
		item[fmt.Sprintf("Attr%d", j)] = AttributeValue{
			"S": fmt.Sprintf("value_%d_with_some_padding_to_make_it_longer", j),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ItemToJSON(item)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkItemFromJSONLarge(b *testing.B) {
	item := Item{
		"PK": {"S": "user#1"},
		"SK": {"S": "profile"},
	}
	for j := 0; j < 50; j++ {
		item[fmt.Sprintf("Attr%d", j)] = AttributeValue{
			"S": fmt.Sprintf("value_%d_with_some_padding_to_make_it_longer", j),
		}
	}
	data, _ := ItemToJSON(item)
	raw := json.RawMessage(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ItemFromJSON(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProjectItem(b *testing.B) {
	item := Item{
		"PK":   {"S": "user#1"},
		"SK":   {"S": "profile"},
		"Name": {"S": "Alice"},
		"Age":  {"N": "30"},
		"Bio":  {"S": "Some text here"},
		"City": {"S": "New York"},
	}
	projections := []string{"Name", "Age"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProjectItem(item, projections, "PK", "SK")
	}
}

func BenchmarkExtractKeyValue(b *testing.B) {
	item := Item{
		"PK": {"S": "user#1"},
		"SK": {"S": "profile"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ExtractKeyValue(item, "PK", "S")
		if err != nil {
			b.Fatal(err)
		}
	}
}
