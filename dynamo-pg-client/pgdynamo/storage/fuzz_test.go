package storage

import (
	"strings"
	"testing"
)

// FuzzValidateIdentifier tests the validateIdentifier function with random inputs.
func FuzzValidateIdentifier(f *testing.F) {
	f.Add("users")
	f.Add("my-table")
	f.Add("my_table.v2")
	f.Add("Table123")
	f.Add("")
	f.Add("a")
	f.Add("Robert'; DROP TABLE students;--")
	f.Add("table with spaces")
	f.Add("table\ttab")
	f.Add("table\nnewline")
	f.Add("表")
	f.Add("aaaaaaaaaaaaaaaaaaaaaa")
	f.Add("a\"b")
	f.Add("a'b")
	f.Add("a;b")
	f.Add("a(b)")
	f.Add("--comment")
	f.Add("/* block */")
	f.Add(strings.Repeat("a", 256))

	f.Fuzz(func(t *testing.T, name string) {
		err := validateIdentifier(name)

		if err == nil {
			if name == "" {
				t.Error("empty string should not pass validation")
			}
			if len(name) > 255 {
				t.Error("string > 255 chars should not pass validation")
			}
			for _, ch := range name {
				isAlphaNum := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
				isSafe := ch == '_' || ch == '.' || ch == '-'
				if !isAlphaNum && !isSafe {
					t.Errorf("character %q (%U) should not pass validation", string(ch), ch)
				}
			}
		}

		// Verify known-bad patterns are always rejected
		for _, bad := range []string{"'", "\"", ";", "(", ")", " ", "\t", "\n", "\x00"} {
			if strings.Contains(name, bad) && err == nil {
				t.Errorf("name containing %q should be rejected", bad)
			}
		}
	})
}

// FuzzExtractKeyValue tests key extraction with random attribute values.
func FuzzExtractKeyValue(f *testing.F) {
	f.Add("pk", "S", "hello")
	f.Add("sk", "N", "42")
	f.Add("key", "S", "")
	f.Add("key", "N", "3.14")
	f.Add("key", "B", "base64data")
	f.Add("", "S", "value")
	f.Add("key", "", "value")

	f.Fuzz(func(t *testing.T, attrName, attrType, value string) {
		item := Item{
			attrName: AttributeValue{attrType: value},
		}
		got, err := ExtractKeyValue(item, attrName, attrType)
		if err != nil {
			return
		}
		if got != value {
			t.Errorf("got %q, want %q", got, value)
		}
	})
}

// FuzzProjectItem tests ProjectItem doesn't panic or corrupt data.
func FuzzProjectItem(f *testing.F) {
	f.Add("PK", "SK", "Name,Email", "Name")
	f.Add("ID", "", "Data", "Data")
	f.Add("PK", "SK", "", "")

	f.Fuzz(func(t *testing.T, pkName, skName, projectionStr, checkAttr string) {
		item := Item{
			"PK":    {"S": "pk-val"},
			"SK":    {"S": "sk-val"},
			"Name":  {"S": "Alice"},
			"Email": {"S": "a@b.com"},
			"Phone": {"S": "555"},
		}

		var projections []string
		if projectionStr != "" {
			projections = strings.Split(projectionStr, ",")
		}

		result := ProjectItem(item, projections, pkName, skName)

		// Keys should always be included when projections are specified
		if len(projections) > 0 {
			if pkName != "" {
				if _, ok := item[pkName]; ok {
					if _, ok := result[pkName]; !ok {
						t.Errorf("pk %q missing from projected result", pkName)
					}
				}
			}
			if skName != "" {
				if _, ok := item[skName]; ok {
					if _, ok := result[skName]; !ok {
						t.Errorf("sk %q missing from projected result", skName)
					}
				}
			}
		}

		// With no projections, all items should be returned
		if len(projections) == 0 {
			if len(result) != len(item) {
				t.Errorf("no projections: expected %d attrs, got %d", len(item), len(result))
			}
		}
	})
}

func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple", "users", false},
		{"valid with dash", "my-table", false},
		{"valid with underscore", "my_table", false},
		{"valid with dot", "my.table", false},
		{"valid with numbers", "table123", false},
		{"valid mixed", "My-Table_v2.0", false},
		{"empty", "", true},
		{"space", "table name", true},
		{"semicolon", "table;drop", true},
		{"single quote", "table'name", true},
		{"double quote", "table\"name", true},
		{"paren open", "table(name", true},
		{"paren close", "table)name", true},
		{"newline", "table\nname", true},
		{"tab", "table\tname", true},
		{"null byte", "table\x00name", true},
		{"backslash", "table\\name", true},
		{"sql injection", "'; DROP TABLE users; --", true},
		{"unicode", "表", true},
		{"too long", strings.Repeat("a", 256), true},
		{"max length", strings.Repeat("a", 255), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentifier(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateIdentifier(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestBuildKeyAttributeValue(t *testing.T) {
	tests := []struct {
		value    string
		attrType string
	}{
		{"hello", "S"},
		{"42", "N"},
		{"base64==", "B"},
		{"", "S"},
	}
	for _, tt := range tests {
		av := BuildKeyAttributeValue(tt.value, tt.attrType)
		if av[tt.attrType] != tt.value {
			t.Errorf("BuildKeyAttributeValue(%q, %q) = %v, want %q", tt.value, tt.attrType, av, tt.value)
		}
	}
}

func TestProjectItemKeysAlwaysIncluded(t *testing.T) {
	item := Item{
		"PK":    {"S": "pk1"},
		"SK":    {"S": "sk1"},
		"Name":  {"S": "Alice"},
		"Email": {"S": "a@b.com"},
		"Phone": {"S": "555"},
	}

	result := ProjectItem(item, []string{"Name"}, "PK", "SK")

	if _, ok := result["PK"]; !ok {
		t.Error("PK should always be included")
	}
	if _, ok := result["SK"]; !ok {
		t.Error("SK should always be included")
	}
	if _, ok := result["Name"]; !ok {
		t.Error("Name should be in projection")
	}
	if _, ok := result["Email"]; ok {
		t.Error("Email should NOT be in projection")
	}
	if _, ok := result["Phone"]; ok {
		t.Error("Phone should NOT be in projection")
	}
}

func TestProjectItemNoProjection(t *testing.T) {
	item := Item{
		"PK":   {"S": "pk1"},
		"SK":   {"S": "sk1"},
		"Name": {"S": "Alice"},
	}

	result := ProjectItem(item, nil, "PK", "SK")
	if len(result) != len(item) {
		t.Errorf("expected all %d attrs, got %d", len(item), len(result))
	}
}

func TestExtractKeyValueMissingAttr(t *testing.T) {
	item := Item{"Name": {"S": "Alice"}}
	_, err := ExtractKeyValue(item, "PK", "S")
	if err == nil {
		t.Error("expected error for missing attribute")
	}
}

func TestExtractKeyValueWrongType(t *testing.T) {
	item := Item{"PK": {"N": "42"}}
	_, err := ExtractKeyValue(item, "PK", "S")
	if err == nil {
		t.Error("expected error for wrong type")
	}
}
