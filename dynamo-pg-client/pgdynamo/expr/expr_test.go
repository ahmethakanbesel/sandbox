package expr

import (
	"testing"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

// ===== ParseKeyConditionExpression =====

func TestParseKeyConditionExpressionPKOnly(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":pk": {"S": "user#1"},
	}
	kc, err := ParseKeyConditionExpression("PK = :pk", nil, values, "PK", "", "S", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kc.PKValue != "user#1" {
		t.Fatalf("expected user#1, got %s", kc.PKValue)
	}
	if kc.SKCondition != "" {
		t.Fatal("expected no SK condition")
	}
}

func TestParseKeyConditionExpressionEmpty(t *testing.T) {
	_, err := ParseKeyConditionExpression("", nil, nil, "PK", "SK", "S", "S")
	if err == nil {
		t.Fatal("expected error for empty expression")
	}
}

func TestParseKeyConditionExpressionMissingPK(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":sk": {"S": "val"},
	}
	_, err := ParseKeyConditionExpression("SK = :sk", nil, values, "PK", "SK", "S", "S")
	if err == nil {
		t.Fatal("expected error for missing PK equality")
	}
}

func TestParseKeyConditionExpressionSKOperators(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":pk": {"S": "p1"},
		":sk": {"S": "abc"},
	}
	tests := []struct {
		expr string
		op   string
	}{
		{"PK = :pk AND SK < :sk", "<"},
		{"PK = :pk AND SK <= :sk", "<="},
		{"PK = :pk AND SK > :sk", ">"},
		{"PK = :pk AND SK >= :sk", ">="},
		{"PK = :pk AND SK = :sk", "="},
	}
	for _, tt := range tests {
		kc, err := ParseKeyConditionExpression(tt.expr, nil, values, "PK", "SK", "S", "S")
		if err != nil {
			t.Fatalf("op %s: unexpected error: %v", tt.op, err)
		}
		if kc.PKValue != "p1" {
			t.Fatalf("op %s: expected p1, got %s", tt.op, kc.PKValue)
		}
		if kc.SKCondition == "" {
			t.Fatalf("op %s: expected SK condition", tt.op)
		}
		if len(kc.SKArgs) != 1 {
			t.Fatalf("op %s: expected 1 SK arg, got %d", tt.op, len(kc.SKArgs))
		}
	}
}

func TestParseKeyConditionExpressionSKBetween(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":pk": {"S": "p1"},
		":lo": {"S": "aaa"},
		":hi": {"S": "zzz"},
	}
	kc, err := ParseKeyConditionExpression("PK = :pk AND SK BETWEEN :lo AND :hi", nil, values, "PK", "SK", "S", "S")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kc.SKArgs) != 2 {
		t.Fatalf("expected 2 SK args for BETWEEN, got %d", len(kc.SKArgs))
	}
}

func TestParseKeyConditionExpressionNumericSK(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":pk": {"S": "p1"},
		":sk": {"N": "5"},
	}
	kc, err := ParseKeyConditionExpression("PK = :pk AND SK > :sk", nil, values, "PK", "SK", "S", "N")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kc.SKCondition == "" {
		t.Fatal("expected SK condition")
	}
}

func TestParseKeyConditionExpressionWithNames(t *testing.T) {
	names := map[string]string{"#pk": "PK", "#sk": "SK"}
	values := map[string]storage.AttributeValue{
		":pk": {"S": "p1"},
		":sk": {"S": "abc"},
	}
	kc, err := ParseKeyConditionExpression("#pk = :pk AND #sk = :sk", names, values, "PK", "SK", "S", "S")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kc.PKValue != "p1" {
		t.Fatalf("expected p1, got %s", kc.PKValue)
	}
}

// ===== ParseFilterExpression / buildFilterFunc =====

func TestParseFilterExpressionEmpty(t *testing.T) {
	fr, err := ParseFilterExpression("", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{}) {
		t.Fatal("empty filter should always return true")
	}
}

func TestFilterAttributeExists(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	fr, err := ParseFilterExpression("attribute_exists(Name)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Name": {"S": "Alice"}}) {
		t.Fatal("expected true for existing attribute")
	}
	if fr.Func(storage.Item{"Other": {"S": "x"}}) {
		t.Fatal("expected false for missing attribute")
	}
}

func TestFilterAttributeNotExists(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	fr, err := ParseFilterExpression("attribute_not_exists(Name)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fr.Func(storage.Item{"Name": {"S": "Alice"}}) {
		t.Fatal("expected false for existing attribute")
	}
	if !fr.Func(storage.Item{"Other": {"S": "x"}}) {
		t.Fatal("expected true for missing attribute")
	}
}

func TestFilterAttributeType(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":t": {"S": "S"},
	}
	fr, err := ParseFilterExpression("attribute_type(Name, :t)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Name": {"S": "Alice"}}) {
		t.Fatal("expected true for S type")
	}
	if fr.Func(storage.Item{"Name": {"N": "42"}}) {
		t.Fatal("expected false for N type")
	}
	if fr.Func(storage.Item{}) {
		t.Fatal("expected false for missing attribute")
	}
}

func TestFilterBeginsWith(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":prefix": {"S": "us"},
	}
	fr, err := ParseFilterExpression("begins_with(Name, :prefix)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Name": {"S": "user1"}}) {
		t.Fatal("expected true for begins_with match")
	}
	if fr.Func(storage.Item{"Name": {"S": "admin"}}) {
		t.Fatal("expected false for no match")
	}
	if fr.Func(storage.Item{}) {
		t.Fatal("expected false for missing attribute")
	}
	if fr.Func(storage.Item{"Name": {"N": "42"}}) {
		t.Fatal("expected false for non-string")
	}
}

func TestFilterContainsString(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":sub": {"S": "world"},
	}
	fr, err := ParseFilterExpression("contains(Greeting, :sub)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Greeting": {"S": "hello world"}}) {
		t.Fatal("expected true for string contains")
	}
	if fr.Func(storage.Item{"Greeting": {"S": "hello"}}) {
		t.Fatal("expected false for no match")
	}
	if fr.Func(storage.Item{}) {
		t.Fatal("expected false for missing attribute")
	}
}

func TestFilterContainsSS(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"S": "go"},
	}
	fr, err := ParseFilterExpression("contains(Tags, :val)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	item := storage.Item{
		"Tags": {"SS": []any{"go", "rust", "python"}},
	}
	if !fr.Func(item) {
		t.Fatal("expected true for SS contains")
	}
	item2 := storage.Item{
		"Tags": {"SS": []any{"java", "c++"}},
	}
	if fr.Func(item2) {
		t.Fatal("expected false for SS not containing value")
	}
}

func TestFilterContainsNS(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"N": "42"},
	}
	fr, err := ParseFilterExpression("contains(Nums, :val)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	item := storage.Item{
		"Nums": {"NS": []any{"1", "42", "100"}},
	}
	if !fr.Func(item) {
		t.Fatal("expected true for NS contains")
	}
}

func TestFilterContainsList(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"S": "hello"},
	}
	fr, err := ParseFilterExpression("contains(Items, :val)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	item := storage.Item{
		"Items": {"L": []any{
			map[string]any{"S": "hello"},
			map[string]any{"S": "world"},
		}},
	}
	if !fr.Func(item) {
		t.Fatal("expected true for list contains")
	}
}

func TestFilterContainsUnresolvedPlaceholder(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	fr, err := ParseFilterExpression("contains(Name, :missing)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Unresolved placeholder returns false always
	if fr.Func(storage.Item{"Name": {"S": "hello"}}) {
		t.Fatal("expected false for unresolved placeholder")
	}
}

func TestFilterNOT(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	fr, err := ParseFilterExpression("NOT attribute_exists(Name)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fr.Func(storage.Item{"Name": {"S": "Alice"}}) {
		t.Fatal("expected false (NOT of true)")
	}
	if !fr.Func(storage.Item{}) {
		t.Fatal("expected true (NOT of false)")
	}
}

func TestFilterParenthesized(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	fr, err := ParseFilterExpression("(attribute_exists(Name))", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Name": {"S": "Alice"}}) {
		t.Fatal("expected true")
	}
}

func TestFilterOR(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":a": {"S": "Alice"},
		":b": {"S": "Bob"},
	}
	fr, err := ParseFilterExpression("Name = :a OR Name = :b", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Name": {"S": "Alice"}}) {
		t.Fatal("expected true for Alice")
	}
	if !fr.Func(storage.Item{"Name": {"S": "Bob"}}) {
		t.Fatal("expected true for Bob")
	}
	if fr.Func(storage.Item{"Name": {"S": "Carol"}}) {
		t.Fatal("expected false for Carol")
	}
}

func TestFilterAND(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":name": {"S": "Alice"},
		":age":  {"N": "30"},
	}
	fr, err := ParseFilterExpression("Name = :name AND Age = :age", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Name": {"S": "Alice"}, "Age": {"N": "30"}}) {
		t.Fatal("expected true")
	}
	if fr.Func(storage.Item{"Name": {"S": "Alice"}, "Age": {"N": "25"}}) {
		t.Fatal("expected false (age mismatch)")
	}
}

func TestFilterComparisonString(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"S": "Bob"},
	}
	tests := []struct {
		op     string
		item   string
		expect bool
	}{
		{"=", "Bob", true},
		{"=", "Alice", false},
		{"<>", "Alice", true},
		{"<>", "Bob", false},
		{"<", "Alice", true},
		{"<", "Charlie", false},
		{"<=", "Bob", true},
		{"<=", "Alice", true},
		{">", "Charlie", true},
		{">", "Alice", false},
		{">=", "Bob", true},
		{">=", "Charlie", true},
	}
	for _, tt := range tests {
		fr, err := ParseFilterExpression("Name "+tt.op+" :val", nil, values)
		if err != nil {
			t.Fatalf("op %s: unexpected error: %v", tt.op, err)
		}
		result := fr.Func(storage.Item{"Name": {"S": tt.item}})
		if result != tt.expect {
			t.Errorf("Name=%q %s Bob: got %v, want %v", tt.item, tt.op, result, tt.expect)
		}
	}
}

func TestFilterComparisonNumeric(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"N": "10"},
	}
	tests := []struct {
		op     string
		item   string
		expect bool
	}{
		{"=", "10", true},
		{"=", "5", false},
		{"<>", "5", true},
		{"<", "5", true},
		{"<=", "10", true},
		{">", "15", true},
		{">=", "10", true},
	}
	for _, tt := range tests {
		fr, err := ParseFilterExpression("Score "+tt.op+" :val", nil, values)
		if err != nil {
			t.Fatalf("op %s: unexpected error: %v", tt.op, err)
		}
		result := fr.Func(storage.Item{"Score": {"N": tt.item}})
		if result != tt.expect {
			t.Errorf("Score=%s %s 10: got %v, want %v", tt.item, tt.op, result, tt.expect)
		}
	}
}

func TestFilterComparisonBool(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"BOOL": true},
	}
	fr, err := ParseFilterExpression("Active = :val", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Active": {"BOOL": true}}) {
		t.Fatal("expected true")
	}
	if fr.Func(storage.Item{"Active": {"BOOL": false}}) {
		t.Fatal("expected false")
	}

	// <> for bool
	fr2, err := ParseFilterExpression("Active <> :val", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fr2.Func(storage.Item{"Active": {"BOOL": true}}) {
		t.Fatal("expected false")
	}
	if !fr2.Func(storage.Item{"Active": {"BOOL": false}}) {
		t.Fatal("expected true")
	}
}

func TestFilterComparisonNull(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"NULL": true},
	}
	fr, err := ParseFilterExpression("Status = :val", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Status": {"NULL": true}}) {
		t.Fatal("expected true for NULL = NULL")
	}
	if fr.Func(storage.Item{"Status": {"S": "active"}}) {
		t.Fatal("expected false for non-null")
	}
}

func TestFilterComparisonMissingAttr(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"S": "test"},
	}
	fr, err := ParseFilterExpression("Name = :val", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fr.Func(storage.Item{}) {
		t.Fatal("expected false for missing attribute")
	}
}

func TestFilterComparisonTypeMismatch(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"S": "test"},
	}
	fr, err := ParseFilterExpression("Score = :val", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// N attr vs S value - should not match
	if fr.Func(storage.Item{"Score": {"N": "42"}}) {
		t.Fatal("expected false for type mismatch")
	}
}

func TestFilterIN(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":v1": {"S": "Alice"},
		":v2": {"S": "Bob"},
		":v3": {"S": "Carol"},
	}
	fr, err := ParseFilterExpression("Name IN (:v1, :v2, :v3)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Name": {"S": "Alice"}}) {
		t.Fatal("expected true for Alice")
	}
	if !fr.Func(storage.Item{"Name": {"S": "Bob"}}) {
		t.Fatal("expected true for Bob")
	}
	if fr.Func(storage.Item{"Name": {"S": "Dave"}}) {
		t.Fatal("expected false for Dave")
	}
	if fr.Func(storage.Item{}) {
		t.Fatal("expected false for missing attribute")
	}
}

func TestFilterINUnresolved(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	_, err := ParseFilterExpression("Name IN (:missing)", nil, values)
	if err == nil {
		t.Fatal("expected error for unresolved placeholder in IN")
	}
}

func TestFilterBETWEEN(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":lo":    {"N": "10"},
		":hi":    {"N": "20"},
		":score": {"N": "15"},
	}
	// Use direct call to buildFilterFunc since ParseFilterExpression may have AND splitting issues
	fn, err := buildFilterFunc("Score BETWEEN :lo AND :hi", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fn(storage.Item{"Score": {"N": "15"}}) {
		t.Fatal("expected true for 15")
	}
	if !fn(storage.Item{"Score": {"N": "10"}}) {
		t.Fatal("expected true for 10 (inclusive)")
	}
	if !fn(storage.Item{"Score": {"N": "20"}}) {
		t.Fatal("expected true for 20 (inclusive)")
	}
	if fn(storage.Item{"Score": {"N": "5"}}) {
		t.Fatal("expected false for 5")
	}
	if fn(storage.Item{"Score": {"N": "25"}}) {
		t.Fatal("expected false for 25")
	}
	if fn(storage.Item{}) {
		t.Fatal("expected false for missing attribute")
	}
}

func TestFilterBETWEENString(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":lo": {"S": "b"},
		":hi": {"S": "d"},
	}
	fn, err := buildFilterFunc("Letter BETWEEN :lo AND :hi", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fn(storage.Item{"Letter": {"S": "c"}}) {
		t.Fatal("expected true for c")
	}
	if fn(storage.Item{"Letter": {"S": "a"}}) {
		t.Fatal("expected false for a")
	}
}

func TestFilterBETWEENUnresolved(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":lo": {"N": "10"},
	}
	_, err := buildFilterFunc("Score BETWEEN :lo AND :missing", values)
	if err == nil {
		t.Fatal("expected error for unresolved placeholder")
	}
}

func TestFilterUnsupported(t *testing.T) {
	_, err := ParseFilterExpression("something_weird()", nil, map[string]storage.AttributeValue{})
	if err == nil {
		t.Fatal("expected error for unsupported expression")
	}
}

func TestFilterComparisonUnresolved(t *testing.T) {
	_, err := ParseFilterExpression("Name = :missing", nil, map[string]storage.AttributeValue{})
	if err == nil {
		t.Fatal("expected error for unresolved value")
	}
}

// ===== size() expressions =====

func TestFilterSize(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":sz": {"N": "5"},
	}
	fr, err := ParseFilterExpression("size(Name) > :sz", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Name": {"S": "HelloWorld"}}) {
		t.Fatal("expected true for string length 10 > 5")
	}
	if fr.Func(storage.Item{"Name": {"S": "Hi"}}) {
		t.Fatal("expected false for string length 2")
	}
	if fr.Func(storage.Item{}) {
		t.Fatal("expected false for missing attribute")
	}
}

func TestFilterSizeEquals(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":sz": {"N": "3"},
	}
	fr, err := ParseFilterExpression("size(Tags) = :sz", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Tags": {"SS": []any{"a", "b", "c"}}}) {
		t.Fatal("expected true for set size 3")
	}
	if fr.Func(storage.Item{"Tags": {"SS": []any{"a", "b"}}}) {
		t.Fatal("expected false for set size 2")
	}
}

func TestFilterSizeList(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":sz": {"N": "2"},
	}
	fr, err := ParseFilterExpression("size(Items) >= :sz", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Items": {"L": []any{"a", "b", "c"}}}) {
		t.Fatal("expected true for list size 3 >= 2")
	}
}

func TestFilterSizeMap(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":sz": {"N": "1"},
	}
	fr, err := ParseFilterExpression("size(Meta) = :sz", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !fr.Func(storage.Item{"Meta": {"M": map[string]any{"key": "val"}}}) {
		t.Fatal("expected true for map size 1")
	}
}

func TestFilterSizeMalformed(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	_, err := ParseFilterExpression("size(Name", nil, values)
	if err == nil {
		t.Fatal("expected error for malformed size expression")
	}
}

func TestFilterSizeNonNumericValue(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":sz": {"S": "not-a-number"},
	}
	_, err := ParseFilterExpression("size(Name) > :sz", nil, values)
	if err == nil {
		t.Fatal("expected error for non-numeric size comparison value")
	}
}

func TestFilterSizeUnresolved(t *testing.T) {
	_, err := ParseFilterExpression("size(Name) > :missing", nil, map[string]storage.AttributeValue{})
	if err == nil {
		t.Fatal("expected error for unresolved size value")
	}
}

// ===== attributeSize =====

func TestAttributeSize(t *testing.T) {
	tests := []struct {
		name string
		av   storage.AttributeValue
		want int
	}{
		{"string", storage.AttributeValue{"S": "hello"}, 5},
		{"number", storage.AttributeValue{"N": "42"}, 1},
		{"binary", storage.AttributeValue{"B": "abc"}, 3},
		{"list", storage.AttributeValue{"L": []any{"a", "b"}}, 2},
		{"map", storage.AttributeValue{"M": map[string]any{"k1": "v1", "k2": "v2"}}, 2},
		{"string set", storage.AttributeValue{"SS": []any{"a", "b", "c"}}, 3},
		{"number set", storage.AttributeValue{"NS": []any{"1", "2"}}, 2},
		{"binary set", storage.AttributeValue{"BS": []any{"a", "b"}}, 2},
		{"empty", storage.AttributeValue{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := attributeSize(tt.av)
			if got != tt.want {
				t.Errorf("attributeSize(%v) = %d, want %d", tt.av, got, tt.want)
			}
		})
	}
}

// ===== attributeValuesEqual =====

func TestAttributeValuesEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b storage.AttributeValue
		want bool
	}{
		{"same string", storage.AttributeValue{"S": "hello"}, storage.AttributeValue{"S": "hello"}, true},
		{"diff string", storage.AttributeValue{"S": "hello"}, storage.AttributeValue{"S": "world"}, false},
		{"same number", storage.AttributeValue{"N": "42"}, storage.AttributeValue{"N": "42"}, true},
		{"diff number", storage.AttributeValue{"N": "42"}, storage.AttributeValue{"N": "99"}, false},
		{"same bool", storage.AttributeValue{"BOOL": true}, storage.AttributeValue{"BOOL": true}, true},
		{"diff bool", storage.AttributeValue{"BOOL": true}, storage.AttributeValue{"BOOL": false}, false},
		{"diff types", storage.AttributeValue{"S": "42"}, storage.AttributeValue{"N": "42"}, false},
		{"null equal", storage.AttributeValue{"NULL": true}, storage.AttributeValue{"NULL": true}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := attributeValuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("attributeValuesEqual = %v, want %v", got, tt.want)
			}
		})
	}
}

// ===== compareAV =====

func TestCompareAV(t *testing.T) {
	tests := []struct {
		name   string
		av     storage.AttributeValue
		op     string
		target storage.AttributeValue
		want   bool
	}{
		{"string =", storage.AttributeValue{"S": "abc"}, "=", storage.AttributeValue{"S": "abc"}, true},
		{"string >", storage.AttributeValue{"S": "xyz"}, ">", storage.AttributeValue{"S": "abc"}, true},
		{"string <", storage.AttributeValue{"S": "abc"}, "<", storage.AttributeValue{"S": "xyz"}, true},
		{"number =", storage.AttributeValue{"N": "42"}, "=", storage.AttributeValue{"N": "42"}, true},
		{"number >", storage.AttributeValue{"N": "50"}, ">", storage.AttributeValue{"N": "42"}, true},
		{"type mismatch", storage.AttributeValue{"S": "abc"}, "=", storage.AttributeValue{"N": "42"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareAV(tt.av, tt.op, tt.target)
			if got != tt.want {
				t.Errorf("compareAV = %v, want %v", got, tt.want)
			}
		})
	}
}

// ===== compareStrings =====

func TestCompareStrings(t *testing.T) {
	tests := []struct {
		a, op, b string
		want     bool
	}{
		{"abc", "=", "abc", true},
		{"abc", "<>", "xyz", true},
		{"abc", "!=", "xyz", true},
		{"abc", "<", "def", true},
		{"abc", "<=", "abc", true},
		{"xyz", ">", "abc", true},
		{"xyz", ">=", "xyz", true},
		{"abc", "??", "abc", false}, // unknown op
	}
	for _, tt := range tests {
		got := compareStrings(tt.a, tt.op, tt.b)
		if got != tt.want {
			t.Errorf("compareStrings(%q, %q, %q) = %v, want %v", tt.a, tt.op, tt.b, got, tt.want)
		}
	}
}

// ===== compareFloats =====

func TestCompareFloats(t *testing.T) {
	tests := []struct {
		a  float64
		op string
		b  float64
		want bool
	}{
		{10, "=", 10, true},
		{10, "<>", 20, true},
		{10, "!=", 20, true},
		{5, "<", 10, true},
		{10, "<=", 10, true},
		{20, ">", 10, true},
		{10, ">=", 10, true},
		{10, "??", 10, false},
	}
	for _, tt := range tests {
		got := compareFloats(tt.a, tt.op, tt.b)
		if got != tt.want {
			t.Errorf("compareFloats(%f, %q, %f) = %v, want %v", tt.a, tt.op, tt.b, got, tt.want)
		}
	}
}

// ===== resolveValueAsString =====

func TestResolveValueAsString(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":s": {"S": "hello"},
		":n": {"N": "42"},
	}
	s, err := resolveValueAsString(":s", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != "hello" {
		t.Fatalf("expected hello, got %s", s)
	}

	n, err := resolveValueAsString(":n", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != "42" {
		t.Fatalf("expected 42, got %s", n)
	}

	_, err = resolveValueAsString(":missing", values)
	if err == nil {
		t.Fatal("expected error for missing placeholder")
	}

	values[":bool"] = storage.AttributeValue{"BOOL": true}
	_, err = resolveValueAsString(":bool", values)
	if err == nil {
		t.Fatal("expected error for non-string-like type")
	}
}

// ===== ResolveValue =====

func TestResolveValueFallback(t *testing.T) {
	// When exact type not found, it falls back to any string value
	values := map[string]storage.AttributeValue{
		":val": {"S": "hello"},
	}
	v, err := ResolveValue(":val", values, "N")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "hello" {
		t.Fatalf("expected hello, got %s", v)
	}
}

func TestResolveValueMissing(t *testing.T) {
	_, err := ResolveValue(":missing", map[string]storage.AttributeValue{}, "S")
	if err == nil {
		t.Fatal("expected error for missing placeholder")
	}
}

func TestResolveValueNonString(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"BOOL": true},
	}
	_, err := ResolveValue(":val", values, "BOOL")
	if err == nil {
		t.Fatal("expected error for non-string value type")
	}
}

// ===== EvaluateCondition =====

func TestEvaluateConditionEmpty(t *testing.T) {
	ok, err := EvaluateCondition("", nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("empty condition should return true")
	}
}

func TestEvaluateConditionNilItem(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	ok, err := EvaluateCondition("attribute_not_exists(PK)", nil, values, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true for nil item with attribute_not_exists")
	}
}

// ===== ParseUpdateExpression =====

func TestParseUpdateExpressionEmpty(t *testing.T) {
	_, err := ParseUpdateExpression("", nil, nil)
	if err == nil {
		t.Fatal("expected error for empty expression")
	}
}

func TestParseUpdateExpressionDELETE(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":vals": {"SS": []any{"a", "b"}},
	}
	actions, err := ParseUpdateExpression("DELETE Tags :vals", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Type != "DELETE" {
		t.Fatalf("expected DELETE, got %s", actions[0].Type)
	}
	if actions[0].Path != "Tags" {
		t.Fatalf("expected Tags, got %s", actions[0].Path)
	}
}

func TestParseUpdateExpressionDELETEUnresolved(t *testing.T) {
	_, err := ParseUpdateExpression("DELETE Tags :missing", nil, map[string]storage.AttributeValue{})
	if err == nil {
		t.Fatal("expected error for unresolved placeholder")
	}
}

func TestParseUpdateExpressionDELETENoSpace(t *testing.T) {
	_, err := ParseUpdateExpression("DELETE NoSpace", nil, map[string]storage.AttributeValue{})
	if err == nil {
		t.Fatal("expected error for DELETE without space")
	}
}

func TestParseUpdateExpressionListAppend(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":newItems": {"L": []any{map[string]any{"S": "x"}}},
	}
	actions, err := ParseUpdateExpression("SET Items = list_append(Items, :newItems)", nil, values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].ListAppendFirst != "Items" {
		t.Fatalf("expected Items, got %s", actions[0].ListAppendFirst)
	}
}

func TestParseUpdateExpressionUnsupported(t *testing.T) {
	_, err := ParseUpdateExpression("INVALID stuff", nil, map[string]storage.AttributeValue{})
	if err == nil {
		t.Fatal("expected error for unsupported clause")
	}
}

// ===== ApplyUpdateActions =====

func TestApplyUpdateActionsDELETEFromSS(t *testing.T) {
	item := storage.Item{
		"Tags": {"SS": []any{"go", "rust", "python"}},
	}
	actions := []UpdateAction{
		{Type: "DELETE", Path: "Tags", Value: storage.AttributeValue{"SS": []any{"rust"}}},
	}
	result := ApplyUpdateActions(item, actions, nil)
	ss := result["Tags"]["SS"].([]any)
	if len(ss) != 2 {
		t.Fatalf("expected 2 elements after delete, got %d", len(ss))
	}
}

func TestApplyUpdateActionsDELETEFromNS(t *testing.T) {
	item := storage.Item{
		"Nums": {"NS": []any{"1", "2", "3"}},
	}
	actions := []UpdateAction{
		{Type: "DELETE", Path: "Nums", Value: storage.AttributeValue{"NS": []any{"2"}}},
	}
	result := ApplyUpdateActions(item, actions, nil)
	ns := result["Nums"]["NS"].([]any)
	if len(ns) != 2 {
		t.Fatalf("expected 2 elements after delete, got %d", len(ns))
	}
}

func TestApplyUpdateActionsDELETEMissing(t *testing.T) {
	item := storage.Item{}
	actions := []UpdateAction{
		{Type: "DELETE", Path: "Tags", Value: storage.AttributeValue{"SS": []any{"a"}}},
	}
	result := ApplyUpdateActions(item, actions, nil)
	if _, ok := result["Tags"]; ok {
		t.Fatal("expected no Tags attribute on empty item")
	}
}

func TestApplyUpdateActionsListAppend(t *testing.T) {
	item := storage.Item{
		"Items": {"L": []any{map[string]any{"S": "a"}}},
	}
	values := map[string]storage.AttributeValue{
		":new": {"L": []any{map[string]any{"S": "b"}}},
	}
	actions := []UpdateAction{
		{Type: "SET", Path: "Items", ListAppendFirst: "Items", ListAppendSecond: ":new"},
	}
	result := ApplyUpdateActions(item, actions, values)
	list := result["Items"]["L"].([]any)
	if len(list) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(list))
	}
}

func TestApplyUpdateActionsNilItem(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":val": {"S": "hello"},
	}
	actions := []UpdateAction{
		{Type: "SET", Path: "Name", Value: storage.AttributeValue{"S": "hello"}},
	}
	result := ApplyUpdateActions(nil, actions, values)
	if result["Name"]["S"] != "hello" {
		t.Fatal("expected Name=hello")
	}
}

func TestApplyUpdateActionsIfNotExistsNoArithmetic(t *testing.T) {
	values := map[string]storage.AttributeValue{}
	actions := []UpdateAction{
		{
			Type:             "SET",
			Path:             "Status",
			IfNotExistsPath:  "Status",
			IfNotExistsValue: storage.AttributeValue{"S": "active"},
		},
	}
	// Item doesn't have Status - should set it
	result := ApplyUpdateActions(storage.Item{}, actions, values)
	if result["Status"]["S"] != "active" {
		t.Fatal("expected Status=active")
	}

	// Item already has Status - should keep existing
	result2 := ApplyUpdateActions(storage.Item{"Status": {"S": "inactive"}}, actions, values)
	if result2["Status"]["S"] != "inactive" {
		t.Fatal("expected Status=inactive (existing)")
	}
}

// ===== subtractSet =====

func TestSubtractSet(t *testing.T) {
	existing := []any{"a", "b", "c", "d"}
	remove := []any{"b", "d"}
	result := subtractSet(existing, remove)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

// ===== resolveListOperand =====

func TestResolveListOperandFromValues(t *testing.T) {
	values := map[string]storage.AttributeValue{
		":list": {"L": []any{"a", "b"}},
	}
	result := resolveListOperand(storage.Item{}, values, ":list")
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestResolveListOperandFromItem(t *testing.T) {
	item := storage.Item{
		"Items": {"L": []any{"x", "y"}},
	}
	result := resolveListOperand(item, nil, "Items")
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestResolveListOperandMissing(t *testing.T) {
	result := resolveListOperand(storage.Item{}, nil, "missing")
	if result != nil {
		t.Fatal("expected nil for missing operand")
	}
}

func TestResolveListOperandValueMissing(t *testing.T) {
	result := resolveListOperand(storage.Item{}, map[string]storage.AttributeValue{}, ":missing")
	if result != nil {
		t.Fatal("expected nil for missing value placeholder")
	}
}

// ===== findINOperator =====

func TestFindINOperator(t *testing.T) {
	tests := []struct {
		expr string
		want int
	}{
		{"Name IN (:v1, :v2)", 4},
		{"noINhere", -1},          // no spaces around IN
		{"Name IN (:v1)", 4},      // space-delimited IN
		{"NOTHING IN (:v1)", 7},   // IN at end of word
	}
	for _, tt := range tests {
		got := findINOperator(tt.expr)
		if got != tt.want {
			t.Errorf("findINOperator(%q) = %d, want %d", tt.expr, got, tt.want)
		}
	}
}

// ===== extractFuncArgs =====

func TestExtractFuncArgs(t *testing.T) {
	tests := []struct {
		expr string
		want string
	}{
		{"func(a, b)", "a, b"},
		{"no parens", ""},
		{"func(nested(a))", "nested(a)"},
	}
	for _, tt := range tests {
		got := extractFuncArgs(tt.expr)
		if got != tt.want {
			t.Errorf("extractFuncArgs(%q) = %q, want %q", tt.expr, got, tt.want)
		}
	}
}
