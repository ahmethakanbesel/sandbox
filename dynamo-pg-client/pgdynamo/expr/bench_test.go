package expr

import (
	"testing"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

func BenchmarkParseKeyConditionSimple(b *testing.B) {
	values := map[string]storage.AttributeValue{
		":pk": {"S": "user#1"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseKeyConditionExpression("PK = :pk", nil, values, "PK", "", "S", "")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseKeyConditionWithSK(b *testing.B) {
	values := map[string]storage.AttributeValue{
		":pk":     {"S": "user#1"},
		":prefix": {"S": "event#"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseKeyConditionExpression(
			"PK = :pk AND begins_with(SK, :prefix)",
			nil, values, "PK", "SK", "S", "S",
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseKeyConditionBETWEEN(b *testing.B) {
	values := map[string]storage.AttributeValue{
		":pk": {"S": "user#1"},
		":lo": {"S": "a"},
		":hi": {"S": "z"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseKeyConditionExpression(
			"PK = :pk AND SK BETWEEN :lo AND :hi",
			nil, values, "PK", "SK", "S", "S",
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFilterFuncSimple(b *testing.B) {
	values := map[string]storage.AttributeValue{
		":status": {"S": "active"},
	}
	item := storage.Item{
		"Status": {"S": "active"},
	}
	result, err := ParseFilterExpression("Status = :status", nil, values)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result.Func(item)
	}
}

func BenchmarkFilterFuncComplex(b *testing.B) {
	values := map[string]storage.AttributeValue{
		":status": {"S": "active"},
		":age":    {"N": "18"},
	}
	item := storage.Item{
		"Status": {"S": "active"},
		"Age":    {"N": "25"},
		"Name":   {"S": "Alice"},
	}
	result, err := ParseFilterExpression(
		"Status = :status AND Age > :age AND attribute_exists(Name)",
		nil, values,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result.Func(item)
	}
}

func BenchmarkParseUpdateExpression(b *testing.B) {
	values := map[string]storage.AttributeValue{
		":name": {"S": "Bob"},
		":age":  {"N": "31"},
		":inc":  {"N": "1"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseUpdateExpression(
			"SET #n = :name, Age = :age, Counter = Counter + :inc REMOVE OldField",
			map[string]string{"#n": "Name"},
			values,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkApplyUpdateActions(b *testing.B) {
	values := map[string]storage.AttributeValue{
		":name": {"S": "Bob"},
		":inc":  {"N": "1"},
	}
	actions, err := ParseUpdateExpression(
		"SET Name = :name, Counter = Counter + :inc",
		nil, values,
	)
	if err != nil {
		b.Fatal(err)
	}
	item := storage.Item{
		"PK":      {"S": "pk1"},
		"SK":      {"S": "sk1"},
		"Name":    {"S": "Alice"},
		"Counter": {"N": "10"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ApplyUpdateActions(item, actions, values)
	}
}

func BenchmarkResolveNames(b *testing.B) {
	names := map[string]string{
		"#n": "Name",
		"#s": "Status",
		"#a": "Age",
	}
	expression := "#n = :name AND #s = :status AND #a > :age"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ResolveNames(expression, names)
	}
}

func BenchmarkEvaluateCondition(b *testing.B) {
	values := map[string]storage.AttributeValue{
		":status": {"S": "active"},
	}
	item := storage.Item{
		"Status": {"S": "active"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EvaluateCondition("Status = :status", nil, values, item)
	}
}
