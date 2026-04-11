package expr

import (
	"fmt"
	"strings"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

// KeyCondition holds the parsed key condition for a Query.
type KeyCondition struct {
	PKValue     string
	SKCondition string // SQL fragment like "sk = $2" or "sk > $2 AND sk < $3"
	SKArgs      []any
}

// ParseKeyConditionExpression parses a DynamoDB KeyConditionExpression into a KeyCondition.
func ParseKeyConditionExpression(
	expression string,
	names map[string]string,
	values map[string]storage.AttributeValue,
	pkName, skName, pkType, skType string,
) (*KeyCondition, error) {
	if expression == "" {
		return nil, fmt.Errorf("ValidationException: KeyConditionExpression is required")
	}

	expression = ResolveNames(expression, names)
	kc := &KeyCondition{}

	parts := splitOnAND(expression)

	// Rejoin BETWEEN parts: "SK BETWEEN :lo" AND ":hi" should be one part
	if len(parts) == 3 {
		lowerP1 := strings.ToLower(parts[1])
		if strings.Contains(lowerP1, "between") {
			parts = []string{parts[0], parts[1] + " AND " + parts[2]}
		}
	}

	if len(parts) < 1 || len(parts) > 2 {
		return nil, fmt.Errorf("ValidationException: invalid KeyConditionExpression")
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if isPKCondition(part, pkName) {
			val, err := extractEqualityValue(part, values, pkType)
			if err != nil {
				return nil, err
			}
			kc.PKValue = val
		} else if skName != "" {
			cond, args, err := parseSKCondition(part, skName, values, skType)
			if err != nil {
				return nil, err
			}
			kc.SKCondition = cond
			kc.SKArgs = args
		} else {
			return nil, fmt.Errorf("ValidationException: unexpected condition: %s", part)
		}
	}

	if kc.PKValue == "" {
		return nil, fmt.Errorf("ValidationException: KeyConditionExpression must contain partition key equality")
	}

	return kc, nil
}

func isPKCondition(part, pkName string) bool {
	trimmed := strings.TrimSpace(part)
	return strings.HasPrefix(trimmed, pkName+" =") || strings.HasPrefix(trimmed, pkName+"=")
}

func extractEqualityValue(part string, values map[string]storage.AttributeValue, attrType string) (string, error) {
	idx := strings.Index(part, "=")
	if idx < 0 {
		return "", fmt.Errorf("ValidationException: expected equality in: %s", part)
	}
	placeholder := strings.TrimSpace(part[idx+1:])
	return ResolveValue(placeholder, values, attrType)
}

func parseSKCondition(part, skName string, values map[string]storage.AttributeValue, skType string) (string, []any, error) {
	part = strings.TrimSpace(part)

	skCol := "sk"
	if skType == "N" {
		skCol = "CAST(sk AS NUMERIC)"
	}

	// begins_with(sk, :prefix)
	if strings.HasPrefix(strings.ToLower(part), "begins_with") {
		inner := extractFuncArgs(part)
		args := strings.SplitN(inner, ",", 2)
		if len(args) != 2 {
			return "", nil, fmt.Errorf("ValidationException: begins_with requires 2 arguments")
		}
		placeholder := strings.TrimSpace(args[1])
		val, err := ResolveValue(placeholder, values, "S")
		if err != nil {
			return "", nil, err
		}
		return "sk LIKE $2", []any{val + "%"}, nil
	}

	// BETWEEN: sk BETWEEN :a AND :b
	lowerPart := strings.ToLower(part)
	if strings.Contains(lowerPart, " between ") {
		betweenIdx := strings.Index(lowerPart, " between ")
		rest := part[betweenIdx+len(" between "):]
		andParts := splitOnAND(rest)
		if len(andParts) != 2 {
			return "", nil, fmt.Errorf("ValidationException: BETWEEN requires exactly 2 values")
		}
		lo, err := ResolveValue(strings.TrimSpace(andParts[0]), values, skType)
		if err != nil {
			return "", nil, err
		}
		hi, err := ResolveValue(strings.TrimSpace(andParts[1]), values, skType)
		if err != nil {
			return "", nil, err
		}
		cond := fmt.Sprintf("%s BETWEEN $2 AND $3", skCol)
		return cond, []any{lo, hi}, nil
	}

	// Comparison operators: =, <, <=, >, >=
	for _, op := range []string{"<=", ">=", "<>", "<", ">", "="} {
		idx := strings.Index(part, op)
		if idx < 0 {
			continue
		}
		left := strings.TrimSpace(part[:idx])
		if left != skName {
			continue
		}
		placeholder := strings.TrimSpace(part[idx+len(op):])
		val, err := ResolveValue(placeholder, values, skType)
		if err != nil {
			return "", nil, err
		}
		cond := fmt.Sprintf("%s %s $2", skCol, op)
		return cond, []any{val}, nil
	}

	return "", nil, fmt.Errorf("ValidationException: unsupported SK condition: %s", part)
}

// FilterResult holds a Go-evaluable filter function.
type FilterResult struct {
	Func func(item storage.Item) bool
}

// ParseFilterExpression parses a DynamoDB FilterExpression into a Go filter function.
func ParseFilterExpression(
	expression string,
	names map[string]string,
	values map[string]storage.AttributeValue,
) (*FilterResult, error) {
	if expression == "" {
		return &FilterResult{Func: func(storage.Item) bool { return true }}, nil
	}

	expression = ResolveNames(expression, names)
	fn, err := buildFilterFunc(expression, values)
	if err != nil {
		return nil, err
	}
	return &FilterResult{Func: fn}, nil
}

// ParseConditionExpression is identical to ParseFilterExpression — DynamoDB uses the same
// expression syntax for ConditionExpression as for FilterExpression.
func ParseConditionExpression(
	expression string,
	names map[string]string,
	values map[string]storage.AttributeValue,
) (*FilterResult, error) {
	return ParseFilterExpression(expression, names, values)
}

// EvaluateCondition evaluates a condition expression against an item (may be nil for non-existent items).
func EvaluateCondition(
	expression string,
	names map[string]string,
	values map[string]storage.AttributeValue,
	item storage.Item,
) (bool, error) {
	if expression == "" {
		return true, nil
	}
	fr, err := ParseConditionExpression(expression, names, values)
	if err != nil {
		return false, err
	}
	if item == nil {
		item = storage.Item{}
	}
	return fr.Func(item), nil
}

func buildFilterFunc(expression string, values map[string]storage.AttributeValue) (func(storage.Item) bool, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return func(storage.Item) bool { return true }, nil
	}

	// Handle OR (lowest precedence)
	if parts := splitOnOR(expression); len(parts) > 1 {
		var fns []func(storage.Item) bool
		for _, p := range parts {
			fn, err := buildFilterFunc(p, values)
			if err != nil {
				return nil, err
			}
			fns = append(fns, fn)
		}
		return func(item storage.Item) bool {
			for _, fn := range fns {
				if fn(item) {
					return true
				}
			}
			return false
		}, nil
	}

	// Handle AND
	if parts := splitOnAND(expression); len(parts) > 1 {
		var fns []func(storage.Item) bool
		for _, p := range parts {
			fn, err := buildFilterFunc(p, values)
			if err != nil {
				return nil, err
			}
			fns = append(fns, fn)
		}
		return func(item storage.Item) bool {
			for _, fn := range fns {
				if !fn(item) {
					return false
				}
			}
			return true
		}, nil
	}

	// Handle parenthesized expression
	if strings.HasPrefix(expression, "(") && strings.HasSuffix(expression, ")") {
		return buildFilterFunc(expression[1:len(expression)-1], values)
	}

	// Handle NOT
	lower := strings.ToLower(strings.TrimSpace(expression))
	if strings.HasPrefix(lower, "not ") {
		inner := strings.TrimSpace(expression[4:])
		fn, err := buildFilterFunc(inner, values)
		if err != nil {
			return nil, err
		}
		return func(item storage.Item) bool { return !fn(item) }, nil
	}

	// Handle functions
	if strings.HasPrefix(lower, "attribute_exists") {
		attrName := strings.TrimSpace(extractFuncArgs(expression))
		return func(item storage.Item) bool {
			_, ok := item[attrName]
			return ok
		}, nil
	}
	if strings.HasPrefix(lower, "attribute_not_exists") {
		attrName := strings.TrimSpace(extractFuncArgs(expression))
		return func(item storage.Item) bool {
			_, ok := item[attrName]
			return !ok
		}, nil
	}
	if strings.HasPrefix(lower, "attribute_type") {
		inner := extractFuncArgs(expression)
		args := strings.SplitN(inner, ",", 2)
		if len(args) != 2 {
			return nil, fmt.Errorf("ValidationException: attribute_type requires 2 arguments")
		}
		attrName := strings.TrimSpace(args[0])
		typePlaceholder := strings.TrimSpace(args[1])
		expectedType, err := resolveValueAsString(typePlaceholder, values)
		if err != nil {
			return nil, err
		}
		return func(item storage.Item) bool {
			av, ok := item[attrName]
			if !ok {
				return false
			}
			for k := range av {
				if k == expectedType {
					return true
				}
			}
			return false
		}, nil
	}
	if strings.HasPrefix(lower, "begins_with") {
		inner := extractFuncArgs(expression)
		args := strings.SplitN(inner, ",", 2)
		if len(args) != 2 {
			return nil, fmt.Errorf("ValidationException: begins_with requires 2 arguments")
		}
		attrName := strings.TrimSpace(args[0])
		placeholder := strings.TrimSpace(args[1])
		prefix, err := resolveValueAsString(placeholder, values)
		if err != nil {
			return nil, err
		}
		return func(item storage.Item) bool {
			av, ok := item[attrName]
			if !ok {
				return false
			}
			s, ok := av["S"].(string)
			if !ok {
				return false
			}
			return strings.HasPrefix(s, prefix)
		}, nil
	}
	if strings.HasPrefix(lower, "contains") {
		inner := extractFuncArgs(expression)
		args := strings.SplitN(inner, ",", 2)
		if len(args) != 2 {
			return nil, fmt.Errorf("ValidationException: contains requires 2 arguments")
		}
		attrName := strings.TrimSpace(args[0])
		placeholder := strings.TrimSpace(args[1])
		return makeContainsFunc(attrName, placeholder, values), nil
	}

	// Handle size(attr) comparisons: size(attr) op :val
	if strings.HasPrefix(lower, "size") {
		return parseSizeExpression(expression, values)
	}

	// Handle IN: attr IN (:val1, :val2, ...)
	if inIdx := findINOperator(expression); inIdx >= 0 {
		attrName := strings.TrimSpace(expression[:inIdx])
		rest := strings.TrimSpace(expression[inIdx+3:]) // skip " IN "
		// Strip parens
		rest = strings.TrimSpace(rest)
		if strings.HasPrefix(rest, "(") && strings.HasSuffix(rest, ")") {
			rest = rest[1 : len(rest)-1]
		}
		placeholders := strings.Split(rest, ",")
		var avs []storage.AttributeValue
		for _, p := range placeholders {
			p = strings.TrimSpace(p)
			av, ok := values[p]
			if !ok {
				return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", p)
			}
			avs = append(avs, av)
		}
		return func(item storage.Item) bool {
			itemAV, ok := item[attrName]
			if !ok {
				return false
			}
			for _, targetAV := range avs {
				if attributeValuesEqual(itemAV, targetAV) {
					return true
				}
			}
			return false
		}, nil
	}

	// Handle BETWEEN: attr BETWEEN :lo AND :hi
	if betweenIdx := strings.Index(lower, " between "); betweenIdx >= 0 {
		attrName := strings.TrimSpace(expression[:betweenIdx])
		rest := expression[betweenIdx+len(" between "):]
		andParts := splitOnAND(rest)
		if len(andParts) != 2 {
			return nil, fmt.Errorf("ValidationException: BETWEEN requires exactly 2 values")
		}
		loP := strings.TrimSpace(andParts[0])
		hiP := strings.TrimSpace(andParts[1])
		loAV, ok := values[loP]
		if !ok {
			return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", loP)
		}
		hiAV, ok := values[hiP]
		if !ok {
			return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", hiP)
		}
		return func(item storage.Item) bool {
			av, ok := item[attrName]
			if !ok {
				return false
			}
			return compareAV(av, ">=", loAV) && compareAV(av, "<=", hiAV)
		}, nil
	}

	// Handle comparisons: attr op :val
	for _, op := range []string{"<=", ">=", "<>", "!=", "<", ">", "="} {
		idx := findOperator(expression, op)
		if idx < 0 {
			continue
		}
		attrName := strings.TrimSpace(expression[:idx])
		placeholder := strings.TrimSpace(expression[idx+len(op):])

		valAV, ok := values[placeholder]
		if !ok {
			return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", placeholder)
		}

		return makeComparisonFunc(attrName, op, valAV), nil
	}

	return nil, fmt.Errorf("ValidationException: unsupported filter expression: %s", expression)
}

func makeContainsFunc(attrName, placeholder string, values map[string]storage.AttributeValue) func(storage.Item) bool {
	valAV, ok := values[placeholder]
	if !ok {
		return func(storage.Item) bool { return false }
	}
	return func(item storage.Item) bool {
		av, ok := item[attrName]
		if !ok {
			return false
		}
		// String contains
		if s, ok := av["S"].(string); ok {
			if sub, ok := valAV["S"].(string); ok {
				return strings.Contains(s, sub)
			}
		}
		// List contains
		if list, ok := av["L"].([]any); ok {
			for _, elem := range list {
				if elemMap, ok := elem.(map[string]any); ok {
					if attributeValuesEqual(storage.AttributeValue(elemMap), valAV) {
						return true
					}
				}
			}
		}
		// SS contains
		if ss, ok := av["SS"].([]any); ok {
			if target, ok := valAV["S"].(string); ok {
				for _, s := range ss {
					if str, ok := s.(string); ok && str == target {
						return true
					}
				}
			}
		}
		// NS contains
		if ns, ok := av["NS"].([]any); ok {
			if target, ok := valAV["N"].(string); ok {
				for _, n := range ns {
					if str, ok := n.(string); ok && str == target {
						return true
					}
				}
			}
		}
		return false
	}
}

func parseSizeExpression(expression string, values map[string]storage.AttributeValue) (func(storage.Item) bool, error) {
	// size(attr) op :val
	inner := extractFuncArgs(expression)
	attrName := strings.TrimSpace(inner)

	// Find the closing paren, then the operator after it
	closeIdx := strings.Index(expression, ")")
	if closeIdx < 0 {
		return nil, fmt.Errorf("ValidationException: malformed size expression")
	}
	rest := strings.TrimSpace(expression[closeIdx+1:])

	for _, op := range []string{"<=", ">=", "<>", "!=", "<", ">", "="} {
		if strings.HasPrefix(rest, op) {
			placeholder := strings.TrimSpace(rest[len(op):])
			valAV, ok := values[placeholder]
			if !ok {
				return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", placeholder)
			}
			expectedN, ok := valAV["N"].(string)
			if !ok {
				return nil, fmt.Errorf("ValidationException: size comparison requires numeric value")
			}
			expected := parseFloat(expectedN)
			return func(item storage.Item) bool {
				av, ok := item[attrName]
				if !ok {
					return false
				}
				sz := float64(attributeSize(av))
				return compareFloats(sz, op, expected)
			}, nil
		}
	}
	return nil, fmt.Errorf("ValidationException: unsupported size expression: %s", expression)
}

func attributeSize(av storage.AttributeValue) int {
	if s, ok := av["S"].(string); ok {
		return len(s)
	}
	if _, ok := av["N"]; ok {
		return 1
	}
	if b, ok := av["B"].(string); ok {
		return len(b)
	}
	if list, ok := av["L"].([]any); ok {
		return len(list)
	}
	if m, ok := av["M"].(map[string]any); ok {
		return len(m)
	}
	if ss, ok := av["SS"].([]any); ok {
		return len(ss)
	}
	if ns, ok := av["NS"].([]any); ok {
		return len(ns)
	}
	if bs, ok := av["BS"].([]any); ok {
		return len(bs)
	}
	return 0
}

func attributeValuesEqual(a, b storage.AttributeValue) bool {
	// Compare by type
	for _, t := range []string{"S", "N", "B", "BOOL", "NULL"} {
		aVal, aOk := a[t]
		bVal, bOk := b[t]
		if aOk && bOk {
			return fmt.Sprintf("%v", aVal) == fmt.Sprintf("%v", bVal)
		}
	}
	return false
}

func compareAV(av storage.AttributeValue, op string, target storage.AttributeValue) bool {
	if s, ok := av["S"].(string); ok {
		if t, ok := target["S"].(string); ok {
			return compareStrings(s, op, t)
		}
	}
	if n, ok := av["N"].(string); ok {
		if t, ok := target["N"].(string); ok {
			return compareNumericStrings(n, op, t)
		}
	}
	return false
}

func makeComparisonFunc(attrName, op string, valAV storage.AttributeValue) func(storage.Item) bool {
	return func(item storage.Item) bool {
		av, ok := item[attrName]
		if !ok {
			return false
		}
		if expected, ok := valAV["S"]; ok {
			actual, ok := av["S"].(string)
			if !ok {
				return false
			}
			return compareStrings(actual, op, expected.(string))
		}
		if expected, ok := valAV["N"]; ok {
			actual, ok := av["N"].(string)
			if !ok {
				return false
			}
			return compareNumericStrings(actual, op, expected.(string))
		}
		if expected, ok := valAV["BOOL"]; ok {
			actual, ok := av["BOOL"].(bool)
			if !ok {
				return false
			}
			expectedBool, _ := expected.(bool)
			switch op {
			case "=":
				return actual == expectedBool
			case "<>", "!=":
				return actual != expectedBool
			}
		}
		if _, ok := valAV["NULL"]; ok {
			_, hasNull := av["NULL"]
			switch op {
			case "=":
				return hasNull
			case "<>", "!=":
				return !hasNull
			}
		}
		return false
	}
}

func compareStrings(a, op, b string) bool {
	switch op {
	case "=":
		return a == b
	case "<>", "!=":
		return a != b
	case "<":
		return a < b
	case "<=":
		return a <= b
	case ">":
		return a > b
	case ">=":
		return a >= b
	}
	return false
}

func compareNumericStrings(a, op, b string) bool {
	af := parseFloat(a)
	bf := parseFloat(b)
	return compareFloats(af, op, bf)
}

func compareFloats(a float64, op string, b float64) bool {
	switch op {
	case "=":
		return a == b
	case "<>", "!=":
		return a != b
	case "<":
		return a < b
	case "<=":
		return a <= b
	case ">":
		return a > b
	case ">=":
		return a >= b
	}
	return false
}

func parseFloat(s string) float64 {
	var f float64
	_, _ = fmt.Sscanf(s, "%f", &f)
	return f
}

// ResolveNames replaces #name placeholders with actual attribute names.
func ResolveNames(expression string, names map[string]string) string {
	if len(names) == 0 {
		return expression
	}
	for placeholder, name := range names {
		expression = strings.ReplaceAll(expression, placeholder, name)
	}
	return expression
}

// ResolveValue extracts the scalar string value from a value placeholder.
func ResolveValue(placeholder string, values map[string]storage.AttributeValue, attrType string) (string, error) {
	av, ok := values[placeholder]
	if !ok {
		return "", fmt.Errorf("ValidationException: unresolved value placeholder: %s", placeholder)
	}
	raw, ok := av[attrType]
	if !ok {
		for _, v := range av {
			if s, ok := v.(string); ok {
				return s, nil
			}
		}
		return "", fmt.Errorf("ValidationException: value %s is not of type %s", placeholder, attrType)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("ValidationException: value %s type %s is not a string", placeholder, attrType)
	}
	return s, nil
}

func resolveValueAsString(placeholder string, values map[string]storage.AttributeValue) (string, error) {
	av, ok := values[placeholder]
	if !ok {
		return "", fmt.Errorf("ValidationException: unresolved value placeholder: %s", placeholder)
	}
	if s, ok := av["S"].(string); ok {
		return s, nil
	}
	if s, ok := av["N"].(string); ok {
		return s, nil
	}
	return "", fmt.Errorf("ValidationException: cannot resolve %s as string", placeholder)
}

func extractFuncArgs(expression string) string {
	start := strings.Index(expression, "(")
	end := strings.LastIndex(expression, ")")
	if start < 0 || end < 0 || end <= start {
		return ""
	}
	return expression[start+1 : end]
}

func splitOnAND(expression string) []string {
	// BETWEEN-aware AND splitting: "A BETWEEN :lo AND :hi" should NOT split on the AND
	var parts []string
	depth := 0
	start := 0
	upper := strings.ToUpper(expression)
	inBetween := false

	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 {
				// Check for BETWEEN keyword
				if i+9 <= len(upper) && upper[i:i+9] == " BETWEEN " {
					inBetween = true
				}
				// Check for AND keyword
				if i+5 <= len(upper) && upper[i:i+5] == " AND " {
					if inBetween {
						// This AND belongs to BETWEEN, skip it
						inBetween = false
					} else {
						parts = append(parts, strings.TrimSpace(expression[start:i]))
						start = i + 5
						i += 4
					}
				}
			}
		}
	}
	parts = append(parts, strings.TrimSpace(expression[start:]))
	return parts
}

func splitOnOR(expression string) []string {
	return splitOnKeyword(expression, " OR ")
}

func splitOnKeyword(expression, keyword string) []string {
	var parts []string
	depth := 0
	start := 0
	upper := strings.ToUpper(expression)
	upperKW := strings.ToUpper(keyword)

	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && i+len(upperKW) <= len(upper) && upper[i:i+len(upperKW)] == upperKW {
				parts = append(parts, strings.TrimSpace(expression[start:i]))
				start = i + len(upperKW)
				i += len(upperKW) - 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(expression[start:]))
	return parts
}

func findOperator(expression, op string) int {
	depth := 0
	for i := 0; i < len(expression); i++ {
		switch expression[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && i+len(op) <= len(expression) && expression[i:i+len(op)] == op {
				if (op == "<" || op == ">") && i+2 <= len(expression) && (expression[i:i+2] == "<=" || expression[i:i+2] == ">=" || expression[i:i+2] == "<>") {
					continue
				}
				return i
			}
		}
	}
	return -1
}

func findINOperator(expression string) int {
	upper := strings.ToUpper(expression)
	depth := 0
	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && i+4 <= len(upper) && upper[i:i+4] == " IN " {
				return i
			}
		}
	}
	return -1
}
