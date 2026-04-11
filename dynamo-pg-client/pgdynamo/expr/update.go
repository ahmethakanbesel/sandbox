package expr

import (
	"fmt"
	"strings"

	"github.com/ahmethakanbesel/dynamo-pg-client/pgdynamo/storage"
)

// UpdateAction represents a single action in an UpdateExpression.
type UpdateAction struct {
	Type string // SET, REMOVE, ADD, DELETE
	Path string // attribute path
	// For SET: Value is the new value, or Operand1/Operand2 for arithmetic
	Value    storage.AttributeValue
	Operand1 string // for SET path = path + :val
	Operand2 string
	Op       string // "+", "-" for arithmetic
	// For SET with if_not_exists: IfNotExistsPath, IfNotExistsValue
	IfNotExistsPath  string
	IfNotExistsValue storage.AttributeValue
	// For SET with list_append
	ListAppendFirst  string // path or :val
	ListAppendSecond string
}

// ParseUpdateExpression parses a DynamoDB UpdateExpression into a list of actions.
func ParseUpdateExpression(
	expression string,
	names map[string]string,
	values map[string]storage.AttributeValue,
) ([]UpdateAction, error) {
	if expression == "" {
		return nil, fmt.Errorf("ValidationException: UpdateExpression is required")
	}

	expression = ResolveNames(expression, names)
	var actions []UpdateAction

	// Split into clauses: SET ..., REMOVE ..., ADD ..., DELETE ...
	clauses := splitUpdateClauses(expression)
	for _, clause := range clauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}

		upper := strings.ToUpper(clause)
		switch {
		case strings.HasPrefix(upper, "SET "):
			parsed, err := parseSetClause(clause[4:], values)
			if err != nil {
				return nil, err
			}
			actions = append(actions, parsed...)
		case strings.HasPrefix(upper, "REMOVE "):
			parsed := parseRemoveClause(clause[7:])
			actions = append(actions, parsed...)
		case strings.HasPrefix(upper, "ADD "):
			parsed, err := parseAddClause(clause[4:], values)
			if err != nil {
				return nil, err
			}
			actions = append(actions, parsed...)
		case strings.HasPrefix(upper, "DELETE "):
			parsed, err := parseDeleteClause(clause[7:], values)
			if err != nil {
				return nil, err
			}
			actions = append(actions, parsed...)
		default:
			return nil, fmt.Errorf("ValidationException: unsupported update clause: %s", clause)
		}
	}

	return actions, nil
}

// ApplyUpdateActions applies parsed update actions to an item, returning the modified item.
func ApplyUpdateActions(item storage.Item, actions []UpdateAction, values map[string]storage.AttributeValue) storage.Item {
	if item == nil {
		item = storage.Item{}
	}
	// Work on a copy
	result := make(storage.Item, len(item))
	for k, v := range item {
		result[k] = v
	}

	for _, action := range actions {
		switch action.Type {
		case "SET":
			if action.IfNotExistsPath != "" && action.Op != "" {
				// if_not_exists(path, value) +/- operand: resolve the base value, then arithmetic
				var baseVal float64
				if existing, exists := result[action.IfNotExistsPath]; exists {
					if n, ok := existing["N"].(string); ok {
						baseVal = parseFloat(n)
					}
				} else {
					if n, ok := action.IfNotExistsValue["N"].(string); ok {
						baseVal = parseFloat(n)
					}
				}
				val2 := resolveNumericOperand(result, values, action.Operand2)
				var res float64
				switch action.Op {
				case "+":
					res = baseVal + val2
				case "-":
					res = baseVal - val2
				}
				result[action.Path] = storage.AttributeValue{"N": formatFloat(res)}
			} else if action.IfNotExistsPath != "" {
				// if_not_exists(path, value) — use existing value if present
				if _, exists := result[action.IfNotExistsPath]; !exists {
					result[action.Path] = action.IfNotExistsValue
				}
			} else if action.ListAppendFirst != "" {
				// list_append(a, b)
				first := resolveListOperand(result, values, action.ListAppendFirst)
				second := resolveListOperand(result, values, action.ListAppendSecond)
				combined := make([]any, 0, len(first)+len(second))
				combined = append(combined, first...)
				combined = append(combined, second...)
				result[action.Path] = storage.AttributeValue{"L": combined}
			} else if action.Op != "" {
				// Arithmetic: path = operand1 op operand2
				val1 := resolveNumericOperand(result, values, action.Operand1)
				val2 := resolveNumericOperand(result, values, action.Operand2)
				var res float64
				switch action.Op {
				case "+":
					res = val1 + val2
				case "-":
					res = val1 - val2
				}
				result[action.Path] = storage.AttributeValue{"N": formatFloat(res)}
			} else {
				result[action.Path] = action.Value
			}
		case "REMOVE":
			delete(result, action.Path)
		case "ADD":
			existing, exists := result[action.Path]
			if !exists {
				// If attribute doesn't exist, set it to the value
				result[action.Path] = action.Value
			} else {
				// For numbers: add to existing
				if existN, ok := existing["N"].(string); ok {
					if addN, ok := action.Value["N"].(string); ok {
						sum := parseFloat(existN) + parseFloat(addN)
						result[action.Path] = storage.AttributeValue{"N": formatFloat(sum)}
					}
				}
				// For sets: add elements
				if existSS, ok := existing["SS"].([]any); ok {
					if addSS, ok := action.Value["SS"].([]any); ok {
						merged := mergeStringSet(existSS, addSS)
						result[action.Path] = storage.AttributeValue{"SS": merged}
					}
				}
				if existNS, ok := existing["NS"].([]any); ok {
					if addNS, ok := action.Value["NS"].([]any); ok {
						merged := mergeStringSet(existNS, addNS)
						result[action.Path] = storage.AttributeValue{"NS": merged}
					}
				}
			}
		case "DELETE":
			existing, exists := result[action.Path]
			if !exists {
				continue
			}
			// For sets: remove elements
			if existSS, ok := existing["SS"].([]any); ok {
				if delSS, ok := action.Value["SS"].([]any); ok {
					result[action.Path] = storage.AttributeValue{"SS": subtractSet(existSS, delSS)}
				}
			}
			if existNS, ok := existing["NS"].([]any); ok {
				if delNS, ok := action.Value["NS"].([]any); ok {
					result[action.Path] = storage.AttributeValue{"NS": subtractSet(existNS, delNS)}
				}
			}
		}
	}

	return result
}

func splitUpdateClauses(expression string) []string {
	// Split on SET, REMOVE, ADD, DELETE keywords at the top level
	clauses := make([]string, 0, 4)
	upper := strings.ToUpper(expression)
	keywords := []string{"SET ", "REMOVE ", "ADD ", "DELETE "}

	type span struct {
		start int
		kw    string
	}
	var spans []span

	for i := 0; i < len(upper); i++ {
		for _, kw := range keywords {
			if i+len(kw) <= len(upper) && upper[i:i+len(kw)] == kw {
				// Check it's at start or preceded by space/newline
				if i == 0 || upper[i-1] == ' ' || upper[i-1] == '\n' || upper[i-1] == '\t' {
					spans = append(spans, span{start: i, kw: kw})
				}
			}
		}
	}

	if len(spans) == 0 {
		return []string{expression}
	}

	for i, sp := range spans {
		end := len(expression)
		if i+1 < len(spans) {
			end = spans[i+1].start
		}
		clauses = append(clauses, strings.TrimSpace(expression[sp.start:end]))
	}

	return clauses
}

func parseSetClause(clause string, values map[string]storage.AttributeValue) ([]UpdateAction, error) {
	// SET attr1 = val1, attr2 = val2
	parts := splitOnComma(clause)
	actions := make([]UpdateAction, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			return nil, fmt.Errorf("ValidationException: SET requires = in: %s", part)
		}
		path := strings.TrimSpace(part[:eqIdx])
		rhs := strings.TrimSpace(part[eqIdx+1:])

		action := UpdateAction{Type: "SET", Path: path}

		lower := strings.ToLower(rhs)
		// Check for if_not_exists(path, value) optionally followed by arithmetic
		if strings.HasPrefix(lower, "if_not_exists") {
			inner := extractFuncArgs(rhs)
			args := strings.SplitN(inner, ",", 2)
			if len(args) != 2 {
				return nil, fmt.Errorf("ValidationException: if_not_exists requires 2 arguments")
			}
			action.IfNotExistsPath = strings.TrimSpace(args[0])
			valPlaceholder := strings.TrimSpace(args[1])
			av, ok := values[valPlaceholder]
			if !ok {
				return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", valPlaceholder)
			}
			action.IfNotExistsValue = av

			// Check for arithmetic after if_not_exists: if_not_exists(path, :val) + :inc
			closeIdx := strings.LastIndex(rhs, ")")
			if closeIdx >= 0 && closeIdx < len(rhs)-1 {
				rest := strings.TrimSpace(rhs[closeIdx+1:])
				if len(rest) > 0 && (rest[0] == '+' || rest[0] == '-') {
					action.Op = string(rest[0])
					action.Operand1 = "" // sentinel: resolve from if_not_exists at apply time
					action.Operand2 = strings.TrimSpace(rest[1:])
				}
			}
		} else if strings.HasPrefix(lower, "list_append") {
			inner := extractFuncArgs(rhs)
			args := strings.SplitN(inner, ",", 2)
			if len(args) != 2 {
				return nil, fmt.Errorf("ValidationException: list_append requires 2 arguments")
			}
			action.ListAppendFirst = strings.TrimSpace(args[0])
			action.ListAppendSecond = strings.TrimSpace(args[1])
		} else if strings.Contains(rhs, "+") || strings.Contains(rhs, "-") {
			// Arithmetic: path + :val or :val + path or path - :val
			var op string
			var opIdx int
			if idx := strings.Index(rhs, "+"); idx >= 0 {
				op = "+"
				opIdx = idx
			} else if idx := strings.Index(rhs, "-"); idx >= 0 {
				op = "-"
				opIdx = idx
			}
			if op != "" {
				action.Op = op
				action.Operand1 = strings.TrimSpace(rhs[:opIdx])
				action.Operand2 = strings.TrimSpace(rhs[opIdx+1:])
			}
		} else {
			// Simple value assignment
			av, ok := values[rhs]
			if !ok {
				return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", rhs)
			}
			action.Value = av
		}

		actions = append(actions, action)
	}
	return actions, nil
}

func parseRemoveClause(clause string) []UpdateAction {
	parts := splitOnComma(clause)
	var actions []UpdateAction
	for _, part := range parts {
		path := strings.TrimSpace(part)
		if path != "" {
			actions = append(actions, UpdateAction{Type: "REMOVE", Path: path})
		}
	}
	return actions
}

func parseAddClause(clause string, values map[string]storage.AttributeValue) ([]UpdateAction, error) {
	// ADD attr :val
	parts := splitOnComma(clause)
	actions := make([]UpdateAction, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		spaceIdx := strings.Index(part, " ")
		if spaceIdx < 0 {
			return nil, fmt.Errorf("ValidationException: ADD requires attribute and value: %s", part)
		}
		path := strings.TrimSpace(part[:spaceIdx])
		valPlaceholder := strings.TrimSpace(part[spaceIdx+1:])
		av, ok := values[valPlaceholder]
		if !ok {
			return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", valPlaceholder)
		}
		actions = append(actions, UpdateAction{Type: "ADD", Path: path, Value: av})
	}
	return actions, nil
}

func parseDeleteClause(clause string, values map[string]storage.AttributeValue) ([]UpdateAction, error) {
	// DELETE attr :val
	parts := splitOnComma(clause)
	actions := make([]UpdateAction, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		spaceIdx := strings.Index(part, " ")
		if spaceIdx < 0 {
			return nil, fmt.Errorf("ValidationException: DELETE requires attribute and value: %s", part)
		}
		path := strings.TrimSpace(part[:spaceIdx])
		valPlaceholder := strings.TrimSpace(part[spaceIdx+1:])
		av, ok := values[valPlaceholder]
		if !ok {
			return nil, fmt.Errorf("ValidationException: unresolved value placeholder: %s", valPlaceholder)
		}
		actions = append(actions, UpdateAction{Type: "DELETE", Path: path, Value: av})
	}
	return actions, nil
}

func splitOnComma(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func resolveNumericOperand(item storage.Item, values map[string]storage.AttributeValue, operand string) float64 {
	operand = strings.TrimSpace(operand)
	// Check if it's a value placeholder
	if strings.HasPrefix(operand, ":") {
		if av, ok := values[operand]; ok {
			if n, ok := av["N"].(string); ok {
				return parseFloat(n)
			}
		}
		return 0
	}
	// It's an attribute path
	if av, ok := item[operand]; ok {
		if n, ok := av["N"].(string); ok {
			return parseFloat(n)
		}
	}
	return 0
}

func resolveListOperand(item storage.Item, values map[string]storage.AttributeValue, operand string) []any {
	operand = strings.TrimSpace(operand)
	if strings.HasPrefix(operand, ":") {
		if av, ok := values[operand]; ok {
			if list, ok := av["L"].([]any); ok {
				return list
			}
		}
		return nil
	}
	if av, ok := item[operand]; ok {
		if list, ok := av["L"].([]any); ok {
			return list
		}
	}
	return nil
}

func formatFloat(f float64) string {
	// DynamoDB stores numbers without trailing zeros
	s := fmt.Sprintf("%g", f)
	return s
}

func mergeStringSet(existing, add []any) []any {
	seen := make(map[string]bool)
	for _, v := range existing {
		if s, ok := v.(string); ok {
			seen[s] = true
		}
	}
	result := make([]any, len(existing))
	copy(result, existing)
	for _, v := range add {
		if s, ok := v.(string); ok {
			if !seen[s] {
				result = append(result, v)
				seen[s] = true
			}
		}
	}
	return result
}

func subtractSet(existing, remove []any) []any {
	toRemove := make(map[string]bool)
	for _, v := range remove {
		if s, ok := v.(string); ok {
			toRemove[s] = true
		}
	}
	var result []any
	for _, v := range existing {
		if s, ok := v.(string); ok {
			if !toRemove[s] {
				result = append(result, v)
			}
		}
	}
	return result
}
