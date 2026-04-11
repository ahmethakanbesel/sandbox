package pgdynamo

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// DynamoError represents a DynamoDB-style error.
type DynamoError struct {
	Code       string
	Message    string
	StatusCode int
}

func (e *DynamoError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// BuildErrorResponse serializes a DynamoError into JSON bytes and returns the HTTP status.
// Uses fmt.Appendf to avoid map allocation + json.Marshal overhead.
func BuildErrorResponse(err error) ([]byte, int) {
	var de *DynamoError
	if !errors.As(err, &de) {
		de = &DynamoError{
			Code:       "InternalServerError",
			Message:    err.Error(),
			StatusCode: http.StatusInternalServerError,
		}
	}
	body := fmt.Appendf(nil, `{"__type":"com.amazonaws.dynamodb.v20120810#%s","message":"%s"}`,
		de.Code, escapeJSON(de.Message))
	return body, de.StatusCode
}

// escapeJSON escapes special characters in a string for safe JSON embedding.
func escapeJSON(s string) string {
	// Fast path: if no special chars, return as-is
	needsEscape := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\t' || c < 0x20 {
			needsEscape = true
			break
		}
	}
	if !needsEscape {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, c)
			} else {
				b.WriteByte(c)
			}
		}
	}
	return b.String()
}

func ErrResourceNotFound(msg string) *DynamoError {
	return &DynamoError{
		Code:       "ResourceNotFoundException",
		Message:    msg,
		StatusCode: http.StatusBadRequest,
	}
}

func ErrResourceInUse(msg string) *DynamoError {
	return &DynamoError{
		Code:       "ResourceInUseException",
		Message:    msg,
		StatusCode: http.StatusBadRequest,
	}
}

func ErrValidation(msg string) *DynamoError {
	return &DynamoError{
		Code:       "ValidationException",
		Message:    msg,
		StatusCode: http.StatusBadRequest,
	}
}

func ErrConditionalCheckFailed(msg string) *DynamoError {
	return &DynamoError{
		Code:       "ConditionalCheckFailedException",
		Message:    msg,
		StatusCode: http.StatusBadRequest,
	}
}

func ErrTransactionCanceled(reasons []string) *DynamoError {
	return &DynamoError{
		Code:       "TransactionCanceledException",
		Message:    fmt.Sprintf("Transaction canceled, please refer cancellation reasons for specific reasons [%s]", joinReasons(reasons)),
		StatusCode: http.StatusBadRequest,
	}
}

func ErrTransactionConflict(msg string) *DynamoError {
	return &DynamoError{
		Code:       "TransactionConflictException",
		Message:    msg,
		StatusCode: http.StatusBadRequest,
	}
}

func ErrItemCollectionSizeLimitExceeded(msg string) *DynamoError {
	return &DynamoError{
		Code:       "ItemCollectionSizeLimitExceededException",
		Message:    msg,
		StatusCode: http.StatusBadRequest,
	}
}

func ErrProvisionedThroughputExceeded(msg string) *DynamoError {
	return &DynamoError{
		Code:       "ProvisionedThroughputExceededException",
		Message:    msg,
		StatusCode: http.StatusBadRequest,
	}
}

func joinReasons(reasons []string) string {
	return strings.Join(reasons, ", ")
}
