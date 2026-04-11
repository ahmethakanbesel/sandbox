package pgdynamo

import (
	json "github.com/goccy/go-json"
	"errors"
	"net/http"
	"testing"
)

func TestDynamoErrorError(t *testing.T) {
	err := &DynamoError{
		Code:       "ValidationException",
		Message:    "invalid input",
		StatusCode: 400,
	}
	got := err.Error()
	if got != "ValidationException: invalid input" {
		t.Fatalf("expected 'ValidationException: invalid input', got %q", got)
	}
}

func TestBuildErrorResponseDynamoError(t *testing.T) {
	err := ErrResourceNotFound("table not found")
	body, status := BuildErrorResponse(err)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", status)
	}
	var resp map[string]string
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["__type"] != "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException" {
		t.Fatalf("unexpected __type: %s", resp["__type"])
	}
	if resp["message"] != "table not found" {
		t.Fatalf("unexpected message: %s", resp["message"])
	}
}

func TestBuildErrorResponseGenericError(t *testing.T) {
	err := errors.New("something went wrong")
	body, status := BuildErrorResponse(err)
	if status != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", status)
	}
	var resp map[string]string
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["__type"] != "com.amazonaws.dynamodb.v20120810#InternalServerError" {
		t.Fatalf("unexpected __type: %s", resp["__type"])
	}
}

func TestErrValidation(t *testing.T) {
	err := ErrValidation("bad input")
	if err.Code != "ValidationException" {
		t.Fatalf("expected ValidationException, got %s", err.Code)
	}
	if err.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", err.StatusCode)
	}
}

func TestErrTransactionCanceled(t *testing.T) {
	err := ErrTransactionCanceled([]string{"ConditionalCheckFailed", "None"})
	if err.Code != "TransactionCanceledException" {
		t.Fatalf("expected TransactionCanceledException, got %s", err.Code)
	}
	if err.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", err.StatusCode)
	}
}

func TestErrTransactionConflict(t *testing.T) {
	err := ErrTransactionConflict("conflict detected")
	if err.Code != "TransactionConflictException" {
		t.Fatalf("expected TransactionConflictException, got %s", err.Code)
	}
}

func TestErrItemCollectionSizeLimitExceeded(t *testing.T) {
	err := ErrItemCollectionSizeLimitExceeded("too large")
	if err.Code != "ItemCollectionSizeLimitExceededException" {
		t.Fatalf("expected ItemCollectionSizeLimitExceededException, got %s", err.Code)
	}
}

func TestErrProvisionedThroughputExceeded(t *testing.T) {
	err := ErrProvisionedThroughputExceeded("throttled")
	if err.Code != "ProvisionedThroughputExceededException" {
		t.Fatalf("expected ProvisionedThroughputExceededException, got %s", err.Code)
	}
}

func TestJoinReasons(t *testing.T) {
	tests := []struct {
		reasons []string
		want    string
	}{
		{[]string{}, ""},
		{[]string{"a"}, "a"},
		{[]string{"a", "b", "c"}, "a, b, c"},
	}
	for _, tt := range tests {
		got := joinReasons(tt.reasons)
		if got != tt.want {
			t.Errorf("joinReasons(%v) = %q, want %q", tt.reasons, got, tt.want)
		}
	}
}
