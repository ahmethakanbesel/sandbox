package pgdynamo

import (
	"io"
	"testing"
)

func TestExtractOperation(t *testing.T) {
	tests := []struct {
		target string
		want   string
	}{
		{"DynamoDB_20120810.CreateTable", "CreateTable"},
		{"DynamoDB_20120810.PutItem", "PutItem"},
		{"DynamoDB_20120810.Query", "Query"},
		{"", ""},
		{"NoDotsHere", ""},
		{"Ends.With.", ""},
		{"A.", ""},
	}
	for _, tt := range tests {
		got := extractOperation(tt.target)
		if got != tt.want {
			t.Errorf("extractOperation(%q) = %q, want %q", tt.target, got, tt.want)
		}
	}
}

func TestBuildResponse(t *testing.T) {
	body := []byte(`{"result":"ok"}`)
	resp := buildResponse(200, body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/x-amz-json-1.0" {
		t.Fatal("expected application/x-amz-json-1.0 content type")
	}
	if resp.Header.Get("X-Amzn-Requestid") == "" {
		t.Fatal("expected non-empty request id")
	}
	if resp.Header.Get("X-Amz-Crc32") == "" {
		t.Fatal("expected non-empty crc32")
	}
	respBody, _ := io.ReadAll(resp.Body)
	if string(respBody) != `{"result":"ok"}` {
		t.Fatalf("unexpected body: %s", respBody)
	}
}

func TestErrorResponse(t *testing.T) {
	resp := errorResponse(500, "test error")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 500 {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Fatal("expected non-empty body")
	}
}

func TestBuildResponseCRC32(t *testing.T) {
	body := []byte(`{"test":"data"}`)
	r1 := buildResponse(200, body)
	defer func() { _ = r1.Body.Close() }()
	r2 := buildResponse(200, body)
	defer func() { _ = r2.Body.Close() }()
	if r1.Header.Get("X-Amz-Crc32") != r2.Header.Get("X-Amz-Crc32") {
		t.Fatal("expected same CRC32 for same body")
	}
}
