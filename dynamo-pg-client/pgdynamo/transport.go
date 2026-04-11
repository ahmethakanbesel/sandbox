package pgdynamo

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// pgTransport implements the smithy-go HTTPClient interface.
// It intercepts HTTP requests from the AWS SDK and translates them to PostgreSQL operations.
type pgTransport struct {
	handler *Handler
}

// Do implements the HTTPClient interface.
func (t *pgTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	operation := extractOperation(target)
	if operation == "" {
		return errorResponse(400, "missing or invalid X-Amz-Target header"), nil
	}

	// Read request body with pre-sized buffer when ContentLength is known.
	var body []byte
	if req.Body != nil {
		if req.ContentLength > 0 {
			body = make([]byte, req.ContentLength)
			_, err := io.ReadFull(req.Body, body)
			if err != nil {
				return errorResponse(500, "failed to read request body"), nil
			}
		} else {
			var err error
			body, err = io.ReadAll(req.Body)
			if err != nil {
				return errorResponse(500, "failed to read request body"), nil
			}
		}
	}
	if len(body) == 0 {
		body = []byte("{}")
	}

	respBody, err := t.handler.HandleOperation(req.Context(), operation, body)
	if err != nil {
		errBody, statusCode := BuildErrorResponse(err)
		return buildResponse(statusCode, errBody), nil
	}

	return buildResponse(200, respBody), nil
}

func extractOperation(target string) string {
	idx := strings.LastIndex(target, ".")
	if idx < 0 || idx >= len(target)-1 {
		return ""
	}
	return target[idx+1:]
}

func buildResponse(statusCode int, body []byte) *http.Response {
	checksum := crc32.ChecksumIEEE(body)

	return &http.Response{
		StatusCode: statusCode,
		Header: http.Header{
			"Content-Type":      {"application/x-amz-json-1.0"},
			"X-Amzn-Requestid":  {uuid.New().String()},
			"X-Amz-Crc32":      {strconv.FormatUint(uint64(checksum), 10)},
		},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func errorResponse(statusCode int, msg string) *http.Response {
	body := fmt.Appendf(nil, `{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"%s"}`, msg)
	return buildResponse(statusCode, body)
}
