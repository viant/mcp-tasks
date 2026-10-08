package rpc

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	tasks "github.com/viant/mcp-tasks"
)

const sentinelPrefix = "=?base64?"
const sentinelSuffix = "?="

// EncodeHeaderValue implements the July header sentinel, including escaping a
// literal sentinel-shaped value. The JSON body remains the source of truth.
func EncodeHeaderValue(value string) string {
	if safeHeaderASCII(value) && !(strings.HasPrefix(value, sentinelPrefix) && strings.HasSuffix(value, sentinelSuffix)) {
		return value
	}
	return sentinelPrefix + base64.StdEncoding.EncodeToString([]byte(value)) + sentinelSuffix
}

func safeHeaderASCII(value string) bool {
	if strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if (r < 0x20 || r > 0x7e) && r != '\t' {
			return false
		}
	}
	return true
}

func DecodeHeaderValue(value string) (string, error) {
	if !safeHeaderASCII(value) || len(value) > tasks.MaxJSONBytes {
		return "", errors.New("invalid mirrored header")
	}
	if strings.HasPrefix(value, sentinelPrefix) && strings.HasSuffix(value, sentinelSuffix) {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(strings.TrimPrefix(value, sentinelPrefix), sentinelSuffix))
		if err != nil || !utf8.Valid(decoded) {
			return "", errors.New("invalid mirrored header encoding")
		}
		return string(decoded), nil
	}
	return value, nil
}

// A modern 400/404 protocol error is not a transport failure or permission to
// replay execution. Preserve its correlated JSON-RPC error for typed clients.
func readHTTPRPCError(response *http.Response, id json.RawMessage) error {
	raw, err := io.ReadAll(io.LimitReader(response.Body, tasks.MaxJSONBytes+1))
	if err != nil {
		return err
	}
	var frame struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *tasks.RPCError `json:"error"`
	}
	if len(raw) > tasks.MaxJSONBytes || json.Unmarshal(raw, &frame) != nil || frame.JSONRPC != "2.0" || !sameJSON(frame.ID, id) || len(frame.Result) > 0 || frame.Error == nil || frame.Error.Message == "" {
		return errors.New("invalid MCP HTTP error response")
	}
	return frame.Error
}
