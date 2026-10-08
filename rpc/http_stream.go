package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	tasks "github.com/viant/mcp-tasks"
)

var ErrStreamProtocol = errors.New("invalid MCP subscription stream")

// StreamCaller keeps subscriptions/listen open and reports only notifications.
// A nil error means a correlated graceful complete response was received.
type StreamCaller interface {
	Listen(context.Context, Request, func(json.RawMessage) error) (json.RawMessage, error)
}
type ConsumerError struct{ Cause error }

func (e *ConsumerError) Error() string { return e.Cause.Error() }
func (e *ConsumerError) Unwrap() error { return e.Cause }
func (c *HTTPCaller) Listen(ctx context.Context, request Request, notify func(json.RawMessage) error) (json.RawMessage, error) {
	if request.Method != tasks.MethodListen || notify == nil {
		return nil, ErrStreamProtocol
	}
	id := c.sequence.Add(1)
	if id > 1<<53-1 {
		return nil, errors.New("subscription request ID quota exceeded")
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": request.Method, "params": request.Params})
	if err != nil || len(body) > tasks.MaxJSONBytes {
		return nil, ErrStreamProtocol
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if c.HeaderProvider != nil {
		headers, err := c.HeaderProvider(ctx)
		if err != nil {
			return nil, err
		}
		req.Header = headers.Clone()
		if req.Header == nil {
			req.Header = make(http.Header)
		}
	}
	for key, values := range request.Headers {
		req.Header[key] = append([]string(nil), values...)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Method", tasks.MethodListen)
	req.Header.Set("Mcp-Protocol-Version", tasks.ProtocolVersion)
	req.Header.Del("Mcp-Name")
	response, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusNotFound {
			idJSON, _ := json.Marshal(id)
			return nil, readHTTPRPCError(response, idJSON)
		}
		return nil, fmt.Errorf("MCP HTTP status %d", response.StatusCode)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		raw, err := io.ReadAll(io.LimitReader(response.Body, tasks.MaxJSONBytes+1))
		if err != nil {
			return nil, err
		}
		var frame map[string]json.RawMessage
		_ = json.Unmarshal(raw, &frame)
		idRaw, _ := json.Marshal(id)
		if len(raw) > tasks.MaxJSONBytes || !sameJSON(frame["id"], idRaw) || string(frame["jsonrpc"]) != `"2.0"` {
			return nil, ErrStreamProtocol
		}
		if frame["error"] != nil {
			var rpcError tasks.RPCError
			if json.Unmarshal(frame["error"], &rpcError) == nil && rpcError.Message != "" {
				return nil, &rpcError
			}
		}
		return nil, ErrStreamProtocol
	}
	idRaw, _ := json.Marshal(id)
	acknowledged := false
	return readSubscriptionSSE(response.Body, func(raw json.RawMessage) (json.RawMessage, bool, error) {
		var envelope struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
			Result  json.RawMessage `json:"result"`
			Error   *tasks.RPCError `json:"error"`
		}
		if json.Unmarshal(raw, &envelope) != nil || envelope.JSONRPC != "2.0" {
			return nil, false, ErrStreamProtocol
		}
		if len(envelope.ID) > 0 {
			if !sameJSON(envelope.ID, idRaw) || envelope.Method != "" || envelope.Error != nil && len(envelope.Result) > 0 {
				return nil, false, ErrStreamProtocol
			}
			if envelope.Error != nil {
				return nil, false, envelope.Error
			}
			var result tasks.SubscriptionResult
			if !acknowledged || json.Unmarshal(envelope.Result, &result) != nil || result.ResultType != tasks.ResultComplete || !sameJSON(result.Meta[tasks.SubscriptionIDMeta], idRaw) {
				return nil, false, ErrStreamProtocol
			}
			return envelope.Result, true, nil
		}
		if !tasks.ValidObject(envelope.Params) || envelope.Method == "" {
			return nil, false, ErrStreamProtocol
		}
		var params struct {
			Meta map[string]json.RawMessage `json:"_meta"`
		}
		_ = json.Unmarshal(envelope.Params, &params)
		if !sameJSON(params.Meta[tasks.SubscriptionIDMeta], idRaw) {
			return nil, false, ErrStreamProtocol
		}
		if !acknowledged {
			if envelope.Method != tasks.MethodAcknowledged {
				return nil, false, ErrStreamProtocol
			}
			acknowledged = true
		} else if envelope.Method != tasks.MethodNotification {
			return nil, false, ErrStreamProtocol
		}
		if err := notify(raw); err != nil {
			return nil, false, &ConsumerError{Cause: err}
		}
		return nil, false, nil
	})
}
func sameJSON(a, b json.RawMessage) bool {
	var left, right any
	return json.Unmarshal(a, &left) == nil && json.Unmarshal(b, &right) == nil && reflect.DeepEqual(left, right)
}

// The frame budget is bounded; the lifetime of a healthy stream is not capped
// by a total byte budget. The request context controls socket/parser lifetime.
func readSubscriptionSSE(reader io.Reader, frame func(json.RawMessage) (json.RawMessage, bool, error)) (json.RawMessage, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), tasks.MaxJSONBytes)
	var data strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if data.Len() == 0 {
				continue
			}
			raw := json.RawMessage(data.String())
			data.Reset()
			result, done, err := frame(raw)
			if err != nil || done {
				return result, err
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			if data.Len() > tasks.MaxJSONBytes {
				return nil, ErrStreamProtocol
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, ErrStreamProtocol
		}
		return nil, err
	}
	return nil, io.ErrUnexpectedEOF
}
