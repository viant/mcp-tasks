package rpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	tasks "github.com/viant/mcp-tasks"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
)

type Request struct {
	Method  string
	Params  json.RawMessage
	Headers http.Header
}
type Caller interface {
	Call(context.Context, Request) (json.RawMessage, error)
}
type CallerFunc func(context.Context, Request) (json.RawMessage, error)

func (f CallerFunc) Call(ctx context.Context, request Request) (json.RawMessage, error) {
	return f(ctx, request)
}

// TransportCaller uses a supplied MCP transport. For HTTP, its header provider
// must consume HeadersFromContext; stdio needs no routing headers. It adds no
// retry policy and does not change the transport's authentication context.
type TransportCaller struct {
	Transport transport.Transport
	sequence  atomic.Uint64
}
type headersKey struct{}

func HeadersFromContext(ctx context.Context) http.Header {
	headers, _ := ctx.Value(headersKey{}).(http.Header)
	return headers.Clone()
}
func (c *TransportCaller) Call(ctx context.Context, request Request) (json.RawMessage, error) {
	if c.Transport == nil {
		return nil, errors.New("MCP transport unavailable")
	}
	id := fmt.Sprintf("tasks-%d", c.sequence.Add(1))
	req, err := jsonrpc.NewRequest(request.Method, request.Params)
	if err != nil {
		return nil, err
	}
	req.Id = id
	response, err := c.Transport.Send(context.WithValue(ctx, headersKey{}, request.Headers.Clone()), req)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("missing JSON-RPC response")
	}
	if response.Jsonrpc != "2.0" || response.Id != req.Id {
		return nil, errors.New("JSON-RPC response identity mismatch")
	}
	if len(response.Result) > tasks.MaxJSONBytes {
		return nil, errors.New("JSON-RPC response exceeds budget")
	}
	if response.Error != nil {
		return nil, &tasks.RPCError{Code: response.Error.Code, Message: response.Error.Message, Data: response.Error.Data}
	}
	return append(json.RawMessage(nil), response.Result...), nil
}

// HTTPCaller is a bounded JSON/SSE request adapter, with no retries or redirects.
// Configure host authentication through Client.Transport or HeaderProvider.
type HTTPCaller struct {
	endpoint       string
	client         *http.Client
	HeaderProvider func(context.Context) (http.Header, error)
	sequence       atomic.Uint64
}

func NewHTTPCaller(endpoint string, client *http.Client) (*HTTPCaller, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, errors.New("valid MCP HTTP(S) endpoint required")
	}
	if client == nil {
		client = &http.Client{}
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &HTTPCaller{endpoint: endpoint, client: &copy}, nil
}
func (c *HTTPCaller) Call(ctx context.Context, request Request) (json.RawMessage, error) {
	id := fmt.Sprintf("tasks-%d", c.sequence.Add(1))
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": request.Method, "params": request.Params})
	if err != nil {
		return nil, err
	}
	if len(body) > tasks.MaxJSONBytes {
		return nil, errors.New("RPC request exceeds budget")
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
	req.Header.Set("Mcp-Method", request.Method)
	req.Header.Set("Mcp-Protocol-Version", tasks.ProtocolVersion)
	if request.Method == tasks.MethodGet || request.Method == tasks.MethodUpdate || request.Method == tasks.MethodCancel {
		var params tasks.GetParams
		if json.Unmarshal(request.Params, &params) != nil || !tasks.ValidID(params.TaskID) {
			return nil, errors.New("invalid task routing identity")
		}
		req.Header.Set("Mcp-Name", EncodeHeaderValue(params.TaskID))
	}
	if request.Method == tasks.MethodCallTool {
		var named struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(request.Params, &named) != nil || !tasks.ValidID(named.Name) {
			return nil, errors.New("invalid tool routing identity")
		}
		req.Header.Set("Mcp-Name", EncodeHeaderValue(named.Name))
	}
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
	if strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		return readSSE(response.Body, id)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, tasks.MaxJSONBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > tasks.MaxJSONBytes {
		return nil, errors.New("RPC response exceeds budget")
	}
	return decodeResponse(raw, id)
}
func decodeResponse(raw json.RawMessage, id string) (json.RawMessage, error) {
	var response struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      string          `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *tasks.RPCError `json:"error"`
	}
	if json.Unmarshal(raw, &response) != nil || response.JSONRPC != "2.0" || response.ID != id || (len(response.Result) > 0) == (response.Error != nil) {
		return nil, errors.New("invalid JSON-RPC response")
	}
	if response.Error != nil {
		return nil, response.Error
	}
	if !tasks.ValidObject(response.Result) {
		return nil, errors.New("JSON-RPC result must be an object")
	}
	return response.Result, nil
}
func readSSE(reader io.Reader, id string) (json.RawMessage, error) {
	scanner := bufio.NewScanner(io.LimitReader(reader, tasks.MaxJSONBytes*2+1))
	scanner.Buffer(make([]byte, 4096), tasks.MaxJSONBytes)
	var data strings.Builder
	total := 0
	for scanner.Scan() {
		line := scanner.Text()
		total += len(line) + 1
		if total > tasks.MaxJSONBytes*2 {
			return nil, errors.New("SSE response exceeds budget")
		}
		if line == "" {
			if data.Len() == 0 {
				continue
			}
			raw := []byte(data.String())
			data.Reset()
			var frame map[string]json.RawMessage
			if json.Unmarshal(raw, &frame) != nil {
				return nil, errors.New("invalid SSE JSON")
			}
			if _, hasID := frame["id"]; hasID {
				return decodeResponse(raw, id)
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			if data.Len() > tasks.MaxJSONBytes {
				return nil, errors.New("SSE frame exceeds budget")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("SSE response ended before RPC result")
}
