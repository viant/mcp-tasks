package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	taskclient "github.com/viant/mcp-tasks/client"
	"github.com/viant/mcp-tasks/rpc"
)

func TestHTTPRequiredMetadataOptionalClientInfoAndErrors(t *testing.T) {
	base, _, _ := host(t)
	var builds atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpc.HTTPHandler{Build: func(_ context.Context, n transport.Notifier) transport.Handler { builds.Add(1); return base }}.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerKey{}, alice)))
	}))
	defer server.Close()
	caps := schema.ClientCapabilities{}
	tasks.Advertise(&caps)
	metadata := map[string]any{"io.modelcontextprotocol/protocolVersion": tasks.ProtocolVersion, tasks.ClientCapabilitiesMeta: caps}
	send := func(meta map[string]any, change func(*http.Request)) (int, *jsonrpc.Error) {
		raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "owned-request", "method": tasks.MethodGet, "params": map[string]any{"taskId": "missing-task", "_meta": meta}})
		req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(raw))
		req.Header.Set("Mcp-Protocol-Version", tasks.ProtocolVersion)
		req.Header.Set("Mcp-Method", tasks.MethodGet)
		req.Header.Set("Mcp-Name", "missing-task")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if change != nil {
			change(req)
		}
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var wire jsonrpc.Response
		if response.StatusCode != http.StatusForbidden {
			if err := json.NewDecoder(response.Body).Decode(&wire); err != nil {
				t.Fatal(err)
			}
		}
		return response.StatusCode, wire.Error
	}
	// Optional clientInfo omitted: request reaches the actual owned backend.
	status, wireErr := send(metadata, nil)
	if status != 400 || wireErr == nil || wireErr.Code != -32602 || builds.Load() != 1 {
		t.Fatal("optional clientInfo rejected before backend", status, wireErr, builds.Load())
	}
	before := builds.Load()
	for _, mutation := range []func(*http.Request){func(r *http.Request) { r.Header.Del("Mcp-Method") }, func(r *http.Request) { r.Header.Set("Mcp-Name", "foreign-task") }, func(r *http.Request) { r.Header.Set("Mcp-Protocol-Version", "2025-11-25") }} {
		status, wireErr = send(metadata, mutation)
		if status != 400 || wireErr == nil || wireErr.Code != -32020 || builds.Load() != before {
			t.Fatal("routing admitted work", status, wireErr)
		}
	}
	malformed := map[string]any{"io.modelcontextprotocol/protocolVersion": tasks.ProtocolVersion}
	status, wireErr = send(malformed, nil)
	if status != 400 || wireErr == nil || wireErr.Code != -32602 || builds.Load() != before {
		t.Fatal("missing required capabilities object admitted", status, wireErr)
	}
	status, _ = send(metadata, func(r *http.Request) { r.Header.Set("Origin", "https://foreign.example") })
	if status != 403 || builds.Load() != before {
		t.Fatal("unapproved Origin admitted")
	}
	// Missing extension is a correlated protocol error even on HTTP 400; no
	// transport uncertainty or retry is introduced by the typed client.
	caller, _ := rpc.NewHTTPCaller(server.URL, server.Client())
	params, _ := json.Marshal(map[string]any{"taskId": "missing-task", "_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": tasks.ProtocolVersion, tasks.ClientCapabilitiesMeta: map[string]any{}}})
	_, err := caller.Call(context.Background(), rpc.Request{Method: tasks.MethodGet, Params: params})
	var protocol *tasks.RPCError
	if !errors.As(err, &protocol) || protocol.Code != -32021 {
		t.Fatal("lost HTTP protocol error", err)
	}
	client, _ := taskclient.New(caller, options(true))
	_, err = client.Get(context.Background(), "missing-task")
	if !errors.As(err, &protocol) || protocol.Code != -32602 {
		t.Fatal("typed client lost HTTP 400 error", err)
	}
	_, err = client.Get(context.Background(), "task-世界")
	if !errors.As(err, &protocol) || protocol.Code != -32602 {
		t.Fatal("Unicode task identity/header was changed", err)
	}
	outcome, err := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "工具"})
	if err != nil || outcome.Task != nil || len(outcome.Result) == 0 {
		t.Fatal("Unicode ordinary tool routing failed", err)
	}
	params, _ = json.Marshal(map[string]any{"_meta": metadata})
	_, err = caller.Call(context.Background(), rpc.Request{Method: "unregistered/method", Params: params})
	if !errors.As(err, &protocol) || protocol.Code != -32601 {
		t.Fatal("lost HTTP 404 method error", err)
	}
}

func TestHeaderSentinelAndNonNullRequestIdentity(t *testing.T) {
	for _, value := range []string{"plain", "Hello, 世界", " padded ", "line1\nline2", "=?base64?literal?="} {
		wire := rpc.EncodeHeaderValue(value)
		decoded, err := rpc.DecodeHeaderValue(wire)
		if err != nil || decoded != value {
			t.Fatal("header identity changed", err)
		}
		if value != "plain" && wire == value {
			t.Fatal("unsafe header not encoded")
		}
	}
	for _, value := range []string{"=?base64?invalid!?=", "plain\nheader", "世界"} {
		if _, err := rpc.DecodeHeaderValue(value); err == nil {
			t.Fatal("malformed header accepted")
		}
	}
	for _, id := range []any{nil, true, 1.5, map[string]any{}} {
		if rpc.ValidRequestID(id) {
			t.Fatal("invalid request identity", id)
		}
	}
	for _, id := range []any{"", "request", 0, -1, int64(42)} {
		if !rpc.ValidRequestID(id) {
			t.Fatal("valid request identity rejected", id)
		}
	}
}
