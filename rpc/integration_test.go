package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	protocli "github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/logger"
	"github.com/viant/mcp-protocol/schema"
	protoserver "github.com/viant/mcp-protocol/server"
	tasks "github.com/viant/mcp-tasks"
	taskclient "github.com/viant/mcp-tasks/client"
	"github.com/viant/mcp-tasks/reference"
	"github.com/viant/mcp-tasks/rpc"
	"github.com/viant/mcp-tasks/store"
	mcpclient "github.com/viant/mcp/client"
	mcpserver "github.com/viant/mcp/server"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type ownerKey struct{}

var alice = tasks.Owner{Tenant: "fixture", Subject: "alice"}

type loopback struct {
	handler  transport.Handler
	calls    []string
	sequence atomic.Uint64
}

func (l *loopback) Send(ctx context.Context, request *jsonrpc.Request) (*jsonrpc.Response, error) {
	// Real transports allocate missing IDs before dispatch; emulate that wire
	// boundary rather than admitting a nil-ID high-level client request.
	if request.Id == nil {
		request.Id = l.sequence.Add(1)
	}
	l.calls = append(l.calls, request.Method)
	response := &jsonrpc.Response{Jsonrpc: "2.0", Id: request.Id}
	l.handler.Serve(ctx, request, response)
	return response, nil
}
func (*loopback) Notify(context.Context, *jsonrpc.Notification) error { return nil }
func host(t *testing.T) (*rpc.Handler, *reference.Service, *loopback) {
	t.Helper()
	file, err := store.Open(t.TempDir(), 64)
	if err != nil {
		t.Fatal(err)
	}
	service, err := reference.New(file, reference.Options{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := mcpserver.New(mcpserver.WithNewHandler(func(_ context.Context, _ transport.Notifier, _ logger.Logger, client protocli.Operations) (protoserver.Handler, error) {
		registry := protoserver.NewRegistry()
		err := protoserver.RegisterTool[struct{}, struct{}](registry, "regular", "Generic immediate fixture", func(context.Context, struct{}) (*schema.CallToolResult, *jsonrpc.Error) {
			return &schema.CallToolResult{ResultType: "complete", Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: "regular fixture"}}}, nil
		})
		return &protoserver.DefaultHandler{Registry: registry, Client: client}, err
	}))
	if err != nil {
		t.Fatal(err)
	}
	loop := &loopback{}
	next := server.NewHandler(context.Background(), loop)
	handler := &rpc.Handler{Next: next, Service: service, Owner: func(ctx context.Context, _ *jsonrpc.Request) (tasks.Owner, error) {
		owner, _ := ctx.Value(ownerKey{}).(tasks.Owner)
		if !owner.Valid() {
			return tasks.Owner{}, errors.New("fixture authentication required")
		}
		return owner, nil
	}, Decide: func(_ context.Context, _ tasks.Owner, req *jsonrpc.Request) (rpc.Decision, error) {
		var params struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(req.Params, &params) != nil {
			return rpc.Decision{}, &tasks.RPCError{Code: -32602, Message: "invalid fixture tool params"}
		}
		if params.Name == "required_task" {
			return rpc.Decision{Required: true, Create: &tasks.CreateOptions{Origin: json.RawMessage(`{"nativeRunId":"existing-run"}`)}}, nil
		}
		if params.Name == "optional_task" {
			return rpc.Decision{Create: &tasks.CreateOptions{}}, nil
		}
		return rpc.Decision{}, nil
	}}
	loop.handler = handler
	return handler, service, loop
}
func options(tasksEnabled bool) taskclient.Options {
	return taskclient.Options{Tasks: tasksEnabled, Info: schema.Implementation{Name: "fixture", Version: "1"}, Capabilities: schema.ClientCapabilities{Elicitation: &schema.ClientCapabilitiesElicitation{}}}
}
func TestRealMCPHandlerAndLegacyClientWithOptInTaskClient(t *testing.T) {
	_, service, loop := host(t)
	ctx := context.WithValue(context.Background(), ownerKey{}, alice)
	// A real high-level MCP client keeps its ordinary fixed tools/call contract.
	legacy := mcpclient.New("legacy-fixture", "1", loop, mcpclient.WithProtocolVersion(tasks.ProtocolVersion))
	if _, err := legacy.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	regular, err := legacy.CallTool(ctx, &schema.CallToolRequestParams{Name: "regular"})
	if err != nil || regular.ResultType != "complete" {
		t.Fatal("ordinary MCP call changed", err)
	}
	if _, err = legacy.CallTool(ctx, &schema.CallToolRequestParams{Name: "required_task"}); err == nil {
		t.Fatal("required task capability not enforced")
	}
	caller := &rpc.TransportCaller{Transport: loop}
	client, err := taskclient.New(caller, options(true))
	if err != nil {
		t.Fatal(err)
	}
	discovery, err := client.Discover(ctx)
	if err != nil || !tasks.ServerSupports(discovery.Capabilities) {
		t.Fatal("server/discover Tasks capability", err)
	}
	outcome, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: "required_task"})
	if err != nil || outcome.Task == nil {
		t.Fatal("task-aware tool call", err)
	}
	task := outcome.Task
	state, err := client.Get(ctx, task.TaskID)
	if err != nil || state.Status != tasks.Working {
		t.Fatal("durable handle not immediately readable", err)
	}
	request := tasks.InputRequest{Method: "elicitation/create", Params: json.RawMessage(`{"mode":"form","message":"fixture"}`)}
	if err = service.RequireInput(ctx, alice, task.TaskID, tasks.InputRequests{"input": request}); err != nil {
		t.Fatal(err)
	}
	state, err = client.Get(ctx, task.TaskID)
	if err != nil || state.Status != tasks.InputRequired {
		t.Fatal("task input", err)
	}
	if err = client.Update(ctx, task.TaskID, tasks.InputResponses{"input": json.RawMessage(`{"action":"accept","content":{"fixture":true}}`)}); err != nil {
		t.Fatal(err)
	}
	final := json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"task fixture"}],"isError":true}`)
	if err = service.Complete(ctx, alice, task.TaskID, final); err != nil {
		t.Fatal(err)
	}
	result, err := client.Wait(ctx, task.TaskID, taskclient.WaitOptions{})
	if err != nil || string(result) != string(final) {
		t.Fatal("final original result", err)
	}
	foreign := context.WithValue(context.Background(), ownerKey{}, tasks.Owner{Tenant: "fixture", Subject: "bob"})
	_, err = client.Get(foreign, task.TaskID)
	var rpcError *tasks.RPCError
	if !errors.As(err, &rpcError) || rpcError.Code != -32602 {
		t.Fatal("foreign task disclosure", err)
	}
}
func TestHTTPRoutingAndPerRequestNegotiation(t *testing.T) {
	handler, service, _ := host(t)
	var taskID string
	methods := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var req jsonrpc.Request
		if json.Unmarshal(raw, &req) != nil {
			t.Error("invalid JSON-RPC request")
			return
		}
		methods = append(methods, req.Method)
		if r.Header.Get("Mcp-Method") != req.Method {
			t.Error("Mcp-Method missing")
		}
		if req.Method == tasks.MethodGet || req.Method == tasks.MethodUpdate || req.Method == tasks.MethodCancel {
			var params tasks.GetParams
			_ = json.Unmarshal(req.Params, &params)
			if r.Header.Get("Mcp-Name") != params.TaskID {
				t.Error("Mcp-Name must route taskId")
			}
		}
		if req.Method == tasks.MethodCallTool && r.Header.Get("Mcp-Name") != "required_task" {
			t.Error("normal tool routing missing")
		}
		ctx := context.WithValue(r.Context(), ownerKey{}, alice)
		response := &jsonrpc.Response{Jsonrpc: "2.0", Id: req.Id}
		handler.Serve(ctx, &req, response)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	caller, err := rpc.NewHTTPCaller(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, _ := taskclient.New(caller, options(true))
	outcome, err := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "required_task"})
	if err != nil {
		t.Fatal(err)
	}
	taskID = outcome.Task.TaskID
	if err = client.Cancel(context.Background(), taskID); err != nil {
		t.Fatal(err)
	}
	state, err := client.Get(context.Background(), taskID)
	if err != nil || state.Status != tasks.Working {
		t.Fatal("cancellation ack asserted terminal status", err)
	}
	record, _ := service.PrivateRecord(context.Background(), alice, taskID)
	if !record.CancelRequested {
		t.Fatal("cancel intent not durable")
	}
	// The next request has no capability despite the earlier negotiated request.
	params, _ := json.Marshal(map[string]any{"taskId": taskID, "_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": tasks.ProtocolVersion, tasks.ClientCapabilitiesMeta: map[string]any{}}})
	request := &jsonrpc.Request{Jsonrpc: "2.0", Id: 17, Method: tasks.MethodGet, Params: params}
	response := &jsonrpc.Response{}
	handler.Serve(context.WithValue(context.Background(), ownerKey{}, alice), request, response)
	if response.Error == nil || response.Error.Code != -32021 {
		t.Fatal("cached/session capability authorized a different request")
	}
	if len(methods) != 3 {
		t.Fatal("unexpected retries", methods)
	}
}
func TestMalformedUpdateAndTaskSubscriptions(t *testing.T) {
	handler, _, _ := host(t)
	ctx := context.WithValue(context.Background(), ownerKey{}, alice)
	call := func(method string, params map[string]any) *jsonrpc.Response {
		raw, _ := json.Marshal(params)
		response := &jsonrpc.Response{}
		handler.Serve(ctx, &jsonrpc.Request{Jsonrpc: "2.0", Id: 1, Method: method, Params: raw}, response)
		return response
	}
	noCapability := call("subscriptions/listen", map[string]any{"notifications": map[string]any{"taskIds": []string{"fixture"}}, "_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": tasks.ProtocolVersion, tasks.ClientCapabilitiesMeta: map[string]any{}}})
	if noCapability.Error == nil || noCapability.Error.Code != -32021 {
		t.Fatal("subscription missing-capability code")
	}
	caps := schema.ClientCapabilities{}
	tasks.Advertise(&caps)
	meta := map[string]any{tasks.ClientCapabilitiesMeta: caps, "io.modelcontextprotocol/clientInfo": schema.Implementation{Name: "fixture", Version: "1"}, "io.modelcontextprotocol/protocolVersion": tasks.ProtocolVersion}
	configured := call("subscriptions/listen", map[string]any{"notifications": map[string]any{"taskIds": []string{"fixture"}}, "_meta": meta})
	if configured.Error == nil || configured.Error.Code != -32601 {
		t.Fatal("pretended optional stream support")
	}
	malformed := call(tasks.MethodUpdate, map[string]any{"taskId": "fixture", "inputResponses": "invalid", "_meta": meta})
	if malformed.Error == nil || malformed.Error.Code != -32602 {
		t.Fatal("invalid params became an execution error")
	}
}
