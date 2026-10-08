package rpc_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	mcp "github.com/viant/mcp"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	"github.com/viant/mcp-tasks/rpc"
)

type ordinaryHTTPClientHandler struct{ sequence atomic.Uint64 }

func (*ordinaryHTTPClientHandler) Notify(context.Context, *jsonrpc.Notification) error   { return nil }
func (h *ordinaryHTTPClientHandler) NextRequestID() jsonrpc.RequestId                    { return h.sequence.Add(1) }
func (h *ordinaryHTTPClientHandler) LastRequestID() jsonrpc.RequestId                    { return h.sequence.Load() }
func (*ordinaryHTTPClientHandler) Implements(string) bool                                { return false }
func (*ordinaryHTTPClientHandler) Init(context.Context, *schema.ClientCapabilities)      {}
func (*ordinaryHTTPClientHandler) OnNotification(context.Context, *jsonrpc.Notification) {}
func (*ordinaryHTTPClientHandler) ListRoots(context.Context, *jsonrpc.TypedRequest[*schema.ListRootsRequest]) (*schema.ListRootsResult, *jsonrpc.Error) {
	return nil, jsonrpc.NewMethodNotFound("unsupported", nil)
}
func (*ordinaryHTTPClientHandler) CreateMessage(context.Context, *jsonrpc.TypedRequest[*schema.CreateMessageRequest]) (*schema.CreateMessageResult, *jsonrpc.Error) {
	return nil, jsonrpc.NewMethodNotFound("unsupported", nil)
}
func (*ordinaryHTTPClientHandler) Elicit(context.Context, *jsonrpc.TypedRequest[*schema.ElicitRequest]) (*schema.ElicitResult, *jsonrpc.Error) {
	return nil, jsonrpc.NewMethodNotFound("unsupported", nil)
}

func TestRealHighLevelMCPHTTPClientKeepsOrdinaryCallsWithoutTasks(t *testing.T) {
	base, _, _ := host(t)
	var discoveries, calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Mcp-Method") == "server/discover" {
			discoveries.Add(1)
		}
		if r.Header.Get("Mcp-Method") == "tools/call" {
			calls.Add(1)
		}
		rpc.HTTPHandler{Build: func(_ context.Context, n transport.Notifier) transport.Handler { return base }}.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerKey{}, alice)))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := mcp.NewClientWithContext(ctx, &ordinaryHTTPClientHandler{}, &mcp.ClientOptions{Name: "ordinary-http", Version: "1", ProtocolVersion: tasks.ProtocolVersion, Transport: mcp.ClientTransport{Type: "streamable", ClientTransportHTTP: mcp.ClientTransportHTTP{URL: server.URL}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: "regular"})
	if err != nil || result.ResultType != "complete" || len(result.Content) != 1 || discoveries.Load() != 1 || calls.Load() != 1 {
		t.Fatal("ordinary HTTP contract changed", err, discoveries.Load(), calls.Load())
	}
}
