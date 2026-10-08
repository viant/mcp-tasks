package rpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	taskclient "github.com/viant/mcp-tasks/client"
	"github.com/viant/mcp-tasks/rpc"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPSubscriptionRejectsBeforeAckWrongIDAndUnrequestedMethods(t *testing.T) {
	for _, test := range []struct {
		name, method string
		wrongID      bool
	}{{"before ack", tasks.MethodNotification, false}, {"wrong subscription", tasks.MethodAcknowledged, true}, {"unrequested logging", "notifications/message", false}} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					ID json.RawMessage `json:"id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				id := req.ID
				if test.wrongID {
					id = json.RawMessage(`9999`)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if test.method == "notifications/message" {
					fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"method\":%q,\"params\":{\"_meta\":{\"io.modelcontextprotocol/subscriptionId\":%s}}}\n\n", tasks.MethodAcknowledged, id)
				}
				fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"method\":%q,\"params\":{\"_meta\":{\"io.modelcontextprotocol/subscriptionId\":%s}}}\n\n", test.method, id)
			}))
			defer server.Close()
			caller, _ := rpc.NewHTTPCaller(server.URL, server.Client())
			_, err := caller.Listen(context.Background(), rpc.Request{Method: tasks.MethodListen, Params: json.RawMessage(`{}`)}, func(json.RawMessage) error { return nil })
			if !errors.Is(err, rpc.ErrStreamProtocol) {
				t.Fatal("invalid stream accepted", err)
			}
		})
	}
}
func TestHTTPSubscriptionBoundsFramesNotStreamLifetime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		id := req.ID
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"method\":%q,\"params\":{\"notifications\":{\"taskIds\":[\"fixture\"]},\"_meta\":{\"io.modelcontextprotocol/subscriptionId\":%s}}}\n\n", tasks.MethodAcknowledged, id)
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"method\":%q,\"params\":{\"statusMessage\":%q,\"_meta\":{\"io.modelcontextprotocol/subscriptionId\":%s}}}\n\n", tasks.MethodNotification, strings.Repeat("x", 3<<20), id)
		}
		fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"resultType\":\"complete\",\"_meta\":{\"io.modelcontextprotocol/subscriptionId\":%s}}}\n\n", id, id)
	}))
	defer server.Close()
	caller, _ := rpc.NewHTTPCaller(server.URL, server.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count := 0
	_, err := caller.Listen(ctx, rpc.Request{Method: tasks.MethodListen, Params: json.RawMessage(`{}`)}, func(json.RawMessage) error { count++; return nil })
	if err != nil || count != 4 {
		t.Fatal("healthy stream was capped by lifetime bytes", count, err)
	}
}

func TestTypedHTTPSubscriptionRejectsMissingRequiredTaskField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"method\":%q,\"params\":{\"notifications\":{\"taskIds\":[\"fixture\"]},\"_meta\":{\"io.modelcontextprotocol/subscriptionId\":%s}}}\n\n", tasks.MethodAcknowledged, req.ID)
		fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"method\":%q,\"params\":{\"taskId\":\"fixture\",\"status\":\"working\",\"createdAt\":\"2026-10-08T00:00:00Z\",\"lastUpdatedAt\":\"2026-10-08T00:00:00Z\",\"_meta\":{\"io.modelcontextprotocol/subscriptionId\":%s}}}\n\n", tasks.MethodNotification, req.ID)
	}))
	defer server.Close()
	client := subscriptionClient(t, server.URL, server.Client(), schema.ClientCapabilities{})
	delivered := false
	err := client.Subscribe(context.Background(), []string{"fixture"}, taskclient.SubscriptionOptions{OnTask: func(tasks.DetailedTask) error { delivered = true; return nil }})
	if !errors.Is(err, rpc.ErrStreamProtocol) || delivered {
		t.Fatal("invalid full state reached observer", err)
	}
}
