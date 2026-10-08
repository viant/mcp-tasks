package rpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	tasks "github.com/viant/mcp-tasks"
	"github.com/viant/mcp-tasks/rpc"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPCallerSSEAndResponseCorrelation(t *testing.T) {
	for _, test := range []struct {
		name             string
		badID, truncated bool
	}{{"complete", false, false}, {"wrong response", true, false}, {"truncated", false, true}} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var request struct {
					ID string `json:"id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&request)
				if r.Header.Get("Mcp-Name") != "fixture-task" || r.Header.Get("Mcp-Method") != tasks.MethodGet {
					t.Error("missing task HTTP routing")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"fixture/notification\"}\n\n")
				if test.truncated {
					return
				}
				id := request.ID
				if test.badID {
					id = "different"
				}
				fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":%q,\"result\":{\"resultType\":\"complete\"}}\n\n", id)
			}))
			defer server.Close()
			caller, err := rpc.NewHTTPCaller(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			_, err = caller.Call(context.Background(), rpc.Request{Method: tasks.MethodGet, Params: json.RawMessage(`{"taskId":"fixture-task"}`)})
			if test.badID || test.truncated {
				if err == nil {
					t.Fatal("invalid SSE response accepted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("transport retried an RPC", calls)
			}
		})
	}
}
