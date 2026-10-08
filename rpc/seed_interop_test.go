package rpc_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/viant/jsonrpc/transport"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	taskclient "github.com/viant/mcp-tasks/client"
	"github.com/viant/mcp-tasks/reference"
	"github.com/viant/mcp-tasks/rpc"
)

type alreadyCompletedBackend struct{ *reference.Service }

func (b alreadyCompletedBackend) Create(ctx context.Context, owner tasks.Owner, caps schema.ClientCapabilities, options tasks.CreateOptions) (tasks.CreateTaskResult, error) {
	seed, err := b.Service.Create(ctx, owner, caps, options)
	if err != nil {
		return seed, err
	}
	if err = b.Complete(ctx, owner, seed.TaskID, json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"original completed result"}],"isError":true}`)); err != nil {
		return seed, err
	}
	state, err := b.Get(ctx, owner, seed.TaskID)
	return tasks.CreateTaskResult{ResultType: tasks.ResultTask, DetailedTask: state.DetailedTask}, err
}

func TestRealHTTPCompletedCreationSeedRetrievesOriginalResult(t *testing.T) {
	base, service, _ := host(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpc.HTTPHandler{Build: func(_ context.Context, notifier transport.Notifier) transport.Handler {
			return &rpc.Handler{Next: base.Next, Service: alreadyCompletedBackend{service}, Owner: base.Owner, Decide: base.Decide}
		}}.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerKey{}, alice)))
	}))
	defer server.Close()
	caller, _ := rpc.NewHTTPCaller(server.URL, server.Client())
	client, _ := taskclient.New(caller, options(true))
	outcome, err := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "optional_task"})
	if err != nil || outcome.Task == nil || outcome.Task.Status != tasks.Completed || len(outcome.Task.Result) != 0 {
		t.Fatal("completed Task seed rejected or leaked detailed payload", err)
	}
	result, err := client.Wait(context.Background(), outcome.Task.TaskID, taskclient.WaitOptions{Initial: &outcome.Task.Task})
	if err != nil || string(result) != `{"resultType":"complete","content":[{"type":"text","text":"original completed result"}],"isError":true}` {
		t.Fatal("did not retrieve original tool-level error result", string(result), err)
	}
}
