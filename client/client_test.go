package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	client "github.com/viant/mcp-tasks/client"
	"github.com/viant/mcp-tasks/rpc"
	"io"
	"testing"
	"time"
)

func encoded(state tasks.DetailedTask) json.RawMessage {
	raw, _ := json.Marshal(tasks.GetTaskResult{ResultType: tasks.ResultComplete, DetailedTask: state})
	return raw
}
func state(status tasks.Status) tasks.DetailedTask {
	interval := int64(20)
	return tasks.DetailedTask{Task: tasks.Task{TaskID: "fixture-task", Status: status, CreatedAt: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC), LastUpdatedAt: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC), TTLMS: nil, PollIntervalMS: &interval}}
}
func requester(t *testing.T, caller rpc.Caller, optIn bool, wait func(context.Context, time.Duration) error) *client.Client {
	t.Helper()
	c, err := client.New(caller, client.Options{Tasks: optIn, Info: schema.Implementation{Name: "fixture", Version: "1"}, Capabilities: schema.ClientCapabilities{Elicitation: &schema.ClientCapabilitiesElicitation{}}, Wait: wait})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestPollingHonorsChangingIntervalsAndDeduplicatesInputs(t *testing.T) {
	ctx := context.Background()
	working := state(tasks.Working)
	first := state(tasks.InputRequired)
	first.InputRequests = tasks.InputRequests{"elicitation-key": {Method: "elicitation/create", Params: json.RawMessage(`{"mode":"form","message":"fixture"}`)}}
	resumed := state(tasks.Working)
	interval := int64(40)
	resumed.PollIntervalMS = &interval
	done := state(tasks.Completed)
	done.Result = json.RawMessage(`{"resultType":"complete","content":[],"isError":true}`)
	sequence := []tasks.DetailedTask{working, first, first, resumed, done}
	get, updates, handled := 0, 0, 0
	var delays []time.Duration
	var checkpoints []client.Cursor
	caller := rpc.CallerFunc(func(_ context.Context, request rpc.Request) (json.RawMessage, error) {
		if request.Headers.Get("Mcp-Name") != "fixture-task" {
			t.Fatal("task routing header")
		}
		caps, err := tasks.RequestCapabilities(request.Params)
		if err != nil || !tasks.Supports(caps) {
			t.Fatal("per-request capabilities")
		}
		if request.Method == tasks.MethodUpdate {
			updates++
			return json.RawMessage(`{"resultType":"complete"}`), nil
		}
		result := encoded(sequence[get])
		get++
		return result, nil
	})
	c := requester(t, caller, true, func(_ context.Context, duration time.Duration) error { delays = append(delays, duration); return nil })
	cursor := &client.Cursor{TaskID: "fixture-task"}
	result, err := c.Wait(ctx, "fixture-task", client.WaitOptions{Cursor: cursor, Initial: &working.Task,
		Input: func(_ context.Context, key string, request tasks.InputRequest) (json.RawMessage, error) {
			handled++
			if key != "elicitation-key" || request.Method != "elicitation/create" {
				t.Fatal("input identity")
			}
			return json.RawMessage(`{"action":"accept","content":{"fixture":true}}`), nil
		},
		SaveCursor: func(_ context.Context, cursor client.Cursor) error {
			checkpoints = append(checkpoints, cursor)
			return nil
		},
	})
	if err != nil || string(result) != string(done.Result) || updates != 1 || handled != 1 || get != 5 {
		t.Fatal("polling/input flow", err, get, updates, handled)
	}
	if len(delays) != 5 || delays[len(delays)-1] != 40*time.Millisecond {
		t.Fatal("mutable polling intervals", delays)
	}
	if cursor.Attempts["elicitation-key"].Stage != "answered" || len(checkpoints) == 0 {
		t.Fatal("input checkpoint")
	}
}
func TestUnknownInputUpdateIsNeverReplayed(t *testing.T) {
	required := state(tasks.InputRequired)
	required.InputRequests = tasks.InputRequests{"key": {Method: "elicitation/create", Params: json.RawMessage(`{"mode":"form"}`)}}
	updates, handled := 0, 0
	caller := rpc.CallerFunc(func(_ context.Context, request rpc.Request) (json.RawMessage, error) {
		if request.Method == tasks.MethodUpdate {
			updates++
			return nil, io.ErrUnexpectedEOF
		}
		return encoded(required), nil
	})
	c := requester(t, caller, true, func(context.Context, time.Duration) error { return nil })
	cursor := &client.Cursor{TaskID: "fixture-task"}
	options := client.WaitOptions{Cursor: cursor, Input: func(context.Context, string, tasks.InputRequest) (json.RawMessage, error) {
		handled++
		return json.RawMessage(`{"action":"accept"}`), nil
	}}
	_, err := c.Wait(context.Background(), "fixture-task", options)
	var unknown *client.UncertainError
	if !errors.As(err, &unknown) || updates != 1 || handled != 1 {
		t.Fatal("lost update", err)
	}
	_, err = c.Wait(context.Background(), "fixture-task", options)
	if !errors.Is(err, client.ErrUncertainInput) || updates != 1 || handled != 1 {
		t.Fatal("replayed uncertain input", err)
	}
}
func TestNormalResultsAndOriginalCallNoReplay(t *testing.T) {
	calls := 0
	regular := json.RawMessage(`{"content":[{"type":"text","text":"fixture"}],"isError":false}`)
	caller := rpc.CallerFunc(func(_ context.Context, request rpc.Request) (json.RawMessage, error) {
		calls++
		caps, _ := tasks.RequestCapabilities(request.Params)
		if tasks.Supports(caps) {
			t.Fatal("unexpected Tasks opt-in")
		}
		return regular, nil
	})
	c := requester(t, caller, false, nil)
	outcome, err := c.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "regular"})
	if err != nil || outcome.Task != nil || string(outcome.Result) != string(regular) {
		t.Fatal("regular result changed", err)
	}
	failing := requester(t, rpc.CallerFunc(func(context.Context, rpc.Request) (json.RawMessage, error) { calls++; return nil, io.EOF }), true, nil)
	_, err = failing.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "slow"})
	var unknown *client.UncertainError
	if !errors.As(err, &unknown) || calls != 2 {
		t.Fatal("original call replayed", calls, err)
	}
}
func TestCancellationAckAndObserverDisconnect(t *testing.T) {
	methods := []string{}
	caller := rpc.CallerFunc(func(ctx context.Context, request rpc.Request) (json.RawMessage, error) {
		methods = append(methods, request.Method)
		if request.Method == tasks.MethodCancel {
			return json.RawMessage(`{"resultType":"complete"}`), nil
		}
		return encoded(state(tasks.Working)), nil
	})
	c := requester(t, caller, true, func(ctx context.Context, _ time.Duration) error { <-ctx.Done(); return ctx.Err() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Wait(ctx, "fixture-task", client.WaitOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("disconnect not honored", err)
	}
	for _, method := range methods {
		if method == tasks.MethodCancel {
			t.Fatal("disconnect cancelled server task")
		}
	}
	if err = c.Cancel(context.Background(), "fixture-task"); err != nil {
		t.Fatal(err)
	}
	if methods[len(methods)-1] != tasks.MethodCancel {
		t.Fatal("wrong task cancellation method")
	}
}
func TestRejectWrongTaskIdentityChangedInputAndUnnegotiatedHandle(t *testing.T) {
	working := state(tasks.Working)
	working.TaskID = "other-task"
	c := requester(t, rpc.CallerFunc(func(context.Context, rpc.Request) (json.RawMessage, error) { return encoded(working), nil }), true, nil)
	if _, err := c.Get(context.Background(), "fixture-task"); !errors.Is(err, client.ErrInvalidPeer) {
		t.Fatal("foreign response", err)
	}
	raw, _ := json.Marshal(tasks.CreateTaskResult{ResultType: tasks.ResultTask, DetailedTask: state(tasks.Working)})
	c = requester(t, rpc.CallerFunc(func(context.Context, rpc.Request) (json.RawMessage, error) { return raw, nil }), false, nil)
	if _, err := c.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "fixture"}); !errors.Is(err, client.ErrInvalidPeer) {
		t.Fatal("unnegotiated handle", err)
	}
	c = requester(t, rpc.CallerFunc(func(context.Context, rpc.Request) (json.RawMessage, error) {
		return json.RawMessage(`{"resultType":"unnegotiated-extension","content":[]}`), nil
	}), true, nil)
	if _, err := c.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "fixture"}); !errors.Is(err, client.ErrInvalidPeer) {
		t.Fatal("unknown result discriminator accepted", err)
	}
}
