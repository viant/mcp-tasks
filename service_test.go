package tasks_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	"github.com/viant/mcp-tasks/reference"
	"github.com/viant/mcp-tasks/store"
	"sync"
	"testing"
	"time"
)

var owner = tasks.Owner{Tenant: "fixture-tenant", Subject: "fixture-owner"}

func capabilities() schema.ClientCapabilities {
	caps := schema.ClientCapabilities{Elicitation: &schema.ClientCapabilitiesElicitation{}, Sampling: &schema.ClientCapabilitiesSampling{}, Roots: &schema.ClientCapabilitiesRoots{}}
	tasks.Advertise(&caps)
	return caps
}
func service(t *testing.T, clock func() time.Time) (*reference.Service, *store.File) {
	t.Helper()
	file, err := store.Open(t.TempDir(), 64)
	if err != nil {
		t.Fatal(err)
	}
	ttl := int64(60000)
	svc, err := reference.New(file, reference.Options{Clock: clock, DefaultTTLMS: &ttl, PollIntervalMS: 25})
	if err != nil {
		t.Fatal(err)
	}
	return svc, file
}
func create(t *testing.T, svc *reference.Service) tasks.CreateTaskResult {
	t.Helper()
	result, err := svc.Create(context.Background(), owner, capabilities(), tasks.CreateOptions{Method: tasks.MethodCallTool, Origin: json.RawMessage(`{"nativeRunId":"fixture-native-run","executionMode":"detach"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestLifecycleInputsOwnershipAndToolErrors(t *testing.T) {
	ctx := context.Background()
	svc, _ := service(t, time.Now)
	created := create(t, svc)
	if len(created.TaskID) != 48 || created.ResultType != "task" {
		t.Fatal("task creation contract")
	}
	for _, other := range []tasks.Owner{{Tenant: "other", Subject: owner.Subject}, {Tenant: owner.Tenant, Subject: "other"}} {
		if _, err := svc.Get(ctx, other, created.TaskID); !errors.Is(err, tasks.ErrNotFound) {
			t.Fatal("foreign get")
		}
		if _, err := svc.Update(ctx, other, created.TaskID, tasks.InputResponses{}); !errors.Is(err, tasks.ErrNotFound) {
			t.Fatal("foreign update")
		}
		if _, err := svc.Cancel(ctx, other, created.TaskID); !errors.Is(err, tasks.ErrNotFound) {
			t.Fatal("foreign cancel")
		}
	}
	request := tasks.InputRequest{Method: "elicitation/create", Params: json.RawMessage(`{"mode":"form","message":"Fixture input","requestedSchema":{"type":"object"}}`)}
	if err := svc.RequireInput(ctx, owner, created.TaskID, tasks.InputRequests{"first": request, "second": request}); err != nil {
		t.Fatal(err)
	}
	response := json.RawMessage(`{"action":"accept","content":{"value":"fixture"}}`)
	ack, err := svc.Update(ctx, owner, created.TaskID, tasks.InputResponses{"first": response, "never-issued": response})
	if err != nil || ack.ResultType != "complete" {
		t.Fatal(err)
	}
	state, err := svc.Get(ctx, owner, created.TaskID)
	if err != nil || state.Status != tasks.InputRequired || len(state.InputRequests) != 1 {
		t.Fatal("partial response", err)
	}
	if _, err = svc.Update(ctx, owner, created.TaskID, tasks.InputResponses{"first": response}); err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Update(ctx, owner, created.TaskID, tasks.InputResponses{"second": response}); err != nil {
		t.Fatal(err)
	}
	state, _ = svc.Get(ctx, owner, created.TaskID)
	if state.Status != tasks.Working {
		t.Fatal("resume after all inputs")
	}
	if err = svc.RequireInput(ctx, owner, created.TaskID, tasks.InputRequests{"first": request}); !errors.Is(err, tasks.ErrKeyReuse) {
		t.Fatal("key reused", err)
	}
	result := json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"tool-level error"}],"isError":true,"_meta":{"fixture":"private-host-metadata"}}`)
	if err = svc.Complete(ctx, owner, created.TaskID, result); err != nil {
		t.Fatal(err)
	}
	state, _ = svc.Get(ctx, owner, created.TaskID)
	if state.Status != tasks.Completed || string(state.Result) != string(result) || state.Error != nil {
		t.Fatal("tool error must be completed")
	}
	if err = svc.Fail(ctx, owner, created.TaskID, &tasks.RPCError{Code: -32603, Message: "late failure"}); !errors.Is(err, tasks.ErrTerminal) {
		t.Fatal("terminal task changed")
	}
	record, _ := svc.PrivateRecord(ctx, owner, created.TaskID)
	if len(record.Responses) != 2 || len(record.Issued) != 2 {
		t.Fatal("accepted input durability")
	}
}
func TestInputRequestMutationAndMissingBaseCapability(t *testing.T) {
	ctx := context.Background()
	svc, _ := service(t, time.Now)
	caps := schema.ClientCapabilities{}
	tasks.Advertise(&caps)
	created, err := svc.Create(ctx, owner, caps, tasks.CreateOptions{Method: tasks.MethodCallTool})
	if err != nil {
		t.Fatal(err)
	}
	request := tasks.InputRequest{Method: "elicitation/create", Params: json.RawMessage(`{"mode":"form"}`)}
	if err = svc.RequireInput(ctx, owner, created.TaskID, tasks.InputRequests{"key": request}); err == nil {
		t.Fatal("unsupported input capability")
	}
	created = create(t, svc)
	if err = svc.RequireInput(ctx, owner, created.TaskID, tasks.InputRequests{"key": request}); err != nil {
		t.Fatal(err)
	}
	changed := request
	changed.Params = json.RawMessage(`{"mode":"url","url":"https://fixture.example"}`)
	if err = svc.RequireInput(ctx, owner, created.TaskID, tasks.InputRequests{"key": changed}); !errors.Is(err, tasks.ErrKeyReuse) {
		t.Fatal("outstanding key substituted", err)
	}
}
func TestCancelIsCooperativeAndTerminalRace(t *testing.T) {
	ctx := context.Background()
	svc, _ := service(t, time.Now)
	created := create(t, svc)
	ack, err := svc.Cancel(ctx, owner, created.TaskID)
	if err != nil || ack.ResultType != "complete" {
		t.Fatal(err)
	}
	state, _ := svc.Get(ctx, owner, created.TaskID)
	record, _ := svc.PrivateRecord(ctx, owner, created.TaskID)
	if state.Status != tasks.Working || !record.CancelRequested {
		t.Fatal("ack must not claim stopped work")
	}
	if err = svc.Complete(ctx, owner, created.TaskID, json.RawMessage(`{"resultType":"complete","content":[]}`)); err != nil {
		t.Fatal("completion may win cancellation", err)
	}
	created = create(t, svc)
	var group sync.WaitGroup
	group.Add(2)
	errorsOut := make(chan error, 2)
	go func() { defer group.Done(); errorsOut <- svc.MarkCancelled(ctx, owner, created.TaskID) }()
	go func() {
		defer group.Done()
		errorsOut <- svc.Complete(ctx, owner, created.TaskID, json.RawMessage(`{"resultType":"complete","content":[]}`))
	}()
	group.Wait()
	close(errorsOut)
	success, terminal := 0, 0
	for err := range errorsOut {
		if err == nil {
			success++
		} else if errors.Is(err, tasks.ErrTerminal) {
			terminal++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || terminal != 1 {
		t.Fatal("competing terminal transitions", success, terminal)
	}
}
func TestTTLAndProtocolFailure(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	svc, file := service(t, func() time.Time { return now })
	created := create(t, svc)
	ctx := context.Background()
	failure := &tasks.RPCError{Code: -32603, Message: "fixture protocol failure", Data: json.RawMessage(`{"reason":"fixture"}`)}
	if err := svc.Fail(ctx, owner, created.TaskID, failure); err != nil {
		t.Fatal(err)
	}
	notification, err := svc.Notification(ctx, owner, created.TaskID)
	if err != nil || notification.Method != "notifications/tasks" || notification.Params.Status != tasks.Failed || notification.Params.Error.Code != -32603 {
		t.Fatal("full-state notification", err)
	}
	now = now.Add(time.Minute)
	if _, err = svc.Get(ctx, owner, created.TaskID); !errors.Is(err, tasks.ErrNotFound) {
		t.Fatal("expired task usable", err)
	}
	removed, err := file.Sweep(ctx, now)
	if err != nil || removed != 1 {
		t.Fatal("TTL cleanup", removed, err)
	}
}

type invisibleStore struct{ creates int }

func (s *invisibleStore) Create(context.Context, tasks.Record) error { s.creates++; return nil }
func (s *invisibleStore) Get(context.Context, tasks.Owner, string) (tasks.Record, error) {
	return tasks.Record{}, tasks.ErrNotFound
}
func (*invisibleStore) CompareAndSwap(context.Context, tasks.Owner, string, uint64, tasks.Record) error {
	return nil
}
func TestCreationRequiresDurableVisibilityAndCapability(t *testing.T) {
	storage := &invisibleStore{}
	svc, _ := reference.New(storage, reference.Options{})
	ctx := context.Background()
	_, err := svc.Create(ctx, owner, schema.ClientCapabilities{}, tasks.CreateOptions{Method: tasks.MethodCallTool})
	var rpcError *tasks.RPCError
	if !errors.As(err, &rpcError) || rpcError.Code != -32021 || storage.creates != 0 {
		t.Fatal("unnegotiated creation")
	}
	result, err := svc.Create(ctx, owner, capabilities(), tasks.CreateOptions{Method: tasks.MethodCallTool})
	if !errors.Is(err, tasks.ErrDurability) || result.TaskID != "" || storage.creates != 1 {
		t.Fatal("speculative handle", err)
	}
}
