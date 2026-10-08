package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	taskclient "github.com/viant/mcp-tasks/client"
	"github.com/viant/mcp-tasks/rpc"
)

func subscriptionClient(t *testing.T, url string, httpClient *http.Client, caps schema.ClientCapabilities) *taskclient.Client {
	t.Helper()
	caller, err := rpc.NewHTTPCaller(url, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	client, err := taskclient.New(caller, taskclient.Options{Tasks: true, Info: schema.Implementation{Name: "stream-fixture", Version: "1"}, Capabilities: caps, Wait: func(ctx context.Context, _ time.Duration) error { return ctx.Err() }})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func TestRealHTTPTaskSubscriptionFullStateAndDisconnect(t *testing.T) {
	base, service, _ := host(t)
	ctx := context.WithValue(context.Background(), ownerKey{}, alice)
	caps := schema.ClientCapabilities{Elicitation: &schema.ClientCapabilitiesElicitation{}}
	tasks.Advertise(&caps)
	created, err := service.Create(ctx, alice, caps, tasks.CreateOptions{Method: tasks.MethodCallTool})
	if err != nil {
		t.Fatal(err)
	}
	var streams atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streams.Add(1)
		handler := rpc.HTTPHandler{Build: func(_ context.Context, notifier transport.Notifier) transport.Handler {
			return &rpc.Handler{Next: base.Next, Service: service, Owner: base.Owner, Notifier: notifier, Subscriptions: &rpc.SubscriptionOptions{ReconcileInterval: 10 * time.Millisecond, MaxDuration: time.Second}}
		}}
		handler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerKey{}, alice)))
	}))
	defer server.Close()
	client := subscriptionClient(t, server.URL, server.Client(), caps)
	observerCtx, stop := context.WithCancel(context.Background())
	received := make(chan tasks.DetailedTask, 8)
	acknowledged := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- client.Subscribe(observerCtx, []string{created.TaskID}, taskclient.SubscriptionOptions{OnAcknowledged: func(ids []string) error {
			if len(ids) != 1 || ids[0] != created.TaskID {
				return errors.New("wrong filter")
			}
			acknowledged <- struct{}{}
			return nil
		}, OnTask: func(state tasks.DetailedTask) error { received <- state; return nil }})
	}()
	<-acknowledged
	initial := <-received
	if initial.Status != tasks.Working {
		t.Fatal("initial full snapshot")
	}
	request := tasks.InputRequest{Method: "elicitation/create", Params: json.RawMessage(`{"mode":"form","message":"ordinary fixture input"}`)}
	if err = service.RequireInput(ctx, alice, created.TaskID, tasks.InputRequests{"key": request}); err != nil {
		t.Fatal(err)
	}
	select {
	case state := <-received:
		if state.Status != tasks.InputRequired || len(state.InputRequests) != 1 {
			t.Fatal("missing full input state")
		}
	case <-time.After(time.Second):
		t.Fatal("input update not streamed")
	}
	stop()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnect leaked stream")
	}
	record, err := service.PrivateRecord(ctx, alice, created.TaskID)
	if err != nil || record.CancelRequested {
		t.Fatal("observer disconnect cancelled native work")
	}
	// An independently controlled task survives reconnect and includes its full
	// original tool result, including private host metadata and tool-level error.
	final := json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"fixture final"}],"isError":true,"_meta":{"fixture":"host-only"}}`)
	if err = service.Complete(ctx, alice, created.TaskID, final); err != nil {
		t.Fatal(err)
	}
	var terminal tasks.DetailedTask
	err = client.Subscribe(ctx, []string{created.TaskID}, taskclient.SubscriptionOptions{OnTask: func(state tasks.DetailedTask) error { terminal = state; return nil }})
	if err != nil || terminal.Status != tasks.Completed || string(terminal.Result) != string(final) {
		t.Fatal("terminal full state", err)
	}
}
func TestHTTPReconnectRereadsCanonicalStateWithoutReplay(t *testing.T) {
	base, service, _ := host(t)
	ctx := context.WithValue(context.Background(), ownerKey{}, alice)
	caps := schema.ClientCapabilities{}
	tasks.Advertise(&caps)
	created, err := service.Create(ctx, alice, caps, tasks.CreateOptions{Method: tasks.MethodCallTool})
	if err != nil {
		t.Fatal(err)
	}
	var listens atomic.Int32
	var gets atomic.Int32
	terminal := json.RawMessage(`{"resultType":"complete","content":[]}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonrpc.Request
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			t.Error("request")
			return
		}
		if req.Method == tasks.MethodListen && listens.Add(1) == 1 {
			id, _ := json.Marshal(req.Id)
			meta := map[string]json.RawMessage{tasks.SubscriptionIDMeta: id}
			ack, _ := json.Marshal(tasks.SubscriptionAcknowledged{JSONRPC: "2.0", Method: tasks.MethodAcknowledged, Params: tasks.SubscriptionAcknowledgedParams{Notifications: tasks.SubscriptionFilter{TaskIDs: []string{created.TaskID}}, Meta: meta}})
			state, _ := service.Get(ctx, alice, created.TaskID)
			notification, _ := json.Marshal(tasks.TaskStatusNotification{JSONRPC: "2.0", Method: tasks.MethodNotification, Params: tasks.TaskNotificationParams{DetailedTask: state.DetailedTask, Meta: meta}})
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "data: %s\n\ndata: %s\n\n", ack, notification)
			w.(http.Flusher).Flush()
			if err = service.Complete(ctx, alice, created.TaskID, terminal); err != nil {
				t.Error(err)
			}
			return // abrupt EOF, no correlated completion
		}
		if req.Method == tasks.MethodGet {
			gets.Add(1)
		}
		// Reuse the decoded request through the real generic handler/notifier path.
		encoded, _ := json.Marshal(req)
		r.Body = io.NopCloser(bytes.NewReader(encoded))
		rpc.HTTPHandler{Build: func(_ context.Context, n transport.Notifier) transport.Handler {
			return &rpc.Handler{Service: service, Owner: base.Owner, Notifier: n, Subscriptions: &rpc.SubscriptionOptions{ReconcileInterval: 10 * time.Millisecond}}
		}}.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerKey{}, alice)))
	}))
	defer server.Close()
	client := subscriptionClient(t, server.URL, server.Client(), caps)
	states := []tasks.Status{}
	acks := 0
	err = client.Subscribe(ctx, []string{created.TaskID}, taskclient.SubscriptionOptions{MaxReconnects: 1, OnAcknowledged: func([]string) error { acks++; return nil }, OnTask: func(state tasks.DetailedTask) error { states = append(states, state.Status); return nil }})
	if err != nil || listens.Load() != 2 || gets.Load() != 1 || acks != 2 || len(states) != 2 || states[1] != tasks.Completed {
		t.Fatal("reconnect canonical refresh", err, listens.Load(), gets.Load(), acks, states)
	}
}
func TestSubscriptionRejectsForeignTenantMixedFiltersAndMissingInputCapability(t *testing.T) {
	base, service, _ := host(t)
	ctx := context.WithValue(context.Background(), ownerKey{}, alice)
	caps := schema.ClientCapabilities{Elicitation: &schema.ClientCapabilitiesElicitation{}}
	tasks.Advertise(&caps)
	created, err := service.Create(ctx, alice, caps, tasks.CreateOptions{Method: tasks.MethodCallTool})
	if err != nil {
		t.Fatal(err)
	}
	if err = service.RequireInput(ctx, alice, created.TaskID, tasks.InputRequests{"key": {Method: "elicitation/create", Params: json.RawMessage(`{"mode":"form"}`)}}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		owner  tasks.Owner
		caps   schema.ClientCapabilities
		filter tasks.SubscriptionFilter
		code   int
	}{
		{"foreign subject", tasks.Owner{Tenant: alice.Tenant, Subject: "other"}, caps, tasks.SubscriptionFilter{TaskIDs: []string{created.TaskID}}, -32602},
		{"foreign tenant", tasks.Owner{Tenant: "other", Subject: alice.Subject}, caps, tasks.SubscriptionFilter{TaskIDs: []string{created.TaskID}}, -32602},
		{"missing task capability", alice, schema.ClientCapabilities{}, tasks.SubscriptionFilter{TaskIDs: []string{created.TaskID}}, -32021},
		{"missing elicitation capability", alice, func() schema.ClientCapabilities { c := schema.ClientCapabilities{}; tasks.Advertise(&c); return c }(), tasks.SubscriptionFilter{TaskIDs: []string{created.TaskID}}, -32021},
		{"mixed", alice, caps, tasks.SubscriptionFilter{TaskIDs: []string{created.TaskID}, ToolsListChanged: true}, -32602},
	} {
		t.Run(test.name, func(t *testing.T) {
			notifications := 0
			handler := &rpc.Handler{Service: service, Owner: base.Owner, Notifier: &rpc.SerialNotifier{NotifyFunc: func(context.Context, *jsonrpc.Notification) error { notifications++; return nil }}, Subscriptions: &rpc.SubscriptionOptions{}}
			params, _ := json.Marshal(tasks.ListenParams{Notifications: test.filter, Meta: map[string]json.RawMessage{tasks.ClientCapabilitiesMeta: mustJSON(test.caps), "io.modelcontextprotocol/protocolVersion": mustJSON(tasks.ProtocolVersion), "io.modelcontextprotocol/clientInfo": mustJSON(schema.Implementation{Name: "fixture", Version: "1"})}})
			response := &jsonrpc.Response{}
			handler.Serve(context.WithValue(context.Background(), ownerKey{}, test.owner), &jsonrpc.Request{Jsonrpc: "2.0", Id: 1, Method: tasks.MethodListen, Params: params}, response)
			if response.Error == nil || response.Error.Code != test.code || notifications != 0 {
				t.Fatal("rejected stream leaked payload", response.Error, notifications)
			}
		})
	}
}
func mustJSON(value any) json.RawMessage { raw, _ := json.Marshal(value); return raw }

func TestStdioSubscriptionCancellationOnlyClosesOwnedObserver(t *testing.T) {
	for _, id := range []any{42, 0, -7, "subscription", "42"} {
		t.Run(fmt.Sprint(id), func(t *testing.T) { testStdioSubscriptionCancellation(t, id) })
	}
}

func testStdioSubscriptionCancellation(t *testing.T, id any) {
	base, service, _ := host(t)
	ctx := context.WithValue(context.Background(), ownerKey{}, alice)
	caps := schema.ClientCapabilities{}
	tasks.Advertise(&caps)
	created, err := service.Create(ctx, alice, caps, tasks.CreateOptions{Method: tasks.MethodCallTool})
	if err != nil {
		t.Fatal(err)
	}
	ack := make(chan struct{}, 1)
	handler := &rpc.Handler{Service: service, Owner: base.Owner, Subscriptions: &rpc.SubscriptionOptions{ReconcileInterval: 10 * time.Millisecond, MaxDuration: time.Second}, Notifier: &rpc.SerialNotifier{NotifyFunc: func(_ context.Context, n *jsonrpc.Notification) error {
		if n.Method == tasks.MethodAcknowledged {
			ack <- struct{}{}
		}
		return nil
	}}}
	params := mustJSON(tasks.ListenParams{Notifications: tasks.SubscriptionFilter{TaskIDs: []string{created.TaskID}}, Meta: map[string]json.RawMessage{tasks.ClientCapabilitiesMeta: mustJSON(caps), "io.modelcontextprotocol/protocolVersion": mustJSON(tasks.ProtocolVersion), "io.modelcontextprotocol/clientInfo": mustJSON(schema.Implementation{Name: "fixture", Version: "1"})}})
	response := &jsonrpc.Response{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Serve(ctx, &jsonrpc.Request{Jsonrpc: "2.0", Id: id, Method: tasks.MethodListen, Params: params}, response)
	}()
	<-ack
	cancelParams, _ := json.Marshal(map[string]any{"requestId": id})
	cancelNotification := &jsonrpc.Notification{Jsonrpc: "2.0", Method: "notifications/cancelled", Params: cancelParams}
	handler.OnNotification(context.WithValue(context.Background(), ownerKey{}, tasks.Owner{Tenant: alice.Tenant, Subject: "foreign"}), cancelNotification)
	select {
	case <-done:
		t.Fatal("foreign principal cancelled stream")
	case <-time.After(10 * time.Millisecond):
	}
	handler.OnNotification(ctx, cancelNotification)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stdio cancellation leaked observer")
	}
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	var completion tasks.SubscriptionResult
	idJSON, _ := json.Marshal(id)
	if json.Unmarshal(response.Result, &completion) != nil || completion.ResultType != "complete" || string(completion.Meta[tasks.SubscriptionIDMeta]) != string(idJSON) {
		t.Fatal("graceful subscription response missing identity")
	}
	record, err := service.PrivateRecord(ctx, alice, created.TaskID)
	if err != nil || record.CancelRequested {
		t.Fatal("subscription cancellation cancelled task")
	}
}
func TestInputCapabilityIsRecheckedAfterAcknowledgement(t *testing.T) {
	base, service, _ := host(t)
	ctx := context.WithValue(context.Background(), ownerKey{}, alice)
	creationCaps := schema.ClientCapabilities{Elicitation: &schema.ClientCapabilitiesElicitation{}}
	tasks.Advertise(&creationCaps)
	created, err := service.Create(ctx, alice, creationCaps, tasks.CreateOptions{Method: tasks.MethodCallTool})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpc.HTTPHandler{Build: func(_ context.Context, n transport.Notifier) transport.Handler {
			return &rpc.Handler{Service: service, Owner: base.Owner, Notifier: n, Subscriptions: &rpc.SubscriptionOptions{ReconcileInterval: 10 * time.Millisecond}}
		}}.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ownerKey{}, alice)))
	}))
	defer server.Close()
	client := subscriptionClient(t, server.URL, server.Client(), schema.ClientCapabilities{})
	initial := make(chan struct{}, 1)
	done := make(chan error, 1)
	leaked := false
	observer, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- client.Subscribe(observer, []string{created.TaskID}, taskclient.SubscriptionOptions{OnTask: func(state tasks.DetailedTask) error {
			if state.Status == tasks.InputRequired {
				leaked = true
			}
			initial <- struct{}{}
			return nil
		}})
	}()
	<-initial
	if err = service.RequireInput(ctx, alice, created.TaskID, tasks.InputRequests{"input": {Method: "elicitation/create", Params: json.RawMessage(`{"mode":"form","message":"fixture withheld"}`)}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		var protocol *tasks.RPCError
		if !errors.As(err, &protocol) || protocol.Code != -32021 || leaked {
			t.Fatal("mid-stream input capability leak", err, leaked)
		}
	case <-time.After(time.Second):
		t.Fatal("missing capability did not end stream")
	}
}
