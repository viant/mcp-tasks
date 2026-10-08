package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
)

// SubscriptionOptions enables the actual notification transport. Nil on Handler
// means unsupported. ReconcileInterval bounds loss/overflow recovery; MaxDuration
// bounds resource lifetime and ends with a graceful correlated result.
type SubscriptionOptions struct {
	ReconcileInterval time.Duration
	MaxDuration       time.Duration
	MaxConcurrent     int
}
type activeSubscription struct {
	owner  tasks.Owner
	cancel context.CancelFunc
}

func subscriptionID(id any) (json.RawMessage, string, error) {
	raw, err := json.Marshal(id)
	if err != nil {
		return nil, "", err
	}
	if ValidRequestID(id) {
		return raw, string(raw), nil
	}
	return nil, "", &tasks.RPCError{Code: -32600, Message: "valid subscription request ID required"}
}

// ValidRequestID accepts MCP string/integer request IDs without admitting null,
// booleans, objects or fractional JSON numbers into execution/control routing.
func ValidRequestID(id any) bool {
	raw, err := json.Marshal(id)
	if err != nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil {
		return false
	}
	if _, ok := value.(string); ok {
		return true
	}
	if number, ok := value.(json.Number); ok {
		integer, err := strconv.ParseInt(number.String(), 10, 64)
		return err == nil && integer >= -(1<<53-1) && integer <= 1<<53-1
	}
	return false
}
func (h *Handler) listen(ctx context.Context, request *jsonrpc.Request, caps schema.ClientCapabilities) (tasks.SubscriptionResult, error) {
	var params tasks.ListenParams
	if err := decode(request.Params, &params); err != nil {
		return tasks.SubscriptionResult{}, err
	}
	if params.Notifications.Validate() != nil {
		return tasks.SubscriptionResult{}, &tasks.RPCError{Code: -32602, Message: "bounded unique taskIds required"}
	}
	if params.Notifications.Mixed() {
		return tasks.SubscriptionResult{}, &tasks.RPCError{Code: -32602, Message: "mixed task/base filters require an explicitly composed host subscription"}
	}
	owner, err := h.resolve(ctx, request)
	if err != nil {
		return tasks.SubscriptionResult{}, tasks.ErrNotFound
	}
	id, key, err := subscriptionID(request.Id)
	if err != nil {
		return tasks.SubscriptionResult{}, err
	}
	options := *h.Subscriptions
	if options.ReconcileInterval <= 0 {
		options.ReconcileInterval = time.Second
	}
	if options.MaxDuration <= 0 {
		options.MaxDuration = 5 * time.Minute
	}
	if options.MaxConcurrent <= 0 {
		options.MaxConcurrent = 32
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.subscriptionsMu.Lock()
	if h.activeSubscriptions == nil {
		h.activeSubscriptions = map[string]activeSubscription{}
	}
	if _, duplicate := h.activeSubscriptions[key]; duplicate || len(h.activeSubscriptions) >= options.MaxConcurrent {
		h.subscriptionsMu.Unlock()
		return tasks.SubscriptionResult{}, &tasks.RPCError{Code: -32602, Message: "subscription ID active or capacity reached"}
	}
	h.activeSubscriptions[key] = activeSubscription{owner: owner, cancel: cancel}
	h.subscriptionsMu.Unlock()
	defer func() { h.subscriptionsMu.Lock(); delete(h.activeSubscriptions, key); h.subscriptionsMu.Unlock() }()
	read := func() ([]tasks.DetailedTask, error) {
		currentOwner, err := h.resolve(ctx, request)
		if err != nil || currentOwner != owner {
			return nil, tasks.ErrNotFound
		}
		snapshots := make([]tasks.DetailedTask, 0, len(params.Notifications.TaskIDs))
		for _, taskID := range params.Notifications.TaskIDs {
			state, err := h.Service.Get(ctx, owner, taskID)
			if err != nil {
				return nil, err
			}
			if state.TaskID != taskID || state.ResultType != tasks.ResultComplete || state.DetailedTask.Validate() != nil {
				return nil, errors.New("invalid canonical task snapshot")
			}
			for _, input := range state.InputRequests {
				if !tasks.SupportsInput(caps, input.Method) {
					return nil, missingInputCapability(input.Method)
				}
			}
			snapshots = append(snapshots, state.DetailedTask)
		}
		return snapshots, nil
	}
	// Subscribe before taking the initial authoritative snapshot. Hints never
	// carry payloads, and lost/overflow hints are covered by reconciliation.
	var wake tasks.Wakeup
	if source, ok := h.Service.(tasks.ChangeSource); ok {
		wake, err = source.WatchTasks(ctx, owner, params.Notifications.TaskIDs)
		if err != nil {
			return tasks.SubscriptionResult{}, err
		}
		defer wake.Close()
	}
	var events <-chan struct{}
	if wake != nil {
		events = wake.C()
	}
	snapshots, err := read()
	if err != nil {
		return tasks.SubscriptionResult{}, err
	}
	meta := map[string]json.RawMessage{tasks.SubscriptionIDMeta: id}
	ack := tasks.SubscriptionAcknowledgedParams{Notifications: tasks.SubscriptionFilter{TaskIDs: append([]string(nil), params.Notifications.TaskIDs...)}, Meta: meta}
	raw, _ := json.Marshal(ack)
	if err = h.Notifier.Notify(ctx, &jsonrpc.Notification{Jsonrpc: "2.0", Method: tasks.MethodAcknowledged, Params: raw}); err != nil {
		return tasks.SubscriptionResult{}, err
	}
	hashes := map[string]string{}
	emit := func(snapshots []tasks.DetailedTask) (bool, error) {
		terminal := true
		for _, state := range snapshots {
			if !state.Status.Terminal() {
				terminal = false
			}
			canonical, _ := json.Marshal(state)
			if hashes[state.TaskID] == string(canonical) {
				continue
			}
			params := tasks.TaskNotificationParams{DetailedTask: state, Meta: meta}
			raw, err := json.Marshal(params)
			if err != nil || len(raw) > tasks.MaxJSONBytes {
				return false, errors.New("task notification exceeds budget")
			}
			if err = h.Notifier.Notify(ctx, &jsonrpc.Notification{Jsonrpc: "2.0", Method: tasks.MethodNotification, Params: raw}); err != nil {
				return false, err
			}
			hashes[state.TaskID] = string(canonical)
		}
		return terminal, nil
	}
	done, err := emit(snapshots)
	if err != nil {
		return tasks.SubscriptionResult{}, err
	}
	ticker := time.NewTicker(options.ReconcileInterval)
	defer ticker.Stop()
	duration := time.NewTimer(options.MaxDuration)
	defer duration.Stop()
	for !done {
		select {
		case <-ctx.Done():
			return tasks.SubscriptionResult{ResultType: tasks.ResultComplete, Meta: meta}, nil
		case <-duration.C:
			return tasks.SubscriptionResult{ResultType: tasks.ResultComplete, Meta: meta}, nil
		case _, open := <-events:
			if !open {
				events = nil
			}
		case <-ticker.C:
		}
		snapshots, err = read()
		if ctx.Err() != nil {
			return tasks.SubscriptionResult{ResultType: tasks.ResultComplete, Meta: meta}, nil
		}
		if err != nil {
			return tasks.SubscriptionResult{}, err
		}
		done, err = emit(snapshots)
		if ctx.Err() != nil {
			return tasks.SubscriptionResult{ResultType: tasks.ResultComplete, Meta: meta}, nil
		}
		if err != nil {
			return tasks.SubscriptionResult{}, err
		}
	}
	return tasks.SubscriptionResult{ResultType: tasks.ResultComplete, Meta: meta}, nil
}
func (h *Handler) cancelSubscription(ctx context.Context, notification *jsonrpc.Notification) bool {
	if notification == nil || notification.Method != "notifications/cancelled" {
		return false
	}
	var params struct {
		RequestID any `json:"requestId"`
	}
	if json.Unmarshal(notification.Params, &params) != nil {
		return false
	}
	_, key, err := subscriptionID(params.RequestID)
	if err != nil {
		return false
	}
	h.subscriptionsMu.Lock()
	active, ok := h.activeSubscriptions[key]
	h.subscriptionsMu.Unlock()
	if !ok {
		return false
	}
	owner, err := h.resolve(ctx, &jsonrpc.Request{Jsonrpc: "2.0", Id: params.RequestID, Method: tasks.MethodListen, Params: notification.Params})
	if err != nil || owner != active.owner {
		return true
	}
	active.cancel()
	return true
}

// HandlerNotifier lets a request-scoped HTTP transport deliver standard
// notifications without sharing response writers across requests.
type notificationSinkKey struct{}

func WithNotificationSink(ctx context.Context, notify func(context.Context, *jsonrpc.Notification) error) context.Context {
	return context.WithValue(ctx, notificationSinkKey{}, notify)
}

type ContextNotifier struct{}

func (ContextNotifier) Notify(ctx context.Context, notification *jsonrpc.Notification) error {
	notify, _ := ctx.Value(notificationSinkKey{}).(func(context.Context, *jsonrpc.Notification) error)
	if notify == nil {
		return fmt.Errorf("notification transport unavailable")
	}
	return notify(ctx, notification)
}

// SerialNotifier prevents interleaved writes when one host transport is shared.
type SerialNotifier struct {
	mu         sync.Mutex
	NotifyFunc func(context.Context, *jsonrpc.Notification) error
}

func (s *SerialNotifier) Notify(ctx context.Context, n *jsonrpc.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.NotifyFunc(ctx, n)
}
