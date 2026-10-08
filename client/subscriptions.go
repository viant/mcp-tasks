package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	tasks "github.com/viant/mcp-tasks"
	"github.com/viant/mcp-tasks/rpc"
	"net/http"
	"reflect"
	"sort"
	"time"
)

type SubscriptionOptions struct {
	MaxReconnects  int
	ReconnectDelay time.Duration
	OnAcknowledged func([]string) error
	OnTask         func(tasks.DetailedTask) error
}

// Subscribe observes full canonical state and never handles inputs or cancels
// tasks. A dropped transport reconnects at most MaxReconnects times; each new
// subscription gets a fresh ID and a fresh server snapshot, not cursor replay.
func (c *Client) Subscribe(ctx context.Context, ids []string, options SubscriptionOptions) error {
	if !c.options.Tasks {
		return tasks.MissingCapability()
	}
	filter := tasks.SubscriptionFilter{TaskIDs: append([]string(nil), ids...)}
	if err := filter.Validate(); err != nil {
		return err
	}
	caller, ok := c.caller.(rpc.StreamCaller)
	if !ok {
		return errors.New("subscription-capable transport required")
	}
	if options.OnTask == nil {
		return errors.New("task observer callback required")
	}
	if options.MaxReconnects < 0 || options.MaxReconnects > 16 {
		return errors.New("reconnect bound must be 0..16")
	}
	if options.ReconnectDelay <= 0 {
		options.ReconnectDelay = 250 * time.Millisecond
	}
	params, err := c.params(tasks.ListenParams{Notifications: filter})
	if err != nil {
		return err
	}
	hashes := map[string]string{}
	wanted := map[string]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	deliver := func(state tasks.DetailedTask) error {
		if !wanted[state.TaskID] || state.Validate() != nil {
			return rpc.ErrStreamProtocol
		}
		for _, input := range state.InputRequests {
			if !tasks.SupportsInput(c.options.Capabilities, input.Method) {
				return rpc.ErrStreamProtocol
			}
		}
		raw, _ := json.Marshal(state)
		if hashes[state.TaskID] == string(raw) {
			return nil
		}
		if err := options.OnTask(state); err != nil {
			return err
		}
		hashes[state.TaskID] = string(raw)
		return nil
	}
	for reconnect := 0; ; reconnect++ {
		acknowledged := false
		_, err = caller.Listen(ctx, rpc.Request{Method: tasks.MethodListen, Params: params, Headers: http.Header{"Mcp-Method": []string{tasks.MethodListen}}}, func(raw json.RawMessage) error {
			var envelope struct {
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
			}
			if json.Unmarshal(raw, &envelope) != nil {
				return rpc.ErrStreamProtocol
			}
			if !acknowledged {
				var ack tasks.SubscriptionAcknowledgedParams
				if envelope.Method != tasks.MethodAcknowledged || json.Unmarshal(envelope.Params, &ack) != nil || ack.Notifications.Mixed() {
					return rpc.ErrStreamProtocol
				}
				requested := append([]string(nil), ids...)
				accepted := append([]string(nil), ack.Notifications.TaskIDs...)
				sort.Strings(requested)
				sort.Strings(accepted)
				if !reflect.DeepEqual(requested, accepted) {
					return fmt.Errorf("server did not acknowledge the requested task IDs")
				}
				acknowledged = true
				if options.OnAcknowledged != nil {
					return options.OnAcknowledged(append([]string(nil), ids...))
				}
				return nil
			}
			if envelope.Method != tasks.MethodNotification {
				return rpc.ErrStreamProtocol
			}
			state, decodeErr := tasks.DecodeNotificationParams(envelope.Params)
			if decodeErr != nil {
				return rpc.ErrStreamProtocol
			}
			return deliver(state.DetailedTask)
		})
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var consumer *rpc.ConsumerError
		var protocol *tasks.RPCError
		if errors.As(err, &consumer) || errors.As(err, &protocol) || errors.Is(err, rpc.ErrStreamProtocol) {
			return err
		}
		if reconnect >= options.MaxReconnects {
			return fmt.Errorf("subscription reconnect budget exhausted: %w", err)
		}
		// Refresh authoritative state after a dropped stream. No execution/update or
		// cancellation is replayed. The next server stream snapshots again after
		// attaching event hints, closing the Get-to-subscribe race.
		for _, id := range ids {
			state, getErr := c.Get(ctx, id)
			if getErr != nil {
				return getErr
			}
			if err = deliver(state.DetailedTask); err != nil {
				return err
			}
		}
		if err = c.options.Wait(ctx, options.ReconnectDelay); err != nil {
			return err
		}
	}
}
