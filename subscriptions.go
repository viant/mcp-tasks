package tasks

import (
	"context"
	"encoding/json"
	"errors"
)

const (
	MethodListen             = "subscriptions/listen"
	MethodAcknowledged       = "notifications/subscriptions/acknowledged"
	SubscriptionIDMeta       = "io.modelcontextprotocol/subscriptionId"
	MaximumSubscriptionTasks = 64
)

// SubscriptionFilter embeds the extension's taskIds field. The base fields
// remain typed for explicit composition/rejection, never silently discarded.
type SubscriptionFilter struct {
	TaskIDs               []string `json:"taskIds,omitempty"`
	ToolsListChanged      bool     `json:"toolsListChanged,omitempty"`
	PromptsListChanged    bool     `json:"promptsListChanged,omitempty"`
	ResourcesListChanged  bool     `json:"resourcesListChanged,omitempty"`
	ResourceSubscriptions []string `json:"resourceSubscriptions,omitempty"`
}

func (f SubscriptionFilter) Mixed() bool {
	return f.ToolsListChanged || f.PromptsListChanged || f.ResourcesListChanged || len(f.ResourceSubscriptions) > 0
}
func (f SubscriptionFilter) Validate() error {
	if len(f.TaskIDs) == 0 || len(f.TaskIDs) > MaximumSubscriptionTasks {
		return errors.New("bounded explicit taskIds required")
	}
	seen := map[string]bool{}
	for _, id := range f.TaskIDs {
		if !ValidID(id) || seen[id] {
			return errors.New("invalid or duplicate taskId")
		}
		seen[id] = true
	}
	return nil
}

type ListenParams struct {
	Notifications SubscriptionFilter         `json:"notifications"`
	Meta          map[string]json.RawMessage `json:"_meta,omitempty"`
}
type SubscriptionAcknowledgedParams struct {
	Notifications SubscriptionFilter         `json:"notifications"`
	Meta          map[string]json.RawMessage `json:"_meta"`
}
type SubscriptionAcknowledged struct {
	JSONRPC string                         `json:"jsonrpc"`
	Method  string                         `json:"method"`
	Params  SubscriptionAcknowledgedParams `json:"params"`
}
type SubscriptionResult struct {
	ResultType string                     `json:"resultType"`
	Meta       map[string]json.RawMessage `json:"_meta"`
}
type TaskNotificationParams struct {
	DetailedTask
	Meta map[string]json.RawMessage `json:"_meta,omitempty"`
}

// Wakeup contains only invalidation hints. Subscribers re-read canonical owned
// state after every hint/overflow and on reconnect; this is not an event queue.
type Wakeup interface {
	C() <-chan struct{}
	Close() error
}
type ChangeSource interface {
	WatchTasks(context.Context, Owner, []string) (Wakeup, error)
}
