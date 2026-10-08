package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/viant/mcp-protocol/schema"
)

var (
	ErrNotFound   = errors.New("task unavailable")
	ErrConflict   = errors.New("task revision conflict")
	ErrTerminal   = errors.New("task is terminal")
	ErrKeyReuse   = errors.New("task input key cannot be reused")
	ErrDurability = errors.New("task creation was not durably visible")
)

// Owner is authenticated host authority, not a wire field. Both components
// are required, even for a single-tenant host (use a fixed tenant identifier).
type Owner struct {
	Tenant  string `json:"tenant"`
	Subject string `json:"subject"`
}

func (o Owner) Valid() bool {
	return o.Tenant != "" && o.Subject != "" && len(o.Tenant) <= 1024 && len(o.Subject) <= 1024
}

// Record is private adapter state. Origin can identify an existing native run;
// no origin metadata, capability snapshot or accepted inputs is sent to clients.
type Record struct {
	Owner              Owner                     `json:"owner"`
	Revision           uint64                    `json:"revision"`
	OriginMethod       string                    `json:"originMethod"`
	Origin             json.RawMessage           `json:"origin,omitempty"`
	ClientCapabilities schema.ClientCapabilities `json:"clientCapabilities"`
	State              DetailedTask              `json:"state"`
	Issued             InputRequests             `json:"issued"`
	Responses          InputResponses            `json:"responses"`
	CancelRequested    bool                      `json:"cancelRequested"`
}

// Store success means committed and immediately visible to Get. CAS must be
// atomic across every writer; callers never retry a failed/uncertain write.
// Implementations isolate owners and return ErrNotFound for foreign IDs too.
type Store interface {
	Create(context.Context, Record) error
	Get(context.Context, Owner, string) (Record, error)
	CompareAndSwap(context.Context, Owner, string, uint64, Record) error
}

// Backend lets an existing native runtime expose the wire lifecycle directly,
// without adopting the reference Service or maintaining a second task engine.
type Backend interface {
	Create(context.Context, Owner, schema.ClientCapabilities, CreateOptions) (CreateTaskResult, error)
	Get(context.Context, Owner, string) (GetTaskResult, error)
	Update(context.Context, Owner, string, InputResponses) (Acknowledgement, error)
	Cancel(context.Context, Owner, string) (Acknowledgement, error)
}

type CreateOptions struct {
	Method string
	Origin json.RawMessage
	// Nil TTL uses the service default; Unlimited explicitly selects null.
	TTLMS         *int64
	Unlimited     bool
	StatusMessage string
}
