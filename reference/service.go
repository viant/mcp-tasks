// Package reference supplies an optional durable state implementation. It is
// not a scheduler; native hosts may implement tasks.Backend directly instead.
package reference

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	"time"
)

const maxSafeInteger = int64(1<<53 - 1)

type Options struct {
	Clock          func() time.Time
	ID             func() (string, error)
	DefaultTTLMS   *int64
	PollIntervalMS int64
	// ValidateInput is pure schema/policy validation, never a dispatch callback.
	ValidateInput func(context.Context, tasks.Record, string, tasks.InputRequest, json.RawMessage) error
}
type Service struct {
	store   tasks.Store
	options Options
}

func New(store tasks.Store, options Options) (*Service, error) {
	if store == nil {
		return nil, errors.New("durable task store required")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.ID == nil {
		options.ID = randomID
	}
	if options.PollIntervalMS == 0 {
		options.PollIntervalMS = 5000
	}
	if options.PollIntervalMS < 0 || options.PollIntervalMS > maxSafeInteger {
		return nil, errors.New("invalid default polling interval")
	}
	if options.DefaultTTLMS != nil && (*options.DefaultTTLMS <= 0 || *options.DefaultTTLMS > maxSafeInteger) {
		return nil, errors.New("default TTL must be positive or unlimited")
	}
	options.DefaultTTLMS = cloneInt(options.DefaultTTLMS)
	return &Service{store: store, options: options}, nil
}
func randomID() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
func cloneInt(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
func clone[T any](value T) (T, error) {
	var result T
	raw, err := json.Marshal(value)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(raw, &result)
	return result, err
}

func (s *Service) Create(ctx context.Context, owner tasks.Owner, caps schema.ClientCapabilities, options tasks.CreateOptions) (tasks.CreateTaskResult, error) {
	if !tasks.Supports(caps) {
		return tasks.CreateTaskResult{}, tasks.MissingCapability()
	}
	if !owner.Valid() {
		return tasks.CreateTaskResult{}, tasks.ErrNotFound
	}
	if options.Method != tasks.MethodCallTool {
		return tasks.CreateTaskResult{}, errors.New("only tools/call supports task creation")
	}
	if len(options.Origin) > 0 && !tasks.ValidObject(options.Origin) {
		return tasks.CreateTaskResult{}, errors.New("invalid private origin object")
	}
	ttl := cloneInt(options.TTLMS)
	if ttl == nil {
		ttl = cloneInt(s.options.DefaultTTLMS)
	}
	if options.Unlimited {
		ttl = nil
	}
	if ttl != nil && (*ttl <= 0 || *ttl > maxSafeInteger) {
		return tasks.CreateTaskResult{}, errors.New("creation TTL must be positive or unlimited")
	}
	id, err := s.options.ID()
	if err != nil {
		return tasks.CreateTaskResult{}, err
	}
	now := s.options.Clock().UTC()
	interval := s.options.PollIntervalMS
	record := tasks.Record{Owner: owner, Revision: 1, OriginMethod: options.Method, Origin: options.Origin, ClientCapabilities: caps,
		State: tasks.DetailedTask{Task: tasks.Task{TaskID: id, Status: tasks.Working, StatusMessage: options.StatusMessage, CreatedAt: now, LastUpdatedAt: now, TTLMS: ttl, PollIntervalMS: &interval}}, Issued: tasks.InputRequests{}, Responses: tasks.InputResponses{}}
	if err = record.State.Validate(); err != nil {
		return tasks.CreateTaskResult{}, err
	}
	record, err = clone(record)
	if err != nil {
		return tasks.CreateTaskResult{}, err
	}
	if err = s.store.Create(ctx, record); err != nil {
		return tasks.CreateTaskResult{}, err
	}
	// A returned handle never speculates about an eventual write becoming visible.
	stored, err := s.load(ctx, owner, id)
	if err != nil {
		return tasks.CreateTaskResult{}, fmt.Errorf("%w: %v", tasks.ErrDurability, err)
	}
	return tasks.CreateTaskResult{ResultType: tasks.ResultTask, DetailedTask: stored.State}, nil
}
func (s *Service) load(ctx context.Context, owner tasks.Owner, id string) (tasks.Record, error) {
	if !owner.Valid() || !tasks.ValidID(id) {
		return tasks.Record{}, tasks.ErrNotFound
	}
	record, err := s.store.Get(ctx, owner, id)
	if err != nil {
		return tasks.Record{}, err
	}
	if record.Owner != owner || record.State.TaskID != id || record.State.Expired(s.options.Clock()) {
		return tasks.Record{}, tasks.ErrNotFound
	}
	if record.Revision == 0 || record.OriginMethod != tasks.MethodCallTool || record.State.Validate() != nil {
		return tasks.Record{}, errors.New("invalid stored task")
	}
	return clone(record)
}
func (s *Service) Get(ctx context.Context, owner tasks.Owner, id string) (tasks.GetTaskResult, error) {
	record, err := s.load(ctx, owner, id)
	if err != nil {
		return tasks.GetTaskResult{}, err
	}
	return tasks.GetTaskResult{ResultType: tasks.ResultComplete, DetailedTask: record.State}, nil
}

// PrivateRecord gives a trusted adapter its durable input/cancel/origin state.
func (s *Service) PrivateRecord(ctx context.Context, owner tasks.Owner, id string) (tasks.Record, error) {
	return s.load(ctx, owner, id)
}

// mutate retries only a known CAS conflict. Its closure must be pure.
func (s *Service) mutate(ctx context.Context, owner tasks.Owner, id string, change func(*tasks.Record) error) error {
	for attempt := 0; attempt < 16; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := s.load(ctx, owner, id)
		if err != nil {
			return err
		}
		revision := record.Revision
		if err = change(&record); err != nil {
			return err
		}
		record.Revision++
		record.State.LastUpdatedAt = s.options.Clock().UTC()
		if record.State.LastUpdatedAt.Before(record.State.CreatedAt) {
			record.State.LastUpdatedAt = record.State.CreatedAt
		}
		if err = record.State.Validate(); err != nil {
			return err
		}
		if err = s.store.CompareAndSwap(ctx, owner, id, revision, record); errors.Is(err, tasks.ErrConflict) {
			continue
		} else {
			return err
		}
	}
	return tasks.ErrConflict
}
func (s *Service) Complete(ctx context.Context, owner tasks.Owner, id string, result json.RawMessage) error {
	result = append(json.RawMessage(nil), result...)
	return s.mutate(ctx, owner, id, func(record *tasks.Record) error {
		if record.State.Status.Terminal() {
			return tasks.ErrTerminal
		}
		record.State.Status = tasks.Completed
		record.State.InputRequests = nil
		record.State.Error = nil
		record.State.Result = result
		return nil
	})
}
func (s *Service) Fail(ctx context.Context, owner tasks.Owner, id string, rpcError *tasks.RPCError) error {
	if rpcError == nil || rpcError.Message == "" {
		return errors.New("JSON-RPC execution error required")
	}
	copy, err := clone(rpcError)
	if err != nil {
		return err
	}
	return s.mutate(ctx, owner, id, func(record *tasks.Record) error {
		if record.State.Status.Terminal() {
			return tasks.ErrTerminal
		}
		record.State.Status = tasks.Failed
		record.State.InputRequests = nil
		record.State.Result = nil
		record.State.Error = copy
		return nil
	})
}
func (s *Service) RequireInput(ctx context.Context, owner tasks.Owner, id string, requests tasks.InputRequests) error {
	if len(requests) == 0 || len(requests) > 256 {
		return errors.New("bounded inputRequests required")
	}
	copy, err := clone(requests)
	if err != nil {
		return err
	}
	for key, request := range copy {
		if !tasks.ValidID(key) {
			return errors.New("invalid input key")
		}
		if err = request.Validate(); err != nil {
			return err
		}
	}
	return s.mutate(ctx, owner, id, func(record *tasks.Record) error {
		if record.State.Status.Terminal() {
			return tasks.ErrTerminal
		}
		if record.Issued == nil {
			record.Issued = tasks.InputRequests{}
		}
		if record.State.InputRequests == nil {
			record.State.InputRequests = tasks.InputRequests{}
		}
		for key, request := range copy {
			if !tasks.SupportsInput(record.ClientCapabilities, request.Method) {
				return errors.New("client did not declare this input request capability")
			}
			if issued, ok := record.Issued[key]; ok {
				a, _ := json.Marshal(issued)
				b, _ := json.Marshal(request)
				if string(a) != string(b) {
					return tasks.ErrKeyReuse
				}
				if _, pending := record.State.InputRequests[key]; !pending {
					return tasks.ErrKeyReuse
				}
			}
			record.Issued[key] = request
			record.State.InputRequests[key] = request
		}
		record.State.Status = tasks.InputRequired
		return nil
	})
}
func (s *Service) Update(ctx context.Context, owner tasks.Owner, id string, responses tasks.InputResponses) (tasks.Acknowledgement, error) {
	if err := tasks.ValidateResponses(responses); err != nil {
		return tasks.Acknowledgement{}, &tasks.RPCError{Code: -32602, Message: "invalid inputResponses"}
	}
	copy, err := clone(responses)
	if err != nil {
		return tasks.Acknowledgement{}, err
	}
	err = s.mutate(ctx, owner, id, func(record *tasks.Record) error {
		// Unknown/already-satisfied keys, including updates after completion, are ignored.
		for key, response := range copy {
			request, pending := record.State.InputRequests[key]
			if !pending {
				continue
			}
			if s.options.ValidateInput != nil {
				snapshot, err := clone(*record)
				if err != nil {
					return err
				}
				if err = s.options.ValidateInput(ctx, snapshot, key, request, append(json.RawMessage(nil), response...)); err != nil {
					return err
				}
			}
			if record.Responses == nil {
				record.Responses = tasks.InputResponses{}
			}
			record.Responses[key] = response
			delete(record.State.InputRequests, key)
		}
		if record.State.Status == tasks.InputRequired && len(record.State.InputRequests) == 0 {
			record.State.Status = tasks.Working
			record.State.InputRequests = nil
		}
		return nil
	})
	return tasks.Acknowledgement{ResultType: tasks.ResultComplete}, err
}
func (s *Service) Cancel(ctx context.Context, owner tasks.Owner, id string) (tasks.Acknowledgement, error) {
	err := s.mutate(ctx, owner, id, func(record *tasks.Record) error { record.CancelRequested = true; return nil })
	return tasks.Acknowledgement{ResultType: tasks.ResultComplete}, err
}

// MarkCancelled is called only after the existing executor confirms cancellation.
func (s *Service) MarkCancelled(ctx context.Context, owner tasks.Owner, id string) error {
	return s.mutate(ctx, owner, id, func(record *tasks.Record) error {
		if record.State.Status.Terminal() {
			return tasks.ErrTerminal
		}
		record.State.Status = tasks.Cancelled
		record.State.InputRequests = nil
		record.State.Result = nil
		record.State.Error = nil
		return nil
	})
}
func (s *Service) SetMetadata(ctx context.Context, owner tasks.Owner, id, message string, ttl, pollInterval *int64) error {
	return s.mutate(ctx, owner, id, func(record *tasks.Record) error {
		record.State.StatusMessage = message
		record.State.TTLMS = cloneInt(ttl)
		record.State.PollIntervalMS = cloneInt(pollInterval)
		return nil
	})
}
func (s *Service) Notification(ctx context.Context, owner tasks.Owner, id string) (tasks.TaskStatusNotification, error) {
	result, err := s.Get(ctx, owner, id)
	if err != nil {
		return tasks.TaskStatusNotification{}, err
	}
	return tasks.TaskStatusNotification{JSONRPC: "2.0", Method: tasks.MethodNotification, Params: result.DetailedTask}, nil
}
