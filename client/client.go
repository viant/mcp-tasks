// Package client provides opt-in task-aware calls. It never retries an original
// tools/call or a failed mutation, and disconnect never means task cancellation.
package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	"github.com/viant/mcp-tasks/rpc"
	"net/http"
	"sort"
	"time"
)

var (
	ErrInvalidPeer    = errors.New("invalid MCP task response")
	ErrCancelled      = errors.New("task cancelled")
	ErrExpired        = errors.New("task TTL elapsed")
	ErrUncertainInput = errors.New("task input outcome is uncertain; reconcile before another attempt")
)

type Options struct {
	Capabilities schema.ClientCapabilities
	Info         schema.Implementation
	Tasks        bool
	Clock        func() time.Time
	Wait         func(context.Context, time.Duration) error
}
type Client struct {
	caller  rpc.Caller
	options Options
}

func New(caller rpc.Caller, options Options) (*Client, error) {
	if caller == nil || options.Info.Name == "" || options.Info.Version == "" {
		return nil, errors.New("caller and client identity required")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.Wait == nil {
		options.Wait = wait
	}
	raw, err := json.Marshal(options.Capabilities)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &options.Capabilities); err != nil {
		return nil, err
	}
	// Tasks is an explicit opt-in, never inherited from a legacy capability slot.
	delete(options.Capabilities.Extensions, tasks.Extension)
	if options.Tasks {
		tasks.Advertise(&options.Capabilities)
	}
	return &Client{caller: caller, options: options}, nil
}
func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (c *Client) params(input any) (json.RawMessage, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	var params map[string]json.RawMessage
	if json.Unmarshal(raw, &params) != nil || params == nil {
		return nil, errors.New("object request params required")
	}
	var meta map[string]json.RawMessage
	_ = json.Unmarshal(params["_meta"], &meta)
	if meta == nil {
		meta = map[string]json.RawMessage{}
	}
	meta[tasks.ClientCapabilitiesMeta], _ = json.Marshal(c.options.Capabilities)
	meta["io.modelcontextprotocol/clientInfo"], _ = json.Marshal(c.options.Info)
	meta["io.modelcontextprotocol/protocolVersion"], _ = json.Marshal(tasks.ProtocolVersion)
	params["_meta"], _ = json.Marshal(meta)
	return json.Marshal(params)
}
func (c *Client) call(ctx context.Context, method, id string, input any) (json.RawMessage, error) {
	if id != "" && !tasks.ValidID(id) {
		return nil, errors.New("invalid task identifier")
	}
	params, err := c.params(input)
	if err != nil {
		return nil, err
	}
	headers := http.Header{"Mcp-Method": []string{method}, "Mcp-Protocol-Version": []string{tasks.ProtocolVersion}}
	if id != "" {
		headers.Set("Mcp-Name", rpc.EncodeHeaderValue(id))
	} else if method == tasks.MethodCallTool {
		var named struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(params, &named)
		headers.Set("Mcp-Name", rpc.EncodeHeaderValue(named.Name))
	}
	result, err := c.caller.Call(ctx, rpc.Request{Method: method, Params: params, Headers: headers})
	if err != nil {
		var known *tasks.RPCError
		if method != tasks.MethodGet && method != "server/discover" && !errors.As(err, &known) {
			return nil, &UncertainError{Method: method, TaskID: id, Cause: err}
		}
		return nil, err
	}
	if !tasks.ValidObject(result) {
		return nil, ErrInvalidPeer
	}
	return result, nil
}

type UncertainError struct {
	Method, TaskID string
	Cause          error
}

func (e *UncertainError) Error() string {
	return fmt.Sprintf("%s outcome is uncertain; inspect the original operation before retrying", e.Method)
}
func (e *UncertainError) Unwrap() error { return e.Cause }

type ToolOutcome struct {
	Result json.RawMessage
	Task   *tasks.CreateTaskResult
}

// CallTool preserves normal and MRTR results verbatim; a task handle is exposed
// separately. It sends exactly one original request and never asks for a task.
func (c *Client) CallTool(ctx context.Context, params *schema.CallToolRequestParams) (ToolOutcome, error) {
	if params == nil || params.Name == "" {
		return ToolOutcome{}, errors.New("tool name required")
	}
	raw, err := c.call(ctx, tasks.MethodCallTool, "", params)
	if err != nil {
		return ToolOutcome{}, err
	}
	var result struct {
		Type string `json:"resultType"`
	}
	if json.Unmarshal(raw, &result) != nil {
		return ToolOutcome{}, ErrInvalidPeer
	}
	if result.Type != tasks.ResultTask {
		if result.Type != "" && result.Type != tasks.ResultComplete && result.Type != "input_required" {
			return ToolOutcome{}, ErrInvalidPeer
		}
		return ToolOutcome{Result: raw}, nil
	}
	if !c.options.Tasks {
		return ToolOutcome{}, ErrInvalidPeer
	}
	task, err := tasks.DecodeCreate(raw)
	if err != nil {
		return ToolOutcome{}, fmt.Errorf("%w: %v", ErrInvalidPeer, err)
	}
	for _, input := range task.InputRequests {
		if !tasks.SupportsInput(c.options.Capabilities, input.Method) {
			return ToolOutcome{}, ErrInvalidPeer
		}
	}
	return ToolOutcome{Task: &task}, nil
}
func (c *Client) Discover(ctx context.Context) (*schema.DiscoverResult, error) {
	raw, err := c.call(ctx, "server/discover", "", map[string]any{})
	if err != nil {
		return nil, err
	}
	var result schema.DiscoverResult
	if err = json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
func (c *Client) Get(ctx context.Context, id string) (tasks.GetTaskResult, error) {
	if !c.options.Tasks {
		return tasks.GetTaskResult{}, tasks.MissingCapability()
	}
	raw, err := c.call(ctx, tasks.MethodGet, id, tasks.GetParams{TaskID: id})
	if err != nil {
		return tasks.GetTaskResult{}, err
	}
	result, err := tasks.DecodeGet(raw)
	if err != nil || result.TaskID != id {
		return tasks.GetTaskResult{}, ErrInvalidPeer
	}
	for _, input := range result.InputRequests {
		if !tasks.SupportsInput(c.options.Capabilities, input.Method) {
			return tasks.GetTaskResult{}, ErrInvalidPeer
		}
	}
	return result, nil
}
func (c *Client) Update(ctx context.Context, id string, responses tasks.InputResponses) error {
	if !c.options.Tasks {
		return tasks.MissingCapability()
	}
	if err := tasks.ValidateResponses(responses); err != nil {
		return err
	}
	raw, err := c.call(ctx, tasks.MethodUpdate, id, tasks.UpdateParams{TaskID: id, InputResponses: responses})
	if err != nil {
		return err
	}
	return acknowledgement(raw)
}
func (c *Client) Cancel(ctx context.Context, id string) error {
	if !c.options.Tasks {
		return tasks.MissingCapability()
	}
	raw, err := c.call(ctx, tasks.MethodCancel, id, tasks.CancelParams{TaskID: id})
	if err != nil {
		return err
	}
	return acknowledgement(raw)
}
func acknowledgement(raw json.RawMessage) error {
	var result map[string]json.RawMessage
	if json.Unmarshal(raw, &result) != nil || string(result["resultType"]) != `"complete"` {
		return ErrInvalidPeer
	}
	for key := range result {
		if key != "resultType" && key != "_meta" {
			return ErrInvalidPeer
		}
	}
	return nil
}

type Attempt struct {
	Fingerprint string          `json:"fingerprint"`
	Stage       string          `json:"stage"`
	Response    json.RawMessage `json:"response,omitempty"`
}

// Cursor must be persisted by SaveCursor before external input handling/update
// when crash recovery is required. Reuse it to resume; do not replay CallTool.
type Cursor struct {
	TaskID    string             `json:"taskId"`
	CreatedAt time.Time          `json:"createdAt"`
	Attempts  map[string]Attempt `json:"attempts"`
}
type InputHandler func(context.Context, string, tasks.InputRequest) (json.RawMessage, error)
type WaitOptions struct {
	Cursor     *Cursor
	Initial    *tasks.Task
	Input      InputHandler
	SaveCursor func(context.Context, Cursor) error
	OnState    func(tasks.GetTaskResult)
}
type InputRequiredError struct{ Task tasks.GetTaskResult }

func (e *InputRequiredError) Error() string {
	return "task requires ordinary host input/consent handling"
}
func fingerprint(request tasks.InputRequest) string {
	raw, _ := json.Marshal(request)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (c *Client) checkpoint(ctx context.Context, options WaitOptions, cursor *Cursor) error {
	if options.SaveCursor == nil {
		return nil
	}
	raw, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	var copy Cursor
	if err = json.Unmarshal(raw, &copy); err != nil {
		return err
	}
	return options.SaveCursor(ctx, copy)
}
func (c *Client) pause(ctx context.Context, ms int64) error {
	if ms < 0 {
		return ErrInvalidPeer
	}
	if ms < 10 {
		ms = 10
	}
	// Long wire intervals are represented without overflowing time.Duration.
	for ms > 0 {
		chunk := ms
		if chunk > int64(time.Hour/time.Millisecond) {
			chunk = int64(time.Hour / time.Millisecond)
		}
		if err := c.options.Wait(ctx, time.Duration(chunk)*time.Millisecond); err != nil {
			return err
		}
		ms -= chunk
	}
	return nil
}
func poll(task tasks.Task) int64 {
	if task.PollIntervalMS != nil {
		return *task.PollIntervalMS
	}
	return 1000
}
func (c *Client) Wait(ctx context.Context, id string, options WaitOptions) (json.RawMessage, error) {
	if !tasks.ValidID(id) {
		return nil, errors.New("invalid task identifier")
	}
	cursor := options.Cursor
	if cursor == nil {
		cursor = &Cursor{TaskID: id, Attempts: map[string]Attempt{}}
	}
	if cursor.TaskID != "" && cursor.TaskID != id {
		return nil, ErrInvalidPeer
	}
	cursor.TaskID = id
	if cursor.Attempts == nil {
		cursor.Attempts = map[string]Attempt{}
	}
	if options.Initial != nil {
		if options.Initial.TaskID != id || options.Initial.Validate() != nil {
			return nil, ErrInvalidPeer
		}
		cursor.CreatedAt = options.Initial.CreatedAt
		if !options.Initial.Status.Terminal() {
			if options.Initial.Expired(c.options.Clock()) {
				return nil, ErrExpired
			}
			if err := c.pause(ctx, poll(*options.Initial)); err != nil {
				return nil, err
			}
		}
	}
	for {
		state, err := c.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if !cursor.CreatedAt.IsZero() && !cursor.CreatedAt.Equal(state.CreatedAt) {
			return nil, ErrInvalidPeer
		}
		cursor.CreatedAt = state.CreatedAt
		if options.OnState != nil {
			options.OnState(state)
		}
		switch state.Status {
		case tasks.Completed:
			return append(json.RawMessage(nil), state.Result...), nil
		case tasks.Failed:
			return nil, state.Error
		case tasks.Cancelled:
			return nil, ErrCancelled
		}
		if state.Expired(c.options.Clock()) {
			return nil, ErrExpired
		}
		// A disappeared key is no longer outstanding; no input side effect is repeated.
		for key, attempt := range cursor.Attempts {
			if _, pending := state.InputRequests[key]; !pending && attempt.Stage == "submitted" {
				attempt.Stage = "answered"
				cursor.Attempts[key] = attempt
			}
		}
		if state.Status == tasks.InputRequired {
			if options.Input == nil {
				return nil, &InputRequiredError{Task: state}
			}
			keys := make([]string, 0, len(state.InputRequests))
			for key := range state.InputRequests {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				request := state.InputRequests[key]
				hash := fingerprint(request)
				attempt, seen := cursor.Attempts[key]
				if seen && attempt.Fingerprint != hash {
					return nil, ErrInvalidPeer
				}
				if seen && attempt.Stage == "answered" {
					continue
				}
				if seen && (attempt.Stage == "handling" || attempt.Stage == "submitted" || attempt.Stage == "uncertain") {
					return nil, ErrUncertainInput
				}
				if !seen {
					attempt = Attempt{Fingerprint: hash, Stage: "handling"}
					cursor.Attempts[key] = attempt
					if err = c.checkpoint(ctx, options, cursor); err != nil {
						return nil, err
					}
					response, handleErr := options.Input(ctx, key, request)
					if handleErr != nil {
						attempt.Stage = "uncertain"
						cursor.Attempts[key] = attempt
						_ = c.checkpoint(ctx, options, cursor)
						return nil, fmt.Errorf("%w: %v", ErrUncertainInput, handleErr)
					}
					if !tasks.ValidObject(response) {
						return nil, ErrUncertainInput
					}
					attempt.Stage = "prepared"
					attempt.Response = append(json.RawMessage(nil), response...)
					cursor.Attempts[key] = attempt
					if err = c.checkpoint(ctx, options, cursor); err != nil {
						return nil, err
					}
				}
				attempt.Stage = "submitted"
				cursor.Attempts[key] = attempt
				if err = c.checkpoint(ctx, options, cursor); err != nil {
					return nil, err
				}
				if err = c.Update(ctx, id, tasks.InputResponses{key: attempt.Response}); err != nil {
					return nil, err
				}
				attempt.Stage = "answered"
				cursor.Attempts[key] = attempt
				if err = c.checkpoint(ctx, options, cursor); err != nil {
					return nil, err
				}
			}
		}
		if err = c.checkpoint(ctx, options, cursor); err != nil {
			return nil, err
		}
		if err = c.pause(ctx, poll(state.Task)); err != nil {
			return nil, err
		}
	}
}
