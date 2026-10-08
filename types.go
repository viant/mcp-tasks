package tasks

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	Extension             = "io.modelcontextprotocol/tasks"
	ProtocolVersion       = "2026-07-28"
	MethodCallTool        = "tools/call"
	MethodGet             = "tasks/get"
	MethodUpdate          = "tasks/update"
	MethodCancel          = "tasks/cancel"
	MethodNotification    = "notifications/tasks"
	ResultTask            = "task"
	ResultComplete        = "complete"
	MissingCapabilityCode = -32021
	MaxJSONBytes          = 4 << 20
	maxSafeInteger        = int64(1<<53 - 1)
)

type Status string

const (
	Working       Status = "working"
	InputRequired Status = "input_required"
	Completed     Status = "completed"
	Cancelled     Status = "cancelled"
	Failed        Status = "failed"
)

func (s Status) Terminal() bool { return s == Completed || s == Cancelled || s == Failed }
func (s Status) Valid() bool    { return s == Working || s == InputRequired || s.Terminal() }

// Task fields are flat on the wire. TTLMS is required and nil means unlimited.
type Task struct {
	TaskID         string    `json:"taskId"`
	Status         Status    `json:"status"`
	StatusMessage  string    `json:"statusMessage,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
	LastUpdatedAt  time.Time `json:"lastUpdatedAt"`
	TTLMS          *int64    `json:"ttlMs"`
	PollIntervalMS *int64    `json:"pollIntervalMs,omitempty"`
}

// InputRequest is an MRTR request object, without a JSON-RPC request ID.
// Hosts must apply their ordinary request-specific trust and consent policy.
type InputRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}
type InputRequests map[string]InputRequest
type InputResponses map[string]json.RawMessage

// RPCError is the JSON-RPC error object retained on a failed task.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("JSON-RPC %d: %s", e.Code, e.Message)
}

type DetailedTask struct {
	Task
	InputRequests InputRequests   `json:"inputRequests,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         *RPCError       `json:"error,omitempty"`
}
type CreateTaskResult struct {
	ResultType string `json:"resultType"`
	DetailedTask
	Meta map[string]json.RawMessage `json:"_meta,omitempty"`
}
type GetTaskResult struct {
	ResultType string `json:"resultType"`
	DetailedTask
	Meta map[string]json.RawMessage `json:"_meta,omitempty"`
}
type Acknowledgement struct {
	ResultType string                     `json:"resultType"`
	Meta       map[string]json.RawMessage `json:"_meta,omitempty"`
}
type GetParams struct {
	TaskID string                     `json:"taskId"`
	Meta   map[string]json.RawMessage `json:"_meta,omitempty"`
}
type CancelParams = GetParams
type UpdateParams struct {
	TaskID         string                     `json:"taskId"`
	InputResponses InputResponses             `json:"inputResponses"`
	Meta           map[string]json.RawMessage `json:"_meta,omitempty"`
}
type TaskStatusNotification struct {
	JSONRPC string       `json:"jsonrpc"`
	Method  string       `json:"method"`
	Params  DetailedTask `json:"params"`
}

func ValidID(id string) bool {
	return id != "" && len(id) <= 1024 && utf8.ValidString(id) && !strings.ContainsAny(id, "\x00\r\n")
}
func ValidObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	var obj map[string]json.RawMessage
	return len(raw) > 0 && len(raw) <= MaxJSONBytes && raw[0] == '{' && json.Unmarshal(raw, &obj) == nil && obj != nil
}
func (t Task) Expired(now time.Time) bool {
	if t.TTLMS == nil {
		return false
	}
	return now.UnixMilli()-t.CreatedAt.UnixMilli() >= *t.TTLMS
}
func (t Task) Validate() error {
	if !ValidID(t.TaskID) || !t.Status.Valid() || t.CreatedAt.IsZero() || t.LastUpdatedAt.IsZero() || t.LastUpdatedAt.Before(t.CreatedAt) {
		return errors.New("invalid task identity, status or timestamps")
	}
	// Integer bounds preserve interoperable JSON numeric values.
	if t.TTLMS != nil && (*t.TTLMS < 0 || *t.TTLMS > maxSafeInteger) {
		return errors.New("invalid task ttlMs")
	}
	if t.PollIntervalMS != nil && (*t.PollIntervalMS < 0 || *t.PollIntervalMS > maxSafeInteger) {
		return errors.New("invalid task pollIntervalMs")
	}
	return nil
}
func (r InputRequest) Validate() error {
	if r.Method != "elicitation/create" && r.Method != "sampling/createMessage" && r.Method != "roots/list" {
		return errors.New("unsupported task input request method")
	}
	if r.Method == "roots/list" && len(r.Params) == 0 {
		return nil
	}
	if !ValidObject(r.Params) {
		return errors.New("task input request requires object params")
	}
	return nil
}
func (t DetailedTask) Validate() error {
	if err := t.Task.Validate(); err != nil {
		return err
	}
	switch t.Status {
	case Working, Cancelled:
		if len(t.InputRequests) > 0 || len(t.Result) > 0 || t.Error != nil {
			return errors.New("task status does not permit payload")
		}
	case InputRequired:
		if len(t.InputRequests) == 0 || len(t.Result) > 0 || t.Error != nil {
			return errors.New("input_required task requires outstanding requests only")
		}
		for key, request := range t.InputRequests {
			if !ValidID(key) {
				return errors.New("invalid input key")
			}
			if err := request.Validate(); err != nil {
				return err
			}
		}
	case Completed:
		if !ValidObject(t.Result) || len(t.InputRequests) > 0 || t.Error != nil {
			return errors.New("completed task requires a result object only")
		}
		var result map[string]json.RawMessage
		_ = json.Unmarshal(t.Result, &result)
		if string(result["resultType"]) == `"task"` {
			return errors.New("completed task cannot contain another task handle")
		}
	case Failed:
		if t.Error == nil || t.Error.Message == "" || len(t.Result) > 0 || len(t.InputRequests) > 0 {
			return errors.New("failed task requires a JSON-RPC error only")
		}
	}
	raw, err := json.Marshal(t)
	if err != nil || len(raw) > MaxJSONBytes {
		return errors.New("task exceeds JSON budget")
	}
	return nil
}
func ValidateResponses(responses InputResponses) error {
	if responses == nil || len(responses) > 256 {
		return errors.New("inputResponses object is required and bounded")
	}
	for key, result := range responses {
		if !ValidID(key) || !ValidObject(result) {
			return errors.New("invalid input response")
		}
	}
	raw, err := json.Marshal(responses)
	if err != nil || len(raw) > MaxJSONBytes {
		return errors.New("inputResponses exceeds budget")
	}
	return nil
}

// DecodeGet checks mandatory fields, including required-but-null ttlMs.
func DecodeGet(raw json.RawMessage) (GetTaskResult, error) {
	var result GetTaskResult
	if err := decodeTask(raw, &result); err != nil {
		return result, err
	}
	if result.ResultType != ResultComplete {
		return result, errors.New("tasks/get requires resultType complete")
	}
	return result, result.DetailedTask.Validate()
}
func DecodeCreate(raw json.RawMessage) (CreateTaskResult, error) {
	var result CreateTaskResult
	if err := decodeTask(raw, &result); err != nil {
		return result, err
	}
	if result.ResultType != ResultTask {
		return result, errors.New("task creation requires resultType task")
	}
	return result, result.DetailedTask.Validate()
}
func decodeTask(raw json.RawMessage, result any) error {
	if !ValidObject(raw) {
		return errors.New("invalid task JSON object")
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	for _, key := range []string{"resultType", "taskId", "status", "createdAt", "lastUpdatedAt", "ttlMs"} {
		if _, ok := fields[key]; !ok {
			return fmt.Errorf("missing task field %s", key)
		}
	}
	return json.Unmarshal(raw, result)
}
