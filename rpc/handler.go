// Package rpc contains opt-in JSON-RPC integration. Its resolver must run the
// host's ordinary authentication/policy; wrapping an unauthenticated handler
// does not inherit the wrapped handler's private authorization callbacks.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	tasks "github.com/viant/mcp-tasks"
	"io"
)

type OwnerResolver func(context.Context, *jsonrpc.Request) (tasks.Owner, error)
type Decision struct {
	Create   *tasks.CreateOptions
	Required bool
}

// DecideTool is pure planning/policy. It must never start execution. Schedule
// through the canonical runtime only after its owned task creation is durable.
type DecideTool func(context.Context, tasks.Owner, *jsonrpc.Request) (Decision, error)
type Handler struct {
	Next    transport.Handler
	Service tasks.Backend
	Owner   OwnerResolver
	Decide  DecideTool
}

func (h *Handler) Serve(ctx context.Context, request *jsonrpc.Request, response *jsonrpc.Response) {
	if request == nil || response == nil {
		return
	}
	if request.Jsonrpc != jsonrpc.Version {
		response.Error = jsonrpc.NewInvalidRequest("invalid JSON-RPC version", nil)
		return
	}
	taskMethod := request.Method == tasks.MethodGet || request.Method == tasks.MethodUpdate || request.Method == tasks.MethodCancel
	caps, err := tasks.RequestCapabilities(request.Params)
	if err != nil && (taskMethod || request.Method == tasks.MethodCallTool) {
		response.Error = jsonrpc.NewInvalidParamsError("invalid request capabilities", nil)
		return
	}
	if taskMethod {
		if !tasks.Supports(caps) {
			response.Error = asRPC(tasks.MissingCapability())
			return
		}
		if !validJulyMeta(request.Params) {
			response.Error = jsonrpc.NewInvalidParamsError("July per-request metadata required", nil)
			return
		}
		owner, err := h.resolve(ctx, request)
		if err != nil {
			response.Error = jsonrpc.NewInvalidParamsError("task unavailable", nil)
			return
		}
		var value any
		switch request.Method {
		case tasks.MethodGet:
			var params tasks.GetParams
			err = decode(request.Params, &params)
			if err == nil {
				var result tasks.GetTaskResult
				result, err = h.Service.Get(ctx, owner, params.TaskID)
				if err == nil {
					for _, input := range result.InputRequests {
						if !tasks.SupportsInput(caps, input.Method) {
							err = missingInputCapability(input.Method)
							break
						}
					}
				}
				value = result
			}
		case tasks.MethodUpdate:
			var params tasks.UpdateParams
			err = decode(request.Params, &params)
			if err == nil {
				value, err = h.Service.Update(ctx, owner, params.TaskID, params.InputResponses)
			}
		case tasks.MethodCancel:
			var params tasks.CancelParams
			err = decode(request.Params, &params)
			if err == nil {
				value, err = h.Service.Cancel(ctx, owner, params.TaskID)
			}
		}
		h.reply(response, value, err)
		return
	}
	if request.Method == tasks.MethodCallTool && h.Decide != nil {
		owner, err := h.resolve(ctx, request)
		if err != nil {
			response.Error = jsonrpc.NewInvalidParamsError("tool authority unavailable", nil)
			return
		}
		decision, err := h.Decide(ctx, owner, request)
		if err != nil {
			h.reply(response, nil, err)
			return
		}
		if decision.Required && !tasks.Supports(caps) {
			response.Error = asRPC(tasks.MissingCapability())
			return
		}
		if decision.Create != nil && tasks.Supports(caps) {
			if !validJulyMeta(request.Params) {
				response.Error = jsonrpc.NewInvalidParamsError("July per-request metadata required", nil)
				return
			}
			options := *decision.Create
			options.Method = tasks.MethodCallTool
			value, err := h.Service.Create(ctx, owner, caps, options)
			h.reply(response, value, err)
			return
		}
	}
	if request.Method == "subscriptions/listen" {
		var params struct {
			Notifications struct {
				TaskIDs []string `json:"taskIds"`
			} `json:"notifications"`
		}
		if json.Unmarshal(request.Params, &params) == nil && params.Notifications.TaskIDs != nil {
			if !tasks.Supports(caps) {
				response.Error = asRPC(tasks.MissingCapability())
				return
			}
			// No stream owner is installed by this minimal decorator. Never fake an ack.
			response.Error = jsonrpc.NewMethodNotFound("task subscriptions are not configured", nil)
			return
		}
	}
	if h.Next == nil {
		response.Error = jsonrpc.NewMethodNotFound("method not configured", nil)
		return
	}
	h.Next.Serve(ctx, request, response)
	if response.Error != nil {
		return
	}
	var result map[string]json.RawMessage
	if json.Unmarshal(response.Result, &result) != nil {
		return
	}
	if string(result["resultType"]) == `"task"` && (request.Method != tasks.MethodCallTool || !tasks.Supports(caps)) {
		response.Result = nil
		response.Error = jsonrpc.NewInternalError("task result not permitted for this request", nil)
		return
	}
	if request.Method == "server/discover" && h.Service != nil && h.Owner != nil {
		var capabilities map[string]json.RawMessage
		_ = json.Unmarshal(result["capabilities"], &capabilities)
		if capabilities == nil {
			capabilities = map[string]json.RawMessage{}
		}
		var extensions map[string]json.RawMessage
		_ = json.Unmarshal(capabilities["extensions"], &extensions)
		if extensions == nil {
			extensions = map[string]json.RawMessage{}
		}
		extensions[tasks.Extension] = json.RawMessage(`{}`)
		capabilities["extensions"], _ = json.Marshal(extensions)
		result["capabilities"], _ = json.Marshal(capabilities)
		response.Result, _ = json.Marshal(result)
	}
}
func (h *Handler) OnNotification(ctx context.Context, notification *jsonrpc.Notification) {
	if h.Next != nil {
		h.Next.OnNotification(ctx, notification)
	}
}
func (h *Handler) resolve(ctx context.Context, request *jsonrpc.Request) (tasks.Owner, error) {
	if h.Service == nil || h.Owner == nil {
		return tasks.Owner{}, errors.New("task host not configured")
	}
	owner, err := h.Owner(ctx, request)
	if err != nil || !owner.Valid() {
		return tasks.Owner{}, tasks.ErrNotFound
	}
	return owner, nil
}
func decode(raw json.RawMessage, value any) error {
	if !tasks.ValidObject(raw) {
		return &tasks.RPCError{Code: -32602, Message: "invalid request parameters"}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return &tasks.RPCError{Code: -32602, Message: "invalid request parameters"}
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return &tasks.RPCError{Code: -32602, Message: "invalid request parameters"}
	}
	return nil
}
func validJulyMeta(params json.RawMessage) bool {
	var input struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}
	if json.Unmarshal(params, &input) != nil {
		return false
	}
	var version string
	if json.Unmarshal(input.Meta["io.modelcontextprotocol/protocolVersion"], &version) != nil || version != tasks.ProtocolVersion {
		return false
	}
	var info struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	return json.Unmarshal(input.Meta["io.modelcontextprotocol/clientInfo"], &info) == nil && info.Name != "" && info.Version != ""
}
func asRPC(err *tasks.RPCError) *jsonrpc.Error {
	return jsonrpc.NewError(err.Code, err.Message, err.Data)
}
func (h *Handler) reply(response *jsonrpc.Response, value any, err error) {
	if err != nil {
		var rpcError *tasks.RPCError
		switch {
		case errors.As(err, &rpcError):
			response.Error = asRPC(rpcError)
		case errors.Is(err, tasks.ErrNotFound):
			response.Error = jsonrpc.NewInvalidParamsError("task unavailable", nil)
		default:
			response.Error = jsonrpc.NewInternalError("task operation failed", nil)
		}
		return
	}
	response.Result, err = json.Marshal(value)
	if err != nil {
		response.Error = jsonrpc.NewInternalError("task response encoding failed", nil)
	}
}
func missingInputCapability(method string) *tasks.RPCError {
	key := map[string]string{"elicitation/create": "elicitation", "sampling/createMessage": "sampling", "roots/list": "roots"}[method]
	data, _ := json.Marshal(map[string]any{"requiredCapabilities": map[string]any{key: map[string]any{}}})
	return &tasks.RPCError{Code: tasks.MissingCapabilityCode, Message: fmt.Sprintf("Missing client capability for %s", method), Data: data}
}
