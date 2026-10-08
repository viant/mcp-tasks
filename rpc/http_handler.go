package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
)

// HTTPHandler is a bounded stateless July JSON-RPC/SSE adapter. Host middleware
// must install authenticated request context before Owner is consulted.
// Build receives a request-scoped notifier, just like viant/mcp's transport
// handler factory. Normal methods may delegate to the host's existing handler.
type HTTPHandler struct {
	Build func(context.Context, transport.Notifier) transport.Handler
	// Present Origin headers are denied by default. Hosts may explicitly admit
	// approved web origins; this does not replace request authentication.
	AllowOrigin func(string) bool
}

func (h HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" && (h.AllowOrigin == nil || !h.AllowOrigin(origin)) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var request jsonrpc.Request
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, tasks.MaxJSONBytes))
	if err != nil || json.Unmarshal(raw, &request) != nil {
		http.Error(w, "invalid bounded JSON-RPC request", http.StatusBadRequest)
		return
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	_, hasID := fields["id"]
	if request.Jsonrpc != "2.0" || request.Method == "" || hasID && !ValidRequestID(request.Id) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(&jsonrpc.Response{Jsonrpc: "2.0", Id: request.Id, Error: jsonrpc.NewInvalidRequest("invalid MCP request identity", nil)})
		return
	}
	if h.Build == nil {
		http.Error(w, "MCP handler unavailable", http.StatusServiceUnavailable)
		return
	}
	if routingError := validateHTTPRouting(r, &request); routingError != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(&jsonrpc.Response{Jsonrpc: "2.0", Id: request.Id, Error: asRPC(routingError)})
		return
	}
	if r.Header.Get("Mcp-Protocol-Version") == tasks.ProtocolVersion {
		w.Header().Set("Mcp-Protocol-Version", tasks.ProtocolVersion)
	}
	stream := request.Method == tasks.MethodListen
	if stream {
		if _, ok := w.(http.Flusher); !ok {
			http.Error(w, "streaming unavailable", http.StatusInternalServerError)
			return
		}
	}
	sink := &httpNotificationSink{writer: w, stream: stream}
	handler := h.Build(r.Context(), sink)
	if handler == nil {
		http.Error(w, "MCP handler unavailable", http.StatusServiceUnavailable)
		return
	}
	response := &jsonrpc.Response{Jsonrpc: "2.0", Id: request.Id}
	if request.Id == nil && request.Jsonrpc == "2.0" && request.Method != "" {
		handler.OnNotification(r.Context(), &jsonrpc.Notification{Jsonrpc: "2.0", Method: request.Method, Params: request.Params})
		w.WriteHeader(http.StatusAccepted)
		return
	}
	handler.Serve(r.Context(), &request, response)
	if r.Context().Err() != nil {
		return
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.started {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(15 * time.Second))
		encoded, _ := json.Marshal(response)
		_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", encoded)
		w.(http.Flusher).Flush()
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if response.Error != nil {
		switch response.Error.Code {
		case -32601:
			w.WriteHeader(http.StatusNotFound)
		case -32600, -32602, -32020, -32021, -32022:
			w.WriteHeader(http.StatusBadRequest)
		}
	}
	_ = json.NewEncoder(w).Encode(response)
}

// Validate extension routing before handler construction/admission. Legacy
// ordinary calls still delegate to the host's normal transport semantics.
func validateHTTPRouting(r *http.Request, request *jsonrpc.Request) *tasks.RPCError {
	var params map[string]json.RawMessage
	_ = json.Unmarshal(request.Params, &params)
	var meta map[string]json.RawMessage
	_ = json.Unmarshal(params["_meta"], &meta)
	var version string
	_ = json.Unmarshal(meta["io.modelcontextprotocol/protocolVersion"], &version)
	header := r.Header.Get("Mcp-Protocol-Version")
	taskMethod := request.Method == tasks.MethodGet || request.Method == tasks.MethodUpdate || request.Method == tasks.MethodCancel
	if !taskMethod && version != tasks.ProtocolVersion && header != tasks.ProtocolVersion {
		return nil
	}
	if header == "" || header != version {
		return &tasks.RPCError{Code: schema.ErrorCodeHeaderMismatch, Message: "Mcp-Protocol-Version must match per-request metadata"}
	}
	if version != tasks.ProtocolVersion {
		return &tasks.RPCError{Code: schema.ErrorCodeUnsupportedProtocolVersion, Message: "dated Tasks protocol version required"}
	}
	if !ValidRequestMetadata(request.Params) {
		return &tasks.RPCError{Code: -32602, Message: "required per-request metadata missing or malformed"}
	}
	if r.Header.Get("Mcp-Method") != request.Method {
		return &tasks.RPCError{Code: schema.ErrorCodeHeaderMismatch, Message: "Mcp-Method must match request method"}
	}
	field := ""
	if taskMethod {
		field = "taskId"
	} else {
		switch request.Method {
		case "tools/call", "prompts/get":
			field = "name"
		case "resources/read":
			field = "uri"
		}
	}
	if field != "" {
		var name string
		_ = json.Unmarshal(params[field], &name)
		headerName, err := DecodeHeaderValue(r.Header.Get("Mcp-Name"))
		if name == "" || err != nil || headerName != name {
			return &tasks.RPCError{Code: schema.ErrorCodeHeaderMismatch, Message: "Mcp-Name must match request routing identity"}
		}
	}
	return nil
}

type httpNotificationSink struct {
	mu              sync.Mutex
	writer          http.ResponseWriter
	stream, started bool
}

func (s *httpNotificationSink) Notify(ctx context.Context, notification *jsonrpc.Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.stream {
		return fmt.Errorf("request does not own a subscription stream")
	}
	raw, err := json.Marshal(notification)
	if err != nil || len(raw) > tasks.MaxJSONBytes {
		return fmt.Errorf("notification exceeds budget")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = http.NewResponseController(s.writer).SetWriteDeadline(time.Now().Add(15 * time.Second))
	if !s.started {
		s.writer.Header().Set("Content-Type", "text/event-stream")
		s.writer.Header().Set("Cache-Control", "no-cache")
		s.writer.Header().Set("X-Accel-Buffering", "no")
		s.writer.WriteHeader(http.StatusOK)
		s.started = true
	}
	_, err = fmt.Fprintf(s.writer, "event: message\ndata: %s\n\n", raw)
	s.writer.(http.Flusher).Flush()
	return err
}
