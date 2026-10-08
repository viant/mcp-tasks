package tasks

import (
	"encoding/json"
	"errors"
	"github.com/viant/mcp-protocol/schema"
)

const ClientCapabilitiesMeta = "io.modelcontextprotocol/clientCapabilities"

// Advertise preserves unrelated capabilities. Old capabilities.tasks is never used.
func Advertise(caps *schema.ClientCapabilities) {
	if caps == nil {
		return
	}
	clone := make(map[string]map[string]interface{}, len(caps.Extensions)+1)
	for key, value := range caps.Extensions {
		clone[key] = value
	}
	clone[Extension] = map[string]interface{}{}
	caps.Extensions = clone
}
func AdvertiseServer(caps *schema.ServerCapabilities) {
	if caps == nil {
		return
	}
	clone := make(map[string]map[string]interface{}, len(caps.Extensions)+1)
	for key, value := range caps.Extensions {
		clone[key] = value
	}
	clone[Extension] = map[string]interface{}{}
	caps.Extensions = clone
}
func Supports(caps schema.ClientCapabilities) bool {
	value, ok := caps.Extensions[Extension]
	return ok && value != nil
}
func ServerSupports(caps schema.ServerCapabilities) bool {
	value, ok := caps.Extensions[Extension]
	return ok && value != nil
}

// RequestCapabilities reads this request only; there is no initialize fallback.
func RequestCapabilities(params json.RawMessage) (schema.ClientCapabilities, error) {
	var envelope struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}
	var caps schema.ClientCapabilities
	if err := json.Unmarshal(params, &envelope); err != nil {
		return caps, err
	}
	raw, ok := envelope.Meta[ClientCapabilitiesMeta]
	if !ok {
		return caps, nil
	}
	if !ValidObject(raw) {
		return caps, errors.New("invalid per-request client capabilities")
	}
	return caps, json.Unmarshal(raw, &caps)
}
func MissingCapability() *RPCError {
	data, _ := json.Marshal(map[string]interface{}{"requiredCapabilities": map[string]interface{}{"extensions": map[string]interface{}{Extension: map[string]interface{}{}}}})
	return &RPCError{Code: MissingCapabilityCode, Message: "Missing required client capability", Data: data}
}
func SupportsInput(caps schema.ClientCapabilities, method string) bool {
	switch method {
	case "elicitation/create":
		return caps.Elicitation != nil
	case "sampling/createMessage":
		return caps.Sampling != nil
	case "roots/list":
		return caps.Roots != nil
	}
	return false
}
