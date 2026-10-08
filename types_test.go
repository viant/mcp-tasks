package tasks_test

import (
	"encoding/json"
	tasks "github.com/viant/mcp-tasks"
	"strings"
	"testing"
	"time"
)

func TestStableFlatWireAndRequiredFields(t *testing.T) {
	raw := json.RawMessage(`{"resultType":"task","taskId":"fixture-task","status":"working","createdAt":"2026-10-07T00:00:00Z","lastUpdatedAt":"2026-10-07T00:00:00Z","ttlMs":null,"pollIntervalMs":20}`)
	result, err := tasks.DecodeCreate(raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), `"task":`) || !strings.Contains(string(encoded), `"ttlMs":null`) {
		t.Fatal("flat required nullable shape")
	}
	for _, key := range []string{"taskId", "status", "createdAt", "lastUpdatedAt", "ttlMs", "resultType"} {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		delete(fields, key)
		invalid, _ := json.Marshal(fields)
		if _, err = tasks.DecodeCreate(invalid); err == nil {
			t.Fatal("accepted missing", key)
		}
	}
	invalid := strings.Replace(string(raw), `"working"`, `"completed"`, 1)
	if _, err = tasks.DecodeCreate([]byte(invalid)); err == nil {
		t.Fatal("missing completed result")
	}
	invalid = strings.Replace(string(raw), `"task"`, `"complete"`, 1)
	if _, err = tasks.DecodeCreate([]byte(invalid)); err == nil {
		t.Fatal("wrong creation discriminator")
	}
	if _, err = tasks.DecodeGet(raw); err == nil {
		t.Fatal("get cannot return task discriminator")
	}
	if (tasks.Task{CreatedAt: time.Now(), TTLMS: nil}).Expired(time.Now().Add(24 * time.Hour)) {
		t.Fatal("null TTL expired")
	}
}
func TestRejectStatusPayloadAndNestedTaskResults(t *testing.T) {
	base := `"taskId":"fixture","createdAt":"2026-10-07T00:00:00Z","lastUpdatedAt":"2026-10-07T00:00:00Z","ttlMs":50,"resultType":"complete"`
	for _, payload := range []string{`"status":"failed"`, `"status":"completed","result":{"resultType":"task","taskId":"nested"}`, `"status":"cancelled","result":{}`, `"status":"working","inputRequests":{"key":{"method":"elicitation/create","params":{}}}`, `"status":"input_required","inputRequests":{}`} {
		if _, err := tasks.DecodeGet([]byte(`{` + base + `,` + payload + `}`)); err == nil {
			t.Fatal("invalid payload", payload)
		}
	}
}
