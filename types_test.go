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
	if _, err = tasks.DecodeCreate([]byte(invalid)); err != nil {
		t.Fatal("creation is a Task seed, not a DetailedTask", err)
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

func TestCreationSeedSeparatesStatusPayloadFromGetAndNotification(t *testing.T) {
	base := `"taskId":"fixture","createdAt":"2026-10-07T00:00:00Z","lastUpdatedAt":"2026-10-07T00:00:00Z","ttlMs":null`
	for _, status := range []tasks.Status{tasks.Working, tasks.InputRequired, tasks.Completed, tasks.Failed, tasks.Cancelled} {
		raw := []byte(`{` + base + `,"resultType":"task","status":"` + string(status) + `"}`)
		seed, err := tasks.DecodeCreate(raw)
		if err != nil || seed.Status != status {
			t.Fatal("valid seed rejected", status, err)
		}
		if !status.Terminal() && status != tasks.InputRequired {
			continue
		}
		if status == tasks.Cancelled {
			continue
		}
		if _, err = tasks.DecodeGet([]byte(strings.Replace(string(raw), `"task"`, `"complete"`, 1))); err == nil {
			t.Fatal("detailed status accepted without required payload", status)
		}
	}
	seed, _ := tasks.DecodeCreate([]byte(`{` + base + `,"resultType":"task","status":"completed"}`))
	seed.Result = json.RawMessage(`{"content":[],"privateResult":"later Get only"}`)
	raw, _ := json.Marshal(seed)
	if strings.Contains(string(raw), "privateResult") || strings.Contains(string(raw), `"result":`) {
		t.Fatal("creation emitted DetailedTask payload")
	}
	valid := []byte(`{` + base + `,"status":"working"}`)
	if _, err := tasks.DecodeNotificationParams(valid); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(valid, &fields)
	delete(fields, "ttlMs")
	invalid, _ := json.Marshal(fields)
	if _, err := tasks.DecodeNotificationParams(invalid); err == nil {
		t.Fatal("notification accepted missing ttlMs")
	}
}
func TestRejectStatusPayloadAndNestedTaskResults(t *testing.T) {
	base := `"taskId":"fixture","createdAt":"2026-10-07T00:00:00Z","lastUpdatedAt":"2026-10-07T00:00:00Z","ttlMs":50,"resultType":"complete"`
	for _, payload := range []string{`"status":"failed"`, `"status":"failed","error":{"message":"missing code"}`, `"status":"failed","error":{"code":null,"message":"null code"}`, `"status":"completed","result":{"resultType":"task","taskId":"nested"}`, `"status":"cancelled","result":{}`, `"status":"working","inputRequests":{"key":{"method":"elicitation/create","params":{}}}`, `"status":"input_required","inputRequests":{}`} {
		if _, err := tasks.DecodeGet([]byte(`{` + base + `,` + payload + `}`)); err == nil {
			t.Fatal("invalid payload", payload)
		}
	}
}
