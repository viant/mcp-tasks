package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	"github.com/viant/mcp-tasks/reference"
	"github.com/viant/mcp-tasks/store"
	"sync"
	"testing"
	"time"
)

func TestDurableReopenAndCrossInstanceCAS(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	first, err := store.Open(directory, 16)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Open(directory, 16)
	if err != nil {
		t.Fatal(err)
	}
	service, _ := reference.New(first, reference.Options{})
	caps := schema.ClientCapabilities{}
	tasks.Advertise(&caps)
	owner := tasks.Owner{Tenant: "tenant", Subject: "owner"}
	created, err := service.Create(ctx, owner, caps, tasks.CreateOptions{Method: tasks.MethodCallTool, Origin: json.RawMessage(`{"nativeRun":"existing-run"}`)})
	if err != nil {
		t.Fatal(err)
	}
	before, err := second.Get(ctx, owner, created.TaskID)
	if err != nil {
		t.Fatal("durable task not visible after reopen", err)
	}
	next := before
	next.Revision++
	next.State.LastUpdatedAt = time.Now()
	next.State.StatusMessage = "candidate"
	var group sync.WaitGroup
	group.Add(2)
	results := make(chan error, 2)
	go func() {
		defer group.Done()
		results <- first.CompareAndSwap(ctx, owner, created.TaskID, before.Revision, next)
	}()
	go func() {
		defer group.Done()
		results <- second.CompareAndSwap(ctx, owner, created.TaskID, before.Revision, next)
	}()
	group.Wait()
	close(results)
	success, conflicts := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, tasks.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal("non-atomic cross-instance CAS", success, conflicts)
	}
	foreign := tasks.Owner{Tenant: "tenant", Subject: "foreign"}
	if _, err = second.Get(ctx, foreign, created.TaskID); !errors.Is(err, tasks.ErrNotFound) {
		t.Fatal("ownership leak")
	}
}
