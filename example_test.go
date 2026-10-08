package tasks_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/mcp-protocol/schema"
	tasks "github.com/viant/mcp-tasks"
	"github.com/viant/mcp-tasks/reference"
	"github.com/viant/mcp-tasks/store"
	"os"
)

func Example() {
	// Existing runtimes can implement tasks.Backend directly. This optional
	// reference example stores state only; it never schedules a worker.
	directory, err := os.MkdirTemp("", "mcp-tasks-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(directory)
	storage, err := store.Open(directory, 100)
	if err != nil {
		panic(err)
	}
	backend, err := reference.New(storage, reference.Options{})
	if err != nil {
		panic(err)
	}
	capabilities := schema.ClientCapabilities{}
	tasks.Advertise(&capabilities)
	owner := tasks.Owner{Tenant: "example", Subject: "authenticated-user"}
	ctx := context.Background()
	created, err := backend.Create(ctx, owner, capabilities, tasks.CreateOptions{Method: tasks.MethodCallTool})
	if err != nil {
		panic(err)
	}
	// A real host completes this from its original canonical executor result.
	err = backend.Complete(ctx, owner, created.TaskID, json.RawMessage(`{"resultType":"complete","content":[{"type":"text","text":"done"}]}`))
	if err != nil {
		panic(err)
	}
	result, err := backend.Get(ctx, owner, created.TaskID)
	if err != nil {
		panic(err)
	}
	fmt.Println(created.ResultType, result.ResultType, result.Status)
	// Output: task complete completed
}
