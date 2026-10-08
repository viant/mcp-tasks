# MCP Tasks for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/viant/mcp-tasks.svg)](https://pkg.go.dev/github.com/viant/mcp-tasks)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Go support for the official [`io.modelcontextprotocol/tasks` extension, 2026-07-28](https://tasks.extensions.modelcontextprotocol.io/specification/2026-07-28/tasks).
This module targets the dated specification, not the draft or the incompatible
2025-11-25 Tasks API. Core MCP schemas remain in `mcp-protocol`; transports remain
in `viant/mcp`. This module supplies extension types, policy/state interfaces,
a reference durable store, an opt-in RPC decorator, and a polling client.

## Quick start

Requires Go 1.25.5 or later.

```sh
go get github.com/viant/mcp-tasks
```

An existing runtime implements the small `tasks.Backend` interface. It creates
an owned handle only after its native execution is durably admitted, projects
that execution on `Get`, and delegates `Update` / `Cancel` to its ordinary control
APIs. The optional reference implementation is useful for a small service or
for testing a new adapter:

```go
storage, err := store.Open("./task-state", 1000)
if err != nil { return err }
backend, err := reference.New(storage, reference.Options{})
if err != nil { return err }

caps := schema.ClientCapabilities{}
tasks.Advertise(&caps)
owner := tasks.Owner{Tenant: "my-service", Subject: authenticatedSubject}
handle, err := backend.Create(ctx, owner, caps, tasks.CreateOptions{
    Method: tasks.MethodCallTool,
})
// Return handle only after err == nil. Dispatch work through your existing
// executor; persist its original MCP result with backend.Complete(...).
```

Imports for this example are `github.com/viant/mcp-tasks` (alias `tasks`),
`github.com/viant/mcp-tasks/reference`, `github.com/viant/mcp-tasks/store`, and
`github.com/viant/mcp-protocol/schema`. A complete runnable example is in
[`example_test.go`](example_test.go).

An opt-in client handles either an ordinary result or a task handle:

```go
caller, err := rpc.NewHTTPCaller(endpoint, authenticatedHTTPClient)
if err != nil { return err }
requester, err := client.New(caller, client.Options{
    Tasks: true,
    Info: schema.Implementation{Name: "my-client", Version: "1.0"},
})
if err != nil { return err }
outcome, err := requester.CallTool(ctx, &schema.CallToolRequestParams{
    Name: "generate_report",
})
if err != nil { return err }
if outcome.Task != nil {
    // Persist taskId and the input cursor to recover observation after restart.
    result, err := requester.Wait(ctx, outcome.Task.TaskID, client.WaitOptions{
        Initial: &outcome.Task.Task,
    })
    _ = result
    return err
}
// outcome.Result contains the unchanged regular or MRTR result.
```

`Wait` returns an `InputRequiredError` unless an explicit `Input` callback is
provided. That callback must use the same sampling, elicitation, consent and
approval policy as normal MCP requests. `SaveCursor` records input attempt state
before side effects; a lost acknowledgement is not permission to retry it.
`Cancel` explicitly acknowledges cancellation intent. Cancelling the observation
context only stops polling and leaves native work running.

## Design

A task is a durable view of an existing execution, not an execution scheduler.
`Store` adapters may map the task directly onto an application's canonical run
store. `reference.Service` maintains the reference task state; it does not start workers,
retry operations, replay original tool calls, or cancel work on disconnect.
The supplied file store is a small reference backend, not a replacement for
Agently's existing durable execution engine.

The server decides whether to create a task for each tool request. The client
must declare the extension in that request's `_meta` capabilities; declarations
on another request or an older initialization do not authorize this response.
Calls that do not use task augmentation keep their existing result and behavior.
The creator commits the owned record and verifies that it can be read before
exposing its handle. Hosts that create work through the reference backend must
schedule it only after this durable barrier; adapters may instead capture an
already-admitted canonical native run.

Task IDs use 192 bits of random entropy, but are not treated as authorization.
The caller supplies an authenticated tenant/subject identity from trusted request
context. Every get, update and cancellation checks it; callers cannot select
owners through wire parameters. Foreign and missing IDs have the same outcome.
Input requests use lifetime-unique keys, durable response acceptance, and the
same host consent/elicitation/sampling policy as their non-task equivalents.
Cancellation records intent; its acknowledgement does not prove work stopped.

## Packages and contract

- Root `tasks`: wire types, negotiation and backend/storage contracts.
- `reference`: optional lifecycle/state implementation, with no scheduler.
- `store`: file-backed durable reference implementation with process-safe locks,
  atomic replacement, file/directory sync, optimistic revisions and explicit
  expiry cleanup. It starts no executions.
- `rpc`: `transport.Handler` decorator, per-request authentication/negotiation,
  raw result dispatch, and a bounded HTTP JSON-RPC caller with task routing headers.
- `client`: task-aware tool results, polling, explicit update/cancel and optional
  input handling/checkpoints. Transport errors never retry a mutation or the
  original tool call; unknown input outcomes require reconciliation.

| Surface | Behavior |
| --- | --- |
| `tools/call` | Either the normal result or flat `resultType: "task"` handle |
| `tasks/get` | `resultType: "complete"`, full current state and status payload |
| `tasks/update` | Partial keyed input acceptance; complete empty acknowledgement |
| `tasks/cancel` | Durable cooperative intent; complete empty acknowledgement |
| `working`, `input_required` | Progress and all outstanding requests |
| `completed` | Original result, including tool-level `isError: true` |
| `failed` | JSON-RPC error only |
| `notifications/tasks` | Typed full-state notification and ownership helpers; stream wiring remains host-owned |
| `subscriptions/listen` task filters | Missing-capability validation; actual task subscriptions explicitly unsupported by the basic decorator |
| HTTP | `Mcp-Method` plus `Mcp-Name: taskId` on get/update/cancel |
| Legacy API | No `tasks/list`, `tasks/result`, task-preference parameter or legacy task capability claim |

`Store.Create` and `Store.CompareAndSwap` must be atomic and durable before
success. A failed/uncertain commit must not be repeated automatically. CAS
conflicts may reread state and retry only the pure state mutation, never a worker
or input-handler side effect. Host integrations own execution policy, quotas,
job dispatch, approval, durable input delivery and recovery of uncertain work.

## Integration boundary

The RPC decorator can wrap an existing JSON-RPC handler. Its owner resolver must
perform the same authentication and authorization as the host; interception
must not bypass a sibling handler's private authorization pipeline. Non-task
methods delegate unchanged. A tool decision callback is read-only planning and
policy, not worker dispatch. Discovery advertises Tasks only for a configured
service. Optional subscription streams are not silently acknowledged.

Minimal native `viant/mcp` hooks needed for seamless high-level integration:

1. Extension dispatch after normal protocol/request-context/auth preparation,
   before its closed method switch, with raw JSON result support.
2. Preserve the `tools/call` result discriminator before decoding the fixed
   `CallToolResult` shape; opt-in callers can expose a handle or poll to completion.
3. Route Tasks requests by `taskId` in the Streamable HTTP `Mcp-Name` header and
   attach July per-request metadata. A raw transport supplied to this module can
   already carry extension requests without changing core schemas.
4. If notifications are enabled later, add owned `taskIds` filters and full-state
   notifications to the existing subscription hub. Do not send task progress or
   logging on that stream.

For Agently, adapt these hooks to existing runs, deferred inputs, approvals,
resume/cancel APIs and AG-UI projections; do not create another native task engine.
Keep native causal links in the adapter's private record, not caller-supplied IDs.

| Existing execution mode | Required adapter behavior / acceptance |
| --- | --- |
| Foreground | Preserve ordinary results for non-negotiated requests; same final tool result under Tasks |
| Skill fork | Parent still waits for the canonical child result; retain parent/child and invocation links |
| Detached child | Child outlives parent/HTTP observer disconnect; task polling observes the same owned child |
| Approval / input | Surface existing requests once, preserve keys and policy, resume the original run |
| Cancellation | Call existing scoped cancellation; acknowledgement remains distinct from the terminal outcome |
| Recovery | Recover canonical run/task IDs and input cursor; never replay execution to reconstruct a handle |
| AG-UI | Project canonical native events; Tasks polling must not become a competing execution owner |

Existing native fork/detach tests alone do not prove standard Tasks interoperability.
The consuming adapter needs tests for each row over a real MCP connection, including
lost responses, restart, foreign-owner polling, partial inputs and cancellation races.
The Agently Core source adapter lives in `sdk/mcp_tasks.go`, with an opt-in
transport wrapper in `protocol/mcp/expose/tasks.go`. It projects existing owned
AG-UI/native executions and persists aliases/control receipts in the existing
native store. Enabling it requires the host's trusted capture, exported-tool
policy and owner resolver; importing this module does not automatically enable
an endpoint or authorize arbitrary tools. No runtime/UI deployment is performed
by this module.

## Testing and status

```sh
go test ./...
go test -race ./...
go vet ./...
```

Tests cover flat wire decoding, required/null fields, per-request capability
negotiation, missing capability error `-32021`, owner/tenant isolation, atomic
cross-instance storage, durable reopen, partial/deduplicated input, unique keys,
TTL, tool versus protocol errors, cooperative cancellation and competing
terminal transitions. Integration tests use the real `viant/mcp` handler and
ordinary high-level client, a task-aware raw client, and HTTP routing headers.
No live service, application account, credentials or UI is required.

The dated Tasks request/poll/update/cancel contract is implemented and tested.
Optional server subscription streaming is not implemented: the decorator rejects
it explicitly and exposes only typed full-state notifications for host adapters.
The older 2025-11-25 API is deliberately separate. Production store retention,
quotas, process shutdown, request authentication, native scheduling and recovery
are host responsibilities. Tests do not certify an application's deployment.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
