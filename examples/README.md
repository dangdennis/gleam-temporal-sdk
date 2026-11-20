# Examples

One module per pattern, under `src/examples/`. Each is self-contained and reads top to
bottom: what it shows and when to use it, the definitions, the workflow and activity code,
and a `run` function that starts a worker on its own task queue, runs the scenario and
returns what happened.

| File | Shows |
|---|---|
| [`hello.gleam`](src/examples/hello.gleam) | One workflow, one activity, a worker, and a client that starts the workflow and reads its result |
| [`activity_retries.gleam`](src/examples/activity_retries.gleam) | Activity timeouts, a retry policy, failure types, non-retryable failures |
| [`timers.gleam`](src/examples/timers.gleam) | `sleep`, `with_timeout`, `await_timeout`, racing futures |
| [`cancellation.gleam`](src/examples/cancellation.gleam) | Cancelling a scope, cleanup in a non-cancellable scope, a workflow cancelled from the client |
| [`fan_out.gleam`](src/examples/fan_out.gleam) | Many activities at once, `try_await_all` and `await_all` |
| [`signals.gleam`](src/examples/signals.gleam) | Receiving signals, waiting with a deadline, `client.signal`, signal-with-start |
| [`queries.gleam`](src/examples/queries.gleam) | Query handlers over state that changes, queries with and without an argument |
| [`updates.gleam`](src/examples/updates.gleam) | A validator, receiving and answering updates, `client.execute_update`, update-with-start |
| [`child_workflows.gleam`](src/examples/child_workflows.gleam) | Starting a child and reading its result, signalling a child, a child that outlives its parent |
| [`saga.gleam`](src/examples/saga.gleam) | Compensating for completed steps when a later one fails |
| [`continue_as_new.gleam`](src/examples/continue_as_new.gleam) | A long loop whose history stays bounded |
| [`local_activities.gleam`](src/examples/local_activities.gleam) | Short activities run inside the workflow's worker |
| [`heartbeats.gleam`](src/examples/heartbeats.gleam) | Heartbeats with progress, resuming after a failed attempt, an activity that accepts cancellation |
| [`async_completion.gleam`](src/examples/async_completion.gleam) | An activity finished later, from outside, by its task token |
| [`standalone_activities.gleam`](src/examples/standalone_activities.gleam) | An activity started from a client with no workflow: execute, retries, describe, cancel |
| [`schedules.gleam`](src/examples/schedules.gleam) | Creating, describing, pausing, triggering, backfilling and deleting a schedule |
| [`data.gleam`](src/examples/data.gleam) | `temporal/schema` objects with nested and optional fields, several arguments, no arguments, a hand-written codec |
| [`encryption.gleam`](src/examples/encryption.gleam) | The encryption payload codec, and rotating its key |
| [`middleware.gleam`](src/examples/middleware.gleam) | Workflow and activity middleware, rewriting what a workflow schedules, headers |
| [`logging_and_metrics.gleam`](src/examples/logging_and_metrics.gleam) | Log lines with fields from workflows and activities, Prometheus metrics |
| [`search_attributes.gleam`](src/examples/search_attributes.gleam) | Search attributes and memo, `client.list`, `client.count`, `client.describe` |
| [`versioning.gleam`](src/examples/versioning.gleam) | `patched` and `deprecate_patch`, with the replays that show a change is safe |
| [`testing.gleam`](src/examples/testing.gleam) | An activity tested without a server, a workflow on a time-skipping server, a history replayed |
| [`otp.gleam`](src/examples/otp.gleam) | A worker under a supervisor, graceful shutdown, draining on SIGTERM |
| [`interop.gleam`](src/examples/interop.gleam) | Calling activities that another SDK's worker implements, with its names and JSON shapes |
| [`cloud.gleam`](src/examples/cloud.gleam) | Connecting to Temporal Cloud with an API key or a client certificate. Compiled, not run |
| [`nexus.gleam`](src/examples/nexus.gleam) | Nexus operations: a handler, and a workflow that calls them |

## Running them

From the repository root, run them all. This builds the native library, starts a
development server if none is running, and runs `examples/test`, which calls every example
and checks what it returns:

```sh
bin/examples
```

To run one and print what it returned, start a server and name the example by its file:

```sh
bin/build-nif                    # once, from the repository root
temporal server start-dev \
  --dynamic-config-value system.enableNexus=true \
  --dynamic-config-value activity.enableStandalone=true
cd examples
gleam run -- hello
```

Three need something set up first:

- `search_attributes` uses a custom search attribute:
  `temporal operator search-attribute create --name ExamplesCustomer --type Keyword`
- `nexus` uses an endpoint:
  `temporal operator nexus endpoint create --name examples-nexus --target-namespace default --target-task-queue examples-nexus-handler`
- `cloud` reads its settings from the environment; see the top of the file.

The tests do the first two themselves.

Task queues and workflow ids are fixed and start with `examples-`. Running an example again
reuses its ids, which Temporal allows once the earlier run has closed. If a run was
interrupted and left a workflow open, terminate it (`temporal workflow terminate
--workflow-id <id>`, or `temporal activity terminate --activity-id <id>` for a standalone
activity) or restart the development server.

## Using them in your own project

The examples depend on the SDK by path. In your own project depend on it as the
[README](../README.md#install) describes, and copy what you need: nothing in an example
depends on another.
