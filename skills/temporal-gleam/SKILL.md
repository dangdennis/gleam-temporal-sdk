---
name: temporal-gleam
description: Use when writing or debugging Temporal workflows, activities, workers or clients in Gleam with the temporal_gleam SDK - defining workflows and activities, signals, queries, updates, child workflows, timers, cancellation, retries, schedules, testing with replay or time skipping, or diagnosing non-determinism and decode errors.
---

# temporal_gleam

A Temporal SDK for Gleam on the BEAM. Every snippet below is copied from a compiling
example in [`examples/src/examples/`](../../examples/src/examples/) or from its test; open
the linked file for the whole program.

## Install

```toml
[dependencies]
temporal_gleam = { git = "https://github.com/dangdennis/gleam-temporal-sdk", ref = "v0.2.0" }
```

```sh
gleam deps download
gleam run -m temporal/install   # the native library; again after `gleam clean`, and in CI
temporal server start-dev       # a local service, from the Temporal CLI
```

| Module | For |
|---|---|
| `temporal/workflow` | Definitions, workflow code: activities, timers, futures, scopes, `now`, `random_int`, `uuid4`, logging, `patched`, `continue_as_new` |
| `temporal/workflow/{signal,query,update,child,local_activity,operation,continue_as_new}` | One module per thing a workflow does besides activities |
| `temporal/activity` | Activity definitions and code: info, heartbeats, cancellation, `is_worker_shutting_down`, logging |
| `temporal/worker` | Running workflows and activities: `run`, `start`/`stop`, `await_shutdown_signal`, `supervised`; middleware |
| `temporal/client` | Connect; start, execute, signal, query, update, cancel, describe, list |
| `temporal/retry`, `temporal/failure` | Retry policies; the `Failure` type |
| `temporal/codec`, `temporal/schema` | Codecs for values; JSON objects described once |
| `temporal/schedule`, `temporal/nexus`, `temporal/encryption`, `temporal/telemetry`, `temporal/log` | Schedules; serving Nexus; payload encryption; metrics and the core library's log level; log fields |
| `temporal/testing`, `temporal/replayer` | Test servers, activity contexts; history replay |

## The core shape: definition, handler, worker, client

From [`hello.gleam`](../../examples/src/examples/hello.gleam). A type mismatch between a
definition and its implementation or caller is a compile error.

```gleam
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow

const task_queue = "examples-hello"

// A definition names a workflow or activity and fixes its input and output types. The
// code that implements one and the code that calls it share the definition.

pub fn greet() {
  workflow.Definition("greet", codec.string(), codec.string())
}

pub fn say_hello() {
  activity.Definition("say_hello", codec.string(), codec.string())
}

/// Workflow code decides what happens and in which order. It must be deterministic.
fn greet_workflow(
  ctx: workflow.Context,
  name: String,
) -> Result(String, Failure) {
  let options = workflow.activity_options(start_to_close: duration.seconds(10))
  workflow.execute_activity(ctx, say_hello(), name, options)
}

/// Activity code does the work. It is ordinary Gleam and may do anything.
fn say_hello_activity(
  _ctx: activity.Context,
  name: String,
) -> Result(String, Failure) {
  Ok("Hello, " <> name <> "!")
}

/// The worker: the task queue it polls and what it can run.
fn worker_config(client: Client) -> worker.Config {
  worker.new(client, task_queue)
  |> worker.with_workflows([workflow.handler(greet(), greet_workflow)])
  |> worker.with_activities([activity.handler(say_hello(), say_hello_activity)])
}

/// Start the worker, run one workflow and stop the worker again, as a script or a test
/// does.
pub fn run(client: Client) -> Result(String, client.Error) {
  use worker <- result.try(worker.start(worker_config(client)))
  let options = client.start_options(workflow_id: "examples-hello", task_queue:)
  let greeting = client.execute(client, greet(), "Gleam", options)
  worker.stop(worker)
  greeting
}

/// How a worker runs for real: `worker.run` serves the task queue until the operating
/// system asks the program to stop with SIGTERM, as a deploy does, then lets the
/// activities in flight finish and returns. Never keep a worker alive with
/// `process.sleep_forever`: SIGTERM would stop the VM under its activities.
pub fn main() -> Nil {
  let assert Ok(client) = client.connect(client.local())
  let assert Ok(Nil) = worker.run(worker_config(client))
  Nil
}
```

`run` is for scripts and tests. A deployed worker is `main`: `worker.run(config)` serves
until SIGTERM and then stops gracefully. Never keep a worker alive with
`process.sleep_forever()`; a deploy would abandon every activity in flight. For several
workers use `worker.start` on each, `worker.await_shutdown_signal()`, then `worker.stop`
on each; in an OTP application's supervision tree use `worker.supervised(config)`.

The client comes from `client.connect(client.local())`. Workflow and activity functions
always have the shape `fn(Context, input) -> Result(output, Failure)`. One input, one
output: for several arguments use `codec.args2`..`args4` and a tuple, for none
`codec.no_args()`, for a JSON object `schema.codec(schema.object2(..))`. See
[`data.gleam`](../../examples/src/examples/data.gleam).

## Determinism rules for workflow code

A workflow function is replayed from its history, so it must issue the same commands in
the same order every time. Nothing enforces this; replay tests find the mistakes.

| Never, in workflow code | Use instead |
|---|---|
| `process.sleep`, `timer` modules | `workflow.sleep(ctx, duration)`, `workflow.timer` |
| System time (`timestamp.system_time`) | `workflow.now(ctx)` |
| Randomness, generated ids | `workflow.random_int(ctx, max)`, `workflow.uuid4(ctx)` |
| IO, HTTP, databases, files, `io.println` | An activity; `workflow.log_info` for logging |
| Sending or receiving process messages, spawning | Signals, activities, child workflows |
| Reading mutable global state (ETS, `persistent_term`, env vars, config) | Pass it in the input, or read it in an activity |
| Side effects such as metrics or error reports | `workflow.unless_replaying(ctx, fn() { .. })` |

- `workflow.*`, `signal.*`, `update.*`, `child.*` and the rest work only in the process the
  worker runs the workflow in: not in activities, tests, or processes spawned from it.
- Workflow state is the function's arguments and local variables. Loop by recursion.
- Changing the order or kind of commands (activities, timers, children, signals sent) in
  code that has runs in flight needs `workflow.patched`: see Testing.

## Patterns: one way to do each thing

**Options** are a constructor that takes what is required, then `with_` setters in a
pipeline. Never build or spread the option records. From
[`activity_retries.gleam`](../../examples/src/examples/activity_retries.gleam):

```gleam
  let policy =
    retry.policy(maximum_attempts: 5)
    |> retry.with_initial_interval(duration.milliseconds(100))
    |> retry.with_backoff_coefficient(2.0)
    |> retry.with_maximum_interval(duration.seconds(2))
    |> retry.with_non_retryable_error_types(["CardDeclined"])
  let options =
    // The longest one attempt may run.
    workflow.activity_options(start_to_close: duration.seconds(5))
    // The longest the activity may take across every attempt.
    |> workflow.with_schedule_to_close(duration.minutes(1))
    |> workflow.with_retry(policy)
```

The same pattern everywhere: `client.start_options(workflow_id:, task_queue:)`,
`child.options(workflow_id:)`, `local_activity.options(start_to_close:)`,
`operation.options()`, `worker.new(client, task_queue)`, `schedule.new(spec, action)`.

**Sequence fallible steps** with `use x <- result.try(..)`: every wait returns a `Result`.
**Futures:** `start_*` returns a future at once; `execute_*` is start plus `await`. Wait
with `workflow.await`, `await_all`, `try_await_all` (stops at the first failure) or `race`.
From [`fan_out.gleam`](../../examples/src/examples/fan_out.gleam):

```gleam
  // All or nothing: the values in the order given, or the first failure.
  let measuring =
    list.map(images, workflow.start_activity(ctx, measure(), _, options))
  use sizes <- result.try(workflow.try_await_all(measuring))
```

**Signals and queries.** `signal.receive` blocks; `signal.next` is a future, to race or to
wait on with a deadline (`workflow.await_timeout(ctx, future, duration)`). A query handler
closes over the current state and is set again when it changes. From
[`queries.gleam`](../../examples/src/examples/queries.gleam):

```gleam
  // Set again on every turn of the loop, so the handlers always see the current count.
  query.set_handler(ctx, total(), fn(_) { count })
  query.set_handler(ctx, at_least(), fn(threshold) { count >= threshold })

  let next =
    workflow.race([
      signal.next(ctx, add()) |> workflow.map(Some),
      signal.next(ctx, finish()) |> workflow.map(fn(_) { None }),
    ])
  case workflow.await(next) {
    Some(amount) -> counter_workflow(ctx, count + amount)
    None -> Ok(count)
  }
```

**Updates** are pulled like signals (`update.next`, `update.receive`) and answered with
`update.complete`; they do not run beside the workflow as in other SDKs.
`update.set_validator` refuses bad ones. From
[`updates.gleam`](../../examples/src/examples/updates.gleam):

```gleam
  case workflow.await(event) {
    Added(request) -> {
      let items = list.append(items, [update.argument(request)])
      // Work done before answering, such as an activity, happens while the client waits.
      update.complete(ctx, request, Ok(list.length(items)))
      cart_workflow(ctx, items)
    }
    CheckedOut -> Ok(items)
  }
```

Client side: `client.signal(handle, def, value)`, `client.query(handle, def, argument)`,
`client.execute_update(handle, def, argument)`, `client.signal_with_start`,
`client.execute_update_with_start`; `client.handle(client, def, workflow_id)` makes a handle
for a workflow started elsewhere.

**Cancellation scopes.** Whatever is started with a context fails with a cancellation when
its scope is cancelled; cancelling the workflow cancels the `ctx` it was given.
`workflow.with_scope` + `workflow.cancel(scope)` cancels part of a workflow,
`workflow.with_timeout` gives a part a deadline, `workflow.non_cancellable` shields cleanup.
From [`cancellation.gleam`](../../examples/src/examples/cancellation.gleam), after a
`workflow.sleep(ctx, ..)` returned `Error(cancelled)`:

```gleam
      use _ <- result.try(
        workflow.non_cancellable(ctx, fn(shielded) {
          workflow.execute_activity(shielded, release_seat(), seat, options)
        }),
      )
      // Returning the cancellation is what ends the workflow as cancelled.
      Error(cancelled)
```

An activity learns of cancellation only by heartbeating: call `activity.heartbeat(ctx)`,
check `activity.is_cancelled(ctx)`, and return `Error(failure.cancelled())`. See
[`heartbeats.gleam`](../../examples/src/examples/heartbeats.gleam).

**Worker shutdown.** When a worker is asked to stop (`worker.stop`, SIGTERM under
`worker.run`, a supervisor shutting down), in order: polling stops and
`activity.is_worker_shutting_down(ctx)` turns true; workflow tasks in flight finish;
activities run on for the shutdown grace (`worker.with_shutdown_grace`, 10 s); those left
are told to cancel with `activity.cancel_reason(ctx) == Some(activity.WorkerShutdown)`;
those left after the activity cancellation grace
(`worker.with_activity_cancellation_grace`, 30 s) are killed. An activity cancelled or
killed this way is retried on another worker. `worker.stop` returns when all of it is
over. A long activity should check `is_worker_shutting_down`, heartbeat its progress
with `activity.heartbeat_with` and return an error, so a deploy does not wait for it.

**Continue-as-new** is `workflow.continue_as_new(ctx, definition, input)`; the new run
keeps the task queue, memo, search attributes, retry policy and headers. To change them
or set timeouts: `continue_as_new.options() |> continue_as_new.with_task_queue(..)` and
`continue_as_new.run(ctx, definition, input, options)`, from
`temporal/workflow/continue_as_new`.

**Outbound middleware** takes the workflow's context first:
`worker.with_outbound_activity(fn(ctx, call) { .. })`. Change the call with
`middleware.with_activity_type` and its siblings, and guard a change with
`workflow.patched(ctx, ..)`. See
[`middleware.gleam`](../../examples/src/examples/middleware.gleam).

**Values from other SDKs.** TypeScript's `undefined`, and any argument a caller leaves
out, arrive as a null: declare them `schema.codec(schema.optional(..))`, or `codec.nil()`
for a result that is nothing. `codec.float()` reads `10`; `codec.int()` reads `1e21`.
Integers above 2^53 lose precision in JavaScript: send them as strings.

Everything else has an example: see the table in
[`examples/README.md`](../../examples/README.md) (timers, child workflows, saga,
continue-as-new, local activities, schedules, middleware, supervised workers, other SDKs'
activities, Nexus, encryption, Temporal Cloud).

## Failures

Workflows and activities fail by returning `Error(Failure)`. From
[`activity_retries.gleam`](../../examples/src/examples/activity_retries.gleam):

```gleam
  // The type lets the retry policy, and the workflow, tell failures apart.
  Error(
    failure.new("the card was declined") |> failure.with_type("CardDeclined"),
  )
```

- `|> failure.non_retryable` stops retries whatever the policy says.
- A failed activity or child reaches the workflow as a wrapper (`kind` `Activity` or
  `ChildWorkflow`). The error the code returned is `failure.root_cause(failed)`; match on
  its `error_type`. `failure.is_cancellation` and `failure.is_timeout` look through wrappers.
- Returning a received failure as it is passes it on whole: clients see an activity
  failure with its cause. `failure.with_type`, `non_retryable` and `with_details` make it
  the workflow's own application failure instead, with the same cause. Never set a
  failure's `original` field.
- In an activity, `activity.is_final_attempt(ctx)` says whether the retry policy allows
  no attempt after this one; `activity.info(ctx).retry` is the policy.
- Retried: a failed activity, without limit unless `workflow.with_retry` says otherwise;
  a local activity likewise; an attempt that passes its start-to-close or heartbeat
  timeout. Not retried: a `non_retryable` failure, a type the policy lists, a
  cancellation, a schedule-to-close timeout, and workflows and child workflows unless
  given a policy with `client.with_retry` or `child.with_retry`.
- A crash in workflow code (a failed `let assert`) fails the workflow task, not the
  workflow: the server retries the task until fixed code is deployed. A crash in an
  activity is an ordinary retryable failure.
- Client calls return `Result(_, client.Error)`: `WorkflowFailed(failure)`,
  `WorkflowCancelled`, `WorkflowTimedOut`, `NotFound`, `AlreadyExists`, `UpdateFailed`,
  `QueryFailed`, `DecodeFailed`, `ConnectionFailed`.

## Testing

A replay test runs recorded histories against the current workflow code; it needs no
server. Fetch histories with `client.history(handle)`. From
[`versioning.gleam`](../../examples/src/examples/versioning.gleam) and its test:

```gleam
  let replay = fn(version, history) {
    replayer.replay([workflow.handler(process_order(), version)], history)
  }
```

It returns `Ok(Nil)`, or `Error(replayer.Nondeterminism(_))` when the code issues different
commands than the history records. The guard that makes such a change safe:

```gleam
  use _ <- result.try(case workflow.patched(ctx, patch_id) {
    True -> step(ctx, audit(), order_id)
    False -> Ok(Nil)
  })
```

A time-skipping test runs on a server whose clock jumps over timers: create it with
`testing.start_time_skipping()`, start the worker with `testing.client(env)`, and call
`testing.stop(env)` at the end. From
[`testing.gleam`](../../examples/src/examples/testing.gleam), a week-long wait that takes
no time:

```gleam
    use handle <- result.try(client.start(
      client,
      remind(),
      "renew passport",
      options,
    ))
    // `testing.result` lets the clock skip while it waits; `client.result` would not.
    use reminder <- result.try(testing.result(env, handle))
```

The same file tests an activity without a server, with `testing.activity_env`. To catch
non-determinism early, run test workers with `worker.with_max_cached_workflows(0)`, which
replays every workflow task.

## Common mistakes and what they produce

| Message or symptom | Cause and fix |
|---|---|
| Workflow task failure `workflow type "x" is not registered on this worker, which has a, b. Add its handler to worker.with_workflows.`; the workflow hangs | The worker on that task queue has no handler for the type. Register it; the task is retried, so the workflow then continues |
| Activity failure `activity type "x" is not registered on this worker, which has ... Add its handler to worker.with_activities.` (type `NotFound`) | Same for activities. Also check `workflow.with_task_queue` and outbound middleware renames |
| `workflow "x": could not decode the input: field "id": expected String, found Int` (type `InvalidArgument`, not retried) | The caller's codec and the definition's disagree, or another SDK sends different JSON. Same message shape for `activity "x"` |
| `activity "x": could not decode the result: ...` (type `InvalidResult`) | The activity's output codec does not match what the worker returned |
| `could not decode the input: argument 2: expected Int, found Nil`, or `... the result: expected String, found Nil` | The other side sent nothing: a TypeScript `undefined`, or an argument left out. Make that value `schema.optional(..)`, or the result `codec.nil()` |
| Activity failure `the activity process exited before reporting a result: killed` (type `ActivityProcessExited`, retried) | The activity's process was killed, or died with a process it had linked to (`process.spawn`, a port, a pool). Use `process.spawn_unlinked` and monitors, or trap exits |
| Log warning `temporal: killed an activity that was still running after its cancellation grace` | An activity ignored a cancellation, a timeout or the worker's shutdown for the whole `worker.with_activity_cancellation_grace`. Heartbeat and check `activity.is_cancelled(ctx)` in long activities |
| Every deploy makes activities time out and run again | The worker is kept alive with `process.sleep_forever()`, so SIGTERM stops the VM at once. Use `worker.run`, or `worker.await_shutdown_signal()` then `worker.stop` |
| `workflow.log_info` lines do not show | Erlang's `logger` starts at level `notice`. Lower it: `logger:set_primary_config(level, info)`, or the `logging` package's `configure`. Warnings and errors show as they are; SDK lines carry the metadata `temporal => true` |
| `client.DecodeFailed(..)` from `client.result` or `client.query` | The handle's definition has the wrong output codec, or the client lacks the payload codec the worker uses |
| Crash `temporal_not_in_workflow`: `This function works only in workflow code, in the process the worker runs the workflow in. Activities, tests and processes a workflow spawns cannot call it.` | A `workflow.*` function called from an activity, a test or a spawned process |
| `replayer.Nondeterminism(..)`, or workflow tasks failing with a nondeterminism error after a deploy | The code issues different commands than the history records. Guard the change with `workflow.patched` |
| `client.UpdateFailed` with `no handler registered for update "x"` | The workflow had not called `update.set_validator`, `update.next` or `update.receive` for it by the time it next waited |
| Log warning `signal "x" dropped: could not decode it: ...` | The sender's value does not match the signal definition's codec |
| `client.AlreadyExists(..)` on start | A workflow with that id is running. Use `client.with_id_conflict_policy(client.UseExisting)` or another id |
| Crash `temporal_library_not_loaded`: ``The Temporal native library could not be loaded. Run `gleam run -m temporal/install` to download it, or compile it with bin/build-nif in the package directory.`` | The native library is missing, typically after `gleam clean` or in CI |
| `Error(telemetry.AlreadyStarted)` | `telemetry.serve_prometheus` or `telemetry.set_core_log_level` was called after the first `client.connect`. Call them first in `main`, the log level before the metrics |
| `client.ConnectionFailed("could not connect to http://localhost:7233 within 10s: timed out")` | The service did not answer in time: a wrong address, a firewall, or TLS on one side only. `client.with_connect_timeout` changes the wait |
| Workflow task failure `the workflow task's completion was rejected: ...`, logged as `temporal: workflow task completion rejected, failing the task: ...` | The workflow asked for something that cannot be sent, such as a negative timeout. Fix the value; the task is retried |
| Log error `temporal worker: activity polling on task queue "x" failed, registering the worker again: ...` | The service answered polls with an error for over a minute, usually a proxy or load balancer in front of it. The worker recovers by itself once the service does |
| Tasks wait in the queue while the worker has free slots | Too few polls for the round trip to the service. Raise `worker.with_max_concurrent_workflow_task_polls` and `worker.with_max_concurrent_activity_task_polls` from their default of 10 |
| A workflow never progresses and nothing fails | No worker polls its task queue, or the worker crashed in workflow code and is retrying the task: check the worker's log |
