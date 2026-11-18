# temporal_gleam

A [Temporal](https://temporal.io) SDK for Gleam on the BEAM.

It is built on [`sdk-core`](https://github.com/temporalio/sdk-core), the Rust library behind
Temporal's TypeScript, Python, .NET and Ruby SDKs. Core owns the hard part, the state machines
that turn workflow history into work and check replays for determinism. This package adds a
Rust NIF that exposes core to Erlang, and a Gleam runtime that runs workflow and activity code.

Status: experimental. The features under "What works" are tested against a real Temporal
server, apart from the options listed under "Passed through, not tested". It has not run in
production and has had no load testing.

## Example

```gleam
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow

// Definitions name a workflow or activity and fix the types it sends and receives.
// The code that implements one and the code that calls it share the definition.
pub fn say_hello() {
  activity.Definition("say_hello", codec.string(), codec.string())
}

pub fn greet() {
  workflow.Definition("greet", codec.string(), codec.string())
}

fn greet_workflow(ctx: workflow.Context, name: String) -> Result(String, Failure) {
  let options = workflow.activity_options(start_to_close_ms: 10_000)
  workflow.execute_activity(ctx, say_hello(), name, options)
}

pub fn main() {
  let assert Ok(client) = client.connect(client.local())
  let assert Ok(worker) =
    worker.new(client, "greetings")
    |> worker.with_workflows([workflow.handler(greet(), greet_workflow)])
    |> worker.with_activities([
      activity.handler(say_hello(), fn(_ctx, name) { Ok("Hello, " <> name <> "!") }),
    ])
    |> worker.start

  let options = client.start_options(workflow_id: "greet-1", task_queue: "greetings")
  let assert Ok("Hello, Gleam!") = client.execute(client, greet(), "Gleam", options)
  worker.stop(worker)
}
```

Passing the wrong input type to `execute_activity` or `client.execute` is a compile error.

## What works

| Area | Supported |
|---|---|
| Workflows | Activities, durable timers, signals, queries, child workflows, continue-as-new, patching and `deprecate_patch`, memo, deterministic `now` / `random_int` / `uuid4`, logging that is silent on replay |
| Cancellation | Scopes carried by the `Context`: `with_scope`, `with_timeout`, `non_cancellable`, `cancel`; cancelling a scope cancels its timers, activities and child workflows |
| Concurrency | `Future` values with `map`, `race`, `await`, `await_all`, `await_timeout`, `await_cancellable` |
| Activities | Typed handlers, retry policies, all four timeouts, heartbeats with details, cancellation delivered to the activity, activity info, failure details |
| Child workflows | Start and await separately, parent close policy, id reuse policy, timeouts, retries, memo, cancellation type |
| Client | Start with options (memo, search attributes, id reuse and conflict policies, timeouts, retries, start delay, cron), signal-with-start, execute, result, signal, query, cancel, terminate, describe, list, count, fetch history; typed errors |
| Schedules | Create, describe, update, pause, unpause, trigger, delete, list; intervals with offsets, calendars, cron expressions, time zones, overlap policies |
| Workers | Concurrency limits, activity rate limits, workflow-only and activity-only workers, workflow and activity middleware, outbound rewriting of scheduled activities and child workflows |
| Connection | Plaintext, TLS, mutual TLS, API keys |
| Testing | `replayer.replay` for recorded histories; `temporal/testing` for a time-skipping server, a local dev server, and activity contexts that need no server |
| Data | JSON payloads (`json/plain`), compatible with the Temporal CLI and other SDKs |

## Not built

- Workflow updates, local activities, Nexus, worker versioning
- Signalling or cancelling external workflows from workflow code, and signalling a child
- Changing search attributes or memo from inside a workflow
- Headers, so middleware cannot pass context from a client to a workflow or activity
- Payload codecs (encryption, compression) and custom data converters
- Metrics and tracing export
- Schedule backfill, and memo or search attributes on the schedule itself (they can be set
  on the workflows it starts)
- Enforcement of determinism: nothing stops workflow code from calling a clock or spawning a
  process. Replay tests are how such mistakes are found.
- Packaging: the NIF is built from source with `bin/build-nif`; there are no precompiled
  binaries, and only Linux has been tried.

## Passed through, not tested

These are handed to the server or to core unchanged, and no test checks their effect:

- API keys, and any connection to Temporal Cloud. TLS and mutual TLS are tested locally.
- Start options: retry policy, start delay, cron schedule, execution and task timeouts,
  `TerminateExisting`
- Child options: retry policy, memo, timeouts other than the run timeout, every
  cancellation type but the default
- Activity options: schedule-to-start and schedule-to-close timeouts, `AbandonActivity`
- Schedules: jitter, catch-up window, pause on failure, every overlap policy's effect
- Worker limits: they are accepted, but no test measures that they limit anything

## Writing workflows

Workflow code runs more than once: Temporal replays it against recorded history to rebuild
its state. It must therefore issue the same commands in the same order every time. Take time,
randomness and anything else that varies from the `temporal/workflow` module, and do all IO in
activities.

Workflow state lives in ordinary local variables. A query handler is a closure over them, so
register it again whenever the answer should change:

```gleam
fn accumulator_workflow(ctx: workflow.Context, sum: Int) -> Result(Int, Failure) {
  workflow.set_query_handler(ctx, total(), fn(_) { sum })
  case workflow.receive_signal(ctx, added()) {
    0 -> Ok(sum)
    n -> accumulator_workflow(ctx, sum + n)
  }
}
```

## Cancellation

Cancellation travels with the `Context`, as in Go. Whatever a workflow starts with a
context belongs to that context's scope and fails with a cancellation when the scope is
cancelled. A request to cancel the workflow cancels the context the workflow function was
given, so `sleep`, `execute_activity` and `execute_child` all return a `Result`:

```gleam
fn order_workflow(ctx: workflow.Context, order: Order) -> Result(Receipt, Failure) {
  // Give the payment five minutes; the activity is cancelled if it overruns.
  let paid =
    workflow.with_timeout(ctx, 300_000, fn(scope) {
      workflow.execute_activity(scope, charge(), order, options())
    })
  case paid {
    Ok(receipt) -> Ok(receipt)
    Error(failure) -> {
      // Runs even if the workflow itself was cancelled.
      let _ =
        workflow.non_cancellable(ctx, fn(shielded) {
          workflow.execute_activity(shielded, release_stock(), order, options())
        })
      Error(failure)
    }
  }
}
```

A workflow that returns the cancellation it was given ends as cancelled. `failure.is_cancellation`
and `failure.is_timeout` tell the two apart.

An activity learns it was cancelled when it heartbeats. It checks `activity.is_cancelled`
and returns `Error(failure.cancelled())` to accept; it is never killed.

To wait for a signal with a deadline, the equivalent of TypeScript's `condition(fn, timeout)`:

```gleam
case workflow.await_timeout(ctx, workflow.signal(ctx, approved()), 86_400_000) {
  Ok(approval) -> ...
  Error(_) -> ...  // timed out, or the workflow was cancelled
}
```

## Middleware

Middleware is a function. Inbound middleware wraps every workflow or activity on a worker;
outbound middleware rewrites what a workflow schedules.

```gleam
worker.new(client, "main")
|> worker.with_activity_middleware(fn(ctx, next) {
  case next() {
    Error(failure) -> {
      report(ctx.info.activity_type, failure)
      Error(failure)
    }
    ok -> ok
  }
})
|> worker.with_workflow_middleware(fn(ctx, next) {
  case next() {
    Error(failure) -> {
      // Workflow middleware is workflow code: side effects go through unless_replaying.
      workflow.unless_replaying(ctx, fn() { report_workflow(failure) })
      Error(failure)
    }
    ok -> ok
  }
})
|> worker.with_outbound_activity(fn(call) {
  middleware.ActivityCall(..call, activity_type: rename(call.activity_type))
})
```

When replaying a history, pass the same outbound middleware to `replayer.replay_with`.

## Testing workflows

```gleam
let assert Ok(env) = testing.start_time_skipping()
let assert Ok(worker) =
  worker.new(testing.client(env), "tests")
  |> worker.with_workflows([workflow.handler(reminder(), reminder_workflow)])
  |> worker.start
// A workflow that waits a week finishes at once.
let result = testing.execute(env, reminder(), Nil, options)
worker.stop(worker)
testing.stop(env)
```

The test server is downloaded on first use; `testing.start_time_skipping_with` uses one
already on disk. `testing.activity_env` builds a context for calling an activity
function directly, records its heartbeats, and can deliver a cancellation.

## Building and testing

Requirements: Gleam 1.19+, Erlang/OTP 27+, Rust 1.88+. The tests also need the
[Temporal CLI](https://docs.temporal.io/cli) and `openssl` on the `PATH`; the time-skipping
test downloads Temporal's test server unless `TEMPORAL_TEST_SERVER` names one.

```sh
bin/build-nif   # compile the Rust NIF into priv/
bin/test        # build, start a dev server if none is running, run every test
```

## How correctness is checked

| Layer | What it shows | Where |
|---|---|---|
| Runtime unit tests | For hand-written activations, the runtime emits exactly the expected commands, including the order of cancellations. No server or NIF involved | `test/runtime_test.gleam` |
| End-to-end scenarios | Each feature works against a real Temporal server | `test/integration_test.gleam`, `test/features_test.gleam`, `test/schedule_test.gleam` |
| Replay on every task | Every workflow scenario also runs with the workflow cache disabled, so each workflow task replays the run from the start; core fails the task on any divergence | same files |
| History replay | Each finished run's history is replayed through `replayer`; reordered code is rejected as non-deterministic | same files |
| Randomized programs | Random programs of timers, activities, fan-outs, races, deadlines, scope cancellations, random draws and child workflows must match a pure model and replay cleanly | `test/property_test.gleam` |
| Cross-SDK history | Three histories recorded by the official TypeScript SDK, two of them using cancellation scopes, replay through the equivalent Gleam workflows; diverging code is rejected | `test/golden_test.gleam` |
| Wire format | The Temporal CLI can start a Gleam workflow and read its result | `test/integration_test.gleam` |
| Time skipping | A week-long sleep finishes at once on the test server | `test/testing_test.gleam` |
| TLS | Workflows run through a proxy that requires mutual TLS; untrusted and certificate-less connections are refused | `test/tls_test.gleam` |

Not yet done: Temporal's cross-SDK [features](https://github.com/temporalio/features) suite,
load and chaos testing with [omes](https://github.com/temporalio/omes), and fault injection
on the NIF boundary.

## Design

- **One process per workflow run.** The process calls the workflow function directly. When
  the code awaits something that has not happened, the process answers the current activation
  with the commands gathered so far and blocks in `receive` for the next one. Evicting a run
  from the cache kills the process; the next task replays it in a new one.
- **Types cross the NIF as Erlang terms.** The Rust side builds and reads the Gleam records in
  `temporal/internal/bridge` directly, so there is no protobuf library on the BEAM side.
  `bridge.gleam` and `native/temporal_nif/src/convert.rs` must change together.
- **Cancellation is a tree of scopes kept in the runtime.** A `Context` names a scope. Each
  scope lists what was started in it, nested scopes included, in order; cancelling one walks
  that list and emits the cancel commands in the order the TypeScript SDK would, which is
  what lets histories recorded there replay here.
- **A crash in workflow code fails the workflow task, not the workflow.** The server retries
  the task, so deploying a fix lets the workflow continue.
