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
import gleam/time/duration
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
  let options = workflow.activity_options(start_to_close: duration.seconds(10))
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
So is passing a bare number where a length of time is wanted: every timeout and delay is a
`gleam/time/duration.Duration`, and every point in time a `timestamp.Timestamp`.

## What works

| Area | Supported |
|---|---|
| Workflows | Activities, local activities, durable timers, signals, queries, updates, child workflows, continue-as-new, patching and `deprecate_patch`, memo and search attribute changes, deterministic `now` / `random_int` / `uuid4`, logging that is silent on replay |
| Cancellation | Scopes carried by the `Context`: `with_scope`, `with_timeout`, `non_cancellable`, `cancel`; cancelling a scope cancels its timers, activities, local activities, child workflows, Nexus operations and pending signals |
| Concurrency | `Future` values with `map`, `race`, `await`, `await_all`, `await_timeout`, `await_cancellable` |
| Activities | Typed handlers, retry policies, all four timeouts, heartbeats with details, cancellation delivered to the activity, activity info, failure details |
| Child workflows | Start and await separately, signal, parent close policy, id reuse policy, timeouts, retries, memo, cancellation type |
| Other workflows | Signal and request cancellation of any workflow by id from workflow code |
| Nexus | Call operations from workflows; serve operations from a worker, answered directly or backed by a workflow |
| Client | Start with options (memo, search attributes, id reuse and conflict policies, timeouts, retries, start delay, cron), signal-with-start, execute, result, signal, query, update, cancel, terminate, describe, list, count, fetch history; typed errors |
| Schedules | Create, describe, update, pause, unpause, trigger, delete, list; intervals with offsets, calendars, cron expressions, time zones, overlap policies |
| Workers | Concurrency limits, activity rate limits, workflow-only and activity-only workers, workflow and activity middleware, outbound rewriting, worker deployment versioning with pinned and auto-upgrading workflows |
| Data | JSON payloads (`json/plain`), compatible with the Temporal CLI and other SDKs; payload codecs, with an AES-256-GCM encryption codec included |
| Connection | Plaintext, TLS, mutual TLS, API keys |
| Testing | `replayer.replay` for recorded histories; `temporal/testing` for a time-skipping server, a local dev server, and activity contexts that need no server |

## Not built

- Headers, so middleware cannot pass context from a client to a workflow or activity
- Update-with-start, and updates sent to a workflow before its first task has run are
  only accepted if the workflow asks for them before it first waits
- Asynchronous activity completion (finishing an activity from another process by token)
- Nexus operation links in the UI, and Nexus handlers other than "answer directly" and
  "run a workflow"
- Metrics and tracing export
- Schedule backfill, and memo or search attributes on the schedule itself (they can be set
  on the workflows it starts)
- Client calls to manage worker deployments: use the Temporal CLI or UI
- Enforcement of determinism: nothing stops workflow code from calling a clock or spawning a
  process. Replay tests are how such mistakes are found.
- Packaging: the NIF is built from source with `bin/build-nif`; there are no precompiled
  binaries, and only Linux has been tried.

## Passed through, not tested

These are handed to the server or to core unchanged, and no test checks their effect:

- API keys, and any connection to Temporal Cloud. TLS and mutual TLS are tested locally.
- Start options: retry policy, start delay, cron schedule, execution timeout,
  `TerminateExisting`
- Child options: retry policy, memo, timeouts other than the run timeout, every
  cancellation type but the default
- Activity options: schedule-to-start and schedule-to-close timeouts, `AbandonActivity`
- Nexus options: the schedule-to-close timeout, every cancellation type but the default,
  and cancelling a running operation end to end
- Search attribute changes from inside a workflow
- Schedules: jitter, catch-up window, pause on failure, every overlap policy's effect
- Worker limits: they are accepted, but no test measures that they limit anything
- Versioning: ramping versions and unversioned workers

## Running in production

A worker is a small tree of processes. `worker.start` links it to the caller;
`worker.supervised` gives a child specification for a `gleam/otp` supervisor:

```gleam
static_supervisor.new(static_supervisor.OneForOne)
|> static_supervisor.add(worker.supervised(config))
|> static_supervisor.start
```

What it does when things go wrong, without configuration:

| Fault | What happens |
|---|---|
| A polling loop crashes | The worker restarts it. The task it was waiting for is handed back to the server at once, not lost. |
| Loops keep crashing (more than five times a minute) | The worker gives up and exits abnormally, so its supervisor or caller sees it. |
| A workflow run's process dies | The task fails, core drops the run, and the server replays it from history. |
| Workflow code crashes | The workflow task fails and is retried until fixed code is deployed. `workflow.fail_on_crash` makes it fail the workflow instead. |
| An activity crashes or exits | It is reported as failed and its retry policy applies. |
| A cancelled activity will not stop | It is killed thirty seconds after being asked, and reported as cancelled. Change with `worker.with_activity_cancellation_grace`. |
| The worker's owner dies without stopping it | The worker is shut down, so a replacement can take over its task queue. |
| A workflow ends without answering an update | The update fails with `WorkflowFinished`; the client is not left waiting. |
| The worker process is killed outright | The server times the tasks out and gives them to another worker. |

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
    workflow.with_timeout(ctx, duration.minutes(5), fn(scope) {
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
case workflow.await_timeout(ctx, workflow.signal(ctx, approved()), duration.hours(24)) {
  Ok(approval) -> ...
  Error(_) -> ...  // timed out, or the workflow was cancelled
}
```

## Updates

An update is a request a client sends to a running workflow and waits on: it may change
the workflow, like a signal, and it returns a value, like a query. The workflow takes
updates when it is ready for them, the way it takes signals, and answers each one:

```gleam
fn cart_workflow(ctx: workflow.Context, items: List(Item)) -> Result(Int, Failure) {
  // Decide what to accept. A refused update leaves no trace in history.
  workflow.set_update_validator(ctx, add_item(), fn(item) {
    case item.quantity > 0 {
      True -> Ok(Nil)
      False -> Error("quantity must be positive")
    }
  })
  let update = workflow.receive_update(ctx, add_item())
  let items = [workflow.update_argument(update), ..items]
  // Work done before answering, such as an activity, happens while the client waits.
  workflow.complete_update(ctx, update, Ok(list.length(items)))
  cart_workflow(ctx, items)
}

let assert Ok(count) = client.execute_update(handle, add_item(), item)
```

This is the same shape as a BEAM `call`: one process, one request at a time, state in the
function's arguments. It differs from the other SDKs, where update handlers run alongside
the workflow's main function. Histories are compatible either way.

## Local activities

`workflow.execute_local_activity` runs an activity inside the workflow's own worker,
without a round trip through the server. Use it for work that takes a few seconds at most.
A local activity is recorded only when it finishes, so if its worker dies the activity
runs again from the start on another worker: it must be safe to repeat.

## Nexus

Nexus lets a workflow call an operation another team exposes, by name, through an endpoint
registered on the server. Both sides share a `workflow.NexusOperation` value:

```gleam
pub fn place_order() {
  workflow.NexusOperation("orders", "place", order_codec(), receipt_codec())
}

// Caller, in any namespace allowed to use the endpoint:
workflow.execute_nexus_operation(ctx, "orders-endpoint", place_order(), order, workflow.nexus_options())

// Handler: the operation runs a workflow and finishes when it does.
worker.new(client, "orders")
|> worker.with_workflows([workflow.handler(order_workflow(), run_order)])
|> worker.with_nexus_handlers([
  nexus.workflow_run(place_order(), order_workflow(), fn(ctx, order) {
    #(order, client.start_options(workflow_id: "order-" <> ctx.request_id, task_queue: "orders"))
  }),
])
```

`nexus.sync` answers an operation directly from a function, for short work.

## Encrypting payloads

A payload codec transforms everything the client and its workers send to the service, so
that the service stores ciphertext:

```gleam
let codec = encryption.codec(encryption.Key(id: "2026-01", secret: key), [])
client.connect(client.Options(..client.local(), codec: Some(codec)))
```

The included codec is AES-256-GCM in the format of the encryption codec in Temporal's own
samples, so a TypeScript, Go or Python worker with the same key reads the same data. Pass
retired keys as the second argument for as long as histories written with them may be
read. Search attributes are never encrypted. With a codec on, the Temporal UI and CLI show
ciphertext, and `replayer.replay_with` needs the codec too. Write your own
`codec.PayloadCodec` for compression or a key service.

## Worker versioning

```gleam
worker.new(client, "main")
|> worker.with_workflows([
  workflow.handler(checkout(), run_checkout) |> workflow.with_versioning(workflow.Pinned),
])
|> worker.with_deployment(worker.Deployment(
  name: "checkout",
  build_id: "2026-01-15.3",
  default_behavior: workflow.AutoUpgrade,
))
```

A pinned workflow finishes on the build it started on, so incompatible changes can ship
without `patched`. An auto-upgrading one moves to the current build at its next task. Which
build is current is set outside the SDK, with `temporal worker deployment
set-current-version` or the UI; a versioned worker receives nothing until then.

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
test downloads Temporal's test server unless `TEMPORAL_TEST_SERVER` names one. A dev server
you start yourself needs Nexus and worker deployments enabled, as in `bin/test`.

```sh
bin/build-nif   # compile the Rust NIF into priv/
bin/test        # build, start a dev server if none is running, run every test
bin/conformance # bin/test, then replay this SDK's histories on the TypeScript SDK
```

## How correctness is checked

| Layer | What it shows | Where |
|---|---|---|
| Boundary layout | Every job, command, activity result and Nexus result crosses the Gleam–Rust boundary and arrives with each field in place; nothing else checks that hand-matched layout | `test/bridge_test.gleam` |
| Runtime unit tests | For hand-written activations, the runtime emits exactly the expected commands, in order. No server or NIF involved | `test/runtime_test.gleam` |
| End-to-end scenarios | Each feature works against a real Temporal server | `test/integration_test.gleam`, `test/features_test.gleam`, `test/advanced_test.gleam`, `test/schedule_test.gleam` |
| Replay on every task | Every workflow scenario also runs with the workflow cache disabled, so each workflow task replays the run from the start; core fails the task on any divergence | same files |
| History replay | Each finished run's history is replayed through `replayer`; reordered code is rejected as non-deterministic | same files |
| Randomized programs | Random programs of timers, activities, fan-outs, races, deadlines, scope cancellations, random draws and child workflows must match a pure model and replay cleanly | `test/property_test.gleam` |
| Cross-SDK conformance | Every feature is a workflow written in Gleam and in TypeScript; histories replay in both directions | `test/conformance_test.gleam`, `bin/conformance` |
| Wire format | The Temporal CLI can start a Gleam workflow and read its result | `test/integration_test.gleam` |
| Worker loss | A worker killed in the middle of a local activity is replaced and the workflow completes | `test/advanced_test.gleam` |
| Faults inside a worker | Killed polling loops are restarted without losing a task, killed workflow runs are replayed, an activity that ignores cancellation is killed after its grace period, and a supervisor restarts a worker that dies | `test/resilience_test.gleam` |
| Time skipping | A week-long sleep finishes at once on the test server | `test/testing_test.gleam` |
| TLS | Workflows run through a proxy that requires mutual TLS; untrusted and certificate-less connections are refused | `test/tls_test.gleam` |

### The conformance harness

`bin/conformance` runs everything and prints a table. For each feature it shows that:

1. the history the official TypeScript SDK recorded replays through the Gleam twin;
2. a Gleam twin that diverges is rejected against that history;
3. the Gleam twin runs on a real server in both cache modes and replays its own history;
4. the history the Gleam twin recorded replays through the TypeScript twin, on the
   TypeScript SDK.

The features are: basics (timers, activities, signals, children), cancellation scopes,
waiting with a deadline, updates, signalling and cancelling other workflows, local
activities, patching with memo changes and continue-as-new, Nexus calls, and encrypted
payloads. Steps 1 to 3 run as part of `bin/test`; step 4 needs Node.js.

Adding a feature means adding its twin to both `test/conformance_test.gleam` and
`test/conformance/typescript/workflows.js`, a driver to `harness.js`, and re-recording with
`npm run record`.

Not yet done: Temporal's cross-SDK [features](https://github.com/temporalio/features) suite,
load and chaos testing with [omes](https://github.com/temporalio/omes), and fault injection
on the NIF boundary. The conformance harness checks replay compatibility, which is command
order and identity; it does not compare payload contents or timing between the SDKs.

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
- **Updates are pulled, not pushed.** A workflow is one process running one function, so
  an update cannot run beside it. The runtime accepts or rejects each update as it
  arrives and queues the accepted ones for the workflow to take.
- **A payload codec runs at the edge.** The pollers and the client apply it as payloads
  enter and leave, so workflow code and the runtime only ever see plain payloads.
- **A worker is a manager process and its polling loops.** The manager owns the
  connection to core, restarts loops, and stops everything when it or its owner dies.
  Workflow runs watch the loop that feeds them and exit with it.
- **A future resolves once.** Asking it again returns the same value, which matters for
  the ones that take something when they resolve, such as the next signal.
- **A crash in workflow code fails the workflow task, not the workflow.** The server retries
  the task, so deploying a fix lets the workflow continue.
