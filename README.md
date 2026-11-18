# temporal_gleam

A [Temporal](https://temporal.io) SDK for Gleam on the BEAM.

It is built on [`sdk-core`](https://github.com/temporalio/sdk-core), the Rust library behind
Temporal's TypeScript, Python, .NET and Ruby SDKs. Core owns the hard part, the state machines
that turn workflow history into work and check replays for determinism. This package adds a
Rust NIF that exposes core to Erlang, and a Gleam runtime that runs workflow and activity code.

Status: experimental. Everything in the first list below is covered by tests against a real
Temporal server unless marked untested; the second list is not built yet.

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

  let options = client.StartOptions(workflow_id: "greet-1", task_queue: "greetings")
  let assert Ok("Hello, Gleam!") = client.execute(client, greet(), "Gleam", options)
  worker.stop(worker)
}
```

Passing the wrong input type to `execute_activity` or `client.execute` is a compile error.

## What works

| Area | Supported |
|---|---|
| Workflows | Activities, durable timers, signals, queries, child workflows, cancellation requests, continue-as-new, patching, deterministic `now` / `random_int` / `uuid4` |
| Concurrency | `Future` values with `map`, `race`, `await`, `await_all` |
| Activities | Typed handlers, retries with policies. Heartbeats and stopping an activity the server cancelled are implemented but untested |
| Client | Start, execute, result (following continue-as-new), signal, query, cancel, terminate, fetch history |
| Testing | `replayer.replay` checks recorded histories against current workflow code |
| Data | JSON payloads (`json/plain`), compatible with the Temporal CLI and other SDKs |

## Not built yet

- Workflow updates, local activities, Nexus, schedules, search attributes and memos
- Cancelling an individual activity, timer or child workflow from workflow code
- Signalling or cancelling external workflows from workflow code
- TLS client certificates, payload codecs (encryption), interceptors, metrics and tracing
- A time-skipping test environment
- Enforcement of determinism: nothing stops workflow code from calling a clock or spawning a
  process. Replay tests are how such mistakes are found.
- Packaging: the NIF is built from source with `bin/build-nif`; there are no precompiled
  binaries, and only Linux has been tried.

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

## Building and testing

Requirements: Gleam 1.19+, Erlang/OTP 27+, Rust 1.88+, and the
[Temporal CLI](https://docs.temporal.io/cli) for the integration tests.

```sh
bin/build-nif   # compile the Rust NIF into priv/
bin/test        # build, start a dev server if none is running, run every test
```

## How correctness is checked

| Layer | What it shows | Where |
|---|---|---|
| Runtime unit tests | For hand-written activations, the runtime emits exactly the expected commands. No server or NIF involved | `test/runtime_test.gleam` |
| End-to-end scenarios | Each feature works against a real Temporal server | `test/integration_test.gleam` |
| Replay on every task | Every scenario also runs with the workflow cache disabled, so each workflow task replays the run from the start; core fails the task on any divergence | same file |
| History replay | Each finished run's history is replayed through `replayer`; reordered code is rejected as non-deterministic | same file |
| Randomized programs | Random programs of timers, activities, fan-outs, races, random draws and child workflows must match a pure model and replay cleanly | `test/property_test.gleam` |
| Cross-SDK history | A history recorded by the official TypeScript SDK replays through the equivalent Gleam workflow | `test/golden_test.gleam` |
| Wire format | The Temporal CLI can start a Gleam workflow and read its result | `test/integration_test.gleam` |

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
- **A crash in workflow code fails the workflow task, not the workflow.** The server retries
  the task, so deploying a fix lets the workflow continue.
