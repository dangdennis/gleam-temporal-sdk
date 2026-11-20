//// End-to-end tests against a real Temporal service on localhost:7233 (see `bin/test`).
////
//// Every scenario runs twice: with workflow caching, and with the cache disabled so that each
//// workflow task replays the run from the start of its history. The second mode is the
//// strongest determinism check available: any divergence between a first execution and its
//// replay makes core fail the task and the scenario time out. Each finished run's history is
//// then replayed once more through `replayer`.

import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import support.{type Env, cmd, start_options, unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/replayer
import temporal/retry
import temporal/worker
import temporal/workflow.{type Context}
import temporal/workflow/child
import temporal/workflow/query
import temporal/workflow/signal

// ---- activities ----

fn say_hello() {
  activity.Definition("say_hello", codec.string(), codec.string())
}

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

/// Fails until the attempt number reaches its input.
fn flaky() {
  activity.Definition("flaky", codec.int(), codec.int())
}

fn activities() -> List(activity.Handler) {
  [
    activity.handler(say_hello(), fn(_, name) { Ok("Hello, " <> name <> "!") }),
    activity.handler(double(), fn(_, n) { Ok(n * 2) }),
    activity.handler(flaky(), fn(ctx, succeed_on) {
      case activity.info(ctx).attempt >= succeed_on {
        True -> Ok(activity.info(ctx).attempt)
        False -> Error(failure.new("not yet") |> failure.with_type("Flaky"))
      }
    }),
  ]
}

fn options() -> workflow.ActivityOptions {
  workflow.activity_options(start_to_close: duration.milliseconds(10_000))
}

fn quick_retry(maximum_attempts: Int) -> workflow.ActivityOptions {
  options()
  |> workflow.with_retry(
    retry.policy(maximum_attempts:)
    |> retry.with_initial_interval(duration.milliseconds(10))
    |> retry.with_backoff_coefficient(1.0),
  )
}

// ---- workflows ----

fn greet() {
  workflow.Definition("greet", codec.string(), codec.string())
}

fn greet_workflow(ctx: Context, name: String) -> Result(String, Failure) {
  workflow.execute_activity(ctx, say_hello(), name, options())
}

fn timers() {
  workflow.Definition("timers", codec.int(), codec.int())
}

/// Sleeps and returns how much workflow time passed.
fn timers_workflow(ctx: Context, ms: Int) -> Result(Int, Failure) {
  let before = workflow.now(ctx)
  let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(ms))
  let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(ms))
  Ok(
    timestamp.difference(before, workflow.now(ctx))
    |> duration.to_milliseconds,
  )
}

fn fan_out() {
  workflow.Definition("fan_out", codec.int(), codec.int())
}

fn fan_out_workflow(ctx: Context, count: Int) -> Result(Int, Failure) {
  int.range(from: count, to: 0, with: [], run: list.prepend)
  |> list.map(workflow.start_activity(ctx, double(), _, options()))
  |> workflow.await_all
  |> list.try_fold(0, fn(sum, result) {
    case result {
      Ok(n) -> Ok(sum + n)
      Error(failure) -> Error(failure)
    }
  })
}

fn retrying() {
  workflow.Definition("retrying", codec.int(), codec.string())
}

/// Runs an activity that succeeds on its third attempt, allowing `attempts` attempts.
fn retrying_workflow(ctx: Context, attempts: Int) -> Result(String, Failure) {
  case workflow.execute_activity(ctx, flaky(), 3, quick_retry(attempts)) {
    Ok(attempt) -> Ok("succeeded on attempt " <> int.to_string(attempt))
    Error(failure) -> {
      let cause = failure.root_cause(failure)
      Ok(
        "gave up: "
        <> cause.message
        <> " ("
        <> option.unwrap(cause.error_type, "untyped")
        <> ")",
      )
    }
  }
}

fn failing() {
  workflow.Definition("failing", codec.nil(), codec.nil())
}

fn failing_workflow(_ctx: Context, _: Nil) -> Result(Nil, Failure) {
  Error(failure.new("out of stock") |> failure.with_type("OutOfStock"))
}

fn added() {
  workflow.SignalDefinition("added", codec.int())
}

fn total() {
  workflow.QueryDefinition("total", codec.nil(), codec.int())
}

fn accumulator() {
  workflow.Definition("accumulator", codec.int(), codec.int())
}

/// Sums signalled numbers until it is signalled a zero.
fn accumulator_workflow(ctx: Context, sum: Int) -> Result(Int, Failure) {
  query.set_handler(ctx, total(), fn(_) { sum })
  case signal.receive(ctx, added()) {
    0 -> Ok(sum)
    n -> accumulator_workflow(ctx, sum + n)
  }
}

fn cancellable() {
  workflow.Definition("cancellable", codec.nil(), codec.string())
}

fn cancellable_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  use _ <- result.map(workflow.sleep(ctx, duration.milliseconds(600_000)))
  "finished"
}

fn countdown() {
  workflow.Definition("countdown", codec.int(), codec.string())
}

fn countdown_workflow(ctx: Context, remaining: Int) -> Result(String, Failure) {
  let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10))
  case remaining {
    0 -> Ok("lift-off on attempt " <> int.to_string(workflow.info(ctx).attempt))
    _ -> workflow.continue_as_new(ctx, countdown(), remaining - 1)
  }
}

fn parent() {
  workflow.Definition("parent", codec.string(), codec.string())
}

fn parent_workflow(ctx: Context, name: String) -> Result(String, Failure) {
  let child_id = workflow.info(ctx).workflow_id <> "-child"
  let options = child.options(workflow_id: child_id)
  case child.execute(ctx, greet(), name, options) {
    Ok(greeting) -> Ok("child said: " <> greeting)
    Error(failure) -> Error(failure)
  }
}

fn randomness() {
  workflow.Definition("randomness", codec.nil(), codec.string())
}

/// Draws random values on both sides of a timer, so that a replay which restarted the
/// sequence or drew differently would schedule a different timer and be caught by core.
fn randomness_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  let first = workflow.random_int(ctx, 50)
  let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10 + first))
  let id = workflow.uuid4(ctx)
  let assert Ok(Nil) =
    workflow.sleep(
      ctx,
      duration.milliseconds(10 + workflow.random_int(ctx, 50)),
    )
  Ok(int.to_string(first) <> ":" <> id)
}

fn patching() {
  workflow.Definition("patching", codec.int(), codec.int())
}

fn patching_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10))
  case workflow.patched(ctx, "double-it") {
    True -> workflow.execute_activity(ctx, double(), n, options())
    False -> Ok(n)
  }
}

fn replay_probe() {
  workflow.Definition("replay_probe", codec.nil(), codec.bool())
}

/// Reports whether the code before its timer was, in the execution that produced the
/// result, a replay.
fn replay_probe_workflow(ctx: Context, _: Nil) -> Result(Bool, Failure) {
  let replaying = workflow.is_replaying(ctx)
  let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10))
  Ok(replaying)
}

fn crashing() {
  workflow.Definition("crashing", codec.nil(), codec.nil())
}

fn crashing_workflow(_ctx: Context, _: Nil) -> Result(Nil, Failure) {
  panic as "a bug in workflow code"
}

fn workflows() -> List(workflow.Handler) {
  [
    workflow.handler(greet(), greet_workflow),
    workflow.handler(timers(), timers_workflow),
    workflow.handler(fan_out(), fan_out_workflow),
    workflow.handler(retrying(), retrying_workflow),
    workflow.handler(failing(), failing_workflow),
    workflow.handler(accumulator(), accumulator_workflow),
    workflow.handler(cancellable(), cancellable_workflow),
    workflow.handler(countdown(), countdown_workflow),
    workflow.handler(parent(), parent_workflow),
    workflow.handler(randomness(), randomness_workflow),
    workflow.handler(patching(), patching_workflow),
    workflow.handler(crashing(), crashing_workflow),
    workflow.handler(replay_probe(), replay_probe_workflow),
  ]
}

// ---- harness ----

/// Start a workflow, wait for it, and check that its history replays cleanly.
fn run(
  env: Env,
  definition: workflow.Definition(input, output),
  input: input,
) -> Result(output, client.Error) {
  let assert Ok(handle) =
    client.start(env.client, definition, input, start_options(env))
  let result = client.result(handle)
  assert_replays(handle)
  result
}

fn assert_replays(handle: client.Handle(output)) -> Nil {
  let assert Ok(history) = client.history(handle)
  assert replayer.replay(workflows(), history) == Ok(Nil)
}

// ---- scenarios ----

pub fn activity_round_trip_test() {
  use env <- in_both_modes("greet")
  assert run(env, greet(), "Gleam") == Ok("Hello, Gleam!")
}

pub fn timers_advance_workflow_time_test() {
  use env <- in_both_modes("timers")
  let assert Ok(elapsed) = run(env, timers(), 100)
  assert elapsed >= 200
}

pub fn parallel_activities_test() {
  use env <- in_both_modes("fan-out")
  assert run(env, fan_out(), 10) == Ok(110)
}

pub fn activity_retries_until_success_test() {
  use env <- in_both_modes("retry-ok")
  assert run(env, retrying(), 5) == Ok("succeeded on attempt 3")
}

pub fn activity_retries_are_bounded_test() {
  use env <- in_both_modes("retry-exhausted")
  assert run(env, retrying(), 2) == Ok("gave up: not yet (Flaky)")
}

pub fn workflow_failure_reaches_the_client_test() {
  use env <- in_both_modes("failing")
  let assert Error(client.WorkflowFailed(failure)) = run(env, failing(), Nil)
  assert failure.message == "out of stock"
  assert failure.error_type == Some("OutOfStock")
}

pub fn signals_and_queries_test() {
  use env <- in_both_modes("accumulator")
  let assert Ok(handle) =
    client.start(env.client, accumulator(), 100, start_options(env))
  assert client.query(handle, total(), Nil) == Ok(100)
  list.each([1, 2, 3], fn(n) {
    let assert Ok(Nil) = client.signal(handle, added(), n)
  })
  assert client.query(handle, total(), Nil) == Ok(106)
  let assert Ok(Nil) = client.signal(handle, added(), 0)
  assert client.result(handle) == Ok(106)
  // A closed workflow still answers queries, by replaying its history.
  assert client.query(handle, total(), Nil) == Ok(106)
  let assert Error(_) =
    client.query(
      handle,
      workflow.QueryDefinition("unknown", codec.nil(), codec.int()),
      Nil,
    )
  assert_replays(handle)
}

pub fn cancellation_test() {
  use env <- in_both_modes("cancellable")
  let assert Ok(handle) =
    client.start(env.client, cancellable(), Nil, start_options(env))
  let assert Ok(Nil) = client.cancel(handle)
  assert client.result(handle) == Error(client.WorkflowCancelled)
  assert_replays(handle)
}

pub fn continue_as_new_is_followed_to_the_final_run_test() {
  use env <- in_both_modes("countdown")
  let assert Ok(handle) =
    client.start(env.client, countdown(), 3, start_options(env))
  assert client.result(handle) == Ok("lift-off on attempt 1")
  // The handle's own run ended by continuing as new; its history must replay to that too.
  assert_replays(handle)
}

pub fn child_workflow_test() {
  use env <- in_both_modes("parent")
  assert run(env, parent(), "child") == Ok("child said: Hello, child!")
}

pub fn randomness_is_replay_safe_test() {
  use env <- in_both_modes("randomness")
  let assert Ok(first) = run(env, randomness(), Nil)
  let assert Ok(second) = run(env, randomness(), Nil)
  // Different runs get different seeds.
  assert first != second
}

pub fn patched_code_runs_on_new_executions_test() {
  use env <- in_both_modes("patching")
  assert run(env, patching(), 21) == Ok(42)
}

pub fn starting_a_running_workflow_id_again_is_rejected_test() {
  use env <- in_both_modes("duplicate")
  let options = start_options(env)
  let assert Ok(handle) = client.start(env.client, accumulator(), 0, options)
  let assert Error(client.AlreadyExists(_)) =
    client.start(env.client, accumulator(), 0, options)
  let assert Ok(Nil) = client.terminate(handle, "test over")
  assert client.result(handle) == Error(client.WorkflowTerminated)
}

pub fn a_workflow_crash_does_not_fail_the_workflow_or_the_worker_test() {
  use env <- in_both_modes("crashing")
  let assert Ok(handle) =
    client.start(env.client, crashing(), Nil, start_options(env))
  // The task fails and is retried; the workflow stays open, waiting for a fixed deploy.
  assert run(env, greet(), "still here") == Ok("Hello, still here!")
  let assert Ok(Nil) = client.terminate(handle, "test over")
  assert client.result(handle) == Error(client.WorkflowTerminated)
}

/// Payloads use the encoding the Temporal CLI and the other SDKs read and write.
pub fn payloads_interoperate_with_the_temporal_cli_test() {
  use env <- in_both_modes("cli")
  let output =
    cmd(
      "temporal workflow execute --type greet --task-queue "
      <> env.task_queue
      <> " --workflow-id "
      <> env.task_queue
      <> " --input '\"CLI\"' 2>&1",
    )
  assert string.contains(output, "Hello, CLI!")
}

/// Guards the premise of this suite: without a cache, a task really does replay the run.
pub fn disabling_the_cache_replays_every_task_test() {
  use env <- in_both_modes("replay-probe")
  let uncached = string.starts_with(env.task_queue, "replay-probe-0-")
  assert run(env, replay_probe(), Nil) == Ok(uncached)
}

// ---- replay catches incompatible code ----

fn reordered() {
  workflow.Definition("reordered", codec.int(), codec.int())
}

pub fn replay_detects_reordered_commands_test() {
  let assert Ok(client) = client.connect(client.local())
  let task_queue = "reordered-" <> unique()
  let original =
    workflow.handler(reordered(), fn(ctx, n) {
      let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10))
      workflow.execute_activity(ctx, double(), n, options())
    })
  let changed =
    workflow.handler(reordered(), fn(ctx, n) {
      let doubled = workflow.execute_activity(ctx, double(), n, options())
      let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10))
      doubled
    })
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows([original])
    |> worker.with_activities(activities())
    |> worker.start
  let assert Ok(handle) =
    client.start(
      client,
      reordered(),
      4,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
  assert client.result(handle) == Ok(8)
  worker.stop(worker)

  let assert Ok(history) = client.history(handle)
  assert replayer.replay([original], history) == Ok(Nil)
  let assert Error(replayer.Nondeterminism(_)) =
    replayer.replay([changed], history)
}

/// Run `scenario` against a fresh worker in both cache modes.
fn in_both_modes(name: String, scenario: fn(Env) -> Nil) -> Nil {
  use config <- support.in_both_modes(name, support.local_client(), _, scenario)
  config
  |> worker.with_workflows(workflows())
  |> worker.with_activities(activities())
}

// ---- what an activity is told about its task ----

fn inspect_task() {
  activity.Definition("inspect_task", codec.nil(), codec.string())
}

/// Fails on its first attempt and succeeds on its second, leaving what each was told
/// where the test can read it.
fn inspect_task_activity(
  ctx: activity.Context,
  _: Nil,
) -> Result(String, Failure) {
  let info = activity.info(ctx)
  let told =
    info.run_id
    <> " "
    <> string.inspect(
      option.map(info.retry, fn(policy) {
        #(policy.maximum_attempts, policy.non_retryable_error_types)
      }),
    )
    <> " "
    <> string.inspect(activity.is_final_attempt(ctx))
  support.record(info.workflow_id <> int.to_string(info.attempt), told)
  case info.attempt {
    1 -> Error(failure.new("first attempt"))
    _ -> Ok(told)
  }
}

fn inspecting() {
  workflow.Definition("inspecting", codec.nil(), codec.string())
}

fn inspecting_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  let twice =
    options()
    |> workflow.with_retry(
      retry.policy(maximum_attempts: 2)
      |> retry.with_initial_interval(duration.milliseconds(10))
      |> retry.with_non_retryable_error_types(["Fatal"]),
    )
  workflow.execute_activity(ctx, inspect_task(), Nil, twice)
}

pub fn an_activity_knows_its_workflows_run_and_its_retry_policy_test() {
  use env <- support.in_both_modes(
    "activity-info",
    support.local_client(),
    fn(config) {
      config
      |> worker.with_workflows([
        workflow.handler(inspecting(), inspecting_workflow),
      ])
      |> worker.with_activities([
        activity.handler(inspect_task(), inspect_task_activity),
      ])
    },
  )
  let options = start_options(env)
  let assert Ok(handle) = client.start(env.client, inspecting(), Nil, options)
  let assert Some(run_id) = client.run_id(handle)
  assert run_id != ""
  assert client.result(handle) == Ok(run_id <> " Some(#(2, [\"Fatal\"])) True")
  assert support.recorded(options.workflow_id <> "1")
    == run_id <> " Some(#(2, [\"Fatal\"])) False"
}

// ---- a completion the NIF will not take ----

@external(erlang, "temporal_test_ffi", "capture_log_metadata")
fn capture_log_metadata(body: fn() -> Nil) -> List(#(String, List(String)))

fn backwards() {
  workflow.Definition("backwards", codec.nil(), codec.nil())
}

/// Asks for an activity the NIF refuses to hand to core: no timeout is negative.
fn backwards_workflow(ctx: Context, _: Nil) -> Result(Nil, Failure) {
  let options = workflow.activity_options(start_to_close: duration.seconds(-5))
  use _ <- result.map(workflow.execute_activity(ctx, double(), 1, options))
  Nil
}

pub fn a_completion_the_nif_rejects_fails_the_workflow_task_test() {
  let client = support.local_client()
  let task_queue = "backwards-" <> unique()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(backwards(), backwards_workflow)])
    |> worker.start
  let shown = fn() {
    cmd("temporal workflow show --output json --workflow-id " <> task_queue)
  }
  let logged =
    capture_log_metadata(fn() {
      let assert Ok(handle) =
        client.start(
          client,
          backwards(),
          Nil,
          client.start_options(workflow_id: task_queue, task_queue:),
        )
      // The task fails at once, with the reason. Before, nothing answered it, and the
      // server waited ten seconds to time it out: its slot on the worker was held and
      // the history said only that a task had timed out.
      assert support.eventually(fn() {
        string.contains(shown(), "EVENT_TYPE_WORKFLOW_TASK_FAILED")
      })
      let assert Ok(Nil) = client.terminate(handle, "done")
      Nil
    })
  let history = shown()
  assert string.contains(history, "a duration must be a non-negative Int")
  assert !string.contains(history, "EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT")
  // And it is logged as an error that names the run.
  assert list.any(logged, fn(line) {
    string.contains(line.0, "workflow task completion rejected")
    && string.contains(line.0, "a duration must be a non-negative Int")
    && list.any(line.1, string.starts_with(_, "run_id=<<\""))
  })
  worker.stop(worker)
}
