//// Testing: the three kinds of test, cheapest first.
////
//// 1. An activity is a function. Call it with a context from `testing.activity_env`; no
////    server is involved.
//// 2. A workflow needs a server. `testing.start_time_skipping` starts one whose clock
////    jumps ahead while workflows only wait, so a week-long timeout takes no time.
//// 3. A recorded history replays against the current code with `replayer.replay`, which
////    is how to check that a change is compatible with runs already in flight.
////
//// The functions here are what the bodies of such tests look like; `examples/test` calls
//// them and asserts on what they return.

import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/replayer
import temporal/testing
import temporal/worker
import temporal/workflow
import temporal/workflow/signal

const task_queue = "examples-testing"

pub fn remind() {
  workflow.Definition("remind", codec.string(), codec.string())
}

pub fn acknowledge() {
  workflow.SignalDefinition("acknowledge", codec.nil())
}

/// Counts down from its input, heartbeating the number left, and returns how many steps
/// it took.
pub fn count_down() {
  activity.Definition("count_down", codec.int(), codec.int())
}

fn remind_workflow(
  ctx: workflow.Context,
  task: String,
) -> Result(String, Failure) {
  let week = duration.hours(7 * 24)
  case workflow.await_timeout(ctx, signal.next(ctx, acknowledge()), week) {
    Ok(Nil) -> Ok(task <> ": acknowledged")
    Error(_) -> Ok(task <> ": escalated after a week")
  }
}

pub fn count_down_activity(
  ctx: activity.Context,
  from: Int,
) -> Result(Int, Failure) {
  count(ctx, from, 0)
}

fn count(ctx: activity.Context, left: Int, steps: Int) -> Result(Int, Failure) {
  case activity.is_cancelled(ctx), left {
    True, _ -> Error(failure.cancelled())
    False, 0 -> Ok(steps)
    False, _ -> {
      activity.heartbeat_with(ctx, codec.int(), left - 1)
      count(ctx, left - 1, steps + 1)
    }
  }
}

// ---- 1. an activity, without a server ----

/// What the activity returned and the heartbeat details it recorded.
pub fn activity_alone() -> #(Result(Int, Failure), List(Int)) {
  // `testing.activity_info()` is the info of a first attempt; change fields with a record
  // update to test a retry or a resume from heartbeat details.
  let env = testing.activity_env(testing.activity_info())
  let outcome = count_down_activity(testing.activity_context(env), 3)
  #(outcome, testing.heartbeats(env, codec.int()))
}

/// What the activity returns once it has been asked to stop.
pub fn activity_cancelled() -> Result(Int, Failure) {
  let env = testing.activity_env(testing.activity_info())
  testing.cancel_activity(env, activity.CancelRequested)
  count_down_activity(testing.activity_context(env), 3)
}

// ---- 2. a workflow, on a server that skips time ----

/// Create `env` with `testing.start_time_skipping()` and stop it with `testing.stop`.
/// Returns the workflow's result and its history.
pub fn workflow_with_time_skipping(
  env: testing.Env,
) -> Result(#(String, client.History), client.Error) {
  let client = testing.client(env)
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(remind(), remind_workflow)])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-testing", task_queue:)
  let outcome = {
    use handle <- result.try(client.start(
      client,
      remind(),
      "renew passport",
      options,
    ))
    // `testing.result` lets the clock skip while it waits; `client.result` would not.
    use reminder <- result.try(testing.result(env, handle))
    use history <- result.try(client.history(handle))
    Ok(#(reminder, history))
  }
  worker.stop(worker)
  outcome
}

// ---- 3. a recorded history, replayed ----

/// `Ok(Nil)` when the current workflow code issues the commands the history records.
pub fn replay_history(
  history: client.History,
) -> Result(Nil, replayer.ReplayError) {
  replayer.replay([workflow.handler(remind(), remind_workflow)], history)
}
