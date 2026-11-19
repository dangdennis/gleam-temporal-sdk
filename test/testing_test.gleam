//// The test helpers themselves: a time-skipping server and server-less activity contexts.

import gleam/option.{Some}
import gleam/result
import support.{unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/testing
import temporal/worker
import temporal/workflow.{type Context}

@external(erlang, "temporal_test_ffi", "test_server_path")
fn test_server_path() -> Result(String, Nil)

const week = 604_800_000

fn patient() {
  workflow.Definition("patient", codec.nil(), codec.int())
}

/// Sleeps for a week and reports how much workflow time passed.
fn patient_workflow(ctx: Context, _: Nil) -> Result(Int, Failure) {
  let started = workflow.now(ctx)
  use _ <- result.map(workflow.sleep(ctx, week))
  workflow.now(ctx) - started
}

fn reminded() {
  workflow.SignalDefinition("reminded", codec.nil())
}

fn reminder() {
  workflow.Definition("reminder", codec.nil(), codec.string())
}

fn reminder_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  case workflow.await_timeout(ctx, workflow.signal(ctx, reminded()), week) {
    Ok(Nil) -> Ok("reminded in time")
    Error(_) -> Ok("gave up after a week")
  }
}

fn start_server() -> testing.Env {
  let assert Ok(env) = case test_server_path() {
    Ok(path) -> testing.start_time_skipping_with(executable: path)
    Error(Nil) -> testing.start_time_skipping()
  }
  env
}

pub fn a_week_long_sleep_finishes_at_once_test() {
  let env = start_server()
  let task_queue = "skipping-" <> unique()
  let assert Ok(worker) =
    worker.new(testing.client(env), task_queue)
    |> worker.with_workflows([
      workflow.handler(patient(), patient_workflow),
      workflow.handler(reminder(), reminder_workflow),
    ])
    |> worker.start
  let options = fn() {
    client.start_options(workflow_id: "wf-" <> unique(), task_queue:)
  }

  let before = testing.now(env)
  let assert Ok(elapsed) = testing.execute(env, patient(), Nil, options())
  assert elapsed >= week
  assert testing.now(env) - before >= week

  // Time stands still between calls, so a signal sent now arrives before the deadline.
  let assert Ok(handle) =
    client.start(testing.client(env), reminder(), Nil, options())
  let assert Ok(Nil) = client.signal(handle, reminded(), Nil)
  assert testing.result(env, handle) == Ok("reminded in time")

  // `sleep` moves the clock past the deadline without anyone waiting for a result.
  let assert Ok(handle) =
    client.start(testing.client(env), reminder(), Nil, options())
  testing.sleep(env, week + 1000)
  assert testing.result(env, handle) == Ok("gave up after a week")

  worker.stop(worker)
  testing.stop(env)
}

fn counting(ctx: activity.Context, upto: Int) -> Result(Int, Failure) {
  let from = activity.heartbeat_details(ctx, codec.int()) |> option_or(0)
  count(ctx, from, upto)
}

fn count(ctx: activity.Context, n: Int, upto: Int) -> Result(Int, Failure) {
  case activity.is_cancelled(ctx), n >= upto {
    True, _ -> Error(failure.cancelled())
    False, True -> Ok(n)
    False, False -> {
      activity.heartbeat_with(ctx, codec.int(), n + 1)
      count(ctx, n + 1, upto)
    }
  }
}

fn option_or(value, default) {
  case value {
    Some(value) -> value
    _ -> default
  }
}

pub fn an_activity_can_be_tested_without_a_server_test() {
  let env = testing.activity_env(testing.activity_info())
  assert counting(testing.activity_context(env), 3) == Ok(3)
  assert testing.heartbeats(env, codec.int()) == [1, 2, 3]

  // A retry resumes from the progress the last attempt recorded.
  let info =
    activity.Info(..testing.activity_info(), attempt: 2, heartbeat_details: [
      codec.int().encode(2),
    ])
  let env = testing.activity_env(info)
  assert counting(testing.activity_context(env), 3) == Ok(3)
  assert testing.heartbeats(env, codec.int()) == [3]

  let env = testing.activity_env(testing.activity_info())
  testing.cancel_activity(env, activity.CancelRequested)
  let assert Error(failure.Failure(kind: failure.Cancelled, ..)) =
    counting(testing.activity_context(env), 3)
  assert testing.heartbeat_count(env) == 0
}
