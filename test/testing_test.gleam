//// The test helpers themselves: a time-skipping server and server-less activity contexts.

import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/order
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import support.{unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/log
import temporal/retry
import temporal/testing
import temporal/worker
import temporal/workflow.{type Context}
import temporal/workflow/signal

@external(erlang, "temporal_test_ffi", "test_server_path")
fn test_server_path() -> Result(String, Nil)

fn week() {
  duration.hours(7 * 24)
}

fn patient() {
  workflow.Definition("patient", codec.nil(), codec.int())
}

/// Sleeps for a week and reports how much workflow time passed.
fn patient_workflow(ctx: Context, _: Nil) -> Result(Int, Failure) {
  let started = workflow.now(ctx)
  use _ <- result.map(workflow.sleep(ctx, week()))
  timestamp.difference(started, workflow.now(ctx)) |> duration.to_milliseconds
}

fn reminded() {
  workflow.SignalDefinition("reminded", codec.nil())
}

fn reminder() {
  workflow.Definition("reminder", codec.nil(), codec.string())
}

fn reminder_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  case workflow.await_timeout(ctx, signal.next(ctx, reminded()), week()) {
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
  use <- support.unless_musl("a_week_long_sleep_finishes_at_once_test")
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
  assert elapsed >= duration.to_milliseconds(week())
  assert duration.compare(
      timestamp.difference(before, testing.now(env)),
      week(),
    )
    != order.Lt

  // Time stands still between calls, so a signal sent now arrives before the deadline.
  let assert Ok(handle) =
    client.start(testing.client(env), reminder(), Nil, options())
  let assert Ok(Nil) = client.signal(handle, reminded(), Nil)
  assert testing.result(env, handle) == Ok("reminded in time")

  // `sleep` moves the clock past the deadline without anyone waiting for a result.
  let assert Ok(handle) =
    client.start(testing.client(env), reminder(), Nil, options())
  testing.sleep(env, duration.add(week(), duration.seconds(1)))
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
    testing.activity_info()
    |> testing.with_attempt(2)
    |> testing.with_heartbeat_details(codec.int(), 2)
  assert info.attempt == 2
  let env = testing.activity_env(info)
  assert counting(testing.activity_context(env), 3) == Ok(3)
  assert testing.heartbeats(env, codec.int()) == [3]

  let env = testing.activity_env(testing.activity_info())
  testing.cancel_activity(env, activity.CancelRequested)
  let assert Error(failure.Failure(kind: failure.Cancelled, ..)) =
    counting(testing.activity_context(env), 3)
  assert testing.heartbeat_count(env) == 0
}

pub fn an_activity_context_can_be_that_of_a_standalone_activity_test() {
  let standalone = fn(info) {
    testing.activity_env(info)
    |> testing.activity_context
    |> activity.is_standalone
  }
  assert !standalone(testing.activity_info())
  let info = testing.activity_info() |> testing.as_standalone
  assert standalone(info)
  assert info.workflow_id == "" && info.run_id == "" && info.workflow_type == ""
}

pub fn the_final_attempt_is_the_one_the_retry_policy_ends_on_test() {
  let on_attempt = fn(attempt, retry) {
    activity.Info(..testing.activity_info(), attempt:, retry:)
    |> testing.activity_env
    |> testing.activity_context
    |> activity.is_final_attempt
  }
  let three = Some(retry.policy(maximum_attempts: 3))
  assert !on_attempt(1, three)
  assert !on_attempt(2, three)
  assert on_attempt(3, three)
  assert on_attempt(1, Some(retry.policy(maximum_attempts: 1)))
  // Zero is no limit, and so is a task that came without a policy.
  assert !on_attempt(1000, Some(retry.policy(maximum_attempts: 0)))
  assert !on_attempt(1000, None)
  assert testing.activity_info().retry == None
  // The same, built with the setters.
  let third =
    testing.activity_info()
    |> testing.with_attempt(3)
    |> testing.with_retry(retry.policy(maximum_attempts: 3))
    |> testing.with_activity_type("charge")
    |> testing.with_headers(
      dict.from_list([#("tenant", codec.encode(codec.string(), "acme"))]),
    )
  assert activity.is_final_attempt(
    testing.activity_context(testing.activity_env(third)),
  )
  assert third.activity_type == "charge"
  assert dict.size(third.headers) == 1
}

@external(erlang, "temporal_test_ffi", "capture_log_metadata")
fn capture_log_metadata(body: fn() -> Nil) -> List(#(String, List(String)))

pub fn an_activity_logs_with_its_identity_and_fields_test() {
  let info = testing.activity_info()
  let ctx = testing.activity_context(testing.activity_env(info))
  let expected = fn(message, fields) {
    #(
      message,
      list.sort(
        list.append(fields, [
          "activity_id=<<\"" <> info.activity_id <> "\">>",
          "activity_type=<<\"" <> info.activity_type <> "\">>",
          "attempt=" <> int.to_string(info.attempt),
          "run_id=<<\"" <> info.run_id <> "\">>",
          "workflow_id=<<\"" <> info.workflow_id <> "\">>",
        ]),
        string.compare,
      ),
    )
  }
  assert capture_log_metadata(fn() {
      activity.log_debug(ctx, "looking up", [])
      activity.log_info(ctx, "charged", [
        log.int("cents", 1250),
        log.bool("retried", False),
      ])
      activity.log_warning(ctx, "slow", [log.float("seconds", 2.5)])
      activity.log_error(ctx, "declined", [log.string("reason", "expired")])
    })
    == [
      expected("looking up", []),
      expected("charged", ["fields.cents=1250", "fields.retried=false"]),
      expected("slow", ["fields.seconds=2.5"]),
      expected("declined", ["fields.reason=<<\"expired\">>"]),
    ]
}
