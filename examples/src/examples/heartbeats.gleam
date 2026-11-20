//// Heartbeats: a long activity that reports progress, resumes from it after a failure,
//// and stops when it is cancelled.
////
//// Use heartbeats for any activity that runs longer than a few seconds. With a heartbeat
//// timeout the server notices a dead worker quickly instead of waiting for the whole
//// start-to-close timeout, and heartbeating is the only way an activity learns that it
//// was cancelled.

import gleam/erlang/process
import gleam/int
import gleam/option
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/retry
import temporal/worker
import temporal/workflow

const task_queue = "examples-heartbeats"

pub fn import_rows() {
  workflow.Definition("import_rows", codec.int(), codec.string())
}

/// Imports rows up to its input and returns the row its last attempt started from.
pub fn load_rows() {
  activity.Definition("load_rows", codec.int(), codec.int())
}

/// Follows a feed until it is cancelled.
pub fn follow_feed() {
  activity.Definition("follow_feed", codec.nil(), codec.nil())
}

fn import_rows_workflow(
  ctx: workflow.Context,
  rows: Int,
) -> Result(String, Failure) {
  let options =
    workflow.activity_options(start_to_close: duration.minutes(10))
    // An attempt that goes this long without a heartbeat is failed and retried.
    |> workflow.with_heartbeat_timeout(duration.seconds(1))
    |> workflow.with_retry(
      retry.policy(maximum_attempts: 3)
      |> retry.with_initial_interval(duration.milliseconds(100)),
    )
    // Wait for the activity to say it stopped, instead of moving on at once.
    |> workflow.with_cancellation(workflow.WaitCancellationCompleted)

  use resumed_from <- result.try(workflow.execute_activity(
    ctx,
    load_rows(),
    rows,
    options,
  ))

  // Cancel an activity by cancelling its scope, here with a deadline.
  let followed =
    workflow.with_timeout(ctx, duration.milliseconds(300), fn(scope) {
      workflow.execute_activity(scope, follow_feed(), Nil, options)
    })
  let feed = case followed {
    Error(failed) ->
      case failure.is_cancellation(failure.root_cause(failed)) {
        True -> "feed stopped"
        False -> "feed failed"
      }
    Ok(Nil) -> "feed ended"
  }
  Ok(
    int.to_string(rows)
    <> " rows, the last attempt resumed from row "
    <> int.to_string(resumed_from)
    <> "; "
    <> feed,
  )
}

fn load_rows_activity(
  ctx: activity.Context,
  rows: Int,
) -> Result(Int, Failure) {
  // What the previous attempt last recorded, if there was one.
  let start = activity.heartbeat_details(ctx, codec.int()) |> option.unwrap(0)
  load_from(ctx, start, rows) |> result.replace(start)
}

fn load_from(
  ctx: activity.Context,
  row: Int,
  rows: Int,
) -> Result(Nil, Failure) {
  let first_attempt = activity.info(ctx).attempt == 1
  case activity.is_cancelled(ctx) {
    // Accept the cancellation.
    True -> Error(failure.cancelled())
    False if row >= rows -> Ok(Nil)
    // Stands in for a crash halfway through the first attempt.
    False if first_attempt && row == rows / 2 ->
      Error(failure.new("connection lost"))
    False -> {
      // ... import the row here ...
      activity.heartbeat_with(ctx, codec.int(), row + 1)
      load_from(ctx, row + 1, rows)
    }
  }
}

fn follow_feed_activity(ctx: activity.Context, _: Nil) -> Result(Nil, Failure) {
  activity.heartbeat(ctx)
  case activity.is_cancelled(ctx) {
    True -> Error(failure.cancelled())
    False -> {
      process.sleep(50)
      follow_feed_activity(ctx, Nil)
    }
  }
}

pub fn run(client: Client) -> Result(String, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(import_rows(), import_rows_workflow),
    ])
    |> worker.with_activities([
      activity.handler(load_rows(), load_rows_activity),
      activity.handler(follow_feed(), follow_feed_activity),
    ])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-heartbeats", task_queue:)
  let summary = client.execute(client, import_rows(), 10, options)
  worker.stop(worker)
  summary
}
