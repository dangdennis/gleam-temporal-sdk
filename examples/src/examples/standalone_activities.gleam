//// Standalone activities: activities a client starts directly, with no workflow around
//// them.
////
//// Use one for a single step that should be durable, retried and observable but needs
//// no orchestration: sending an email, calling a webhook, running one job. The server
//// schedules, retries and times it out as it does for a workflow's activities, and the
//// activity code is the same. When the step grows a second step, put both in a workflow.
////
//// The server must have standalone activities enabled: Temporal CLI 1.10's development
//// server has; elsewhere set the dynamic config `activity.enableStandalone`.

import gleam/erlang/process
import gleam/int
import gleam/result
import gleam/string
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/client/standalone_activity
import temporal/codec
import temporal/failure.{type Failure}
import temporal/retry
import temporal/worker

const task_queue = "examples-standalone-activities"

/// Sends a receipt to an address and returns the provider's message id.
pub fn send_receipt() {
  activity.Definition("send_receipt", codec.string(), codec.string())
}

/// Follows a feed until it is cancelled.
pub fn follow_feed() {
  activity.Definition("follow_feed", codec.nil(), codec.nil())
}

fn send_receipt_activity(
  ctx: activity.Context,
  address: String,
) -> Result(String, Failure) {
  let info = activity.info(ctx)
  // There is no workflow to name: `info.workflow_id` is empty and the activity has a run
  // id of its own.
  let assert True = activity.is_standalone(ctx)
  case info.attempt {
    // Stands in for a provider that is down the first time.
    1 -> Error(failure.new("mail provider unavailable"))
    attempt ->
      Ok("sent to " <> address <> " on attempt " <> int.to_string(attempt))
  }
}

fn follow_feed_activity(ctx: activity.Context, _: Nil) -> Result(Nil, Failure) {
  // Heartbeating is how an activity learns that it was cancelled.
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
  // Any worker with the activity registered on the task queue runs it; it needs no
  // workflows.
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_activities([
      activity.handler(send_receipt(), send_receipt_activity),
      activity.handler(follow_feed(), follow_feed_activity),
    ])
    |> worker.start,
  )
  let summary = scenario(client)
  worker.stop(worker)
  summary
}

fn scenario(client: Client) -> Result(String, client.Error) {
  // Start it and wait for its result. A start-to-close timeout is required.
  let options =
    standalone_activity.options(
      activity_id: "examples-standalone-receipt",
      task_queue:,
      start_to_close: duration.seconds(10),
    )
    |> standalone_activity.with_retry(
      retry.policy(maximum_attempts: 3)
      |> retry.with_initial_interval(duration.milliseconds(100)),
    )
  use receipt <- result.try(standalone_activity.execute(
    client,
    send_receipt(),
    "ada@example.com",
    options,
  ))

  // Or start it and keep the handle: to describe it, cancel it, or wait later.
  let options =
    standalone_activity.options(
      activity_id: "examples-standalone-feed",
      task_queue:,
      start_to_close: duration.minutes(10),
    )
    // The server tells an activity that it was cancelled when it heartbeats.
    |> standalone_activity.with_heartbeat_timeout(duration.seconds(1))
  use feed <- result.try(standalone_activity.start(
    client,
    follow_feed(),
    Nil,
    options,
  ))
  use description <- result.try(standalone_activity.describe(feed))
  use _ <- result.try(standalone_activity.cancel(feed, "example is over"))
  let ended = case standalone_activity.result(feed) {
    Error(client.ActivityCancelled) -> "cancelled"
    Error(other) -> string.inspect(other)
    Ok(Nil) -> "ended"
  }

  // A handle can be made again from the id alone, in another process or another day.
  let again =
    standalone_activity.handle(
      client,
      send_receipt(),
      "examples-standalone-receipt",
    )
  use described <- result.try(standalone_activity.describe(again))
  Ok(
    receipt
    <> "; receipt is "
    <> string.inspect(described.status)
    <> " after "
    <> int.to_string(described.attempt)
    <> " attempts; feed was "
    <> string.inspect(description.status)
    <> ", then "
    <> ended,
  )
}
