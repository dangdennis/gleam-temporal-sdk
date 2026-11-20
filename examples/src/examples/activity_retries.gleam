//// Activity options and retries: timeouts, a retry policy, and the two ways to stop a
//// failure from being retried.
////
//// Use this whenever an activity calls something that can fail. By default the server
//// retries a failed activity without limit, so set the policy to what the work can bear.

import gleam/int
import gleam/option.{None, Some}
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/retry
import temporal/worker
import temporal/workflow

const task_queue = "examples-activity-retries"

pub fn checkout() {
  workflow.Definition("checkout", codec.string(), codec.string())
}

/// Fails twice with a retryable failure, then succeeds with the attempt number.
pub fn reserve_stock() {
  activity.Definition("reserve_stock", codec.string(), codec.int())
}

/// Fails with the `CardDeclined` type, which the retry policy names as not worth retrying.
pub fn charge_card() {
  activity.Definition("charge_card", codec.string(), codec.string())
}

/// Fails with a failure the activity itself marks as non-retryable.
pub fn send_receipt() {
  activity.Definition("send_receipt", codec.string(), codec.nil())
}

fn checkout_workflow(
  ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
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

  // Retried until it succeeds: the workflow only sees the final outcome.
  use attempt <- result.try(workflow.execute_activity(
    ctx,
    reserve_stock(),
    order_id,
    options,
  ))

  // An activity failure wraps the error the activity returned: `root_cause` unwraps it.
  let charged = case
    workflow.execute_activity(ctx, charge_card(), order_id, options)
  {
    Ok(reference) -> "charged " <> reference
    Error(failed) -> "not charged: " <> describe(failure.root_cause(failed))
  }
  let receipt = case
    workflow.execute_activity(ctx, send_receipt(), order_id, options)
  {
    Ok(Nil) -> "receipt sent"
    Error(failed) -> "no receipt: " <> describe(failure.root_cause(failed))
  }
  Ok(
    "stock reserved on attempt "
    <> int.to_string(attempt)
    <> "; "
    <> charged
    <> "; "
    <> receipt,
  )
}

fn describe(failure: Failure) -> String {
  case failure.error_type {
    Some(error_type) -> error_type
    None -> failure.message
  }
}

fn reserve_stock_activity(
  ctx: activity.Context,
  _order_id: String,
) -> Result(Int, Failure) {
  case activity.info(ctx).attempt {
    // A plain failure is retryable.
    attempt if attempt < 3 -> Error(failure.new("warehouse is busy"))
    attempt -> Ok(attempt)
  }
}

fn charge_card_activity(
  _ctx: activity.Context,
  _order_id: String,
) -> Result(String, Failure) {
  // The type lets the retry policy, and the workflow, tell failures apart.
  Error(
    failure.new("the card was declined") |> failure.with_type("CardDeclined"),
  )
}

fn send_receipt_activity(
  _ctx: activity.Context,
  _order_id: String,
) -> Result(Nil, Failure) {
  // Not retried whatever the policy says.
  Error(failure.new("no email address on file") |> failure.non_retryable)
}

pub fn run(client: Client) -> Result(String, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(checkout(), checkout_workflow)])
    |> worker.with_activities([
      activity.handler(reserve_stock(), reserve_stock_activity),
      activity.handler(charge_card(), charge_card_activity),
      activity.handler(send_receipt(), send_receipt_activity),
    ])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-activity-retries", task_queue:)
  let summary = client.execute(client, checkout(), "order-1", options)
  worker.stop(worker)
  summary
}
