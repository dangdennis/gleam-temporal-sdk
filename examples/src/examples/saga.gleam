//// Saga: undo the steps already taken when a later step fails.
////
//// Use it when a workflow changes several systems that share no transaction. Keep a list
//// of what to undo, newest first, and run it in a non-cancellable scope so that the
//// compensation also happens when the workflow is cancelled.

import gleam/erlang/process.{type Subject}
import gleam/list
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

const task_queue = "examples-saga"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// The type of the failure the workflow ended with.
    failed_with: String,
    /// The activities that ran, in order.
    steps: List(String),
  )
}

pub fn book_trip() {
  workflow.Definition("book_trip", codec.string(), codec.string())
}

fn step(name: String) {
  activity.Definition(name, codec.string(), codec.nil())
}

pub fn book_flight() {
  step("book_flight")
}

pub fn cancel_flight() {
  step("cancel_flight")
}

pub fn book_hotel() {
  step("book_hotel")
}

pub fn cancel_hotel() {
  step("cancel_hotel")
}

/// Always fails: there are no cars.
pub fn book_car() {
  step("book_car")
}

fn options() {
  workflow.activity_options(start_to_close: duration.seconds(10))
  |> workflow.with_retry(retry.policy(maximum_attempts: 3))
}

fn book_trip_workflow(
  ctx: workflow.Context,
  trip: String,
) -> Result(String, Failure) {
  let book = fn(step) { workflow.execute_activity(ctx, step, trip, options()) }

  use _ <- result.try(book(book_flight()))
  let undo = [cancel_flight()]
  use _ <- result.try(book(book_hotel()) |> or_compensate(ctx, trip, undo))
  let undo = [cancel_hotel(), ..undo]
  use _ <- result.try(book(book_car()) |> or_compensate(ctx, trip, undo))
  Ok("booked " <> trip)
}

/// On failure, run every compensation, newest first, then pass the failure on.
fn or_compensate(
  outcome: Result(a, Failure),
  ctx: workflow.Context,
  trip: String,
  undo: List(activity.Definition(String, Nil)),
) -> Result(a, Failure) {
  use failed <- result.try_recover(outcome)
  workflow.non_cancellable(ctx, fn(shielded) {
    list.each(undo, fn(step) {
      // A compensation that fails for good is logged and the rest still run.
      case workflow.execute_activity(shielded, step, trip, options()) {
        Ok(Nil) -> Nil
        Error(_) ->
          workflow.log_error(ctx, "compensation failed: " <> step.name, [])
      }
    })
  })
  Error(failed)
}

/// An activity that reports its name to `steps` and succeeds.
fn recording(
  definition: activity.Definition(String, Nil),
  steps: Subject(String),
) -> activity.Handler {
  activity.handler(definition, fn(_ctx, _trip) {
    process.send(steps, definition.name)
    Ok(Nil)
  })
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  // The activities report to this process, so that `run` can return what ran.
  let steps = process.new_subject()
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(book_trip(), book_trip_workflow)])
    |> worker.with_activities([
      recording(book_flight(), steps),
      recording(cancel_flight(), steps),
      recording(book_hotel(), steps),
      recording(cancel_hotel(), steps),
      activity.handler(book_car(), fn(_ctx, _trip) {
        process.send(steps, "book_car")
        Error(
          failure.new("no cars left")
          |> failure.with_type("NoCars")
          |> failure.non_retryable,
        )
      }),
    ])
    |> worker.start,
  )
  let options = client.start_options(workflow_id: "examples-saga", task_queue:)
  let failed_with = case
    client.execute(client, book_trip(), "lisbon", options)
  {
    Error(client.WorkflowFailed(failed)) ->
      failure.root_cause(failed).error_type |> option.unwrap("untyped")
    Error(_) -> "another error"
    Ok(_) -> "nothing"
  }
  worker.stop(worker)
  Ok(Outcome(failed_with:, steps: drain(steps, [])))
}

fn drain(steps: Subject(String), seen: List(String)) -> List(String) {
  case process.receive(steps, 0) {
    Ok(step) -> drain(steps, [step, ..seen])
    Error(Nil) -> list.reverse(seen)
  }
}
