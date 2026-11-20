//// Cancellation scopes: cancel part of a workflow, run cleanup that cancellation cannot
//// reach, and respond to a cancellation request from a client.
////
//// Cancellation travels with the `Context`. Whatever is started with a context fails
//// with a cancellation when that context's scope is cancelled, so every wait returns a
//// `Result` and cleanup is ordinary error handling.

import gleam/erlang/process
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow

const task_queue = "examples-cancellation"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// What the workflow that cancelled a scope of its own returned.
    scoped: String,
    /// Whether the workflow cancelled by the client ended as cancelled.
    cancelled: Bool,
    /// What its cleanup released.
    released: String,
  )
}

pub fn scoped() {
  workflow.Definition("scoped", codec.nil(), codec.string())
}

pub fn hold_seat() {
  workflow.Definition("hold_seat", codec.string(), codec.nil())
}

pub fn release_seat() {
  activity.Definition("release_seat", codec.string(), codec.nil())
}

/// Cancels a scope of its own. Only what was started in the scope is cancelled; the
/// workflow carries on.
fn scoped_workflow(ctx: workflow.Context, _: Nil) -> Result(String, Failure) {
  let waited =
    workflow.with_scope(ctx, fn(scope) {
      let timer = workflow.timer(scope, duration.hours(1))
      workflow.cancel(scope)
      workflow.await(timer)
    })
  use _ <- result.try(workflow.sleep(ctx, duration.milliseconds(10)))
  case waited {
    Error(failed) ->
      case failure.is_cancellation(failed) {
        True -> Ok("the scope was cancelled, the workflow was not")
        False -> Error(failed)
      }
    Ok(Nil) -> Ok("the timer fired")
  }
}

/// Holds a seat until the workflow is cancelled. A request to cancel the workflow cancels
/// `ctx`, which fails the sleep.
fn hold_seat_workflow(
  ctx: workflow.Context,
  seat: String,
) -> Result(Nil, Failure) {
  case workflow.sleep(ctx, duration.hours(1)) {
    Ok(Nil) -> Ok(Nil)
    Error(cancelled) -> {
      // `ctx` is cancelled, so an activity started with it would fail at once. A
      // non-cancellable scope shields the cleanup.
      let options =
        workflow.activity_options(start_to_close: duration.seconds(10))
      use _ <- result.try(
        workflow.non_cancellable(ctx, fn(shielded) {
          workflow.execute_activity(shielded, release_seat(), seat, options)
        }),
      )
      // Returning the cancellation is what ends the workflow as cancelled.
      Error(cancelled)
    }
  }
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  // The activity reports to this process, so that `run` can return what it released.
  let releases = process.new_subject()
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(scoped(), scoped_workflow),
      workflow.handler(hold_seat(), hold_seat_workflow),
    ])
    |> worker.with_activities([
      activity.handler(release_seat(), fn(_ctx, seat) {
        process.send(releases, seat)
        Ok(Nil)
      }),
    ])
    |> worker.start,
  )
  let outcome = {
    use scoped <- result.try(client.execute(
      client,
      scoped(),
      Nil,
      client.start_options(
        workflow_id: "examples-cancellation-scoped",
        task_queue:,
      ),
    ))
    use handle <- result.try(client.start(
      client,
      hold_seat(),
      "12A",
      client.start_options(
        workflow_id: "examples-cancellation-hold",
        task_queue:,
      ),
    ))
    use _ <- result.try(client.cancel(handle))
    let cancelled = client.result(handle) == Error(client.WorkflowCancelled)
    let released = process.receive(releases, 5000) |> result.unwrap("nothing")
    Ok(Outcome(scoped:, cancelled:, released:))
  }
  worker.stop(worker)
  outcome
}
