//// Signals: one-way messages to a running workflow.
////
//// Use a signal to tell a workflow that something happened. The workflow takes signals
//// when it is ready for them; those that arrive early are buffered, in order. To ask a
//// workflow something and get an answer, use a query or an update instead.

import gleam/result
import gleam/time/duration
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow
import temporal/workflow/signal

const task_queue = "examples-signals"

pub fn review() {
  workflow.Definition("review", codec.string(), codec.string())
}

/// A reviewer's comment.
pub fn comment() {
  workflow.SignalDefinition("comment", codec.string())
}

/// The decision: approved or not.
pub fn decide() {
  workflow.SignalDefinition("decide", codec.bool())
}

fn review_workflow(
  ctx: workflow.Context,
  document: String,
) -> Result(String, Failure) {
  // receive: block until the next signal of this kind.
  let first_comment = signal.receive(ctx, comment())

  // next: a future of the next signal, to wait for with a deadline or to race.
  let decision =
    workflow.await_timeout(ctx, signal.next(ctx, decide()), duration.minutes(5))
  case decision {
    Ok(True) -> Ok(document <> " approved (" <> first_comment <> ")")
    Ok(False) -> Ok(document <> " rejected (" <> first_comment <> ")")
    Error(failed) ->
      case failure.is_timeout(failed) {
        True -> Ok(document <> " expired")
        // The workflow was cancelled while it waited.
        False -> Error(failed)
      }
  }
}

pub fn run(client: Client) -> Result(String, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(review(), review_workflow)])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-signals", task_queue:)
  let outcome = {
    // Signal-with-start: start the workflow if it is not running, and signal it either
    // way. The signal is there before the workflow's first task.
    use handle <- result.try(client.signal_with_start(
      client,
      review(),
      "report.pdf",
      options,
      comment(),
      "looks good",
    ))
    // Signal a running workflow. Any process that knows the workflow id can make the
    // handle: `client.handle(client, review(), "examples-signals")`.
    use _ <- result.try(client.signal(handle, decide(), True))
    client.result(handle)
  }
  worker.stop(worker)
  outcome
}
