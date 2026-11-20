//// Asynchronous activity completion: an activity that is finished later, from somewhere
//// else, by its task token.
////
//// Use it when the answer comes from outside the worker: a person, a webhook, another
//// service. The activity hands its token to whoever will answer and returns; the answer
//// arrives through `client.complete_activity` or `client.fail_activity`. To wait for
//// something that can reach the workflow directly, a signal is simpler.

import gleam/erlang/process
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow

const task_queue = "examples-async-completion"

pub fn request_approval() {
  workflow.Definition("request_approval", codec.string(), codec.string())
}

pub fn ask_manager() {
  activity.Definition("ask_manager", codec.string(), codec.string())
}

fn request_approval_workflow(
  ctx: workflow.Context,
  request: String,
) -> Result(String, Failure) {
  // The start-to-close timeout is how long the answer may take.
  let options = workflow.activity_options(start_to_close: duration.hours(24))
  workflow.execute_activity(ctx, ask_manager(), request, options)
}

pub fn run(client: Client) -> Result(String, client.Error) {
  // Stands in for the outside world: a queue, a database row, a link in an email.
  let inbox = process.new_subject()
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(request_approval(), request_approval_workflow),
    ])
    |> worker.with_activities([
      activity.handler(ask_manager(), fn(ctx, _request) {
        // Hand the token over, then say that the result comes later.
        process.send(inbox, activity.task_token(ctx))
        Error(activity.complete_later(ctx))
      }),
    ])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-async-completion", task_queue:)
  let outcome = {
    use handle <- result.try(client.start(
      client,
      request_approval(),
      "a new laptop",
      options,
    ))
    // Later, anywhere with a client and the token:
    let assert Ok(token) = process.receive(inbox, 10_000)
    use _ <- result.try(client.complete_activity(
      client,
      token,
      ask_manager(),
      "approved",
    ))
    client.result(handle)
  }
  worker.stop(worker)
  outcome
}
