//// Nexus: an operation one team serves and a workflow in another team calls.
////
//// Use Nexus to call across namespaces or team boundaries through a named endpoint,
//// without the caller knowing which task queue or workflow does the work. Both sides
//// share a `workflow.NexusOperation`: the service, the operation's name, and its types.
////
//// The endpoint is registered on the server and points at the handler's task queue:
////
////     temporal operator nexus endpoint create --name examples-nexus \
////       --target-namespace default --target-task-queue examples-nexus-handler
////
//// A handler answers directly (`nexus.sync`, for work of a few seconds at most) or by
//// starting a workflow (`nexus.workflow_run`), in which case the operation finishes when
//// the workflow does.

import gleam/int
import gleam/list
import gleam/result
import gleam/string
import gleam/time/duration
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/nexus
import temporal/worker
import temporal/workflow
import temporal/workflow/operation

/// The task queue the endpoint must target.
pub const handler_task_queue = "examples-nexus-handler"

const caller_task_queue = "examples-nexus-caller"

// ---- the contract both teams share ----

pub fn normalise() {
  workflow.NexusOperation("text", "normalise", codec.string(), codec.string())
}

pub fn count_words() {
  workflow.NexusOperation("text", "count_words", codec.string(), codec.int())
}

// ---- the handler's side ----

fn count_words_run() {
  workflow.Definition("count_words_run", codec.string(), codec.int())
}

fn count_words_workflow(
  _ctx: workflow.Context,
  text: String,
) -> Result(Int, Failure) {
  Ok(list.length(string.split(text, " ")))
}

fn handlers() -> List(nexus.Handler) {
  [
    // Answered directly by a function.
    nexus.sync(normalise(), fn(_ctx, text) {
      Ok(string.lowercase(string.trim(text)))
    }),
    // Served by a workflow. The request id is stable across the caller's retries, so a
    // retried request reaches the workflow it already started.
    nexus.workflow_run(count_words(), count_words_run(), fn(ctx, text) {
      #(
        text,
        client.start_options(
          workflow_id: "examples-nexus-" <> ctx.request_id,
          task_queue: handler_task_queue,
        ),
      )
    }),
  ]
}

// ---- the caller's side ----

/// The input is the endpoint's name.
pub fn analyse() {
  workflow.Definition("analyse", codec.string(), codec.string())
}

fn analyse_workflow(
  ctx: workflow.Context,
  endpoint: String,
) -> Result(String, Failure) {
  let options =
    operation.options()
    |> operation.with_schedule_to_close(duration.minutes(1))
  use text <- result.try(operation.execute(
    ctx,
    endpoint,
    normalise(),
    "  Hello Nexus World ",
    options,
  ))
  use words <- result.try(operation.execute(
    ctx,
    endpoint,
    count_words(),
    text,
    options,
  ))
  Ok(text <> ": " <> int.to_string(words) <> " words")
}

/// `endpoint` is the name of a Nexus endpoint that targets `handler_task_queue`.
pub fn run(client: Client, endpoint: String) -> Result(String, client.Error) {
  use handler <- result.try(
    worker.new(client, handler_task_queue)
    |> worker.with_workflows([
      workflow.handler(count_words_run(), count_words_workflow),
    ])
    |> worker.with_nexus_handlers(handlers())
    |> worker.start,
  )
  use caller <- result.try(
    worker.new(client, caller_task_queue)
    |> worker.with_workflows([workflow.handler(analyse(), analyse_workflow)])
    |> worker.start,
  )
  let options =
    client.start_options(
      workflow_id: "examples-nexus",
      task_queue: caller_task_queue,
    )
  let analysis = client.execute(client, analyse(), endpoint, options)
  worker.stop(caller)
  worker.stop(handler)
  analysis
}
