//// Queries: read a running workflow's state from outside without changing it.
////
//// Workflow state lives in the workflow function's arguments and local variables, so the
//// workflow sets a handler that closes over the current state, and sets it again when
//// the state changes. A handler must only read.

import gleam/option.{None, Some}
import gleam/result
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow
import temporal/workflow/query
import temporal/workflow/signal

const task_queue = "examples-queries"

/// What `run` read from the workflow.
pub type Outcome {
  Outcome(total: Int, at_least_five: Bool, final: Int)
}

pub fn counter() {
  workflow.Definition("counter", codec.int(), codec.int())
}

pub fn add() {
  workflow.SignalDefinition("add", codec.int())
}

pub fn finish() {
  workflow.SignalDefinition("finish", codec.nil())
}

/// A query without an argument.
pub fn total() {
  workflow.QueryDefinition("total", codec.nil(), codec.int())
}

/// A query with an argument.
pub fn at_least() {
  workflow.QueryDefinition("at_least", codec.int(), codec.bool())
}

fn counter_workflow(ctx: workflow.Context, count: Int) -> Result(Int, Failure) {
  // Set again on every turn of the loop, so the handlers always see the current count.
  query.set_handler(ctx, total(), fn(_) { count })
  query.set_handler(ctx, at_least(), fn(threshold) { count >= threshold })

  let next =
    workflow.race([
      signal.next(ctx, add()) |> workflow.map(Some),
      signal.next(ctx, finish()) |> workflow.map(fn(_) { None }),
    ])
  case workflow.await(next) {
    Some(amount) -> counter_workflow(ctx, count + amount)
    None -> Ok(count)
  }
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(counter(), counter_workflow)])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-queries", task_queue:)
  let outcome = {
    use handle <- result.try(client.start(client, counter(), 0, options))
    use _ <- result.try(client.signal(handle, add(), 2))
    use _ <- result.try(client.signal(handle, add(), 3))
    // A query sees every signal the server recorded before it.
    use total <- result.try(client.query(handle, total(), Nil))
    use at_least_five <- result.try(client.query(handle, at_least(), 5))
    use _ <- result.try(client.signal(handle, finish(), Nil))
    use final <- result.try(client.result(handle))
    Ok(Outcome(total:, at_least_five:, final:))
  }
  worker.stop(worker)
  outcome
}
