//// Search attributes and memo: data attached to a workflow that is visible from outside,
//// and the client calls that find and describe workflows.
////
//// Search attributes are indexed: use them to find workflows with `client.list` and
//// `client.count`. A memo is not indexed: use it for notes that `client.describe` and the
//// workflow itself can read. Neither is for large data.
////
//// A custom search attribute must be registered on the namespace first:
////
////     temporal operator search-attribute create --name ExamplesCustomer --type Keyword

import gleam/dict
import gleam/erlang/process
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/search_attributes
import temporal/worker
import temporal/workflow
import temporal/workflow/signal

const task_queue = "examples-search-attributes"

/// The custom search attribute this example uses, a `Keyword`.
pub const attribute = "ExamplesCustomer"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// How many running workflows the customer had.
    count: Int,
    /// Their ids, as listed.
    workflow_ids: List(String),
    /// The memo of one of them, as described: the note it was started with and the stage
    /// the workflow set itself.
    note: String,
    stage: String,
  )
}

pub fn handle_ticket() {
  workflow.Definition("handle_ticket", codec.string(), codec.string())
}

pub fn close() {
  workflow.SignalDefinition("close", codec.nil())
}

fn handle_ticket_workflow(
  ctx: workflow.Context,
  ticket: String,
) -> Result(String, Failure) {
  // Read the memo the workflow was started with, and change what the server shows.
  let note = workflow.memo(ctx, "note", codec.string()) |> option.unwrap("")
  workflow.upsert_memo(ctx, "stage", codec.string(), "open")
  signal.receive(ctx, close())
  // Search attributes can be changed the same way, with `upsert_search_attributes`.
  workflow.upsert_memo(ctx, "stage", codec.string(), "closed")
  Ok(ticket <> ": " <> note)
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(handle_ticket(), handle_ticket_workflow),
    ])
    |> worker.start,
  )
  let start = fn(ticket) {
    let options =
      client.start_options(
        workflow_id: "examples-search-attributes-" <> ticket,
        task_queue:,
      )
      |> client.with_search_attributes(
        dict.from_list([#(attribute, search_attributes.Keyword("acme"))]),
      )
      |> client.with_memo(
        dict.from_list([#("note", codec.encode(codec.string(), "be kind"))]),
      )
    client.start(client, handle_ticket(), ticket, options)
  }
  let outcome = {
    use first <- result.try(start("ticket-1"))
    use second <- result.try(start("ticket-2"))

    // A visibility query, in the syntax of the Temporal UI's search box.
    let query = attribute <> " = \"acme\" AND ExecutionStatus = \"Running\""
    use count <- result.try(wait_for_count(client, query, 2, 50))
    use found <- result.try(client.list(client, query, limit: 10))

    use _ <- result.try(
      list.try_each([first, second], fn(handle) {
        use _ <- result.try(client.signal(handle, close(), Nil))
        client.result(handle)
      }),
    )

    // `describe` returns the memo as payloads; decode each with its codec.
    use description <- result.try(client.describe(first))
    let memo = fn(key) {
      dict.get(description.memo, key)
      |> result.try(fn(payload) {
        codec.decode(codec.string(), payload) |> result.replace_error(Nil)
      })
      |> result.unwrap("")
    }
    Ok(Outcome(
      count:,
      workflow_ids: list.map(found, fn(run) { run.workflow_id })
        |> list.sort(string.compare),
      note: memo("note"),
      stage: memo("stage"),
    ))
  }
  worker.stop(worker)
  outcome
}

/// The visibility store lags behind: poll until it has caught up.
fn wait_for_count(
  client: Client,
  query: String,
  expected: Int,
  tries: Int,
) -> Result(Int, client.Error) {
  use count <- result.try(client.count(client, query))
  case count >= expected || tries <= 0 {
    True -> Ok(count)
    False -> {
      process.sleep(200)
      wait_for_count(client, query, expected, tries - 1)
    }
  }
}
