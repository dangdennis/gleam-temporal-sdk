//// Updates: requests a client sends to a running workflow and waits on. An update may
//// change the workflow, like a signal, and returns a value, like a query.
////
//// The workflow takes updates when it is ready for them and answers each one, the shape
//// of a BEAM `call`. A validator refuses bad requests before they are recorded.

import gleam/list
import gleam/result
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/schema
import temporal/worker
import temporal/workflow
import temporal/workflow/signal
import temporal/workflow/update

const task_queue = "examples-updates"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// The cart's size after each accepted update.
    sizes: List(Int),
    /// Why the validator refused the empty item.
    refused: String,
    /// The cart the workflow returned.
    items: List(String),
  )
}

fn items_codec() {
  schema.codec(schema.list(schema.string()))
}

pub fn cart() {
  workflow.Definition("cart", items_codec(), items_codec())
}

/// Add an item; the answer is the number of items in the cart.
pub fn add_item() {
  workflow.UpdateDefinition("add_item", codec.string(), codec.int())
}

pub fn check_out() {
  workflow.SignalDefinition("check_out", codec.nil())
}

type Event {
  Added(update.Update(String, Int))
  CheckedOut
}

fn cart_workflow(
  ctx: workflow.Context,
  items: List(String),
) -> Result(List(String), Failure) {
  // Decide what to accept. A refused update leaves no trace in history. The validator
  // must only look at its argument.
  update.set_validator(ctx, add_item(), fn(item) {
    case item {
      "" -> Error("an item needs a name")
      _ -> Ok(Nil)
    }
  })
  let event =
    workflow.race([
      update.next(ctx, add_item()) |> workflow.map(Added),
      signal.next(ctx, check_out()) |> workflow.map(fn(_) { CheckedOut }),
    ])
  case workflow.await(event) {
    Added(request) -> {
      let items = list.append(items, [update.argument(request)])
      // Work done before answering, such as an activity, happens while the client waits.
      update.complete(ctx, request, Ok(list.length(items)))
      cart_workflow(ctx, items)
    }
    CheckedOut -> Ok(items)
  }
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(cart(), cart_workflow)])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-updates", task_queue:)
    // Update-with-start must say what to do when the workflow is already running.
    |> client.with_id_conflict_policy(client.UseExisting)
  let outcome = {
    // Update-with-start: the first thing done to the cart is to add to it, so start the
    // workflow and send the update as one request.
    use #(handle, first) <- result.try(client.execute_update_with_start(
      client,
      cart(),
      [],
      options,
      add_item(),
      "apples",
    ))
    // An update to a running workflow.
    use second <- result.try(client.execute_update(handle, add_item(), "pears"))
    // The validator refuses this one: the call fails and the cart is unchanged.
    let refused = case client.execute_update(handle, add_item(), "") {
      Error(client.UpdateFailed(rejected)) -> rejected.message
      _ -> "not refused"
    }
    use _ <- result.try(client.signal(handle, check_out(), Nil))
    use items <- result.try(client.result(handle))
    Ok(Outcome(sizes: [first, second], refused:, items:))
  }
  worker.stop(worker)
  outcome
}
