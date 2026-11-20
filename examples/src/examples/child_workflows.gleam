//// Child workflows: workflows started by a workflow.
////
//// Use a child to give part of the work its own history, id and task queue, or to start
//// something that outlives its parent. For a plain function call, call a function; for a
//// side effect, use an activity.

import gleam/result
import gleam/time/duration
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow
import temporal/workflow/child
import temporal/workflow/signal

const task_queue = "examples-child-workflows"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// The parent's result.
    order: String,
    /// The result of the child that outlived the parent.
    audit: String,
  )
}

pub fn fulfil_order() {
  workflow.Definition("fulfil_order", codec.string(), codec.string())
}

pub fn pack() {
  workflow.Definition("pack", codec.string(), codec.string())
}

pub fn ship() {
  workflow.Definition("ship", codec.string(), codec.string())
}

pub fn audit() {
  workflow.Definition("audit", codec.string(), codec.string())
}

/// The address to ship to, sent to the `ship` child.
pub fn address() {
  workflow.SignalDefinition("address", codec.string())
}

fn fulfil_order_workflow(
  ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
  // Start a child and wait for its result.
  use packed <- result.try(child.execute(
    ctx,
    pack(),
    order_id,
    child.options(workflow_id: order_id <> "-pack"),
  ))

  // Start a child, signal it once it runs, then wait for its result.
  let shipping =
    child.start(
      ctx,
      ship(),
      order_id,
      child.options(workflow_id: order_id <> "-ship"),
    )
  use _run_id <- result.try(workflow.await(child.started(shipping)))
  use _ <- result.try(
    workflow.await(child.signal(ctx, shipping, address(), "1 Main Street")),
  )
  use shipped <- result.try(workflow.await(child.result(shipping)))

  // A child that carries on after the parent closes. By default it would be terminated.
  // The parent must still wait until it has started, or it never starts.
  let auditing =
    child.start(
      ctx,
      audit(),
      order_id,
      child.options(workflow_id: order_id <> "-audit")
        |> child.with_parent_close_policy(child.Abandon),
    )
  use _run_id <- result.try(workflow.await(child.started(auditing)))

  Ok(packed <> ", " <> shipped)
}

fn pack_workflow(
  _ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
  Ok("packed " <> order_id)
}

fn ship_workflow(
  ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
  let address = signal.receive(ctx, address())
  Ok("shipped " <> order_id <> " to " <> address)
}

fn audit_workflow(
  ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
  use _ <- result.try(workflow.sleep(ctx, duration.milliseconds(500)))
  Ok("audited " <> order_id)
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(fulfil_order(), fulfil_order_workflow),
      workflow.handler(pack(), pack_workflow),
      workflow.handler(ship(), ship_workflow),
      workflow.handler(audit(), audit_workflow),
    ])
    |> worker.start,
  )
  let order_id = "examples-child-workflows"
  let options = client.start_options(workflow_id: order_id, task_queue:)
  let outcome = {
    use order <- result.try(client.execute(
      client,
      fulfil_order(),
      order_id,
      options,
    ))
    // The parent has closed; the abandoned child is still running under its own id.
    let auditing = client.handle(client, audit(), order_id <> "-audit")
    use audit <- result.try(client.result(auditing))
    Ok(Outcome(order:, audit:))
  }
  worker.stop(worker)
  outcome
}
