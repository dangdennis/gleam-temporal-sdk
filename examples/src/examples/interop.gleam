//// Interop: a Gleam workflow that calls activities implemented by a worker written with
//// another Temporal SDK.
////
//// Activities and workflows are called by name on a task queue, and values travel as
//// JSON, so the two sides need not share a language. Here the billing team runs a
//// TypeScript worker on the `examples-interop-billing` queue that exports:
////
//// ```ts
//// export async function chargeCustomer(customerId: string, amountCents: number): Promise<Charge>
//// // interface Charge { transactionId: string; amountCents: number; receiptUrl?: string }
//// ```
////
//// The Gleam side writes a definition with the same name, an argument-list codec for the
//// two arguments, and a schema with the other side's field names. A second Gleam worker
//// stands in for the TypeScript one here, so that the example runs by itself. The same
//// works in the other direction, and for child workflows with `child.with_task_queue`.

import gleam/option.{type Option, None}
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/schema
import temporal/worker
import temporal/workflow

const task_queue = "examples-interop"

/// The queue the other SDK's worker polls.
const billing_task_queue = "examples-interop-billing"

/// The other side's `Charge` object. The Gleam names are ours; the JSON names are theirs.
pub type Charge {
  Charge(transaction_id: String, amount_cents: Int, receipt_url: Option(String))
}

fn charge_schema() {
  schema.object3(
    Charge,
    schema.field("transactionId", schema.string(), fn(c: Charge) {
      c.transaction_id
    }),
    schema.field("amountCents", schema.int(), fn(c: Charge) { c.amount_cents }),
    // `receiptUrl?:` in TypeScript: absent or null reads as `None`.
    schema.field("receiptUrl", schema.optional(schema.string()), fn(c: Charge) {
      c.receipt_url
    }),
  )
}

/// The name is the other side's function name, exactly.
pub fn charge_customer() {
  activity.Definition(
    "chargeCustomer",
    codec.args2(codec.string(), codec.int()),
    schema.codec(charge_schema()),
  )
}

pub fn renew_subscription() {
  workflow.Definition("renew_subscription", codec.string(), codec.string())
}

fn renew_subscription_workflow(
  ctx: workflow.Context,
  customer_id: String,
) -> Result(String, Failure) {
  let options =
    workflow.activity_options(start_to_close: duration.seconds(30))
    // Send the activity to the other worker's queue instead of this workflow's own.
    |> workflow.with_task_queue(billing_task_queue)
  use charge <- result.try(workflow.execute_activity(
    ctx,
    charge_customer(),
    #(customer_id, 1500),
    options,
  ))
  Ok(charge.transaction_id)
}

/// Stands in for the TypeScript worker: an activity-only worker on the billing queue.
fn start_billing_worker(client: Client) -> Result(worker.Worker, client.Error) {
  worker.new(client, billing_task_queue)
  |> worker.with_activities([
    activity.handler(charge_customer(), fn(_ctx, arguments) {
      let #(customer_id, amount_cents) = arguments
      Ok(Charge(
        transaction_id: "txn-" <> customer_id,
        amount_cents:,
        receipt_url: None,
      ))
    }),
  ])
  |> worker.start
}

pub fn run(client: Client) -> Result(String, client.Error) {
  use billing <- result.try(start_billing_worker(client))
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(renew_subscription(), renew_subscription_workflow),
    ])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-interop", task_queue:)
  let transaction =
    client.execute(client, renew_subscription(), "customer-42", options)
  worker.stop(worker)
  worker.stop(billing)
  transaction
}
