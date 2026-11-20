//// The code of docs/guide.md, kept compiling and running so that the guide stays true.

import gleam/option.{Some}
import gleam/result
import gleam/time/duration
import support.{unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/log
import temporal/retry
import temporal/schema
import temporal/testing
import temporal/worker
import temporal/workflow
import temporal/workflow/signal

pub type Order {
  Order(id: String, amount: Int)
}

fn order_codec() {
  schema.codec(schema.object2(
    Order,
    schema.field("id", schema.string(), fn(order: Order) { order.id }),
    schema.field("amount", schema.int(), fn(order: Order) { order.amount }),
  ))
}

fn process_order() {
  workflow.Definition("process_order", order_codec(), codec.string())
}

fn charge() {
  activity.Definition("charge", order_codec(), codec.string())
}

fn refund() {
  activity.Definition("refund", codec.string(), codec.nil())
}

fn shipped() {
  workflow.SignalDefinition("shipped", codec.string())
}

fn process_order_workflow(
  ctx: workflow.Context,
  order: Order,
) -> Result(String, Failure) {
  let options =
    workflow.activity_options(start_to_close: duration.seconds(30))
    |> workflow.with_retry(retry.policy(maximum_attempts: 5))
  use charge_id <- result.try(workflow.execute_activity(
    ctx,
    charge(),
    order,
    options,
  ))
  workflow.log_info(ctx, "charged", [log.string("charge_id", charge_id)])
  case
    workflow.await_timeout(ctx, signal.next(ctx, shipped()), duration.hours(24))
  {
    Ok(tracking) -> Ok(tracking)
    Error(_) -> {
      use _ <- result.try(
        workflow.non_cancellable(ctx, fn(shielded) {
          workflow.execute_activity(shielded, refund(), charge_id, options)
        }),
      )
      Error(
        failure.new("order was not shipped in time")
        |> failure.with_type("NotShipped"),
      )
    }
  }
}

@external(erlang, "temporal_test_ffi", "test_server_path")
fn test_server_path() -> Result(String, Nil)

pub fn the_guides_order_workflow_ships_or_refunds_test() {
  let assert Ok(env) = case test_server_path() {
    Ok(path) -> testing.start_time_skipping_with(executable: path)
    Error(Nil) -> testing.start_time_skipping()
  }
  let task_queue = "orders-" <> unique()
  let refunds = "refunded:" <> task_queue
  let assert Ok(worker) =
    worker.new(testing.client(env), task_queue)
    |> worker.with_workflows([
      workflow.handler(process_order(), process_order_workflow),
    ])
    |> worker.with_activities([
      activity.handler(charge(), fn(_, _) { Ok("charge-1") }),
      activity.handler(refund(), fn(_, charge_id) {
        Ok(support.record(refunds, charge_id))
      }),
    ])
    |> worker.start
  let options = fn(id) {
    client.start_options(workflow_id: task_queue <> id, task_queue:)
  }

  // Shipped in time.
  let assert Ok(handle) =
    client.start(
      testing.client(env),
      process_order(),
      Order("1", 500),
      options("-1"),
    )
  let assert Ok(Nil) = client.signal(handle, shipped(), "1Z999")
  assert testing.result(env, handle) == Ok("1Z999")
  assert support.recorded(refunds) == ""

  // Never shipped: the day passes at once, and the charge is refunded.
  let assert Error(client.WorkflowFailed(failure)) =
    testing.execute(env, process_order(), Order("2", 500), options("-2"))
  assert failure.error_type == Some("NotShipped")
  assert support.recorded(refunds) == "charge-1"

  worker.stop(worker)
  testing.stop(env)
}
