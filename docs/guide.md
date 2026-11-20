# Your first workflow

This walks through a small order workflow: charge a card, wait for a shipping confirmation
for up to a day, and refund if it never comes. It assumes you know Gleam and have heard of
Temporal.

## Set up

Add the package to `gleam.toml` and download its native library:

```toml
[dependencies]
temporal_gleam = { git = "https://github.com/dangdennis/gleam-temporal-sdk", ref = "v0.2.0" }
```

```sh
gleam deps download
gleam run -m temporal/install   # a prebuilt library; no Rust toolchain needed
temporal server start-dev       # a local Temporal service, from the Temporal CLI
```

## Describe what exists

A definition names a workflow, activity or signal and fixes the types it sends and
receives. The code that implements one and the code that calls it share the definition, so
a mismatch is a compile error. Definitions are functions because Gleam constants cannot
call the codec functions.

Values travel as JSON. A record is described once, with `temporal/schema`, and that one
description both writes and reads it.

```gleam
import temporal/activity
import temporal/codec
import temporal/schema
import temporal/workflow

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

pub fn process_order() {
  workflow.Definition("process_order", order_codec(), codec.string())
}

pub fn charge() {
  activity.Definition("charge", order_codec(), codec.string())
}

pub fn refund() {
  activity.Definition("refund", codec.string(), codec.nil())
}

pub fn shipped() {
  workflow.SignalDefinition("shipped", codec.string())
}
```

## Write the activities

Activities are where side effects happen. They are ordinary Gleam: call an API, write to a
database, anything. Return an `Error` to fail; the server retries according to the retry
policy the workflow asked for.

```gleam
import temporal/failure.{type Failure}

fn charge_activity(_ctx: activity.Context, order: Order) -> Result(String, Failure) {
  case payments.charge(order.id, order.amount) {
    Ok(charge_id) -> Ok(charge_id)
    Error(payments.CardDeclined) ->
      // Not worth retrying.
      Error(failure.new("card declined") |> failure.with_type("CardDeclined") |> failure.non_retryable)
    Error(payments.Unavailable) -> Error(failure.new("payment service unavailable"))
  }
}
```

## Write the workflow

A workflow is a function that the server can replay: it runs again from the start, against
its recorded history, whenever a worker needs to rebuild its state. So it must make the
same requests in the same order every time. Take time, randomness and waiting from
`temporal/workflow`, and do everything else in activities.

Options are built the same way everywhere: a constructor that takes what is required, then
a `with_` function for each thing to change.

```gleam
import gleam/result
import gleam/time/duration
import temporal/log
import temporal/retry
import temporal/workflow/signal

fn process_order_workflow(ctx: workflow.Context, order: Order) -> Result(String, Failure) {
  let options =
    workflow.activity_options(start_to_close: duration.seconds(30))
    |> workflow.with_retry(retry.policy(maximum_attempts: 5))
  use charge_id <- result.try(workflow.execute_activity(ctx, charge(), order, options))
  // Logged once, however often the workflow is replayed.
  workflow.log_info(ctx, "charged", [log.string("charge_id", charge_id)])

  // Wait up to a day for the warehouse to say it shipped.
  case workflow.await_timeout(ctx, signal.next(ctx, shipped()), duration.hours(24)) {
    Ok(tracking) -> Ok(tracking)
    Error(_) -> {
      // Timed out, or the workflow was cancelled. Refund even if it was cancelled.
      use _ <- result.try(
        workflow.non_cancellable(ctx, fn(shielded) {
          workflow.execute_activity(shielded, refund(), charge_id, options)
        }),
      )
      Error(failure.new("order was not shipped in time") |> failure.with_type("NotShipped"))
    }
  }
}
```

That day-long wait costs nothing: the workflow is not running while it waits, and it
survives deploys and crashes.

## Run a worker

A worker polls a task queue and runs what it is given. `worker.run` keeps it going until
the operating system asks the program to stop with SIGTERM, as every deploy does, then
lets the activities in flight finish and returns.

```gleam
import temporal/client
import temporal/worker

pub fn main() {
  let assert Ok(client) = client.connect(client.local())
  let assert Ok(Nil) =
    worker.new(client, "orders")
    |> worker.with_workflows([workflow.handler(process_order(), process_order_workflow)])
    |> worker.with_activities([
      activity.handler(charge(), charge_activity),
      activity.handler(refund(), refund_activity),
    ])
    |> worker.run
}
```

Do not keep a worker alive with `process.sleep_forever()`: the VM stops within
milliseconds of a SIGTERM, and every activity in flight is abandoned until its timeout.
In an OTP application, add `worker.supervised(config)` to the supervision tree instead;
the README's [Running a worker](../README.md#running-a-worker) says what a graceful stop
does, step by step.

## Start a workflow

```gleam
let options = client.start_options(workflow_id: "order-" <> order.id, task_queue: "orders")
let assert Ok(handle) = client.start(client, process_order(), order, options)

// Later, from anywhere that knows the order id:
let handle = client.handle(client, process_order(), "order-" <> order.id)
let assert Ok(Nil) = client.signal(handle, shipped(), "1Z999AA10123456784")
let assert Ok(tracking) = client.result(handle)
```

The workflow id is the order's identity: starting a second workflow with the same id while
the first is running is refused, which makes the start safe to retry.

## Test it

Three kinds of test, cheapest first.

An activity is a function; call it with a context from `testing.activity_env`.

A workflow needs a server. `testing.start_time_skipping` starts one whose clock jumps
ahead while workflows only wait, so the day-long timeout takes no time:

```gleam
import temporal/testing

pub fn an_unshipped_order_is_refunded_test() {
  let assert Ok(env) = testing.start_time_skipping()
  let assert Ok(worker) =
    worker.new(testing.client(env), "orders")
    |> worker.with_workflows([workflow.handler(process_order(), process_order_workflow)])
    |> worker.with_activities([
      activity.handler(charge(), fn(_, _) { Ok("charge-1") }),
      activity.handler(refund(), fn(_, _) { Ok(Nil) }),
    ])
    |> worker.start
  let options = client.start_options(workflow_id: "order-1", task_queue: "orders")
  let assert Error(client.WorkflowFailed(failure)) =
    testing.execute(env, process_order(), Order("1", 500), options)
  assert failure.error_type == Some("NotShipped")
  worker.stop(worker)
  testing.stop(env)
}
```

Before deploying a change to workflow code, replay histories of workflows that are still
running against it. `client.history` fetches one; keep a few as fixtures.

```gleam
import temporal/replayer

pub fn the_new_code_can_continue_running_orders_test() {
  let assert Ok(encoded) = simplifile.read_bits("test/fixtures/order-1234.history")
  let history = client.History(workflow_id: "order-1234", encoded:)
  assert replayer.replay([workflow.handler(process_order(), process_order_workflow)], history)
    == Ok(Nil)
}
```

If the replay reports nondeterminism, the change would break running workflows. Guard it
with `workflow.patched`, or pin workflows to the build they started on with worker
versioning.

## Where to go next

The README lists everything else: child workflows, updates, local activities, schedules,
Nexus, encryption, middleware, metrics, and what the worker does when things go wrong.
