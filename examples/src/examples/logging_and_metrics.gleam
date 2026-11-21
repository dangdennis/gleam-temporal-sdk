//// Logging and metrics: structured log lines from workflows and activities, and the
//// SDK's metrics served for Prometheus.
////
//// Logs go through Erlang's `logger` with the identity of the workflow or activity as
//// metadata, plus the fields you add, in one map under the metadata key `fields`. A
//// workflow logs nothing on replay, so each line appears once however often the workflow
//// is replayed. Never log from workflow code with `io.println`: it would print again on
//// every replay.
////
//// Metrics have one constraint: `telemetry.serve_prometheus` must be called once, before
//// the first `client.connect` in the VM. Call it first thing in `main`, as `main` here
//// does; calling it later returns `Error(telemetry.AlreadyStarted)`. The endpoint has no
//// authentication, so bind an address only the scraper can reach: loopback here; see
//// `temporal/telemetry` for Kubernetes and virtual machines.

import gleam/erlang/process
import gleam/io
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/log
import temporal/telemetry
import temporal/worker
import temporal/workflow

const task_queue = "examples-logging-and-metrics"

pub fn ship_order() {
  workflow.Definition("ship_order", codec.string(), codec.int())
}

pub fn count_parcels() {
  activity.Definition("count_parcels", codec.string(), codec.int())
}

fn ship_order_workflow(
  ctx: workflow.Context,
  order_id: String,
) -> Result(Int, Failure) {
  // The workflow's id, run id, type and task queue are attached automatically.
  workflow.log_info(ctx, "shipping", [log.string("order_id", order_id)])
  let options = workflow.activity_options(start_to_close: duration.seconds(10))
  use parcels <- result.try(workflow.execute_activity(
    ctx,
    count_parcels(),
    order_id,
    options,
  ))
  // Field values keep their types, and reach the log handler in the `fields` map, keyed
  // by name: `fields => #{<<"order_id">> => <<"A-1">>, <<"parcels">> => 3, ...}`.
  workflow.log_info(ctx, "shipped", [
    log.string("order_id", order_id),
    log.int("parcels", parcels),
    log.bool("express", False),
  ])
  Ok(parcels)
}

fn count_parcels_activity(
  ctx: activity.Context,
  order_id: String,
) -> Result(Int, Failure) {
  // The activity's id and type, its workflow's id and the attempt are attached.
  activity.log_debug(ctx, "counting parcels", [log.string("order_id", order_id)])
  activity.log_warning(ctx, "one parcel is oversized", [
    log.float("kilograms", 31.5),
  ])
  Ok(3)
}

pub fn run(client: Client) -> Result(Int, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(ship_order(), ship_order_workflow),
    ])
    |> worker.with_activities([
      activity.handler(count_parcels(), count_parcels_activity),
    ])
    |> worker.start,
  )
  let options =
    client.start_options(
      workflow_id: "examples-logging-and-metrics",
      task_queue:,
    )
  let parcels = client.execute(client, ship_order(), "order-1", options)
  worker.stop(worker)
  parcels
}

/// Serve metrics, then connect and run. `address` is an IP address and port such as
/// `127.0.0.1:9464`; the metrics are at `http://<address>/metrics`.
pub fn run_with_metrics(address: String) -> Result(Int, String) {
  // First, before anything else touches the SDK.
  use serving <- result.try(
    telemetry.serve_prometheus(address)
    |> result.map_error(fn(error) {
      case error {
        telemetry.AlreadyStarted -> "metrics must be set up before connecting"
        telemetry.InvalidAddress(message) -> message
        telemetry.BindFailed(message) -> message
      }
    }),
  )
  io.println("serving metrics at http://" <> serving <> "/metrics")
  use client <- result.try(
    client.connect(client.local()) |> result.replace_error("could not connect"),
  )
  run(client) |> result.replace_error("the workflow did not complete")
}

/// Run with metrics on port 9464 and keep serving them until the VM is stopped.
pub fn main() -> Nil {
  let assert Ok(_) = run_with_metrics("127.0.0.1:9464")
  process.sleep_forever()
}
