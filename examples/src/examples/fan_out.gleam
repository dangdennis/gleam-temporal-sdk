//// Parallel fan-out: start many activities, then wait for all of them.
////
//// Starting returns a future at once, so the activities run concurrently; only the
//// waiting is sequential. Use `try_await_all` when one failure should fail the lot, and
//// `await_all` when each result is handled by itself.

import gleam/int
import gleam/list
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/retry
import temporal/schema
import temporal/worker
import temporal/workflow

const task_queue = "examples-fan-out"

pub type Summary {
  Summary(total: Int, resized: Int, failed: Int)
}

fn summary_codec() {
  schema.codec(schema.object3(
    Summary,
    schema.field("total", schema.int(), fn(s: Summary) { s.total }),
    schema.field("resized", schema.int(), fn(s: Summary) { s.resized }),
    schema.field("failed", schema.int(), fn(s: Summary) { s.failed }),
  ))
}

pub fn process_batch() {
  workflow.Definition(
    "process_batch",
    schema.codec(schema.list(schema.int())),
    summary_codec(),
  )
}

pub fn measure() {
  activity.Definition("measure", codec.int(), codec.int())
}

/// Fails for every third image.
pub fn resize() {
  activity.Definition("resize", codec.int(), codec.nil())
}

fn process_batch_workflow(
  ctx: workflow.Context,
  images: List(Int),
) -> Result(Summary, Failure) {
  let options =
    workflow.activity_options(start_to_close: duration.seconds(10))
    |> workflow.with_retry(retry.policy(maximum_attempts: 1))

  // All or nothing: the values in the order given, or the first failure.
  let measuring =
    list.map(images, workflow.start_activity(ctx, measure(), _, options))
  use sizes <- result.try(workflow.try_await_all(measuring))

  // Each by itself: every result, failed or not, in the order given.
  let resizing =
    list.map(images, workflow.start_activity(ctx, resize(), _, options))
  let #(resized, failed) = result.partition(workflow.await_all(resizing))

  Ok(Summary(
    total: int.sum(sizes),
    resized: list.length(resized),
    failed: list.length(failed),
  ))
}

fn measure_activity(
  _ctx: activity.Context,
  image: Int,
) -> Result(Int, Failure) {
  Ok(image * 100)
}

fn resize_activity(_ctx: activity.Context, image: Int) -> Result(Nil, Failure) {
  case image % 3 {
    0 -> Error(failure.new("image is corrupt"))
    _ -> Ok(Nil)
  }
}

pub fn run(client: Client) -> Result(Summary, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(process_batch(), process_batch_workflow),
    ])
    |> worker.with_activities([
      activity.handler(measure(), measure_activity),
      activity.handler(resize(), resize_activity),
    ])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-fan-out", task_queue:)
  let summary =
    client.execute(client, process_batch(), [1, 2, 3, 4, 5, 6], options)
  worker.stop(worker)
  summary
}
