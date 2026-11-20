//// Timers and deadlines: `sleep`, `with_timeout`, `await_timeout` and racing futures.
////
//// Workflow timers are durable: they keep counting while no worker is running, and a
//// month costs the same as a millisecond. Never use `process.sleep` in workflow code.

import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/schema
import temporal/worker
import temporal/workflow
import temporal/workflow/signal

const task_queue = "examples-timers"

/// What each part of the workflow observed.
pub type Report {
  Report(slept_ms: Int, bounded: String, awaited: String, winner: String)
}

fn report_codec() {
  schema.codec(schema.object4(
    Report,
    schema.field("sleptMs", schema.int(), fn(r: Report) { r.slept_ms }),
    schema.field("bounded", schema.string(), fn(r: Report) { r.bounded }),
    schema.field("awaited", schema.string(), fn(r: Report) { r.awaited }),
    schema.field("winner", schema.string(), fn(r: Report) { r.winner }),
  ))
}

pub fn timers() {
  workflow.Definition("timers", codec.nil(), report_codec())
}

pub fn wake() {
  workflow.SignalDefinition("wake", codec.nil())
}

fn timers_workflow(ctx: workflow.Context, _: Nil) -> Result(Report, Failure) {
  // sleep: wait, then carry on. Workflow time comes from `workflow.now`.
  let before = workflow.now(ctx)
  use _ <- result.try(workflow.sleep(ctx, duration.milliseconds(200)))
  let slept =
    timestamp.difference(before, workflow.now(ctx))
    |> duration.to_milliseconds

  // with_timeout: give a piece of work a deadline. Whatever the body started with
  // `scope` is cancelled when the deadline passes.
  let bounded = case
    workflow.with_timeout(ctx, duration.milliseconds(200), fn(scope) {
      workflow.sleep(scope, duration.hours(1))
    })
  {
    Ok(Nil) -> "finished"
    Error(failed) ->
      case failure.is_timeout(failed) {
        True -> "timed out"
        False -> "cancelled"
      }
  }

  // await_timeout: wait for a future, here a signal, but not for ever.
  let awaited = case
    workflow.await_timeout(
      ctx,
      signal.next(ctx, wake()),
      duration.milliseconds(200),
    )
  {
    Ok(Nil) -> "woken"
    Error(_) -> "nobody called"
  }

  // race: the first future to resolve wins. The scope cancels the loser, so its timer
  // does not stay behind in the workflow.
  let winner =
    workflow.with_scope(ctx, fn(scope) {
      let fast =
        workflow.timer(scope, duration.milliseconds(100))
        |> workflow.map(fn(_) { "fast" })
      let slow =
        workflow.timer(scope, duration.hours(1))
        |> workflow.map(fn(_) { "slow" })
      let winner = workflow.await(workflow.race([fast, slow]))
      workflow.cancel(scope)
      winner
    })

  Ok(Report(slept_ms: slept, bounded:, awaited:, winner:))
}

pub fn run(client: Client) -> Result(Report, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(timers(), timers_workflow)])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-timers", task_queue:)
  let report = client.execute(client, timers(), Nil, options)
  worker.stop(worker)
  report
}
