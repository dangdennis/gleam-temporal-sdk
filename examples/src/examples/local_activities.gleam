//// Local activities: activities run inside the workflow's own worker, without a round
//// trip through the server's task queue.
////
//// Use one for short work, a few seconds at most, where the latency of a normal activity
//// matters. A local activity cannot heartbeat, must be registered on the worker that runs
//// the workflow, and runs again from the start if that worker dies: it must be safe to
//// repeat.

import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/retry
import temporal/worker
import temporal/workflow
import temporal/workflow/local_activity

const task_queue = "examples-local-activities"

pub fn convert() {
  workflow.Definition(
    "convert",
    codec.args2(codec.int(), codec.string()),
    codec.int(),
  )
}

/// The exchange rate for a currency, in hundredths.
pub fn lookup_rate() {
  activity.Definition("lookup_rate", codec.string(), codec.int())
}

fn convert_workflow(
  ctx: workflow.Context,
  input: #(Int, String),
) -> Result(Int, Failure) {
  let #(cents, currency) = input
  let options =
    local_activity.options(start_to_close: duration.seconds(2))
    |> local_activity.with_retry(retry.policy(maximum_attempts: 3))
  // The same definition and handler as a normal activity; only the call differs.
  use rate <- result.try(local_activity.execute(
    ctx,
    lookup_rate(),
    currency,
    options,
  ))
  Ok(cents * rate / 100)
}

fn lookup_rate_activity(
  _ctx: activity.Context,
  currency: String,
) -> Result(Int, Failure) {
  case currency {
    "EUR" -> Ok(92)
    "GBP" -> Ok(79)
    _ ->
      Error(
        failure.new("no rate for " <> currency)
        |> failure.with_type("UnknownCurrency")
        |> failure.non_retryable,
      )
  }
}

pub fn run(client: Client) -> Result(Int, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(convert(), convert_workflow)])
    |> worker.with_activities([
      activity.handler(lookup_rate(), lookup_rate_activity),
    ])
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-local-activities", task_queue:)
  let converted = client.execute(client, convert(), #(1000, "EUR"), options)
  worker.stop(worker)
  converted
}
