//// Middleware and headers: code that runs around every workflow or activity on a worker,
//// rewrites what workflows schedule, and context that travels beside the arguments.
////
//// Use middleware for what every workflow or activity needs and none should repeat:
//// logging, error reporting, tenancy, tracing. Headers carry the context: set when a
//// workflow is started, they reach the workflow and are passed on to the activities and
//// child workflows it starts.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}
import temporal/log
import temporal/middleware
import temporal/worker
import temporal/workflow

const task_queue = "examples-middleware"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// The workflow's result: the tenant as the workflow and as the activity saw it.
    result: String,
    /// The activity types the activity middleware saw.
    activities: List(String),
  )
}

pub fn notify() {
  workflow.Definition("notify", codec.string(), codec.string())
}

pub fn send_email() {
  activity.Definition("send_email", codec.string(), codec.string())
}

/// The implementation the outbound middleware redirects `send_email` to.
pub fn send_email_v2() {
  activity.Definition("send_email_v2", codec.string(), codec.string())
}

/// A header's value as a string, or `"none"`.
fn header(headers: Dict(String, Payload), name: String) -> String {
  dict.get(headers, name)
  |> result.try(fn(payload) {
    codec.decode(codec.string(), payload) |> result.replace_error(Nil)
  })
  |> result.unwrap("none")
}

fn notify_workflow(
  ctx: workflow.Context,
  user: String,
) -> Result(String, Failure) {
  let tenant = header(workflow.headers(ctx), "tenant")
  let options = workflow.activity_options(start_to_close: duration.seconds(10))
  // Scheduled as `send_email`; the outbound middleware decides what really runs.
  use sent <- result.try(workflow.execute_activity(
    ctx,
    send_email(),
    user,
    options,
  ))
  Ok("workflow saw " <> tenant <> "; " <> sent)
}

fn send_email_v2_activity(
  ctx: activity.Context,
  user: String,
) -> Result(String, Failure) {
  // The workflow's headers were passed on without the workflow doing anything.
  let tenant = header(activity.info(ctx).headers, "tenant")
  Ok("activity saw " <> tenant <> " for " <> user)
}

/// Workflow middleware: runs around every workflow on the worker. It is workflow code, so
/// it must be deterministic; side effects go through `workflow.unless_replaying`, and
/// workflow logging is already silent on replay.
fn log_outcome(ctx: workflow.Context, next) {
  let outcome = next()
  workflow.log_info(ctx, "workflow finished", [
    log.bool("ok", result.is_ok(outcome)),
  ])
  outcome
}

/// Activity middleware: runs around every activity on the worker, and may do anything.
fn record_activity(seen: Subject(String)) -> activity.Middleware {
  fn(ctx, next) {
    process.send(seen, activity.info(ctx).activity_type)
    next()
  }
}

/// Outbound middleware: rewrites an activity as the workflow schedules it. It runs inside
/// workflow code and its answer is recorded in history, so it must keep giving the same
/// answer for as long as those histories can be replayed.
fn redirect(call: middleware.ActivityCall) -> middleware.ActivityCall {
  case call.activity_type {
    "send_email" ->
      middleware.ActivityCall(..call, activity_type: "send_email_v2")
    _ -> call
  }
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  let seen = process.new_subject()
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(notify(), notify_workflow)])
    |> worker.with_activities([
      activity.handler(send_email_v2(), send_email_v2_activity),
    ])
    |> worker.with_workflow_middleware(log_outcome)
    |> worker.with_activity_middleware(record_activity(seen))
    |> worker.with_outbound_activity(redirect)
    |> worker.start,
  )
  let options =
    client.start_options(workflow_id: "examples-middleware", task_queue:)
    |> client.with_headers(
      dict.from_list([#("tenant", codec.encode(codec.string(), "acme"))]),
    )
  let outcome = {
    use result <- result.try(client.execute(client, notify(), "ada", options))
    Ok(Outcome(result:, activities: drain(seen, [])))
  }
  worker.stop(worker)
  outcome
}

fn drain(seen: Subject(String), found: List(String)) -> List(String) {
  case process.receive(seen, 0) {
    Ok(item) -> drain(seen, [item, ..found])
    Error(Nil) -> list.reverse(found)
  }
}
