//// The smallest complete program: one workflow that runs one activity, a worker that
//// serves both, and a client that starts the workflow and waits for its result.
////
//// Start here. Every other example has this shape with one thing added.

import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow

const task_queue = "examples-hello"

// A definition names a workflow or activity and fixes its input and output types. The
// code that implements one and the code that calls it share the definition.

pub fn greet() {
  workflow.Definition("greet", codec.string(), codec.string())
}

pub fn say_hello() {
  activity.Definition("say_hello", codec.string(), codec.string())
}

/// Workflow code decides what happens and in which order. It must be deterministic.
fn greet_workflow(
  ctx: workflow.Context,
  name: String,
) -> Result(String, Failure) {
  let options = workflow.activity_options(start_to_close: duration.seconds(10))
  workflow.execute_activity(ctx, say_hello(), name, options)
}

/// Activity code does the work. It is ordinary Gleam and may do anything.
fn say_hello_activity(
  _ctx: activity.Context,
  name: String,
) -> Result(String, Failure) {
  Ok("Hello, " <> name <> "!")
}

pub fn run(client: Client) -> Result(String, client.Error) {
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(greet(), greet_workflow)])
    |> worker.with_activities([
      activity.handler(say_hello(), say_hello_activity),
    ])
    |> worker.start,
  )
  let options = client.start_options(workflow_id: "examples-hello", task_queue:)
  let greeting = client.execute(client, greet(), "Gleam", options)
  worker.stop(worker)
  greeting
}
