//// Settings of the SDK's runtime that can only be seen from outside it: how many polls a
//// worker keeps open, in its metrics, and what the core library logs, on standard error.
//// Both are fixed when the runtime starts, so each scenario runs in a node of its own.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/string
import gleam/time/duration
import support.{cmd, eventually, unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/telemetry
import temporal/worker
import temporal/workflow

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn doubling() {
  workflow.Definition("doubling", codec.int(), codec.int())
}

fn config(task_queue: String) -> worker.Config {
  worker.new(support.local_client(), task_queue)
  |> worker.with_workflows([
    workflow.handler(doubling(), fn(ctx, n) {
      workflow.execute_activity(
        ctx,
        double(),
        n,
        workflow.activity_options(start_to_close: duration.seconds(10)),
      )
    }),
  ])
  |> worker.with_activities([activity.handler(double(), fn(_, n) { Ok(n * 2) })])
}

/// Run `tuning_test:<call>` in a node of its own, in the background, and return its
/// operating system process id.
fn spawn_node(call: String) -> String {
  cmd(
    "erl -noshell -pa build/dev/erlang/*/ebin -eval 'tuning_test:"
    <> call
    <> "' >/dev/null 2>&1 & echo $!",
  )
  |> string.trim
}

/// Entry point of a node that serves metrics and runs three workers: one as it comes,
/// one tuned, and one that asks for more polls than it has slots for, and for fewer
/// workflow polls than a caching worker needs.
pub fn polling_workers(port: String, task_queue: String) -> Nil {
  let assert Ok(_) = telemetry.serve_prometheus("127.0.0.1:" <> port)
  let assert Ok(_) = config(task_queue <> "-default") |> worker.start
  let assert Ok(_) =
    config(task_queue <> "-tuned")
    |> worker.with_max_concurrent_workflow_task_polls(20)
    |> worker.with_max_concurrent_activity_task_polls(7)
    |> worker.start
  let assert Ok(_) =
    config(task_queue <> "-clamped")
    |> worker.with_max_concurrent_activities(3)
    |> worker.with_max_concurrent_workflow_task_polls(1)
    |> worker.start
  process.sleep_forever()
}

/// The polls a worker has open, from core's `num_pollers` gauge: workflow tasks, on the
/// task queue and on the worker's own sticky queue together, and activity tasks.
fn open_polls(metrics: String, task_queue: String) -> #(Int, Int) {
  let count = fn(poller_type) {
    string.split(metrics, "\n")
    |> list.filter(fn(line) {
      string.starts_with(line, "temporal_num_pollers{")
      && string.contains(line, "poller_type=\"" <> poller_type <> "\"")
      && string.contains(line, "task_queue=\"" <> task_queue <> "\"")
    })
    |> list.filter_map(fn(line) {
      string.split(line, " ") |> list.last |> result_to_int
    })
    |> int.sum
  }
  #(
    count("workflow_task") + count("sticky_workflow_task"),
    count("activity_task"),
  )
}

fn result_to_int(text: Result(String, Nil)) -> Result(Int, Nil) {
  case text {
    Ok(text) -> int.parse(text)
    Error(Nil) -> Error(Nil)
  }
}

pub fn a_worker_keeps_as_many_polls_open_as_it_is_set_to_test() {
  let port = "19465"
  let task_queue = "polls-" <> unique()
  let node =
    spawn_node(
      "polling_workers(<<\"" <> port <> "\">>, <<\"" <> task_queue <> "\">>)",
    )
  let polls = fn(suffix) {
    open_polls(
      cmd("curl -s http://127.0.0.1:" <> port <> "/metrics"),
      task_queue <> suffix,
    )
  }
  // Ten of each unless told otherwise, as Temporal's TypeScript SDK has it.
  let default = eventually(fn() { polls("-default") == #(10, 10) })
  let tuned = eventually(fn() { polls("-tuned") == #(20, 7) })
  // Never more polls than slots for what they return, and never fewer workflow polls
  // than the two a worker with a cache needs.
  let clamped = eventually(fn() { polls("-clamped") == #(2, 3) })
  let seen = #(polls("-default"), polls("-tuned"), polls("-clamped"))
  let _ = cmd("kill -9 " <> node)
  assert #(default, tuned, clamped, seen)
    == #(True, True, True, #(#(10, 10), #(20, 7), #(2, 3)))
}

fn crashing() {
  workflow.Definition("crashing", codec.nil(), codec.nil())
}

/// Entry point of a node whose only workflow crashes, which makes core fail the workflow
/// task and warn about it. `level` is the core log level to ask for first, or `default`.
pub fn crashing_worker(task_queue: String, level: String) -> Nil {
  let assert Ok(Nil) = case level {
    "off" -> telemetry.set_core_log_level(telemetry.CoreOff)
    "debug" -> telemetry.set_core_log_level(telemetry.CoreDebug)
    _ -> Ok(Nil)
  }
  let client = support.local_client()
  let assert Ok(_) =
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(crashing(), fn(_, _) { panic as "a bug" }),
    ])
    |> worker.start
  let assert Ok(_) =
    client.start(
      client,
      crashing(),
      Nil,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
  process.sleep_forever()
}

/// What a node running `crashing_worker` writes to standard error in its first seconds.
fn standard_error(level: String) -> String {
  let task_queue = "core-log-" <> level <> "-" <> unique()
  let written =
    cmd(
      "timeout 4 erl -noshell -pa build/dev/erlang/*/ebin -eval 'tuning_test:crashing_worker(<<\""
      <> task_queue
      <> "\">>, <<\""
      <> level
      <> "\">>)' 2>&1 >/dev/null",
    )
  let _ =
    cmd("temporal workflow terminate --workflow-id " <> task_queue <> " 2>&1")
  written
}

pub fn core_warnings_are_printed_on_standard_error_test() {
  let written = standard_error("default")
  assert string.contains(written, "WARN")
  assert string.contains(written, "Failing workflow task")
  // Warnings and up, no more.
  assert !string.contains(written, "DEBUG")
  assert !string.contains(written, "INFO")
}

pub fn the_core_log_level_can_be_set_before_the_runtime_starts_test() {
  assert standard_error("off") == ""
  let chatty = standard_error("debug")
  assert string.contains(chatty, "Failing workflow task")
  assert string.contains(chatty, "DEBUG")
}

pub fn the_core_log_level_cannot_be_set_once_the_runtime_has_started_test() {
  let _ = support.local_client()
  assert telemetry.set_core_log_level(telemetry.CoreDebug)
    == Error(telemetry.AlreadyStarted)
}
