//// What a worker does when things around it go wrong or go away: an activity's process
//// dies or hangs, the worker is stopped, the operating system asks the VM to stop. Also
//// that the SDK's log lines can be seen by someone who has configured nothing.

import gleam/erlang/process.{type Pid}
import gleam/int
import gleam/list
import gleam/option.{Some}
import gleam/otp/static_supervisor
import gleam/result
import gleam/string
import gleam/time/duration
import support.{cmd, eventually, read_file, record, recorded, unique, write_file}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/log
import temporal/retry
import temporal/testing
import temporal/worker
import temporal/workflow.{type Context}

@external(erlang, "temporal_test_ffi", "record_self")
fn record_self(key: String) -> Nil

@external(erlang, "temporal_test_ffi", "recorded_pid")
fn recorded_pid(key: String) -> Result(Pid, Nil)

@external(erlang, "temporal_test_ffi", "os_pid")
fn os_pid() -> String

@external(erlang, "temporal_test_ffi", "quietly")
fn quietly(body: fn() -> Nil) -> Nil

@external(erlang, "temporal_test_ffi", "start_application")
fn start_application(name: String, start_supervisor: fn() -> Pid) -> Nil

@external(erlang, "temporal_ffi", "system_time_ms")
fn now_ms() -> Int

/// How many workers the SIGTERM handler would drain.
@external(erlang, "temporal_test_ffi", "workers_drained_on_sigterm")
fn workers_drained_on_sigterm() -> Int

@external(erlang, "temporal_test_ffi", "signal_handlers")
fn signal_handlers() -> String

@external(erlang, "temporal_test_ffi", "capture_log_metadata")
fn capture_log_metadata(body: fn() -> Nil) -> List(#(String, List(String)))

/// Run `readiness_test:<call>` in a node of its own, in the background, with its output
/// and, once it has exited, its exit status under `build/readiness/<name>`.
fn spawn_node(name: String, call: String) -> Nil {
  let _ =
    cmd("mkdir -p build/readiness && rm -f build/readiness/" <> name <> ".*")
  let _ =
    cmd(
      "(ERL_CRASH_DUMP_BYTES=0 erl -noshell -pa build/dev/erlang/*/ebin -eval 'readiness_test:"
      <> call
      <> "' >build/readiness/"
      <> name
      <> ".out 2>&1; echo $? >build/readiness/"
      <> name
      <> ".status) >/dev/null 2>&1 &",
    )
  Nil
}

fn node_file(name: String, extension: String) -> String {
  read_file("build/readiness/" <> name <> "." <> extension)
  |> result.try(fn(bytes) {
    bit_array_to_string(bytes) |> result.replace_error("not text")
  })
  |> result.unwrap("")
  |> string.trim
}

@external(erlang, "gleam@bit_array", "to_string")
fn bit_array_to_string(bytes: BitArray) -> Result(String, Nil)

// ---- logging ----

/// Entry point of a node that logs through the SDK with `logger` as it comes.
pub fn log_lines() -> Nil {
  let ctx =
    testing.activity_context(testing.activity_env(testing.activity_info()))
  bridge.log(bridge.Error, "temporal worker: polling failed", [], [])
  activity.log_warning(ctx, "card declined", [log.string("reason", "expired")])
  activity.log_error(ctx, "charge failed", [])
  // Below the level `logger` starts with, like any other info line.
  activity.log_info(ctx, "charged", [])
  process.sleep(300)
}

pub fn the_sdks_log_lines_reach_the_default_logger_handler_test() {
  let output =
    cmd(
      "erl -noshell -pa build/dev/erlang/*/ebin -eval 'readiness_test:log_lines(), halt(0).' 2>&1",
    )
  assert string.contains(output, "temporal worker: polling failed")
  assert string.contains(output, "card declined")
  assert string.contains(output, "charge failed")
  assert !string.contains(output, "charged")
}

// ---- activity processes ----

fn victim() {
  activity.Definition("victim", codec.string(), codec.nil())
}

/// Says where it runs and then never returns, heartbeats or looks at its cancellation.
fn victim_activity(_: activity.Context, key: String) -> Result(Nil, Failure) {
  record_self(key)
  process.sleep_forever()
  Ok(Nil)
}

fn linker() {
  activity.Definition("linker", codec.nil(), codec.nil())
}

/// Links to a process that crashes, as an activity that uses a connection pool or a port
/// might.
fn linker_activity(_: activity.Context, _: Nil) -> Result(Nil, Failure) {
  process.spawn(fn() {
    process.sleep(100)
    panic as "helper crashed"
  })
  process.sleep_forever()
  Ok(Nil)
}

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn once(start_to_close: duration.Duration) -> workflow.ActivityOptions {
  workflow.activity_options(start_to_close:)
  |> workflow.with_retry(retry.policy(maximum_attempts: 1))
}

fn bereaved() {
  workflow.Definition("bereaved", codec.string(), codec.string())
}

/// Runs the named activity once, then another on the same worker, and returns how the
/// first ended and what the second returned.
fn bereaved_workflow(ctx: Context, first: String) -> Result(String, Failure) {
  let outcome = case first {
    "victim" ->
      workflow.execute_activity(
        ctx,
        victim(),
        workflow.info(ctx).workflow_id,
        once(duration.seconds(60)),
      )
    "hung" ->
      workflow.execute_activity(
        ctx,
        victim(),
        workflow.info(ctx).workflow_id,
        once(duration.seconds(1)),
      )
    _ ->
      workflow.execute_activity(ctx, linker(), Nil, once(duration.seconds(60)))
  }
  let ended = case outcome {
    Ok(Nil) -> "returned"
    Error(failure) ->
      case failure.is_timeout(failure) {
        True -> "timed out"
        False -> failure.root_cause(failure).message
      }
  }
  use doubled <- result.try(workflow.execute_activity(
    ctx,
    double(),
    21,
    workflow.activity_options(start_to_close: duration.seconds(30)),
  ))
  Ok(ended <> ", then " <> int.to_string(doubled))
}

/// A worker with a single activity slot, so that a slot that leaks stops it for good.
fn one_slot_worker(task_queue: String) -> worker.Worker {
  let assert Ok(worker) =
    worker.new(support.local_client(), task_queue)
    |> worker.with_workflows([workflow.handler(bereaved(), bereaved_workflow)])
    |> worker.with_activities([
      activity.handler(victim(), victim_activity),
      activity.handler(linker(), linker_activity),
      activity.handler(double(), fn(_, n) { Ok(n * 2) }),
    ])
    |> worker.with_max_concurrent_activities(1)
    |> worker.with_activity_cancellation_grace(Some(duration.milliseconds(500)))
    |> worker.start
  worker
}

fn start_bereaved(task_queue: String, first: String) -> client.Handle(String) {
  let assert Ok(handle) =
    client.start(
      support.local_client(),
      bereaved(),
      first,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
  handle
}

pub fn a_killed_activity_process_fails_the_activity_and_frees_its_slot_test() {
  use <- quietly
  let task_queue = "killed-" <> unique()
  let worker = one_slot_worker(task_queue)
  let handle = start_bereaved(task_queue, "victim")
  assert eventually(fn() { result.is_ok(recorded_pid(task_queue)) })
  let assert Ok(pid) = recorded_pid(task_queue)
  let started = now_ms()
  process.kill(pid)
  // The failure arrives at once, not when the activity's minute is up, and the worker's
  // only slot is free for the next activity.
  assert client.result(handle)
    == Ok(
      "the activity process exited before reporting a result: killed, then 42",
    )
  assert now_ms() - started < 20_000
  worker.stop(worker)
}

pub fn an_activity_taken_down_by_a_linked_process_fails_and_frees_its_slot_test() {
  use <- quietly
  let task_queue = "linked-" <> unique()
  let worker = one_slot_worker(task_queue)
  let handle = start_bereaved(task_queue, "linker")
  let assert Ok(outcome) = client.result(handle)
  assert string.starts_with(
    outcome,
    "the activity process exited before reporting a result: ",
  )
  assert string.contains(outcome, "helper crashed")
  assert string.ends_with(outcome, ", then 42")
  worker.stop(worker)
}

pub fn a_hung_activity_is_killed_after_its_timeout_and_frees_its_slot_test() {
  let task_queue = "hung-" <> unique()
  let worker = one_slot_worker(task_queue)
  let handle = start_bereaved(task_queue, "hung")
  // The server gives up on the activity after its second. The worker asks it to stop
  // five seconds after that, kills it half a second later, and only then has a slot for
  // the second activity.
  assert client.result(handle) == Ok("timed out, then 42")
  let assert Ok(pid) = recorded_pid(task_queue)
  assert !process.is_alive(pid)
  worker.stop(worker)
}

// ---- stopping ----

fn long() {
  activity.Definition("long", codec.int(), codec.string())
}

/// Runs for `ms` whatever it is told, and notes when it first saw the worker shutting down
/// and when it was asked to cancel.
fn long_activity(ctx: activity.Context, ms: Int) -> Result(String, Failure) {
  let key = activity.info(ctx).workflow_id
  record_self(key)
  record(key <> ":attempts", recorded(key <> ":attempts") <> "x")
  watch_until(ctx, key, now_ms() + ms)
  record(key <> ":finished", int.to_string(now_ms()))
  Ok("attempt " <> int.to_string(activity.info(ctx).attempt))
}

fn watch_until(ctx: activity.Context, key: String, until: Int) -> Nil {
  let note = fn(what) {
    case recorded(key <> ":" <> what) {
      "" -> record(key <> ":" <> what, int.to_string(now_ms()))
      _ -> Nil
    }
  }
  case activity.is_worker_shutting_down(ctx) {
    True -> note("shutting-down")
    False -> Nil
  }
  case activity.cancel_reason(ctx) {
    Some(activity.WorkerShutdown) -> note("cancelled")
    _ -> Nil
  }
  case now_ms() >= until {
    True -> Nil
    False -> {
      process.sleep(20)
      watch_until(ctx, key, until)
    }
  }
}

fn patient() {
  workflow.Definition("patient", codec.int(), codec.string())
}

fn patient_workflow(ctx: Context, ms: Int) -> Result(String, Failure) {
  workflow.execute_activity(
    ctx,
    long(),
    ms,
    workflow.activity_options(start_to_close: duration.seconds(60)),
  )
}

fn workflow_worker(task_queue: String) -> worker.Worker {
  let assert Ok(worker) =
    worker.new(support.local_client(), task_queue)
    |> worker.with_workflows([workflow.handler(patient(), patient_workflow)])
    |> worker.start
  worker
}

fn activity_config(task_queue: String) -> worker.Config {
  worker.new(support.local_client(), task_queue)
  |> worker.with_activities([
    activity.handler(long(), long_activity),
    activity.handler(victim(), victim_activity),
  ])
}

fn start_patient(task_queue: String, ms: Int) -> client.Handle(String) {
  let assert Ok(handle) =
    client.start(
      support.local_client(),
      patient(),
      ms,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
  handle
}

fn noted(task_queue: String, what: String) -> Int {
  int.parse(recorded(task_queue <> ":" <> what)) |> result.unwrap(0)
}

pub fn stop_returns_once_the_activities_in_flight_have_finished_test() {
  let task_queue = "stop-" <> unique()
  let workflows = workflow_worker(task_queue)
  let assert Ok(activities) =
    activity_config(task_queue)
    |> worker.with_shutdown_grace(duration.milliseconds(400))
    |> worker.start
  let handle = start_patient(task_queue, 1500)
  assert eventually(fn() { result.is_ok(recorded_pid(task_queue)) })
  let assert Ok(pid) = recorded_pid(task_queue)

  let asked = now_ms()
  worker.stop(activities)
  let stopped = now_ms()
  // The activity ran on past the shutdown grace and the cancellation that followed it,
  // and `stop` waited for it.
  assert !process.is_alive(pid)
  let finished = noted(task_queue, "finished")
  assert finished > 0 && finished <= stopped
  // It knew of the shutdown at once, and was asked to cancel when the grace was up.
  let knew = noted(task_queue, "shutting-down")
  let cancelled = noted(task_queue, "cancelled")
  assert knew > 0 && knew - asked < 300
  assert cancelled - asked >= 400 && cancelled < finished
  // Its result counts: nothing was retried.
  assert client.result(handle) == Ok("attempt 1")
  assert recorded(task_queue <> ":attempts") == "x"
  worker.stop(workflows)
}

fn stuck() {
  workflow.Definition("stuck", codec.nil(), codec.nil())
}

fn stuck_workflow(ctx: Context, _: Nil) -> Result(Nil, Failure) {
  workflow.execute_activity(
    ctx,
    victim(),
    workflow.info(ctx).workflow_id,
    once(duration.seconds(60)),
  )
}

pub fn stop_kills_an_activity_that_outstays_both_graces_test() {
  let task_queue = "stop-stuck-" <> unique()
  let client = support.local_client()
  let assert Ok(workflows) =
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(stuck(), stuck_workflow)])
    |> worker.start
  let assert Ok(activities) =
    activity_config(task_queue)
    |> worker.with_shutdown_grace(duration.milliseconds(300))
    |> worker.with_activity_cancellation_grace(Some(duration.milliseconds(300)))
    |> worker.start
  let assert Ok(handle) =
    client.start(
      client,
      stuck(),
      Nil,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
  assert eventually(fn() { result.is_ok(recorded_pid(task_queue)) })
  let assert Ok(pid) = recorded_pid(task_queue)
  let asked = now_ms()
  worker.stop(activities)
  let took = now_ms() - asked
  assert !process.is_alive(pid)
  assert took >= 600 && took < 10_000
  // The server hears of it as a failure caused by the shutdown, not a minute later.
  let assert Error(client.WorkflowFailed(failure)) = client.result(handle)
  assert failure.root_cause(failure).error_type == Some("WorkerShutdown")
  worker.stop(workflows)
}

pub fn an_activity_under_test_can_be_told_its_worker_is_shutting_down_test() {
  let env = testing.activity_env(testing.activity_info())
  let ctx = testing.activity_context(env)
  assert !activity.is_worker_shutting_down(ctx)
  testing.begin_worker_shutdown(env)
  assert activity.is_worker_shutting_down(ctx)
}

// ---- the operating system asks the VM to stop ----

/// Entry point of a node that runs an activity worker the supported way.
pub fn run_until_asked_to_stop(task_queue: String) -> Nil {
  write_file("build/readiness/" <> task_queue <> ".pid", <<os_pid():utf8>>)
  let assert Ok(Nil) =
    activity_config(task_queue)
    |> worker.with_shutdown_grace(duration.seconds(20))
    |> worker.run
  Nil
}

/// Entry point of a node that runs the worker under a supervisor in an OTP application
/// and leaves the signal to the VM, which answers it with `init:stop()`.
pub fn supervise_until_asked_to_stop(task_queue: String) -> Nil {
  start_application("temporal_gleam_readiness", fn() {
    let assert Ok(supervisor) =
      static_supervisor.new(static_supervisor.OneForOne)
      |> static_supervisor.add(worker.supervised(
        activity_config(task_queue)
        |> worker.with_shutdown_grace(duration.seconds(20)),
      ))
      |> static_supervisor.start
    supervisor.pid
  })
  write_file("build/readiness/" <> task_queue <> ".pid", <<os_pid():utf8>>)
  process.sleep_forever()
}

/// Entry point of a node that starts a worker and does nothing about signals.
pub fn start_and_sleep(task_queue: String) -> Nil {
  let assert Ok(_) =
    activity_config(task_queue)
    |> worker.with_shutdown_grace(duration.seconds(20))
    |> worker.start
  write_file("build/readiness/" <> task_queue <> ".pid", <<os_pid():utf8>>)
  process.sleep_forever()
}

/// Entry point of a node that runs the worker under a supervisor started from a plain
/// `main`, outside any OTP application: `init:stop()` would kill it without asking.
pub fn supervise_from_main(task_queue: String) -> Nil {
  let assert Ok(_) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(worker.supervised(
      activity_config(task_queue)
      |> worker.with_shutdown_grace(duration.seconds(20)),
    ))
    |> static_supervisor.start
  write_file("build/readiness/" <> task_queue <> ".pid", <<os_pid():utf8>>)
  process.sleep_forever()
}

/// Entry point of a node with a worker on each of two task queues.
pub fn start_two_and_sleep(first: String, second: String) -> Nil {
  list.each([first, second], fn(task_queue) {
    let assert Ok(_) =
      activity_config(task_queue)
      |> worker.with_shutdown_grace(duration.seconds(20))
      |> worker.start
  })
  write_file("build/readiness/" <> first <> ".pid", <<os_pid():utf8>>)
  process.sleep_forever()
}

/// Entry point of a node whose worker leaves SIGTERM to the VM.
pub fn start_without_signal_handling(task_queue: String) -> Nil {
  let assert Ok(_) =
    activity_config(task_queue)
    |> worker.with_shutdown_grace(duration.seconds(20))
    |> worker.without_shutdown_signal_handling
    |> worker.start
  print("handlers: " <> signal_handlers())
  write_file("build/readiness/" <> task_queue <> ".pid", <<os_pid():utf8>>)
  process.sleep_forever()
}

/// Whether an activity of the workflow is running now. The history cannot say: the
/// server writes that an activity started only once it knows how the attempt ended.
fn activity_started(workflow_id: String) -> Bool {
  string.contains(
    cmd(
      "temporal workflow describe --output json --workflow-id "
      <> workflow_id
      <> " 2>&1",
    ),
    "PENDING_ACTIVITY_STATE_STARTED",
  )
}

/// How many of the node's output lines contain `text`.
fn lines_with(name: String, text: String) -> Int {
  list.length(string.split(node_file(name, "out"), text)) - 1
}

/// Start a node, get a three-second activity going in it, send the node SIGTERM, and
/// check that the activity was allowed to finish and the node left of its own accord.
fn sigterm_lets_the_activity_finish(task_queue: String, entry: String) -> Nil {
  let workflows = workflow_worker(task_queue)
  spawn_node(task_queue, entry <> "(<<\"" <> task_queue <> "\">>), halt(0).")
  let handle = start_patient(task_queue, 3000)
  // The activity runs in the other node; this one asks the server whether it started.
  assert eventually(fn() {
    node_file(task_queue, "pid") != "" && activity_started(task_queue)
  })
  let _ = cmd("kill -TERM " <> node_file(task_queue, "pid"))
  // Completed by the first attempt, where a dropped activity would have been retried
  // after its minute.
  assert client.result(handle) == Ok("attempt 1")
  assert eventually(fn() { node_file(task_queue, "status") != "" })
  assert node_file(task_queue, "status") == "0"
  // The worker stopped once, whoever asked it to, and said what became of the activity.
  assert lines_with(task_queue, " stopped in ") == 1
  assert lines_with(
      task_queue,
      "activities: 1 in flight, 1 finished, 0 accepted cancellation, 0 killed",
    )
    == 1
  worker.stop(workflows)
}

pub fn sigterm_drains_a_worker_that_was_only_started_test() {
  let task_queue = "started-" <> unique()
  sigterm_lets_the_activity_finish(task_queue, "start_and_sleep")
  // With `logger` as it comes, the worker said at start what SIGTERM would do. The two
  // graces are twenty seconds and the default fifteen.
  assert lines_with(
      task_queue,
      "temporal: worker on task queue "
        <> task_queue
        <> " drains on SIGTERM, for up to 35s",
    )
    == 1
}

pub fn sigterm_drains_a_worker_supervised_outside_an_application_test() {
  sigterm_lets_the_activity_finish(
    "supervised-" <> unique(),
    "supervise_from_main",
  )
}

pub fn sigterm_drains_every_started_worker_test() {
  let first = "two-a-" <> unique()
  let second = "two-b-" <> unique()
  let workflows = list.map([first, second], workflow_worker)
  spawn_node(
    first,
    "start_two_and_sleep(<<\""
      <> first
      <> "\">>, <<\""
      <> second
      <> "\">>), halt(0).",
  )
  let handles = list.map([first, second], start_patient(_, 3000))
  assert eventually(fn() {
    node_file(first, "pid") != ""
    && activity_started(first)
    && activity_started(second)
  })
  // A second SIGTERM while the workers drain changes nothing.
  let _ = cmd("kill -TERM " <> node_file(first, "pid"))
  process.sleep(300)
  let _ = cmd("kill -TERM " <> node_file(first, "pid"))
  assert list.map(handles, client.result) == [Ok("attempt 1"), Ok("attempt 1")]
  assert eventually(fn() { node_file(first, "status") != "" })
  assert node_file(first, "status") == "0"
  assert lines_with(first, " stopped in ") == 2
  assert lines_with(first, "SIGTERM received, stopping gracefully") == 1
  list.each(workflows, worker.stop)
}

pub fn a_worker_without_signal_handling_leaves_sigterm_to_the_vm_test() {
  let task_queue = "unhandled-" <> unique()
  let workflows = workflow_worker(task_queue)
  spawn_node(
    task_queue,
    "start_without_signal_handling(<<\"" <> task_queue <> "\">>), halt(0).",
  )
  let handle = start_patient(task_queue, 10_000)
  assert eventually(fn() {
    node_file(task_queue, "pid") != "" && activity_started(task_queue)
  })
  let asked = now_ms()
  let _ = cmd("kill -TERM " <> node_file(task_queue, "pid"))
  // The VM stops at once, as it does by default, with the activity far from finished.
  assert eventually(fn() { node_file(task_queue, "status") != "" })
  assert now_ms() - asked < 3000
  assert node_file(task_queue, "status") == "0"
  assert !string.contains(
    cmd("temporal workflow show --workflow-id " <> task_queue <> " 2>&1"),
    "ActivityTaskCompleted",
  )
  // Nothing took the VM's handler's place, and nothing was said about draining.
  let output = node_file(task_queue, "out")
  assert string.contains(output, "handlers: [erl_signal_handler")
  assert !string.contains(output, "temporal")
  let assert Ok(Nil) = client.terminate(handle, "the test is over")
  worker.stop(workflows)
}

pub fn a_stopped_worker_is_no_longer_drained_on_sigterm_test() {
  let before = workers_drained_on_sigterm()
  let assert Ok(worker) =
    activity_config("deregistered-" <> unique()) |> worker.start
  assert workers_drained_on_sigterm() == before + 1
  worker.stop(worker)
  assert eventually(fn() { workers_drained_on_sigterm() == before })
  // One that never asked is never there.
  let assert Ok(worker) =
    activity_config("unregistered-" <> unique())
    |> worker.without_shutdown_signal_handling
    |> worker.start
  assert workers_drained_on_sigterm() == before
  worker.stop(worker)
}

pub fn a_worker_logs_its_drain_budget_and_how_its_stop_went_test() {
  let task_queue = "budget-" <> unique()
  let lines =
    capture_log_metadata(fn() {
      let assert Ok(worker) =
        activity_config(task_queue)
        |> worker.with_shutdown_grace(duration.seconds(2))
        |> worker.with_activity_cancellation_grace(
          Some(duration.milliseconds(500)),
        )
        |> worker.start
      worker.stop(worker)
    })
    |> list.filter(fn(line) { string.contains(line.0, task_queue) })
  let assert [#(started, budget), #(stopped, summary)] = lines
  assert started
    == "temporal: worker on task queue "
    <> task_queue
    <> " drains on SIGTERM, for up to 2.5s"
  assert list.contains(budget, "fields.shutdown_grace_ms=2000")
  assert list.contains(budget, "fields.activity_cancellation_grace_ms=500")
  assert list.contains(budget, "fields.drain_ms=2500")
  assert string.starts_with(
    stopped,
    "temporal: worker on task queue " <> task_queue <> " stopped in ",
  )
  assert string.ends_with(
    stopped,
    "activities: 0 in flight, 0 finished, 0 accepted cancellation, 0 killed",
  )
  assert list.contains(summary, "fields.activities_in_flight=0")
  assert list.contains(summary, "fields.activities_finished=0")
  assert list.contains(summary, "fields.activities_cancelled=0")
  assert list.contains(summary, "fields.activities_killed=0")
  assert list.any(summary, string.starts_with(_, "fields.duration_ms="))

  // A worker that leaves the signal alone has no budget to announce.
  let quiet = "no-budget-" <> unique()
  let lines =
    capture_log_metadata(fn() {
      let assert Ok(worker) =
        activity_config(quiet)
        |> worker.without_shutdown_signal_handling
        |> worker.start
      worker.stop(worker)
    })
    |> list.filter(fn(line) { string.contains(line.0, quiet) })
  let assert [#(stopped, _)] = lines
  assert string.contains(stopped, " stopped in ")
}

// ---- activities cut off by a shutdown ----

fn cut_off() {
  activity.Definition("cut_off", codec.string(), codec.string())
}

/// On its first attempt, waits for its worker to shut down and then does as `how` says:
/// finishes, accepts the cancellation that follows, or ignores everything until it is
/// killed. Any later attempt returns at once.
fn cut_off_activity(
  ctx: activity.Context,
  how: String,
) -> Result(String, Failure) {
  let attempt = "attempt " <> int.to_string(activity.info(ctx).attempt)
  case activity.info(ctx).attempt, how {
    1, "finishes" -> {
      wait_until(fn() { activity.is_worker_shutting_down(ctx) })
      Ok(attempt)
    }
    1, "accepts" -> {
      wait_until(fn() {
        activity.cancel_reason(ctx) == Some(activity.WorkerShutdown)
      })
      Error(failure.cancelled())
    }
    1, _ -> {
      process.sleep_forever()
      Ok(attempt)
    }
    _, _ -> Ok(attempt)
  }
}

fn wait_until(condition: fn() -> Bool) -> Nil {
  case condition() {
    True -> Nil
    False -> {
      process.sleep(20)
      wait_until(condition)
    }
  }
}

fn resilient() {
  workflow.Definition("resilient", codec.string(), codec.string())
}

/// Returns what its activity returned, or says so if it saw the activity cancelled.
fn resilient_workflow(ctx: Context, how: String) -> Result(String, Failure) {
  let outcome =
    workflow.execute_activity(
      ctx,
      cut_off(),
      how,
      workflow.activity_options(start_to_close: duration.seconds(60)),
    )
  case outcome {
    Ok(attempt) -> Ok(attempt)
    Error(error) ->
      case failure.is_cancellation(error) {
        True -> Ok("the workflow saw a cancellation")
        False -> Error(error)
      }
  }
}

fn cut_off_config(task_queue: String) -> worker.Config {
  worker.new(support.local_client(), task_queue)
  |> worker.with_activities([activity.handler(cut_off(), cut_off_activity)])
  |> worker.with_shutdown_grace(duration.milliseconds(500))
  |> worker.with_activity_cancellation_grace(Some(duration.milliseconds(500)))
}

/// Entry point of a node whose worker's graces are too short for its activities.
pub fn start_cut_off_and_sleep(task_queue: String) -> Nil {
  let assert Ok(_) = cut_off_config(task_queue) |> worker.start
  write_file("build/readiness/" <> task_queue <> ".pid", <<os_pid():utf8>>)
  process.sleep_forever()
}

pub fn an_activity_cut_off_by_sigterm_is_retried_on_another_worker_test() {
  let task_queue = "cut-off-" <> unique()
  let client = support.local_client()
  let assert Ok(workflows) =
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(resilient(), resilient_workflow)])
    |> worker.start
  spawn_node(
    task_queue,
    "start_cut_off_and_sleep(<<\"" <> task_queue <> "\">>), halt(0).",
  )
  let hows = ["finishes", "accepts", "stuck"]
  let id = fn(how) { task_queue <> "-" <> how }
  let handles =
    list.map(hows, fn(how) {
      let assert Ok(handle) =
        client.start(
          client,
          resilient(),
          how,
          client.start_options(workflow_id: id(how), task_queue:),
        )
      handle
    })
  assert eventually(fn() {
    node_file(task_queue, "pid") != ""
    && list.all(hows, fn(how) { activity_started(id(how)) })
  })
  let asked = now_ms()
  let _ = cmd("kill -TERM " <> node_file(task_queue, "pid"))
  // The node drains for its two half-second graces and leaves.
  assert eventually(fn() { node_file(task_queue, "status") != "" })
  assert node_file(task_queue, "status") == "0"
  assert lines_with(
      task_queue,
      "activities: 3 in flight, 1 finished, 1 accepted cancellation, 1 killed",
    )
    == 1

  // Another worker takes the two activities that were cut off: at once, by their retry
  // policy, not when their minute is up. No workflow hears of a cancellation, which it
  // never asked for.
  let assert Ok(replacement) = cut_off_config(task_queue) |> worker.start
  assert list.map(handles, client.result)
    == [Ok("attempt 1"), Ok("attempt 2"), Ok("attempt 2")]
  assert now_ms() - asked < 20_000
  // The server was told why the first attempts ended.
  list.each(["accepts", "stuck"], fn(how) {
    let history =
      cmd(
        "temporal workflow show --output json --workflow-id "
        <> id(how)
        <> " 2>&1",
      )
    assert string.contains(history, "WorkerShutdown")
    assert !string.contains(history, "ActivityTaskCancel")
  })
  worker.stop(replacement)
  worker.stop(workflows)
}

pub fn sigterm_stops_a_running_worker_gracefully_test() {
  sigterm_lets_the_activity_finish(
    "sigterm-" <> unique(),
    "run_until_asked_to_stop",
  )
}

pub fn a_supervised_worker_drains_when_the_vm_is_stopped_test() {
  sigterm_lets_the_activity_finish(
    "init-stop-" <> unique(),
    "supervise_until_asked_to_stop",
  )
}

pub fn waiting_for_the_signal_leaves_other_signals_to_the_vm_test() {
  let name = "signals-" <> unique()
  spawn_node(name, "await_signal(<<\"" <> name <> "\">>), halt(0).")
  assert eventually(fn() { node_file(name, "pid") != "" })
  let pid = node_file(name, "pid")
  // SIGTERM wakes everything that waits for it, and whatever asks later.
  let _ = cmd("kill -TERM " <> pid)
  assert eventually(fn() { node_file(name, "status") != "" })
  assert node_file(name, "status") == "0"
  let output = node_file(name, "out")
  assert string.contains(output, "woken woken afterwards")
  // The handler is there once, however many processes asked for it at the same moment.
  assert list.length(string.split(output, "SIGTERM received")) == 2

  // SIGUSR1 still makes the VM halt with a crash report, as its own handler has it do.
  let name = "signals-" <> unique()
  spawn_node(name, "await_signal(<<\"" <> name <> "\">>), halt(0).")
  assert eventually(fn() { node_file(name, "pid") != "" })
  let _ = cmd("kill -USR1 " <> node_file(name, "pid"))
  assert eventually(fn() { node_file(name, "status") != "" })
  assert node_file(name, "status") != "0"
  assert string.contains(node_file(name, "out"), "Received SIGUSR1")
}

/// Entry point of a node in which two processes wait for the signal, and a third asks
/// after it has come.
pub fn await_signal(name: String) -> Nil {
  let woken = process.new_subject()
  list.each([1, 2], fn(_) {
    process.spawn(fn() {
      worker.await_shutdown_signal()
      process.send(woken, "woken")
    })
  })
  process.sleep(200)
  write_file("build/readiness/" <> name <> ".pid", <<os_pid():utf8>>)
  let assert Ok(first) = process.receive(woken, 30_000)
  let assert Ok(second) = process.receive(woken, 30_000)
  worker.await_shutdown_signal()
  print(first <> " " <> second <> " afterwards")
}

@external(erlang, "io", "put_chars")
fn print(text: String) -> Nil
