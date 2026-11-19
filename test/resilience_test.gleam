//// What happens when parts of a worker die: a polling loop, a workflow run's process, an
//// activity that will not stop, the worker itself under a supervisor.

import gleam/erlang/process.{type Pid}
import gleam/list
import gleam/option.{Some}
import gleam/otp/static_supervisor
import gleam/time/duration
import support.{eventually, unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow.{type Context}

@external(erlang, "temporal_test_ffi", "linked")
fn linked(pid: Pid) -> List(Pid)

@external(erlang, "temporal_test_ffi", "monitored_by")
fn monitored_by(pid: Pid) -> List(Pid)

/// Run `body` without the crash reports that killing processes on purpose produces.
@external(erlang, "temporal_test_ffi", "quietly")
fn quietly(body: fn() -> Nil) -> Nil

@external(erlang, "temporal_test_ffi", "shut_down")
fn shut_down(supervisor: Pid) -> Nil

fn nudge() {
  workflow.SignalDefinition("nudge", codec.nil())
}

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn waiter() {
  workflow.Definition("waiter", codec.int(), codec.int())
}

/// Waits for a signal, then doubles its input with an activity.
fn waiter_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  workflow.receive_signal(ctx, nudge())
  workflow.execute_activity(
    ctx,
    double(),
    n,
    workflow.activity_options(start_to_close: duration.milliseconds(10_000)),
  )
}

fn config(task_queue: String) -> worker.Config {
  worker.new(support.local_client(), task_queue)
  |> worker.with_workflows([workflow.handler(waiter(), waiter_workflow)])
  |> worker.with_activities([activity.handler(double(), fn(_, n) { Ok(n * 2) })])
}

fn start_waiter(task_queue: String, id: String) -> client.Handle(Int) {
  let assert Ok(handle) =
    client.start(
      support.local_client(),
      waiter(),
      21,
      client.start_options(workflow_id: task_queue <> id, task_queue:),
    )
  handle
}

/// The worker's polling loops: what its manager is linked to, other than this process.
fn loops(worker: worker.Worker) -> List(Pid) {
  linked(worker.manager(worker))
  |> list.filter(fn(pid) { pid != process.self() })
}

pub fn a_killed_polling_loop_is_restarted_test() {
  use <- quietly
  let task_queue = "loops-" <> unique()
  let assert Ok(worker) = worker.start(config(task_queue))
  let waiting = start_waiter(task_queue, "-before")
  let before = loops(worker)
  assert list.length(before) == 2
  // Wait until the run is in memory, then kill every loop under it.
  assert eventually(fn() {
    list.any(before, fn(pid) { monitored_by(pid) != [] })
  })
  list.each(before, process.kill)
  assert eventually(fn() {
    let after = loops(worker)
    list.length(after) == 2 && !list.any(after, list.contains(before, _))
  })

  // The run that was in memory is replayed by the new loop, and new work is taken.
  let assert Ok(Nil) = client.signal(waiting, nudge(), Nil)
  assert client.result(waiting) == Ok(42)
  let fresh = start_waiter(task_queue, "-after")
  let assert Ok(Nil) = client.signal(fresh, nudge(), Nil)
  assert client.result(fresh) == Ok(42)
  worker.stop(worker)
}

pub fn a_killed_workflow_run_is_replayed_test() {
  use <- quietly
  let task_queue = "runs-" <> unique()
  let assert Ok(worker) = worker.start(config(task_queue))
  let handle = start_waiter(task_queue, "")
  // The run's process monitors the loop that feeds it.
  let runs = fn() { list.flat_map(loops(worker), monitored_by) }
  assert eventually(fn() { runs() != [] })
  list.each(runs(), process.kill)
  let assert Ok(Nil) = client.signal(handle, nudge(), Nil)
  assert client.result(handle) == Ok(42)
  worker.stop(worker)
}

fn deaf() {
  activity.Definition("deaf", codec.nil(), codec.nil())
}

/// Heartbeats for ever and never looks at whether it was cancelled.
fn deaf_activity(ctx: activity.Context, _: Nil) -> Result(Nil, Failure) {
  activity.heartbeat(ctx)
  process.sleep(100)
  deaf_activity(ctx, Nil)
}

fn impatient() {
  workflow.Definition("impatient", codec.nil(), codec.string())
}

/// Cancels an activity and waits for it to report that it stopped.
fn impatient_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  let options =
    workflow.ActivityOptions(
      ..workflow.activity_options(start_to_close: duration.milliseconds(120_000)),
      heartbeat_timeout: Some(duration.milliseconds(1000)),
      cancellation: workflow.WaitCancellationCompleted,
    )
  let outcome =
    workflow.with_timeout(ctx, duration.milliseconds(300), fn(scope) {
      workflow.execute_activity(scope, deaf(), Nil, options)
    })
  case outcome {
    Ok(Nil) -> Ok("finished")
    Error(failure) ->
      Ok(case failure.is_timeout(failure) {
        True -> "stopped"
        False -> "failed"
      })
  }
}

pub fn an_activity_that_ignores_cancellation_is_killed_after_its_grace_test() {
  let task_queue = "deaf-" <> unique()
  let client = support.local_client()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(impatient(), impatient_workflow)])
    |> worker.with_activities([activity.handler(deaf(), deaf_activity)])
    |> worker.with_activity_cancellation_grace(Some(duration.milliseconds(500)))
    |> worker.start
  // Without the grace period the activity would run for its full two minutes.
  assert client.execute(
      client,
      impatient(),
      Nil,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
    == Ok("stopped")
  worker.stop(worker)
}

pub fn a_supervisor_restarts_a_worker_that_dies_test() {
  use <- quietly
  let task_queue = "supervised-" <> unique()
  let found = process.new_subject()
  // The supervisor runs in a process of its own, so that this one need not trap exits.
  process.spawn_unlinked(fn() {
    let assert Ok(supervisor) =
      static_supervisor.new(static_supervisor.OneForOne)
      |> static_supervisor.add(worker.supervised(config(task_queue)))
      |> static_supervisor.start
    process.send(found, #(supervisor.pid, process.self()))
    process.sleep_forever()
  })
  let assert Ok(#(supervisor, owner)) = process.receive(found, 10_000)
  // What the supervisor is linked to, apart from the process that started it, is the
  // worker it runs.
  let managers = fn() {
    linked(supervisor)
    |> list.filter(fn(pid) { pid != owner && process.is_alive(pid) })
  }
  assert eventually(fn() { list.length(managers()) >= 1 })
  let handle = start_waiter(task_queue, "")
  let before = managers()
  list.each(before, process.kill)
  // A new manager, and with it a new worker, takes over.
  assert eventually(fn() {
    let after = managers()
    after != [] && !list.any(after, list.contains(before, _))
  })
  let assert Ok(Nil) = client.signal(handle, nudge(), Nil)
  assert client.result(handle) == Ok(42)
  shut_down(supervisor)
}

pub fn a_worker_whose_loops_keep_crashing_gives_up_test() {
  use <- quietly
  let task_queue = "doomed-" <> unique()
  let found = process.new_subject()
  // An owner of its own: the worker takes its owner down with it when it gives up.
  let owner =
    process.spawn_unlinked(fn() {
      let assert Ok(worker) = worker.start(config(task_queue))
      process.send(found, worker)
      process.sleep_forever()
    })
  let assert Ok(worker) = process.receive(found, 10_000)
  let manager = worker.manager(worker)
  let kill_loops = fn() {
    list.each(
      linked(manager) |> list.filter(fn(pid) { pid != owner }),
      process.kill,
    )
  }
  // Six rounds of crashes inside a minute is one more than a worker tolerates.
  assert eventually(fn() {
    kill_loops()
    !process.is_alive(manager)
  })
  assert eventually(fn() { !process.is_alive(owner) })
}
