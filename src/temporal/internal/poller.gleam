//// The loops that take tasks from core and hand them to workflow and activity processes.

import gleam/dict.{type Dict}
import gleam/dynamic
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/string
import temporal/activity
import temporal/codec.{type PayloadCodec}
import temporal/failure
import temporal/internal/bridge.{type NifWorker}
import temporal/internal/runtime
import temporal/internal/time
import temporal/internal/transcode
import temporal/middleware.{type Outbound}
import temporal/retry

/// A run that core removed from the cache, with core's `EvictionReason` value.
pub type Eviction {
  Eviction(run_id: String, reason: Int, message: String)
}

/// Dispatch workflow activations until the worker shuts down. Returns the evictions seen,
/// newest first, when `record_evictions` is set; a long-lived worker leaves it unset so the
/// list cannot grow without bound.
///
/// This loop and the two below return an error when a poll fails. Core retries what is
/// worth retrying itself, so by then it has given up and the worker takes no more tasks.
pub fn workflow_loop(
  worker: NifWorker,
  workflows: Dict(String, runtime.WorkflowFn),
  task_queue: String,
  outbound: Outbound,
  codec: Option(PayloadCodec),
  record_evictions: Bool,
) -> Result(List(Eviction), String) {
  do_workflow_loop(
    worker,
    workflows,
    task_queue,
    outbound,
    codec,
    record_evictions,
    dict.new(),
    [],
  )
}

fn do_workflow_loop(
  worker: NifWorker,
  workflows: Dict(String, runtime.WorkflowFn),
  task_queue: String,
  outbound: Outbound,
  codec: Option(PayloadCodec),
  record_evictions: Bool,
  runs: Dict(String, Pid),
  evictions: List(Eviction),
) -> Result(List(Eviction), String) {
  let loop = process.self()
  case bridge.poll_workflow(worker) {
    Error(bridge.Shutdown) -> Ok(evictions)
    Error(bridge.PollFailed(message)) -> Error(message)
    Ok(activation) -> {
      let eviction =
        list.find_map(activation.jobs, fn(job) {
          case job {
            bridge.RemoveFromCache(message, reason) ->
              Ok(Eviction(activation.run_id, reason, message))
            _ -> Error(Nil)
          }
        })
      let complete = complete_workflow(worker, codec, _)
      let existing = case dict.get(runs, activation.run_id) {
        Ok(pid) ->
          case process.is_alive(pid) {
            True -> Ok(pid)
            False -> Error(Nil)
          }
        Error(Nil) -> Error(Nil)
      }
      let decoded = transcode.decode_activation(codec, activation)
      let runs = case decoded {
        Ok(activation) ->
          dispatch(activation, existing, eviction, runs, complete, fn() {
            // A run belongs to the loop that feeds it: if the loop dies, so does the run.
            bridge.follow(loop)
            runtime.run(workflows, task_queue, outbound, complete)
          })
        // Core evicts the run after a failed task and replays it, so the run's process,
        // if there is one, is left as it is.
        Error(reason) -> {
          complete(bridge.TaskFailure(
            activation.run_id,
            failure.new(
              "payload codec could not decode the workflow task: " <> reason,
            ),
          ))
          runs
        }
      }
      let evictions = case eviction, record_evictions {
        Ok(eviction), True -> [eviction, ..evictions]
        _, _ -> evictions
      }
      do_workflow_loop(
        worker,
        workflows,
        task_queue,
        outbound,
        codec,
        record_evictions,
        runs,
        evictions,
      )
    }
  }
}

/// Answer an activation. An answer the NIF or core will not take, such as an activity
/// with a negative timeout, would leave the workflow task holding its slot until the server
/// timed it out, with nothing to show why. The task is failed instead: the reason is in
/// the workflow's history and the server retries the task.
fn complete_workflow(
  worker: NifWorker,
  codec: Option(PayloadCodec),
  completion: bridge.Completion,
) -> Nil {
  let run_id = completion.run_id
  let report = fn(message) {
    bridge.log(bridge.Error, message, [#("run_id", dynamic.string(run_id))])
  }
  let encoded = transcode.encode_completion(codec, completion)
  case bridge.complete_workflow(worker, encoded), completion {
    Ok(Nil), _ -> Nil
    Error(reason), bridge.Success(..) -> {
      report(
        "temporal: workflow task completion rejected, failing the task: "
        <> reason,
      )
      let failed =
        bridge.TaskFailure(
          run_id,
          failure.new("the workflow task's completion was rejected: " <> reason),
        )
      case bridge.complete_workflow(worker, failed) {
        Ok(Nil) -> Nil
        Error(reason) ->
          report("temporal: could not fail the workflow task: " <> reason)
      }
    }
    Error(reason), bridge.TaskFailure(..) ->
      report("temporal: could not fail the workflow task: " <> reason)
  }
}

fn dispatch(
  activation: bridge.Activation,
  existing: Result(Pid, Nil),
  eviction: Result(Eviction, Nil),
  runs: Dict(String, Pid),
  complete: fn(bridge.Completion) -> Nil,
  run: fn() -> Nil,
) -> Dict(String, Pid) {
  case existing, eviction {
    Ok(pid), Ok(_) -> {
      bridge.send_activation(pid, activation)
      dict.delete(runs, activation.run_id)
    }
    Ok(pid), Error(Nil) -> {
      bridge.send_activation(pid, activation)
      runs
    }
    // Nothing to evict: the run's process is already gone.
    Error(Nil), Ok(_) -> {
      complete(bridge.Success(activation.run_id, [], 0))
      dict.delete(runs, activation.run_id)
    }
    Error(Nil), Error(Nil) -> {
      let pid = process.spawn_unlinked(run)
      bridge.send_activation(pid, activation)
      dict.insert(runs, activation.run_id, pid)
    }
  }
}

/// Run activity tasks until the worker shuts down, each in its own process. Local
/// activities arrive here too. `shutting_down` is what `activity.is_worker_shutting_down`
/// reads.
///
/// Core answers the last poll with `Shutdown` only once every activity it handed out has
/// been reported, so when this returns no activity of the worker is still running.
pub fn activity_loop(
  worker: NifWorker,
  activities: Dict(String, activity.Handler),
  task_queue: String,
  codec: Option(PayloadCodec),
  cancellation_grace_ms: Option(Int),
  shutting_down: fn() -> Bool,
) -> Result(Nil, String) {
  do_activity_loop(
    ActivityLoop(
      worker:,
      activities:,
      task_queue:,
      codec:,
      cancellation_grace_ms:,
      shutting_down:,
    ),
    dict.new(),
  )
}

type ActivityLoop {
  ActivityLoop(
    worker: NifWorker,
    activities: Dict(String, activity.Handler),
    task_queue: String,
    codec: Option(PayloadCodec),
    cancellation_grace_ms: Option(Int),
    shutting_down: fn() -> Bool,
  )
}

fn do_activity_loop(
  loop: ActivityLoop,
  running: Dict(BitArray, Subject(Watch)),
) -> Result(Nil, String) {
  let running =
    dict.filter(running, fn(_, watcher) {
      case process.subject_owner(watcher) {
        Ok(pid) -> process.is_alive(pid)
        Error(Nil) -> False
      }
    })
  case bridge.poll_activity(loop.worker) {
    Error(bridge.Shutdown) -> Ok(Nil)
    Error(bridge.PollFailed(message)) -> Error(message)
    Ok(bridge.ActivityStart(task_token:, activity_id:, ..) as task) ->
      case transcode.decode_activity_task(loop.codec, task) {
        Ok(task) -> {
          let watcher = start_activity(loop, task)
          do_activity_loop(loop, dict.insert(running, task_token, watcher))
        }
        Error(reason) -> {
          complete_activity(
            loop.worker,
            task_token,
            activity_id,
            bridge.ActivityFailed(failure.new(
              "payload codec could not decode the activity task: " <> reason,
            )),
          )
          do_activity_loop(loop, running)
        }
      }
    Ok(bridge.ActivityCancel(task_token:, reason:)) -> {
      // The activity decides how to stop: it sees the request through
      // `activity.is_cancelled` and reports its own outcome. An activity that has already
      // finished needs nothing.
      case dict.get(running, task_token) {
        Ok(watcher) -> process.send(watcher, CancelRequested(reason))
        Error(Nil) -> Nil
      }
      do_activity_loop(loop, running)
    }
  }
}

/// Report an activity's outcome. One the NIF or core will not take is logged and the
/// activity failed in its place, so that the server retries it now instead of waiting for
/// its timeout.
fn complete_activity(
  worker: NifWorker,
  task_token: BitArray,
  activity_id: String,
  result: bridge.ActivityResult,
) -> Nil {
  let report = fn(message) {
    bridge.log(bridge.Error, message, [
      #("activity_id", dynamic.string(activity_id)),
    ])
  }
  case bridge.complete_activity(worker, task_token, result) {
    Ok(Nil) -> Nil
    Error(reason) -> {
      report(
        "temporal: activity result rejected, failing the activity: " <> reason,
      )
      let failed =
        bridge.ActivityFailed(failure.new(
          "the activity's result was rejected: " <> reason,
        ))
      case bridge.complete_activity(worker, task_token, failed) {
        Ok(Nil) -> Nil
        Error(reason) ->
          report("temporal: could not fail the activity: " <> reason)
      }
    }
  }
}

/// What the process watching one activity is told.
type Watch {
  /// The activity's own process, with its outcome, just before it exits.
  Finished(bridge.ActivityResult)
  /// The activity's process is gone.
  Exited(process.Down)
  /// Core wants the activity to stop; `reason` is its `ActivityCancelReason` value.
  CancelRequested(reason: Int)
  /// The activity was asked to stop a grace period ago.
  GraceOver
}

/// Start one activity: the process that runs it and the process that watches that one.
/// Returns where to send the watcher a cancellation.
///
/// The watcher is the only process that reports the activity's outcome to core, and it
/// reports exactly one: the result the activity's process hands it, a failure if that
/// process dies without handing over a result, or a cancellation if it has to be killed.
/// Whatever happens to the activity's process, core gets its slot back.
fn start_activity(
  loop: ActivityLoop,
  task: bridge.ActivityTask,
) -> Subject(Watch) {
  let assert bridge.ActivityStart(task_token:, activity_id:, ..) = task
  let ready = process.new_subject()
  process.spawn_unlinked(fn() {
    let inbox = process.new_subject()
    process.send(ready, inbox)
    let activity =
      process.spawn_unlinked(fn() {
        process.send(inbox, Finished(run_activity(loop, task)))
      })
    let monitor = process.monitor(activity)
    let selector =
      process.new_selector()
      |> process.select(inbox)
      |> process.select_specific_monitor(monitor, Exited)
    let report = fn(result) {
      complete_activity(
        loop.worker,
        task_token,
        activity_id,
        transcode.encode_activity_result(loop.codec, result),
      )
    }
    // The activity's process hands over its outcome as the last thing it does. Waiting
    // for it to be gone means that an activity core counts as finished is not running.
    let gone = fn() {
      let _ =
        process.new_selector()
        |> process.select_specific_monitor(monitor, Exited)
        |> process.selector_receive(5000)
      Nil
    }
    watch_activity(
      Watcher(loop:, activity:, activity_id:, selector:, inbox:, report:, gone:),
      False,
    )
  })
  process.receive_forever(ready)
}

type Watcher {
  Watcher(
    loop: ActivityLoop,
    activity: Pid,
    activity_id: String,
    selector: process.Selector(Watch),
    inbox: Subject(Watch),
    report: fn(bridge.ActivityResult) -> Nil,
    gone: fn() -> Nil,
  )
}

fn watch_activity(watcher: Watcher, grace_running: Bool) -> Nil {
  let Watcher(loop:, activity:, activity_id:, selector:, inbox:, report:, gone:) =
    watcher
  let continue = watch_activity(watcher, _)
  case process.selector_receive_forever(selector) {
    Finished(result) -> {
      gone()
      report(result)
    }
    // Killed from outside, or taken down by a process it was linked to. The failure is
    // retryable: the server runs the activity again by its retry policy.
    Exited(down) ->
      report(bridge.ActivityFailed(
        failure.new(
          "the activity process exited before reporting a result: "
          <> exit_reason(down),
        )
        |> failure.with_type("ActivityProcessExited"),
      ))
    CancelRequested(reason) -> {
      bridge.send_activity_cancel(activity, reason)
      case loop.cancellation_grace_ms, grace_running {
        Some(grace_ms), False -> {
          process.send_after(inbox, grace_ms, GraceOver)
          continue(True)
        }
        _, _ -> continue(grace_running)
      }
    }
    // A stuck activity must not hold its slot on the worker for good.
    GraceOver -> {
      bridge.kill(activity)
      gone()
      bridge.log(
        bridge.Warning,
        "temporal: killed an activity that was still running after its cancellation grace",
        [#("activity_id", dynamic.string(activity_id))],
      )
      report(bridge.ActivityCancelled(failure.cancelled()))
    }
  }
}

fn exit_reason(down: process.Down) -> String {
  case down {
    process.ProcessDown(reason: process.Killed, ..) -> "killed"
    process.ProcessDown(reason: process.Normal, ..) -> "normal"
    process.ProcessDown(reason: process.Abnormal(reason), ..) ->
      string.inspect(reason)
    process.PortDown(..) -> "unknown"
  }
}

/// Run one activity in the calling process and return its outcome.
fn run_activity(
  loop: ActivityLoop,
  task: bridge.ActivityTask,
) -> bridge.ActivityResult {
  let ActivityLoop(
    worker:,
    activities:,
    task_queue:,
    codec:,
    shutting_down:,
    ..,
  ) = loop
  let assert bridge.ActivityStart(
    task_token:,
    activity_type:,
    activity_id:,
    workflow_id:,
    run_id:,
    input:,
    attempt:,
    heartbeat_details:,
    workflow_type:,
    start_to_close_ms:,
    heartbeat_timeout_ms:,
    scheduled_time_ms:,
    is_local:,
    headers:,
    retry:,
    activity_run_id:,
  ) = task
  let ctx =
    activity.new_context(
      info: activity.Info(
        activity_id:,
        activity_type:,
        workflow_id:,
        run_id:,
        workflow_type:,
        activity_run_id: case activity_run_id {
          "" -> None
          _ -> Some(activity_run_id)
        },
        task_queue:,
        attempt:,
        retry: option.map(retry, retry.from_bridge),
        heartbeat_details:,
        start_to_close: time.option_from_ms(start_to_close_ms),
        heartbeat_timeout: time.option_from_ms(heartbeat_timeout_ms),
        scheduled_time: time.timestamp(scheduled_time_ms),
        is_local:,
        headers:,
      ),
      task_token:,
      // A local activity has no heartbeat: the workflow task it runs in is its lease.
      heartbeat: fn(details) {
        case is_local {
          True -> Nil
          False ->
            bridge.heartbeat(
              worker,
              task_token,
              transcode.encode_list(codec, details),
            )
        }
      },
      cancel_reason: fn() {
        option.map(bridge.activity_cancel_reason(), cancel_reason)
      },
      worker_shutting_down: shutting_down,
    )
  case dict.get(activities, activity_type) {
    Error(Nil) ->
      bridge.ActivityFailed(
        failure.new(
          "activity type \""
          <> activity_type
          <> "\" is not registered on this worker, which has "
          <> runtime.known(dict.keys(activities))
          <> ". Add its handler to worker.with_activities.",
        )
        |> failure.with_type("NotFound"),
      )
    // Exits raised inside the activity are caught too, so that it fails with what it
    // exited with. One that is killed from outside is the watcher's to report.
    Ok(handler) ->
      case
        bridge.rescue_all(fn() { handler.run(ctx, input) }),
        bridge.activity_deferred()
      {
        // Whatever a deferred activity returns, its outcome comes later.
        Ok(_), True -> bridge.ActivityWillCompleteAsync
        Ok(Ok(payload)), False -> bridge.ActivityCompleted(payload)
        Ok(Error(failure.Failure(kind: failure.Cancelled, ..) as failure)),
          False
        -> bridge.ActivityCancelled(failure)
        Ok(Error(failure)), False -> bridge.ActivityFailed(failure)
        Error(crash), _ ->
          bridge.ActivityFailed(failure.new("activity crashed: " <> crash))
      }
  }
}

/// Core's `ActivityCancelReason` values.
fn cancel_reason(reason: Int) -> activity.CancelReason {
  case reason {
    0 -> activity.NotFound
    2 -> activity.TimedOut
    3 -> activity.WorkerShutdown
    4 -> activity.Paused
    5 -> activity.Reset
    _ -> activity.CancelRequested
  }
}

/// Answer Nexus tasks until the worker shuts down, each in its own process. `handle` turns
/// a task into its result and must not crash.
pub fn nexus_loop(
  worker: NifWorker,
  handle: fn(bridge.NexusTask) -> bridge.NexusResult,
) -> Result(Nil, String) {
  case bridge.poll_nexus(worker) {
    Error(bridge.Shutdown) -> Ok(Nil)
    Error(bridge.PollFailed(message)) -> Error(message)
    Ok(task) -> {
      process.spawn_unlinked(fn() {
        complete_nexus(worker, task.task_token, handle(task))
      })
      nexus_loop(worker, handle)
    }
  }
}

/// Answer a Nexus task. An answer the NIF or core will not take is logged and the task
/// reported as one the handler failed on, which the caller's server retries.
fn complete_nexus(
  worker: NifWorker,
  task_token: BitArray,
  result: bridge.NexusResult,
) -> Nil {
  case bridge.complete_nexus(worker, task_token, result) {
    Ok(Nil) -> Nil
    Error(reason) -> {
      bridge.log(
        bridge.Error,
        "temporal: Nexus task result rejected, failing the task: " <> reason,
        [],
      )
      let failed =
        bridge.NexusHandlerFailed(
          "INTERNAL",
          failure.new("the Nexus task's result was rejected: " <> reason),
        )
      case bridge.complete_nexus(worker, task_token, failed) {
        Ok(Nil) -> Nil
        Error(reason) ->
          bridge.log(
            bridge.Error,
            "temporal: could not fail the Nexus task: " <> reason,
            [],
          )
      }
    }
  }
}
