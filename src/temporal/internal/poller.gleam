//// The loops that take tasks from core and hand them to workflow and activity processes.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid}
import gleam/list
import gleam/option.{type Option}
import temporal/activity
import temporal/codec.{type PayloadCodec}
import temporal/failure
import temporal/internal/bridge.{type NifWorker}
import temporal/internal/runtime
import temporal/internal/time
import temporal/internal/transcode
import temporal/middleware.{type Outbound}

/// A run that core removed from the cache, with core's `EvictionReason` value.
pub type Eviction {
  Eviction(run_id: String, reason: Int, message: String)
}

/// Dispatch workflow activations until the worker shuts down. Returns the evictions seen,
/// newest first, when `record_evictions` is set; a long-lived worker leaves it unset so the
/// list cannot grow without bound.
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
      let complete = fn(completion) {
        let completion = transcode.encode_completion(codec, completion)
        case bridge.complete_workflow(worker, completion) {
          Ok(Nil) -> Nil
          Error(reason) ->
            bridge.log(
              bridge.Error,
              "temporal: workflow completion rejected: " <> reason,
              dict.new(),
            )
        }
      }
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
/// activities arrive here too.
pub fn activity_loop(
  worker: NifWorker,
  activities: Dict(String, activity.Handler),
  task_queue: String,
  codec: Option(PayloadCodec),
  cancellation_grace_ms: Option(Int),
) -> Nil {
  do_activity_loop(
    worker,
    activities,
    task_queue,
    codec,
    cancellation_grace_ms,
    dict.new(),
  )
}

fn do_activity_loop(
  worker: NifWorker,
  activities: Dict(String, activity.Handler),
  task_queue: String,
  codec: Option(PayloadCodec),
  cancellation_grace_ms: Option(Int),
  running: Dict(BitArray, Pid),
) -> Nil {
  let running = dict.filter(running, fn(_, pid) { process.is_alive(pid) })
  let continue = fn(running) {
    do_activity_loop(
      worker,
      activities,
      task_queue,
      codec,
      cancellation_grace_ms,
      running,
    )
  }
  case bridge.poll_activity(worker) {
    Error(bridge.Shutdown) -> Nil
    // Crash, so that the worker's manager sees it and restarts the loop.
    Error(bridge.PollFailed(message)) ->
      panic as { "activity polling failed: " <> message }
    Ok(bridge.ActivityStart(task_token:, ..) as task) ->
      case transcode.decode_activity_task(codec, task) {
        Ok(task) -> {
          let pid = run_activity(worker, activities, task_queue, codec, task)
          continue(dict.insert(running, task_token, pid))
        }
        Error(reason) -> {
          let _ =
            bridge.complete_activity(
              worker,
              task_token,
              bridge.ActivityFailed(failure.new(
                "payload codec could not decode the activity task: " <> reason,
              )),
            )
          continue(running)
        }
      }
    Ok(bridge.ActivityCancel(task_token:, reason:)) -> {
      // The activity decides how to stop: it sees the request through
      // `activity.is_cancelled` and reports its own outcome. An activity that has already
      // finished needs nothing.
      case dict.get(running, task_token) {
        Ok(pid) -> {
          bridge.send_activity_cancel(pid, reason)
          enforce_grace(worker, task_token, pid, cancellation_grace_ms)
        }
        Error(Nil) -> Nil
      }
      continue(running)
    }
  }
}

/// Kill a cancelled activity that outstays its grace period and report it as cancelled, so
/// that a stuck activity cannot hold its slot on the worker for good.
fn enforce_grace(
  worker: NifWorker,
  task_token: BitArray,
  pid: Pid,
  grace_ms: Option(Int),
) -> Nil {
  case grace_ms {
    option.None -> Nil
    option.Some(grace_ms) -> {
      process.spawn_unlinked(fn() {
        process.sleep(grace_ms)
        case process.is_alive(pid) {
          False -> Nil
          True -> {
            bridge.kill(pid)
            let _ =
              bridge.complete_activity(
                worker,
                task_token,
                bridge.ActivityCancelled(failure.cancelled()),
              )
            Nil
          }
        }
      })
      Nil
    }
  }
}

/// Start the process that runs one activity and reports its outcome.
fn run_activity(
  worker: NifWorker,
  activities: Dict(String, activity.Handler),
  task_queue: String,
  codec: Option(PayloadCodec),
  task: bridge.ActivityTask,
) -> Pid {
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
  ) = task
  let ctx =
    activity.new_context(
      info: activity.Info(
        activity_id:,
        activity_type:,
        workflow_id:,
        run_id:,
        workflow_type:,
        task_queue:,
        attempt:,
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
    )
  process.spawn_unlinked(fn() {
    let result = case dict.get(activities, activity_type) {
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
      // Exits are caught too: this process must always report an outcome, or the
      // activity would hang until its timeout.
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
    let _ =
      bridge.complete_activity(
        worker,
        task_token,
        transcode.encode_activity_result(codec, result),
      )
    Nil
  })
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
) -> Nil {
  case bridge.poll_nexus(worker) {
    Error(bridge.Shutdown) -> Nil
    Error(bridge.PollFailed(message)) ->
      panic as { "nexus polling failed: " <> message }
    Ok(task) -> {
      process.spawn_unlinked(fn() {
        let _ = bridge.complete_nexus(worker, task.task_token, handle(task))
        Nil
      })
      nexus_loop(worker, handle)
    }
  }
}
