//// The loops that take tasks from core and hand them to workflow and activity processes.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid}
import gleam/io
import gleam/list
import gleam/option
import temporal/activity
import temporal/failure
import temporal/internal/bridge.{type NifWorker}
import temporal/internal/runtime
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
  record_evictions: Bool,
) -> Result(List(Eviction), String) {
  do_workflow_loop(
    worker,
    workflows,
    task_queue,
    outbound,
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
  record_evictions: Bool,
  runs: Dict(String, Pid),
  evictions: List(Eviction),
) -> Result(List(Eviction), String) {
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
        case bridge.complete_workflow(worker, completion) {
          Ok(Nil) -> Nil
          Error(reason) ->
            io.println_error(
              "temporal: workflow completion rejected: " <> reason,
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
      let runs = case existing, eviction {
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
          complete(bridge.Success(activation.run_id, []))
          dict.delete(runs, activation.run_id)
        }
        Error(Nil), Error(Nil) -> {
          let pid =
            process.spawn_unlinked(fn() {
              runtime.run(workflows, task_queue, outbound, complete)
            })
          bridge.send_activation(pid, activation)
          dict.insert(runs, activation.run_id, pid)
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
        record_evictions,
        runs,
        evictions,
      )
    }
  }
}

/// Run activity tasks until the worker shuts down, each in its own process.
pub fn activity_loop(
  worker: NifWorker,
  activities: Dict(String, activity.Handler),
  task_queue: String,
) -> Nil {
  do_activity_loop(worker, activities, task_queue, dict.new())
}

fn do_activity_loop(
  worker: NifWorker,
  activities: Dict(String, activity.Handler),
  task_queue: String,
  running: Dict(BitArray, Pid),
) -> Nil {
  let running = dict.filter(running, fn(_, pid) { process.is_alive(pid) })
  case bridge.poll_activity(worker) {
    Error(bridge.Shutdown) -> Nil
    Error(bridge.PollFailed(message)) ->
      io.println_error("temporal: activity polling failed: " <> message)
    Ok(bridge.ActivityStart(
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
    )) -> {
      let ctx =
        activity.Context(
          info: activity.Info(
            activity_id:,
            activity_type:,
            workflow_id:,
            run_id:,
            workflow_type:,
            task_queue:,
            attempt:,
            heartbeat_details:,
            start_to_close_ms:,
            heartbeat_timeout_ms:,
            scheduled_time_ms:,
          ),
          heartbeat: fn(details) {
            bridge.heartbeat(worker, task_token, details)
          },
          cancel_reason: fn() {
            option.map(bridge.activity_cancel_reason(), cancel_reason)
          },
        )
      let pid =
        process.spawn_unlinked(fn() {
          let result = case dict.get(activities, activity_type) {
            Error(Nil) ->
              bridge.ActivityFailed(
                failure.new(
                  "activity type \""
                  <> activity_type
                  <> "\" is not registered on this worker",
                )
                |> failure.with_type("NotFound"),
              )
            // Exits are caught too: this process must always report an outcome, or the
            // activity would hang until its timeout.
            Ok(handler) ->
              case bridge.rescue_all(fn() { handler.run(ctx, input) }) {
                Ok(Ok(payload)) -> bridge.ActivityCompleted(payload)
                Ok(Error(
                  failure.Failure(kind: failure.Cancelled, ..) as failure,
                )) -> bridge.ActivityCancelled(failure)
                Ok(Error(failure)) -> bridge.ActivityFailed(failure)
                Error(crash) ->
                  bridge.ActivityFailed(failure.new(
                    "activity crashed: " <> crash,
                  ))
              }
          }
          let _ = bridge.complete_activity(worker, task_token, result)
          Nil
        })
      do_activity_loop(
        worker,
        activities,
        task_queue,
        dict.insert(running, task_token, pid),
      )
    }
    Ok(bridge.ActivityCancel(task_token:, reason:)) -> {
      // The activity decides how to stop: it sees the request through
      // `activity.is_cancelled` and reports its own outcome. An activity that has already
      // finished needs nothing.
      case dict.get(running, task_token) {
        Ok(pid) -> bridge.send_activity_cancel(pid, reason)
        Error(Nil) -> Nil
      }
      do_activity_loop(worker, activities, task_queue, running)
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
