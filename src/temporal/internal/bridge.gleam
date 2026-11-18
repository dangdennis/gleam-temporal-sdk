//// The types exchanged with the Rust NIF and the NIF's functions.
////
//// `native/temporal_nif/src/convert.rs` builds and reads these values by their Erlang
//// representation, so a constructor's name, field order and field types are part of the
//// contract with that file: change both together.

import gleam/erlang/process.{type Pid}
import gleam/option.{type Option}
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}

pub type Connection

pub type NifWorker

pub type Activation {
  Activation(
    run_id: String,
    timestamp_ms: Int,
    is_replaying: Bool,
    history_length: Int,
    jobs: List(Job),
  )
}

pub type Job {
  InitializeWorkflow(
    workflow_type: String,
    workflow_id: String,
    arguments: List(Payload),
    randomness_seed: Int,
    attempt: Int,
  )
  FireTimer(seq: Int)
  ResolveActivity(seq: Int, result: Resolution)
  UpdateRandomSeed(seed: Int)
  QueryWorkflow(query_id: String, query_type: String, arguments: List(Payload))
  CancelWorkflow(reason: String)
  SignalWorkflow(signal_name: String, input: List(Payload))
  NotifyHasPatch(patch_id: String)
  ResolveChildWorkflowStart(seq: Int, result: ChildStart)
  ResolveChildWorkflow(seq: Int, result: Resolution)
  /// `reason` is core's `EvictionReason` enum value.
  RemoveFromCache(message: String, reason: Int)
  /// A job this SDK does not implement, such as a workflow update.
  UnsupportedJob(name: String)
}

/// How an activity or child workflow ended.
pub type Resolution {
  Completed(result: Option(Payload))
  Failed(failure: Failure)
  Cancelled(failure: Failure)
}

pub type ChildStart {
  ChildStarted(run_id: String)
  ChildStartFailed(workflow_id: String, cause: Int)
  ChildStartCancelled(failure: Failure)
}

pub type RetryPolicy {
  RetryPolicy(
    initial_interval_ms: Int,
    backoff_coefficient: Float,
    maximum_interval_ms: Option(Int),
    /// Zero means unlimited.
    maximum_attempts: Int,
    non_retryable_error_types: List(String),
  )
}

pub type Command {
  StartTimer(seq: Int, ms: Int)
  CancelTimer(seq: Int)
  ScheduleActivity(
    seq: Int,
    activity_id: String,
    activity_type: String,
    task_queue: String,
    arguments: List(Payload),
    schedule_to_close_ms: Option(Int),
    start_to_close_ms: Option(Int),
    heartbeat_timeout_ms: Option(Int),
    retry: Option(RetryPolicy),
  )
  RequestCancelActivity(seq: Int)
  RespondToQuery(query_id: String, result: Result(Payload, Failure))
  CompleteWorkflow(result: Option(Payload))
  FailWorkflow(failure: Failure)
  ContinueAsNew(
    workflow_type: String,
    task_queue: String,
    arguments: List(Payload),
  )
  CancelWorkflowExecution
  SetPatchMarker(patch_id: String, deprecated: Bool)
  StartChildWorkflow(
    seq: Int,
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    input: List(Payload),
  )
  CancelChildWorkflow(seq: Int)
}

pub type Completion {
  Success(run_id: String, commands: List(Command))
  /// Fails the workflow task, not the workflow: the server retries the task.
  TaskFailure(run_id: String, failure: Failure)
}

pub type ActivityTask {
  ActivityStart(
    task_token: BitArray,
    activity_type: String,
    activity_id: String,
    workflow_id: String,
    run_id: String,
    input: List(Payload),
    attempt: Int,
    heartbeat_details: List(Payload),
  )
  ActivityCancel(task_token: BitArray, reason: Int)
}

pub type ActivityResult {
  ActivityCompleted(result: Option(Payload))
  ActivityFailed(failure: Failure)
  ActivityCancelled(failure: Failure)
}

pub type PollError {
  /// The worker is shut down and has no more tasks of this kind.
  Shutdown
  PollFailed(message: String)
}

pub type StartError {
  AlreadyStarted
  StartFailed(message: String)
}

pub type Outcome {
  OutcomeCompleted(result: List(Payload))
  OutcomeFailed(failure: Failure)
  OutcomeCancelled
  OutcomeTerminated
  OutcomeTimedOut
}

@external(erlang, "temporal_ffi", "connect")
pub fn connect(
  target: String,
  namespace: String,
  identity: String,
  api_key: Option(String),
) -> Result(Connection, String)

@external(erlang, "temporal_ffi", "worker_new")
pub fn worker_new(
  connection: Connection,
  task_queue: String,
  max_cached_workflows: Int,
) -> Result(NifWorker, String)

@external(erlang, "temporal_ffi", "replay_worker_new")
pub fn replay_worker_new(
  task_queue: String,
  histories: List(#(String, BitArray)),
) -> Result(NifWorker, String)

@external(erlang, "temporal_ffi", "poll_workflow")
pub fn poll_workflow(worker: NifWorker) -> Result(Activation, PollError)

@external(erlang, "temporal_ffi", "complete_workflow")
pub fn complete_workflow(
  worker: NifWorker,
  completion: Completion,
) -> Result(Nil, String)

@external(erlang, "temporal_ffi", "poll_activity")
pub fn poll_activity(worker: NifWorker) -> Result(ActivityTask, PollError)

@external(erlang, "temporal_ffi", "complete_activity")
pub fn complete_activity(
  worker: NifWorker,
  task_token: BitArray,
  result: ActivityResult,
) -> Result(Nil, String)

@external(erlang, "temporal_ffi", "heartbeat")
pub fn heartbeat(worker: NifWorker, task_token: BitArray) -> Nil

@external(erlang, "temporal_ffi", "worker_shutdown")
pub fn worker_shutdown(worker: NifWorker) -> Nil

@external(erlang, "temporal_ffi", "start_workflow")
pub fn start_workflow(
  connection: Connection,
  workflow_id: String,
  workflow_type: String,
  task_queue: String,
  input: List(Payload),
) -> Result(String, StartError)

@external(erlang, "temporal_ffi", "signal_workflow")
pub fn signal_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
  signal_name: String,
  input: List(Payload),
) -> Result(Nil, String)

@external(erlang, "temporal_ffi", "query_workflow")
pub fn query_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
  query_type: String,
  arguments: List(Payload),
) -> Result(List(Payload), String)

@external(erlang, "temporal_ffi", "cancel_workflow")
pub fn cancel_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
) -> Result(Nil, String)

@external(erlang, "temporal_ffi", "terminate_workflow")
pub fn terminate_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
  reason: String,
) -> Result(Nil, String)

@external(erlang, "temporal_ffi", "workflow_result")
pub fn workflow_result(
  connection: Connection,
  workflow_id: String,
  run_id: String,
) -> Result(Outcome, String)

@external(erlang, "temporal_ffi", "fetch_history")
pub fn fetch_history(
  connection: Connection,
  workflow_id: String,
  run_id: String,
) -> Result(BitArray, String)

@external(erlang, "temporal_ffi", "send_activation")
pub fn send_activation(pid: Pid, activation: Activation) -> Nil

@external(erlang, "temporal_ffi", "receive_activation")
pub fn receive_activation() -> Activation

/// Run `body`, returning a description of the crash if it panics.
@external(erlang, "temporal_ffi", "rescue")
pub fn rescue(body: fn() -> a) -> Result(a, String)

@external(erlang, "temporal_ffi", "exit_normal")
pub fn exit_normal() -> a

@external(erlang, "temporal_ffi", "kill")
pub fn kill(pid: Pid) -> Nil
