//// The types exchanged with the Rust NIF and the NIF's functions.
////
//// The Rust side of each type is generated from this file: after changing a type here,
//// run `bin/codegen` and fix what the Rust compiler then points at. CI fails if the
//// generated file is stale. `Payload` and `Failure` live in their own modules and are
//// still matched by hand, in `native/temporal_nif/src/convert.rs`.

import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
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
    continue_as_new_suggested: Bool,
  )
}

pub type Job {
  InitializeWorkflow(
    workflow_type: String,
    workflow_id: String,
    arguments: List(Payload),
    randomness_seed: Int,
    attempt: Int,
    first_execution_run_id: String,
    /// Empty unless this run continues an earlier one.
    continued_from_run_id: String,
    memo: Dict(String, Payload),
    headers: Dict(String, Payload),
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
  /// A client's update. `protocol_instance_id` ties the workflow's answers to it.
  /// `run_validator` is false on replay, when the update is already known to be accepted.
  DoUpdate(
    id: String,
    protocol_instance_id: String,
    name: String,
    input: List(Payload),
    run_validator: Bool,
  )
  /// The outcome of signalling another workflow: `None` is success.
  ResolveExternalSignal(seq: Int, failure: Option(Failure))
  ResolveExternalCancel(seq: Int, failure: Option(Failure))
  ResolveNexusStart(seq: Int, result: NexusStart)
  ResolveNexus(seq: Int, result: Resolution)
}

/// How an activity or child workflow ended.
pub type Resolution {
  Completed(result: Option(Payload))
  Failed(failure: Failure)
  Cancelled(failure: Failure)
  /// A local activity that must be retried after a timer: core asks for one when the
  /// wait is too long to spend inside a workflow task.
  Backoff(attempt: Int, backoff_ms: Int, original_schedule_time_ms: Int)
}

pub type NexusStart {
  /// The operation runs on; its result follows in a `ResolveNexus` job.
  NexusStartedAsync(operation_token: String)
  /// The operation finished at once; its result follows in the same activation.
  NexusStartedSync
  NexusStartFailed(failure: Failure)
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
    schedule_to_start_ms: Option(Int),
    /// Core's `ActivityCancellationType` value.
    cancellation_type: Int,
    headers: Dict(String, Payload),
  )
  RequestCancelActivity(seq: Int)
  RespondToQuery(query_id: String, result: Result(Payload, Failure))
  CompleteWorkflow(result: Option(Payload))
  FailWorkflow(failure: Failure)
  ContinueAsNew(
    workflow_type: String,
    task_queue: String,
    arguments: List(Payload),
    options: ContinueAsNewOptions,
  )
  CancelWorkflowExecution
  SetPatchMarker(patch_id: String, deprecated: Bool)
  StartChildWorkflow(
    seq: Int,
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    input: List(Payload),
    options: ChildStartOptions,
  )
  CancelChildWorkflow(seq: Int)
  ScheduleLocalActivity(
    seq: Int,
    activity_id: String,
    activity_type: String,
    /// 1 for a first attempt; the attempt a `Backoff` named when retrying after one.
    attempt: Int,
    /// The time a `Backoff` named, when retrying after one.
    original_schedule_time_ms: Option(Int),
    arguments: List(Payload),
    schedule_to_close_ms: Option(Int),
    schedule_to_start_ms: Option(Int),
    start_to_close_ms: Option(Int),
    retry: Option(RetryPolicy),
    local_retry_threshold_ms: Option(Int),
    cancellation_type: Int,
    headers: Dict(String, Payload),
  )
  RequestCancelLocalActivity(seq: Int)
  SignalExternal(
    seq: Int,
    target: SignalTarget,
    signal_name: String,
    input: List(Payload),
  )
  CancelSignalExternal(seq: Int)
  CancelExternal(seq: Int, workflow_id: String, run_id: String, reason: String)
  UpdateAccepted(protocol_instance_id: String)
  UpdateRejected(protocol_instance_id: String, failure: Failure)
  UpdateCompleted(protocol_instance_id: String, result: Payload)
  ScheduleNexusOperation(
    seq: Int,
    endpoint: String,
    service: String,
    operation: String,
    input: Payload,
    schedule_to_close_ms: Option(Int),
    /// Core's `NexusOperationCancellationType` value.
    cancellation_type: Int,
  )
  CancelNexusOperation(seq: Int)
  UpsertSearchAttributes(attributes: Dict(String, Payload))
  UpsertMemo(memo: Dict(String, Payload))
}

pub type SignalTarget {
  /// An empty `run_id` means the workflow's current run.
  ExternalWorkflow(workflow_id: String, run_id: String)
  ChildWorkflowTarget(workflow_id: String)
}

pub type ContinueAsNewOptions {
  ContinueAsNewOptions(
    run_timeout_ms: Option(Int),
    task_timeout_ms: Option(Int),
    /// Empty keeps the memo of the run that ends.
    memo: Dict(String, Payload),
    /// Empty keeps the search attributes of the run that ends.
    search_attributes: Dict(String, Payload),
    /// The new run gets exactly these: core does not carry headers over.
    headers: Dict(String, Payload),
    /// `None` keeps the retry policy of the run that ends.
    retry: Option(RetryPolicy),
  )
}

pub type ChildStartOptions {
  ChildStartOptions(
    execution_timeout_ms: Option(Int),
    run_timeout_ms: Option(Int),
    task_timeout_ms: Option(Int),
    /// Core's `ParentClosePolicy` value.
    parent_close_policy: Int,
    /// The API's `WorkflowIdReusePolicy` value.
    id_reuse_policy: Int,
    retry: Option(RetryPolicy),
    memo: Dict(String, Payload),
    search_attributes: Dict(String, Payload),
    /// Core's `ChildWorkflowCancellationType` value.
    cancellation_type: Int,
    headers: Dict(String, Payload),
  )
}

pub type Completion {
  /// `versioning_behavior` is the API's `VersioningBehavior` value; 0 leaves it to the
  /// worker's default.
  Success(run_id: String, commands: List(Command), versioning_behavior: Int)
  /// Fails the workflow task, not the workflow: the server retries the task.
  TaskFailure(run_id: String, failure: Failure)
}

pub type ActivityTask {
  ActivityStart(
    task_token: BitArray,
    activity_type: String,
    activity_id: String,
    /// Empty, like `run_id` and `workflow_type`, for a standalone activity.
    workflow_id: String,
    run_id: String,
    input: List(Payload),
    attempt: Int,
    heartbeat_details: List(Payload),
    workflow_type: String,
    start_to_close_ms: Option(Int),
    heartbeat_timeout_ms: Option(Int),
    scheduled_time_ms: Int,
    is_local: Bool,
    headers: Dict(String, Payload),
    /// The policy the server retries the activity with.
    retry: Option(RetryPolicy),
    /// The run of a standalone activity: one a client started without a workflow. Empty
    /// for an activity a workflow scheduled.
    activity_run_id: String,
  )
  ActivityCancel(task_token: BitArray, reason: Int)
}

pub type WorkerOptions {
  WorkerOptions(
    max_cached_workflows: Int,
    max_concurrent_workflow_tasks: Int,
    max_concurrent_activities: Int,
    max_concurrent_nexus_tasks: Int,
    /// How many polls for each kind of task may be open at once.
    max_concurrent_workflow_task_polls: Int,
    max_concurrent_activity_task_polls: Int,
    max_concurrent_nexus_task_polls: Int,
    max_task_queue_activities_per_second: Option(Float),
    max_activities_per_second: Option(Float),
    enable_workflows: Bool,
    enable_activities: Bool,
    enable_nexus: Bool,
    deployment: Option(Deployment),
    shutdown_grace_ms: Int,
  )
}

pub type Deployment {
  Deployment(
    name: String,
    build_id: String,
    /// Whether the server routes tasks by deployment version.
    use_versioning: Bool,
    /// The API's `VersioningBehavior` value for workflows that do not set their own.
    default_behavior: Int,
  )
}

pub type NexusTask {
  NexusStartTask(
    task_token: BitArray,
    service: String,
    operation: String,
    request_id: String,
    input: Option(Payload),
    /// Where the service must report an asynchronous operation's result. Empty when the
    /// caller did not ask for a callback.
    callback_url: String,
    callback_headers: Dict(String, String),
    headers: Dict(String, String),
  )
  NexusCancelTask(
    task_token: BitArray,
    service: String,
    operation: String,
    operation_token: String,
  )
  /// The task timed out or the worker is shutting down; it must be acknowledged.
  NexusTaskCancelled(task_token: BitArray)
}

pub type NexusResult {
  NexusSyncSuccess(result: Payload)
  NexusAsyncSuccess(operation_token: String)
  /// The operation itself failed or was cancelled: the caller's operation ends.
  NexusOperationFailed(failure: Failure)
  /// The handler could not process the request. `error_type` is one of the Nexus error
  /// types, such as `BAD_REQUEST`, `NOT_FOUND` or `INTERNAL`; the caller retries the
  /// retryable ones.
  NexusHandlerFailed(error_type: String, failure: Failure)
  NexusCancelAcknowledged
  NexusTaskCancelAcknowledged
}

@external(erlang, "temporal_ffi", "poll_nexus")
pub fn poll_nexus(worker: NifWorker) -> Result(NexusTask, PollError)

@external(erlang, "temporal_ffi", "complete_nexus")
pub fn complete_nexus(
  worker: NifWorker,
  task_token: BitArray,
  result: NexusResult,
) -> Result(Nil, String)

pub type ActivityResult {
  ActivityCompleted(result: Option(Payload))
  ActivityFailed(failure: Failure)
  ActivityCancelled(failure: Failure)
  /// The activity will be finished later, from elsewhere, by its task token.
  ActivityWillCompleteAsync
}

pub type PollError {
  /// The worker is shut down and has no more tasks of this kind.
  Shutdown
  PollFailed(message: String)
}

/// A failed request: the gRPC status code and message.
pub type RpcError {
  RpcError(code: Int, message: String)
}

/// The least severe of core's own log lines to print.
pub type CoreLogLevel {
  CoreLogOff
  CoreLogError
  CoreLogWarning
  CoreLogInfo
  CoreLogDebug
}

pub type Tls {
  Tls(
    server_root_ca: Option(BitArray),
    domain: Option(String),
    /// Certificate and private key, both PEM.
    client_certificate: Option(#(BitArray, BitArray)),
  )
}

pub type StartRequest {
  StartRequest(
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    input: List(Payload),
    /// The API's `WorkflowIdReusePolicy` value.
    id_reuse_policy: Int,
    /// The API's `WorkflowIdConflictPolicy` value.
    id_conflict_policy: Int,
    execution_timeout_ms: Option(Int),
    run_timeout_ms: Option(Int),
    task_timeout_ms: Option(Int),
    retry: Option(RetryPolicy),
    memo: Dict(String, Payload),
    search_attributes: Dict(String, Payload),
    start_delay_ms: Option(Int),
    /// Empty for none.
    cron_schedule: String,
    /// Makes retries of the same start idempotent. Empty for a random one.
    request_id: String,
    /// Nexus callbacks to invoke when the workflow closes: `#(url, headers)`.
    callbacks: List(#(String, Dict(String, String))),
    headers: Dict(String, Payload),
  )
}

/// How far an update has got: 2 is accepted, 3 is completed (the API's
/// `UpdateWorkflowExecutionLifecycleStage`).
pub type UpdateOutcome {
  UpdateSucceeded(result: List(Payload))
  UpdateFailed(failure: Failure)
  /// Accepted but not finished: ask again with `poll_update`.
  UpdatePending
}

/// The server's description of a run.
pub type Execution {
  Execution(
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    task_queue: String,
    /// The API's `WorkflowExecutionStatus` value.
    status: Int,
    start_time_ms: Int,
    close_time_ms: Option(Int),
    history_length: Int,
    memo: Dict(String, Payload),
    search_attributes: Dict(String, Payload),
    parent_workflow_id: Option(String),
  )
}

pub type Outcome {
  OutcomeCompleted(result: List(Payload))
  OutcomeFailed(failure: Failure)
  OutcomeCancelled
  OutcomeTerminated
  OutcomeTimedOut
}

pub type ConnectOptions {
  ConnectOptions(
    target: String,
    namespace: String,
    identity: String,
    api_key: Option(String),
    tls: Option(Tls),
    /// HTTP/2 keep-alive pings: `#(interval_ms, timeout_ms)`.
    keep_alive: Option(#(Int, Int)),
    connect_timeout_ms: Int,
  )
}

@external(erlang, "temporal_ffi", "connect")
pub fn connect(options: ConnectOptions) -> Result(Connection, String)

pub type ScheduleCalendar {
  /// Each field is a list of `#(start, end, step)` ranges.
  ScheduleCalendar(
    second: List(#(Int, Int, Int)),
    minute: List(#(Int, Int, Int)),
    hour: List(#(Int, Int, Int)),
    day_of_month: List(#(Int, Int, Int)),
    month: List(#(Int, Int, Int)),
    year: List(#(Int, Int, Int)),
    day_of_week: List(#(Int, Int, Int)),
    comment: String,
  )
}

pub type ScheduleSpec {
  ScheduleSpec(
    /// `#(every_ms, offset_ms)`.
    intervals: List(#(Int, Int)),
    calendars: List(ScheduleCalendar),
    cron_expressions: List(String),
    /// Empty for UTC.
    timezone: String,
    jitter_ms: Option(Int),
  )
}

pub type Schedule {
  Schedule(
    spec: ScheduleSpec,
    action: StartRequest,
    /// The API's `ScheduleOverlapPolicy` value.
    overlap_policy: Int,
    catchup_window_ms: Option(Int),
    pause_on_failure: Bool,
    paused: Bool,
    note: String,
  )
}

pub type ScheduleInfo {
  ScheduleInfo(
    action_count: Int,
    /// `#(scheduled_time_ms, workflow_id)`, oldest first.
    recent_actions: List(#(Int, String)),
    next_action_times_ms: List(Int),
    running_workflows: Int,
  )
}

pub type SchedulePatch {
  Pause(note: String)
  Unpause(note: String)
  Trigger(overlap_policy: Int)
  Backfill(from_ms: Int, to_ms: Int, overlap_policy: Int)
}

/// Create the schedule.
@external(erlang, "temporal_ffi", "schedule_create")
pub fn schedule_create(
  connection: Connection,
  schedule_id: String,
  schedule: Schedule,
  trigger_immediately: Bool,
) -> Result(Nil, RpcError)

/// The schedule, what it has done, and the schedule as the server stores it: opaque bytes
/// for `schedule_update`.
@external(erlang, "temporal_ffi", "schedule_describe")
pub fn schedule_describe(
  connection: Connection,
  schedule_id: String,
) -> Result(#(Schedule, ScheduleInfo, BitArray), RpcError)

/// Store `changed` in place of `described`, the schedule `schedule_describe` answered with
/// `stored`. Only the fields that differ between the two are written; everything else,
/// including what `Schedule` does not model, stays as the server has it. Rejected if the
/// schedule changed since it was described.
@external(erlang, "temporal_ffi", "schedule_update")
pub fn schedule_update(
  connection: Connection,
  schedule_id: String,
  stored: BitArray,
  described: Schedule,
  changed: Schedule,
) -> Result(Nil, RpcError)

@external(erlang, "temporal_ffi", "schedule_delete")
pub fn schedule_delete(
  connection: Connection,
  schedule_id: String,
) -> Result(Nil, RpcError)

@external(erlang, "temporal_ffi", "schedule_patch")
pub fn schedule_patch(
  connection: Connection,
  schedule_id: String,
  patch: SchedulePatch,
) -> Result(Nil, RpcError)

/// `#(schedule_id, workflow_type, paused, note)` for every schedule.
@external(erlang, "temporal_ffi", "schedule_list")
pub fn schedule_list(
  connection: Connection,
) -> Result(List(#(String, String, Bool, String)), RpcError)

pub type Server

pub type ServerKind {
  TimeSkipping
  Dev
}

pub type TimeOp {
  Lock
  Unlock
  Sleep(ms: Int)
  Now
}

/// Start a test server and return it with its `http://host:port` target.
@external(erlang, "temporal_ffi", "server_start")
pub fn server_start(
  kind: ServerKind,
  executable: Option(String),
) -> Result(#(Server, String), String)

@external(erlang, "temporal_ffi", "server_stop")
pub fn server_stop(server: Server) -> Nil

@external(erlang, "temporal_ffi", "time_control")
pub fn time_control(
  connection: Connection,
  operation: TimeOp,
) -> Result(Int, RpcError)

@external(erlang, "temporal_ffi", "worker_new")
pub fn worker_new(
  connection: Connection,
  task_queue: String,
  options: WorkerOptions,
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
pub fn heartbeat(
  worker: NifWorker,
  task_token: BitArray,
  details: List(Payload),
) -> Nil

/// Tell a running activity process that cancellation was requested.
@external(erlang, "temporal_ffi", "send_activity_cancel")
pub fn send_activity_cancel(pid: Pid, reason: Int) -> Nil

/// The cancellation reason delivered to the calling activity process, if any.
@external(erlang, "temporal_ffi", "activity_cancel_reason")
pub fn activity_cancel_reason() -> Option(Int)

/// Like `rescue`, but also catches exits, for code whose process must report its outcome.
@external(erlang, "temporal_ffi", "rescue_all")
pub fn rescue_all(body: fn() -> a) -> Result(a, String)

/// Log through Erlang's `logger`. Metadata values keep their types; when a key is given
/// twice the later entry wins.
@external(erlang, "temporal_ffi", "log")
pub fn log(
  level: LogLevel,
  message: String,
  metadata: List(#(String, Dynamic)),
) -> Nil

pub type LogLevel {
  Debug
  Info
  Warning
  Error
}

/// Stop polling and wait until core's worker has shut down: every task in flight completed
/// and every poll answered with `Shutdown`. `limit_ms` bounds the wait; zero waits for as
/// long as it takes.
@external(erlang, "temporal_ffi", "worker_shutdown")
pub fn worker_shutdown(worker: NifWorker, limit_ms: Int) -> Nil

/// A switch that any process can read and that only ever goes from off to on.
pub type Flag

@external(erlang, "temporal_ffi", "new_flag")
pub fn new_flag() -> Flag

@external(erlang, "temporal_ffi", "raise_flag")
pub fn raise_flag(flag: Flag) -> Nil

@external(erlang, "temporal_ffi", "flag_raised")
pub fn flag_raised(flag: Flag) -> Bool

/// Block until the operating system asks the VM to stop with SIGTERM. Returns at once if
/// it already has.
@external(erlang, "temporal_signal", "await")
pub fn await_shutdown_signal() -> Nil

/// Take SIGTERM over from the VM's default handler, which would stop the VM at once.
@external(erlang, "temporal_signal", "install")
pub fn trap_shutdown_signal() -> Nil

/// Start a workflow and return its run id. With `signal`, signal the workflow instead,
/// starting it first if it is not running.
@external(erlang, "temporal_ffi", "start_workflow")
pub fn start_workflow(
  connection: Connection,
  request: StartRequest,
  signal: Option(#(String, List(Payload))),
) -> Result(String, RpcError)

@external(erlang, "temporal_ffi", "signal_workflow")
pub fn signal_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
  signal_name: String,
  input: List(Payload),
) -> Result(Nil, RpcError)

@external(erlang, "temporal_ffi", "query_workflow")
pub fn query_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
  query_type: String,
  arguments: List(Payload),
) -> Result(List(Payload), RpcError)

@external(erlang, "temporal_ffi", "cancel_workflow")
pub fn cancel_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
) -> Result(Nil, RpcError)

@external(erlang, "temporal_ffi", "terminate_workflow")
pub fn terminate_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
  reason: String,
) -> Result(Nil, RpcError)

@external(erlang, "temporal_ffi", "workflow_result")
pub fn workflow_result(
  connection: Connection,
  workflow_id: String,
  run_id: String,
) -> Result(Outcome, RpcError)

@external(erlang, "temporal_ffi", "fetch_history")
pub fn fetch_history(
  connection: Connection,
  workflow_id: String,
  run_id: String,
) -> Result(BitArray, RpcError)

/// Send an update and wait until it reaches `wait_stage`. `target` is the workflow id, run
/// id, update id (empty for a generated one) and update name. Answers the run id the
/// update landed on, the update's id and its outcome so far.
@external(erlang, "temporal_ffi", "update_workflow")
pub fn update_workflow(
  connection: Connection,
  target: #(String, String, String, String),
  arguments: List(Payload),
  wait_stage: Int,
) -> Result(#(String, String, UpdateOutcome), RpcError)

/// Send an update, starting the workflow first if it is not running. `update` is the
/// update's id (empty for a generated one) and name. Answers the workflow's run id, the
/// update's id and its outcome.
@external(erlang, "temporal_ffi", "update_with_start")
pub fn update_with_start(
  connection: Connection,
  request: StartRequest,
  update: #(String, String),
  arguments: List(Payload),
) -> Result(#(String, String, UpdateOutcome), RpcError)

/// Finish an activity that deferred its completion.
@external(erlang, "temporal_ffi", "complete_activity_async")
pub fn complete_activity_async(
  connection: Connection,
  task_token: BitArray,
  result: ActivityResult,
) -> Result(Nil, RpcError)

/// Mark the activity running in the calling process as one that will be completed later.
@external(erlang, "temporal_ffi", "defer_activity")
pub fn defer_activity() -> Nil

@external(erlang, "temporal_ffi", "activity_deferred")
pub fn activity_deferred() -> Bool

/// Wait for an update to complete.
@external(erlang, "temporal_ffi", "poll_update")
pub fn poll_update(
  connection: Connection,
  workflow_id: String,
  run_id: String,
  update_id: String,
) -> Result(UpdateOutcome, RpcError)

@external(erlang, "temporal_ffi", "describe_workflow")
pub fn describe_workflow(
  connection: Connection,
  workflow_id: String,
  run_id: String,
) -> Result(Execution, RpcError)

@external(erlang, "temporal_ffi", "list_workflows")
pub fn list_workflows(
  connection: Connection,
  query: String,
  limit: Int,
) -> Result(List(Execution), RpcError)

@external(erlang, "temporal_ffi", "count_workflows")
pub fn count_workflows(
  connection: Connection,
  query: String,
) -> Result(Int, RpcError)

// ---- standalone activities ----

pub type ActivityStartRequest {
  ActivityStartRequest(
    activity_id: String,
    activity_type: String,
    task_queue: String,
    input: List(Payload),
    schedule_to_close_ms: Option(Int),
    schedule_to_start_ms: Option(Int),
    start_to_close_ms: Option(Int),
    heartbeat_timeout_ms: Option(Int),
    retry: Option(RetryPolicy),
    /// The API's `ActivityIdReusePolicy` value.
    id_reuse_policy: Int,
    /// The API's `ActivityIdConflictPolicy` value.
    id_conflict_policy: Int,
    search_attributes: Dict(String, Payload),
    start_delay_ms: Option(Int),
    headers: Dict(String, Payload),
  )
}

/// How a standalone activity ended. A cancellation, termination or timeout is a failure
/// of that kind.
pub type ActivityOutcome {
  ActivitySucceeded(result: List(Payload))
  ActivityEnded(failure: Failure)
}

/// The server's description of a standalone activity.
pub type ActivityExecution {
  ActivityExecution(
    activity_id: String,
    run_id: String,
    activity_type: String,
    task_queue: String,
    /// The API's `ActivityExecutionStatus` value.
    status: Int,
    /// The API's `PendingActivityState` value; 0 once the activity has closed.
    run_state: Int,
    attempt: Int,
    schedule_time_ms: Int,
    close_time_ms: Option(Int),
    last_failure: Option(Failure),
    heartbeat_details: List(Payload),
    last_worker_identity: String,
    search_attributes: Dict(String, Payload),
  )
}

/// A standalone activity as a list shows it.
pub type ActivitySummary {
  ActivitySummary(
    activity_id: String,
    run_id: String,
    activity_type: String,
    task_queue: String,
    status: Int,
    schedule_time_ms: Int,
    close_time_ms: Option(Int),
    search_attributes: Dict(String, Payload),
  )
}

/// Start a standalone activity and return its run id.
@external(erlang, "temporal_ffi", "start_activity")
pub fn start_activity(
  connection: Connection,
  request: ActivityStartRequest,
) -> Result(String, RpcError)

/// Wait for a standalone activity to close. An empty `run_id` means the latest run.
@external(erlang, "temporal_ffi", "activity_result")
pub fn activity_result(
  connection: Connection,
  activity_id: String,
  run_id: String,
) -> Result(ActivityOutcome, RpcError)

@external(erlang, "temporal_ffi", "describe_activity")
pub fn describe_activity(
  connection: Connection,
  activity_id: String,
  run_id: String,
) -> Result(ActivityExecution, RpcError)

@external(erlang, "temporal_ffi", "cancel_activity")
pub fn cancel_activity(
  connection: Connection,
  activity_id: String,
  run_id: String,
  reason: String,
) -> Result(Nil, RpcError)

@external(erlang, "temporal_ffi", "terminate_activity")
pub fn terminate_activity(
  connection: Connection,
  activity_id: String,
  run_id: String,
  reason: String,
) -> Result(Nil, RpcError)

@external(erlang, "temporal_ffi", "list_activities")
pub fn list_activities(
  connection: Connection,
  query: String,
  limit: Int,
) -> Result(List(ActivitySummary), RpcError)

@external(erlang, "temporal_ffi", "count_activities")
pub fn count_activities(
  connection: Connection,
  query: String,
) -> Result(Int, RpcError)

@external(erlang, "temporal_ffi", "send_activation")
pub fn send_activation(pid: Pid, activation: Activation) -> Nil

@external(erlang, "temporal_ffi", "receive_activation")
pub fn receive_activation() -> Activation

/// Run `body`, returning a description of the crash if it panics.
@external(erlang, "temporal_ffi", "rescue")
pub fn rescue(body: fn() -> a) -> Result(a, String)

/// A slot in the calling process that holds at most one value.
pub type Cell(a)

@external(erlang, "temporal_ffi", "new_cell")
pub fn new_cell() -> Cell(a)

@external(erlang, "temporal_ffi", "read_cell")
pub fn read_cell(cell: Cell(a)) -> Option(a)

@external(erlang, "temporal_ffi", "write_cell")
pub fn write_cell(cell: Cell(a), value: a) -> Nil

/// Make the calling workflow run process exit when `owner` does. Takes effect the next
/// time the process waits for an activation.
@external(erlang, "temporal_ffi", "follow")
pub fn follow(owner: Pid) -> Nil

@external(erlang, "temporal_ffi", "exit_normal")
pub fn exit_normal() -> a

@external(erlang, "temporal_ffi", "kill")
pub fn kill(pid: Pid) -> Nil
