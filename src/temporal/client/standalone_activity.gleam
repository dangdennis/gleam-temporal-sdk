//// Run an activity directly from a client, with no workflow around it: a standalone
//// activity. The server schedules, retries and times it out as it would for a workflow,
//// and any worker with the activity registered on the task queue runs it.
////
//// ```gleam
//// let options =
////   standalone_activity.options(
////     activity_id: "charge-order-42",
////     task_queue: "payments",
////     start_to_close: duration.seconds(30),
////   )
////   |> standalone_activity.with_retry(retry.policy(maximum_attempts: 3))
//// let assert Ok(receipt) =
////   standalone_activity.execute(client, charge(), order, options)
//// ```
////
//// The activity is defined and registered like any other: see `temporal/activity`. In
//// its code `activity.is_standalone` tells the two apart.
////
//// The server must have standalone activities enabled. Temporal CLI 1.10's development
//// server has; elsewhere it is the dynamic config `activity.enableStandalone`. Without
//// it every call here fails with `RequestFailed`.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import temporal/activity
import temporal/client.{type Client, type Error}
import temporal/codec.{type Codec, type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/time
import temporal/internal/transcode
import temporal/retry
import temporal/search_attributes

/// A standalone activity and the codec for its result.
pub opaque type Handle(output) {
  Handle(
    client: Client,
    activity_id: String,
    /// `None` refers to the latest run with the id.
    run_id: Option(String),
    output: Codec(output),
  )
}

/// Whether an activity id may be used again once the activity with it has closed.
pub type IdReusePolicy {
  /// The default.
  AllowDuplicate
  /// Only if the last activity with the id did not complete successfully.
  AllowDuplicateFailedOnly
  /// Never.
  RejectDuplicate
}

/// What to do when an activity with the same id is still running.
pub type IdConflictPolicy {
  /// Refuse to start. The default.
  FailIfRunning
  /// Return a handle to the running activity instead of starting one.
  UseExisting
}

/// How a standalone activity is started. Build with `options` and the `with_` functions
/// that take one.
pub type Options {
  Options(
    /// The activity's identity. No two open standalone activities in a namespace share
    /// an id.
    activity_id: String,
    task_queue: String,
    /// Longest a single attempt may run.
    start_to_close: Duration,
    /// Longest the activity may take across all retries.
    schedule_to_close: Option(Duration),
    /// Longest the activity may wait for a worker to pick it up.
    schedule_to_start: Option(Duration),
    /// The longest gap between heartbeats before the attempt is failed. `None` never
    /// fails an attempt for silence.
    heartbeat_timeout: Option(Duration),
    /// `None` uses the server's default policy, which retries without limit.
    retry: Option(retry.Policy),
    /// Whether the id may be used again once an activity with it has closed.
    id_reuse_policy: IdReusePolicy,
    /// What to do if an activity with the id is still running.
    id_conflict_policy: IdConflictPolicy,
    /// Indexed values, for finding the activity with `list`.
    search_attributes: Dict(String, search_attributes.Value),
    /// Wait this long before the first attempt.
    start_delay: Option(Duration),
    /// Context, such as a trace id, that travels beside the argument to the activity.
    headers: Dict(String, Payload),
  )
}

/// Where a standalone activity is in its life.
pub type Status {
  /// Not closed: waiting for a worker, running an attempt, or waiting to retry.
  Running
  Completed
  Failed
  Cancelled
  Terminated
  TimedOut
  /// An operator paused the activity.
  Paused
  /// A status this SDK does not know.
  UnknownStatus
}

/// What an activity that has not closed is doing.
pub type RunState {
  /// Waiting for a worker to pick up the next attempt, or for the retry interval.
  Scheduled
  /// A worker is running an attempt.
  Started
  /// Cancellation was requested and the worker has not answered yet.
  CancelRequested
}

/// A standalone activity as the server describes it.
pub type Description {
  Description(
    activity_id: String,
    run_id: String,
    activity_type: String,
    task_queue: String,
    status: Status,
    /// `None` once the activity has closed, or while an operator has it paused.
    run_state: Option(RunState),
    /// The attempt that is running or ran last. Starts at 1.
    attempt: Int,
    /// When the activity was started.
    schedule_time: Timestamp,
    /// `None` while the activity is open.
    close_time: Option(Timestamp),
    /// How the latest failed attempt failed. `None` if no attempt has.
    last_failure: Option(Failure),
    /// What the activity last passed to `activity.heartbeat_with`.
    heartbeat_details: List(Payload),
    /// The worker that last picked the activity up. Empty if none has.
    last_worker_identity: String,
    /// The raw payloads; their `data` is JSON.
    search_attributes: Dict(String, Payload),
  )
}

/// A standalone activity as `list` shows it. `describe` says more.
pub type Summary {
  Summary(
    activity_id: String,
    run_id: String,
    activity_type: String,
    task_queue: String,
    status: Status,
    schedule_time: Timestamp,
    /// `None` while the activity is open.
    close_time: Option(Timestamp),
    /// The raw payloads; their `data` is JSON.
    search_attributes: Dict(String, Payload),
  )
}

/// Options with the defaults: the server's retry policy, which retries without limit,
/// no other timeout, and a refusal to start while an activity with the same id is running.
pub fn options(
  activity_id activity_id: String,
  task_queue task_queue: String,
  start_to_close start_to_close: Duration,
) -> Options {
  Options(
    activity_id:,
    task_queue:,
    start_to_close:,
    schedule_to_close: None,
    schedule_to_start: None,
    heartbeat_timeout: None,
    retry: None,
    id_reuse_policy: AllowDuplicate,
    id_conflict_policy: FailIfRunning,
    search_attributes: dict.new(),
    start_delay: None,
    headers: dict.new(),
  )
}

/// The longest the activity may take across all retries.
pub fn with_schedule_to_close(options: Options, timeout: Duration) -> Options {
  Options(..options, schedule_to_close: Some(timeout))
}

/// The longest the activity may wait for a worker to pick it up.
pub fn with_schedule_to_start(options: Options, timeout: Duration) -> Options {
  Options(..options, schedule_to_start: Some(timeout))
}

/// Fail an attempt that goes this long without a heartbeat. Also what lets the activity
/// learn it was cancelled: the server tells activities that heartbeat.
pub fn with_heartbeat_timeout(options: Options, timeout: Duration) -> Options {
  Options(..options, heartbeat_timeout: Some(timeout))
}

/// Retry the activity by this policy instead of the server's default, which retries
/// without limit.
pub fn with_retry(options: Options, policy: retry.Policy) -> Options {
  Options(..options, retry: Some(policy))
}

/// Whether the id may be used again once an activity with it has closed.
pub fn with_id_reuse_policy(
  options: Options,
  policy: IdReusePolicy,
) -> Options {
  Options(..options, id_reuse_policy: policy)
}

/// What to do if an activity with the id is still running.
pub fn with_id_conflict_policy(
  options: Options,
  policy: IdConflictPolicy,
) -> Options {
  Options(..options, id_conflict_policy: policy)
}

/// Indexed values, for finding the activity with `list`.
pub fn with_search_attributes(
  options: Options,
  attributes: Dict(String, search_attributes.Value),
) -> Options {
  Options(..options, search_attributes: attributes)
}

/// Wait this long before the activity's first attempt.
pub fn with_start_delay(options: Options, delay: Duration) -> Options {
  Options(..options, start_delay: Some(delay))
}

/// Context, such as a trace id, that travels beside the argument to the activity, which
/// reads it from `activity.info`.
pub fn with_headers(
  options: Options,
  headers: Dict(String, Payload),
) -> Options {
  Options(..options, headers:)
}

/// The start request for an activity type and its options, before the codec is applied.
@internal
pub fn start_request(
  activity_type: String,
  input: List(Payload),
  options: Options,
) -> bridge.ActivityStartRequest {
  bridge.ActivityStartRequest(
    activity_id: options.activity_id,
    activity_type:,
    task_queue: options.task_queue,
    input:,
    schedule_to_close_ms: time.option_to_ms(options.schedule_to_close),
    schedule_to_start_ms: time.option_to_ms(options.schedule_to_start),
    start_to_close_ms: Some(time.to_ms(options.start_to_close)),
    heartbeat_timeout_ms: time.option_to_ms(options.heartbeat_timeout),
    retry: option.map(options.retry, retry.to_bridge),
    id_reuse_policy: case options.id_reuse_policy {
      AllowDuplicate -> 1
      AllowDuplicateFailedOnly -> 2
      RejectDuplicate -> 3
    },
    id_conflict_policy: case options.id_conflict_policy {
      FailIfRunning -> 1
      UseExisting -> 2
    },
    search_attributes: search_attributes.encode(options.search_attributes),
    start_delay_ms: time.option_to_ms(options.start_delay),
    headers: options.headers,
  )
}

/// Start a standalone activity and return a handle to it. Does not wait for it to finish.
/// Fails with `AlreadyExists` if the id is taken and the id policies do not allow it.
pub fn start(
  client: Client,
  definition: activity.Definition(input, output),
  input: input,
  options: Options,
) -> Result(Handle(output), Error) {
  let request =
    start_request(
      definition.name,
      codec.encode_arguments(definition.input, input),
      options,
    )
  let request =
    bridge.ActivityStartRequest(
      ..request,
      input: transcode.encode_list(client.payload_codec(client), request.input),
    )
  bridge.start_activity(client.connection(client), request)
  |> result.map(fn(run_id) {
    Handle(client, options.activity_id, Some(run_id), definition.output)
  })
  |> result.map_error(client.request_error)
}

/// Start a standalone activity and wait for its result.
pub fn execute(
  client: Client,
  definition: activity.Definition(input, output),
  input: input,
  options: Options,
) -> Result(output, Error) {
  start(client, definition, input, options) |> result.try(result)
}

/// A handle to the latest run of an existing standalone activity.
pub fn handle(
  client: Client,
  definition: activity.Definition(input, output),
  activity_id: String,
) -> Handle(output) {
  Handle(client, activity_id, None, definition.output)
}

/// The id of the activity the handle refers to.
pub fn activity_id(handle: Handle(output)) -> String {
  handle.activity_id
}

/// The run this handle was started with, or `None` for a handle to the latest run.
pub fn run_id(handle: Handle(output)) -> Option(String) {
  handle.run_id
}

/// The run id as requests carry it: empty for the latest run.
fn run_of(handle: Handle(output)) -> String {
  option.unwrap(handle.run_id, "")
}

/// Wait for the activity to close, through all its retries. An activity that did not
/// complete gives `ActivityFailed` with the failure of its last attempt,
/// `ActivityCancelled`, `ActivityTerminated` or `ActivityTimedOut`.
pub fn result(handle: Handle(output)) -> Result(output, Error) {
  let payload_codec = client.payload_codec(handle.client)
  case
    bridge.activity_result(
      client.connection(handle.client),
      handle.activity_id,
      run_of(handle),
    )
  {
    Ok(bridge.ActivitySucceeded(payloads)) ->
      transcode.decode_list(payload_codec, payloads)
      |> result.try(codec.decode_arguments(handle.output, _))
      |> result.map_error(fn(reason) {
        client.DecodeFailed("the activity's result: " <> reason)
      })
    Ok(bridge.ActivityEnded(failure)) ->
      Error(case failure.kind {
        failure.Cancelled -> client.ActivityCancelled
        failure.Terminated -> client.ActivityTerminated
        failure.Timeout -> client.ActivityTimedOut
        _ -> client.ActivityFailed(decoded_failure(payload_codec, failure))
      })
    Error(error) -> Error(client.request_error(error))
  }
}

/// A failure written without this client's codec is returned as stored.
fn decoded_failure(
  payload_codec: Option(codec.PayloadCodec),
  failure: Failure,
) -> Failure {
  transcode.decode_failure(payload_codec, failure) |> result.unwrap(failure)
}

fn status(value: Int) -> Status {
  case value {
    1 -> Running
    2 -> Completed
    3 -> Failed
    4 -> Cancelled
    5 -> Terminated
    6 -> TimedOut
    7 -> Paused
    _ -> UnknownStatus
  }
}

/// What the server knows about the activity: its status, attempt, last failure and
/// heartbeat details.
pub fn describe(handle: Handle(output)) -> Result(Description, Error) {
  let payload_codec = client.payload_codec(handle.client)
  bridge.describe_activity(
    client.connection(handle.client),
    handle.activity_id,
    run_of(handle),
  )
  |> result.map(fn(execution) {
    Description(
      activity_id: execution.activity_id,
      run_id: execution.run_id,
      activity_type: execution.activity_type,
      task_queue: execution.task_queue,
      status: status(execution.status),
      run_state: case execution.run_state {
        1 -> Some(Scheduled)
        2 -> Some(Started)
        3 -> Some(CancelRequested)
        _ -> None
      },
      attempt: execution.attempt,
      schedule_time: time.timestamp(execution.schedule_time_ms),
      close_time: option.map(execution.close_time_ms, time.timestamp),
      last_failure: option.map(execution.last_failure, decoded_failure(
        payload_codec,
        _,
      )),
      heartbeat_details: transcode.decode_list(
        payload_codec,
        execution.heartbeat_details,
      )
        |> result.unwrap(execution.heartbeat_details),
      last_worker_identity: execution.last_worker_identity,
      search_attributes: execution.search_attributes,
    )
  })
  |> result.map_error(client.request_error)
}

/// Ask the activity to cancel. One that no worker has picked up is cancelled at once.
/// One that is running learns of it the next time it heartbeats, through
/// `activity.is_cancelled`, and decides how to respond; one that never heartbeats never
/// learns. `reason` is recorded on the server.
pub fn cancel(handle: Handle(output), reason: String) -> Result(Nil, Error) {
  bridge.cancel_activity(
    client.connection(handle.client),
    handle.activity_id,
    run_of(handle),
    reason,
  )
  |> result.map_error(client.request_error)
}

/// Close the activity immediately, without giving its code a chance to react. An attempt
/// that is running is not stopped; what it returns is ignored.
pub fn terminate(handle: Handle(output), reason: String) -> Result(Nil, Error) {
  bridge.terminate_activity(
    client.connection(handle.client),
    handle.activity_id,
    run_of(handle),
    reason,
  )
  |> result.map_error(client.request_error)
}

/// Standalone activities matching a visibility query such as `ActivityType = "charge"`,
/// at most `limit` of them. The visibility store lags behind: an activity started a
/// moment ago may be missing.
pub fn list(
  client: Client,
  query: String,
  limit limit: Int,
) -> Result(List(Summary), Error) {
  bridge.list_activities(client.connection(client), query, limit)
  |> result.map(
    list.map(_, fn(summary) {
      Summary(
        activity_id: summary.activity_id,
        run_id: summary.run_id,
        activity_type: summary.activity_type,
        task_queue: summary.task_queue,
        status: status(summary.status),
        schedule_time: time.timestamp(summary.schedule_time_ms),
        close_time: option.map(summary.close_time_ms, time.timestamp),
        search_attributes: summary.search_attributes,
      )
    }),
  )
  |> result.map_error(client.request_error)
}

/// How many standalone activities match a visibility query.
pub fn count(client: Client, query: String) -> Result(Int, Error) {
  bridge.count_activities(client.connection(client), query)
  |> result.map_error(client.request_error)
}
