//// Connect to a Temporal service and start, signal, query and await workflows.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import temporal/activity
import temporal/codec.{type Codec, type Payload, type PayloadCodec}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/time
import temporal/internal/transcode
import temporal/search_attributes
import temporal/workflow

/// A connection to one namespace of a Temporal service. Safe to share between processes.
pub opaque type Client {
  Client(
    connection: bridge.Connection,
    codec: Option(PayloadCodec),
    namespace: String,
  )
}

/// How to reach the service. Start from `local()` or `cloud(..)` and change what differs.
pub type Options {
  Options(
    /// The frontend address, such as `http://localhost:7233`.
    target: String,
    namespace: String,
    /// Recorded in history as the author of this client's requests.
    identity: String,
    /// Sent with every request. Needs `tls`.
    api_key: Option(String),
    /// `None` connects without TLS.
    tls: Option(Tls),
    /// Applied to every payload this client, and any worker started with it, sends or
    /// receives. See `codec.PayloadCodec` and `temporal/encryption`.
    codec: Option(PayloadCodec),
  )
}

/// TLS settings. All `None` means TLS with the system's root certificates.
pub type Tls {
  Tls(
    /// PEM root certificate to trust instead of the system's.
    server_root_ca: Option(BitArray),
    /// Name to verify the server's certificate against, if not the target's host.
    domain: Option(String),
    /// PEM certificate and private key, for mutual TLS.
    client_certificate: Option(#(BitArray, BitArray)),
  )
}

/// A workflow execution and the codec for its result.
pub opaque type Handle(output) {
  Handle(
    client: Client,
    workflow_id: String,
    /// `None` refers to the workflow's latest run.
    run_id: Option(String),
    output: Codec(output),
  )
}

/// The run id as requests carry it: empty for the latest run.
fn run_of(handle: Handle(output)) -> String {
  option.unwrap(handle.run_id, "")
}

/// What to do when a workflow with the same id is already running.
pub type IdConflictPolicy {
  /// Refuse to start. The default.
  FailIfRunning
  /// Return a handle to the running workflow instead of starting one.
  UseExisting
  /// Terminate the running workflow and start a new one.
  TerminateExisting
}

/// How a workflow is started. Build with `start_options` and change what differs.
pub type StartOptions {
  StartOptions(
    /// The workflow's identity. No two open workflows in a namespace share an id.
    workflow_id: String,
    task_queue: String,
    /// Whether an id may be used again once its workflow has closed.
    id_reuse_policy: workflow.IdReusePolicy,
    /// What to do if a workflow with the id is still running.
    id_conflict_policy: IdConflictPolicy,
    /// Longest the workflow may take across retries and continue-as-new.
    execution_timeout: Option(Duration),
    /// Longest a single run may take.
    run_timeout: Option(Duration),
    /// Longest one workflow task may take. `None` is ten seconds.
    task_timeout: Option(Duration),
    /// `None` does not retry a failed workflow.
    retry: Option(workflow.RetryPolicy),
    /// Unindexed data readable by the workflow and by `describe`.
    memo: Dict(String, Payload),
    /// Indexed values, for finding the workflow with `list`.
    search_attributes: Dict(String, search_attributes.Value),
    /// Wait this long before the first workflow task.
    start_delay: Option(Duration),
    /// Run again on this cron timetable. Prefer `temporal/schedule`.
    cron_schedule: Option(String),
    /// Context, such as a trace id, that travels beside the arguments to the workflow
    /// and on to the activities and child workflows it starts.
    headers: Dict(String, Payload),
  )
}

/// Why a client call failed.
pub type Error {
  /// The service could not be reached, or refused the connection.
  ConnectionFailed(message: String)
  /// No such workflow, run or schedule.
  NotFound(message: String)
  /// A workflow with this id is running or may not be started again, or a schedule with
  /// this id exists.
  AlreadyExists(message: String)
  /// The workflow returned a failure.
  WorkflowFailed(failure: Failure)
  /// The workflow accepted a request to cancel.
  WorkflowCancelled
  /// The workflow was terminated from outside.
  WorkflowTerminated
  /// A run or execution timeout passed.
  WorkflowTimedOut
  /// The workflow rejected the update, or accepted it and then failed it.
  UpdateFailed(failure: Failure)
  /// The workflow rejected a query, or the query handler failed.
  QueryFailed(message: String)
  /// The server's answer could not be decoded with the codec given.
  DecodeFailed(message: String)
  /// Any other failure, with its gRPC status code.
  RequestFailed(code: Int, message: String)
}

/// Where a run is in its life.
pub type Status {
  Running
  Completed
  Failed
  Cancelled
  Terminated
  ContinuedAsNew
  TimedOut
  /// A status this SDK does not know.
  UnknownStatus
}

/// A run as the server describes it.
pub type Description {
  Description(
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    task_queue: String,
    status: Status,
    start_time: Timestamp,
    /// `None` while the run is open.
    close_time: Option(Timestamp),
    history_length: Int,
    memo: Dict(String, Payload),
    /// The raw payloads; their `data` is JSON.
    search_attributes: Dict(String, Payload),
    /// `None` unless the run was started as a child.
    parent_workflow_id: Option(String),
  )
}

/// A workflow's complete event history, as fetched from the server.
pub type History {
  History(workflow_id: String, encoded: BitArray)
}

/// Options for a local development server.
pub fn local() -> Options {
  Options(
    target: "http://localhost:7233",
    namespace: "default",
    identity: "temporal-gleam",
    api_key: None,
    tls: None,
    codec: None,
  )
}

/// Options for Temporal Cloud with an API key: TLS with the system's root certificates.
/// `address` is the namespace's gRPC endpoint without a scheme, such as
/// `us-east-1.aws.api.temporal.io:7233`.
pub fn cloud(
  address address: String,
  namespace namespace: String,
  api_key api_key: String,
) -> Options {
  Options(
    target: "https://" <> address,
    namespace:,
    identity: "temporal-gleam",
    api_key: Some(api_key),
    tls: Some(Tls(server_root_ca: None, domain: None, client_certificate: None)),
    codec: None,
  )
}

/// Connect to the service. Fails if it cannot be reached or refuses the credentials.
pub fn connect(options: Options) -> Result(Client, Error) {
  bridge.connect(
    options.target,
    options.namespace,
    options.identity,
    options.api_key,
    option.map(options.tls, fn(tls) {
      bridge.Tls(tls.server_root_ca, tls.domain, tls.client_certificate)
    }),
  )
  |> result.map(Client(_, options.codec, options.namespace))
  |> result.map_error(ConnectionFailed)
}

/// The codec workers started with this client apply.
@internal
pub fn payload_codec(client: Client) -> Option(PayloadCodec) {
  client.codec
}

/// The namespace the client is connected to.
pub fn namespace(client: Client) -> String {
  client.namespace
}

/// The connection workers started with this client poll through.
@internal
pub fn connection(client: Client) -> bridge.Connection {
  client.connection
}

/// Options with the defaults: no timeouts, no retries, and a refusal to start while a
/// workflow with the same id is running.
pub fn start_options(
  workflow_id workflow_id: String,
  task_queue task_queue: String,
) -> StartOptions {
  StartOptions(
    workflow_id:,
    task_queue:,
    id_reuse_policy: workflow.AllowDuplicate,
    id_conflict_policy: FailIfRunning,
    execution_timeout: None,
    run_timeout: None,
    task_timeout: None,
    retry: None,
    memo: dict.new(),
    search_attributes: dict.new(),
    start_delay: None,
    cron_schedule: None,
    headers: dict.new(),
  )
}

/// The start request for a workflow type and its options, before the codec is applied.
@internal
pub fn start_request(
  workflow_type: String,
  input: List(Payload),
  options: StartOptions,
) -> bridge.StartRequest {
  bridge.StartRequest(
    workflow_id: options.workflow_id,
    workflow_type:,
    task_queue: options.task_queue,
    input:,
    id_reuse_policy: case options.id_reuse_policy {
      workflow.AllowDuplicate -> 1
      workflow.AllowDuplicateFailedOnly -> 2
      workflow.RejectDuplicate -> 3
    },
    id_conflict_policy: case options.id_conflict_policy {
      FailIfRunning -> 1
      UseExisting -> 2
      TerminateExisting -> 3
    },
    execution_timeout_ms: time.option_to_ms(options.execution_timeout),
    run_timeout_ms: time.option_to_ms(options.run_timeout),
    task_timeout_ms: time.option_to_ms(options.task_timeout),
    retry: option.map(options.retry, workflow.to_bridge_retry),
    memo: options.memo,
    search_attributes: search_attributes.encode(options.search_attributes),
    start_delay_ms: time.option_to_ms(options.start_delay),
    cron_schedule: option.unwrap(options.cron_schedule, ""),
    request_id: "",
    callbacks: [],
    headers: options.headers,
  )
}

// gRPC status codes.
const not_found = 5

const already_exists = 6

/// Apply the client's payload codec to a start request.
@internal
pub fn encode_request(
  client: Client,
  request: bridge.StartRequest,
) -> bridge.StartRequest {
  bridge.StartRequest(
    ..request,
    input: transcode.encode_list(client.codec, request.input),
    memo: transcode.encode_map(client.codec, request.memo),
  )
}

/// Start a workflow so that the Nexus operation named by `callbacks` completes when it
/// does. `request_id` makes a retried start land on the same workflow.
@internal
pub fn start_for_nexus(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
  request_id: String,
  callbacks: List(#(String, Dict(String, String))),
) -> Result(Handle(output), Error) {
  let request =
    start_request(
      definition.name,
      codec.encode_arguments(definition.input, input),
      options,
    )
  let request =
    encode_request(
      client,
      bridge.StartRequest(..request, request_id:, callbacks:),
    )
  case bridge.start_workflow(client.connection, request, None) {
    Ok(run_id) ->
      Ok(Handle(client, options.workflow_id, Some(run_id), definition.output))
    Error(error) -> Error(request_error(error))
  }
}

fn start_with(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
  signal: Option(#(String, List(Payload))),
) -> Result(Handle(output), Error) {
  let request =
    start_request(
      definition.name,
      codec.encode_arguments(definition.input, input),
      options,
    )
    |> encode_request(client, _)
  let signal =
    option.map(signal, fn(signal) {
      #(signal.0, transcode.encode_list(client.codec, signal.1))
    })
  // Signal-with-start has no "fail if running": reaching a running workflow is its point.
  let request = case signal, options.id_conflict_policy {
    Some(_), FailIfRunning ->
      bridge.StartRequest(..request, id_conflict_policy: 0)
    _, _ -> request
  }
  case bridge.start_workflow(client.connection, request, signal) {
    Ok(run_id) ->
      Ok(Handle(client, options.workflow_id, Some(run_id), definition.output))
    Error(error) -> Error(request_error(error))
  }
}

/// Start a workflow and return a handle to the run. Does not wait for it to finish.
pub fn start(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
) -> Result(Handle(output), Error) {
  start_with(client, definition, input, options, None)
}

/// Send a signal to the workflow with this id, starting it first if it is not running.
/// The signal is delivered before the workflow's first task either way.
pub fn signal_with_start(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
  signal: workflow.SignalDefinition(a),
  value: a,
) -> Result(Handle(output), Error) {
  start_with(
    client,
    definition,
    input,
    options,
    Some(#(signal.name, codec.encode_arguments(signal.codec, value))),
  )
}

/// A handle to the latest run of an existing workflow.
pub fn handle(
  client: Client,
  definition: workflow.Definition(input, output),
  workflow_id: String,
) -> Handle(output) {
  Handle(client, workflow_id, None, definition.output)
}

/// The id of the workflow the handle refers to.
pub fn workflow_id(handle: Handle(output)) -> String {
  handle.workflow_id
}

/// The run this handle was started with, or `None` for a handle to the latest run.
pub fn run_id(handle: Handle(output)) -> Option(String) {
  handle.run_id
}

/// Wait for the workflow to finish. Follows continue-as-new and retries to the final run.
pub fn result(handle: Handle(output)) -> Result(output, Error) {
  case
    bridge.workflow_result(
      handle.client.connection,
      handle.workflow_id,
      run_of(handle),
    )
  {
    Ok(bridge.OutcomeCompleted(payloads)) ->
      transcode.decode_list(handle.client.codec, payloads)
      |> result.try(codec.decode_arguments(handle.output, _))
      |> result.map_error(DecodeFailed)
    Ok(bridge.OutcomeFailed(failure)) ->
      Error(WorkflowFailed(
        transcode.decode_failure(handle.client.codec, failure)
        |> result.unwrap(failure),
      ))
    Ok(bridge.OutcomeCancelled) -> Error(WorkflowCancelled)
    Ok(bridge.OutcomeTerminated) -> Error(WorkflowTerminated)
    Ok(bridge.OutcomeTimedOut) -> Error(WorkflowTimedOut)
    Error(error) -> Error(request_error(error))
  }
}

/// Start a workflow and wait for its result.
pub fn execute(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
) -> Result(output, Error) {
  start(client, definition, input, options) |> result.try(result)
}

/// A failed request as the error callers see.
@internal
pub fn request_error(error: bridge.RpcError) -> Error {
  case error {
    bridge.RpcError(code, message) if code == not_found -> NotFound(message)
    bridge.RpcError(code, message) if code == already_exists ->
      AlreadyExists(message)
    bridge.RpcError(code, message) -> RequestFailed(code, message)
  }
}

/// Send a signal. Returns once the server has recorded it, not once the workflow has
/// acted on it.
pub fn signal(
  handle: Handle(output),
  definition: workflow.SignalDefinition(a),
  value: a,
) -> Result(Nil, Error) {
  bridge.signal_workflow(
    handle.client.connection,
    handle.workflow_id,
    run_of(handle),
    definition.name,
    transcode.encode_list(
      handle.client.codec,
      codec.encode_arguments(definition.codec, value),
    ),
  )
  |> result.map_error(request_error)
}

/// Ask the workflow for a value without changing it. Works on finished workflows too,
/// as long as a worker for them is running.
pub fn query(
  handle: Handle(output),
  definition: workflow.QueryDefinition(argument, result),
  argument: argument,
) -> Result(result, Error) {
  case
    bridge.query_workflow(
      handle.client.connection,
      handle.workflow_id,
      run_of(handle),
      definition.name,
      transcode.encode_list(
        handle.client.codec,
        codec.encode_arguments(definition.argument, argument),
      ),
    )
  {
    Ok(payloads) ->
      transcode.decode_list(handle.client.codec, payloads)
      |> result.try(codec.decode_arguments(definition.result, _))
      |> result.map_error(DecodeFailed)
    Error(bridge.RpcError(code, message)) if code == not_found ->
      Error(NotFound(message))
    // The server reports a query the workflow could not answer as an invalid argument.
    Error(bridge.RpcError(3, message)) -> Error(QueryFailed(message))
    Error(bridge.RpcError(code, message)) -> Error(RequestFailed(code, message))
  }
}

/// Ask the workflow to cancel. The workflow decides how to respond.
pub fn cancel(handle: Handle(output)) -> Result(Nil, Error) {
  bridge.cancel_workflow(
    handle.client.connection,
    handle.workflow_id,
    run_of(handle),
  )
  |> result.map_error(request_error)
}

/// Stop the workflow immediately, without giving its code a chance to react.
pub fn terminate(handle: Handle(output), reason: String) -> Result(Nil, Error) {
  bridge.terminate_workflow(
    handle.client.connection,
    handle.workflow_id,
    run_of(handle),
    reason,
  )
  |> result.map_error(request_error)
}

/// Fetch the run's history, for `replayer.replay`.
pub fn history(handle: Handle(output)) -> Result(History, Error) {
  bridge.fetch_history(
    handle.client.connection,
    handle.workflow_id,
    run_of(handle),
  )
  |> result.map(History(handle.workflow_id, _))
  |> result.map_error(request_error)
}

fn description(client: Client, execution: bridge.Execution) -> Description {
  Description(
    workflow_id: execution.workflow_id,
    run_id: execution.run_id,
    workflow_type: execution.workflow_type,
    task_queue: execution.task_queue,
    status: case execution.status {
      1 -> Running
      2 -> Completed
      3 -> Failed
      4 -> Cancelled
      5 -> Terminated
      6 -> ContinuedAsNew
      7 -> TimedOut
      _ -> UnknownStatus
    },
    start_time: time.timestamp(execution.start_time_ms),
    close_time: option.map(execution.close_time_ms, time.timestamp),
    history_length: execution.history_length,
    // A memo written without this client's codec is returned as stored.
    memo: transcode.decode_map(client.codec, execution.memo)
      |> result.unwrap(execution.memo),
    search_attributes: execution.search_attributes,
    parent_workflow_id: execution.parent_workflow_id,
  )
}

/// What the server knows about the run: its status, times, memo and search attributes.
pub fn describe(handle: Handle(output)) -> Result(Description, Error) {
  bridge.describe_workflow(
    handle.client.connection,
    handle.workflow_id,
    run_of(handle),
  )
  |> result.map(description(handle.client, _))
  |> result.map_error(request_error)
}

/// Runs matching a visibility query such as `WorkflowType = "greet"`, at most `limit` of
/// them. The visibility store lags behind: a workflow started a moment ago may be missing.
pub fn list(
  client: Client,
  query: String,
  limit limit: Int,
) -> Result(List(Description), Error) {
  bridge.list_workflows(client.connection, query, limit)
  |> result.map(list.map(_, description(client, _)))
  |> result.map_error(request_error)
}

/// How many runs match a visibility query.
pub fn count(client: Client, query: String) -> Result(Int, Error) {
  bridge.count_workflows(client.connection, query)
  |> result.map_error(request_error)
}

// ---- updates ----

/// An update the workflow has accepted.
pub opaque type UpdateHandle(result) {
  UpdateHandle(
    client: Client,
    workflow_id: String,
    run_id: String,
    update_id: String,
    result: Codec(result),
    /// Set when the update had already finished by the time it was accepted.
    outcome: Option(bridge.UpdateOutcome),
  )
}

// The API's `UpdateWorkflowExecutionLifecycleStage` values.
const accepted = 2

const completed = 3

fn send_update(
  handle: Handle(output),
  definition: workflow.UpdateDefinition(argument, result),
  argument: argument,
  update_id: String,
  stage: Int,
) -> Result(UpdateHandle(result), Error) {
  let client = handle.client
  case
    bridge.update_workflow(
      client.connection,
      #(handle.workflow_id, run_of(handle), update_id, definition.name),
      transcode.encode_list(
        client.codec,
        codec.encode_arguments(definition.argument, argument),
      ),
      stage,
    )
  {
    Error(error) -> Error(request_error(error))
    // Rejected by the workflow's validator: there is no update to hold a handle to.
    Ok(#(_, _, bridge.UpdateFailed(failure))) if stage == accepted ->
      Error(UpdateFailed(decoded_failure(client, failure)))
    Ok(#(run_id, update_id, outcome)) ->
      Ok(
        UpdateHandle(
          client:,
          workflow_id: handle.workflow_id,
          run_id:,
          update_id:,
          result: definition.result,
          outcome: case outcome {
            bridge.UpdatePending -> None
            finished -> Some(finished)
          },
        ),
      )
  }
}

fn decoded_failure(client: Client, failure: Failure) -> Failure {
  transcode.decode_failure(client.codec, failure) |> result.unwrap(failure)
}

/// Send an update and wait for the workflow's answer.
pub fn execute_update(
  handle: Handle(output),
  definition: workflow.UpdateDefinition(argument, result),
  argument: argument,
) -> Result(result, Error) {
  send_update(handle, definition, argument, "", completed)
  |> result.try(update_result)
}

/// Send an update and wait only until the workflow has accepted it. Fails with
/// `UpdateFailed` if the workflow rejects it.
pub fn start_update(
  handle: Handle(output),
  definition: workflow.UpdateDefinition(argument, result),
  argument: argument,
) -> Result(UpdateHandle(result), Error) {
  send_update(handle, definition, argument, "", accepted)
}

/// Like `start_update`, with an id of the caller's choosing. Sending the same id again
/// reaches the same update instead of starting another.
pub fn start_update_with_id(
  handle: Handle(output),
  definition: workflow.UpdateDefinition(argument, result),
  argument: argument,
  update_id update_id: String,
) -> Result(UpdateHandle(result), Error) {
  send_update(handle, definition, argument, update_id, accepted)
}

/// The id of the update: the one given to `start_update_with_id`, or a generated one.
pub fn update_id(update: UpdateHandle(result)) -> String {
  update.update_id
}

/// Wait for an accepted update's answer.
pub fn update_result(update: UpdateHandle(result)) -> Result(result, Error) {
  let outcome = case update.outcome {
    Some(outcome) -> Ok(outcome)
    None ->
      bridge.poll_update(
        update.client.connection,
        update.workflow_id,
        update.run_id,
        update.update_id,
      )
  }
  case outcome {
    Error(error) -> Error(request_error(error))
    Ok(bridge.UpdateFailed(failure)) ->
      Error(UpdateFailed(decoded_failure(update.client, failure)))
    Ok(bridge.UpdateSucceeded(payloads)) ->
      transcode.decode_list(update.client.codec, payloads)
      |> result.try(codec.decode_arguments(update.result, _))
      |> result.map_error(DecodeFailed)
    Ok(bridge.UpdatePending) ->
      Error(RequestFailed(
        4,
        "the update did not finish before the request gave up",
      ))
  }
}

/// Send an update to the workflow with this id, starting it first if it is not running, as
/// one request, and wait for the workflow's answer. Use it when the first thing a caller
/// does to a workflow is ask it something, such as adding the first item to a cart.
///
/// The start options must say what to do if the workflow is already running: set
/// `id_conflict_policy` to `UseExisting`, the usual choice, or leave it at `FailIfRunning`
/// to insist on a new workflow.
pub fn execute_update_with_start(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
  update: workflow.UpdateDefinition(argument, result),
  argument: argument,
) -> Result(#(Handle(output), result), Error) {
  let request =
    start_request(
      definition.name,
      codec.encode_arguments(definition.input, input),
      options,
    )
    |> encode_request(client, _)
  case
    bridge.update_with_start(
      client.connection,
      request,
      #("", update.name),
      transcode.encode_list(
        client.codec,
        codec.encode_arguments(update.argument, argument),
      ),
    )
  {
    Error(error) -> Error(request_error(error))
    Ok(#(run_id, update_id, outcome)) -> {
      let handle =
        Handle(client, options.workflow_id, Some(run_id), definition.output)
      UpdateHandle(
        client:,
        workflow_id: options.workflow_id,
        run_id:,
        update_id:,
        result: update.result,
        outcome: Some(outcome),
      )
      |> update_result
      |> result.map(fn(answer) { #(handle, answer) })
    }
  }
}

// ---- finishing activities from outside ----

/// Finish an activity that deferred its completion with `activity.complete_later`, giving
/// its result. `task_token` is what the activity read with `activity.task_token`.
pub fn complete_activity(
  client: Client,
  task_token: BitArray,
  definition: activity.Definition(input, output),
  value: output,
) -> Result(Nil, Error) {
  bridge.complete_activity_async(
    client.connection,
    task_token,
    bridge.ActivityCompleted(
      Some(transcode.encode_one(
        client.codec,
        codec.encode(definition.output, value),
      )),
    ),
  )
  |> result.map_error(request_error)
}

/// Fail an activity that deferred its completion. Its retry policy applies as usual.
pub fn fail_activity(
  client: Client,
  task_token: BitArray,
  failure: Failure,
) -> Result(Nil, Error) {
  bridge.complete_activity_async(
    client.connection,
    task_token,
    bridge.ActivityFailed(transcode.encode_failure(client.codec, failure)),
  )
  |> result.map_error(request_error)
}
