//// Connect to a Temporal service and start, signal, query and await workflows.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import temporal/codec.{type Codec, type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/search_attributes
import temporal/workflow

pub opaque type Client {
  Client(connection: bridge.Connection)
}

pub type Options {
  Options(
    /// The frontend address, such as `http://localhost:7233`.
    target: String,
    namespace: String,
    /// Recorded in history as the author of this client's requests.
    identity: String,
    api_key: Option(String),
    /// `None` connects without TLS.
    tls: Option(Tls),
  )
}

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
    run_id: String,
    output: Codec(output),
  )
}

/// What to do when a workflow with the same id is already running.
pub type IdConflictPolicy {
  /// Refuse to start. The default.
  FailIfRunning
  /// Return a handle to the running workflow instead of starting one.
  UseExisting
  TerminateExisting
}

pub type StartOptions {
  StartOptions(
    workflow_id: String,
    task_queue: String,
    id_reuse_policy: workflow.IdReusePolicy,
    id_conflict_policy: IdConflictPolicy,
    /// Longest the workflow may take across retries and continue-as-new.
    execution_timeout_ms: Option(Int),
    /// Longest a single run may take.
    run_timeout_ms: Option(Int),
    task_timeout_ms: Option(Int),
    /// `None` does not retry a failed workflow.
    retry: Option(workflow.RetryPolicy),
    /// Unindexed data readable by the workflow and by `describe`.
    memo: Dict(String, Payload),
    search_attributes: Dict(String, search_attributes.Value),
    /// Wait this long before the first workflow task.
    start_delay_ms: Option(Int),
    cron_schedule: Option(String),
  )
}

pub type StartError {
  /// A workflow with this id is already running, or its id may not be reused.
  AlreadyStarted
  StartFailed(message: String)
}

/// Why a request about an existing workflow failed.
pub type Error {
  /// No such workflow, run or schedule.
  NotFound(message: String)
  /// A schedule with this id already exists.
  AlreadyExists(message: String)
  /// The workflow rejected a query, or the query handler failed.
  QueryFailed(message: String)
  /// The server's answer could not be decoded with the codec given.
  DecodeFailed(message: String)
  /// Any other failure, with its gRPC status code.
  RequestFailed(code: Int, message: String)
}

/// Why a workflow did not produce a result.
pub type WorkflowError {
  Failed(failure: Failure)
  Cancelled
  Terminated
  TimedOut
  /// The result could not be fetched or decoded.
  ClientError(message: String)
}

pub type Status {
  Running
  Completed
  WorkflowFailed
  WorkflowCancelled
  WorkflowTerminated
  ContinuedAsNew
  WorkflowTimedOut
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
    start_time_ms: Int,
    close_time_ms: Option(Int),
    history_length: Int,
    memo: Dict(String, Payload),
    /// The raw payloads; their `data` is JSON.
    search_attributes: Dict(String, Payload),
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
  )
}

pub fn connect(options: Options) -> Result(Client, String) {
  bridge.connect(
    options.target,
    options.namespace,
    options.identity,
    options.api_key,
    option.map(options.tls, fn(tls) {
      bridge.Tls(tls.server_root_ca, tls.domain, tls.client_certificate)
    }),
  )
  |> result.map(Client)
}

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
    execution_timeout_ms: None,
    run_timeout_ms: None,
    task_timeout_ms: None,
    retry: None,
    memo: dict.new(),
    search_attributes: dict.new(),
    start_delay_ms: None,
    cron_schedule: None,
  )
}

/// Add a memo entry.
pub fn with_memo(
  options: StartOptions,
  key: String,
  codec: Codec(a),
  value: a,
) -> StartOptions {
  StartOptions(
    ..options,
    memo: dict.insert(options.memo, key, codec.encode(value)),
  )
}

pub fn with_search_attribute(
  options: StartOptions,
  name: String,
  value: search_attributes.Value,
) -> StartOptions {
  StartOptions(
    ..options,
    search_attributes: dict.insert(options.search_attributes, name, value),
  )
}

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
    execution_timeout_ms: options.execution_timeout_ms,
    run_timeout_ms: options.run_timeout_ms,
    task_timeout_ms: options.task_timeout_ms,
    retry: option.map(options.retry, fn(retry) {
      bridge.RetryPolicy(
        initial_interval_ms: retry.initial_interval_ms,
        backoff_coefficient: retry.backoff_coefficient,
        maximum_interval_ms: retry.maximum_interval_ms,
        maximum_attempts: retry.maximum_attempts,
        non_retryable_error_types: retry.non_retryable_error_types,
      )
    }),
    memo: options.memo,
    search_attributes: search_attributes.encode(options.search_attributes),
    start_delay_ms: options.start_delay_ms,
    cron_schedule: option.unwrap(options.cron_schedule, ""),
  )
}

// gRPC status codes.
const not_found = 5

const already_exists = 6

fn start_with(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
  signal: Option(#(String, List(Payload))),
) -> Result(Handle(output), StartError) {
  let request =
    start_request(definition.name, [definition.input.encode(input)], options)
  // Signal-with-start has no "fail if running": reaching a running workflow is its point.
  let request = case signal, options.id_conflict_policy {
    Some(_), FailIfRunning ->
      bridge.StartRequest(..request, id_conflict_policy: 0)
    _, _ -> request
  }
  case bridge.start_workflow(client.connection, request, signal) {
    Ok(run_id) ->
      Ok(Handle(client, options.workflow_id, run_id, definition.output))
    Error(bridge.RpcError(code, _)) if code == already_exists ->
      Error(AlreadyStarted)
    Error(bridge.RpcError(_, message)) -> Error(StartFailed(message))
  }
}

pub fn start(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
) -> Result(Handle(output), StartError) {
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
) -> Result(Handle(output), StartError) {
  start_with(
    client,
    definition,
    input,
    options,
    Some(#(signal.name, [signal.codec.encode(value)])),
  )
}

/// A handle to the latest run of an existing workflow.
pub fn handle(
  client: Client,
  definition: workflow.Definition(input, output),
  workflow_id: String,
) -> Handle(output) {
  Handle(client, workflow_id, "", definition.output)
}

pub fn workflow_id(handle: Handle(output)) -> String {
  handle.workflow_id
}

/// The run this handle was started with, or `""` for a handle to the latest run.
pub fn run_id(handle: Handle(output)) -> String {
  handle.run_id
}

/// Wait for the workflow to finish. Follows continue-as-new and retries to the final run.
pub fn result(handle: Handle(output)) -> Result(output, WorkflowError) {
  case
    bridge.workflow_result(
      handle.client.connection,
      handle.workflow_id,
      handle.run_id,
    )
  {
    Ok(bridge.OutcomeCompleted(payloads)) ->
      codec.decode_argument(handle.output, payloads)
      |> result.map_error(ClientError)
    Ok(bridge.OutcomeFailed(failure)) -> Error(Failed(failure))
    Ok(bridge.OutcomeCancelled) -> Error(Cancelled)
    Ok(bridge.OutcomeTerminated) -> Error(Terminated)
    Ok(bridge.OutcomeTimedOut) -> Error(TimedOut)
    Error(bridge.RpcError(_, message)) -> Error(ClientError(message))
  }
}

/// Start a workflow and wait for its result.
pub fn execute(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
) -> Result(output, WorkflowError) {
  case start(client, definition, input, options) {
    Ok(handle) -> result(handle)
    Error(AlreadyStarted) ->
      Error(ClientError("a workflow with this id is already running"))
    Error(StartFailed(message)) -> Error(ClientError(message))
  }
}

@internal
pub fn request_error(error: bridge.RpcError) -> Error {
  case error {
    bridge.RpcError(code, message) if code == not_found -> NotFound(message)
    bridge.RpcError(code, message) if code == already_exists ->
      AlreadyExists(message)
    bridge.RpcError(code, message) -> RequestFailed(code, message)
  }
}

pub fn signal(
  handle: Handle(output),
  definition: workflow.SignalDefinition(a),
  value: a,
) -> Result(Nil, Error) {
  bridge.signal_workflow(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
    definition.name,
    [definition.codec.encode(value)],
  )
  |> result.map_error(request_error)
}

pub fn query(
  handle: Handle(output),
  definition: workflow.QueryDefinition(argument, result),
  argument: argument,
) -> Result(result, Error) {
  case
    bridge.query_workflow(
      handle.client.connection,
      handle.workflow_id,
      handle.run_id,
      definition.name,
      [definition.argument.encode(argument)],
    )
  {
    Ok(payloads) ->
      codec.decode_argument(definition.result, payloads)
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
    handle.run_id,
  )
  |> result.map_error(request_error)
}

/// Stop the workflow immediately, without giving its code a chance to react.
pub fn terminate(handle: Handle(output), reason: String) -> Result(Nil, Error) {
  bridge.terminate_workflow(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
    reason,
  )
  |> result.map_error(request_error)
}

/// Fetch the run's history, for `replayer.replay`.
pub fn history(handle: Handle(output)) -> Result(History, Error) {
  bridge.fetch_history(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
  )
  |> result.map(History(handle.workflow_id, _))
  |> result.map_error(request_error)
}

fn description(execution: bridge.Execution) -> Description {
  Description(
    workflow_id: execution.workflow_id,
    run_id: execution.run_id,
    workflow_type: execution.workflow_type,
    task_queue: execution.task_queue,
    status: case execution.status {
      1 -> Running
      2 -> Completed
      3 -> WorkflowFailed
      4 -> WorkflowCancelled
      5 -> WorkflowTerminated
      6 -> ContinuedAsNew
      7 -> WorkflowTimedOut
      _ -> UnknownStatus
    },
    start_time_ms: execution.start_time_ms,
    close_time_ms: execution.close_time_ms,
    history_length: execution.history_length,
    memo: execution.memo,
    search_attributes: execution.search_attributes,
    parent_workflow_id: execution.parent_workflow_id,
  )
}

pub fn describe(handle: Handle(output)) -> Result(Description, Error) {
  bridge.describe_workflow(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
  )
  |> result.map(description)
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
  |> result.map(list.map(_, description))
  |> result.map_error(request_error)
}

/// How many runs match a visibility query.
pub fn count(client: Client, query: String) -> Result(Int, Error) {
  bridge.count_workflows(client.connection, query)
  |> result.map_error(request_error)
}
