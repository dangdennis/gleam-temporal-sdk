//// Connect to a Temporal service and start, signal, query and await workflows.

import gleam/option.{type Option, None}
import gleam/result
import temporal/codec.{type Codec}
import temporal/failure.{type Failure}
import temporal/internal/bridge
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

pub type StartOptions {
  StartOptions(workflow_id: String, task_queue: String)
}

pub type StartError {
  /// A workflow with this id is already running.
  AlreadyStarted
  StartFailed(message: String)
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
  )
}

pub fn connect(options: Options) -> Result(Client, String) {
  bridge.connect(
    options.target,
    options.namespace,
    options.identity,
    options.api_key,
  )
  |> result.map(Client)
}

@internal
pub fn connection(client: Client) -> bridge.Connection {
  client.connection
}

pub fn start(
  client: Client,
  definition: workflow.Definition(input, output),
  input: input,
  options: StartOptions,
) -> Result(Handle(output), StartError) {
  case
    bridge.start_workflow(
      client.connection,
      options.workflow_id,
      definition.name,
      options.task_queue,
      [definition.input.encode(input)],
    )
  {
    Ok(run_id) ->
      Ok(Handle(client, options.workflow_id, run_id, definition.output))
    Error(bridge.AlreadyStarted) -> Error(AlreadyStarted)
    Error(bridge.StartFailed(message)) -> Error(StartFailed(message))
  }
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
    Error(message) -> Error(ClientError(message))
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

pub fn signal(
  handle: Handle(output),
  definition: workflow.SignalDefinition(a),
  value: a,
) -> Result(Nil, String) {
  bridge.signal_workflow(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
    definition.name,
    [definition.codec.encode(value)],
  )
}

pub fn query(
  handle: Handle(output),
  definition: workflow.QueryDefinition(argument, result),
  argument: argument,
) -> Result(result, String) {
  bridge.query_workflow(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
    definition.name,
    [definition.argument.encode(argument)],
  )
  |> result.try(codec.decode_argument(definition.result, _))
}

/// Ask the workflow to cancel. The workflow decides how to respond.
pub fn cancel(handle: Handle(output)) -> Result(Nil, String) {
  bridge.cancel_workflow(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
  )
}

/// Stop the workflow immediately, without giving its code a chance to react.
pub fn terminate(
  handle: Handle(output),
  reason: String,
) -> Result(Nil, String) {
  bridge.terminate_workflow(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
    reason,
  )
}

/// Fetch the run's history, for `replayer.replay`.
pub fn history(handle: Handle(output)) -> Result(History, String) {
  bridge.fetch_history(
    handle.client.connection,
    handle.workflow_id,
    handle.run_id,
  )
  |> result.map(History(handle.workflow_id, _))
}
