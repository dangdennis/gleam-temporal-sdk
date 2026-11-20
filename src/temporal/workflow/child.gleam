//// Child workflows: workflows started by a workflow, which the server ties to their parent.
////
//// ```gleam
//// let options =
////   child.options(workflow_id: "order-" <> id <> "-shipping")
////   |> child.with_task_queue("shipping")
//// use tracking <- result.try(child.execute(ctx, ship(), order, options))
//// ```

import gleam/dict.{type Dict}
import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/runtime/determinism
import temporal/internal/runtime/run_state
import temporal/internal/runtime/scopes
import temporal/internal/time
import temporal/middleware
import temporal/retry
import temporal/workflow.{
  type Context, type Definition, type Future, type IdReusePolicy,
  type SignalDefinition,
}

/// What happens to a child when its parent closes.
pub type ParentClosePolicy {
  /// The default.
  Terminate
  /// The child carries on.
  Abandon
  /// The child is asked to cancel.
  RequestCancel
}

/// What cancelling a child's scope does.
pub type Cancellation {
  /// Request cancellation and wait until the child has closed. The default.
  WaitUntilCancelled
  /// Request cancellation and wait only until the child has been told.
  WaitUntilRequested
  /// Request cancellation and fail the parent's wait at once.
  TryCancel
  /// Fail the parent's wait at once without telling the child.
  DoNotCancel
}

/// How a child workflow is run. Build with `options` and the `with_` functions.
pub type Options {
  Options(
    /// No two open workflows in a namespace share an id.
    workflow_id: String,
    /// `None` runs the child on the parent's task queue.
    task_queue: Option(String),
    /// What happens to the child when the parent closes.
    parent_close_policy: ParentClosePolicy,
    /// Whether the id may be used again after a run with it has closed.
    id_reuse_policy: IdReusePolicy,
    /// Longest the child may take across retries and continue-as-new.
    execution_timeout: Option(Duration),
    /// Longest a single run of the child may take.
    run_timeout: Option(Duration),
    /// Longest one workflow task may take. `None` is ten seconds.
    task_timeout: Option(Duration),
    /// `None` does not retry a failed child.
    retry: Option(retry.Policy),
    /// Unindexed data the child can read with `workflow.memo`.
    memo: Dict(String, Payload),
    /// What cancelling the child's scope does.
    cancellation: Cancellation,
  )
}

/// A child workflow the parent has asked for.
pub opaque type Child(output) {
  Child(
    workflow_id: String,
    started: Future(Result(String, Failure)),
    result: Future(Result(output, Failure)),
  )
}

/// Options for a child with this id: the parent's task queue, terminated when the parent
/// closes, not retried.
pub fn options(workflow_id workflow_id: String) -> Options {
  Options(
    workflow_id:,
    task_queue: None,
    parent_close_policy: Terminate,
    id_reuse_policy: workflow.AllowDuplicate,
    execution_timeout: None,
    run_timeout: None,
    task_timeout: None,
    retry: None,
    memo: dict.new(),
    cancellation: WaitUntilCancelled,
  )
}

/// Run the child on this task queue instead of the parent's.
pub fn with_task_queue(options: Options, task_queue: String) -> Options {
  Options(..options, task_queue: Some(task_queue))
}

/// What happens to the child when the parent closes.
pub fn with_parent_close_policy(
  options: Options,
  policy: ParentClosePolicy,
) -> Options {
  Options(..options, parent_close_policy: policy)
}

/// Whether the child's id may be used again after a run with it has closed.
pub fn with_id_reuse_policy(
  options: Options,
  policy: IdReusePolicy,
) -> Options {
  Options(..options, id_reuse_policy: policy)
}

/// The longest the child may take across retries and continue-as-new.
pub fn with_execution_timeout(options: Options, timeout: Duration) -> Options {
  Options(..options, execution_timeout: Some(timeout))
}

/// The longest a single run of the child may take.
pub fn with_run_timeout(options: Options, timeout: Duration) -> Options {
  Options(..options, run_timeout: Some(timeout))
}

/// The longest one workflow task of the child may take, instead of ten seconds.
pub fn with_task_timeout(options: Options, timeout: Duration) -> Options {
  Options(..options, task_timeout: Some(timeout))
}

/// Retry the child by this policy when it fails. Without one it is not retried.
pub fn with_retry(options: Options, policy: retry.Policy) -> Options {
  Options(..options, retry: Some(policy))
}

/// Unindexed data the child can read with `workflow.memo`.
pub fn with_memo(options: Options, memo: Dict(String, Payload)) -> Options {
  Options(..options, memo:)
}

/// What cancelling the child's scope does.
pub fn with_cancellation(
  options: Options,
  cancellation: Cancellation,
) -> Options {
  Options(..options, cancellation:)
}

/// Ask for a child workflow. Returns at once; the server starts the child when the
/// workflow next waits.
///
/// A parent that returns before the child has started never starts it, so a child that is
/// meant to outlive its parent (`Abandon`) must at least be awaited with `started`.
pub fn start(
  ctx: Context,
  definition: Definition(input, output),
  input: input,
  options: Options,
) -> Child(output) {
  let seq = determinism.next_child()
  case scopes.cancelled(workflow.scope_of(ctx)) {
    Some(_) ->
      Child(
        workflow_id: options.workflow_id,
        started: workflow.ready(Error(failure.cancelled())),
        result: workflow.ready(Error(failure.cancelled())),
      )
    None -> {
      let state = run_state.get()
      let call =
        state.outbound.child(middleware.ChildCall(
          workflow_type: definition.name,
          workflow_id: options.workflow_id,
          task_queue: option.unwrap(options.task_queue, state.task_queue),
          arguments: codec.encode_arguments(definition.input, input),
          headers: state.headers,
        ))
      run_state.push(bridge.StartChildWorkflow(
        seq:,
        workflow_id: call.workflow_id,
        workflow_type: call.workflow_type,
        task_queue: call.task_queue,
        input: call.arguments,
        options: bridge.ChildStartOptions(
          execution_timeout_ms: time.option_to_ms(options.execution_timeout),
          run_timeout_ms: time.option_to_ms(options.run_timeout),
          task_timeout_ms: time.option_to_ms(options.task_timeout),
          parent_close_policy: case options.parent_close_policy {
            Terminate -> 1
            Abandon -> 2
            RequestCancel -> 3
          },
          id_reuse_policy: case options.id_reuse_policy {
            workflow.AllowDuplicate -> 1
            workflow.AllowDuplicateFailedOnly -> 2
            workflow.RejectDuplicate -> 3
          },
          retry: option.map(options.retry, retry.to_bridge),
          memo: options.memo,
          search_attributes: dict.new(),
          cancellation_type: case options.cancellation {
            DoNotCancel -> 0
            TryCancel -> 1
            WaitUntilCancelled -> 2
            WaitUntilRequested -> 3
          },
          headers: call.headers,
        ),
      ))
      scopes.register(workflow.scope_of(ctx), run_state.ChildOp(seq))
      let started =
        workflow.future(fn() {
          case dict.get(run_state.get().child_starts, seq) {
            Ok(bridge.ChildStarted(run_id)) -> Some(Ok(run_id))
            Ok(bridge.ChildStartFailed(workflow_id, cause)) ->
              Some(Error(start_failure(workflow_id, cause)))
            Ok(bridge.ChildStartCancelled(failure)) -> Some(Error(failure))
            Error(Nil) -> None
          }
        })
      let finished =
        workflow.resolution(
          fn() { dict.get(run_state.get().children, seq) },
          definition.output,
          "child workflow \"" <> definition.name <> "\"",
        )
      Child(
        workflow_id: call.workflow_id,
        started:,
        // A child that never started has no result of its own.
        result: workflow.future(fn() {
          case workflow.peek(started) {
            Some(Error(failure)) -> Some(Error(failure))
            _ -> workflow.peek(finished)
          }
        }),
      )
    }
  }
}

fn start_failure(workflow_id: String, cause: Int) -> Failure {
  failure.new("child workflow \"" <> workflow_id <> "\" could not be started")
  |> failure.with_type(case cause {
    // The API's `StartChildWorkflowExecutionFailedCause` value.
    1 -> "WorkflowExecutionAlreadyStarted"
    _ -> "ChildWorkflowStartFailed"
  })
  |> failure.non_retryable
}

/// The id of the child's first run, once the server has started it. Fails if it could not
/// be started, for example because a workflow with its id is already running.
pub fn started(child: Child(output)) -> Future(Result(String, Failure)) {
  child.started
}

/// The child's result, once it finishes. Fails with the start failure if it never started.
pub fn result(child: Child(output)) -> Future(Result(output, Failure)) {
  child.result
}

/// The child's workflow id.
pub fn workflow_id(child: Child(output)) -> String {
  child.workflow_id
}

/// Start a child workflow and wait for its result.
pub fn execute(
  ctx: Context,
  definition: Definition(input, output),
  input: input,
  options: Options,
) -> Result(output, Failure) {
  workflow.await(start(ctx, definition, input, options).result)
}

/// Signal a child. The future resolves once the server has delivered the signal.
pub fn signal(
  ctx: Context,
  child: Child(output),
  definition: SignalDefinition(a),
  value: a,
) -> Future(Result(Nil, Failure)) {
  workflow.send_signal(
    ctx,
    bridge.ChildWorkflowTarget(child.workflow_id),
    definition,
    value,
  )
}
