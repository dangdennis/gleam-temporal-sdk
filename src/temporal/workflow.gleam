//// Define workflows and write workflow code.
////
//// Workflow code must be deterministic: on replay it has to issue the same commands in the
//// same order. Use this module for everything that touches the outside world or varies
//// between runs (`now`, `random_int`, `uuid4`, `sleep`, activities), and never call clocks,
//// random generators, processes or IO directly from a workflow function.
////
//// ```gleam
//// pub fn greet() {
////   workflow.Definition("greet", codec.string(), codec.string())
//// }
////
//// pub fn greet_workflow(ctx: workflow.Context, name: String) {
////   let options = workflow.activity_options(start_to_close_ms: 10_000)
////   workflow.execute_activity(ctx, say_hello(), name, options)
//// }
//// ```
////
//// Cancellation travels with the `Context`, as in Go. Every timer, activity and child
//// workflow belongs to the scope of the context it was started with, and fails with a
//// cancellation (see `failure.is_cancellation`) when that scope is cancelled. A request to
//// cancel the workflow cancels the context the workflow function was given. Use
//// `with_scope`, `with_timeout` and `non_cancellable` to run part of a workflow under a
//// scope of its own.

import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set
import gleam/string
import temporal/activity
import temporal/codec.{type Codec, type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/runtime.{State}
import temporal/middleware

/// A workflow's name and the codecs for its argument and result. Shared by the code that
/// implements the workflow and the code that starts it, so both agree on the types.
pub type Definition(input, output) {
  Definition(name: String, input: Codec(input), output: Codec(output))
}

pub type SignalDefinition(a) {
  SignalDefinition(name: String, codec: Codec(a))
}

pub type QueryDefinition(argument, result) {
  QueryDefinition(
    name: String,
    argument: Codec(argument),
    result: Codec(result),
  )
}

/// Proof that the caller is workflow code, and the cancellation scope it runs in. Only the
/// worker creates the first one.
pub opaque type Context {
  Context(scope: Int)
}

/// A workflow implementation, ready to register on a worker.
pub type Handler {
  Handler(name: String, run: runtime.WorkflowFn)
}

/// Code that runs around every workflow on a worker: call `next` to run the workflow and
/// return its outcome, changed or not. It runs as workflow code, so it must be
/// deterministic; do side effects such as error reporting through `unless_replaying`.
pub type Middleware =
  fn(Context, fn() -> Result(Option(Payload), Failure)) ->
    Result(Option(Payload), Failure)

/// Something that will have a value later in the workflow: a timer, an activity result, a
/// signal. Combine with `map` and `race`; get the value with `await`.
pub opaque type Future(a) {
  Future(poll: fn() -> Option(a))
}

pub type Info {
  Info(
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    task_queue: String,
    attempt: Int,
    /// The first run of the chain this run belongs to: the same across continue-as-new.
    first_execution_run_id: String,
    /// The run this one continues, if it was started by continue-as-new.
    continued_from_run_id: Option(String),
    /// Events in the run's history so far.
    history_length: Int,
    /// The server's hint that the history is long enough to continue as new.
    continue_as_new_suggested: Bool,
  )
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

/// What cancelling an activity from the workflow does.
pub type ActivityCancellation {
  /// Ask the activity to cancel and fail the workflow's wait at once. The default.
  TryCancel
  /// Ask the activity to cancel and wait until it reports that it stopped.
  WaitCancellationCompleted
  /// Fail the workflow's wait at once without telling the activity.
  AbandonActivity
}

pub type ActivityOptions {
  ActivityOptions(
    /// Longest a single attempt may run.
    start_to_close_ms: Int,
    /// Longest the activity may take across all retries.
    schedule_to_close_ms: Option(Int),
    /// Longest the activity may wait for a worker to pick it up.
    schedule_to_start_ms: Option(Int),
    heartbeat_timeout_ms: Option(Int),
    /// `None` uses the server's default policy, which retries without limit.
    retry: Option(RetryPolicy),
    /// `None` runs the activity on the workflow's own task queue.
    task_queue: Option(String),
    cancellation: ActivityCancellation,
  )
}

/// What happens to a child workflow when its parent closes.
pub type ParentClosePolicy {
  /// The default.
  TerminateChild
  /// The child carries on.
  AbandonChild
  RequestCancelChild
}

/// Whether a workflow id may be used again after a run with that id has closed.
pub type IdReusePolicy {
  /// The default.
  AllowDuplicate
  /// Only if the last run did not complete successfully.
  AllowDuplicateFailedOnly
  RejectDuplicate
}

/// What cancelling a child workflow from the parent does.
pub type ChildCancellation {
  /// Request cancellation and wait until the child has closed. The default.
  WaitChildCancelled
  /// Request cancellation and wait only until the child has been told.
  WaitChildCancelRequested
  /// Request cancellation and fail the parent's wait at once.
  TryCancelChild
  /// Fail the parent's wait at once without telling the child.
  AbandonChildCancellation
}

pub type ChildOptions {
  ChildOptions(
    workflow_id: String,
    /// `None` runs the child on the parent's task queue.
    task_queue: Option(String),
    parent_close_policy: ParentClosePolicy,
    id_reuse_policy: IdReusePolicy,
    /// Longest the child may take across retries and continue-as-new.
    execution_timeout_ms: Option(Int),
    /// Longest a single run of the child may take.
    run_timeout_ms: Option(Int),
    task_timeout_ms: Option(Int),
    /// `None` does not retry a failed child.
    retry: Option(RetryPolicy),
    memo: Dict(String, Payload),
    cancellation: ChildCancellation,
  )
}

/// A child workflow that has started.
pub opaque type Child(output) {
  Child(
    workflow_id: String,
    run_id: String,
    result: Future(Result(output, Failure)),
  )
}

/// Pair a definition with its implementation.
pub fn handler(
  definition: Definition(input, output),
  run: fn(Context, input) -> Result(output, Failure),
) -> Handler {
  Handler(definition.name, fn(arguments) {
    case codec.decode_argument(definition.input, arguments) {
      Ok(input) ->
        run(Context(runtime.root_scope), input)
        |> result.map(fn(output) { Some(definition.output.encode(output)) })
      Error(reason) ->
        Error(
          failure.new("could not decode workflow input: " <> reason)
          |> failure.with_type("InvalidArgument")
          |> failure.non_retryable,
        )
    }
  })
}

/// Run `middleware` around the handler. The first middleware applied is the innermost.
pub fn wrap(handler: Handler, middleware: Middleware) -> Handler {
  Handler(handler.name, fn(arguments) {
    middleware(Context(runtime.root_scope), fn() { handler.run(arguments) })
  })
}

pub fn info(_ctx: Context) -> Info {
  let state = runtime.get()
  Info(
    workflow_id: state.workflow_id,
    run_id: state.run_id,
    workflow_type: state.workflow_type,
    task_queue: state.task_queue,
    attempt: state.attempt,
    first_execution_run_id: state.first_execution_run_id,
    continued_from_run_id: case state.continued_from_run_id {
      "" -> None
      run_id -> Some(run_id)
    },
    history_length: state.history_length,
    continue_as_new_suggested: state.continue_as_new_suggested,
  )
}

/// A value from the memo the workflow was started with. `None` if the key is absent or its
/// value does not decode.
pub fn memo(_ctx: Context, key: String, codec: Codec(a)) -> Option(a) {
  case dict.get(runtime.get().memo, key) {
    Ok(payload) -> option.from_result(codec.decode(payload))
    Error(Nil) -> None
  }
}

/// The workflow's current time in milliseconds since the Unix epoch. It is the time of the
/// workflow task being processed, so it is the same on replay.
pub fn now(_ctx: Context) -> Int {
  runtime.get().now_ms
}

/// A deterministic random integer in `0..max` (exclusive). `max` must be positive.
pub fn random_int(_ctx: Context, max: Int) -> Int {
  runtime.random_bits() % max
}

/// A deterministic random version 4 UUID.
pub fn uuid4(_ctx: Context) -> String {
  let high = runtime.random_bits()
  let low = runtime.random_bits()
  let assert <<a:32, b:16, _:4, c:12, _:2, d:14, e:48>> = <<high:64, low:64>>
  let hex =
    bit_array.base16_encode(<<a:32, b:16, 4:4, c:12, 2:2, d:14, e:48>>)
    |> string.lowercase
  string.slice(hex, 0, 8)
  <> "-"
  <> string.slice(hex, 8, 4)
  <> "-"
  <> string.slice(hex, 12, 4)
  <> "-"
  <> string.slice(hex, 16, 4)
  <> "-"
  <> string.slice(hex, 20, 12)
}

/// Whether the workflow is replaying history rather than making new progress. Only for
/// suppressing side effects that are safe to skip; prefer `unless_replaying`.
pub fn is_replaying(_ctx: Context) -> Bool {
  runtime.get().is_replaying
}

/// Run a side effect the first time the workflow reaches this point and skip it on replay.
/// For effects whose loss is acceptable, such as metrics and error reports: the effect is
/// not recorded in history and a crash in it is ignored.
pub fn unless_replaying(_ctx: Context, effect: fn() -> Nil) -> Nil {
  case runtime.get().is_replaying {
    True -> Nil
    False -> {
      let _ = bridge.rescue_all(effect)
      Nil
    }
  }
}

// ---- logging ----

fn log(level: bridge.LogLevel, message: String) -> Nil {
  let state = runtime.get()
  case state.is_replaying {
    True -> Nil
    False ->
      bridge.log(
        level,
        message,
        dict.from_list([
          #("workflow_id", state.workflow_id),
          #("run_id", state.run_id),
          #("workflow_type", state.workflow_type),
          #("task_queue", state.task_queue),
        ]),
      )
  }
}

/// Log through Erlang's `logger`, with the workflow's identity as metadata. Nothing is
/// logged on replay, so each line appears once however often the workflow is replayed.
pub fn log_debug(_ctx: Context, message: String) -> Nil {
  log(bridge.Debug, message)
}

pub fn log_info(_ctx: Context, message: String) -> Nil {
  log(bridge.Info, message)
}

pub fn log_warning(_ctx: Context, message: String) -> Nil {
  log(bridge.Warning, message)
}

pub fn log_error(_ctx: Context, message: String) -> Nil {
  log(bridge.Error, message)
}

// ---- futures ----

/// Block the workflow until the future has a value.
pub fn await(future: Future(a)) -> a {
  case future.poll() {
    Some(value) -> value
    None -> {
      runtime.block()
      await(future)
    }
  }
}

pub fn map(future: Future(a), with transform: fn(a) -> b) -> Future(b) {
  Future(fn() { option.map(future.poll(), transform) })
}

/// A future that already has its value.
pub fn ready(value: a) -> Future(a) {
  Future(fn() { Some(value) })
}

/// A future that has a value once `check` returns one. `check` runs each time the workflow
/// makes progress, so it must be cheap and must only read workflow state.
pub fn poll(check: fn() -> Option(a)) -> Future(a) {
  Future(check)
}

/// Wait for every future and return their values in the order given. The futures make
/// progress concurrently; only the waiting is sequential.
pub fn await_all(futures: List(Future(a))) -> List(a) {
  list.map(futures, await)
}

/// A future of the first value to become available. Earlier futures in the list win ties,
/// so the choice is deterministic.
pub fn race(futures: List(Future(a))) -> Future(a) {
  Future(fn() {
    list.find_map(futures, fn(future) { option.to_result(future.poll(), Nil) })
    |> option.from_result
  })
}

/// Wait for the future, but no longer than `ms` and only while the context is not
/// cancelled. Fails with a `Timeout` failure or a cancellation. This is how to wait for a
/// signal or a condition with a deadline.
pub fn await_timeout(
  ctx: Context,
  future: Future(a),
  ms: Int,
) -> Result(a, Failure) {
  with_scope(ctx, fn(inner) {
    let deadline =
      map(timer(inner, ms), fn(fired) {
        case fired {
          Ok(Nil) ->
            Error(failure.timeout(
              "timed out after " <> int.to_string(ms) <> "ms",
            ))
          Error(cancelled) -> Error(cancelled)
        }
      })
    let outcome = await(race([map(future, Ok), deadline]))
    cancel(inner)
    outcome
  })
}

/// Wait for the future until the context is cancelled.
pub fn await_cancellable(
  ctx: Context,
  future: Future(a),
) -> Result(a, Failure) {
  await(
    race([
      map(future, Ok),
      map(cancellation(ctx), fn(_) { Error(failure.cancelled()) }),
    ]),
  )
}

// ---- cancellation scopes ----

/// Run `body` in a scope of its own, which `cancel` can cancel without cancelling the rest
/// of the workflow. Cancelling `ctx` cancels the new scope too.
pub fn with_scope(ctx: Context, body: fn(Context) -> a) -> a {
  let scope = runtime.open_scope(ctx.scope, False)
  let value = body(Context(scope))
  runtime.close_scope(scope)
  value
}

/// Run `body` shielded from cancellation: cancelling `ctx`, or the workflow, does not reach
/// what `body` starts. For cleanup that must finish after a cancellation.
pub fn non_cancellable(ctx: Context, body: fn(Context) -> a) -> a {
  let scope = runtime.open_scope(ctx.scope, True)
  let value = body(Context(scope))
  runtime.close_scope(scope)
  value
}

/// Run `body` in a scope that is cancelled after `ms`. If `body` then fails, the result is a
/// `Timeout` failure whose cause is the failure `body` returned.
pub fn with_timeout(
  ctx: Context,
  ms: Int,
  body: fn(Context) -> Result(a, Failure),
) -> Result(a, Failure) {
  let scope = runtime.open_scope(ctx.scope, False)
  let seq = runtime.next_timer()
  case runtime.scope_cancelled(ctx.scope) {
    Some(_) -> Nil
    None -> {
      runtime.push(bridge.StartTimer(seq, int.max(ms, 1)))
      runtime.register(ctx.scope, runtime.TimerOp(seq))
      runtime.set_deadline(seq, scope)
    }
  }
  let outcome = body(Context(scope))
  let timed_out = set.contains(runtime.get().fired_timers, seq)
  case timed_out, runtime.scope_cancelled(ctx.scope) {
    False, None -> runtime.cancel_timer(seq)
    _, _ -> Nil
  }
  runtime.close_scope(scope)
  case outcome, timed_out {
    Error(cause), True ->
      Error(
        failure.Failure(
          ..failure.timeout("timed out after " <> int.to_string(ms) <> "ms"),
          cause: Some(cause),
        ),
      )
    _, _ -> outcome
  }
}

/// Cancel the context's scope: everything pending that was started with it, or with a
/// context derived from it, is asked to cancel.
pub fn cancel(ctx: Context) -> Nil {
  runtime.cancel_scope(ctx.scope, "cancelled by workflow code")
}

pub fn is_cancelled(ctx: Context) -> Bool {
  option.is_some(runtime.scope_cancelled(ctx.scope))
}

/// A future that resolves, with the reason given, when the context is cancelled. For the
/// context a workflow function receives, that is when cancellation of the workflow is
/// requested.
pub fn cancellation(ctx: Context) -> Future(String) {
  Future(fn() { runtime.scope_cancelled(ctx.scope) })
}

// ---- timers ----

/// A durable timer. It keeps counting while no worker is running. Fails with a cancellation
/// if the context is cancelled first.
pub fn timer(ctx: Context, ms: Int) -> Future(Result(Nil, Failure)) {
  // A sequence number is used even when nothing is scheduled, as the other SDKs do, so
  // that later timers are numbered the same.
  let seq = runtime.next_timer()
  case runtime.scope_cancelled(ctx.scope) {
    Some(_) -> ready(Error(failure.cancelled()))
    None -> {
      runtime.push(bridge.StartTimer(seq, int.max(ms, 1)))
      runtime.register(ctx.scope, runtime.TimerOp(seq))
      Future(fn() {
        let state = runtime.get()
        case
          set.contains(state.fired_timers, seq),
          set.contains(state.cancelled_timers, seq)
        {
          True, _ -> Some(Ok(Nil))
          False, True -> Some(Error(failure.cancelled()))
          False, False -> None
        }
      })
    }
  }
}

pub fn sleep(ctx: Context, ms: Int) -> Result(Nil, Failure) {
  await(timer(ctx, ms))
}

// ---- activities ----

/// Options with only the per-attempt timeout set: default retries, the workflow's task queue.
pub fn activity_options(
  start_to_close_ms start_to_close_ms: Int,
) -> ActivityOptions {
  ActivityOptions(
    start_to_close_ms:,
    schedule_to_close_ms: None,
    schedule_to_start_ms: None,
    heartbeat_timeout_ms: None,
    retry: None,
    task_queue: None,
    cancellation: TryCancel,
  )
}

pub fn with_retry(
  options: ActivityOptions,
  retry: RetryPolicy,
) -> ActivityOptions {
  ActivityOptions(..options, retry: Some(retry))
}

/// A retry policy that gives up after `maximum_attempts` attempts, one second apart at first
/// and doubling.
pub fn retry_policy(maximum_attempts maximum_attempts: Int) -> RetryPolicy {
  RetryPolicy(
    initial_interval_ms: 1000,
    backoff_coefficient: 2.0,
    maximum_interval_ms: None,
    maximum_attempts:,
    non_retryable_error_types: [],
  )
}

fn bridge_retry(retry: RetryPolicy) -> bridge.RetryPolicy {
  bridge.RetryPolicy(
    initial_interval_ms: retry.initial_interval_ms,
    backoff_coefficient: retry.backoff_coefficient,
    maximum_interval_ms: retry.maximum_interval_ms,
    maximum_attempts: retry.maximum_attempts,
    non_retryable_error_types: retry.non_retryable_error_types,
  )
}

/// Schedule an activity. The failure, if any, is an `Activity` failure whose `cause` is the
/// error the activity returned; see `failure.root_cause`.
pub fn start_activity(
  ctx: Context,
  definition: activity.Definition(input, output),
  input: input,
  options: ActivityOptions,
) -> Future(Result(output, Failure)) {
  let seq = runtime.next_activity()
  case runtime.scope_cancelled(ctx.scope) {
    Some(_) -> ready(Error(failure.cancelled()))
    None -> {
      let state = runtime.get()
      let call =
        state.outbound.activity(
          middleware.ActivityCall(
            activity_type: definition.name,
            task_queue: option.unwrap(options.task_queue, state.task_queue),
            arguments: [definition.input.encode(input)],
          ),
        )
      runtime.push(
        bridge.ScheduleActivity(
          seq:,
          activity_id: int.to_string(seq),
          activity_type: call.activity_type,
          task_queue: call.task_queue,
          arguments: call.arguments,
          schedule_to_close_ms: options.schedule_to_close_ms,
          start_to_close_ms: Some(options.start_to_close_ms),
          heartbeat_timeout_ms: options.heartbeat_timeout_ms,
          retry: option.map(options.retry, bridge_retry),
          schedule_to_start_ms: options.schedule_to_start_ms,
          cancellation_type: case options.cancellation {
            TryCancel -> 0
            WaitCancellationCompleted -> 1
            AbandonActivity -> 2
          },
        ),
      )
      runtime.register(ctx.scope, runtime.ActivityOp(seq))
      resolution(
        fn() { dict.get(runtime.get().activities, seq) },
        definition.output,
      )
    }
  }
}

/// Schedule an activity and wait for its result.
pub fn execute_activity(
  ctx: Context,
  definition: activity.Definition(input, output),
  input: input,
  options: ActivityOptions,
) -> Result(output, Failure) {
  await(start_activity(ctx, definition, input, options))
}

fn resolution(
  lookup: fn() -> Result(bridge.Resolution, Nil),
  output: Codec(a),
) -> Future(Result(a, Failure)) {
  Future(fn() {
    case lookup() {
      Error(Nil) -> None
      Ok(bridge.Completed(payload)) ->
        Some(
          codec.decode_argument(output, option.values([payload]))
          |> result.map_error(fn(reason) {
            failure.new("could not decode result: " <> reason)
            |> failure.with_type("InvalidResult")
            |> failure.non_retryable
          }),
        )
      Ok(bridge.Failed(failure)) | Ok(bridge.Cancelled(failure)) ->
        Some(Error(failure))
    }
  })
}

// ---- child workflows ----

/// Options for a child with this id: the parent's task queue, terminated when the parent
/// closes, not retried.
pub fn child_options(workflow_id workflow_id: String) -> ChildOptions {
  ChildOptions(
    workflow_id:,
    task_queue: None,
    parent_close_policy: TerminateChild,
    id_reuse_policy: AllowDuplicate,
    execution_timeout_ms: None,
    run_timeout_ms: None,
    task_timeout_ms: None,
    retry: None,
    memo: dict.new(),
    cancellation: WaitChildCancelled,
  )
}

/// Start a child workflow and wait until the server has started it. A child with an
/// `AbandonChild` policy keeps running after the parent returns; any other child must be
/// awaited with `child_result` before the parent finishes.
pub fn start_child(
  ctx: Context,
  definition: Definition(input, output),
  input: input,
  options: ChildOptions,
) -> Result(Child(output), Failure) {
  let seq = runtime.next_child()
  case runtime.scope_cancelled(ctx.scope) {
    Some(_) -> Error(failure.cancelled())
    None -> {
      let state = runtime.get()
      let call =
        state.outbound.child(
          middleware.ChildCall(
            workflow_type: definition.name,
            workflow_id: options.workflow_id,
            task_queue: option.unwrap(options.task_queue, state.task_queue),
            arguments: [definition.input.encode(input)],
          ),
        )
      runtime.push(bridge.StartChildWorkflow(
        seq:,
        workflow_id: call.workflow_id,
        workflow_type: call.workflow_type,
        task_queue: call.task_queue,
        input: call.arguments,
        options: bridge.ChildStartOptions(
          execution_timeout_ms: options.execution_timeout_ms,
          run_timeout_ms: options.run_timeout_ms,
          task_timeout_ms: options.task_timeout_ms,
          parent_close_policy: case options.parent_close_policy {
            TerminateChild -> 1
            AbandonChild -> 2
            RequestCancelChild -> 3
          },
          id_reuse_policy: case options.id_reuse_policy {
            AllowDuplicate -> 1
            AllowDuplicateFailedOnly -> 2
            RejectDuplicate -> 3
          },
          retry: option.map(options.retry, bridge_retry),
          memo: options.memo,
          search_attributes: dict.new(),
          cancellation_type: case options.cancellation {
            AbandonChildCancellation -> 0
            TryCancelChild -> 1
            WaitChildCancelled -> 2
            WaitChildCancelRequested -> 3
          },
        ),
      ))
      runtime.register(ctx.scope, runtime.ChildOp(seq))
      let started =
        Future(fn() {
          case dict.get(runtime.get().child_starts, seq) {
            Ok(bridge.ChildStarted(run_id)) -> Some(Ok(run_id))
            Ok(bridge.ChildStartFailed(workflow_id, cause)) ->
              Some(Error(
                failure.new(
                  "child workflow \""
                  <> workflow_id
                  <> "\" could not be started",
                )
                |> failure.with_type(case cause {
                  1 -> "WorkflowExecutionAlreadyStarted"
                  _ -> "ChildWorkflowStartFailed"
                })
                |> failure.non_retryable,
              ))
            Ok(bridge.ChildStartCancelled(failure)) -> Some(Error(failure))
            Error(Nil) -> None
          }
        })
      use run_id <- result.map(await(started))
      Child(
        workflow_id: call.workflow_id,
        run_id:,
        result: resolution(
          fn() { dict.get(runtime.get().children, seq) },
          definition.output,
        ),
      )
    }
  }
}

/// The child's result, once it finishes.
pub fn child_result(child: Child(output)) -> Future(Result(output, Failure)) {
  child.result
}

pub fn child_workflow_id(child: Child(output)) -> String {
  child.workflow_id
}

pub fn child_run_id(child: Child(output)) -> String {
  child.run_id
}

/// Start a child workflow and wait for its result.
pub fn execute_child(
  ctx: Context,
  definition: Definition(input, output),
  input: input,
  options: ChildOptions,
) -> Result(output, Failure) {
  use child <- result.try(start_child(ctx, definition, input, options))
  await(child.result)
}

// ---- signals and queries ----

/// A future of the next signal of this kind. Signals that arrived before anyone asked are
/// buffered and delivered in order. A signal whose payload does not decode is dropped.
pub fn signal(_ctx: Context, definition: SignalDefinition(a)) -> Future(a) {
  Future(fn() { next_signal(definition) })
}

fn next_signal(definition: SignalDefinition(a)) -> Option(a) {
  case runtime.take_signal(definition.name) {
    None -> None
    Some(payloads) ->
      case codec.decode_argument(definition.codec, payloads) {
        Ok(value) -> Some(value)
        Error(_) -> next_signal(definition)
      }
  }
}

/// Wait for the next signal of this kind.
pub fn receive_signal(ctx: Context, definition: SignalDefinition(a)) -> a {
  await(signal(ctx, definition))
}

/// The next signal of this kind if one has already arrived. Does not wait.
pub fn try_receive_signal(
  _ctx: Context,
  definition: SignalDefinition(a),
) -> Option(a) {
  next_signal(definition)
}

/// Answer queries of this kind with `handler`. Workflow state lives in local variables, so
/// call this again with a new closure whenever the answer should change. The handler must
/// not use any other function of this module.
pub fn set_query_handler(
  _ctx: Context,
  definition: QueryDefinition(argument, result),
  handler: fn(argument) -> result,
) -> Nil {
  let erased = fn(arguments) {
    codec.decode_argument(definition.argument, arguments)
    |> result.map(fn(argument) { definition.result.encode(handler(argument)) })
  }
  runtime.update(fn(state) {
    State(
      ..state,
      query_handlers: dict.insert(state.query_handlers, definition.name, erased),
    )
  })
}

// ---- lifecycle ----

/// End this run and start a fresh one with a new input and an empty history. Does not return.
pub fn continue_as_new(
  _ctx: Context,
  definition: Definition(input, output),
  input: input,
) -> Result(output, Failure) {
  runtime.push(
    bridge.ContinueAsNew(definition.name, runtime.get().task_queue, [
      definition.input.encode(input),
    ]),
  )
  runtime.serve()
}

/// Guard a change to workflow logic so that runs started before the change replay the old
/// code. Returns `True` on the new code path:
///
/// ```gleam
/// case workflow.patched(ctx, "add-audit-step") {
///   True -> new_logic()
///   False -> old_logic()
/// }
/// ```
pub fn patched(_ctx: Context, patch_id: String) -> Bool {
  runtime.patched(patch_id)
}

/// The second step of retiring a patch. Once no run that took the old path can still
/// replay, replace the `patched` call with this and delete the old path; once no run that
/// recorded the patch can still replay, delete this too.
pub fn deprecate_patch(_ctx: Context, patch_id: String) -> Nil {
  runtime.deprecate_patch(patch_id)
}
