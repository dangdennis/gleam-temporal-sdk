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
////   let options = workflow.activity_options(start_to_close: duration.seconds(10))
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
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import temporal/activity
import temporal/codec.{type Codec, type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/runtime.{State}
import temporal/internal/time
import temporal/middleware
import temporal/search_attributes

/// A workflow's name and the codecs for its argument and result. Shared by the code that
/// implements the workflow and the code that starts it, so both agree on the types.
pub type Definition(input, output) {
  Definition(name: String, input: Codec(input), output: Codec(output))
}

/// A signal's name and the codec for its value. A signal is a one-way message to a running
/// workflow.
pub type SignalDefinition(a) {
  SignalDefinition(name: String, codec: Codec(a))
}

/// A query's name and the codecs for its argument and result. A query reads a workflow's
/// state without changing it.
pub type QueryDefinition(argument, result) {
  QueryDefinition(
    name: String,
    argument: Codec(argument),
    result: Codec(result),
  )
}

/// An update's name and the codecs for its argument and result. An update is a request a
/// client sends to a running workflow and waits on: unlike a signal it returns a value, and
/// unlike a query it may change the workflow.
pub type UpdateDefinition(argument, result) {
  UpdateDefinition(
    name: String,
    argument: Codec(argument),
    result: Codec(result),
  )
}

/// An operation another team exposes through a Nexus endpoint: the service it belongs to,
/// its name, and the codecs for its input and output. Shared by the workflows that call it
/// and the handler that implements it.
pub type NexusOperation(input, output) {
  NexusOperation(
    service: String,
    name: String,
    input: Codec(input),
    output: Codec(output),
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

/// What a workflow knows about its own run.
pub type Info {
  Info(
    workflow_id: String,
    /// Changes with every run: after continue-as-new and after a retry.
    run_id: String,
    workflow_type: String,
    task_queue: String,
    /// Starts at 1 and grows each time the workflow is retried.
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

/// How failures are retried. The wait before attempt `n` is `initial_interval` times
/// `backoff_coefficient` to the power `n - 1`, capped at `maximum_interval`.
pub type RetryPolicy {
  RetryPolicy(
    /// The wait before the second attempt.
    initial_interval: Duration,
    /// What the wait is multiplied by after each attempt.
    backoff_coefficient: Float,
    /// The longest wait. `None` is one hundred times the initial interval.
    maximum_interval: Option(Duration),
    /// Zero means unlimited.
    maximum_attempts: Int,
    /// Failures whose `error_type` is listed here are not retried.
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

/// How an activity is run. Build with `activity_options` and change what differs.
pub type ActivityOptions {
  ActivityOptions(
    /// Longest a single attempt may run.
    start_to_close: Duration,
    /// Longest the activity may take across all retries.
    schedule_to_close: Option(Duration),
    /// Longest the activity may wait for a worker to pick it up.
    schedule_to_start: Option(Duration),
    /// The longest gap between heartbeats before the attempt is failed. `None` never fails
    /// an attempt for silence.
    heartbeat_timeout: Option(Duration),
    /// `None` uses the server's default policy, which retries without limit.
    retry: Option(RetryPolicy),
    /// `None` runs the activity on the workflow's own task queue.
    task_queue: Option(String),
    /// What cancelling the activity's scope does.
    cancellation: ActivityCancellation,
    /// The id the server knows the activity by. `None` numbers activities in the order
    /// the workflow schedules them, as the TypeScript and Python SDKs do.
    activity_id: Option(String),
  )
}

/// Whether a workflow id may be used again after a run with that id has closed.
pub type IdReusePolicy {
  /// The default.
  AllowDuplicate
  /// Only if the last run did not complete successfully.
  AllowDuplicateFailedOnly
  /// Never.
  RejectDuplicate
}

/// The cancellation scope a context names.
@internal
pub fn scope_of(ctx: Context) -> Int {
  ctx.scope
}

/// The future's value if it has one, without waiting.
@internal
pub fn peek(future: Future(a)) -> Option(a) {
  future.poll()
}

/// A future that asks `poll` every time it is asked.
@internal
pub fn future(poll: fn() -> Option(a)) -> Future(a) {
  Future(poll)
}

/// Pair a definition with its implementation.
pub fn handler(
  definition: Definition(input, output),
  run: fn(Context, input) -> Result(output, Failure),
) -> Handler {
  Handler(definition.name, fn(arguments) {
    case codec.decode_arguments(definition.input, arguments) {
      Ok(input) ->
        run(Context(runtime.root_scope), input)
        |> result.map(fn(output) {
          Some(codec.encode(definition.output, output))
        })
      Error(reason) ->
        Error(
          failure.new("could not decode workflow input: " <> reason)
          |> failure.with_type("InvalidArgument")
          |> failure.non_retryable,
        )
    }
  })
}

/// Make a crash in this workflow's code fail the workflow, with a non-retryable
/// `WorkflowCrashed` failure, instead of failing the workflow task.
///
/// By default a crash, such as a failed `let assert`, fails only the task, and the server
/// retries it for as long as it takes: the workflow resumes once fixed code is deployed.
/// That is the safer default for a bug. Use this for workflows where failing fast and
/// letting the caller react is better than waiting for a fix.
pub fn fail_on_crash(handler: Handler) -> Handler {
  Handler(handler.name, fn(arguments) {
    case bridge.rescue(fn() { handler.run(arguments) }) {
      Ok(outcome) -> outcome
      Error(crash) ->
        Error(
          failure.new("workflow code crashed: " <> crash)
          |> failure.with_type("WorkflowCrashed")
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

/// The run's identity and how far it has got.
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
pub fn memo(_ctx: Context, key: String, value_codec: Codec(a)) -> Option(a) {
  case dict.get(runtime.get().memo, key) {
    Ok(payload) -> option.from_result(codec.decode(value_codec, payload))
    Error(Nil) -> None
  }
}

/// The headers the workflow was started with: context, such as a trace id, that travels
/// beside the arguments. They are passed on to the activities and child workflows the
/// workflow starts unless outbound middleware changes them.
pub fn headers(_ctx: Context) -> Dict(String, Payload) {
  runtime.get().headers
}

/// The workflow's current time in milliseconds since the Unix epoch. It is the time of the
/// workflow task being processed, so it is the same on replay.
pub fn now(_ctx: Context) -> Timestamp {
  time.timestamp(runtime.get().now_ms)
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

/// Log at info level. See `log_debug`.
pub fn log_info(_ctx: Context, message: String) -> Nil {
  log(bridge.Info, message)
}

/// Log at warning level. See `log_debug`.
pub fn log_warning(_ctx: Context, message: String) -> Nil {
  log(bridge.Warning, message)
}

/// Log at error level. See `log_debug`.
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

/// A future of `transform` applied to the value of `future`.
pub fn map(future: Future(a), with transform: fn(a) -> b) -> Future(b) {
  once(fn() { option.map(future.poll(), transform) })
}

/// A future that keeps its value once it has one, however often it is asked. Without
/// this, a future that takes something when it resolves, such as the next signal, would
/// take another each time it was awaited.
@internal
pub fn once(poll: fn() -> Option(a)) -> Future(a) {
  let cell = bridge.new_cell()
  Future(fn() {
    case bridge.read_cell(cell) {
      Some(value) -> Some(value)
      None -> {
        let value = poll()
        case value {
          Some(value) -> bridge.write_cell(cell, value)
          None -> Nil
        }
        value
      }
    }
  })
}

/// A future that already has its value.
pub fn ready(value: a) -> Future(a) {
  Future(fn() { Some(value) })
}

/// A future that has a value once `check` returns one. `check` runs each time the workflow
/// makes progress, so it must be cheap and must only read workflow state.
pub fn poll(check: fn() -> Option(a)) -> Future(a) {
  once(check)
}

/// Wait for every future and return their values in the order given. The futures make
/// progress concurrently; only the waiting is sequential.
pub fn await_all(futures: List(Future(a))) -> List(a) {
  list.map(futures, await)
}

/// A future of the first value to become available. Earlier futures in the list win ties,
/// so the choice is deterministic.
pub fn race(futures: List(Future(a))) -> Future(a) {
  once(fn() {
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
  within: Duration,
) -> Result(a, Failure) {
  with_scope(ctx, fn(inner) {
    let deadline =
      map(timer(inner, within), fn(fired) {
        case fired {
          Ok(Nil) ->
            Error(failure.timeout("timed out after " <> time.describe(within)))
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
  within: Duration,
  body: fn(Context) -> Result(a, Failure),
) -> Result(a, Failure) {
  let scope = runtime.open_scope(ctx.scope, False)
  // The deadline timer is the scope's first operation, as in the TypeScript SDK.
  let seq = runtime.next_timer()
  case runtime.scope_cancelled(scope) {
    Some(_) -> Nil
    None -> {
      runtime.push(bridge.StartTimer(seq, timer_ms(within)))
      runtime.register(scope, runtime.TimerOp(seq))
      runtime.set_deadline(seq, scope)
    }
  }
  let outcome = body(Context(scope))
  let timed_out = set.contains(runtime.get().fired_timers, seq)
  case runtime.scope_cancelled(scope) {
    Some(_) -> Nil
    None -> runtime.cancel_timer(seq)
  }
  runtime.close_scope(scope)
  case outcome, timed_out {
    Error(cause), True ->
      Error(
        failure.Failure(
          ..failure.timeout("timed out after " <> time.describe(within)),
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

/// Whether the context has been cancelled.
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
pub fn timer(ctx: Context, lasting: Duration) -> Future(Result(Nil, Failure)) {
  // A sequence number is used even when nothing is scheduled, as the other SDKs do, so
  // that later timers are numbered the same.
  let seq = runtime.next_timer()
  case runtime.scope_cancelled(ctx.scope) {
    Some(_) -> ready(Error(failure.cancelled()))
    None -> {
      runtime.push(bridge.StartTimer(seq, timer_ms(lasting)))
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

/// The server's timers are no finer than a millisecond.
fn timer_ms(lasting: Duration) -> Int {
  int.max(time.to_ms(lasting), 1)
}

/// Wait with a durable timer. Fails with a cancellation if the context is cancelled first.
pub fn sleep(ctx: Context, lasting: Duration) -> Result(Nil, Failure) {
  await(timer(ctx, lasting))
}

// ---- activities ----

/// Options with only the per-attempt timeout set: default retries, the workflow's task queue.
pub fn activity_options(
  start_to_close start_to_close: Duration,
) -> ActivityOptions {
  ActivityOptions(
    start_to_close:,
    schedule_to_close: None,
    schedule_to_start: None,
    heartbeat_timeout: None,
    retry: None,
    task_queue: None,
    cancellation: TryCancel,
    activity_id: None,
  )
}

/// A retry policy that gives up after `maximum_attempts` attempts, one second apart at first
/// and doubling.
pub fn retry_policy(maximum_attempts maximum_attempts: Int) -> RetryPolicy {
  RetryPolicy(
    initial_interval: duration.seconds(1),
    backoff_coefficient: 2.0,
    maximum_interval: None,
    maximum_attempts:,
    non_retryable_error_types: [],
  )
}

@internal
pub fn to_bridge_retry(retry: RetryPolicy) -> bridge.RetryPolicy {
  bridge.RetryPolicy(
    initial_interval_ms: time.to_ms(retry.initial_interval),
    backoff_coefficient: retry.backoff_coefficient,
    maximum_interval_ms: time.option_to_ms(retry.maximum_interval),
    maximum_attempts: retry.maximum_attempts,
    non_retryable_error_types: retry.non_retryable_error_types,
  )
}

@internal
pub fn from_bridge_retry(retry: bridge.RetryPolicy) -> RetryPolicy {
  RetryPolicy(
    initial_interval: time.from_ms(retry.initial_interval_ms),
    backoff_coefficient: retry.backoff_coefficient,
    maximum_interval: time.option_from_ms(retry.maximum_interval_ms),
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
        state.outbound.activity(middleware.ActivityCall(
          activity_type: definition.name,
          task_queue: option.unwrap(options.task_queue, state.task_queue),
          arguments: codec.encode_arguments(definition.input, input),
          headers: state.headers,
        ))
      runtime.push(bridge.ScheduleActivity(
        seq:,
        activity_id: option.unwrap(options.activity_id, int.to_string(seq)),
        activity_type: call.activity_type,
        task_queue: call.task_queue,
        arguments: call.arguments,
        schedule_to_close_ms: time.option_to_ms(options.schedule_to_close),
        start_to_close_ms: Some(time.to_ms(options.start_to_close)),
        heartbeat_timeout_ms: time.option_to_ms(options.heartbeat_timeout),
        retry: option.map(options.retry, to_bridge_retry),
        schedule_to_start_ms: time.option_to_ms(options.schedule_to_start),
        cancellation_type: case options.cancellation {
          TryCancel -> 0
          WaitCancellationCompleted -> 1
          AbandonActivity -> 2
        },
        headers: call.headers,
      ))
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

@internal
pub fn resolution(
  lookup: fn() -> Result(bridge.Resolution, Nil),
  output: Codec(a),
) -> Future(Result(a, Failure)) {
  Future(fn() {
    case lookup() {
      Error(Nil) -> None
      Ok(bridge.Completed(payload)) ->
        Some(
          codec.decode_arguments(output, option.values([payload]))
          |> result.map_error(fn(reason) {
            failure.new("could not decode result: " <> reason)
            |> failure.with_type("InvalidResult")
            |> failure.non_retryable
          }),
        )
      Ok(bridge.Failed(failure)) | Ok(bridge.Cancelled(failure)) ->
        Some(Error(failure))
      // The runtime turns a backoff into a timer and a retry; it never gets this far.
      Ok(bridge.Backoff(..)) -> None
    }
  })
}

// ---- signals and queries ----

/// A future of the next signal of this kind. Signals that arrived before anyone asked are
/// buffered and delivered in order. A signal whose payload does not decode is dropped.
pub fn signal(_ctx: Context, definition: SignalDefinition(a)) -> Future(a) {
  once(fn() { next_signal(definition) })
}

fn next_signal(definition: SignalDefinition(a)) -> Option(a) {
  case runtime.take_signal(definition.name) {
    None -> None
    Some(payloads) ->
      case codec.decode_arguments(definition.codec, payloads) {
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
    codec.decode_arguments(definition.argument, arguments)
    |> result.map(fn(argument) {
      codec.encode(definition.result, handler(argument))
    })
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
  runtime.abandon_updates()
  runtime.push(bridge.ContinueAsNew(
    definition.name,
    runtime.get().task_queue,
    codec.encode_arguments(definition.input, input),
  ))
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

// ---- other workflows ----

@internal
pub fn external_outcome(
  lookup: fn() -> Result(Option(Failure), Nil),
) -> Future(Result(Nil, Failure)) {
  Future(fn() {
    case lookup() {
      Error(Nil) -> None
      Ok(None) -> Some(Ok(Nil))
      Ok(Some(failure)) -> Some(Error(failure))
    }
  })
}

@internal
pub fn send_signal(
  ctx: Context,
  target: bridge.SignalTarget,
  definition: SignalDefinition(a),
  value: a,
) -> Future(Result(Nil, Failure)) {
  let seq = runtime.next_signal()
  case runtime.scope_cancelled(ctx.scope) {
    Some(_) -> ready(Error(failure.cancelled()))
    None -> {
      runtime.push(bridge.SignalExternal(
        seq,
        target,
        definition.name,
        codec.encode_arguments(definition.codec, value),
      ))
      runtime.register(ctx.scope, runtime.SignalOp(seq))
      external_outcome(fn() { dict.get(runtime.get().external_signals, seq) })
    }
  }
}

/// Signal another workflow by id; its current run receives it. The future resolves once
/// the server has delivered the signal, or fails if there is no such workflow.
pub fn signal_external(
  ctx: Context,
  workflow_id: String,
  definition: SignalDefinition(a),
  value: a,
) -> Future(Result(Nil, Failure)) {
  send_signal(ctx, bridge.ExternalWorkflow(workflow_id, ""), definition, value)
}

/// Ask another workflow to cancel. Resolves once the server has passed the request on, not
/// when the workflow has stopped.
pub fn cancel_external(
  _ctx: Context,
  workflow_id: String,
) -> Future(Result(Nil, Failure)) {
  let seq = runtime.next_cancel()
  runtime.push(bridge.CancelExternal(
    seq,
    workflow_id,
    "",
    "cancelled by workflow",
  ))
  external_outcome(fn() { dict.get(runtime.get().external_cancels, seq) })
}

// ---- changing what the server knows about this run ----

/// Set or replace a memo entry on the running workflow.
pub fn upsert_memo(
  _ctx: Context,
  key: String,
  value_codec: Codec(a),
  value: a,
) -> Nil {
  let payload = codec.encode(value_codec, value)
  runtime.update(fn(state) {
    State(..state, memo: dict.insert(state.memo, key, payload))
  })
  runtime.push(bridge.UpsertMemo(dict.from_list([#(key, payload)])))
}

/// Set or replace search attributes on the running workflow.
pub fn upsert_search_attributes(
  _ctx: Context,
  attributes: Dict(String, search_attributes.Value),
) -> Nil {
  runtime.push(
    bridge.UpsertSearchAttributes(search_attributes.encode(attributes)),
  )
}

// ---- worker versioning ----

/// How a workflow moves between versions of a worker deployment.
pub type VersioningBehavior {
  /// The run stays on the deployment version it started on until it ends. Incompatible
  /// changes to the workflow can then ship without `patched`.
  Pinned
  /// The run moves to the deployment's current version at its next task, so changes must
  /// stay replay-compatible, with `patched` where they are not.
  AutoUpgrade
}

/// Declare a workflow's versioning behaviour, for workers started with
/// `worker.with_deployment`. Overrides the worker's default.
pub fn with_versioning(
  handler: Handler,
  behavior: VersioningBehavior,
) -> Handler {
  Handler(handler.name, fn(arguments) {
    runtime.update(fn(state) {
      State(..state, versioning_behavior: case behavior {
        Pinned -> 1
        AutoUpgrade -> 2
      })
    })
    handler.run(arguments)
  })
}
