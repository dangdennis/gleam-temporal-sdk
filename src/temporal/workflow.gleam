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

import gleam/bit_array
import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set
import gleam/string
import temporal/activity
import temporal/codec.{type Codec}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/runtime.{State}

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

/// Proof that the caller is workflow code. Only the worker creates one.
pub opaque type Context {
  Context
}

/// A workflow implementation, ready to register on a worker.
pub type Handler {
  Handler(name: String, run: runtime.WorkflowFn)
}

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

pub type ActivityOptions {
  ActivityOptions(
    /// Longest a single attempt may run.
    start_to_close_ms: Int,
    /// Longest the activity may take across all retries.
    schedule_to_close_ms: Option(Int),
    heartbeat_timeout_ms: Option(Int),
    /// `None` uses the server's default policy, which retries without limit.
    retry: Option(RetryPolicy),
    /// `None` runs the activity on the workflow's own task queue.
    task_queue: Option(String),
  )
}

pub type ChildOptions {
  ChildOptions(
    workflow_id: String,
    /// `None` runs the child on the parent's task queue.
    task_queue: Option(String),
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
        run(Context, input)
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

pub fn info(_ctx: Context) -> Info {
  let state = runtime.get()
  Info(
    workflow_id: state.workflow_id,
    run_id: state.run_id,
    workflow_type: state.workflow_type,
    task_queue: state.task_queue,
    attempt: state.attempt,
  )
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
/// suppressing side effects that are safe to skip, such as logging.
pub fn is_replaying(_ctx: Context) -> Bool {
  runtime.get().is_replaying
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

// ---- timers ----

/// A durable timer. It keeps counting while no worker is running.
pub fn timer(_ctx: Context, ms: Int) -> Future(Nil) {
  let seq = runtime.next_timer()
  runtime.push(bridge.StartTimer(seq, int.max(ms, 1)))
  Future(fn() {
    case set.contains(runtime.get().fired_timers, seq) {
      True -> Some(Nil)
      False -> None
    }
  })
}

pub fn sleep(ctx: Context, ms: Int) -> Nil {
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
    heartbeat_timeout_ms: None,
    retry: None,
    task_queue: None,
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

/// Schedule an activity. The failure, if any, is an `Activity` failure whose `cause` is the
/// error the activity returned; see `failure.root_cause`.
pub fn start_activity(
  _ctx: Context,
  definition: activity.Definition(input, output),
  input: input,
  options: ActivityOptions,
) -> Future(Result(output, Failure)) {
  let seq = runtime.next_activity()
  runtime.push(bridge.ScheduleActivity(
    seq:,
    activity_id: int.to_string(seq),
    activity_type: definition.name,
    task_queue: option.unwrap(options.task_queue, runtime.get().task_queue),
    arguments: [definition.input.encode(input)],
    schedule_to_close_ms: options.schedule_to_close_ms,
    start_to_close_ms: Some(options.start_to_close_ms),
    heartbeat_timeout_ms: options.heartbeat_timeout_ms,
    retry: option.map(options.retry, fn(retry) {
      bridge.RetryPolicy(
        initial_interval_ms: retry.initial_interval_ms,
        backoff_coefficient: retry.backoff_coefficient,
        maximum_interval_ms: retry.maximum_interval_ms,
        maximum_attempts: retry.maximum_attempts,
        non_retryable_error_types: retry.non_retryable_error_types,
      )
    }),
  ))
  resolution(
    fn() { dict.get(runtime.get().activities, seq) },
    definition.output,
  )
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

/// Start a child workflow. The future resolves when the child finishes.
pub fn start_child(
  _ctx: Context,
  definition: Definition(input, output),
  input: input,
  options: ChildOptions,
) -> Future(Result(output, Failure)) {
  let seq = runtime.next_child()
  runtime.push(
    bridge.StartChildWorkflow(
      seq:,
      workflow_id: options.workflow_id,
      workflow_type: definition.name,
      task_queue: option.unwrap(options.task_queue, runtime.get().task_queue),
      input: [definition.input.encode(input)],
    ),
  )
  let finished =
    resolution(
      fn() { dict.get(runtime.get().children, seq) },
      definition.output,
    )
  Future(fn() {
    case dict.get(runtime.get().child_starts, seq) {
      Ok(bridge.ChildStartFailed(workflow_id, _)) ->
        Some(Error(
          failure.new(
            "child workflow \"" <> workflow_id <> "\" could not be started",
          )
          |> failure.with_type("ChildWorkflowStartFailed"),
        ))
      Ok(bridge.ChildStartCancelled(failure)) -> Some(Error(failure))
      _ -> finished.poll()
    }
  })
}

pub fn execute_child(
  ctx: Context,
  definition: Definition(input, output),
  input: input,
  options: ChildOptions,
) -> Result(output, Failure) {
  await(start_child(ctx, definition, input, options))
}

// ---- signals, queries, cancellation ----

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

/// A future that resolves, with the reason given, when cancellation of this workflow is
/// requested. Race it against the workflow's work and return `failure.cancelled()` to accept.
pub fn cancellation(_ctx: Context) -> Future(String) {
  Future(fn() { runtime.get().cancel_reason })
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
