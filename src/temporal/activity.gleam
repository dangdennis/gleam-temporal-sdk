//// Define and implement activities: the functions where a workflow's side effects happen.
//// Activity code is ordinary Gleam and may do anything; the server retries it on failure.

import gleam/dict.{type Dict}
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import temporal/codec.{type Codec, type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge

/// An activity's name and the codecs for its argument and result. Shared by the code that
/// implements the activity and the workflows that call it.
pub type Definition(input, output) {
  Definition(name: String, input: Codec(input), output: Codec(output))
}

/// What the worker knows about the task an activity is running for.
pub type Info {
  Info(
    activity_id: String,
    activity_type: String,
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    /// The queue the worker running the activity polls.
    task_queue: String,
    /// Starts at 1 and grows with each retry.
    attempt: Int,
    /// What the previous attempt last passed to `heartbeat_with`.
    heartbeat_details: List(Payload),
    /// Longest this attempt may run.
    start_to_close: Option(Duration),
    /// The longest gap between heartbeats the workflow allows.
    heartbeat_timeout: Option(Duration),
    /// When the activity was first scheduled.
    scheduled_time: Timestamp,
    /// Whether the workflow started this as a local activity.
    is_local: Bool,
    /// Context, such as a trace id, that the workflow passed beside the arguments.
    headers: Dict(String, Payload),
  )
}

/// Why the server or the worker wants an activity to stop.
pub type CancelReason {
  /// The workflow, or its cancellation scope, cancelled the activity.
  CancelRequested
  /// The activity no longer exists on the server, usually because its workflow closed.
  NotFound
  /// A timeout passed. Whatever the activity returns now is ignored.
  TimedOut
  /// The worker is stopping.
  WorkerShutdown
  /// An operator paused the activity.
  Paused
  /// An operator reset the activity.
  Reset
}

/// Passed to every activity: what it is running for, and its line to the server for
/// heartbeats and cancellation. Tests build one with `testing.activity_env`.
pub opaque type Context {
  Context(
    info: Info,
    task_token: BitArray,
    heartbeat: fn(List(Payload)) -> Nil,
    cancel_reason: fn() -> Option(CancelReason),
  )
}

/// What an activity produced, before its result is decoded: a payload, nothing, or a failure.
pub type Outcome =
  Result(Option(Payload), Failure)

/// An activity implementation, ready to register on a worker.
pub type Handler {
  Handler(name: String, run: fn(Context, List(Payload)) -> Outcome)
}

/// Code that runs around every activity on a worker: call `next` to run the activity and
/// return its outcome, changed or not. Unlike workflow middleware it may do anything.
pub type Middleware =
  fn(Context, fn() -> Outcome) -> Outcome

/// Pair a definition with its implementation.
pub fn handler(
  definition: Definition(input, output),
  run: fn(Context, input) -> Result(output, Failure),
) -> Handler {
  Handler(definition.name, fn(ctx, arguments) {
    case codec.decode_arguments(definition.input, arguments) {
      Ok(input) ->
        run(ctx, input)
        |> result.map(fn(output) {
          Some(codec.encode(definition.output, output))
        })
      Error(reason) ->
        Error(
          failure.new("could not decode activity input: " <> reason)
          |> failure.with_type("InvalidArgument")
          |> failure.non_retryable,
        )
    }
  })
}

/// Run `middleware` around the handler. The first middleware applied is the innermost.
pub fn wrap(handler: Handler, middleware: Middleware) -> Handler {
  Handler(handler.name, fn(ctx, arguments) {
    middleware(ctx, fn() { handler.run(ctx, arguments) })
  })
}

@internal
pub fn new_context(
  info info: Info,
  task_token task_token: BitArray,
  heartbeat heartbeat: fn(List(Payload)) -> Nil,
  cancel_reason cancel_reason: fn() -> Option(CancelReason),
) -> Context {
  Context(info:, task_token:, heartbeat:, cancel_reason:)
}

/// The task the activity is running for.
pub fn info(ctx: Context) -> Info {
  ctx.info
}

/// Tell the server the activity is still alive. Long activities with a heartbeat timeout
/// must call this more often than the timeout. Heartbeating is also how an activity learns
/// that it was cancelled: see `is_cancelled`.
pub fn heartbeat(ctx: Context) -> Nil {
  ctx.heartbeat([])
}

/// Heartbeat with progress that the next attempt can read with `heartbeat_details`.
pub fn heartbeat_with(ctx: Context, value_codec: Codec(a), details: a) -> Nil {
  ctx.heartbeat([codec.encode(value_codec, details)])
}

/// The progress the previous attempt last recorded with `heartbeat_with`, if any.
pub fn heartbeat_details(ctx: Context, value_codec: Codec(a)) -> Option(a) {
  case ctx.info.heartbeat_details {
    [payload, ..] -> option.from_result(codec.decode(value_codec, payload))
    [] -> None
  }
}

/// Whether the activity has been asked to stop. The server only tells activities that
/// heartbeat, so check this after heartbeating, from the process the activity runs in. To
/// accept the cancellation, return `Error(failure.cancelled())`.
pub fn is_cancelled(ctx: Context) -> Bool {
  option.is_some(ctx.cancel_reason())
}

/// Why the activity was asked to stop, or `None` if it was not.
pub fn cancel_reason(ctx: Context) -> Option(CancelReason) {
  ctx.cancel_reason()
}

/// The token that names this activity task to the server. Hand it to whatever will finish
/// the activity later with `client.complete_activity`.
pub fn task_token(ctx: Context) -> BitArray {
  ctx.task_token
}

/// Say that the activity will be finished later, from somewhere else: by a person, a
/// webhook, another service. Return the failure this gives; it is not recorded.
///
/// ```gleam
/// fn await_approval(ctx: activity.Context, request: Request) {
///   send_for_approval(request, activity.task_token(ctx))
///   Error(activity.complete_later(ctx))
/// }
/// ```
///
/// The activity stays open, subject to its timeouts, until `client.complete_activity` or
/// `client.fail_activity` is called with its token.
pub fn complete_later(_ctx: Context) -> Failure {
  bridge.defer_activity()
  failure.new("the activity will be completed later")
}
