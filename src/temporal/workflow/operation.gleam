//// Calling Nexus operations from a workflow: entry points another team exposes, by service
//// and operation name, through an endpoint registered on the server. The endpoint decides
//// which namespace and task queue handle the call. See `temporal/nexus` for serving them.
////
//// ```gleam
//// let options =
////   operation.options()
////   |> operation.with_schedule_to_close(duration.minutes(5))
//// operation.execute(ctx, "orders-endpoint", place_order(), order, options)
//// ```

import gleam/dict
import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/runtime/determinism
import temporal/internal/runtime/run_state
import temporal/internal/runtime/scopes
import temporal/internal/time
import temporal/workflow.{type Context, type Future, type NexusOperation}

/// What cancelling an operation's scope does.
pub type Cancellation {
  /// Request cancellation and wait until the operation has ended. The default.
  WaitUntilCancelled
  /// Request cancellation and wait only until the request has been delivered.
  WaitUntilRequested
  /// Request cancellation and fail the workflow's wait at once.
  TryCancel
  /// Fail the workflow's wait at once without telling the operation.
  DoNotCancel
}

/// How an operation is run. Build with `options` and the `with_` functions.
pub type Options {
  Options(
    /// Longest the operation may take, retries included. `None` leaves it to the server.
    schedule_to_close: Option(Duration),
    /// What cancelling the operation's scope does.
    cancellation: Cancellation,
  )
}

/// No deadline of its own; cancellation waits for the operation to end.
pub fn options() -> Options {
  Options(schedule_to_close: None, cancellation: WaitUntilCancelled)
}

/// The longest the operation may take, retries included.
pub fn with_schedule_to_close(options: Options, timeout: Duration) -> Options {
  Options(..options, schedule_to_close: Some(timeout))
}

/// What cancelling the operation's scope does.
pub fn with_cancellation(
  options: Options,
  cancellation: Cancellation,
) -> Options {
  Options(..options, cancellation:)
}

/// An operation the workflow has asked for.
pub opaque type Operation(output) {
  Operation(
    started: Future(Result(Option(String), Failure)),
    result: Future(Result(output, Failure)),
  )
}

/// Ask for an operation. Returns at once; the request is sent when the workflow next waits.
pub fn start(
  ctx: Context,
  endpoint: String,
  operation: NexusOperation(input, output),
  input: input,
  options: Options,
) -> Operation(output) {
  let seq = determinism.next_nexus()
  case scopes.cancelled(workflow.scope_of(ctx)) {
    Some(_) ->
      Operation(
        started: workflow.ready(Error(failure.cancelled())),
        result: workflow.ready(Error(failure.cancelled())),
      )
    None -> {
      run_state.push(
        bridge.ScheduleNexusOperation(
          seq:,
          endpoint:,
          service: operation.service,
          operation: operation.name,
          input: codec.encode(operation.input, input),
          schedule_to_close_ms: time.option_to_ms(options.schedule_to_close),
          cancellation_type: case options.cancellation {
            WaitUntilCancelled -> 0
            DoNotCancel -> 1
            TryCancel -> 2
            WaitUntilRequested -> 3
          },
        ),
      )
      scopes.register(workflow.scope_of(ctx), run_state.NexusOp(seq))
      let started =
        workflow.future(fn() {
          case dict.get(run_state.get().nexus_starts, seq) {
            Ok(bridge.NexusStartedAsync(token)) -> Some(Ok(Some(token)))
            Ok(bridge.NexusStartedSync) -> Some(Ok(None))
            Ok(bridge.NexusStartFailed(failure)) -> Some(Error(failure))
            Error(Nil) -> None
          }
        })
      let finished =
        workflow.resolution(
          fn() { dict.get(run_state.get().nexus_results, seq) },
          operation.output,
          "Nexus operation \""
            <> operation.service
            <> "/"
            <> operation.name
            <> "\"",
        )
      Operation(
        started:,
        // An operation that never started has no result of its own.
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

/// Resolves once the service has taken the operation on, with the token that names it if
/// it runs on, or `None` if it finished at once.
pub fn started(
  operation: Operation(output),
) -> Future(Result(Option(String), Failure)) {
  operation.started
}

/// The operation's result, once it finishes.
pub fn result(operation: Operation(output)) -> Future(Result(output, Failure)) {
  operation.result
}

/// Start an operation and wait for its result.
pub fn execute(
  ctx: Context,
  endpoint: String,
  operation: NexusOperation(input, output),
  input: input,
  options: Options,
) -> Result(output, Failure) {
  workflow.await(start(ctx, endpoint, operation, input, options).result)
}
