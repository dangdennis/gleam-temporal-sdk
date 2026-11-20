//// Local activities: activities run inside the workflow's own worker, without a round trip
//// through the server's task queue.
////
//// Much faster for short work, a few seconds at most. In exchange a local activity cannot
//// heartbeat, must be registered on the worker that runs the workflow, and is recorded
//// only when it finishes: if its worker dies it runs again from the start on another, so
//// it must be safe to repeat.
////
//// ```gleam
//// let options =
////   local_activity.options(start_to_close: duration.seconds(2))
////   |> local_activity.with_retry(retry.policy(maximum_attempts: 3))
//// use rate <- result.try(local_activity.execute(ctx, lookup_rate(), currency, options))
//// ```

import gleam/dict
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}
import temporal/activity
import temporal/codec
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/runtime/determinism
import temporal/internal/runtime/local_activities
import temporal/internal/runtime/run_state
import temporal/internal/runtime/scopes
import temporal/internal/time
import temporal/middleware
import temporal/retry
import temporal/workflow.{type ActivityCancellation, type Context, type Future}

/// How a local activity is run. Build with `options` and the `with_` functions.
pub type Options {
  Options(
    /// Longest a single attempt may run.
    start_to_close: Duration,
    /// Longest the activity may take across all retries.
    schedule_to_close: Option(Duration),
    /// `None` retries without limit, one second apart at first and doubling.
    retry: Option(retry.Policy),
    /// A retry that would wait longer than this is not waited for inside the workflow
    /// task: the workflow starts a durable timer and tries again when it fires. `None`
    /// uses core's default of one minute.
    local_retry_threshold: Option(Duration),
    /// What cancelling the activity's scope does.
    cancellation: ActivityCancellation,
  )
}

/// Options with only the per-attempt timeout set, and default retries.
pub fn options(start_to_close start_to_close: Duration) -> Options {
  Options(
    start_to_close:,
    schedule_to_close: None,
    retry: None,
    local_retry_threshold: None,
    cancellation: workflow.TryCancel,
  )
}

/// The longest the activity may take across all retries.
pub fn with_schedule_to_close(options: Options, timeout: Duration) -> Options {
  Options(..options, schedule_to_close: Some(timeout))
}

/// Retry by this policy instead of without limit.
pub fn with_retry(options: Options, policy: retry.Policy) -> Options {
  Options(..options, retry: Some(policy))
}

/// Retries that would wait longer than this are handed to a durable timer instead of
/// being waited for inside the workflow task. Unset, it is one minute.
pub fn with_local_retry_threshold(
  options: Options,
  threshold: Duration,
) -> Options {
  Options(..options, local_retry_threshold: Some(threshold))
}

/// What cancelling the activity's scope does.
pub fn with_cancellation(
  options: Options,
  cancellation: ActivityCancellation,
) -> Options {
  Options(..options, cancellation:)
}

/// Schedule a local activity. Returns at once with a future of its result.
pub fn start(
  ctx: Context,
  definition: activity.Definition(input, output),
  input: input,
  options: Options,
) -> Future(Result(output, Failure)) {
  case scopes.cancelled(workflow.scope_of(ctx)) {
    Some(_) -> {
      // A sequence number is used even when nothing is scheduled, as the other SDKs do.
      let _ = determinism.next_activity()
      workflow.ready(Error(failure.cancelled()))
    }
    None -> {
      let state = run_state.get()
      let call =
        state.outbound.activity(middleware.ActivityCall(
          activity_type: definition.name,
          task_queue: state.task_queue,
          arguments: codec.encode_arguments(definition.input, input),
          headers: state.headers,
        ))
      let seq =
        local_activities.start(fn(seq) {
          bridge.ScheduleLocalActivity(
            seq:,
            activity_id: int.to_string(seq),
            activity_type: call.activity_type,
            attempt: 1,
            original_schedule_time_ms: None,
            arguments: call.arguments,
            schedule_to_close_ms: time.option_to_ms(options.schedule_to_close),
            schedule_to_start_ms: None,
            start_to_close_ms: Some(time.to_ms(options.start_to_close)),
            retry: option.map(options.retry, retry.to_bridge),
            local_retry_threshold_ms: time.option_to_ms(
              options.local_retry_threshold,
            ),
            cancellation_type: case options.cancellation {
              workflow.TryCancel -> 0
              workflow.WaitCancellationCompleted -> 1
              workflow.AbandonActivity -> 2
            },
            headers: call.headers,
          )
        })
      scopes.register(workflow.scope_of(ctx), run_state.ActivityOp(seq))
      workflow.resolution(
        fn() { dict.get(run_state.get().activities, seq) },
        definition.output,
        "local activity \"" <> definition.name <> "\"",
      )
    }
  }
}

/// Run a local activity and wait for its result.
pub fn execute(
  ctx: Context,
  definition: activity.Definition(input, output),
  input: input,
  options: Options,
) -> Result(output, Failure) {
  workflow.await(start(ctx, definition, input, options))
}
