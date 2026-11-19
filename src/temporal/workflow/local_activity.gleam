//// Local activities: activities run inside the workflow's own worker, without a round trip
//// through the server's task queue.
////
//// Much faster for short work, a few seconds at most. In exchange a local activity cannot
//// heartbeat, must be registered on the worker that runs the workflow, and is recorded
//// only when it finishes: if its worker dies it runs again from the start on another, so
//// it must be safe to repeat.

import gleam/dict
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/time/duration.{type Duration}
import temporal/activity
import temporal/codec
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/runtime
import temporal/internal/time
import temporal/middleware
import temporal/workflow.{
  type ActivityCancellation, type Context, type Future, type RetryPolicy,
}

/// How a local activity is run. Build with `options` and change what differs.
pub type Options {
  Options(
    /// Longest a single attempt may run.
    start_to_close: Duration,
    /// Longest the activity may take across all retries.
    schedule_to_close: Option(Duration),
    /// `None` retries without limit, one second apart at first and doubling.
    retry: Option(RetryPolicy),
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

/// Schedule a local activity. Returns at once with a future of its result.
pub fn start(
  ctx: Context,
  definition: activity.Definition(input, output),
  input: input,
  options: Options,
) -> Future(Result(output, Failure)) {
  case runtime.scope_cancelled(workflow.scope_of(ctx)) {
    Some(_) -> {
      // A sequence number is used even when nothing is scheduled, as the other SDKs do.
      let _ = runtime.next_activity()
      workflow.ready(Error(failure.cancelled()))
    }
    None -> {
      let state = runtime.get()
      let call =
        state.outbound.activity(middleware.ActivityCall(
          activity_type: definition.name,
          task_queue: state.task_queue,
          arguments: codec.encode_arguments(definition.input, input),
        ))
      let seq =
        runtime.start_local(fn(seq) {
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
            retry: option.map(options.retry, workflow.to_bridge_retry),
            local_retry_threshold_ms: time.option_to_ms(
              options.local_retry_threshold,
            ),
            cancellation_type: case options.cancellation {
              workflow.TryCancel -> 0
              workflow.WaitCancellationCompleted -> 1
              workflow.AbandonActivity -> 2
            },
          )
        })
      runtime.register(workflow.scope_of(ctx), runtime.ActivityOp(seq))
      workflow.resolution(
        fn() { dict.get(runtime.get().activities, seq) },
        definition.output,
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
