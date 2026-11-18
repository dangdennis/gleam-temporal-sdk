//// Define and implement activities: the functions where a workflow's side effects happen.
//// Activity code is ordinary Gleam and may do anything; the server retries it on failure.

import gleam/option.{Some}
import gleam/result
import temporal/codec.{type Codec, type Payload}
import temporal/failure.{type Failure}

/// An activity's name and the codecs for its argument and result. Shared by the code that
/// implements the activity and the workflows that call it.
pub type Definition(input, output) {
  Definition(name: String, input: Codec(input), output: Codec(output))
}

pub type Info {
  Info(
    activity_id: String,
    activity_type: String,
    workflow_id: String,
    run_id: String,
    /// Starts at 1 and grows with each retry.
    attempt: Int,
  )
}

pub type Context {
  Context(info: Info, heartbeat: fn() -> Nil)
}

/// An activity implementation, ready to register on a worker.
pub type Handler {
  Handler(
    name: String,
    run: fn(Context, List(Payload)) -> Result(option.Option(Payload), Failure),
  )
}

/// Pair a definition with its implementation.
pub fn handler(
  definition: Definition(input, output),
  run: fn(Context, input) -> Result(output, Failure),
) -> Handler {
  Handler(definition.name, fn(ctx, arguments) {
    case codec.decode_argument(definition.input, arguments) {
      Ok(input) ->
        run(ctx, input)
        |> result.map(fn(output) { Some(definition.output.encode(output)) })
      Error(reason) ->
        Error(
          failure.new("could not decode activity input: " <> reason)
          |> failure.with_type("InvalidArgument")
          |> failure.non_retryable,
        )
    }
  })
}

/// Tell the server the activity is still alive. Long activities with a heartbeat timeout
/// must call this more often than the timeout.
pub fn heartbeat(ctx: Context) -> Nil {
  ctx.heartbeat()
}
