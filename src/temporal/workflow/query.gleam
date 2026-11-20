//// Queries: read a running workflow's state from outside without changing it. The
//// workflow says how to answer each kind of query; a client asks with `client.query`.
////
//// ```gleam
//// fn counter(ctx: workflow.Context, count: Int) -> Result(Int, Failure) {
////   query.set_handler(ctx, current_count(), fn(_) { count })
////   let step = signal.receive(ctx, add())
////   counter(ctx, count + step)
//// }
//// ```

import gleam/dict
import gleam/result
import temporal/codec
import temporal/internal/runtime/run_state.{State}
import temporal/workflow.{type Context, type QueryDefinition}

/// Answer queries of this kind with `handler`. Workflow state lives in local variables, so
/// call this again with a new closure whenever the answer should change. The handler must
/// only read: it must not start, wait for or change anything in the workflow.
pub fn set_handler(
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
  run_state.update(fn(state) {
    State(
      ..state,
      query_handlers: dict.insert(state.query_handlers, definition.name, erased),
    )
  })
}
