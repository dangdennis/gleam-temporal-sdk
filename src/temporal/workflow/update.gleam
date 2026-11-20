//// Updates: requests a client sends to a running workflow and waits on. An update may
//// change the workflow, like a signal, and returns a value, like a query.
////
//// The workflow takes updates when it is ready for them, the way it takes signals, and
//// answers each one. It is the shape of a BEAM `call`: one process, one request at a time.
////
//// ```gleam
//// fn cart(ctx: workflow.Context, items: List(Item)) -> Result(Int, Failure) {
////   let request = update.receive(ctx, add_item())
////   let items = [update.argument(request), ..items]
////   update.complete(ctx, request, Ok(list.length(items)))
////   cart(ctx, items)
//// }
//// ```

import gleam/option.{type Option, None, Some}
import gleam/result
import temporal/codec.{type Codec, type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/internal/runtime/updates
import temporal/workflow.{type Context, type Future, type UpdateDefinition}

/// An update a client sent, taken by the workflow and awaiting its answer.
pub opaque type Update(argument, result) {
  Update(
    id: String,
    protocol_instance_id: String,
    argument: argument,
    result: Codec(result),
  )
}

fn decodes(
  definition: UpdateDefinition(argument, result),
) -> fn(List(Payload)) -> Result(Nil, String) {
  fn(input) {
    codec.decode_arguments(definition.argument, input)
    |> result.replace(Nil)
    |> result.map_error(fn(reason) { invalid_argument(definition, reason) })
  }
}

/// Decide which updates of this kind to accept. An update the validator refuses is
/// rejected without a trace in history; one it passes is accepted and queued for the
/// workflow to take. The validator must not change anything or use workflow functions.
/// Without a validator, every update whose argument decodes is accepted.
///
/// Calling this, or asking for an update with `next` or `receive`, is what tells the
/// worker that the workflow handles this kind of update. Updates of a kind the workflow
/// has not asked about by the time it next waits are rejected.
pub fn set_validator(
  _ctx: Context,
  definition: UpdateDefinition(argument, result),
  validator: fn(argument) -> Result(Nil, String),
) -> Nil {
  updates.register(
    definition.name,
    fn(input) {
      case codec.decode_arguments(definition.argument, input) {
        Ok(argument) -> validator(argument)
        Error(reason) -> Error(invalid_argument(definition, reason))
      }
    },
    True,
  )
}

/// A future of the next accepted update of this kind, to race against other things the
/// workflow is waiting for.
pub fn next(
  _ctx: Context,
  definition: UpdateDefinition(argument, result),
) -> Future(Update(argument, result)) {
  updates.register(definition.name, decodes(definition), False)
  workflow.once(fn() { take(definition) })
}

fn take(
  definition: UpdateDefinition(argument, result),
) -> Option(Update(argument, result)) {
  case updates.take(definition.name) {
    None -> None
    Some(pending) ->
      case codec.decode_arguments(definition.argument, pending.input) {
        Ok(argument) ->
          Some(Update(
            id: pending.id,
            protocol_instance_id: pending.protocol_instance_id,
            argument:,
            result: definition.result,
          ))
        // Only on replay of an update accepted by different code: fail it and move on.
        Error(reason) -> {
          updates.answer(
            pending.protocol_instance_id,
            bridge.UpdateRejected(
              pending.protocol_instance_id,
              failure.new(invalid_argument(definition, reason)),
            ),
          )
          take(definition)
        }
      }
  }
}

/// Wait for the next accepted update of this kind.
pub fn receive(
  ctx: Context,
  definition: UpdateDefinition(argument, result),
) -> Update(argument, result) {
  workflow.await(next(ctx, definition))
}

/// The next accepted update of this kind if one is waiting. Does not wait.
pub fn try_receive(
  _ctx: Context,
  definition: UpdateDefinition(argument, result),
) -> Option(Update(argument, result)) {
  updates.register(definition.name, decodes(definition), False)
  take(definition)
}

/// The value the client sent with the update.
pub fn argument(update: Update(argument, result)) -> argument {
  update.argument
}

/// The id the client gave the update, or the one the server generated.
pub fn id(update: Update(argument, result)) -> String {
  update.id
}

/// Answer an update. The client's call returns the value, or fails with the failure. Only
/// the first answer counts. An update still unanswered when the workflow ends fails with a
/// `WorkflowFinished` failure.
pub fn complete(
  _ctx: Context,
  update: Update(argument, result),
  outcome: Result(result, Failure),
) -> Nil {
  updates.answer(update.protocol_instance_id, case outcome {
    Ok(value) ->
      bridge.UpdateCompleted(
        update.protocol_instance_id,
        codec.encode(update.result, value),
      )
    Error(failure) ->
      bridge.UpdateRejected(update.protocol_instance_id, failure)
  })
}

fn invalid_argument(
  definition: UpdateDefinition(argument, result),
  reason: String,
) -> String {
  "update \""
  <> definition.name
  <> "\": could not decode the argument: "
  <> reason
}
