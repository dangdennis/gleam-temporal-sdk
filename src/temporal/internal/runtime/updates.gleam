//// Workflow updates: the queue of updates clients sent, accepting or rejecting them, and
//// answering the ones the workflow took.

import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/set
import gleam/string
import temporal/codec.{type Payload}
import temporal/failure
import temporal/internal/bridge.{type Command}
import temporal/internal/runtime/run_state.{
  type PendingUpdate, PendingUpdate, State, get, push, put,
}

/// Declare that the workflow handles updates with this name. `check` decides whether an
/// update is accepted; a later registration for the same name replaces it only when
/// `replace` is set.
pub fn register(
  name: String,
  check: fn(List(Payload)) -> Result(Nil, String),
  replace: Bool,
) -> Nil {
  let state = get()
  case replace || !dict.has_key(state.update_checks, name) {
    True ->
      put(
        State(
          ..state,
          update_checks: dict.insert(state.update_checks, name, check),
        ),
      )
    False -> Nil
  }
}

/// Accept or reject an update that has not been settled yet. An accepted update stays
/// queued for the workflow to take; a rejected one is dropped.
pub fn settle(update: PendingUpdate) -> Nil {
  let state = get()
  let verdict = case
    update.run_validator,
    dict.get(state.update_checks, update.name)
  {
    // History already records the acceptance; validators do not run on replay.
    False, _ -> Ok(Nil)
    True, Ok(check) ->
      case bridge.rescue(fn() { check(update.input) }) {
        Ok(verdict) -> result.map_error(verdict, failure.new)
        Error(crash) -> Error(run_state.crashed("update validator", crash))
      }
    True, Error(Nil) ->
      Error(failure.new(
        "no handler registered for update \"" <> update.name <> "\"",
      ))
  }
  case verdict {
    Ok(Nil) -> {
      put(
        State(
          ..state,
          updates: list.map(state.updates, fn(other) {
            case other.protocol_instance_id == update.protocol_instance_id {
              True -> PendingUpdate(..other, accepted: True)
              False -> other
            }
          }),
        ),
      )
      push(bridge.UpdateAccepted(update.protocol_instance_id))
    }
    Error(reason) -> {
      put(
        State(
          ..state,
          updates: list.filter(state.updates, fn(other) {
            other.protocol_instance_id != update.protocol_instance_id
          }),
        ),
      )
      push(bridge.UpdateRejected(
        update.protocol_instance_id,
        reason |> failure.with_type("UpdateRejected"),
      ))
    }
  }
}

/// Answer an update the workflow took. A second answer to the same update is ignored.
pub fn answer(protocol_instance_id: String, answer: Command) -> Nil {
  let state = get()
  case set.contains(state.open_updates, protocol_instance_id) {
    False -> Nil
    True -> {
      put(
        State(
          ..state,
          open_updates: set.delete(state.open_updates, protocol_instance_id),
        ),
      )
      push(answer)
    }
  }
}

/// The workflow is ending: fail every update it has not answered, so that no client is
/// left waiting on a workflow that will never reply.
pub fn abandon() -> Nil {
  let state = get()
  let unanswered =
    list.append(
      list.sort(set.to_list(state.open_updates), string.compare),
      list.map(state.updates, fn(update) { update.protocol_instance_id }),
    )
  put(State(..state, updates: [], open_updates: set.new()))
  list.each(unanswered, fn(protocol_instance_id) {
    push(bridge.UpdateRejected(
      protocol_instance_id,
      failure.new("the workflow finished before answering the update")
        |> failure.with_type("WorkflowFinished")
        |> failure.non_retryable,
    ))
  })
}

/// Take the oldest accepted update with this name, settling queued ones on the way.
pub fn take(name: String) -> Option(PendingUpdate) {
  case list.find(get().updates, fn(update) { update.name == name }) {
    Error(Nil) -> None
    Ok(update) -> {
      case update.accepted {
        True -> Nil
        False -> settle(update)
      }
      let state = get()
      case
        list.find(state.updates, fn(other) {
          other.protocol_instance_id == update.protocol_instance_id
        })
      {
        // Rejected: look at the next one.
        Error(Nil) -> take(name)
        Ok(accepted) -> {
          put(
            State(
              ..state,
              updates: list.filter(state.updates, fn(other) {
                other.protocol_instance_id != update.protocol_instance_id
              }),
              open_updates: set.insert(
                state.open_updates,
                update.protocol_instance_id,
              ),
            ),
          )
          Some(accepted)
        }
      }
    }
  }
}
