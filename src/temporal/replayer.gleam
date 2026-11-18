//// Replay recorded histories against the current workflow code.
////
//// Replaying is how to check that a change to workflow code is compatible with workflows
//// already running: fetch histories from production with `client.history`, keep them as
//// fixtures, and replay them in tests.

import gleam/dict
import gleam/list
import temporal/client.{type History}
import temporal/internal/bridge
import temporal/internal/poller
import temporal/middleware.{type Outbound}
import temporal/workflow

pub type ReplayError {
  /// The code issued different commands than the history records.
  Nondeterminism(message: String)
  /// The workflow code crashed, or the history could not be replayed at all.
  ReplayFailed(message: String)
}

// Values of core's `EvictionReason` that mean the replay went wrong.
const nondeterminism = 3

const benign_reasons = [1, 5, 10]

pub fn replay(
  workflows: List(workflow.Handler),
  history: History,
) -> Result(Nil, ReplayError) {
  replay_with(workflows, history, middleware.none())
}

/// Replay with the outbound middleware the worker that recorded the history ran with.
pub fn replay_with(
  workflows: List(workflow.Handler),
  history: History,
  outbound: Outbound,
) -> Result(Nil, ReplayError) {
  let registry = dict.from_list(list.map(workflows, fn(w) { #(w.name, w.run) }))
  case
    bridge.replay_worker_new("replay", [
      #(history.workflow_id, history.encoded),
    ])
  {
    Error(message) -> Error(ReplayFailed(message))
    Ok(worker) ->
      case poller.workflow_loop(worker, registry, "replay", outbound, True) {
        Error(message) -> Error(ReplayFailed(message))
        Ok(evictions) ->
          list.try_each(list.reverse(evictions), fn(eviction) {
            case eviction.reason {
              reason if reason == nondeterminism ->
                Error(Nondeterminism(eviction.message))
              reason ->
                case list.contains(benign_reasons, reason) {
                  True -> Ok(Nil)
                  False -> Error(ReplayFailed(eviction.message))
                }
            }
          })
      }
  }
}
