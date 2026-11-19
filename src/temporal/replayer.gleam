//// Replay recorded histories against the current workflow code.
////
//// Replaying is how to check that a change to workflow code is compatible with workflows
//// already running: fetch histories from production with `client.history`, keep them as
//// fixtures, and replay them in tests.

import gleam/dict
import gleam/list
import gleam/option.{type Option, None}
import temporal/client.{type History}
import temporal/codec.{type PayloadCodec}
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

/// What the worker that recorded a history ran with, where it changes what replay sees.
pub type Options {
  Options(outbound: Outbound, codec: Option(PayloadCodec))
}

pub fn options() -> Options {
  Options(outbound: middleware.none(), codec: None)
}

pub fn replay(
  workflows: List(workflow.Handler),
  history: History,
) -> Result(Nil, ReplayError) {
  replay_with(workflows, history, options())
}

/// Replay with the outbound middleware and payload codec of the worker that recorded the
/// history.
pub fn replay_with(
  workflows: List(workflow.Handler),
  history: History,
  options: Options,
) -> Result(Nil, ReplayError) {
  let registry = dict.from_list(list.map(workflows, fn(w) { #(w.name, w.run) }))
  case
    bridge.replay_worker_new("replay", [
      #(history.workflow_id, history.encoded),
    ])
  {
    Error(message) -> Error(ReplayFailed(message))
    Ok(worker) ->
      case
        poller.workflow_loop(
          worker,
          registry,
          "replay",
          options.outbound,
          options.codec,
          True,
        )
      {
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
