//// Replay recorded histories against the current workflow code.
////
//// Replaying is how to check that a change to workflow code is compatible with workflows
//// already running: fetch histories from production with `client.history`, keep them as
//// fixtures, and replay them in tests.
////
//// ```gleam
//// let assert Ok(history) = client.history(handle)
//// assert replayer.replay([workflow.handler(greet(), greet_workflow)], history) == Ok(Nil)
//// ```

import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import temporal/client.{type History}
import temporal/codec.{type PayloadCodec}
import temporal/internal/bridge
import temporal/internal/poller
import temporal/middleware.{type ActivityCall, type ChildCall, type Outbound}
import temporal/workflow

/// Why a history did not replay.
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
/// Build with `options` and the `with_` functions.
pub type Options {
  Options(
    /// The worker's outbound middleware.
    outbound: Outbound,
    /// The payload codec of the worker's client.
    codec: Option(PayloadCodec),
  )
}

/// No outbound middleware and no codec.
pub fn options() -> Options {
  Options(outbound: middleware.none(), codec: None)
}

/// Rewrite every activity the workflow schedules, as the recording worker's
/// `worker.with_outbound_activity` did.
pub fn with_outbound_activity(
  options: Options,
  rewrite: fn(workflow.Context, ActivityCall) -> ActivityCall,
) -> Options {
  Options(
    ..options,
    outbound: middleware.add_activity(options.outbound, fn(scope, call) {
      rewrite(workflow.context_of(scope), call)
    }),
  )
}

/// Rewrite every child workflow the workflow starts, as the recording worker's
/// `worker.with_outbound_child` did.
pub fn with_outbound_child(
  options: Options,
  rewrite: fn(workflow.Context, ChildCall) -> ChildCall,
) -> Options {
  Options(
    ..options,
    outbound: middleware.add_child(options.outbound, fn(scope, call) {
      rewrite(workflow.context_of(scope), call)
    }),
  )
}

/// Replay with the payload codec the recording worker's client used.
pub fn with_codec(options: Options, codec: PayloadCodec) -> Options {
  Options(..options, codec: Some(codec))
}

/// Run the workflow code against a recorded history. `Ok` means the code issues the
/// commands the history records, so runs started on the old code can continue on the new.
pub fn replay(
  workflows: List(workflow.Handler),
  history: History,
) -> Result(Nil, ReplayError) {
  replay_with(workflows, history, options())
}

/// Replay with the outbound middleware and payload codec of the worker that recorded the
/// history, given as `options()` and its `with_` functions.
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
          // What a replay answers is compared with history and sent nowhere.
          False,
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
