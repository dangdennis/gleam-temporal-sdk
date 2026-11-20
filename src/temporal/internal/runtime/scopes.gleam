//// Cancellation scopes: opening and closing them, registering the operations they cancel,
//// and cancelling. Also the cancellation of each kind of operation, and scope deadlines.

import gleam/dict
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set
import temporal/failure
import temporal/internal/bridge
import temporal/internal/runtime/run_state.{
  type Op, ActivityOp, ChildOp, Local, LocalBackingOff, LocalRunning, NexusOp,
  Scope, ScopeOp, SignalOp, State, TimerOp, get, push, put, update,
}

/// How many operations a scope takes on before it drops the ones that have settled, so
/// that a long-lived scope does not remember everything it ever started.
const prune_every = 64

/// Open a scope beneath `parent`. A scope opened under a cancelled parent starts cancelled,
/// unless it is shielded.
pub fn open(parent: Int, shielded: Bool) -> Int {
  let state = get()
  let id = state.next_scope
  let already_cancelled = case shielded {
    True -> None
    False -> cancelled(parent)
  }
  put(
    State(
      ..state,
      next_scope: id + 1,
      scopes: dict.insert(
        state.scopes,
        id,
        Scope(
          parent:,
          shielded:,
          cancelled: already_cancelled,
          ops: [],
          added: 0,
        ),
      ),
    ),
  )
  // A shielded scope is not registered with its parent, so cancellation never reaches it.
  case shielded {
    True -> Nil
    False -> register(parent, ScopeOp(id))
  }
  id
}

/// Close a scope. Whatever is still pending in it takes the scope's place in its parent, so
/// that cancelling the parent still reaches it.
pub fn close(id: Int) -> Nil {
  let state = get()
  case dict.get(state.scopes, id) {
    Error(Nil) -> Nil
    Ok(scope) -> {
      let pending = list.filter(scope.ops, is_pending)
      let scopes =
        dict.delete(state.scopes, id)
        |> dict.map_values(fn(other_id, other) {
          let other = case other.parent == id {
            True -> Scope(..other, parent: scope.parent)
            False -> other
          }
          case other_id == scope.parent {
            False -> other
            True ->
              Scope(
                ..other,
                ops: list.flat_map(other.ops, fn(op) {
                  case op == ScopeOp(id), scope.shielded {
                    True, False -> pending
                    _, _ -> [op]
                  }
                }),
              )
          }
        })
      put(State(..state, scopes:))
    }
  }
}

/// The reason the scope was cancelled, if it was.
pub fn cancelled(id: Int) -> Option(String) {
  case dict.get(get().scopes, id) {
    Ok(scope) -> scope.cancelled
    Error(Nil) -> None
  }
}

/// Make `op` cancellable through `scope`.
pub fn register(scope_id: Int, op: Op) -> Nil {
  update(fn(state) {
    case dict.get(state.scopes, scope_id) {
      Ok(scope) -> {
        let scope = case scope.added >= prune_every {
          True ->
            Scope(..scope, ops: list.filter(scope.ops, is_pending), added: 0)
          False -> scope
        }
        State(
          ..state,
          scopes: dict.insert(
            state.scopes,
            scope_id,
            Scope(..scope, ops: [op, ..scope.ops], added: scope.added + 1),
          ),
        )
      }
      Error(Nil) -> state
    }
  })
}

/// Cancel a scope: request cancellation of everything pending in it, in the order it was
/// started, descending into the scopes opened beneath it. The order is fixed, and matches
/// the TypeScript SDK's, because the commands this emits are checked against history on
/// replay.
pub fn cancel(id: Int, reason: String) -> Nil {
  let state = get()
  case dict.get(state.scopes, id) {
    Ok(Scope(cancelled: None, ..) as scope) -> {
      put(
        State(
          ..state,
          scopes: dict.insert(
            state.scopes,
            id,
            Scope(..scope, cancelled: Some(reason), ops: []),
          ),
        ),
      )
      list.each(list.reverse(scope.ops), fn(op) {
        case is_pending(op), op {
          False, _ -> Nil
          True, TimerOp(seq) -> cancel_timer(seq)
          True, ActivityOp(seq) -> cancel_activity(seq)
          True, SignalOp(seq) -> push(bridge.CancelSignalExternal(seq))
          True, NexusOp(seq) -> push(bridge.CancelNexusOperation(seq))
          True, ChildOp(seq) -> push(bridge.CancelChildWorkflow(seq))
          True, ScopeOp(child) -> cancel(child, reason)
        }
      })
    }
    _ -> Nil
  }
}

fn is_pending(op: Op) -> Bool {
  let state = get()
  case op {
    TimerOp(seq) ->
      !set.contains(state.fired_timers, seq)
      && !set.contains(state.cancelled_timers, seq)
    ActivityOp(seq) -> !dict.has_key(state.activities, seq)
    ChildOp(seq) ->
      case
        dict.has_key(state.children, seq),
        dict.get(state.child_starts, seq)
      {
        True, _ -> False
        False, Ok(bridge.ChildStarted(_)) | False, Error(Nil) -> True
        False, Ok(_) -> False
      }
    SignalOp(seq) -> !dict.has_key(state.external_signals, seq)
    NexusOp(seq) ->
      case
        dict.has_key(state.nexus_results, seq),
        dict.get(state.nexus_starts, seq)
      {
        True, _ | False, Ok(bridge.NexusStartFailed(_)) -> False
        False, _ -> True
      }
    ScopeOp(id) -> dict.has_key(state.scopes, id)
  }
}

/// Lives here rather than with the local activities because it needs `cancel_timer`.
fn cancel_activity(seq: Int) -> Nil {
  let state = get()
  case dict.get(state.locals, seq) {
    Error(Nil) -> push(bridge.RequestCancelActivity(seq))
    Ok(Local(waiting: LocalRunning(running), ..)) ->
      push(bridge.RequestCancelLocalActivity(running))
    // Between attempts there is nothing running to cancel: stop the timer and settle the
    // activity as cancelled here.
    Ok(Local(waiting: LocalBackingOff(timer:, ..), ..)) -> {
      put(
        State(
          ..state,
          locals: dict.delete(state.locals, seq),
          activities: dict.insert(
            state.activities,
            seq,
            bridge.Cancelled(failure.cancelled()),
          ),
        ),
      )
      cancel_timer(timer)
    }
  }
}

/// Cancel a timer that has not fired. Core sends no job for a cancelled timer, so the
/// cancellation is recorded here for the timer's future to see.
pub fn cancel_timer(seq: Int) -> Nil {
  let state = get()
  case
    set.contains(state.fired_timers, seq)
    || set.contains(state.cancelled_timers, seq)
  {
    True -> Nil
    False -> {
      put(
        State(
          ..state,
          cancelled_timers: set.insert(state.cancelled_timers, seq),
          deadlines: dict.delete(state.deadlines, seq),
        ),
      )
      push(bridge.CancelTimer(seq))
    }
  }
}

/// Cancel `scope` when the timer `seq` fires.
pub fn set_deadline(seq: Int, scope: Int) -> Nil {
  update(fn(state) {
    State(..state, deadlines: dict.insert(state.deadlines, seq, scope))
  })
}
