//// Local activities across their retries: scheduling, settling, and the durable backoff
//// timer between attempts that are too far apart to wait for inside a workflow task.
////
//// Cancelling one is in `scopes`, with the other operations a scope cancels.

import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{Some}
import temporal/internal/bridge.{type Command}
import temporal/internal/runtime/determinism.{next_activity, next_timer}
import temporal/internal/runtime/run_state.{
  type Local, Local, LocalBackingOff, LocalRunning, State, get, push, put,
  update,
}

/// Schedule a local activity under a fresh sequence number, which is returned and names
/// the activity from then on.
pub fn start(command: fn(Int) -> Command) -> Int {
  let seq = next_activity()
  let command = command(seq)
  push(command)
  update(fn(state) {
    State(
      ..state,
      locals: dict.insert(state.locals, seq, Local(command, LocalRunning(seq))),
    )
  })
  seq
}

/// The local activity whose attempt in flight has this sequence number.
fn running(seq: Int) -> Result(#(Int, Local), Nil) {
  dict.to_list(get().locals)
  |> list.find(fn(entry) { { entry.1 }.waiting == LocalRunning(seq) })
}

/// Record the outcome of an activity attempt, local or not.
pub fn resolve(seq: Int, result: bridge.Resolution) -> Nil {
  let state = get()
  case running(seq), result {
    Ok(#(first, local)), bridge.Backoff(attempt, backoff_ms, original) -> {
      // The next attempt is too far off to wait for inside a workflow task: wait with a
      // durable timer, then schedule the activity again.
      let timer = next_timer()
      push(bridge.StartTimer(timer, int.max(backoff_ms, 1)))
      update(fn(state) {
        State(
          ..state,
          locals: dict.insert(
            state.locals,
            first,
            Local(..local, waiting: LocalBackingOff(timer, attempt, original)),
          ),
        )
      })
    }
    Ok(#(first, _)), _ ->
      put(
        State(
          ..state,
          locals: dict.delete(state.locals, first),
          activities: dict.insert(state.activities, first, result),
        ),
      )
    Error(Nil), _ ->
      put(
        State(..state, activities: dict.insert(state.activities, seq, result)),
      )
  }
}

/// If `timer` was the backoff of a local activity, schedule its next attempt.
pub fn retry_after(timer: Int) -> Nil {
  let waiting =
    dict.to_list(get().locals)
    |> list.find(fn(entry) {
      case { entry.1 }.waiting {
        LocalBackingOff(timer: found, ..) -> found == timer
        LocalRunning(_) -> False
      }
    })
  case waiting {
    Ok(#(first, Local(command, LocalBackingOff(_, attempt, original)))) -> {
      let seq = next_activity()
      let command = case command {
        bridge.ScheduleLocalActivity(..) ->
          // Each attempt after a backoff is a new activity as far as ids go, numbered
          // like the other SDKs number it: the recorded marker carries this id.
          bridge.ScheduleLocalActivity(
            ..command,
            seq:,
            activity_id: int.to_string(seq),
            attempt:,
            original_schedule_time_ms: Some(original),
          )
        other -> other
      }
      push(command)
      update(fn(state) {
        State(
          ..state,
          locals: dict.insert(
            state.locals,
            first,
            Local(command, LocalRunning(seq)),
          ),
        )
      })
    }
    _ -> Nil
  }
}
