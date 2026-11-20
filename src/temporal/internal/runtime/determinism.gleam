//// What a run must reproduce exactly on replay: the sequence numbers that tie commands to
//// their resolutions, the random sequence, and patch decisions.

import gleam/dict
import gleam/int
import gleam/set
import temporal/internal/bridge
import temporal/internal/runtime/run_state.{State, get, push, put}

/// Reserve the sequence number that ties a timer command to its resolution.
pub fn next_timer() -> Int {
  let state = get()
  put(State(..state, next_timer: state.next_timer + 1))
  state.next_timer
}

pub fn next_activity() -> Int {
  let state = get()
  put(State(..state, next_activity: state.next_activity + 1))
  state.next_activity
}

pub fn next_child() -> Int {
  let state = get()
  put(State(..state, next_child: state.next_child + 1))
  state.next_child
}

pub fn next_signal() -> Int {
  let state = get()
  put(State(..state, next_signal: state.next_signal + 1))
  state.next_signal
}

pub fn next_cancel() -> Int {
  let state = get()
  put(State(..state, next_cancel: state.next_cancel + 1))
  state.next_cancel
}

pub fn next_nexus() -> Int {
  let state = get()
  put(State(..state, next_nexus: state.next_nexus + 1))
  state.next_nexus
}

/// Whether the code guarded by this patch should run. Decided once per run: new executions
/// take the patched path and record a marker; replays take it only if history has the marker.
pub fn patched(patch_id: String) -> Bool {
  let state = get()
  case dict.get(state.patch_decisions, patch_id) {
    Ok(decision) -> decision
    Error(Nil) -> {
      let decision =
        !state.is_replaying || set.contains(state.notified_patches, patch_id)
      put(
        State(
          ..state,
          patch_decisions: dict.insert(
            state.patch_decisions,
            patch_id,
            decision,
          ),
        ),
      )
      case decision {
        True -> push(bridge.SetPatchMarker(patch_id, False))
        False -> Nil
      }
      decision
    }
  }
}

/// Record that the code guarded by a patch is now the only code. Lets the `patched` call be
/// deleted once no run started before the patch can still replay.
pub fn deprecate_patch(patch_id: String) -> Nil {
  let state = get()
  case dict.has_key(state.patch_decisions, patch_id) {
    True -> Nil
    False -> {
      put(
        State(
          ..state,
          patch_decisions: dict.insert(state.patch_decisions, patch_id, True),
        ),
      )
      // Like `patched`, a replay only repeats the marker if history has it.
      case
        !state.is_replaying || set.contains(state.notified_patches, patch_id)
      {
        True -> push(bridge.SetPatchMarker(patch_id, True))
        False -> Nil
      }
    }
  }
}

const mask64 = 0xFFFFFFFFFFFFFFFF

/// The next value of the workflow's deterministic random sequence, in `0..2^64` (SplitMix64).
/// Seeded by the server, so replays draw the same values.
pub fn random_bits() -> Int {
  let state = get()
  let seed = int.bitwise_and(state.random + 0x9E3779B97F4A7C15, mask64)
  put(State(..state, random: seed))
  let z = seed
  let z =
    int.bitwise_and(
      int.bitwise_exclusive_or(z, int.bitwise_shift_right(z, 30))
        * 0xBF58476D1CE4E5B9,
      mask64,
    )
  let z =
    int.bitwise_and(
      int.bitwise_exclusive_or(z, int.bitwise_shift_right(z, 27))
        * 0x94D049BB133111EB,
      mask64,
    )
  int.bitwise_exclusive_or(z, int.bitwise_shift_right(z, 31))
}
