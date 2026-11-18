//// The workflow runtime: one Erlang process per cached workflow run.
////
//// The process runs the workflow function directly. When workflow code awaits something
//// that has not happened yet, the process answers the current activation with the commands
//// gathered so far and blocks in `receive` for the next one. A single process applies the
//// jobs and runs the workflow code, so execution order depends only on the history, which
//// is what makes replay deterministic.
////
//// Each received activation is answered exactly once, by `flush` or by a task failure,
//// before the next one is received; core rejects anything else.

import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set.{type Set}
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge.{type Activation, type Command, type Completion}

/// A workflow implementation with its argument and result types erased.
pub type WorkflowFn =
  fn(List(Payload)) -> Result(Option(Payload), Failure)

pub type QueryHandler =
  fn(List(Payload)) -> Result(Payload, String)

pub type State {
  State(
    run_id: String,
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    attempt: Int,
    arguments: List(Payload),
    now_ms: Int,
    is_replaying: Bool,
    random: Int,
    // Timers, activities and child workflows are numbered separately, each from 1, like
    // the other SDKs do. Activity ids derive from these numbers and are checked on replay,
    // so the numbering is what lets a history recorded by another SDK replay here.
    next_timer: Int,
    next_activity: Int,
    next_child: Int,
    fired_timers: Set(Int),
    activities: Dict(Int, bridge.Resolution),
    children: Dict(Int, bridge.Resolution),
    child_starts: Dict(Int, bridge.ChildStart),
    /// Undelivered signals by name, oldest first.
    signals: Dict(String, List(List(Payload))),
    query_handlers: Dict(String, QueryHandler),
    /// Queries wait until the workflow has run as far as the activation allows, so they
    /// observe the state after the events delivered with them.
    pending_queries: List(#(String, String, List(Payload))),
    /// Newest first.
    commands: List(Command),
    cancel_reason: Option(String),
    notified_patches: Set(String),
    patch_decisions: Dict(String, Bool),
    complete: fn(Completion) -> Nil,
  )
}

@external(erlang, "temporal_ffi", "get_state")
pub fn get() -> State

@external(erlang, "temporal_ffi", "put_state")
pub fn put(state: State) -> Nil

pub fn update(change: fn(State) -> State) -> Nil {
  put(change(get()))
}

/// Entry point of a workflow run process. Never returns: the process exits when core
/// evicts the run.
pub fn run(
  workflows: Dict(String, WorkflowFn),
  task_queue: String,
  complete: fn(Completion) -> Nil,
) -> Nil {
  let activation = bridge.receive_activation()
  put(State(
    run_id: activation.run_id,
    workflow_id: "",
    workflow_type: "",
    task_queue:,
    attempt: 1,
    arguments: [],
    now_ms: activation.timestamp_ms,
    is_replaying: activation.is_replaying,
    random: 0,
    next_timer: 1,
    next_activity: 1,
    next_child: 1,
    fired_timers: set.new(),
    activities: dict.new(),
    children: dict.new(),
    child_starts: dict.new(),
    signals: dict.new(),
    query_handlers: dict.new(),
    pending_queries: [],
    commands: [],
    cancel_reason: None,
    notified_patches: set.new(),
    patch_decisions: dict.new(),
    complete:,
  ))
  apply(activation)
  let state = get()
  case dict.get(workflows, state.workflow_type) {
    Error(Nil) ->
      fail_task(
        "workflow type \""
        <> state.workflow_type
        <> "\" is not registered on this worker",
      )
    Ok(workflow) ->
      case bridge.rescue(fn() { workflow(state.arguments) }) {
        Ok(result) -> {
          push(terminal_command(result))
          serve()
        }
        Error(crash) -> fail_task("workflow code crashed: " <> crash)
      }
  }
}

fn terminal_command(result: Result(Option(Payload), Failure)) -> Command {
  case result {
    Ok(payload) -> bridge.CompleteWorkflow(payload)
    Error(failure.Failure(kind: failure.Cancelled, ..)) ->
      bridge.CancelWorkflowExecution
    Error(failure) -> bridge.FailWorkflow(failure)
  }
}

/// Fail the current workflow task. The workflow stays open and the server retries the task,
/// so a bug fixed by a deploy lets the workflow carry on.
fn fail_task(message: String) -> a {
  let state = get()
  state.complete(bridge.TaskFailure(state.run_id, failure.new(message)))
  apply(bridge.receive_activation())
  serve()
}

/// Answer activations forever. Used once the workflow function has finished, to serve
/// queries until core evicts the run.
pub fn serve() -> a {
  block()
  serve()
}

/// Answer the current activation and wait for the next one.
pub fn block() -> Nil {
  flush()
  apply(bridge.receive_activation())
}

fn flush() -> Nil {
  let state = get()
  let answers = list.map(list.reverse(state.pending_queries), answer_query)
  put(State(..state, commands: [], pending_queries: []))
  state.complete(bridge.Success(
    state.run_id,
    list.append(list.reverse(state.commands), answers),
  ))
}

fn answer_query(query: #(String, String, List(Payload))) -> Command {
  let #(query_id, query_type, arguments) = query
  let result = case dict.get(get().query_handlers, query_type) {
    Error(Nil) ->
      Error("no handler registered for query \"" <> query_type <> "\"")
    Ok(handler) ->
      case bridge.rescue(fn() { handler(arguments) }) {
        Ok(result) -> result
        Error(crash) -> Error("query handler crashed: " <> crash)
      }
  }
  bridge.RespondToQuery(query_id, case result {
    Ok(payload) -> Ok(payload)
    Error(message) -> Error(failure.new(message))
  })
}

fn apply(activation: Activation) -> Nil {
  update(fn(state) {
    State(
      ..state,
      is_replaying: activation.is_replaying,
      // Activations that only carry queries or an eviction have no timestamp.
      now_ms: case activation.timestamp_ms {
        0 -> state.now_ms
        now -> now
      },
    )
  })
  list.each(activation.jobs, apply_job)
}

fn apply_job(job: bridge.Job) -> Nil {
  let state = get()
  case job {
    bridge.InitializeWorkflow(
      workflow_type:,
      workflow_id:,
      arguments:,
      randomness_seed:,
      attempt:,
    ) ->
      put(
        State(
          ..state,
          workflow_type:,
          workflow_id:,
          arguments:,
          random: randomness_seed,
          attempt:,
        ),
      )
    bridge.FireTimer(seq) ->
      put(State(..state, fired_timers: set.insert(state.fired_timers, seq)))
    bridge.ResolveActivity(seq, result) ->
      put(
        State(..state, activities: dict.insert(state.activities, seq, result)),
      )
    bridge.ResolveChildWorkflow(seq, result) ->
      put(State(..state, children: dict.insert(state.children, seq, result)))
    bridge.ResolveChildWorkflowStart(seq, result) ->
      put(
        State(
          ..state,
          child_starts: dict.insert(state.child_starts, seq, result),
        ),
      )
    bridge.UpdateRandomSeed(seed) -> put(State(..state, random: seed))
    bridge.QueryWorkflow(query_id, query_type, arguments) ->
      put(
        State(..state, pending_queries: [
          #(query_id, query_type, arguments),
          ..state.pending_queries
        ]),
      )
    bridge.CancelWorkflow(reason) ->
      put(State(..state, cancel_reason: Some(reason)))
    bridge.SignalWorkflow(name, input) -> {
      let queue = case dict.get(state.signals, name) {
        Ok(queue) -> queue
        Error(Nil) -> []
      }
      put(
        State(
          ..state,
          signals: dict.insert(state.signals, name, list.append(queue, [input])),
        ),
      )
    }
    bridge.NotifyHasPatch(patch_id) ->
      put(
        State(
          ..state,
          notified_patches: set.insert(state.notified_patches, patch_id),
        ),
      )
    bridge.RemoveFromCache(..) -> {
      state.complete(bridge.Success(state.run_id, []))
      bridge.exit_normal()
    }
    bridge.UnsupportedJob(name) ->
      fail_task("this SDK does not support the workflow job \"" <> name <> "\"")
  }
}

pub fn push(command: Command) -> Nil {
  update(fn(state) { State(..state, commands: [command, ..state.commands]) })
}

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

/// Take the oldest undelivered signal with this name.
pub fn take_signal(name: String) -> Option(List(Payload)) {
  let state = get()
  case dict.get(state.signals, name) {
    Ok([first, ..rest]) -> {
      put(State(..state, signals: dict.insert(state.signals, name, rest)))
      Some(first)
    }
    _ -> None
  }
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
