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
import temporal/middleware.{type Outbound}

/// A workflow implementation with its argument and result types erased.
pub type WorkflowFn =
  fn(List(Payload)) -> Result(Option(Payload), Failure)

pub type QueryHandler =
  fn(List(Payload)) -> Result(Payload, String)

/// Something a scope cancels: a timer, activity or child workflow, by sequence number.
pub type Op {
  TimerOp(seq: Int)
  ActivityOp(seq: Int)
  ChildOp(seq: Int)
}

/// A cancellation scope. Cancelling one cancels its pending operations and every scope
/// beneath it that is not shielded.
pub type Scope {
  Scope(
    parent: Int,
    shielded: Bool,
    /// The reason, once cancelled.
    cancelled: Option(String),
    /// Newest first.
    ops: List(Op),
  )
}

/// The scope that workflow cancellation cancels.
pub const root_scope = 0

pub type State {
  State(
    run_id: String,
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    attempt: Int,
    first_execution_run_id: String,
    continued_from_run_id: String,
    memo: Dict(String, Payload),
    history_length: Int,
    continue_as_new_suggested: Bool,
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
    cancelled_timers: Set(Int),
    scopes: Dict(Int, Scope),
    next_scope: Int,
    /// Timers that cancel a scope when they fire, by timer sequence number.
    deadlines: Dict(Int, Int),
    outbound: Outbound,
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
  outbound: Outbound,
  complete: fn(Completion) -> Nil,
) -> Nil {
  let activation = bridge.receive_activation()
  put(State(
    run_id: activation.run_id,
    workflow_id: "",
    workflow_type: "",
    task_queue:,
    attempt: 1,
    first_execution_run_id: "",
    continued_from_run_id: "",
    memo: dict.new(),
    history_length: 0,
    continue_as_new_suggested: False,
    arguments: [],
    now_ms: activation.timestamp_ms,
    is_replaying: activation.is_replaying,
    random: 0,
    next_timer: 1,
    next_activity: 1,
    next_child: 1,
    fired_timers: set.new(),
    cancelled_timers: set.new(),
    scopes: dict.from_list([#(root_scope, Scope(root_scope, False, None, []))]),
    next_scope: 1,
    deadlines: dict.new(),
    outbound:,
    activities: dict.new(),
    children: dict.new(),
    child_starts: dict.new(),
    signals: dict.new(),
    query_handlers: dict.new(),
    pending_queries: [],
    commands: [],
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
    // A cancelled activity or child surfaces as a wrapped cancellation. It ends the
    // workflow as cancelled only if the workflow itself was asked to cancel.
    Error(failure) ->
      case failure.is_cancellation(failure), scope_cancelled(root_scope) {
        True, Some(_) -> bridge.CancelWorkflowExecution
        _, _ -> bridge.FailWorkflow(failure)
      }
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
      history_length: activation.history_length,
      continue_as_new_suggested: activation.continue_as_new_suggested,
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
      first_execution_run_id:,
      continued_from_run_id:,
      memo:,
    ) ->
      put(
        State(
          ..state,
          workflow_type:,
          workflow_id:,
          arguments:,
          random: randomness_seed,
          attempt:,
          first_execution_run_id:,
          continued_from_run_id:,
          memo:,
        ),
      )
    bridge.FireTimer(seq) -> {
      put(State(..state, fired_timers: set.insert(state.fired_timers, seq)))
      case dict.get(state.deadlines, seq) {
        Ok(scope) -> cancel_scope(scope, "timed out")
        Error(Nil) -> Nil
      }
    }
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
    bridge.CancelWorkflow(reason) -> cancel_scope(root_scope, reason)
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

// ---- cancellation scopes ----

/// Open a scope beneath `parent`. A scope opened under a cancelled parent starts cancelled,
/// unless it is shielded.
pub fn open_scope(parent: Int, shielded: Bool) -> Int {
  let state = get()
  let cancelled = case shielded {
    True -> None
    False -> scope_cancelled(parent)
  }
  put(
    State(
      ..state,
      next_scope: state.next_scope + 1,
      scopes: dict.insert(
        state.scopes,
        state.next_scope,
        Scope(parent:, shielded:, cancelled:, ops: []),
      ),
    ),
  )
  state.next_scope
}

/// Close a scope. Whatever is still pending in it now belongs to its parent, so that
/// cancelling the parent still reaches it.
pub fn close_scope(id: Int) -> Nil {
  let state = get()
  case dict.get(state.scopes, id) {
    Error(Nil) -> Nil
    Ok(scope) -> {
      let scopes =
        dict.delete(state.scopes, id)
        |> dict.map_values(fn(_, other) {
          case other.parent == id {
            True -> Scope(..other, parent: scope.parent)
            False -> other
          }
        })
      let scopes = case scope.shielded, dict.get(scopes, scope.parent) {
        False, Ok(parent) ->
          dict.insert(
            scopes,
            scope.parent,
            Scope(
              ..parent,
              ops: list.append(list.filter(scope.ops, is_pending), parent.ops),
            ),
          )
        _, _ -> scopes
      }
      put(State(..state, scopes:))
    }
  }
}

/// The reason the scope was cancelled, if it was.
pub fn scope_cancelled(id: Int) -> Option(String) {
  case dict.get(get().scopes, id) {
    Ok(scope) -> scope.cancelled
    Error(Nil) -> None
  }
}

/// Make `op` cancellable through `scope`.
pub fn register(scope_id: Int, op: Op) -> Nil {
  update(fn(state) {
    case dict.get(state.scopes, scope_id) {
      Ok(scope) ->
        State(
          ..state,
          scopes: dict.insert(
            state.scopes,
            scope_id,
            Scope(..scope, ops: [op, ..scope.ops]),
          ),
        )
      Error(Nil) -> state
    }
  })
}

/// Cancel a scope: request cancellation of its pending operations, oldest first, then cancel
/// the scopes beneath it in the order they were opened. The order is fixed because the
/// commands this emits are checked against history on replay.
pub fn cancel_scope(id: Int, reason: String) -> Nil {
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
      list.each(list.reverse(scope.ops), cancel_op)
      dict.to_list(get().scopes)
      |> list.filter(fn(entry) { entry.1.parent == id && !entry.1.shielded })
      |> list.filter(fn(entry) { entry.0 != id })
      |> list.sort(fn(a, b) { int.compare(a.0, b.0) })
      |> list.each(fn(entry) { cancel_scope(entry.0, reason) })
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
  }
}

fn cancel_op(op: Op) -> Nil {
  case is_pending(op), op {
    False, _ -> Nil
    True, TimerOp(seq) -> cancel_timer(seq)
    True, ActivityOp(seq) -> push(bridge.RequestCancelActivity(seq))
    True, ChildOp(seq) -> push(bridge.CancelChildWorkflow(seq))
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
