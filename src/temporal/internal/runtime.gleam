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

/// Something a scope cancels: a timer, activity or child workflow, by sequence number, or a
/// nested scope.
pub type Op {
  TimerOp(seq: Int)
  ActivityOp(seq: Int)
  ChildOp(seq: Int)
  /// A signal to another workflow that has not been delivered yet.
  SignalOp(seq: Int)
  NexusOp(seq: Int)
  /// A scope opened beneath this one, by scope id.
  ScopeOp(id: Int)
}

/// An update a client sent, waiting for the workflow to take it.
pub type PendingUpdate {
  PendingUpdate(
    id: String,
    protocol_instance_id: String,
    name: String,
    input: List(Payload),
    /// False on replay: history already says the update was accepted.
    run_validator: Bool,
    accepted: Bool,
  )
}

/// A local activity across its retries. Core runs the attempts that fit inside a workflow
/// task; for a longer wait it answers with a backoff, and the runtime starts a timer and
/// schedules the activity again under a new sequence number.
pub type Local {
  Local(
    /// The command that schedules it, re-sent with a new seq and attempt after a backoff.
    command: Command,
    /// The sequence number of the attempt in flight, or of the backoff timer.
    waiting: LocalWait,
  )
}

pub type LocalWait {
  LocalRunning(seq: Int)
  LocalBackingOff(timer: Int, attempt: Int, original_schedule_time_ms: Int)
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
    next_signal: Int,
    next_cancel: Int,
    next_nexus: Int,
    /// Outcomes of signals sent to and cancellations requested of other workflows.
    external_signals: Dict(Int, Option(Failure)),
    external_cancels: Dict(Int, Option(Failure)),
    nexus_starts: Dict(Int, bridge.NexusStart),
    nexus_results: Dict(Int, bridge.Resolution),
    /// Local activities by the sequence number of their first attempt, which is also the
    /// key their final outcome is stored under in `activities`.
    locals: Dict(Int, Local),
    /// Oldest first.
    updates: List(PendingUpdate),
    /// The update names the workflow handles, each with the check an update must pass to
    /// be accepted.
    update_checks: Dict(String, fn(List(Payload)) -> Result(Nil, String)),
    /// The API's `VersioningBehavior` value reported with every completion.
    versioning_behavior: Int,
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
    next_signal: 1,
    next_cancel: 1,
    next_nexus: 1,
    external_signals: dict.new(),
    external_cancels: dict.new(),
    nexus_starts: dict.new(),
    nexus_results: dict.new(),
    locals: dict.new(),
    updates: [],
    update_checks: dict.new(),
    versioning_behavior: 0,
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
  // The workflow has run as far as it can: any update it has not claimed is settled now.
  list.each(get().updates, fn(update) {
    case update.accepted {
      True -> Nil
      False -> settle_update(update)
    }
  })
  let state = get()
  let answers = list.map(list.reverse(state.pending_queries), answer_query)
  put(State(..state, commands: [], pending_queries: []))
  state.complete(bridge.Success(
    state.run_id,
    list.append(list.reverse(state.commands), answers),
    state.versioning_behavior,
  ))
}

// ---- updates ----

/// Declare that the workflow handles updates with this name. `check` decides whether an
/// update is accepted; a later registration for the same name replaces it only when
/// `replace` is set.
pub fn register_update(
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
fn settle_update(update: PendingUpdate) -> Nil {
  let state = get()
  let verdict = case
    update.run_validator,
    dict.get(state.update_checks, update.name)
  {
    // History already records the acceptance; validators do not run on replay.
    False, _ -> Ok(Nil)
    True, Ok(check) ->
      case bridge.rescue(fn() { check(update.input) }) {
        Ok(verdict) -> verdict
        Error(crash) -> Error("update validator crashed: " <> crash)
      }
    True, Error(Nil) ->
      Error("no handler registered for update \"" <> update.name <> "\"")
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
        failure.new(reason) |> failure.with_type("UpdateRejected"),
      ))
    }
  }
}

/// Take the oldest accepted update with this name, settling queued ones on the way.
pub fn take_update(name: String) -> Option(PendingUpdate) {
  case list.find(get().updates, fn(update) { update.name == name }) {
    Error(Nil) -> None
    Ok(update) -> {
      case update.accepted {
        True -> Nil
        False -> settle_update(update)
      }
      let state = get()
      case
        list.find(state.updates, fn(other) {
          other.protocol_instance_id == update.protocol_instance_id
        })
      {
        // Rejected: look at the next one.
        Error(Nil) -> take_update(name)
        Ok(accepted) -> {
          put(
            State(
              ..state,
              updates: list.filter(state.updates, fn(other) {
                other.protocol_instance_id != update.protocol_instance_id
              }),
            ),
          )
          Some(accepted)
        }
      }
    }
  }
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
      retry_local_after(seq)
    }
    bridge.ResolveActivity(seq, result) -> resolve_activity(seq, result)
    bridge.DoUpdate(id:, protocol_instance_id:, name:, input:, run_validator:) -> {
      let update =
        PendingUpdate(
          id:,
          protocol_instance_id:,
          name:,
          input:,
          run_validator:,
          accepted: False,
        )
      put(State(..state, updates: list.append(state.updates, [update])))
      // An update the workflow already handles is settled at once, like the other SDKs
      // do. One it does not handle yet waits until the workflow has had a chance to
      // register for it.
      case dict.has_key(state.update_checks, name) {
        True -> settle_update(update)
        False -> Nil
      }
    }
    bridge.ResolveExternalSignal(seq, outcome) ->
      put(
        State(
          ..state,
          external_signals: dict.insert(state.external_signals, seq, outcome),
        ),
      )
    bridge.ResolveExternalCancel(seq, outcome) ->
      put(
        State(
          ..state,
          external_cancels: dict.insert(state.external_cancels, seq, outcome),
        ),
      )
    bridge.ResolveNexusStart(seq, result) ->
      put(
        State(
          ..state,
          nexus_starts: dict.insert(state.nexus_starts, seq, result),
        ),
      )
    bridge.ResolveNexus(seq, result) ->
      put(
        State(
          ..state,
          nexus_results: dict.insert(state.nexus_results, seq, result),
        ),
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
      state.complete(bridge.Success(state.run_id, [], state.versioning_behavior))
      bridge.exit_normal()
    }
  }
}

// ---- local activities ----

/// Schedule a local activity under a fresh sequence number, which is returned and names
/// the activity from then on.
pub fn start_local(command: fn(Int) -> Command) -> Int {
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
fn local_running(seq: Int) -> Result(#(Int, Local), Nil) {
  dict.to_list(get().locals)
  |> list.find(fn(entry) { { entry.1 }.waiting == LocalRunning(seq) })
}

fn resolve_activity(seq: Int, result: bridge.Resolution) -> Nil {
  let state = get()
  case local_running(seq), result {
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
fn retry_local_after(timer: Int) -> Nil {
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

pub fn push(command: Command) -> Nil {
  update(fn(state) { State(..state, commands: [command, ..state.commands]) })
}

// ---- cancellation scopes ----

/// Open a scope beneath `parent`. A scope opened under a cancelled parent starts cancelled,
/// unless it is shielded.
pub fn open_scope(parent: Int, shielded: Bool) -> Int {
  let state = get()
  let id = state.next_scope
  let cancelled = case shielded {
    True -> None
    False -> scope_cancelled(parent)
  }
  put(
    State(
      ..state,
      next_scope: id + 1,
      scopes: dict.insert(
        state.scopes,
        id,
        Scope(parent:, shielded:, cancelled:, ops: []),
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
pub fn close_scope(id: Int) -> Nil {
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

/// Cancel a scope: request cancellation of everything pending in it, in the order it was
/// started, descending into the scopes opened beneath it. The order is fixed, and matches
/// the TypeScript SDK's, because the commands this emits are checked against history on
/// replay.
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
      list.each(list.reverse(scope.ops), fn(op) {
        case is_pending(op), op {
          False, _ -> Nil
          True, TimerOp(seq) -> cancel_timer(seq)
          True, ActivityOp(seq) -> cancel_activity(seq)
          True, SignalOp(seq) -> push(bridge.CancelSignalExternal(seq))
          True, NexusOp(seq) -> push(bridge.CancelNexusOperation(seq))
          True, ChildOp(seq) -> push(bridge.CancelChildWorkflow(seq))
          True, ScopeOp(child) -> cancel_scope(child, reason)
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
