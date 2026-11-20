//// The state of one workflow run and the accessors for it.
////
//// The state lives in the process dictionary of the run's process, so workflow code and
//// the runtime reach it without threading it through every call. Every other runtime
//// module builds on this one.

import gleam/dict.{type Dict}
import gleam/option.{type Option, None}
import gleam/set.{type Set}
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge.{type Command, type Completion}
import temporal/middleware.{type Outbound}

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
    /// How many operations were added since settled ones were last dropped from `ops`.
    added: Int,
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
    headers: Dict(String, Payload),
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
    /// Updates the workflow has taken and not yet answered, by protocol instance id.
    open_updates: Set(String),
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

/// The state of a run that has received its first activation and applied none of it yet.
pub fn new(
  run_id run_id: String,
  task_queue task_queue: String,
  now_ms now_ms: Int,
  is_replaying is_replaying: Bool,
  outbound outbound: Outbound,
  complete complete: fn(Completion) -> Nil,
) -> State {
  State(
    run_id:,
    workflow_id: "",
    workflow_type: "",
    task_queue:,
    attempt: 1,
    first_execution_run_id: "",
    continued_from_run_id: "",
    memo: dict.new(),
    headers: dict.new(),
    history_length: 0,
    continue_as_new_suggested: False,
    arguments: [],
    now_ms:,
    is_replaying:,
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
    open_updates: set.new(),
    update_checks: dict.new(),
    versioning_behavior: 0,
    fired_timers: set.new(),
    cancelled_timers: set.new(),
    scopes: dict.from_list([
      #(root_scope, Scope(root_scope, False, None, [], 0)),
    ]),
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
  )
}

@external(erlang, "temporal_ffi", "get_state")
pub fn get() -> State

@external(erlang, "temporal_ffi", "put_state")
pub fn put(state: State) -> Nil

pub fn update(change: fn(State) -> State) -> Nil {
  put(change(get()))
}

pub fn push(command: Command) -> Nil {
  update(fn(state) { State(..state, commands: [command, ..state.commands]) })
}
