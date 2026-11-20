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
////
//// This module is the activation loop. The run's state is in `runtime/run_state`; scopes,
//// updates, local activities and the replay-stable counters each have a module beside it.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/set
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge.{type Activation, type Command, type Completion}
import temporal/internal/runtime/local_activities
import temporal/internal/runtime/run_state.{
  PendingUpdate, State, get, push, put, root_scope, update,
}
import temporal/internal/runtime/scopes
import temporal/internal/runtime/updates
import temporal/middleware.{type Outbound}

/// A workflow implementation with its argument and result types erased.
pub type WorkflowFn =
  fn(List(Payload)) -> Result(Option(Payload), Failure)

/// Entry point of a workflow run process. Never returns: the process exits when core
/// evicts the run.
pub fn run(
  workflows: Dict(String, WorkflowFn),
  task_queue: String,
  outbound: Outbound,
  complete: fn(Completion) -> Nil,
) -> Nil {
  let activation = bridge.receive_activation()
  put(run_state.new(
    run_id: activation.run_id,
    task_queue:,
    now_ms: activation.timestamp_ms,
    is_replaying: activation.is_replaying,
    outbound:,
    complete:,
  ))
  apply(activation)
  let state = get()
  case dict.get(workflows, state.workflow_type) {
    // The run was in core's cache but its process was not here: the loop that held it
    // restarted. Failing the task makes core evict the run and replay it from the start.
    Error(Nil) if state.workflow_type == "" ->
      fail_task("workflow run is not in memory; replaying it")
    Error(Nil) ->
      fail_task(
        "workflow type \""
        <> state.workflow_type
        <> "\" is not registered on this worker",
      )
    Ok(workflow) ->
      case bridge.rescue(fn() { workflow(state.arguments) }) {
        Ok(result) -> {
          updates.abandon()
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
      case failure.is_cancellation(failure), scopes.cancelled(root_scope) {
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
      False -> updates.settle(update)
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
      headers:,
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
          headers:,
        ),
      )
    bridge.FireTimer(seq) -> {
      put(State(..state, fired_timers: set.insert(state.fired_timers, seq)))
      case dict.get(state.deadlines, seq) {
        Ok(scope) -> scopes.cancel(scope, "timed out")
        Error(Nil) -> Nil
      }
      local_activities.retry_after(seq)
    }
    bridge.ResolveActivity(seq, result) -> local_activities.resolve(seq, result)
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
        True -> updates.settle(update)
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
    bridge.CancelWorkflow(reason) -> scopes.cancel(root_scope, reason)
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
