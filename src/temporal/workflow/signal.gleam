//// Signals: one-way messages to a running workflow. The workflow takes them when it is
//// ready, in the order they arrived; signals that arrive before anyone asks are buffered.
////
//// ```gleam
//// fn approval(ctx: workflow.Context, order: Order) -> Result(Bool, Failure) {
////   let approved = signal.receive(ctx, approve())
////   Ok(approved)
//// }
//// ```
////
//// To wait with a deadline, or for whichever of several things happens first, take the
//// future with `next` and pass it to `workflow.await_timeout` or `workflow.race`. A
//// client sends signals with `client.signal`; a workflow sends them with `send`, or
//// `child.signal` for its own children.

import gleam/option.{type Option}
import temporal/failure.{type Failure}
import temporal/internal/bridge
import temporal/workflow.{type Context, type Future, type SignalDefinition}

/// A future of the next signal of this kind. A signal whose payload does not decode is
/// dropped, with a warning in the log.
pub fn next(_ctx: Context, definition: SignalDefinition(a)) -> Future(a) {
  workflow.once(fn() { workflow.take_signal(definition) })
}

/// Wait for the next signal of this kind.
pub fn receive(ctx: Context, definition: SignalDefinition(a)) -> a {
  workflow.await(next(ctx, definition))
}

/// The next signal of this kind if one has already arrived. Does not wait.
pub fn try_receive(
  _ctx: Context,
  definition: SignalDefinition(a),
) -> Option(a) {
  workflow.take_signal(definition)
}

/// Signal another workflow by id; its current run receives it. The future resolves once
/// the server has delivered the signal, or fails if there is no such workflow.
pub fn send(
  ctx: Context,
  workflow_id: String,
  definition: SignalDefinition(a),
  value: a,
) -> Future(Result(Nil, Failure)) {
  workflow.send_signal(
    ctx,
    bridge.ExternalWorkflow(workflow_id, ""),
    definition,
    value,
  )
}
