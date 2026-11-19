//// Values that outbound middleware rewrites before a workflow's request is recorded. Register
//// middleware with `worker.with_outbound_activity` and `worker.with_outbound_child`.
////
//// Outbound middleware runs inside workflow code, so it must be deterministic and must give
//// the same answer for as long as histories that passed through it can be replayed.

import gleam/dict.{type Dict}
import temporal/codec.{type Payload}

/// An activity a workflow is about to schedule.
pub type ActivityCall {
  ActivityCall(
    activity_type: String,
    task_queue: String,
    arguments: List(Payload),
    /// Starts as the workflow's own headers, so context flows on by default.
    headers: Dict(String, Payload),
  )
}

/// A child workflow a workflow is about to start.
pub type ChildCall {
  ChildCall(
    workflow_type: String,
    workflow_id: String,
    task_queue: String,
    arguments: List(Payload),
    /// Starts as the workflow's own headers, so context flows on by default.
    headers: Dict(String, Payload),
  )
}

/// The outbound middleware of a worker.
pub type Outbound {
  Outbound(
    activity: fn(ActivityCall) -> ActivityCall,
    child: fn(ChildCall) -> ChildCall,
  )
}

/// Outbound middleware that changes nothing.
pub fn none() -> Outbound {
  Outbound(activity: fn(call) { call }, child: fn(call) { call })
}
