//// Values that outbound middleware rewrites before a workflow's request is recorded. Register
//// middleware with `worker.with_outbound_activity` and `worker.with_outbound_child`.
////
//// ```gleam
//// worker.with_outbound_activity(config, fn(ctx, call) {
////   case call.activity_type, workflow.patched(ctx, "send-email-v2") {
////     "send_email", True -> middleware.with_activity_type(call, "send_email_v2")
////     _, _ -> call
////   }
//// })
//// ```
////
//// Outbound middleware runs inside workflow code, with the context of the code that made
//// the request. It must be deterministic, and must give the same answer for as long as
//// histories that passed through it can be replayed: guard a change with
//// `workflow.patched`, as you would in the workflow itself.

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
pub opaque type Outbound {
  // The integer is the cancellation scope of the context the request was made with:
  // `temporal/workflow` builds on this module, so its `Context` cannot be named here.
  Outbound(
    activity: fn(Int, ActivityCall) -> ActivityCall,
    child: fn(Int, ChildCall) -> ChildCall,
  )
}

/// Outbound middleware that changes nothing.
pub fn none() -> Outbound {
  Outbound(activity: fn(_, call) { call }, child: fn(_, call) { call })
}

/// Schedule a different activity type.
pub fn with_activity_type(
  call: ActivityCall,
  activity_type: String,
) -> ActivityCall {
  ActivityCall(..call, activity_type:)
}

/// Schedule the activity on another task queue.
pub fn with_activity_task_queue(
  call: ActivityCall,
  task_queue: String,
) -> ActivityCall {
  ActivityCall(..call, task_queue:)
}

/// Replace the activity's encoded arguments.
pub fn with_activity_arguments(
  call: ActivityCall,
  arguments: List(Payload),
) -> ActivityCall {
  ActivityCall(..call, arguments:)
}

/// Replace the headers the activity is scheduled with.
pub fn with_activity_headers(
  call: ActivityCall,
  headers: Dict(String, Payload),
) -> ActivityCall {
  ActivityCall(..call, headers:)
}

/// Start a different workflow type as the child.
pub fn with_child_workflow_type(
  call: ChildCall,
  workflow_type: String,
) -> ChildCall {
  ChildCall(..call, workflow_type:)
}

/// Start the child under another workflow id.
pub fn with_child_workflow_id(
  call: ChildCall,
  workflow_id: String,
) -> ChildCall {
  ChildCall(..call, workflow_id:)
}

/// Start the child on another task queue.
pub fn with_child_task_queue(call: ChildCall, task_queue: String) -> ChildCall {
  ChildCall(..call, task_queue:)
}

/// Replace the child's encoded arguments.
pub fn with_child_arguments(
  call: ChildCall,
  arguments: List(Payload),
) -> ChildCall {
  ChildCall(..call, arguments:)
}

/// Replace the headers the child is started with.
pub fn with_child_headers(
  call: ChildCall,
  headers: Dict(String, Payload),
) -> ChildCall {
  ChildCall(..call, headers:)
}

/// Add a rewrite of activities, which runs after the ones already there.
@internal
pub fn add_activity(
  outbound: Outbound,
  rewrite: fn(Int, ActivityCall) -> ActivityCall,
) -> Outbound {
  let previous = outbound.activity
  Outbound(..outbound, activity: fn(scope, call) {
    rewrite(scope, previous(scope, call))
  })
}

/// Add a rewrite of child workflows, which runs after the ones already there.
@internal
pub fn add_child(
  outbound: Outbound,
  rewrite: fn(Int, ChildCall) -> ChildCall,
) -> Outbound {
  let previous = outbound.child
  Outbound(..outbound, child: fn(scope, call) {
    rewrite(scope, previous(scope, call))
  })
}

/// Pass an activity a workflow is about to schedule through the middleware.
@internal
pub fn rewrite_activity(
  outbound: Outbound,
  scope: Int,
  call: ActivityCall,
) -> ActivityCall {
  outbound.activity(scope, call)
}

/// Pass a child workflow a workflow is about to start through the middleware.
@internal
pub fn rewrite_child(
  outbound: Outbound,
  scope: Int,
  call: ChildCall,
) -> ChildCall {
  outbound.child(scope, call)
}
