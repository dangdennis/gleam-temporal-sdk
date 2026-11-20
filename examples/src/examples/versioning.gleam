//// Versioning with patches: change the code of a workflow that has runs in flight, and
//// prove with a replay that the change is safe.
////
//// A run that started on old code replays its history on the new code, so the new code
//// must issue the same commands in the same order. `workflow.patched` guards a change:
//// old runs keep the old path, new runs take the new one. Replaying recorded histories
//// against the new code, in a test, shows whether a change needs a guard before it is
//// deployed.
////
//// The life of a patch has three steps, shown by the three versions below:
////
//// 1. `patched` with both paths, while runs from before the change are open.
//// 2. `deprecate_patch` with only the new path, once those runs have closed.
//// 3. Neither, once no run that recorded the patch can still replay.

import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/replayer
import temporal/worker
import temporal/workflow

const task_queue = "examples-versioning"

const patch_id = "audit-before-shipping"

/// What `run` observed.
pub type Outcome {
  Outcome(
    /// A history from version 1, replayed on the guarded change.
    guarded: Result(Nil, replayer.ReplayError),
    /// The same history, replayed on the same change without the guard.
    unguarded: Result(Nil, replayer.ReplayError),
    /// A history from the guarded change, replayed after `deprecate_patch`.
    deprecated: Result(Nil, replayer.ReplayError),
  )
}

pub fn process_order() {
  workflow.Definition("process_order", codec.string(), codec.string())
}

pub fn charge() {
  activity.Definition("charge", codec.string(), codec.nil())
}

pub fn audit() {
  activity.Definition("audit", codec.string(), codec.nil())
}

pub fn ship() {
  activity.Definition("ship", codec.string(), codec.nil())
}

fn step(
  ctx: workflow.Context,
  definition: activity.Definition(String, Nil),
  order_id: String,
) -> Result(Nil, Failure) {
  let options = workflow.activity_options(start_to_close: duration.seconds(10))
  workflow.execute_activity(ctx, definition, order_id, options)
}

/// Version 1, as first deployed: charge, then ship.
pub fn version_1(
  ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
  use _ <- result.try(step(ctx, charge(), order_id))
  use _ <- result.try(step(ctx, ship(), order_id))
  Ok("shipped")
}

/// Version 2 adds an audit between the two. Runs that charged on version 1 never
/// scheduled an audit, so the new step is guarded.
pub fn version_2(
  ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
  use _ <- result.try(step(ctx, charge(), order_id))
  use _ <- result.try(case workflow.patched(ctx, patch_id) {
    True -> step(ctx, audit(), order_id)
    False -> Ok(Nil)
  })
  use _ <- result.try(step(ctx, ship(), order_id))
  Ok("shipped")
}

/// The same change without the guard: what not to deploy.
pub fn version_2_unguarded(
  ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
  use _ <- result.try(step(ctx, charge(), order_id))
  use _ <- result.try(step(ctx, audit(), order_id))
  use _ <- result.try(step(ctx, ship(), order_id))
  Ok("shipped")
}

/// Version 3, for when every run from version 1 has closed: the old path is gone.
pub fn version_3(
  ctx: workflow.Context,
  order_id: String,
) -> Result(String, Failure) {
  use _ <- result.try(step(ctx, charge(), order_id))
  workflow.deprecate_patch(ctx, patch_id)
  use _ <- result.try(step(ctx, audit(), order_id))
  use _ <- result.try(step(ctx, ship(), order_id))
  Ok("shipped")
}

/// Run one order on a worker with this version of the workflow and fetch its history.
/// In real use the histories come from production: fetch them with `client.history`
/// and keep a few as test fixtures.
fn record(
  client: Client,
  version: fn(workflow.Context, String) -> Result(String, Failure),
  workflow_id: String,
) -> Result(client.History, client.Error) {
  let done = fn(_ctx, _order_id) { Ok(Nil) }
  use worker <- result.try(
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(process_order(), version)])
    |> worker.with_activities([
      activity.handler(charge(), done),
      activity.handler(audit(), done),
      activity.handler(ship(), done),
    ])
    |> worker.start,
  )
  let history = {
    use handle <- result.try(client.start(
      client,
      process_order(),
      "order-1",
      client.start_options(workflow_id:, task_queue:),
    ))
    use _ <- result.try(client.result(handle))
    client.history(handle)
  }
  worker.stop(worker)
  history
}

pub fn run(client: Client) -> Result(Outcome, client.Error) {
  use from_version_1 <- result.try(record(
    client,
    version_1,
    "examples-versioning-1",
  ))
  use from_version_2 <- result.try(record(
    client,
    version_2,
    "examples-versioning-2",
  ))

  // The replay test. It needs no server and no activities: only the workflow code.
  let replay = fn(version, history) {
    replayer.replay([workflow.handler(process_order(), version)], history)
  }
  Ok(Outcome(
    guarded: replay(version_2, from_version_1),
    unguarded: replay(version_2_unguarded, from_version_1),
    deprecated: replay(version_3, from_version_2),
  ))
}
