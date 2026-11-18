//// Cross-SDK conformance: histories recorded by the official TypeScript SDK must replay
//// through this SDK without a nondeterminism error. That holds only if each Gleam workflow
//// issues the same commands, in the same order and with the same ids, as its TypeScript
//// twin in `test/golden/typescript/workflows.js`. No server is needed.
////
//// Regenerate the fixture with `npm install && npm run record` in `test/golden/typescript`.

import gleam/bit_array
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/replayer
import temporal/workflow.{type Context}

@external(erlang, "temporal_test_ffi", "read_file")
fn read_file(path: String) -> Result(BitArray, String)

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn added() {
  workflow.SignalDefinition("added", codec.int())
}

fn golden() {
  workflow.Definition("golden", codec.int(), codec.int())
}

fn double_child() {
  workflow.Definition("double_child", codec.int(), codec.int())
}

fn options() {
  workflow.activity_options(start_to_close_ms: 10_000)
}

fn golden_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  let assert Ok(Nil) = workflow.sleep(ctx, 10)
  use a <- try(workflow.execute_activity(ctx, double(), n, options()))
  let assert [Ok(b), Ok(c)] =
    workflow.await_all([
      workflow.start_activity(ctx, double(), a, options()),
      workflow.start_activity(ctx, double(), a + 1, options()),
    ])
  let signalled = workflow.receive_signal(ctx, added())
  let child =
    workflow.child_options(
      workflow_id: workflow.info(ctx).workflow_id <> "-child",
    )
  use doubled <- try(workflow.execute_child(ctx, double_child(), c, child))
  Ok(b + c + signalled + doubled)
}

/// The same workflow with its first two steps swapped.
fn diverging_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  use a <- try(workflow.execute_activity(ctx, double(), n, options()))
  let assert Ok(Nil) = workflow.sleep(ctx, 10)
  Ok(a)
}

fn try(result: Result(a, e), next: fn(a) -> Result(b, e)) -> Result(b, e) {
  case result {
    Ok(value) -> next(value)
    Error(error) -> Error(error)
  }
}

fn fixture() -> client.History {
  let assert Ok(encoded) = read_file("test/fixtures/typescript_sdk_history.bin")
  client.History(workflow_id: "golden-typescript", encoded:)
}

pub fn typescript_sdk_history_replays_test() {
  let handlers = [workflow.handler(golden(), golden_workflow)]
  assert replayer.replay(handlers, fixture()) == Ok(Nil)
}

pub fn typescript_sdk_history_rejects_diverging_code_test() {
  let handlers = [workflow.handler(golden(), diverging_workflow)]
  let assert Error(replayer.Nondeterminism(_)) =
    replayer.replay(handlers, fixture())
}

fn slow() {
  activity.Definition("slow", codec.int(), codec.nil())
}

fn scopes() {
  workflow.Definition("scopes", codec.int(), codec.int())
}

/// The twin of `scopes` in workflows.js: a deadline that cancels an activity, a scope that
/// cancels a timer, and shielded cleanup after the workflow itself is cancelled.
fn scopes_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  scopes_body(ctx, n, fn(ctx, cleanup) {
    workflow.non_cancellable(ctx, cleanup)
  })
}

/// The same workflow without the shield: its cleanup activity is never scheduled.
fn unshielded_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  scopes_body(ctx, n, fn(ctx, cleanup) { cleanup(ctx) })
}

fn scopes_body(
  ctx: Context,
  n: Int,
  shield: fn(Context, fn(Context) -> Result(Int, Failure)) ->
    Result(Int, Failure),
) -> Result(Int, Failure) {
  let _ =
    workflow.with_timeout(ctx, 200, fn(scope) {
      workflow.execute_activity(
        scope,
        slow(),
        2000,
        workflow.activity_options(start_to_close_ms: 30_000),
      )
    })
  use _ <- try(
    workflow.with_scope(ctx, fn(scope) {
      let pending = workflow.timer(scope, 60_000)
      use _ <- try(workflow.sleep(ctx, 10))
      workflow.cancel(scope)
      let _ = workflow.await(pending)
      Ok(Nil)
    }),
  )
  case workflow.sleep(ctx, 60_000) {
    Ok(Nil) -> Ok(n)
    Error(cancelled) -> {
      let _ =
        shield(ctx, fn(ctx) {
          workflow.execute_activity(ctx, double(), n, options())
        })
      Error(cancelled)
    }
  }
}

fn scopes_fixture(name: String) -> client.History {
  let assert Ok(encoded) =
    read_file("test/fixtures/typescript_sdk_" <> name <> "_history.bin")
  client.History(workflow_id: name <> "-typescript", encoded:)
}

pub fn typescript_sdk_cancellation_scopes_replay_test() {
  let handlers = [workflow.handler(scopes(), scopes_workflow)]
  assert replayer.replay(handlers, scopes_fixture("scopes")) == Ok(Nil)
}

/// The run was cancelled while a scope's timer and a plain timer were both pending. Core
/// accepts those two cancellations in either order, so this checks that both are issued,
/// not their order; `runtime_test` pins the order.
pub fn typescript_sdk_early_cancellation_replays_test() {
  let handlers = [workflow.handler(scopes(), scopes_workflow)]
  assert replayer.replay(handlers, scopes_fixture("early_cancel")) == Ok(Nil)
}

pub fn typescript_sdk_cancellation_history_rejects_diverging_code_test() {
  let handlers = [workflow.handler(scopes(), unshielded_workflow)]
  let assert Error(replayer.Nondeterminism(_)) =
    replayer.replay(handlers, scopes_fixture("scopes"))
}

pub fn fixture_is_not_empty_test() {
  // Guards against a truncated fixture, which would replay trivially.
  assert bit_array.byte_size(fixture().encoded) > 1000
}
