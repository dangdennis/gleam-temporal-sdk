//// Cross-SDK conformance: a history recorded by the official TypeScript SDK must replay
//// through this SDK without a nondeterminism error. That holds only if the Gleam workflow
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

pub fn fixture_is_not_empty_test() {
  // Guards against a truncated fixture, which would replay trivially.
  assert bit_array.byte_size(fixture().encoded) > 1000
}
