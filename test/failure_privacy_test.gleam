//// What a failure tells the Temporal service. A failure's message and stack trace are
//// stored in the clear unless they are encoded, so a crash must be described without the
//// values it tripped over, and `client.with_encoded_failure_attributes` must hide the
//// rest, in the format the other SDKs read.

import gleam/bit_array
import gleam/dict
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleam/time/duration
import support.{type Env, cmd, start_options, unique}
import temporal/activity
import temporal/client.{type Client}
import temporal/codec.{type Payload, type PayloadCodec}
import temporal/encryption
import temporal/failure.{type Failure, Failure}
import temporal/internal/bridge
import temporal/internal/transcode
import temporal/replayer
import temporal/retry
import temporal/worker
import temporal/workflow.{type Context}

const secret = "customer-4111111111111111"

@external(erlang, "temporal_test_ffi", "crash")
fn crash(kind: String, value: String) -> Nil

@external(erlang, "temporal_test_ffi", "capture_logs")
fn capture_logs(body: fn() -> Nil) -> List(#(bridge.LogLevel, String, String))

@external(erlang, "temporal_nif", "debug_completion")
fn debug_completion(
  completion: bridge.Completion,
  namespace: String,
) -> Result(String, String)

@external(erlang, "temporal_nif", "debug_activity_result")
fn debug_activity_result(
  result: bridge.ActivityResult,
) -> Result(String, String)

@external(erlang, "temporal_nif", "debug_nexus_result")
fn debug_nexus_result(result: bridge.NexusResult) -> Result(String, String)

@external(erlang, "temporal_test_ffi", "quietly")
fn quietly(body: fn() -> a) -> a

// ---- crashes are described without their values ----

pub fn an_erlang_crash_is_described_without_its_value_test() {
  let kinds = [
    #("function_clause", "error: function_clause"),
    #("badarg", "error: badarg"),
    #(
      "case_clause",
      "error: case_clause: a value did not match (a binary of 25 bytes)",
    ),
    #(
      "badmatch",
      "error: badmatch: a value did not match (a binary of 25 bytes)",
    ),
    #("error", "error: order_rejected (with a binary of 25 bytes, an integer)"),
    #("throw", "throw: rejected (with a binary of 25 bytes)"),
  ]
  list.each(kinds, fn(kind) {
    let assert Error(#(summary, stack_trace, detail)) =
      bridge.rescue(fn() { crash(kind.0, secret) })
    assert summary == kind.1
    assert !string.contains(stack_trace, secret)
    // The frames say where, by module, function, arity, file and line.
    assert string.contains(stack_trace, "temporal_test_ffi:")
    assert string.contains(stack_trace, "temporal_test_ffi.erl:")
    assert string.contains(stack_trace, "failure_privacy_test:")
    // What is logged locally has all of it.
    assert string.contains(detail, secret)
  })
  // A function that matched no clause has its arguments in the stack: only their number
  // is reported.
  let assert Error(#(_, stack_trace, detail)) =
    bridge.rescue(fn() { crash("function_clause", secret) })
  assert string.contains(stack_trace, "temporal_test_ffi:only_nil/2")
  assert string.contains(detail, "only_nil,[<<\"" <> secret <> "\">>,1]")
}

pub fn an_exit_is_described_without_its_value_test() {
  let assert Error(#(summary, stack_trace, detail)) =
    bridge.rescue_all(fn() { crash("exit", secret) })
  assert summary == "exit: shutdown (with a binary of 25 bytes)"
  assert !string.contains(stack_trace, secret)
  assert string.contains(detail, secret)
  // What a linked process exited with is told the same way.
  let #(summary, detail) = bridge.describe_exit(to_dynamic(#(Badmatch, secret)))
  assert summary
    == "exit: badmatch: a value did not match (a binary of 25 bytes)"
  assert string.contains(detail, secret)
}

type Badmatch {
  Badmatch
}

@external(erlang, "gleam@function", "identity")
fn to_dynamic(value: a) -> b

fn refused(value: String) -> Result(Nil, String) {
  Error(value)
}

pub fn a_gleam_crash_is_described_without_its_value_test() {
  let held = refused(secret)
  let assert Error(#(summary, stack_trace, detail)) =
    bridge.rescue(fn() {
      let assert Ok(Nil) = held
    })
  assert string.starts_with(
    summary,
    "let_assert in failure_privacy_test.a_gleam_crash_is_described_without_its_value_test, line ",
  )
  assert !string.contains(summary, secret)
  assert !string.contains(stack_trace, secret)
  assert string.contains(detail, secret)

  let assert Error(#(summary, _, detail)) =
    bridge.rescue(fn() {
      assert held == Ok(Nil)
    })
  assert string.starts_with(summary, "assert in failure_privacy_test.")
  assert !string.contains(summary, secret)
  assert string.contains(detail, secret)

  // A panic's message is written in the code, and is kept.
  let assert Error(#(summary, _, _)) =
    bridge.rescue(fn() { panic as "the ledger does not balance" })
  assert string.ends_with(summary, ": the ledger does not balance")
}

// ---- encoded attributes at the boundary ----

fn key() -> encryption.Key {
  let assert Ok(key) =
    encryption.key(id: "k1", secret: bit_array.concat(list.repeat(<<7>>, 32)))
  key
}

fn encrypting() -> PayloadCodec {
  encryption.codec(key(), [])
}

/// What core is given when a workflow fails with `failure`.
fn sent(failure: Failure) -> String {
  let assert Ok(seen) =
    debug_completion(
      bridge.Success("run", [bridge.FailWorkflow(failure)], 0),
      "ns",
    )
  seen
}

fn json_payload(text: String) -> Payload {
  codec.Payload(
    metadata: dict.from_list([#("encoding", <<"json/plain":utf8>>)]),
    data: <<text:utf8>>,
  )
}

pub fn the_option_moves_message_and_stack_trace_into_the_codec_test() {
  let failure =
    failure.new("card " <> secret <> " declined")
    |> failure.with_stack_trace("at charge (" <> secret <> ")")
    |> failure.with_type("Declined")
  let failure = Failure(..failure, cause: Some(failure.new("cause " <> secret)))

  // Without the option a failure leaves as it is.
  let plain = sent(transcode.encode_failure(Some(encrypting()), False, failure))
  assert string.contains(plain, "message: \"card " <> secret <> " declined\"")
  assert string.contains(plain, "encoded_attributes: None")

  // With it, nothing of the message or the stack trace is left in the clear, in the
  // failure or in its cause, and what identifies the failure to retry policies stays.
  let encoded = transcode.encode_failure(Some(encrypting()), True, failure)
  let seen = sent(encoded)
  assert !string.contains(seen, secret)
  assert !string.contains(seen, "declined")
  assert string.contains(seen, "message: \"Encoded failure\"")
  assert string.contains(seen, "stack_trace: \"\"")
  assert string.contains(seen, "encoded_attributes: Some(")
  assert string.contains(seen, "type: \"Declined\"")
  assert !string.contains(seen, "encoded_attributes: None")

  // The same on every way out: a failed task, an activity's result, a Nexus result.
  let assert Ok(seen) =
    debug_completion(
      transcode.encode_completion(
        Some(encrypting()),
        True,
        bridge.TaskFailure("run", failure),
      ),
      "ns",
    )
  assert !string.contains(seen, secret)
  assert string.contains(seen, "Encoded failure")
  let assert Ok(seen) =
    debug_activity_result(transcode.encode_activity_result(
      Some(encrypting()),
      True,
      // Not an application failure, so that the NIF wraps it in one.
      bridge.ActivityFailed(Failure(..failure, kind: failure.Timeout)),
    ))
  assert !string.contains(seen, secret)
  assert string.contains(seen, "Encoded failure")
  let assert Ok(seen) =
    debug_nexus_result(bridge.NexusHandlerFailed(
      "INTERNAL",
      transcode.encode_failure(Some(encrypting()), True, failure),
    ))
  assert !string.contains(seen, secret)

  // Reading it back restores both, whether or not the reader has the option on.
  let assert Ok(decoded) = transcode.decode_failure(Some(encrypting()), encoded)
  assert decoded.message == failure.message
  assert decoded.stack_trace == failure.stack_trace
  let assert Some(cause) = decoded.cause
  assert cause.message == "cause " <> secret
  // A reader without the key is refused, as for any payload.
  let assert Error(_) =
    transcode.decode_failure(
      Some(
        encryption.codec(
          {
            let assert Ok(other) = encryption.key(id: "k2", secret: <<0:256>>)
            other
          },
          [],
        ),
      ),
      encoded,
    )
  Nil
}

pub fn encoded_attributes_are_in_the_format_of_the_other_sdks_test() {
  // What the TypeScript SDK's DefaultFailureConverter writes: the object
  // `{message, stack_trace}` as a JSON payload, then the codec. With no codec the
  // payload shows.
  let encoded =
    transcode.encode_failure(
      None,
      True,
      failure.new("he said \"no\"")
        |> failure.with_stack_trace("line 1\nline 2"),
    )
  assert encoded.encoded_attributes
    == Some(json_payload(
      "{\"message\":\"he said \\\"no\\\"\",\"stack_trace\":\"line 1\\nline 2\"}",
    ))
  // A failure built here has no stack trace, which is sent as an empty string.
  assert transcode.encode_failure(None, True, failure.new("m")).encoded_attributes
    == Some(json_payload("{\"message\":\"m\",\"stack_trace\":\"\"}"))

  // And reading what another SDK wrote, with no option set.
  let arrived = fn(attributes) {
    Failure(
      ..failure.new("Encoded failure"),
      encoded_attributes: Some(json_payload(attributes)),
    )
  }
  let read = fn(attributes) {
    let assert Ok(decoded) = transcode.decode_failure(None, arrived(attributes))
    #(decoded.message, decoded.stack_trace)
  }
  assert read("{\"message\":\"real\",\"stack_trace\":\"at x.ts:1\"}")
    == #("real", "at x.ts:1")
  // Key order and extra keys do not matter.
  assert read("{\"stack_trace\":\"s\",\"extra\":1,\"message\":\"real\"}")
    == #("real", "s")
  // What does not conform is not applied, field by field, as in TypeScript.
  assert read("{\"message\":\"real\"}") == #("real", "")
  assert read("{\"message\":7,\"stack_trace\":\"s\"}")
    == #("Encoded failure", "s")
  assert read("[1,2]") == #("Encoded failure", "")
  assert read("not json") == #("Encoded failure", "")
}

/// A protobuf field with a length: the tag, the length as a varint, the bytes.
fn field(number: Int, bytes: BitArray) -> BitArray {
  bit_array.concat([
    varint(int.bitwise_or(int.bitwise_shift_left(number, 3), 2)),
    varint(bit_array.byte_size(bytes)),
    bytes,
  ])
}

fn varint(n: Int) -> BitArray {
  case n < 128 {
    True -> <<n>>
    False -> <<{ n % 128 + 128 }, varint(n / 128):bits>>
  }
}

/// An activity failure as another SDK's worker with `encodeCommonAttributes` left it in
/// history and Rust hands it over: `Encoded failure`, the real message and stack trace in
/// `encoded_attributes`, and in `original` the whole protobuf message, with a `source`
/// this SDK does not model.
fn received_encoded(codec: PayloadCodec) -> Failure {
  let assert [attributes] =
    codec.encode([
      json_payload(
        "{\"message\":\"card "
        <> secret
        <> " declined\",\"stack_trace\":\"at charge.ts:9\"}",
      ),
    ])
  let original =
    bit_array.concat([
      field(1, <<"Encoded failure":utf8>>),
      field(2, <<"TypeScriptSDK":utf8>>),
      field(20, codec.to_bytes(attributes)),
      // ApplicationFailureInfo with the type `Declined`.
      field(5, field(1, <<"Declined":utf8>>)),
    ])
  Failure(
    ..failure.new("Encoded failure"),
    error_type: Some("Declined"),
    encoded_attributes: Some(attributes),
    original: Some(original),
  )
}

pub fn a_failure_that_arrived_encoded_goes_back_as_it_came_test() {
  let codec = encrypting()
  let received = received_encoded(codec)
  let assert Ok(decoded) = transcode.decode_failure(Some(codec), received)
  assert decoded.message == "card " <> secret <> " declined"
  assert decoded.stack_trace == "at charge.ts:9"

  // Returned unchanged it is sent as it was received, whatever the option says: the
  // decoded message does not leak, and what this SDK does not model is still there.
  list.each([False, True], fn(option) {
    let seen = sent(transcode.encode_failure(Some(codec), option, decoded))
    assert !string.contains(seen, secret)
    assert string.contains(seen, "message: \"Encoded failure\"")
    assert string.contains(seen, "source: \"TypeScriptSDK\"")
    assert string.contains(seen, "encoded_attributes: Some(")
    assert string.contains(seen, "type: \"Declined\"")
  })
  // And it reads the same again.
  let assert Ok(again) =
    transcode.decode_failure(
      Some(codec),
      transcode.encode_failure(Some(codec), False, decoded),
    )
  assert again.message == decoded.message

  // Changed in a way that makes it a failure of this SDK's, its message is still not
  // sent in the clear, option or not.
  let flagged = Failure(..decoded, non_retryable: True)
  let seen = sent(transcode.encode_failure(Some(codec), False, flagged))
  assert string.contains(seen, "source: \"GleamSDK\"")
  assert string.contains(seen, "non_retryable: true")
  assert string.contains(seen, "message: \"Encoded failure\"")
  assert !string.contains(seen, secret)
  let retyped = failure.with_type(decoded, "OrderAbandoned")
  let seen = sent(transcode.encode_failure(Some(codec), False, retyped))
  assert string.contains(seen, "type: \"OrderAbandoned\"")
  assert !string.contains(seen, secret)

  // Given a new message, it is a new failure: sent as the option says.
  let relabelled = Failure(..decoded, message: "order abandoned")
  let seen = sent(transcode.encode_failure(Some(codec), False, relabelled))
  assert string.contains(seen, "message: \"order abandoned\"")
  assert !string.contains(seen, secret)
  let seen = sent(transcode.encode_failure(Some(codec), True, relabelled))
  assert !string.contains(seen, "order abandoned")
  assert string.contains(seen, "message: \"Encoded failure\"")

  // A worker without the key shows the failure as it arrived and can still pass it on.
  let assert Ok(key) = encryption.key(id: "k1", secret: <<1:256>>)
  let keyless = Some(encryption.codec(key, []))
  let assert Error(_) = transcode.decode_failure(keyless, received)
  let seen = sent(transcode.encode_failure(keyless, False, received))
  assert string.contains(seen, "source: \"TypeScriptSDK\"")
  assert string.contains(seen, "message: \"Encoded failure\"")
}

/// An activity that timed out, as the server reports it, in the clear.
@external(erlang, "temporal_nif", "sample_activity_failure")
fn sample_activity_failure() -> Failure

pub fn a_failure_that_arrived_in_the_clear_is_passed_on_encoded_with_the_option_test() {
  let received = sample_activity_failure()
  // Without the option, as it came.
  let seen = sent(transcode.encode_failure(Some(encrypting()), False, received))
  assert string.contains(seen, "message: \"Activity task timed out\"")
  // With it, still the activity failure it was, its own message encoded. Its cause is
  // part of what was received and stays as the server has it already.
  let seen = sent(transcode.encode_failure(Some(encrypting()), True, received))
  assert string.contains(seen, "ActivityFailureInfo")
  assert string.contains(seen, "identity: \"the-worker\"")
  assert !string.contains(seen, "message: \"Activity task timed out\"")
  assert string.contains(seen, "message: \"Encoded failure\"")
  assert string.contains(seen, "TimeoutFailureInfo")
}

// ---- against the service ----

fn charge() {
  activity.Definition("charge", codec.string(), codec.nil())
}

fn fumble() {
  activity.Definition("fumble", codec.string(), codec.nil())
}

/// An activity a TypeScript worker runs: see `failures.js`.
fn typescript_decline() {
  activity.Definition("ts_decline", codec.string(), codec.nil())
}

fn activities() -> List(activity.Handler) {
  [
    activity.handler(charge(), fn(_, card) {
      Error(
        failure.new("card " <> card <> " declined")
        |> failure.with_type("Declined")
        |> failure.non_retryable,
      )
    }),
    // Crashes on its input, as code that did not expect it would.
    activity.handler(fumble(), fn(_, card) { Ok(crash("case_clause", card)) }),
  ]
}

fn once() -> workflow.ActivityOptions {
  workflow.activity_options(start_to_close: duration.seconds(10))
  |> workflow.with_retry(retry.policy(maximum_attempts: 1))
}

fn text_workflow(name: String) {
  workflow.Definition(name, codec.string(), codec.nil())
}

fn relaying() {
  workflow.Definition(
    "relaying",
    codec.args2(codec.string(), codec.string()),
    codec.nil(),
  )
}

/// Each fails, with what an activity failed with or with a failure of its own.
fn workflows() -> List(workflow.Handler) {
  [
    workflow.handler(text_workflow("charging"), fn(ctx: Context, card) {
      workflow.execute_activity(ctx, charge(), card, once())
    }),
    workflow.handler(text_workflow("fumbling"), fn(ctx: Context, card) {
      workflow.execute_activity(ctx, fumble(), card, once())
    }),
    workflow.handler(text_workflow("refusing"), fn(_, card) {
      Error(failure.new("no delivery to " <> card))
    }),
    workflow.handler(text_workflow("stumbling"), fn(_, card) {
      let assert "" = card
      Ok(Nil)
    }),
    workflow.handler(relaying(), fn(ctx: Context, arguments) {
      let #(task_queue, card) = arguments
      workflow.execute_activity(
        ctx,
        typescript_decline(),
        card,
        once() |> workflow.with_task_queue(task_queue),
      )
    }),
  ]
}

fn configure(config: worker.Config) -> worker.Config {
  config
  |> worker.with_workflows(workflows())
  |> worker.with_activities(activities())
}

/// The history of a workflow as the server holds it, read with the Temporal CLI.
fn stored(workflow_id: String) -> String {
  cmd("temporal workflow show --output json --workflow-id " <> workflow_id)
}

/// Run a workflow that fails; return its failure and what the server holds.
fn fail(env: Env, workflow_type: String) -> #(Failure, String) {
  let options = start_options(env)
  let assert Ok(handle) =
    client.start(env.client, text_workflow(workflow_type), secret, options)
  let assert Error(client.WorkflowFailed(seen)) = client.result(handle)
  #(seen, stored(options.workflow_id))
}

pub fn a_crash_tells_the_service_what_kind_it_was_and_the_log_the_rest_test() {
  let logged =
    capture_logs(fn() {
      use env <- support.in_both_modes(
        "crash-privacy",
        support.local_client(),
        configure,
      )
      // An activity that crashes fails with the kind of error and where it happened.
      let #(seen, history) = fail(env, "fumbling")
      let cause = failure.root_cause(seen)
      assert cause.message
        == "activity crashed: error: case_clause: a value did not match (a binary of 25 bytes)"
      assert string.contains(cause.stack_trace, "temporal_test_ffi:crash/2")
      assert !string.contains(history, secret)
      assert string.contains(history, "a value did not match")

      // A workflow that crashes fails its task the same way.
      let options = start_options(env)
      let assert Ok(handle) =
        client.start(env.client, text_workflow("stumbling"), secret, options)
      assert support.eventually(fn() {
        string.contains(
          stored(options.workflow_id),
          "EVENT_TYPE_WORKFLOW_TASK_FAILED",
        )
      })
      let history = stored(options.workflow_id)
      assert string.contains(history, "workflow code crashed: let_assert in ")
      assert !string.contains(history, secret)
      let assert Ok(Nil) = client.terminate(handle, "done")
      Nil
    })
  // The values are in the local log, at error level, with the identity of what crashed.
  let has = fn(text) {
    list.any(logged, fn(line) {
      line.0 == bridge.Error
      && string.contains(line.1, text)
      && string.contains(line.1, secret)
    })
  }
  assert has("temporal: activity crashed: error: {case_clause,")
  assert has("temporal: workflow code crashed: error: ")
  assert list.any(logged, fn(line) {
    string.contains(line.1, "workflow code crashed")
    && string.starts_with(line.2, "crash-privacy-")
  })
}

fn shared_key_client(encode_failures: Bool) -> Client {
  // The key of the TypeScript half: see `encryption-codec.js`.
  let assert Ok(key) =
    encryption.key(
      id: "conformance",
      secret: bit_array.concat(list.repeat(<<7>>, 32)),
    )
  let options = client.local() |> client.with_codec(encryption.codec(key, []))
  let assert Ok(client) =
    client.connect(case encode_failures {
      True -> client.with_encoded_failure_attributes(options)
      False -> options
    })
  client
}

pub fn with_the_option_no_failure_text_is_stored_in_the_clear_test() {
  // The crashes below are logged, as they should be; not into the test output.
  use <- quietly
  let encoding = shared_key_client(True)
  use env <- support.in_both_modes("encoded-failures", encoding, configure)
  let assert Some(payload_codec) = client.payload_codec(encoding)
  let codec = replayer.options() |> replayer.with_codec(payload_codec)

  // A failing activity: the workflow sees the real failure and passes it on.
  let options = start_options(env)
  let assert Ok(handle) =
    client.start(env.client, text_workflow("charging"), secret, options)
  let assert Error(client.WorkflowFailed(seen)) = client.result(handle)
  assert seen.kind == failure.Activity
  let cause = failure.root_cause(seen)
  assert cause.message == "card " <> secret <> " declined"
  assert cause.error_type == Some("Declined")
  let history = stored(options.workflow_id)
  assert !string.contains(history, secret)
  assert !string.contains(history, "declined")
  assert string.contains(history, "\"message\": \"Encoded failure\"")
  assert string.contains(history, "encodedAttributes")
  // What a retry policy and an operator go by is still readable.
  assert string.contains(history, "\"type\": \"Declined\"")
  let assert Ok(recorded) = client.history(handle)
  assert replayer.replay_with(workflows(), recorded, codec) == Ok(Nil)

  // A client with the key reads the message whether or not it encodes its own.
  let reader =
    client.handle(
      shared_key_client(False),
      text_workflow("charging"),
      options.workflow_id,
    )
  let assert Error(client.WorkflowFailed(read)) = client.result(reader)
  assert failure.root_cause(read).message == cause.message
  // One without it sees what the server sees.
  let outsider =
    client.handle(
      support.local_client(),
      text_workflow("charging"),
      options.workflow_id,
    )
  let assert Error(client.WorkflowFailed(hidden)) = client.result(outsider)
  assert failure.root_cause(hidden).message == "Encoded failure"

  // A failing workflow, a crashing activity and a crashing workflow.
  let #(seen, history) = fail(env, "refusing")
  assert seen.message == "no delivery to " <> secret
  assert !string.contains(history, "no delivery")
  assert string.contains(history, "\"message\": \"Encoded failure\"")
  let #(seen, history) = fail(env, "fumbling")
  assert string.starts_with(
    failure.root_cause(seen).message,
    "activity crashed: error: case_clause",
  )
  assert string.contains(
    failure.root_cause(seen).stack_trace,
    "temporal_test_ffi:crash/2",
  )
  assert !string.contains(history, "crashed")
  assert !string.contains(history, "temporal_test_ffi")
  let options = start_options(env)
  let assert Ok(handle) =
    client.start(env.client, text_workflow("stumbling"), secret, options)
  assert support.eventually(fn() {
    string.contains(
      stored(options.workflow_id),
      "EVENT_TYPE_WORKFLOW_TASK_FAILED",
    )
  })
  let history = stored(options.workflow_id)
  assert !string.contains(history, "crashed")
  assert string.contains(history, "\"message\": \"Encoded failure\"")
  let assert Ok(Nil) = client.terminate(handle, "done")
  Nil
}

/// Headers are outside the codec, as in the TypeScript SDK, whose client never encodes
/// them: see `temporal/encryption`.
pub fn headers_are_not_passed_through_the_codec_test() {
  let encrypting = shared_key_client(True)
  let task_queue = "plain-headers-" <> unique()
  let options =
    client.start_options(workflow_id: task_queue, task_queue:)
    |> client.with_headers(
      dict.from_list([#("trace", codec.encode(codec.string(), "trace-7f3a"))]),
    )
  let assert Ok(handle) =
    client.start(encrypting, text_workflow("refusing"), secret, options)
  let history = stored(task_queue)
  let base64 = fn(text) { bit_array.base64_encode(<<text:utf8>>, True) }
  assert string.contains(history, base64("\"trace-7f3a\""))
  assert !string.contains(history, base64("\"" <> secret <> "\""))
  let assert Ok(Nil) = client.terminate(handle, "done")
  Nil
}

/// Failures cross between this SDK and the TypeScript SDK, both encrypting and both
/// encoding failure attributes, in each direction: written here and read there, written
/// there and read, passed on and read again here.
pub fn encoded_failures_interoperate_with_the_typescript_sdk_test() {
  let typescript = "test/conformance/typescript"
  case
    cmd("command -v node >/dev/null && echo yes"),
    support.read_file(
      typescript <> "/node_modules/@temporalio/client/package.json",
    )
  {
    "yes\n", Ok(_) -> {
      let client = shared_key_client(True)
      let task_queue = "failure-interop-" <> unique()
      let typescript_queue = task_queue <> "-typescript"
      let assert Ok(worker) =
        worker.new(client, task_queue) |> configure |> worker.start
      let output =
        cmd(
          "cd "
          <> typescript
          <> " && node failures.js "
          <> task_queue
          <> " "
          <> typescript_queue
          <> " "
          <> secret
          <> " 2>&1",
        )
      // What the TypeScript client read: every message this SDK encoded or passed on,
      // the failure's type, and the stack trace of the innermost failure.
      assert string.trim(output)
        == "{\"charging\":{\"messages\":[\"Activity task failed\",\"card "
        <> secret
        <> " declined\"],\"type\":\"Declined\",\"stack\":false},"
        <> "\"relaying\":{\"messages\":[\"Activity task failed\",\"typescript declined "
        <> secret
        <> "\"],\"type\":\"Declined\",\"stack\":true},"
        <> "\"refusing\":{\"messages\":[\"no delivery to "
        <> secret
        <> "\"],\"stack\":false}}"

      // What this SDK reads of the failure the TypeScript activity wrote, and the
      // workflow here passed on: its message, and its stack trace.
      let read = fn(workflow_type) {
        let handle =
          client.handle(
            client,
            text_workflow(workflow_type),
            task_queue <> "-" <> workflow_type,
          )
        let assert Error(client.WorkflowFailed(seen)) = client.result(handle)
        seen
      }
      let relayed = read("relaying")
      assert relayed.kind == failure.Activity
      let cause = failure.root_cause(relayed)
      assert cause.message == "typescript declined " <> secret
      assert cause.error_type == Some("Declined")
      assert string.contains(cause.stack_trace, "failures.js")
      assert failure.root_cause(read("charging")).message
        == "card " <> secret <> " declined"

      // And none of it is in the clear on the server, whichever SDK wrote it.
      list.each(["charging", "relaying", "refusing"], fn(workflow_type) {
        let history = stored(task_queue <> "-" <> workflow_type)
        assert string.contains(history, "EVENT_TYPE_WORKFLOW_EXECUTION_FAILED")
        assert !string.contains(history, secret)
        assert !string.contains(history, "declined")
        assert !string.contains(history, "failures.js")
        assert string.contains(history, "\"message\": \"Encoded failure\"")
      })
      worker.stop(worker)
    }
    _, _ ->
      io.println(
        "\nskipped the encoded-failure interop test: it needs node and "
        <> typescript
        <> "/node_modules (bin/conformance installs them)",
      )
  }
}
