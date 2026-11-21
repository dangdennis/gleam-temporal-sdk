//// What the SDK refuses to do or to show: an API key sent without TLS, a secret printed,
//// a key of the wrong size, a plaintext payload under strict encryption.

import gleam/bit_array
import gleam/dict
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import support.{cmd, unique}
import temporal/client.{type Client}
import temporal/codec
import temporal/encryption
import temporal/internal/bridge
import temporal/nexus
import temporal/workflow

// ---- an API key needs TLS ----

fn with_key(target: String) -> client.Options {
  client.local()
  |> client.with_target(target)
  |> client.with_api_key("api-key-0123456789")
}

pub fn an_api_key_is_not_sent_without_tls_test() {
  list.each(
    [
      "http://temporal.internal:7233", "http://10.0.0.7:7233",
      "https://us-east-1.aws.api.temporal.io:7233", "temporal.internal:7233",
      // Names and addresses that only look like this machine.
      "http://localhost.example.com:7233", "http://127.0.0.1.example.com:7233",
      "http://128.0.0.1:7233", "http://127.0.0.1@example.com:7233",
      "http://[::2]:7233", "http://0.0.0.0:7233",
    ],
    fn(target) {
      let assert Error(client.ConnectionFailed(message)) =
        client.connect(with_key(target))
      assert string.contains(message, "API key")
      assert string.contains(message, "without TLS")
      assert string.contains(message, target)
      // The refusal names the target, never the key.
      assert !string.contains(message, "api-key-0123456789")
    },
  )
}

pub fn an_api_key_may_go_to_this_machine_without_tls_test() {
  list.each(
    [
      "http://localhost:7233", "http://LOCALHOST:7233", "http://127.0.0.1:7233",
      "http://127.8.9.10:7233", "http://[::1]:7233", "localhost:7233",
      "http://localhost", "http://127.0.0.1:7233/path",
    ],
    fn(target) {
      assert client.check_api_key_transport(with_key(target)) == Ok(Nil)
    },
  )
  // And the development server takes the connection, key and all.
  let assert Ok(_) = client.connect(with_key("http://localhost:7233"))
  Nil
}

pub fn an_api_key_may_go_anywhere_over_tls_test() {
  assert client.check_api_key_transport(
      with_key("https://temporal.internal:7233")
      |> client.with_tls(client.tls()),
    )
    == Ok(Nil)
  assert client.check_api_key_transport(client.cloud(
      address: "us-east-1.aws.api.temporal.io:7233",
      namespace: "n",
      api_key: "k",
    ))
    == Ok(Nil)
  // No key, no objection.
  assert client.check_api_key_transport(
      client.local() |> client.with_target("http://temporal.internal:7233"),
    )
    == Ok(Nil)
}

// ---- secrets are not printable ----

pub fn client_options_do_not_print_their_secrets_test() {
  let options =
    client.cloud(address: "a:7233", namespace: "n", api_key: "api-key-AAAA")
    |> client.with_tls(
      client.tls()
      |> client.with_client_certificate(
        certificate: <<"certificate-BBBB":utf8>>,
        private_key: <<"private-key-CCCC":utf8>>,
      ),
    )
  let shown = string.inspect(options)
  assert !string.contains(shown, "api-key-AAAA")
  assert !string.contains(shown, "private-key-CCCC")
  // As bytes too: a BitArray that is not text prints as numbers.
  assert !string.contains(shown, "112, 114, 105, 118")
  // What is not secret still shows.
  assert string.contains(shown, "https://a:7233")
  assert string.contains(shown, "certificate-BBBB")

  let replaced = client.local() |> client.with_api_key("api-key-DDDD")
  assert !string.contains(string.inspect(replaced), "api-key-DDDD")
}

@external(erlang, "temporal_nif", "debug_connect_options")
fn debug_connect_options(
  options: bridge.ConnectOptions,
) -> Result(String, String)

pub fn the_native_side_does_not_print_secrets_either_test() {
  // What Rust's `{:?}` shows of the options the NIF is given.
  let assert Ok(shown) =
    debug_connect_options(bridge.ConnectOptions(
      target: "https://a:7233",
      namespace: "n",
      identity: "i",
      api_key: Some("api-key-AAAA"),
      tls: Some(bridge.Tls(
        server_root_ca: None,
        domain: Some("a.example"),
        client_certificate: Some(#(<<"cert":utf8>>, <<"KEYKEY":utf8>>)),
      )),
      keep_alive: None,
      connect_timeout_ms: 1000,
    ))
  assert string.contains(shown, "https://a:7233")
  assert string.contains(shown, "a.example")
  assert string.contains(shown, "<redacted>")
  assert !string.contains(shown, "api-key-AAAA")
  // "KEYKEY" as the bytes Rust prints.
  assert !string.contains(shown, "75, 69, 89")
  assert !string.contains(shown, "KEYKEY")
}

pub fn an_encryption_key_does_not_print_its_secret_test() {
  let secret = <<"0123456789abcdef0123456789abcdef":utf8>>
  let assert Ok(key) = encryption.key(id: "2026-01", secret:)
  let shown = string.inspect(key)
  assert string.contains(shown, "2026-01")
  assert !string.contains(shown, "0123456789abcdef")
  assert !string.contains(shown, "48, 49, 50")
  // Nor does the codec built from it.
  assert !string.contains(
    string.inspect(encryption.codec(key, [key])),
    "0123456789abcdef",
  )
}

// ---- encryption keys and strict decoding ----

fn key(id: String, byte: Int) -> encryption.Key {
  let assert Ok(key) =
    encryption.key(id:, secret: bit_array.concat(list.repeat(<<byte>>, 32)))
  key
}

pub fn a_key_that_is_not_32_bytes_is_refused_when_made_test() {
  let assert Ok(_) = encryption.key(id: "k", secret: <<0:256>>)
  list.each([<<>>, <<1:128>>, <<1:248>>, <<1:264>>, <<1:252>>], fn(secret) {
    let assert Error(message) = encryption.key(id: "short", secret:)
    assert string.contains(message, "\"short\"")
    assert string.contains(message, "32 bytes")
  })
  let assert Error(message) =
    encryption.key(id: "k", secret: <<"hunter2":utf8>>)
  assert string.contains(message, "7 bytes")
  assert !string.contains(message, "hunter2")
}

pub fn unencrypted_payloads_pass_through_decoding_by_default_test() {
  let lenient = encryption.codec(key("k1", 1), [])
  let plain = codec.encode(codec.string(), "written before encryption")
  assert lenient.decode([plain]) == Ok([plain])
  assert lenient.decode(lenient.encode([plain])) == Ok([plain])
}

pub fn a_strict_codec_refuses_what_is_not_encrypted_test() {
  let old = key("k0", 0)
  let current = key("k1", 1)
  let strict = encryption.codec(current, [old]) |> encryption.strict
  let plain = codec.encode(codec.string(), "forged")
  let encrypted = strict.encode([plain])
  // What it wrote, and what a retired key wrote, it reads.
  assert strict.decode(encrypted) == Ok([plain])
  assert strict.decode(encryption.codec(old, []).encode([plain])) == Ok([plain])
  assert strict.decode([]) == Ok([])
  // Plaintext is refused, alone or among encrypted payloads, and so is anything
  // labelled otherwise.
  let assert Error(message) = strict.decode([plain])
  assert string.contains(message, "not encrypted")
  let assert Error(_) = strict.decode(list.append(encrypted, [plain]))
  let assert Error(_) =
    strict.decode([
      codec.Payload(
        metadata: dict.from_list([#("encoding", <<"binary/plain":utf8>>)]),
        data: <<1, 2, 3>>,
      ),
    ])
  let assert Error(_) = strict.decode([codec.Payload(dict.new(), <<>>)])
  // A payload encrypted with a key it does not hold is refused, strict or not.
  let foreign = encryption.codec(key("k9", 9), []).encode([plain])
  let assert Error(message) = strict.decode(foreign)
  assert string.contains(message, "k9")
  let assert Error(_) = encryption.codec(current, [old]).decode(foreign)
  // So is one that claims a known key and was not encrypted with it.
  let assert [forged] = encryption.codec(key("k1", 9), []).encode([plain])
  let assert Error(_) = strict.decode([forged])
  Nil
}

// ---- nexus handlers ----

fn double_slowly() {
  workflow.NexusOperation("maths", "double", codec.int(), codec.int())
}

fn waiting() {
  workflow.Definition("security-waiting", codec.int(), codec.int())
}

/// The dispatcher of a worker in the `default` namespace whose one operation starts a
/// workflow with the id `plan_id`, on a task queue nobody polls.
fn dispatch(client: Client, plan_id: String) {
  nexus.dispatcher(
    [
      nexus.workflow_run(double_slowly(), waiting(), fn(_, n) {
        #(
          n,
          client.start_options(
            workflow_id: plan_id,
            task_queue: "security-unpolled",
          ),
        )
      }),
    ],
    client,
  )
}

fn token(fields: String) -> String {
  bit_array.base64_url_encode(<<fields:utf8>>, False)
}

fn cancel(dispatch, operation_token: String) -> bridge.NexusResult {
  dispatch(bridge.NexusCancelTask(<<>>, "maths", "double", operation_token))
}

@external(erlang, "temporal_test_ffi", "capture_logs")
fn capture_logs(body: fn() -> Nil) -> List(#(bridge.LogLevel, String, String))

pub fn a_nexus_cancel_is_refused_unless_its_token_is_for_this_namespace_test() {
  let assert Ok(client) = client.connect(client.local())
  let workflow_id = "security-nexus-" <> unique()
  let dispatch = dispatch(client, workflow_id)
  // A workflow in this namespace that the caller's token must not reach.
  let assert Ok(handle) =
    client.start(
      client,
      waiting(),
      1,
      client.start_options(workflow_id:, task_queue: "security-unpolled"),
    )
  let status = fn() {
    let assert Ok(description) = client.describe(handle)
    description.status
  }
  let cancel_requested = fn() {
    string.contains(
      cmd("temporal workflow show --workflow-id " <> workflow_id),
      "WorkflowExecutionCancelRequested",
    )
  }

  let logged =
    capture_logs(fn() {
      // A token made in another namespace that names this workflow's id.
      let assert bridge.NexusHandlerFailed("NOT_FOUND", failure) =
        cancel(
          dispatch,
          token(
            "{\"t\":1,\"ns\":\"other-team\",\"wid\":\"" <> workflow_id <> "\"}",
          ),
        )
      assert failure.message == "operation not found"
      // What is not a workflow token of ours at all.
      list.each(
        [
          "",
          "not base64 !",
          token("not json"),
          token("[1]"),
          token("{\"t\":1,\"wid\":\"" <> workflow_id <> "\"}"),
          token("{\"t\":1,\"ns\":7,\"wid\":\"" <> workflow_id <> "\"}"),
          token(
            "{\"t\":2,\"ns\":\"default\",\"wid\":\"" <> workflow_id <> "\"}",
          ),
          token("{\"t\":1,\"ns\":\"default\",\"wid\":\"\"}"),
          token("{\"t\":1,\"ns\":\"default\"}"),
        ],
        fn(operation_token) {
          let assert bridge.NexusHandlerFailed("BAD_REQUEST", failure) =
            cancel(dispatch, operation_token)
          assert failure.message == "invalid operation token"
        },
      )
    })
  assert status() == client.Running
  assert !cancel_requested()
  // The refusal is logged here, with the namespace the token named.
  assert list.any(logged, fn(line) { string.contains(line.1, "other-team") })

  // A token for a workflow that does not exist reads the same to the caller as one
  // for another namespace, and says nothing the service said.
  let _ =
    capture_logs(fn() {
      let assert bridge.NexusHandlerFailed("NOT_FOUND", failure) =
        cancel(
          dispatch,
          token(
            "{\"t\":1,\"ns\":\"default\",\"wid\":\"security-no-such-workflow\"}",
          ),
        )
      assert failure.message == "operation not found"
    })

  // The token this handler issued cancels the workflow.
  let assert bridge.NexusCancelAcknowledged =
    cancel(
      dispatch,
      token("{\"t\":1,\"ns\":\"default\",\"wid\":\"" <> workflow_id <> "\"}"),
    )
  assert cancel_requested()
  let assert Ok(Nil) = client.terminate(handle, "done")
  Nil
}

pub fn a_nexus_start_that_fails_tells_the_caller_nothing_internal_test() {
  let assert Ok(client) = client.connect(client.local())
  let start = fn(dispatch) {
    dispatch(bridge.NexusStartTask(
      task_token: <<>>,
      service: "maths",
      operation: "double",
      request_id: unique(),
      input: Some(codec.encode(codec.int(), 2)),
      callback_url: "",
      callback_headers: dict.new(),
      headers: dict.new(),
    ))
  }
  // The service refuses to start a workflow with no id, and says why at length.
  let logged =
    capture_logs(fn() {
      let assert bridge.NexusHandlerFailed("INTERNAL", failure) =
        start(dispatch(client, ""))
      assert failure.message
        == "could not start the workflow for this operation"
    })
  // What it said is in this worker's log, and only there.
  let assert [#(bridge.Error, line, _)] = logged
  assert string.contains(line, "nexus operation maths/double")
  assert string.contains(line, "RequestFailed(")

  // A workflow id that cannot be used again: the caller learns that much, and not the
  // run the service named.
  let workflow_id = "security-nexus-taken-" <> unique()
  let assert Ok(handle) =
    client.start(
      client,
      waiting(),
      1,
      client.start_options(workflow_id:, task_queue: "security-unpolled")
        |> client.with_id_reuse_policy(workflow.RejectDuplicate),
    )
  let assert Ok(Nil) = client.terminate(handle, "done")
  let assert Ok(run_id) = case client.run_id(handle) {
    Some(run_id) -> Ok(run_id)
    None -> Error(Nil)
  }
  let logged =
    capture_logs(fn() {
      let assert bridge.NexusHandlerFailed(error_type, failure) =
        start(nexus.dispatcher(
          [
            nexus.workflow_run(double_slowly(), waiting(), fn(_, n) {
              #(
                n,
                client.start_options(
                  workflow_id:,
                  task_queue: "security-unpolled",
                )
                  |> client.with_id_reuse_policy(workflow.RejectDuplicate),
              )
            }),
          ],
          client,
        ))
      assert error_type == "BAD_REQUEST"
      assert !string.contains(failure.message, run_id)
      assert !string.contains(failure.message, workflow_id)
    })
  assert list.any(logged, fn(line) { string.contains(line.1, "AlreadyExists(") })
}
