//// TLS and mutual TLS, against the development server behind a TLS-terminating proxy that
//// demands a client certificate. Certificates are made with `openssl` for the test.

import gleam/int
import gleam/option.{None, Some}
import gleam/string
import temporal/activity
import temporal/client
import temporal/codec
import temporal/worker
import temporal/workflow

@external(erlang, "temporal_test_ffi", "unique")
fn unique() -> String

@external(erlang, "temporal_test_ffi", "cmd")
fn cmd(command: String) -> String

@external(erlang, "temporal_test_ffi", "read_file")
fn read_file(path: String) -> Result(BitArray, String)

@external(erlang, "temporal_test_ffi", "tls_proxy")
fn tls_proxy(certificate_directory: String, upstream_port: Int) -> Int

/// A certificate authority, a server certificate for `localhost` and a client certificate.
fn make_certificates() -> String {
  let dir = "/tmp/temporal-gleam-tls-" <> unique()
  let script =
    "set -e; mkdir -p DIR; cd DIR;"
    <> " openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=test-ca"
    <> " -keyout ca.key -out ca.pem;"
    <> " for who in server client; do"
    <> " openssl req -newkey rsa:2048 -nodes -subj /CN=localhost"
    <> " -keyout $who.key -out $who.csr;"
    <> " printf 'subjectAltName=DNS:localhost' > $who.ext;"
    <> " openssl x509 -req -in $who.csr -CA ca.pem -CAkey ca.key -CAcreateserial"
    <> " -days 2 -extfile $who.ext -out $who.pem; done; echo made"
  let output = cmd(string.replace(script, "DIR", dir) <> " 2>&1")
  assert string.contains(output, "made")
  dir
}

fn echo_definition() {
  workflow.Definition("tls_echo", codec.string(), codec.string())
}

fn shout() {
  activity.Definition("shout", codec.string(), codec.string())
}

pub fn workflows_run_over_mutual_tls_test() {
  let dir = make_certificates()
  let port = tls_proxy(dir, 7233)
  let target = "https://localhost:" <> int.to_string(port)
  let assert Ok(ca) = read_file(dir <> "/ca.pem")
  let assert Ok(certificate) = read_file(dir <> "/client.pem")
  let assert Ok(key) = read_file(dir <> "/client.key")
  let options = fn(tls) {
    client.Options(..client.local(), target:, tls: Some(tls))
  }

  // The proxy's certificate is not signed by a system authority.
  let assert Error(_) =
    client.connect(
      options(client.Tls(
        server_root_ca: None,
        domain: None,
        client_certificate: Some(#(certificate, key)),
      )),
    )
  // Trusted server, but the proxy insists on a client certificate.
  let assert Error(_) =
    client.connect(
      options(client.Tls(
        server_root_ca: Some(ca),
        domain: None,
        client_certificate: None,
      )),
    )

  let assert Ok(client) =
    client.connect(
      options(client.Tls(
        server_root_ca: Some(ca),
        domain: None,
        client_certificate: Some(#(certificate, key)),
      )),
    )
  let task_queue = "tls-" <> unique()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(echo_definition(), fn(ctx, text) {
        workflow.execute_activity(
          ctx,
          shout(),
          text,
          workflow.activity_options(start_to_close_ms: 10_000),
        )
      }),
    ])
    |> worker.with_activities([
      activity.handler(shout(), fn(_, text) { Ok(string.uppercase(text)) }),
    ])
    |> worker.start
  assert client.execute(
      client,
      echo_definition(),
      "over tls",
      client.start_options(workflow_id: task_queue, task_queue:),
    )
    == Ok("OVER TLS")
  worker.stop(worker)
}
