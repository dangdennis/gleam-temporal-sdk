//// Temporal Cloud: connect with an API key, or with a client certificate, over TLS.
////
//// Settings come from the environment, under the names the Temporal CLI and the other
//// SDKs use. Credentials never belong in source code.
////
//// | Variable | Holds |
//// |---|---|
//// | `TEMPORAL_ADDRESS` | The namespace's gRPC endpoint, such as `us-east-1.aws.api.temporal.io:7233` |
//// | `TEMPORAL_NAMESPACE` | The namespace, such as `orders.a1b2c` |
//// | `TEMPORAL_API_KEY` | An API key, for `api_key_options` |
//// | `TEMPORAL_TLS_CLIENT_CERT_DATA` | A PEM client certificate, for `mtls_options` |
//// | `TEMPORAL_TLS_CLIENT_KEY_DATA` | Its PEM private key, for `mtls_options` |
////
//// This example is compiled but not run by the tests: it needs a Cloud account.
////
////     TEMPORAL_ADDRESS=... TEMPORAL_NAMESPACE=... TEMPORAL_API_KEY=... gleam run -m examples/cloud

import envoy
import gleam/io
import gleam/result
import temporal/client.{type Client}

fn variable(name: String) -> Result(String, String) {
  envoy.get(name) |> result.replace_error(name <> " is not set")
}

/// Options for an API key: TLS with the system's root certificates, and the key sent
/// with every request. `client.connect` refuses an API key without TLS, which
/// `client.cloud` turns on; the options hold the key so that printing them does not
/// show it.
pub fn api_key_options() -> Result(client.Options, String) {
  use address <- result.try(variable("TEMPORAL_ADDRESS"))
  use namespace <- result.try(variable("TEMPORAL_NAMESPACE"))
  use api_key <- result.try(variable("TEMPORAL_API_KEY"))
  Ok(client.cloud(address:, namespace:, api_key:))
}

/// Options for mutual TLS: the client proves who it is with a certificate instead of a
/// key. The same settings reach a self-hosted service behind TLS; `client.with_server_root_ca`
/// and `client.with_domain` are for one whose certificate the system does not trust or
/// that is issued for another name.
pub fn mtls_options() -> Result(client.Options, String) {
  use address <- result.try(variable("TEMPORAL_ADDRESS"))
  use namespace <- result.try(variable("TEMPORAL_NAMESPACE"))
  use certificate <- result.try(variable("TEMPORAL_TLS_CLIENT_CERT_DATA"))
  use private_key <- result.try(variable("TEMPORAL_TLS_CLIENT_KEY_DATA"))
  let tls =
    client.tls()
    |> client.with_client_certificate(
      certificate: <<certificate:utf8>>,
      private_key: <<private_key:utf8>>,
    )
  Ok(
    client.local()
    |> client.with_target("https://" <> address)
    |> client.with_namespace(namespace)
    |> client.with_tls(tls),
  )
}

/// Connect with an API key if one is set, and with a client certificate otherwise.
pub fn connect() -> Result(Client, String) {
  use options <- result.try(case envoy.get("TEMPORAL_API_KEY") {
    Ok(_) -> api_key_options()
    Error(Nil) -> mtls_options()
  })
  client.connect(options)
  |> result.map_error(fn(error) {
    case error {
      client.ConnectionFailed(message) -> "could not connect: " <> message
      _ -> "could not connect"
    }
  })
}

pub fn main() -> Nil {
  case connect() {
    Ok(client) -> io.println("connected to " <> client.namespace(client))
    Error(reason) -> io.println_error(reason)
  }
}
