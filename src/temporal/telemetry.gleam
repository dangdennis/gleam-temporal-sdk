//// Metrics from the worker and the client: task latencies, queue polling, cache size,
//// request counts and failures, as Temporal's other SDKs report them.
////
//// ```gleam
//// let assert Ok(address) = telemetry.serve_prometheus("0.0.0.0:9464")
//// let assert Ok(client) = client.connect(client.local())
//// ```

/// Why metrics could not be served.
pub type Error {
  /// The SDK's runtime has already started, because a client was connected, a worker or
  /// test server was made, or this function was called before. Metrics can only be set up
  /// first.
  AlreadyStarted
  /// The address is not an IP address and port, such as `0.0.0.0:9464`. Host names are
  /// not accepted.
  InvalidAddress(message: String)
  /// The address could not be listened on: the port is taken, or the process may not
  /// bind it.
  BindFailed(message: String)
}

/// Serve metrics in Prometheus format at `http://<address>/metrics`, where `address` is an
/// IP address and port such as `0.0.0.0:9464`. Port 0 picks a free one. Returns the
/// address being served.
///
/// Call it once, before the first `client.connect`: the SDK's runtime starts then and
/// reads this setting as it does. Later calls fail with `AlreadyStarted`.
@external(erlang, "temporal_nif", "start_metrics")
pub fn serve_prometheus(address: String) -> Result(String, Error)
