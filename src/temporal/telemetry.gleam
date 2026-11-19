//// Metrics from the worker and the client: task latencies, queue polling, cache size,
//// request counts and failures, as Temporal's other SDKs report them.

/// Serve metrics in Prometheus format at `http://<address>/metrics`, where `address` is an
/// IP address and port such as `0.0.0.0:9464`. Port 0 picks a free one. Returns the
/// address being served.
///
/// Call it once, before the first `client.connect`: the SDK's runtime starts then and
/// reads this setting as it does. Later calls fail.
@external(erlang, "temporal_nif", "start_metrics")
pub fn serve_prometheus(address: String) -> Result(String, String)
