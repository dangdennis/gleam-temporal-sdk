//// Metrics from the worker and the client: task latencies, queue polling, cache size,
//// request counts and failures, as Temporal's other SDKs report them. And the log of
//// Temporal's core library, which does the polling, the retrying and the talking to the
//// service beneath this SDK.
////
//// ```gleam
//// let assert Ok(Nil) = telemetry.set_core_log_level(telemetry.CoreInfo)
//// let assert Ok(address) = telemetry.serve_prometheus("0.0.0.0:9464")
//// let assert Ok(client) = client.connect(client.local())
//// ```

import temporal/internal/bridge

/// Why telemetry could not be set up.
pub type Error {
  /// The SDK's runtime has already started, because a client was connected, a worker or
  /// test server was made, or `serve_prometheus` was called. Telemetry can only be set up
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

/// The least severe of the core library's log lines to print.
pub type CoreLogLevel {
  /// Print nothing.
  CoreOff
  CoreError
  /// The default: retries that keep failing, tasks failed back to the server, requests
  /// the server refused.
  CoreWarning
  CoreInfo
  /// Every poll and task. For chasing a problem, not for production.
  CoreDebug
}

/// Choose which of the core library's log lines are printed. They go to standard error,
/// one line each, and do not pass through Erlang's `logger`: they come from native code.
/// Warnings and errors are printed unless this says otherwise.
///
/// Call it before `serve_prometheus` and the first `client.connect`: the SDK's runtime
/// starts then and reads this setting as it does. Later calls fail with `AlreadyStarted`.
pub fn set_core_log_level(level: CoreLogLevel) -> Result(Nil, Error) {
  nif_set_core_log_level(case level {
    CoreOff -> bridge.CoreLogOff
    CoreError -> bridge.CoreLogError
    CoreWarning -> bridge.CoreLogWarning
    CoreInfo -> bridge.CoreLogInfo
    CoreDebug -> bridge.CoreLogDebug
  })
}

@external(erlang, "temporal_nif", "set_core_log_level")
fn nif_set_core_log_level(level: bridge.CoreLogLevel) -> Result(Nil, Error)
