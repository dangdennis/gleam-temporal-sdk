//// What a client and a worker do when the network between them and the service breaks:
//// a service that never answers, a connection that goes silent without closing, and a
//// service that answers every poll with an error.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import support.{unique}
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/internal/bridge
import temporal/worker
import temporal/workflow

/// A TCP proxy to the development server, on a port of its own.
@external(erlang, "temporal_test_ffi", "tcp_proxy")
fn tcp_proxy(upstream_port: Int) -> Int

@external(erlang, "temporal_test_ffi", "tcp_proxy_mode")
fn tcp_proxy_mode(port: Int, mode: ProxyMode) -> Nil

/// See `tcp_proxy_mode` in `temporal_test_ffi.erl`.
type ProxyMode {
  Forward
  Blackhole
  NotFound
}

/// A port that accepts connections and never says anything on them.
@external(erlang, "temporal_test_ffi", "silent_listener")
fn silent_listener() -> Int

@external(erlang, "temporal_test_ffi", "capture_logs")
fn capture_logs(body: fn() -> Nil) -> List(#(bridge.LogLevel, String, String))

/// Inside `capture_logs`: wait until a line that contains `text` has been logged.
@external(erlang, "temporal_test_ffi", "await_log")
fn await_log(text: String, timeout_ms: Int) -> Bool

/// EUnit's way of giving one test longer than the rest: `{timeout, Seconds, Test}`.
pub type Eunit {
  Timeout
}

pub fn connect_gives_up_on_a_service_that_never_answers_test() {
  let target = "http://127.0.0.1:" <> int.to_string(silent_listener())
  let options =
    client.local()
    |> client.with_target(target)
    |> client.with_connect_timeout(duration.milliseconds(500))
  let started = timestamp.system_time()
  let assert Error(client.ConnectionFailed(message)) = client.connect(options)
  let waited = timestamp.difference(started, timestamp.system_time())
  assert duration.to_milliseconds(waited) < 5000
  // The message says what was being reached and how long was allowed.
  assert string.contains(message, target)
  assert string.contains(message, "500ms")
}

pub fn the_connection_defaults_are_the_typescript_sdks_test() {
  let options = client.local()
  assert options.connect_timeout == duration.seconds(10)
  assert options.keep_alive
    == Some(client.KeepAlive(
      interval: duration.seconds(30),
      timeout: duration.seconds(15),
    ))
  let cloud = client.cloud(address: "a:7233", namespace: "n", api_key: "k")
  assert cloud.connect_timeout == options.connect_timeout
  assert cloud.keep_alive == options.keep_alive
  assert client.without_keep_alive(options).keep_alive == None
}

pub fn connection_settings_that_make_no_sense_are_refused_test() {
  let assert Error(client.ConnectionFailed(message)) =
    client.local()
    |> client.with_keep_alive(
      interval: duration.seconds(0),
      timeout: duration.seconds(1),
    )
    |> client.connect
  assert string.contains(message, "keep-alive interval")
  let assert Error(client.ConnectionFailed(message)) =
    client.local()
    |> client.with_connect_timeout(duration.seconds(-1))
    |> client.connect
  assert string.contains(message, "connect timeout")
}

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn doubling() {
  workflow.Definition("doubling", codec.int(), codec.int())
}

/// A worker on `task_queue` that reaches the service with `through`.
fn start_worker(through: Client, task_queue: String) -> worker.Worker {
  let assert Ok(worker) =
    worker.new(through, task_queue)
    |> worker.with_workflows([
      workflow.handler(doubling(), fn(ctx, n) {
        workflow.execute_activity(
          ctx,
          double(),
          n,
          workflow.activity_options(start_to_close: duration.seconds(10)),
        )
      }),
    ])
    |> worker.with_activities([
      activity.handler(double(), fn(_, n) { Ok(n * 2) }),
    ])
    |> worker.start
  worker
}

/// Run a workflow on the worker, started by a client that does not go through the proxy.
fn double_21(task_queue: String, id: String) -> Result(Int, client.Error) {
  client.execute(
    support.local_client(),
    doubling(),
    21,
    client.start_options(workflow_id: task_queue <> id, task_queue:)
      |> client.with_execution_timeout(duration.seconds(40)),
  )
}

/// The connection goes silent without being closed, for longer than the keep-alive allows,
/// and stays that way: what was sent into it is lost for good. Without keep-alive pings
/// the worker's polls would wait on the dead connection and it would never take a task
/// again.
pub fn a_worker_outlives_a_connection_that_goes_silent_test() {
  let proxy = tcp_proxy(7233)
  let task_queue = "half-open-" <> unique()
  let assert Ok(through) =
    client.local()
    |> client.with_target("http://127.0.0.1:" <> int.to_string(proxy))
    |> client.with_keep_alive(
      interval: duration.seconds(1),
      timeout: duration.seconds(1),
    )
    |> client.connect
  let worker = start_worker(through, task_queue)
  assert double_21(task_queue, "-before") == Ok(42)

  tcp_proxy_mode(proxy, Blackhole)
  process.sleep(6000)
  tcp_proxy_mode(proxy, Forward)

  assert double_21(task_queue, "-after") == Ok(42)
  assert process.is_alive(worker.manager(worker))
  worker.stop(worker)
}

/// The service answers every poll with an error that is not worth retrying, for longer
/// than the minute core puts up with it. Core then shuts its worker down for good, and a
/// worker that left it at that would sit there, alive and idle. This one says what
/// happened in the log and registers afresh.
///
/// Takes a minute and a half, most of it core's patience.
pub fn a_worker_whose_polls_fail_for_good_registers_again_test_() {
  #(Timeout, 30, fn() {
    let proxy = tcp_proxy(7233)
    let task_queue = "refused-" <> unique()
    let assert Ok(through) =
      client.local()
      |> client.with_target("http://127.0.0.1:" <> int.to_string(proxy))
      |> client.connect
    let worker = start_worker(through, task_queue)
    assert double_21(task_queue, "-before") == Ok(42)

    let logged =
      capture_logs(fn() {
        tcp_proxy_mode(proxy, NotFound)
        assert await_log("registered again and polling", 150_000)
        tcp_proxy_mode(proxy, Forward)
        assert double_21(task_queue, "-after") == Ok(42)
      })
    // Nothing is given up on without an error that says so.
    assert list.any(logged, fn(line) {
      line.0 == bridge.Error
      && string.contains(line.1, "polling on task queue \"" <> task_queue)
      && string.contains(line.1, "failed, registering the worker again")
      && string.contains(line.1, "404")
    })
    assert process.is_alive(worker.manager(worker))
    worker.stop(worker)
  })
}
