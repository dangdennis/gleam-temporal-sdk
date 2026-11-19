//// Test helpers: throwaway servers, a clock that skips ahead, and activity contexts that
//// need no server at all.
////
//// ```gleam
//// let assert Ok(env) = testing.start_time_skipping()
//// let assert Ok(worker) =
////   worker.new(testing.client(env), "tests")
////   |> worker.with_workflows([...])
////   |> worker.start
//// // A workflow that sleeps for a week finishes at once.
//// let assert Ok(handle) = client.start(testing.client(env), ...)
//// let result = testing.result(env, handle)
//// worker.stop(worker)
//// testing.stop(env)
//// ```

import gleam/dict
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import temporal/activity
import temporal/client.{type Client}
import temporal/codec.{type Payload}
import temporal/internal/bridge
import temporal/internal/time
import temporal/workflow

/// A server started for a test, and a client connected to it.
pub opaque type Env {
  Env(server: bridge.Server, client: Client, time_skipping: Bool)
}

fn start(
  kind: bridge.ServerKind,
  executable: Option(String),
) -> Result(Env, client.Error) {
  use #(server, target) <- result.try(
    bridge.server_start(kind, executable)
    |> result.map_error(client.ConnectionFailed),
  )
  case client.connect(client.Options(..client.local(), target:)) {
    Ok(client) -> Ok(Env(server, client, kind == bridge.TimeSkipping))
    Error(reason) -> {
      bridge.server_stop(server)
      Error(reason)
    }
  }
}

/// Start a server whose clock skips ahead whenever workflows are only waiting on timers,
/// so that long sleeps and timeouts take no real time. Time only skips while `result` or
/// `sleep` is waiting; otherwise the clock runs normally.
///
/// The server is downloaded on first use and cached in the temporary directory. It does
/// not implement everything a real server does; visibility queries in particular.
pub fn start_time_skipping() -> Result(Env, client.Error) {
  start(bridge.TimeSkipping, None)
}

/// Like `start_time_skipping`, with a `temporal-test-server` executable already on disk.
pub fn start_time_skipping_with(
  executable executable: String,
) -> Result(Env, client.Error) {
  start(bridge.TimeSkipping, Some(executable))
}

/// Start a full development server on a free port, with a real clock. The Temporal CLI is
/// downloaded on first use and cached in the temporary directory.
pub fn start_local() -> Result(Env, client.Error) {
  start(bridge.Dev, None)
}

/// Like `start_local`, with a `temporal` executable already on disk.
pub fn start_local_with(
  executable executable: String,
) -> Result(Env, client.Error) {
  start(bridge.Dev, Some(executable))
}

/// A client connected to the test server.
pub fn client(env: Env) -> Client {
  env.client
}

/// Stop the server. Stop the workers connected to it first.
pub fn stop(env: Env) -> Nil {
  bridge.server_stop(env.server)
}

/// Wait for a workflow's result. On a time-skipping server the clock skips ahead while
/// this waits; on a local server it is `client.result`.
pub fn result(
  env: Env,
  handle: client.Handle(output),
) -> Result(output, client.Error) {
  case env.time_skipping {
    False -> client.result(handle)
    True -> {
      let connection = client.connection(env.client)
      let _ = bridge.time_control(connection, bridge.Unlock)
      let outcome = client.result(handle)
      let _ = bridge.time_control(connection, bridge.Lock)
      outcome
    }
  }
}

/// Start a workflow and wait for its result, skipping time where the server can.
pub fn execute(
  env: Env,
  definition: workflow.Definition(input, output),
  input: input,
  options: client.StartOptions,
) -> Result(output, client.Error) {
  client.start(env.client, definition, input, options)
  |> result.try(result(env, _))
}

/// Let `ms` pass. A time-skipping server jumps its clock forward, firing the timers that
/// fall due; a local server makes the caller wait.
pub fn sleep(env: Env, lasting: Duration) -> Nil {
  let ms = time.to_ms(lasting)
  case env.time_skipping {
    False -> process.sleep(ms)
    True -> {
      let _ =
        bridge.time_control(client.connection(env.client), bridge.Sleep(ms))
      Nil
    }
  }
}

/// The server's current time. On a time-skipping server this runs ahead of the real clock
/// by however much time was skipped.
pub fn now(env: Env) -> Timestamp {
  case env.time_skipping {
    False -> timestamp.system_time()
    True ->
      bridge.time_control(client.connection(env.client), bridge.Now)
      |> result.map(time.timestamp)
      |> result.unwrap(timestamp.system_time())
  }
}

// ---- activities without a server ----

/// An activity context for calling an activity function directly in a test, and what the
/// activity did with it.
pub opaque type ActivityEnv {
  ActivityEnv(
    context: activity.Context,
    heartbeats: process.Subject(List(Payload)),
    cancellation: process.Subject(activity.CancelReason),
  )
}

/// The info of a first attempt, to adjust with record update syntax.
pub fn activity_info() -> activity.Info {
  activity.Info(
    activity_id: "test",
    activity_type: "test",
    workflow_id: "test",
    run_id: "test",
    workflow_type: "test",
    task_queue: "test",
    attempt: 1,
    heartbeat_details: [],
    start_to_close: None,
    heartbeat_timeout: None,
    scheduled_time: time.timestamp(0),
    is_local: False,
    headers: dict.new(),
  )
}

/// Create a context. Call it from the process that will inspect the results.
pub fn activity_env(info: activity.Info) -> ActivityEnv {
  let heartbeats = process.new_subject()
  let cancellation = process.new_subject()
  // The activity may run in another process, so the cancellation is kept where both the
  // test and the activity can reach it: in a subject the context re-fills after reading.
  let cancel_reason = fn() {
    case process.receive(cancellation, 0) {
      Ok(reason) -> {
        process.send(cancellation, reason)
        Some(reason)
      }
      Error(Nil) -> None
    }
  }
  ActivityEnv(
    context: activity.new_context(
      info:,
      task_token: <<"test":utf8>>,
      heartbeat: fn(details) { process.send(heartbeats, details) },
      cancel_reason:,
    ),
    heartbeats:,
    cancellation:,
  )
}

/// The context to pass to the activity function under test.
pub fn activity_context(env: ActivityEnv) -> activity.Context {
  env.context
}

/// Make the activity see a cancellation request from now on.
pub fn cancel_activity(env: ActivityEnv, reason: activity.CancelReason) -> Nil {
  process.send(env.cancellation, reason)
}

/// The details of every heartbeat so far, oldest first, decoded with `codec`. Heartbeats
/// without details, or with details that do not decode, are left out.
pub fn heartbeats(env: ActivityEnv, value_codec: codec.Codec(a)) -> List(a) {
  drain(env.heartbeats, [])
  |> list.filter_map(fn(details) {
    case details {
      [payload, ..] ->
        codec.decode(value_codec, payload) |> result.replace_error(Nil)
      [] -> Error(Nil)
    }
  })
}

/// How many times the activity has heartbeated since the last call.
pub fn heartbeat_count(env: ActivityEnv) -> Int {
  list.length(drain(env.heartbeats, []))
}

fn drain(
  subject: process.Subject(List(Payload)),
  seen: List(List(Payload)),
) -> List(List(Payload)) {
  case process.receive(subject, 0) {
    Ok(details) -> drain(subject, [details, ..seen])
    Error(Nil) -> list.reverse(seen)
  }
}
