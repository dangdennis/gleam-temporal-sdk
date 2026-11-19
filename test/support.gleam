//// Helpers shared by the tests that talk to a real Temporal service.

import gleam/erlang/process
import gleam/int
import gleam/list
import temporal/client.{type Client}
import temporal/worker

/// A string that differs on every call, for ids and task queue names.
@external(erlang, "temporal_test_ffi", "unique")
pub fn unique() -> String

/// Run a shell command and return its output.
@external(erlang, "temporal_test_ffi", "cmd")
pub fn cmd(command: String) -> String

@external(erlang, "temporal_test_ffi", "read_file")
pub fn read_file(path: String) -> Result(BitArray, String)

/// Write a file, creating its directory.
@external(erlang, "temporal_test_ffi", "write_file")
pub fn write_file(path: String, bytes: BitArray) -> Nil

/// A scratch store that activities and the test that started them both reach.
@external(erlang, "temporal_test_ffi", "record")
pub fn record(key: String, value: String) -> Nil

/// What was last recorded under `key`, or `""`.
@external(erlang, "temporal_test_ffi", "recorded")
pub fn recorded(key: String) -> String

/// A client and the task queue of the worker serving the scenario.
pub type Env {
  Env(client: Client, task_queue: String)
}

/// Run `scenario` twice against a fresh worker on its own task queue: with the workflow
/// cache, and without it, so that every workflow task replays the run from the start.
/// `configure` adds the workflows, activities and anything else the worker needs.
pub fn in_both_modes(
  name: String,
  client: Client,
  configure: fn(worker.Config) -> worker.Config,
  scenario: fn(Env) -> Nil,
) -> Nil {
  list.each([1000, 0], fn(max_cached) {
    let task_queue = name <> "-" <> int.to_string(max_cached) <> "-" <> unique()
    let assert Ok(worker) =
      worker.new(client, task_queue)
      |> worker.with_max_cached_workflows(max_cached)
      |> configure
      |> worker.start
    scenario(Env(client, task_queue))
    worker.stop(worker)
  })
}

pub fn local_client() -> Client {
  let assert Ok(client) = client.connect(client.local())
  client
}

/// Start options for a fresh workflow id on the scenario's task queue.
pub fn start_options(env: Env) -> client.StartOptions {
  client.start_options(
    workflow_id: env.task_queue <> "-" <> unique(),
    task_queue: env.task_queue,
  )
}

/// Poll, for up to twenty seconds, for something the server reports with a delay.
pub fn eventually(check: fn() -> Bool) -> Bool {
  list.any(list.repeat(Nil, 100), fn(_) {
    case check() {
      True -> True
      False -> {
        process.sleep(200)
        False
      }
    }
  })
}

/// Register the custom search attribute the tests use. Registering it again is harmless.
pub fn register_search_attribute() -> String {
  let _ =
    cmd(
      "temporal operator search-attribute create --name GleamTestKeyword --type Keyword 2>&1",
    )
  "GleamTestKeyword"
}
