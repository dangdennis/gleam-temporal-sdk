//// Run workflows and activities by polling a task queue.

import gleam/dict
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/result
import temporal/activity
import temporal/client.{type Client}
import temporal/internal/bridge
import temporal/internal/poller
import temporal/workflow

pub type Config {
  Config(
    client: Client,
    task_queue: String,
    workflows: List(workflow.Handler),
    activities: List(activity.Handler),
    /// How many workflow runs to keep in memory between tasks. A run that is not cached is
    /// replayed from its history on its next task, so `0` replays every task: slow, but it
    /// exposes non-deterministic workflow code immediately. Use it in tests.
    max_cached_workflows: Int,
  )
}

pub opaque type Worker {
  Worker(nif: bridge.NifWorker)
}

pub fn new(client: Client, task_queue: String) -> Config {
  Config(
    client:,
    task_queue:,
    workflows: [],
    activities: [],
    max_cached_workflows: 1000,
  )
}

pub fn with_workflows(
  config: Config,
  workflows: List(workflow.Handler),
) -> Config {
  Config(..config, workflows: list.append(config.workflows, workflows))
}

pub fn with_activities(
  config: Config,
  activities: List(activity.Handler),
) -> Config {
  Config(..config, activities: list.append(config.activities, activities))
}

pub fn with_max_cached_workflows(config: Config, max: Int) -> Config {
  Config(..config, max_cached_workflows: max)
}

/// Start polling in background processes. Returns once the worker is registered.
pub fn start(config: Config) -> Result(Worker, String) {
  use nif <- result.map(bridge.worker_new(
    client.connection(config.client),
    config.task_queue,
    config.max_cached_workflows,
  ))
  let workflows =
    dict.from_list(list.map(config.workflows, fn(w) { #(w.name, w.run) }))
  let activities =
    dict.from_list(list.map(config.activities, fn(a) { #(a.name, a) }))
  process.spawn_unlinked(fn() {
    case poller.workflow_loop(nif, workflows, config.task_queue, False) {
      Ok(_) -> Nil
      Error(message) ->
        io.println_error("temporal: workflow polling failed: " <> message)
    }
  })
  process.spawn_unlinked(fn() { poller.activity_loop(nif, activities) })
  Worker(nif)
}

/// Stop polling and wait, up to ten seconds, for tasks in flight to finish.
pub fn stop(worker: Worker) -> Nil {
  bridge.worker_shutdown(worker.nif)
}
