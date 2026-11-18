//// Run workflows and activities by polling a task queue.

import gleam/dict
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import temporal/activity
import temporal/client.{type Client}
import temporal/internal/bridge
import temporal/internal/poller
import temporal/middleware.{
  type ActivityCall, type ChildCall, type Outbound, Outbound,
}
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
    max_concurrent_workflow_tasks: Int,
    max_concurrent_activities: Int,
    /// A limit the server enforces across every worker on the task queue.
    max_task_queue_activities_per_second: Option(Float),
    /// A limit on this worker alone.
    max_activities_per_second: Option(Float),
    /// Outermost first.
    workflow_middleware: List(workflow.Middleware),
    activity_middleware: List(activity.Middleware),
    outbound: Outbound,
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
    max_concurrent_workflow_tasks: 100,
    max_concurrent_activities: 100,
    max_task_queue_activities_per_second: None,
    max_activities_per_second: None,
    workflow_middleware: [],
    activity_middleware: [],
    outbound: middleware.none(),
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

pub fn with_max_concurrent_workflow_tasks(config: Config, max: Int) -> Config {
  Config(..config, max_concurrent_workflow_tasks: max)
}

/// How many activities this worker runs at once.
pub fn with_max_concurrent_activities(config: Config, max: Int) -> Config {
  Config(..config, max_concurrent_activities: max)
}

/// Limit how fast the task queue hands out activities, across all its workers. The server
/// enforces it, and the most recent worker to set it wins.
pub fn with_max_task_queue_activities_per_second(
  config: Config,
  rate: Float,
) -> Config {
  Config(..config, max_task_queue_activities_per_second: Some(rate))
}

/// Limit how fast this worker starts activities.
pub fn with_max_activities_per_second(config: Config, rate: Float) -> Config {
  Config(..config, max_activities_per_second: Some(rate))
}

/// Run `middleware` around every workflow. Middleware added first runs outermost.
pub fn with_workflow_middleware(
  config: Config,
  middleware: workflow.Middleware,
) -> Config {
  Config(
    ..config,
    workflow_middleware: list.append(config.workflow_middleware, [middleware]),
  )
}

/// Run `middleware` around every activity. Middleware added first runs outermost.
pub fn with_activity_middleware(
  config: Config,
  middleware: activity.Middleware,
) -> Config {
  Config(
    ..config,
    activity_middleware: list.append(config.activity_middleware, [middleware]),
  )
}

/// Rewrite every activity a workflow schedules, for example to rename an activity type.
/// See `temporal/middleware` for the rules such a function must follow.
pub fn with_outbound_activity(
  config: Config,
  rewrite: fn(ActivityCall) -> ActivityCall,
) -> Config {
  let previous = config.outbound.activity
  Config(
    ..config,
    outbound: Outbound(..config.outbound, activity: fn(call) {
      rewrite(previous(call))
    }),
  )
}

/// Rewrite every child workflow a workflow starts.
pub fn with_outbound_child(
  config: Config,
  rewrite: fn(ChildCall) -> ChildCall,
) -> Config {
  let previous = config.outbound.child
  Config(
    ..config,
    outbound: Outbound(..config.outbound, child: fn(call) {
      rewrite(previous(call))
    }),
  )
}

/// Start polling in background processes. Returns once the worker is registered. A worker
/// with no workflows polls only for activities, and the other way round.
pub fn start(config: Config) -> Result(Worker, String) {
  use nif <- result.map(bridge.worker_new(
    client.connection(config.client),
    config.task_queue,
    bridge.WorkerOptions(
      max_cached_workflows: config.max_cached_workflows,
      max_concurrent_workflow_tasks: config.max_concurrent_workflow_tasks,
      max_concurrent_activities: config.max_concurrent_activities,
      max_task_queue_activities_per_second: config.max_task_queue_activities_per_second,
      max_activities_per_second: config.max_activities_per_second,
      enable_workflows: config.workflows != [],
      enable_activities: config.activities != [],
    ),
  ))
  let workflows =
    dict.from_list(
      list.map(config.workflows, fn(handler) {
        let wrapped =
          list.fold_right(config.workflow_middleware, handler, workflow.wrap)
        #(wrapped.name, wrapped.run)
      }),
    )
  let activities =
    dict.from_list(
      list.map(config.activities, fn(handler) {
        let wrapped =
          list.fold_right(config.activity_middleware, handler, activity.wrap)
        #(wrapped.name, wrapped)
      }),
    )
  case config.workflows {
    [] -> Nil
    _ -> {
      process.spawn_unlinked(fn() {
        case
          poller.workflow_loop(
            nif,
            workflows,
            config.task_queue,
            config.outbound,
            False,
          )
        {
          Ok(_) -> Nil
          Error(message) ->
            io.println_error("temporal: workflow polling failed: " <> message)
        }
      })
      Nil
    }
  }
  case config.activities {
    [] -> Nil
    _ -> {
      process.spawn_unlinked(fn() {
        poller.activity_loop(nif, activities, config.task_queue)
      })
      Nil
    }
  }
  Worker(nif)
}

/// Stop polling and wait, up to ten seconds, for tasks in flight to finish.
pub fn stop(worker: Worker) -> Nil {
  bridge.worker_shutdown(worker.nif)
}
