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
import temporal/nexus
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
    nexus_handlers: List(nexus.Handler),
    deployment: Option(Deployment),
  )
}

/// The version of a worker deployment this worker belongs to. Workers that run the same
/// code share a build id; the server routes each workflow to the build it should run on.
pub type Deployment {
  Deployment(
    name: String,
    build_id: String,
    /// What workflows that do not declare a behaviour with `workflow.with_versioning` do.
    default_behavior: workflow.VersioningBehavior,
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
    nexus_handlers: [],
    deployment: None,
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

/// Serve Nexus operations from this worker's task queue. A Nexus endpoint registered on the
/// server must point at the queue.
pub fn with_nexus_handlers(
  config: Config,
  handlers: List(nexus.Handler),
) -> Config {
  Config(..config, nexus_handlers: list.append(config.nexus_handlers, handlers))
}

/// Run as one version of a worker deployment, so the server routes workflows by build.
/// Tasks reach this worker only once its version is the deployment's current or ramping
/// version, or for workflows pinned to it; set that with the Temporal CLI or UI.
pub fn with_deployment(config: Config, deployment: Deployment) -> Config {
  Config(..config, deployment: Some(deployment))
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
      enable_nexus: config.nexus_handlers != [],
      deployment: option.map(config.deployment, fn(deployment) {
        bridge.Deployment(
          name: deployment.name,
          build_id: deployment.build_id,
          use_versioning: True,
          default_behavior: case deployment.default_behavior {
            workflow.Pinned -> 1
            workflow.AutoUpgrade -> 2
          },
        )
      }),
    ),
  ))
  let codec = client.payload_codec(config.client)
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
            codec,
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
  // The activity poller also delivers local activities, which core issues to any worker
  // that runs workflows.
  case config.activities, config.workflows {
    [], [] -> Nil
    _, _ -> {
      process.spawn_unlinked(fn() {
        poller.activity_loop(nif, activities, config.task_queue, codec)
      })
      Nil
    }
  }
  case config.nexus_handlers {
    [] -> Nil
    handlers -> {
      let dispatch = nexus.dispatcher(handlers, config.client)
      process.spawn_unlinked(fn() { poller.nexus_loop(nif, dispatch) })
      Nil
    }
  }
  Worker(nif)
}

/// Stop polling and wait, up to ten seconds, for tasks in flight to finish.
pub fn stop(worker: Worker) -> Nil {
  bridge.worker_shutdown(worker.nif)
}
