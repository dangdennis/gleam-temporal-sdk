//// Run workflows and activities by polling a task queue.

import gleam/dict.{type Dict}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/string
import gleam/time/duration.{type Duration}
import temporal/activity
import temporal/client.{type Client}
import temporal/internal/bridge
import temporal/internal/poller
import temporal/middleware.{
  type ActivityCall, type ChildCall, type Outbound, Outbound,
}
import temporal/nexus
import temporal/workflow

/// Everything a worker runs with. Build with `new` and the `with_` functions.
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
    /// How long a cancelled activity gets to stop by itself before its process is killed
    /// and it is reported as cancelled. `None` never kills.
    activity_cancellation_grace: Option(Duration),
    /// How long stopping the worker waits for activities in flight.
    shutdown_grace: Duration,
  )
}

/// The version of a worker deployment this worker belongs to. Workers that run the same
/// code share a build id; the server routes each workflow to the build it should run on.
pub type Deployment {
  Deployment(
    /// Shared by every build of the same service.
    name: String,
    /// Names one build of its code.
    build_id: String,
    /// What workflows that do not declare a behaviour with `workflow.with_versioning` do.
    default_behavior: workflow.VersioningBehavior,
  )
}

/// A running worker.
pub opaque type Worker {
  Worker(manager: Pid, control: Subject(Control))
}

type Control {
  Stop(reply: Subject(Nil))
}

/// A worker for one task queue, with nothing registered yet.
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
    activity_cancellation_grace: Some(duration.seconds(30)),
    shutdown_grace: duration.seconds(10),
  )
}

/// Register workflows. A task for a workflow type the worker does not know fails and is
/// retried, so the workflow waits for a worker that does.
pub fn with_workflows(
  config: Config,
  workflows: List(workflow.Handler),
) -> Config {
  Config(..config, workflows: list.append(config.workflows, workflows))
}

/// Register activities.
pub fn with_activities(
  config: Config,
  activities: List(activity.Handler),
) -> Config {
  Config(..config, activities: list.append(config.activities, activities))
}

/// How many workflow runs to keep in memory between tasks. See `Config`.
pub fn with_max_cached_workflows(config: Config, max: Int) -> Config {
  Config(..config, max_cached_workflows: max)
}

/// How many workflow tasks this worker processes at once.
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

/// Kill an activity that is still running this long after it was asked to stop, and report
/// it as cancelled. The default is thirty seconds. An activity that checks
/// `activity.is_cancelled` stops well within that; the limit is for one that is stuck, which
/// would otherwise hold its slot on the worker for good. `None` never kills.
pub fn with_activity_cancellation_grace(
  config: Config,
  grace: Option(Duration),
) -> Config {
  Config(..config, activity_cancellation_grace: grace)
}

/// How long `stop` waits for activities in flight, ten seconds by default. When the
/// worker is asked to stop, running activities see a `WorkerShutdown` cancellation at
/// once, so that they can finish or checkpoint; whatever is still running when the time is
/// up is abandoned, and the server gives it to another worker when its timeout passes.
pub fn with_shutdown_grace(config: Config, grace: Duration) -> Config {
  Config(..config, shutdown_grace: grace)
}

/// Start the worker, linked to the calling process. Returns once the server has accepted
/// it. A worker with no workflows polls only for activities, and the other way round.
///
/// The worker restarts a polling loop that crashes. If loops keep crashing, more than five
/// times in a minute, the worker gives up and exits abnormally, which takes the calling
/// process with it unless it traps exits: a worker that cannot work should not look
/// healthy. Under a supervisor, use `supervised` instead.
pub fn start(config: Config) -> Result(Worker, client.Error) {
  let started = process.new_subject()
  let parent = process.self()
  let manager = process.spawn(fn() { manage(config, parent, started) })
  case process.receive_forever(started) {
    Ok(control) -> Ok(Worker(manager, control))
    Error(reason) -> Error(client.ConnectionFailed(reason))
  }
}

/// A child specification for running the worker under a `gleam/otp` supervisor, which
/// restarts it if it gives up.
pub fn supervised(config: Config) -> supervision.ChildSpecification(Worker) {
  supervision.worker(fn() {
    case start(config) {
      Ok(worker) -> Ok(actor.Started(worker.manager, worker))
      Error(error) -> Error(actor.InitFailed(string.inspect(error)))
    }
  })
  // Stopping waits for tasks in flight, which is longer than a supervisor waits by default.
  |> supervision.timeout(
    duration.to_milliseconds(config.shutdown_grace) + 10_000,
  )
}

@internal
pub fn manager(worker: Worker) -> Pid {
  worker.manager
}

/// Stop polling and wait for tasks in flight to finish, for as long as the worker's
/// shutdown grace allows.
pub fn stop(worker: Worker) -> Nil {
  case process.is_alive(worker.manager) {
    True -> process.call_forever(worker.control, Stop)
    False -> Nil
  }
}

// ---- the manager process ----

type Manager {
  Manager(
    nif: bridge.NifWorker,
    /// What each running loop is, by its process.
    loops: Dict(Pid, Loop),
    /// When recent restarts happened, in milliseconds, newest first.
    restarts: List(Int),
  )
}

type Loop {
  Loop(name: String, run: fn() -> Nil)
}

type Event {
  Control(Control)
  Exited(process.ExitMessage)
}

const max_restarts = 5

const restart_window_ms = 60_000

@external(erlang, "temporal_ffi", "system_time_ms")
fn now_ms() -> Int

fn manage(
  config: Config,
  parent: Pid,
  started: Subject(Result(Subject(Control), String)),
) -> Nil {
  process.trap_exits(True)
  case register(config, 30) {
    Error(reason) -> process.send(started, Error(reason))
    Ok(nif) -> {
      let control = process.new_subject()
      let loops =
        loops(config, nif)
        |> list.map(fn(loop) { #(process.spawn(loop.run), loop) })
        |> dict.from_list
      process.send(started, Ok(control))
      let events =
        process.new_selector()
        |> process.select_map(control, Control)
        |> process.select_trapped_exits(Exited)
      supervise(Manager(nif, loops, []), parent, events)
    }
  }
}

/// Register with core. A worker that replaces one that just died may be refused while the
/// old one is still letting go of the task queue, so that refusal alone is retried.
fn register(config: Config, attempts: Int) -> Result(bridge.NifWorker, String) {
  let registered =
    bridge.worker_new(
      client.connection(config.client),
      config.task_queue,
      worker_options(config),
    )
  case registered {
    Error(reason) if attempts > 0 ->
      case string.contains(reason, "overlapping worker task types") {
        True -> {
          process.sleep(500)
          register(config, attempts - 1)
        }
        False -> registered
      }
    _ -> registered
  }
}

fn supervise(
  manager: Manager,
  parent: Pid,
  events: process.Selector(Event),
) -> Nil {
  case process.selector_receive_forever(events) {
    Control(Stop(reply)) -> {
      bridge.worker_shutdown(manager.nif)
      process.send(reply, Nil)
    }
    Exited(process.ExitMessage(pid, reason)) ->
      case dict.get(manager.loops, pid), reason {
        // The process that started the worker, or its supervisor, is going away.
        Error(Nil), _ if pid == parent -> bridge.worker_shutdown(manager.nif)
        // Something else that was linked to this process.
        Error(Nil), _ -> supervise(manager, parent, events)
        // A loop ends normally only when the worker is shutting down.
        Ok(_), process.Normal ->
          supervise(
            Manager(..manager, loops: dict.delete(manager.loops, pid)),
            parent,
            events,
          )
        Ok(loop), _ -> {
          let now = now_ms()
          let recent =
            list.filter(manager.restarts, fn(at) {
              now - at < restart_window_ms
            })
          report(config_error(
            loop.name <> " loop crashed: " <> string.inspect(reason),
          ))
          case list.length(recent) >= max_restarts {
            True -> {
              bridge.worker_shutdown(manager.nif)
              panic as "temporal worker gave up: its polling loops keep crashing"
            }
            False ->
              supervise(
                Manager(
                  ..manager,
                  loops: manager.loops
                    |> dict.delete(pid)
                    |> dict.insert(process.spawn(loop.run), loop),
                  restarts: [now, ..recent],
                ),
                parent,
                events,
              )
          }
        }
      }
  }
}

fn config_error(message: String) -> String {
  "temporal worker: " <> message
}

fn report(message: String) -> Nil {
  bridge.log(bridge.Error, message, [])
}

fn worker_options(config: Config) -> bridge.WorkerOptions {
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
    shutdown_grace_ms: duration.to_milliseconds(config.shutdown_grace),
  )
}

/// The polling loops this worker needs. Each returns when the worker shuts down and
/// crashes if polling fails.
fn loops(config: Config, nif: bridge.NifWorker) -> List(Loop) {
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
  let workflow_loop =
    Loop("workflow", fn() {
      let assert Ok(_) =
        poller.workflow_loop(
          nif,
          workflows,
          config.task_queue,
          config.outbound,
          codec,
          False,
        )
      Nil
    })
  // The activity poller also delivers local activities, which core issues to any worker
  // that runs workflows.
  let activity_loop =
    Loop("activity", fn() {
      poller.activity_loop(
        nif,
        activities,
        config.task_queue,
        codec,
        option.map(config.activity_cancellation_grace, duration.to_milliseconds),
      )
    })
  let nexus_loop =
    Loop("nexus", fn() {
      poller.nexus_loop(
        nif,
        nexus.dispatcher(config.nexus_handlers, config.client),
      )
    })
  list.flatten([
    case config.workflows {
      [] -> []
      _ -> [workflow_loop]
    },
    case config.activities, config.workflows {
      [], [] -> []
      _, _ -> [activity_loop]
    },
    case config.nexus_handlers {
      [] -> []
      _ -> [nexus_loop]
    },
  ])
}
