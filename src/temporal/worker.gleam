//// Run workflows and activities by polling a task queue.

import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
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
    max_concurrent_nexus_tasks: Int,
    /// How many requests for each kind of task the worker keeps open with the server.
    max_concurrent_workflow_task_polls: Int,
    max_concurrent_activity_task_polls: Int,
    max_concurrent_nexus_task_polls: Int,
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
    max_concurrent_nexus_tasks: 100,
    max_concurrent_workflow_task_polls: 10,
    max_concurrent_activity_task_polls: 10,
    max_concurrent_nexus_task_polls: 10,
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

/// How many Nexus tasks this worker handles at once.
pub fn with_max_concurrent_nexus_tasks(config: Config, max: Int) -> Config {
  Config(..config, max_concurrent_nexus_tasks: max)
}

/// How many requests for workflow tasks the worker keeps open with the server, 10 by
/// default. Each returns one task, so this bounds how fast the worker can take them:
/// raise it when workflow tasks wait in the queue while the worker has slots free, which
/// shows first where a round trip to the server is slow. It is never more than
/// `max_concurrent_workflow_tasks`, and at least 2 for a worker that caches workflows.
pub fn with_max_concurrent_workflow_task_polls(
  config: Config,
  max: Int,
) -> Config {
  Config(..config, max_concurrent_workflow_task_polls: max)
}

/// How many requests for activity tasks the worker keeps open with the server, 10 by
/// default and never more than `max_concurrent_activities`. Raise it when activities are
/// short and wait in the queue while the worker has slots free.
pub fn with_max_concurrent_activity_task_polls(
  config: Config,
  max: Int,
) -> Config {
  Config(..config, max_concurrent_activity_task_polls: max)
}

/// How many requests for Nexus tasks the worker keeps open with the server, 10 by default
/// and never more than `max_concurrent_nexus_tasks`. Raise it when Nexus requests wait
/// while the worker has slots free.
pub fn with_max_concurrent_nexus_task_polls(
  config: Config,
  max: Int,
) -> Config {
  Config(..config, max_concurrent_nexus_task_polls: max)
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
///
/// A worker outlasts an outage of the service or the network: polling is retried for as
/// long as it takes. If the service answers polls with an error that retrying does not
/// cure for over a minute, the worker logs it and registers afresh, again and again until
/// that succeeds.
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
    config: Config,
    /// The process that started the worker.
    parent: Pid,
    events: process.Selector(Event),
    /// Where a loop reports that polling failed.
    failures: Subject(Event),
    nif: bridge.NifWorker,
    /// How many times the worker has registered with core. A loop reports a failed poll
    /// with the registration it polled for, so that a late report about an earlier one is
    /// not taken for news about this one.
    registration: Int,
    /// What each running loop is, by its process.
    loops: Dict(Pid, Loop),
    /// When recent restarts happened, in milliseconds, newest first.
    restarts: List(Int),
  )
}

type Loop {
  /// `run` returns when the worker shuts down, and with an error when polling fails.
  Loop(name: String, run: fn() -> Result(Nil, String))
}

type Event {
  Control(Control)
  Exited(process.ExitMessage)
  PollingFailed(registration: Int, loop: String, message: String)
}

const max_restarts = 5

const restart_window_ms = 60_000

/// The longest wait between attempts to register again after polling failed.
const max_register_wait_ms = 30_000

@external(erlang, "temporal_ffi", "system_time_ms")
fn now_ms() -> Int

@external(erlang, "erlang", "exit")
fn exit(reason: Dynamic) -> Nil

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
      let failures = process.new_subject()
      let events =
        process.new_selector()
        |> process.select_map(control, Control)
        |> process.select(failures)
        |> process.select_trapped_exits(Exited)
      let manager =
        start_loops(
          Manager(config, parent, events, failures, nif, 1, dict.new(), []),
        )
      process.send(started, Ok(control))
      supervise(manager)
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

fn start_loops(manager: Manager) -> Manager {
  let loops =
    loops(manager.config, manager.nif)
    |> list.map(fn(loop) { #(start_loop(manager, loop), loop) })
    |> dict.from_list
  Manager(..manager, loops:)
}

fn start_loop(manager: Manager, loop: Loop) -> Pid {
  let Manager(failures:, registration:, ..) = manager
  process.spawn(fn() {
    case loop.run() {
      Ok(Nil) -> Nil
      Error(message) ->
        process.send(failures, PollingFailed(registration, loop.name, message))
    }
  })
}

/// Stop for good, as the process that started the worker, or its supervisor, goes away.
fn leave(reason: process.ExitReason) -> Nil {
  // A supervisor that asks its child to shut down expects it to leave with the reason it
  // was asked with, and reports anything else as an error.
  case reason {
    process.Abnormal(asked) -> exit(asked)
    _ -> Nil
  }
}

fn supervise(manager: Manager) -> Nil {
  case process.selector_receive_forever(manager.events) {
    Control(Stop(reply)) -> {
      bridge.worker_shutdown(manager.nif)
      process.send(reply, Nil)
    }
    PollingFailed(registration:, ..) if registration != manager.registration ->
      supervise(manager)
    // Core gives up on polling only after retrying for as long as it thinks worthwhile,
    // and then shuts its worker down: this one will never take another task. Carrying
    // on needs a new registration.
    PollingFailed(loop:, message:, ..) -> {
      report(config_error(
        loop
        <> " polling on task queue \""
        <> manager.config.task_queue
        <> "\" failed, registering the worker again: "
        <> message,
      ))
      bridge.worker_shutdown(manager.nif)
      // The old loops end by themselves, and are no longer this worker's to restart.
      register_again(Manager(..manager, loops: dict.new()), 1000)
    }
    Exited(process.ExitMessage(pid, reason)) ->
      case dict.get(manager.loops, pid), reason {
        Error(Nil), _ if pid == manager.parent -> {
          bridge.worker_shutdown(manager.nif)
          leave(reason)
        }
        // Something else that was linked to this process.
        Error(Nil), _ -> supervise(manager)
        // A loop ends normally when the worker is shutting down, or after reporting
        // that polling failed.
        Ok(_), process.Normal ->
          supervise(Manager(..manager, loops: dict.delete(manager.loops, pid)))
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
              report(config_error(
                "giving up on task queue \""
                <> manager.config.task_queue
                <> "\": its polling loops keep crashing",
              ))
              panic as "temporal worker gave up: its polling loops keep crashing"
            }
            False ->
              supervise(
                Manager(
                  ..manager,
                  loops: manager.loops
                    |> dict.delete(pid)
                    |> dict.insert(start_loop(manager, loop), loop),
                  restarts: [now, ..recent],
                ),
              )
          }
        }
      }
  }
}

/// Register with core afresh and start polling again. While the service cannot be reached
/// this fails, so it is tried for as long as it takes, less and less often, each failure
/// logged: the worker should be working again as soon as the service is.
fn register_again(manager: Manager, wait_ms: Int) -> Nil {
  case register(manager.config, 30) {
    Ok(nif) -> {
      bridge.log(
        bridge.Info,
        config_error(
          "registered again and polling task queue \""
          <> manager.config.task_queue
          <> "\"",
        ),
        [],
      )
      let manager =
        Manager(..manager, nif:, registration: manager.registration + 1)
      supervise(start_loops(manager))
    }
    Error(reason) -> {
      report(config_error(
        "could not register on task queue \""
        <> manager.config.task_queue
        <> "\", trying again in "
        <> int.to_string(wait_ms)
        <> "ms: "
        <> reason,
      ))
      wait_to_register(
        manager,
        now_ms() + wait_ms,
        int.min(wait_ms * 2, max_register_wait_ms),
      )
    }
  }
}

/// Wait until `until`, still answering `stop` and the exit of the worker's owner. The
/// worker in core is already shut down.
fn wait_to_register(manager: Manager, until: Int, next_wait_ms: Int) -> Nil {
  let remaining = int.max(until - now_ms(), 0)
  case process.selector_receive(manager.events, remaining) {
    Error(Nil) -> register_again(manager, next_wait_ms)
    Ok(Control(Stop(reply))) -> process.send(reply, Nil)
    Ok(Exited(process.ExitMessage(pid, reason))) if pid == manager.parent ->
      leave(reason)
    Ok(_) -> wait_to_register(manager, until, next_wait_ms)
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
    max_concurrent_nexus_tasks: config.max_concurrent_nexus_tasks,
    max_concurrent_workflow_task_polls: config.max_concurrent_workflow_task_polls,
    max_concurrent_activity_task_polls: config.max_concurrent_activity_task_polls,
    max_concurrent_nexus_task_polls: config.max_concurrent_nexus_task_polls,
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

/// The polling loops this worker needs.
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
      poller.workflow_loop(
        nif,
        workflows,
        config.task_queue,
        config.outbound,
        codec,
        False,
      )
      |> result.replace(Nil)
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
