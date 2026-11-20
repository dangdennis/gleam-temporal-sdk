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
import temporal/middleware.{type ActivityCall, type ChildCall, type Outbound}
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
    /// How long activities in flight carry on undisturbed after the worker is asked to
    /// stop, before they are told to cancel.
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

/// Rewrite every activity a workflow schedules, local activities included: for example
/// to rename an activity type. `rewrite` is given the context the workflow scheduled the
/// activity with, so it can call `workflow.patched` and the rest of `temporal/workflow`;
/// change the call with the `with_activity_` functions of `temporal/middleware`, which
/// also has the rules such a function must follow. A rewrite added later sees what the
/// earlier ones made of the call.
pub fn with_outbound_activity(
  config: Config,
  rewrite: fn(workflow.Context, ActivityCall) -> ActivityCall,
) -> Config {
  Config(
    ..config,
    outbound: middleware.add_activity(config.outbound, fn(scope, call) {
      rewrite(workflow.context_of(scope), call)
    }),
  )
}

/// Rewrite every child workflow a workflow starts. See `with_outbound_activity`.
pub fn with_outbound_child(
  config: Config,
  rewrite: fn(workflow.Context, ChildCall) -> ChildCall,
) -> Config {
  Config(
    ..config,
    outbound: middleware.add_child(config.outbound, fn(scope, call) {
      rewrite(workflow.context_of(scope), call)
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
/// would otherwise hold its slot on the worker for good.
///
/// An activity is asked to stop when its workflow cancels it, when the worker's shutdown
/// grace runs out, and when one of its timeouts has passed: an activity that hangs is
/// killed this long after its start-to-close or heartbeat timeout, plus the five seconds
/// the worker allows the server's clock.
///
/// `None` never kills. `stop` then waits for as long as the slowest activity takes.
pub fn with_activity_cancellation_grace(
  config: Config,
  grace: Option(Duration),
) -> Config {
  Config(..config, activity_cancellation_grace: grace)
}

/// How long activities in flight carry on undisturbed once the worker is asked to stop,
/// ten seconds by default. Set it to what a typical activity needs to finish. See `stop`
/// for everything that happens, in order.
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
///
/// When the supervisor shuts down, the worker stops as `stop` describes, and the
/// supervisor waits for it: the shutdown grace, the activity cancellation grace and
/// fifteen seconds more. With no activity cancellation grace it waits for thirty seconds
/// in its place and then kills the worker.
///
/// The VM answers SIGTERM with `init:stop()`, which shuts down supervisors that belong to
/// an OTP application and kills every other process. A worker supervised under an
/// application's supervision tree therefore drains on SIGTERM as it is; anywhere else, use
/// `run` or `await_shutdown_signal`.
pub fn supervised(config: Config) -> supervision.ChildSpecification(Worker) {
  supervision.worker(fn() {
    case start(config) {
      Ok(worker) -> Ok(actor.Started(worker.manager, worker))
      Error(error) -> Error(actor.InitFailed(string.inspect(error)))
    }
  })
  // Stopping waits for tasks in flight, which is longer than a supervisor waits by default.
  |> supervision.timeout(case stop_limit_ms(config) {
    0 ->
      duration.to_milliseconds(config.shutdown_grace)
      + 30_000
      + stop_margin_ms
      + loops_margin_ms
    limit -> limit + loops_margin_ms
  })
}

/// Start the worker and run it until the operating system asks the program to stop with
/// SIGTERM, which is what `docker stop`, Kubernetes and systemd send. Then stop it
/// gracefully, as `stop` describes, and return. This is how to run a worker in production:
///
/// ```gleam
/// pub fn main() {
///   let assert Ok(client) = client.connect(client.local())
///   let assert Ok(Nil) =
///     worker.new(client, "orders")
///     |> worker.with_workflows([...])
///     |> worker.run
/// }
/// ```
///
/// Without this, SIGTERM stops the VM within milliseconds and every activity in flight is
/// abandoned until its timeout. The process manager has to allow the time a graceful stop
/// takes: the shutdown grace plus the activity cancellation grace at most.
///
/// For several workers in one program, use `start`, `await_shutdown_signal` and `stop`.
pub fn run(config: Config) -> Result(Nil, client.Error) {
  // Before the worker starts, so that a signal during start-up is not lost.
  bridge.trap_shutdown_signal()
  use worker <- result.map(start(config))
  await_shutdown_signal()
  stop(worker)
}

/// Block until the operating system asks the program to stop with SIGTERM, for programs
/// that stop their workers themselves:
///
/// ```gleam
/// let assert Ok(orders) = worker.start(orders_config)
/// let assert Ok(billing) = worker.start(billing_config)
/// worker.await_shutdown_signal()
/// worker.stop(orders)
/// worker.stop(billing)
/// ```
///
/// The first call takes SIGTERM over from the VM, which would otherwise stop at once; from
/// then on the program is responsible for exiting. Once the signal has arrived every call
/// returns immediately, and further SIGTERMs change nothing. Other signals keep their
/// usual effect. SIGINT cannot be waited for: the VM does not hand it to Erlang code.
pub fn await_shutdown_signal() -> Nil {
  bridge.await_shutdown_signal()
}

@internal
pub fn manager(worker: Worker) -> Pid {
  worker.manager
}

/// Stop the worker gracefully and return once it has stopped: no activity of the worker
/// is still running and nothing more will be reported to the server. In order:
///
/// 1. The worker stops asking for tasks, and `activity.is_worker_shutting_down` turns
///    true in every running activity.
/// 2. Workflow tasks in flight are finished. Workflows are not harmed by a worker
///    stopping: another worker carries them on.
/// 3. Activities in flight keep running for the shutdown grace
///    (`with_shutdown_grace`, ten seconds by default). One that finishes in that time is
///    reported as usual.
/// 4. Activities still running are then told to cancel, with the reason `WorkerShutdown`:
///    see `activity.cancel_reason`. One that accepts, by returning
///    `Error(failure.cancelled())`, is reported to the server as failed by the shutdown,
///    and retried on another worker by its retry policy. One that finishes its work
///    instead is reported with its result.
/// 5. Activities still running the activity cancellation grace later
///    (`with_activity_cancellation_grace`, thirty seconds by default) are killed, and
///    reported and retried the same way.
///
/// So `stop` takes at most the two graces and a little more, and usually as long as the
/// slowest activity. The same happens when the process that started the worker exits, or
/// its supervisor shuts down.
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
    /// What the worker's activities read to learn that it is shutting down.
    shutting_down: bridge.Flag,
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

/// How long stopping waits for core beyond the two grace periods.
const stop_margin_ms = 10_000

/// How long stopping waits for the polling loops to end once core has shut down.
const loops_margin_ms = 5000

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
          Manager(
            config:,
            parent:,
            events:,
            failures:,
            nif:,
            shutting_down: bridge.new_flag(),
            registration: 1,
            loops: dict.new(),
            restarts: [],
          ),
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
    loops(manager.config, manager.nif, manager.shutting_down)
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
      shut_down(manager)
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
      // Core has stopped this worker's polling already. Its activities are told so, and
      // given the shutdown grace to finish before the worker registers afresh.
      bridge.raise_flag(manager.shutting_down)
      bridge.worker_shutdown(
        manager.nif,
        duration.to_milliseconds(manager.config.shutdown_grace) + 5000,
      )
      // The old loops end by themselves, and are no longer this worker's to restart.
      register_again(
        Manager(..manager, loops: dict.new(), shutting_down: bridge.new_flag()),
        1000,
      )
    }
    Exited(process.ExitMessage(pid, reason)) ->
      case dict.get(manager.loops, pid), reason {
        Error(Nil), _ if pid == manager.parent -> {
          shut_down(manager)
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
              shut_down(
                Manager(..manager, loops: dict.delete(manager.loops, pid)),
              )
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

/// Stop the worker in core and wait until it has stopped: see `stop`.
fn shut_down(manager: Manager) -> Nil {
  bridge.raise_flag(manager.shutting_down)
  bridge.worker_shutdown(manager.nif, stop_limit_ms(manager.config))
  // Core is done once every poll has been answered with a shutdown, which is what ends
  // the loops. They are given a moment to be gone before this returns.
  await_loops(manager, now_ms() + loops_margin_ms)
}

/// The longest a graceful stop can take in core: both grace periods and a margin. Zero,
/// which is no limit, when activities are never killed.
fn stop_limit_ms(config: Config) -> Int {
  case config.activity_cancellation_grace {
    None -> 0
    Some(grace) ->
      duration.to_milliseconds(config.shutdown_grace)
      + duration.to_milliseconds(grace)
      + stop_margin_ms
  }
}

fn await_loops(manager: Manager, until: Int) -> Nil {
  let remaining = int.max(until - now_ms(), 0)
  case dict.is_empty(manager.loops) {
    True -> Nil
    False ->
      // Only exits are taken, so that a request to stop that arrives now is not lost.
      case
        process.new_selector()
        |> process.select_trapped_exits(fn(exit) { exit.pid })
        |> process.selector_receive(remaining)
      {
        Ok(pid) ->
          await_loops(
            Manager(..manager, loops: dict.delete(manager.loops, pid)),
            until,
          )
        Error(Nil) -> Nil
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
fn loops(
  config: Config,
  nif: bridge.NifWorker,
  shutting_down: bridge.Flag,
) -> List(Loop) {
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
        fn() { bridge.flag_raised(shutting_down) },
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
