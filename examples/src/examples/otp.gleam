//// OTP: run a worker under a supervisor, and shut it down gracefully.
////
//// `worker.start` links the worker to the calling process, which suits scripts and tests.
//// A service adds `worker.supervised` to its supervision tree instead: the supervisor
//// restarts the worker if it gives up, and stops it in order when the application stops.
////
//// Stopping is graceful. The worker stops polling, activities in flight see a
//// `WorkerShutdown` cancellation and get the shutdown grace to finish, and whatever they
//// return in that time is reported. Work that is not finished is not lost: the server
//// hands it to another worker when its timeout passes.

import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/static_supervisor.{type Supervisor}
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow

const task_queue = "examples-otp"

pub fn render() {
  workflow.Definition("render", codec.string(), codec.string())
}

pub fn render_page() {
  activity.Definition("render_page", codec.string(), codec.string())
}

fn render_workflow(
  ctx: workflow.Context,
  page: String,
) -> Result(String, Failure) {
  let options = workflow.activity_options(start_to_close: duration.seconds(30))
  workflow.execute_activity(ctx, render_page(), page, options)
}

/// The worker's configuration: everything `worker.start` would take.
fn worker_config(
  client: Client,
  started: process.Subject(String),
) -> worker.Config {
  worker.new(client, task_queue)
  |> worker.with_workflows([workflow.handler(render(), render_workflow)])
  |> worker.with_activities([
    activity.handler(render_page(), fn(_ctx, page) {
      process.send(started, page)
      // Stands in for work that takes a while.
      process.sleep(500)
      Ok("rendered " <> page)
    }),
  ])
  // How long stopping waits for activities in flight. The default is ten seconds.
  |> worker.with_shutdown_grace(duration.seconds(20))
}

/// A supervisor with the worker as its child. In an application this is the tree started
/// from the application's `start` function, with the rest of the service beside the
/// worker.
pub fn start_supervisor(
  client: Client,
  started: process.Subject(String),
) -> Result(actor.Started(Supervisor), actor.StartError) {
  static_supervisor.new(static_supervisor.OneForOne)
  |> static_supervisor.add(worker.supervised(worker_config(client, started)))
  |> static_supervisor.start
}

/// Stop the supervisor and wait until it is down. A supervisor stops when its parent, here
/// the calling process, tells it to, and stops the worker first, giving it its grace
/// period. In an application the VM does this as it shuts down.
fn stop_supervisor(supervisor: actor.Started(Supervisor)) -> Nil {
  let monitor = process.monitor(supervisor.pid)
  process.send_exit(supervisor.pid)
  let down =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
  process.selector_receive_forever(down)
}

/// Starts a workflow, stops the worker while its activity is running, and starts a new
/// worker, as a deploy would. Returns the workflow's result and how many times the
/// activity ran: once, because the first worker let it finish.
pub fn run(client: Client) -> Result(#(String, Int), client.Error) {
  let started = process.new_subject()
  let assert Ok(supervisor) = start_supervisor(client, started)
  let options = client.start_options(workflow_id: "examples-otp", task_queue:)
  let handle = client.start(client, render(), "home", options)

  // Wait until the activity is running, then shut down.
  let assert Ok(_) = process.receive(started, 10_000)
  stop_supervisor(supervisor)

  // The next deploy.
  let assert Ok(supervisor) = start_supervisor(client, started)
  let outcome = {
    use handle <- result.try(handle)
    use rendered <- result.try(client.result(handle))
    Ok(#(rendered, 1 + count(started, 0)))
  }
  stop_supervisor(supervisor)
  outcome
}

fn count(started: process.Subject(String), seen: Int) -> Int {
  case process.receive(started, 0) {
    Ok(_) -> count(started, seen + 1)
    Error(Nil) -> seen
  }
}
