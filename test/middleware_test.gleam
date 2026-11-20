//// Outbound middleware that needs the workflow's context: a rename shipped behind a patch,
//// as an interceptor in another SDK would do it.

import gleam/int
import gleam/result
import gleam/string
import gleam/time/duration
import support.{type Env}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/middleware
import temporal/replayer
import temporal/worker
import temporal/workflow.{type Context}
import temporal/workflow/child
import temporal/workflow/local_activity

fn greet() {
  activity.Definition("greet", codec.string(), codec.string())
}

fn greet_v2() {
  activity.Definition("greet_v2", codec.string(), codec.string())
}

fn count() {
  workflow.Definition("count", codec.string(), codec.int())
}

fn greeter() {
  workflow.Definition("greeter", codec.string(), codec.string())
}

/// Greets with an activity, with a local activity, and counts with a child workflow.
fn greeter_workflow(ctx: Context, name: String) -> Result(String, Failure) {
  let options = workflow.activity_options(start_to_close: duration.seconds(10))
  use greeting <- result.try(workflow.execute_activity(
    ctx,
    greet(),
    name,
    options,
  ))
  use again <- result.try(local_activity.execute(
    ctx,
    greet(),
    name,
    local_activity.options(start_to_close: duration.seconds(10)),
  ))
  use length <- result.try(child.execute(
    ctx,
    count(),
    name,
    child.options(workflow_id: workflow.info(ctx).workflow_id <> "-count"),
  ))
  Ok(greeting <> "; " <> again <> "; " <> int.to_string(length))
}

/// Sends `greet` to its successor, for runs that started after the change shipped.
fn rename_behind_a_patch(
  ctx: Context,
  call: middleware.ActivityCall,
) -> middleware.ActivityCall {
  case call.activity_type {
    "greet" ->
      case workflow.patched(ctx, "greet-v2") {
        True -> middleware.with_activity_type(call, "greet_v2")
        False -> call
      }
    _ -> call
  }
}

/// Starts children under an id that says which run of the parent they belong to.
fn tag_children(
  ctx: Context,
  call: middleware.ChildCall,
) -> middleware.ChildCall {
  middleware.with_child_workflow_id(
    call,
    call.workflow_id <> "-of-" <> workflow.info(ctx).workflow_type,
  )
}

fn workflows() -> List(workflow.Handler) {
  [
    workflow.handler(greeter(), greeter_workflow),
    workflow.handler(count(), fn(_, text) { Ok(string.length(text)) }),
  ]
}

fn configure(config: worker.Config) -> worker.Config {
  config
  |> worker.with_workflows(workflows())
  |> worker.with_activities([
    activity.handler(greet(), fn(_, name) { Ok("hello " <> name) }),
    activity.handler(greet_v2(), fn(_, name) { Ok("good day " <> name) }),
  ])
}

fn run_greeter(env: Env) -> client.History {
  let assert Ok(handle) =
    client.start(env.client, greeter(), "Ada", support.start_options(env))
  let assert Ok(_) = client.result(handle)
  let assert Ok(history) = client.history(handle)
  history
}

pub fn an_outbound_rewrite_can_patch_test() {
  // A run from before the middleware existed: it scheduled `greet`.
  let before = {
    let task_queue = "outbound-before-" <> support.unique()
    let client = support.local_client()
    let assert Ok(worker) =
      worker.new(client, task_queue) |> configure |> worker.start
    let env = support.Env(client, task_queue)
    let assert Ok(handle) =
      client.start(client, greeter(), "Ada", support.start_options(env))
    assert client.result(handle) == Ok("hello Ada; hello Ada; 3")
    worker.stop(worker)
    let assert Ok(history) = client.history(handle)
    history
  }
  let with_middleware =
    replayer.options()
    |> replayer.with_outbound_activity(rename_behind_a_patch)
    |> replayer.with_outbound_child(tag_children)

  use env <- support.in_both_modes(
    "outbound-patched",
    support.local_client(),
    fn(config) {
      configure(config)
      |> worker.with_outbound_activity(rename_behind_a_patch)
      |> worker.with_outbound_child(tag_children)
    },
  )
  let assert Ok(handle) =
    client.start(env.client, greeter(), "Ada", support.start_options(env))
  // New runs take the patch: the activity and the local activity are both renamed.
  assert client.result(handle) == Ok("good day Ada; good day Ada; 3")
  let history = run_greeter(env)
  assert replayer.replay_with(workflows(), history, with_middleware) == Ok(Nil)
  // Without the middleware the same code schedules `greet` and no patch marker.
  let assert Error(replayer.Nondeterminism(_)) =
    replayer.replay(workflows(), history)
  // The run from before replays through the middleware: `patched` is false for it, and
  // the old activity type stands.
  assert replayer.replay_with(
      workflows(),
      before,
      replayer.options()
        |> replayer.with_outbound_activity(rename_behind_a_patch),
    )
    == Ok(Nil)
}
