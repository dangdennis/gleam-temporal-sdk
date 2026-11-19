//// Options that are handed to the server or to core unchanged: each scenario shows that
//// the option has its effect, not just that it is accepted.

import gleam/dict
import gleam/erlang/process
import gleam/option.{Some}
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import support.{type Env, eventually, start_options, unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/nexus
import temporal/search_attributes
import temporal/worker
import temporal/workflow.{type Context}
import temporal/workflow/child
import temporal/workflow/operation

fn nap() {
  activity.Definition("nap", codec.int(), codec.nil())
}

fn options() {
  workflow.activity_options(start_to_close: duration.seconds(30))
}

fn stamp() {
  workflow.Definition("stamp", codec.nil(), codec.string())
}

/// Returns its attempt number, failing the first attempt.
fn stamp_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  case workflow.info(ctx).attempt {
    1 -> Error(failure.new("first attempt fails"))
    attempt -> Ok("attempt " <> string.inspect(attempt))
  }
}

fn idle() {
  workflow.Definition("idle", codec.nil(), codec.string())
}

fn idle_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  use _ <- result.map(workflow.sleep(ctx, duration.hours(1)))
  "woke"
}

fn overrunning() {
  workflow.Definition("overrunning", codec.nil(), codec.string())
}

/// An activity given two seconds in total that needs ten.
fn overrunning_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  let limited =
    workflow.ActivityOptions(
      ..options(),
      schedule_to_close: Some(duration.seconds(2)),
    )
  case workflow.execute_activity(ctx, nap(), 10_000, limited) {
    Ok(Nil) -> Ok("finished")
    Error(failure) ->
      Ok("timed out: " <> string.inspect(failure.is_timeout(failure)))
  }
}

fn nursery() {
  workflow.Definition("nursery", codec.nil(), codec.string())
}

/// Runs a child that fails its first attempt, with a retry policy.
fn nursery_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  child.execute(
    ctx,
    stamp(),
    Nil,
    child.Options(
      ..child.options(workflow_id: workflow.info(ctx).workflow_id <> "-child"),
      retry: Some(
        workflow.RetryPolicy(
          ..workflow.retry_policy(maximum_attempts: 3),
          initial_interval: duration.milliseconds(10),
        ),
      ),
    ),
  )
}

fn tagging() {
  workflow.Definition("tagging", codec.string(), codec.nil())
}

fn tagging_workflow(ctx: Context, attribute: String) -> Result(Nil, Failure) {
  workflow.upsert_search_attributes(
    ctx,
    dict.from_list([#(attribute, search_attributes.Keyword("tagged"))]),
  )
  workflow.sleep(ctx, duration.milliseconds(10))
}

fn linger() {
  workflow.NexusOperation("waiting", "linger", codec.nil(), codec.string())
}

fn calling() {
  workflow.Definition("calling", codec.string(), codec.string())
}

/// Gives a Nexus operation a deadline it cannot meet.
fn calling_workflow(ctx: Context, endpoint: String) -> Result(String, Failure) {
  let outcome =
    workflow.with_timeout(ctx, duration.seconds(1), fn(scope) {
      operation.execute(scope, endpoint, linger(), Nil, operation.options())
    })
  case outcome {
    Ok(text) -> Ok(text)
    Error(failure) ->
      Ok("gave up: " <> string.inspect(failure.is_timeout(failure)))
  }
}

fn in_both_modes(name: String, scenario: fn(Env) -> Nil) -> Nil {
  use config <- support.in_both_modes(name, support.local_client(), _, scenario)
  config
  |> worker.with_workflows([
    workflow.handler(stamp(), stamp_workflow),
    workflow.handler(idle(), idle_workflow),
    workflow.handler(overrunning(), overrunning_workflow),
    workflow.handler(nursery(), nursery_workflow),
    workflow.handler(tagging(), tagging_workflow),
    workflow.handler(calling(), calling_workflow),
  ])
  |> worker.with_activities([
    activity.handler(nap(), fn(_, ms) { Ok(process.sleep(ms)) }),
  ])
  |> worker.with_nexus_handlers([
    nexus.workflow_run(linger(), idle(), fn(ctx, _) {
      #(
        Nil,
        client.start_options(
          workflow_id: "lingering-" <> ctx.request_id,
          task_queue: config.task_queue,
        ),
      )
    }),
  ])
}

pub fn a_workflow_retry_policy_starts_a_failed_workflow_again_test() {
  use env <- in_both_modes("retried")
  let options =
    client.StartOptions(
      ..start_options(env),
      retry: Some(
        workflow.RetryPolicy(
          ..workflow.retry_policy(maximum_attempts: 3),
          initial_interval: duration.milliseconds(10),
        ),
      ),
    )
  assert client.execute(env.client, stamp(), Nil, options) == Ok("attempt 2")
}

pub fn a_start_delay_holds_the_first_task_back_test() {
  use env <- in_both_modes("delayed")
  let options =
    client.StartOptions(
      ..start_options(env),
      retry: Some(workflow.retry_policy(maximum_attempts: 2)),
      start_delay: Some(duration.seconds(2)),
    )
  let before = timestamp.system_time()
  let assert Ok(_) = client.execute(env.client, stamp(), Nil, options)
  let waited = timestamp.difference(before, timestamp.system_time())
  assert duration.to_milliseconds(waited) >= 2000
}

pub fn terminate_existing_replaces_the_running_workflow_test() {
  use env <- in_both_modes("replaced")
  let options =
    client.StartOptions(
      ..start_options(env),
      id_conflict_policy: client.TerminateExisting,
    )
  let assert Ok(first) = client.start(env.client, idle(), Nil, options)
  let assert Ok(second) = client.start(env.client, idle(), Nil, options)
  assert client.run_id(first) != client.run_id(second)
  assert client.result(first) == Error(client.WorkflowTerminated)
  let assert Ok(Nil) = client.terminate(second, "test over")
  Nil
}

pub fn an_execution_timeout_ends_the_workflow_test() {
  use env <- in_both_modes("expiring")
  let options =
    client.StartOptions(
      ..start_options(env),
      execution_timeout: Some(duration.seconds(1)),
    )
  assert client.execute(env.client, idle(), Nil, options)
    == Error(client.WorkflowTimedOut)
}

pub fn a_schedule_to_close_timeout_fails_the_activity_test() {
  use env <- in_both_modes("overrunning")
  assert client.execute(env.client, overrunning(), Nil, start_options(env))
    == Ok("timed out: True")
}

pub fn a_child_retry_policy_starts_a_failed_child_again_test() {
  use env <- in_both_modes("nursery")
  assert client.execute(env.client, nursery(), Nil, start_options(env))
    == Ok("attempt 2")
}

pub fn a_workflow_can_change_its_search_attributes_test() {
  use env <- in_both_modes("tagging")
  let attribute = support.register_search_attribute()
  let assert Ok(handle) =
    client.start(env.client, tagging(), attribute, start_options(env))
  assert client.result(handle) == Ok(Nil)
  assert eventually(fn() {
    let assert Ok(description) = client.describe(handle)
    case dict.get(description.search_attributes, attribute) {
      Ok(payload) -> payload.data == <<"\"tagged\"":utf8>>
      Error(Nil) -> False
    }
  })
}

pub fn cancelling_a_nexus_operation_cancels_the_workflow_behind_it_test() {
  use env <- in_both_modes("calling")
  let endpoint = "options-" <> unique()
  let created =
    support.cmd(
      "temporal operator nexus endpoint create --name "
      <> endpoint
      <> " --target-namespace default --target-task-queue "
      <> env.task_queue
      <> " 2>&1",
    )
  assert string.contains(created, endpoint)
  assert client.execute(env.client, calling(), endpoint, start_options(env))
    == Ok("gave up: True")
  // The workflow the operation started was cancelled, not left to run for its hour.
  let query =
    "WorkflowType = \"idle\" AND TaskQueue = \"" <> env.task_queue <> "\""
  assert eventually(fn() {
    case client.list(env.client, query, limit: 5) {
      Ok([run]) -> run.status == client.Cancelled
      _ -> False
    }
  })
}
