//// End-to-end tests of cancellation, timeouts, child policies, client options, middleware
//// and activity context, against a real Temporal service. Like `integration_test`, every
//// scenario runs with and without the workflow cache and its histories are replayed.

import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import gleam/time/duration
import support.{type Env, eventually, record, recorded, start_options, unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/middleware
import temporal/replayer
import temporal/search_attributes
import temporal/worker
import temporal/workflow.{type Context}
import temporal/workflow/child

// ---- activities ----

/// Heartbeats until cancelled or until its input, in milliseconds, has passed.
fn patient() {
  activity.Definition("patient", codec.int(), codec.string())
}

fn patient_activity(ctx: activity.Context, remaining: Int) {
  activity.heartbeat(ctx)
  case activity.cancel_reason(ctx), remaining <= 0 {
    Some(reason), _ -> {
      record(activity.info(ctx).workflow_id, "saw " <> string.inspect(reason))
      Error(failure.cancelled())
    }
    None, True -> Ok("ran to the end")
    None, False -> {
      process.sleep(50)
      patient_activity(ctx, remaining - 50)
    }
  }
}

fn note() {
  activity.Definition("note", codec.string(), codec.nil())
}

/// Records progress on its first attempt and fails; later attempts resume from it.
fn resumable() {
  activity.Definition("resumable", codec.nil(), codec.int())
}

fn resumable_activity(ctx: activity.Context, _: Nil) {
  case activity.heartbeat_details(ctx, codec.int()) {
    Some(progress) -> Ok(progress + activity.info(ctx).attempt)
    None -> {
      activity.heartbeat_with(ctx, codec.int(), 40)
      Error(failure.new("interrupted"))
    }
  }
}

fn describe_self() {
  activity.Definition("describe_self", codec.nil(), codec.string())
}

fn detailed_failure() {
  activity.Definition("detailed_failure", codec.nil(), codec.nil())
}

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn activities() -> List(activity.Handler) {
  [
    activity.handler(patient(), patient_activity),
    activity.handler(note(), fn(ctx, text) {
      Ok(record(activity.info(ctx).workflow_id, text))
    }),
    activity.handler(resumable(), resumable_activity),
    activity.handler(describe_self(), fn(ctx, _) {
      let info = activity.info(ctx)
      Ok(string.join(
        [
          info.activity_type,
          info.workflow_type,
          info.activity_id,
          int.to_string(info.attempt),
          string.inspect(option.map(
            info.start_to_close,
            duration.to_milliseconds,
          )),
          string.inspect(info.task_queue == ""),
        ],
        "/",
      ))
    }),
    activity.handler(detailed_failure(), fn(_, _) {
      Error(
        failure.new("over budget")
        |> failure.with_type("OverBudget")
        |> failure.non_retryable
        |> failure.with_details([codec.encode(codec.int(), 250)]),
      )
    }),
    // Registered under the name the outbound middleware rewrites `double` to.
    activity.handler(
      activity.Definition("double_v2", codec.int(), codec.int()),
      fn(_, n) { Ok(n * 2) },
    ),
  ]
}

fn options() {
  workflow.activity_options(start_to_close: duration.milliseconds(30_000))
}

fn heartbeating() {
  workflow.ActivityOptions(
    ..options(),
    heartbeat_timeout: Some(duration.milliseconds(1000)),
    cancellation: workflow.WaitCancellationCompleted,
  )
}

// ---- workflows ----

fn scoped() {
  workflow.Definition("scoped", codec.nil(), codec.string())
}

/// Cancels one activity through its scope and carries on.
fn scoped_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  let outcome =
    workflow.with_scope(ctx, fn(scope) {
      let pending =
        workflow.start_activity(scope, patient(), 60_000, heartbeating())
      let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(300))
      workflow.cancel(scope)
      workflow.await(pending)
    })
  let assert Error(failure) = outcome
  assert failure.is_cancellation(failure)
  assert !workflow.is_cancelled(ctx)
  let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10))
  Ok("carried on")
}

fn tidy() {
  workflow.Definition("tidy", codec.nil(), codec.string())
}

/// Waits to be cancelled, then cleans up in a shielded scope.
fn tidy_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  case workflow.execute_activity(ctx, patient(), 60_000, heartbeating()) {
    Ok(text) -> Ok(text)
    Error(cancelled) -> {
      let assert Ok(Nil) =
        workflow.non_cancellable(ctx, fn(shielded) {
          let assert Ok(Nil) =
            workflow.sleep(shielded, duration.milliseconds(10))
          workflow.execute_activity(shielded, note(), "cleaned up", options())
        })
      Error(cancelled)
    }
  }
}

fn deadline() {
  workflow.Definition("deadline", codec.int(), codec.string())
}

fn deadline_workflow(ctx: Context, ms: Int) -> Result(String, Failure) {
  let outcome =
    workflow.with_timeout(ctx, duration.milliseconds(ms), fn(scope) {
      workflow.execute_activity(scope, patient(), 4000, heartbeating())
    })
  case outcome {
    Ok(text) -> Ok(text)
    Error(failure.Failure(kind: failure.Timeout, ..)) -> Ok("timed out")
    Error(other) -> Error(other)
  }
}

fn nudge() {
  workflow.SignalDefinition("nudge", codec.string())
}

fn waiting() {
  workflow.Definition("waiting", codec.int(), codec.string())
}

fn waiting_workflow(ctx: Context, ms: Int) -> Result(String, Failure) {
  case
    workflow.await_timeout(
      ctx,
      workflow.signal(ctx, nudge()),
      duration.milliseconds(ms),
    )
  {
    Ok(text) -> Ok("nudged: " <> text)
    Error(failure.Failure(kind: failure.Timeout, ..)) -> Ok("nobody nudged")
    Error(other) -> Error(other)
  }
}

fn resuming() {
  workflow.Definition("resuming", codec.nil(), codec.int())
}

fn resuming_workflow(ctx: Context, _: Nil) -> Result(Int, Failure) {
  let retry =
    workflow.RetryPolicy(
      ..workflow.retry_policy(maximum_attempts: 3),
      initial_interval: duration.milliseconds(10),
    )
  workflow.execute_activity(
    ctx,
    resumable(),
    Nil,
    workflow.ActivityOptions(..options(), retry: Some(retry)),
  )
}

fn slow_child() {
  workflow.Definition("slow_child", codec.string(), codec.string())
}

fn slow_child_workflow(ctx: Context, name: String) -> Result(String, Failure) {
  use _ <- result.map(workflow.sleep(ctx, duration.milliseconds(300)))
  "child of " <> name
}

fn detaching() {
  workflow.Definition("detaching", codec.string(), codec.string())
}

/// Starts a child that outlives it and returns the child's run id.
fn detaching_workflow(
  ctx: Context,
  child_id: String,
) -> Result(String, Failure) {
  let options =
    child.Options(
      ..child.options(workflow_id: child_id),
      parent_close_policy: child.Abandon,
    )
  // Waiting for the child to start is what makes sure it does.
  child.start(ctx, slow_child(), workflow.info(ctx).workflow_id, options)
  |> child.started
  |> workflow.await
}

fn supervising() {
  workflow.Definition("supervising", codec.string(), codec.string())
}

/// Gives a child a deadline and reports how the child's cancellation came back.
fn supervising_workflow(
  ctx: Context,
  child_id: String,
) -> Result(String, Failure) {
  let outcome =
    workflow.with_timeout(ctx, duration.milliseconds(200), fn(scope) {
      child.execute(
        scope,
        waiting(),
        60_000,
        child.options(workflow_id: child_id),
      )
    })
  case outcome {
    Ok(text) -> Ok(text)
    Error(failure) ->
      Ok(
        "child stopped: timeout="
        <> string.inspect(failure.is_timeout(failure))
        <> " cancelled="
        <> string.inspect(
          option.map(failure.cause, failure.is_cancellation) == Some(True),
        ),
      )
  }
}

fn remembering() {
  workflow.Definition("remembering", codec.nil(), codec.string())
}

fn remembering_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  workflow.log_info(ctx, "reading the memo")
  let info = workflow.info(ctx)
  assert info.first_execution_run_id == info.run_id
  Ok(
    workflow.memo(ctx, "origin", codec.string())
    |> option.unwrap("no memo"),
  )
}

fn introspecting() {
  workflow.Definition("introspecting", codec.nil(), codec.string())
}

fn introspecting_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  workflow.execute_activity(ctx, describe_self(), Nil, options())
}

fn budgeting() {
  workflow.Definition("budgeting", codec.nil(), codec.int())
}

fn budgeting_workflow(ctx: Context, _: Nil) -> Result(Int, Failure) {
  case workflow.execute_activity(ctx, detailed_failure(), Nil, options()) {
    Ok(Nil) -> Ok(0)
    Error(failure) -> {
      let cause = failure.root_cause(failure)
      let assert [payload] = cause.details
      codec.decode(codec.int(), payload) |> result.map_error(failure.new)
    }
  }
}

fn doubling() {
  workflow.Definition("doubling", codec.int(), codec.int())
}

fn doubling_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  workflow.execute_activity(ctx, double(), n, options())
}

fn failing() {
  workflow.Definition("failing", codec.nil(), codec.nil())
}

fn workflows() -> List(workflow.Handler) {
  [
    workflow.handler(scoped(), scoped_workflow),
    workflow.handler(tidy(), tidy_workflow),
    workflow.handler(deadline(), deadline_workflow),
    workflow.handler(waiting(), waiting_workflow),
    workflow.handler(resuming(), resuming_workflow),
    workflow.handler(slow_child(), slow_child_workflow),
    workflow.handler(detaching(), detaching_workflow),
    workflow.handler(supervising(), supervising_workflow),
    workflow.handler(remembering(), remembering_workflow),
    workflow.handler(introspecting(), introspecting_workflow),
    workflow.handler(budgeting(), budgeting_workflow),
    workflow.handler(doubling(), doubling_workflow),
    workflow.handler(failing(), fn(_, _) { Error(failure.new("plain")) }),
  ]
}

// ---- middleware ----

fn outbound() -> middleware.Outbound {
  middleware.Outbound(
    ..middleware.none(),
    activity: fn(call: middleware.ActivityCall) {
      case call.activity_type {
        "double" -> middleware.ActivityCall(..call, activity_type: "double_v2")
        _ -> call
      }
    },
  )
}

/// Tags every workflow failure, and reports it once however often the run is replayed.
fn tag_failures(ctx: Context, next) {
  case next() {
    Ok(value) -> Ok(value)
    Error(failure) -> {
      workflow.unless_replaying(ctx, fn() {
        let key = "reports:" <> workflow.info(ctx).workflow_id
        record(key, recorded(key) <> "x")
      })
      Error(failure.with_type(failure, "Tagged"))
    }
  }
}

fn count_activities(ctx: activity.Context, next) {
  let key = "ran:" <> activity.info(ctx).workflow_id
  record(key, recorded(key) <> activity.info(ctx).activity_type <> ";")
  next()
}

// ---- harness ----

fn assert_replays(handle: client.Handle(output)) -> Nil {
  let assert Ok(history) = client.history(handle)
  let wrapped = list.map(workflows(), workflow.wrap(_, tag_failures))
  assert replayer.replay_with(
      wrapped,
      history,
      replayer.Options(..replayer.options(), outbound: outbound()),
    )
    == Ok(Nil)
}

fn run(
  env: Env,
  definition: workflow.Definition(input, output),
  input: input,
) -> Result(output, client.Error) {
  let assert Ok(handle) =
    client.start(env.client, definition, input, start_options(env))
  let result = client.result(handle)
  assert_replays(handle)
  result
}

// ---- cancellation ----

pub fn a_scope_cancels_its_activity_and_the_activity_sees_it_test() {
  use env <- in_both_modes("scoped")
  let assert Ok(handle) =
    client.start(env.client, scoped(), Nil, start_options(env))
  assert client.result(handle) == Ok("carried on")
  assert recorded(client.workflow_id(handle)) == "saw CancelRequested"
  assert_replays(handle)
}

pub fn cleanup_runs_shielded_after_the_workflow_is_cancelled_test() {
  use env <- in_both_modes("tidy")
  let assert Ok(handle) =
    client.start(env.client, tidy(), Nil, start_options(env))
  process.sleep(300)
  let assert Ok(Nil) = client.cancel(handle)
  assert client.result(handle) == Error(client.WorkflowCancelled)
  assert recorded(client.workflow_id(handle)) == "cleaned up"
  assert_replays(handle)
}

pub fn with_timeout_stops_work_that_overruns_test() {
  use env <- in_both_modes("deadline")
  assert run(env, deadline(), 300) == Ok("timed out")
  assert run(env, deadline(), 20_000) == Ok("ran to the end")
}

pub fn await_timeout_waits_for_a_signal_or_gives_up_test() {
  use env <- in_both_modes("waiting")
  assert run(env, waiting(), 100) == Ok("nobody nudged")
  let assert Ok(handle) =
    client.start(env.client, waiting(), 60_000, start_options(env))
  let assert Ok(Nil) = client.signal(handle, nudge(), "hello")
  assert client.result(handle) == Ok("nudged: hello")
  assert_replays(handle)
}

// ---- activities ----

pub fn heartbeat_details_reach_the_next_attempt_test() {
  use env <- in_both_modes("resuming")
  assert run(env, resuming(), Nil) == Ok(42)
}

pub fn activity_info_describes_the_task_test() {
  use env <- in_both_modes("introspecting")
  assert run(env, introspecting(), Nil)
    == Ok("describe_self/introspecting/1/1/Some(30000)/False")
}

pub fn failure_details_cross_the_activity_boundary_test() {
  use env <- in_both_modes("budgeting")
  assert run(env, budgeting(), Nil) == Ok(250)
}

// ---- child workflows ----

pub fn an_abandoned_child_outlives_its_parent_test() {
  use env <- in_both_modes("detaching")
  let child_id = env.task_queue <> "-child"
  let assert Ok(child_run_id) = run(env, detaching(), child_id)
  let child = client.handle(env.client, slow_child(), child_id)
  let assert Ok(description) = client.describe(child)
  assert description.run_id == child_run_id
  assert option.is_some(description.parent_workflow_id)
  let assert Ok("child of " <> _) = client.result(child)
  Nil
}

pub fn a_deadline_cancels_a_child_workflow_test() {
  use env <- in_both_modes("supervising")
  let child_id = env.task_queue <> "-child"
  assert run(env, supervising(), child_id)
    == Ok("child stopped: timeout=True cancelled=True")
  let child = client.handle(env.client, waiting(), child_id)
  assert client.result(child) == Error(client.WorkflowCancelled)
}

// ---- client ----

pub fn memo_reaches_the_workflow_and_the_description_test() {
  use env <- in_both_modes("remembering")
  let options =
    client.StartOptions(
      ..start_options(env),
      memo: dict.from_list([
        #("origin", codec.encode(codec.string(), "checkout")),
      ]),
    )
  let assert Ok(handle) = client.start(env.client, remembering(), Nil, options)
  assert client.result(handle) == Ok("checkout")
  let assert Ok(description) = client.describe(handle)
  assert description.status == client.Completed
  assert description.workflow_type == "remembering"
  assert description.task_queue == env.task_queue
  assert option.is_some(description.close_time)
  let assert Ok(payload) = dict.get(description.memo, "origin")
  assert codec.decode(codec.string(), payload) == Ok("checkout")
  assert_replays(handle)
}

pub fn signal_with_start_starts_once_and_signals_every_time_test() {
  use env <- in_both_modes("signal-with-start")
  let options = start_options(env)
  let assert Ok(first) =
    client.signal_with_start(
      env.client,
      waiting(),
      60_000,
      options,
      nudge(),
      "first",
    )
  assert client.result(first) == Ok("nudged: first")
  assert_replays(first)
  // A second call reaches the running workflow instead of starting another.
  let options = start_options(env)
  let assert Ok(running) = client.start(env.client, waiting(), 60_000, options)
  let assert Ok(signalled) =
    client.signal_with_start(
      env.client,
      waiting(),
      60_000,
      options,
      nudge(),
      "second",
    )
  assert client.run_id(signalled) == client.run_id(running)
  assert client.result(running) == Ok("nudged: second")
}

pub fn use_existing_returns_the_running_workflow_test() {
  use env <- in_both_modes("use-existing")
  let options =
    client.StartOptions(
      ..start_options(env),
      id_conflict_policy: client.UseExisting,
    )
  let assert Ok(first) = client.start(env.client, waiting(), 60_000, options)
  let assert Ok(second) = client.start(env.client, waiting(), 60_000, options)
  assert client.run_id(first) == client.run_id(second)
  let assert Ok(Nil) = client.signal(second, nudge(), "one of us")
  assert client.result(first) == Ok("nudged: one of us")
}

pub fn a_closed_workflow_id_can_be_refused_test() {
  use env <- in_both_modes("reject-duplicate")
  let options =
    client.StartOptions(
      ..start_options(env),
      id_reuse_policy: workflow.RejectDuplicate,
    )
  let assert Ok(handle) = client.start(env.client, waiting(), 10, options)
  assert client.result(handle) == Ok("nobody nudged")
  let assert Error(client.AlreadyExists(_)) =
    client.start(env.client, waiting(), 10, options)
  Nil
}

pub fn a_run_timeout_ends_the_workflow_test() {
  use env <- in_both_modes("run-timeout")
  let options =
    client.StartOptions(
      ..start_options(env),
      run_timeout: Some(duration.milliseconds(1000)),
    )
  let assert Ok(handle) = client.start(env.client, waiting(), 60_000, options)
  assert client.result(handle) == Error(client.WorkflowTimedOut)
}

pub fn requests_about_a_missing_workflow_are_not_found_test() {
  let assert Ok(client) = client.connect(client.local())
  let missing =
    client.handle(client, waiting(), "no-such-workflow-" <> unique())
  let assert Error(client.NotFound(_)) = client.signal(missing, nudge(), "x")
  let assert Error(client.NotFound(_)) = client.describe(missing)
  let assert Error(client.NotFound(_)) = client.cancel(missing)
  let assert Error(client.NotFound(_)) = client.terminate(missing, "x")
  let assert Error(client.NotFound(_)) = client.history(missing)
}

pub fn workflows_can_be_listed_and_counted_test() {
  let assert Ok(client) = client.connect(client.local())
  let task_queue = "listing-" <> unique()
  let attribute = support.register_search_attribute()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows(workflows())
    |> worker.start
  let handles =
    list.map([1, 2, 3], fn(n) {
      let assert Ok(handle) =
        client.start(
          client,
          waiting(),
          10,
          client.StartOptions(
            ..client.start_options(
              workflow_id: task_queue <> "-" <> int.to_string(n),
              task_queue:,
            ),
            search_attributes: dict.from_list([
              #(attribute, search_attributes.Keyword(task_queue)),
            ]),
          ),
        )
      handle
    })
  list.each(handles, fn(handle) {
    assert client.result(handle) == Ok("nobody nudged")
  })
  worker.stop(worker)
  let query = attribute <> " = \"" <> task_queue <> "\""
  assert eventually(fn() { client.count(client, query) == Ok(3) })
  let assert Ok(found) = client.list(client, query, limit: 2)
  assert list.length(found) == 2
  assert list.all(found, fn(run) { run.status == client.Completed })
}

// ---- middleware and workers ----

pub fn outbound_middleware_renames_the_activity_test() {
  use env <- in_both_modes("renaming")
  let assert Ok(handle) =
    client.start(env.client, doubling(), 21, start_options(env))
  assert client.result(handle) == Ok(42)
  // The activity middleware saw the rewritten name: that is what the worker ran.
  assert recorded("ran:" <> client.workflow_id(handle)) == "double_v2;"
  assert_replays(handle)
}

pub fn workflow_middleware_reports_a_failure_once_test() {
  use env <- in_both_modes("tagging")
  let assert Ok(handle) =
    client.start(env.client, failing(), Nil, start_options(env))
  let assert Error(client.WorkflowFailed(failure)) = client.result(handle)
  assert failure.error_type == Some("Tagged")
  assert_replays(handle)
  assert recorded("reports:" <> client.workflow_id(handle)) == "x"
}

pub fn workflows_and_activities_can_run_on_separate_workers_test() {
  let assert Ok(client) = client.connect(client.local())
  let task_queue = "split-" <> unique()
  let assert Ok(workflow_worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows(workflows())
    |> worker.start
  let assert Ok(activity_worker) =
    worker.new(client, task_queue)
    |> worker.with_activities(activities())
    |> worker.with_max_concurrent_activities(1)
    |> worker.with_max_task_queue_activities_per_second(50.0)
    |> worker.start
  assert client.execute(
      client,
      introspecting(),
      Nil,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
    == Ok("describe_self/introspecting/1/1/Some(30000)/False")
  worker.stop(workflow_worker)
  worker.stop(activity_worker)
}

/// Run `scenario` against a fresh worker in both cache modes, with every kind of
/// middleware installed.
fn in_both_modes(name: String, scenario: fn(Env) -> Nil) -> Nil {
  use config <- support.in_both_modes(name, support.local_client(), _, scenario)
  config
  |> worker.with_workflows(workflows())
  |> worker.with_activities(activities())
  |> worker.with_max_concurrent_activities(20)
  |> worker.with_max_activities_per_second(500.0)
  |> worker.with_workflow_middleware(tag_failures)
  |> worker.with_activity_middleware(count_activities)
  |> worker.with_outbound_activity(outbound().activity)
}
