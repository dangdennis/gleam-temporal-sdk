//// Scenarios from Temporal's cross-SDK features repository
//// (https://github.com/temporalio/features) that the rest of the suite does not already
//// cover, each named after the feature it ports.

import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import gleam/order
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import support.{eventually, unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/replayer
import temporal/retry
import temporal/schedule
import temporal/worker
import temporal/workflow.{type Context}
import temporal/workflow/query
import temporal/workflow/signal

fn options() {
  workflow.activity_options(start_to_close: duration.minutes(1))
}

// ---- data_converter/binary, data_converter/empty, data_converter/json ----

/// The payloads the other SDKs write for the same values, from each feature's
/// `payload.json`.
pub fn data_converter_payloads_match_the_other_sdks_test() {
  let deadbeef = <<0xde, 0xad, 0xbe, 0xef>>
  assert codec.encode(codec.bytes(), deadbeef)
    == codec.Payload(
      metadata: dict.from_list([#("encoding", <<"binary/plain":utf8>>)]),
      data: deadbeef,
    )
  assert bit_array.base64_encode(deadbeef, True) == "3q2+7w=="
  assert codec.decode(codec.bytes(), codec.encode(codec.bytes(), deadbeef))
    == Ok(deadbeef)

  assert codec.encode(codec.nil(), Nil)
    == codec.Payload(
      metadata: dict.from_list([#("encoding", <<"binary/null":utf8>>)]),
      data: <<>>,
    )

  assert codec.encode(codec.bool(), True)
    == codec.Payload(
      metadata: dict.from_list([#("encoding", <<"json/plain":utf8>>)]),
      data: <<"true":utf8>>,
    )
}

fn echo_bytes() {
  workflow.Definition("echo_bytes", codec.bytes(), codec.bytes())
}

pub fn data_converter_binary_round_trips_through_a_workflow_test() {
  let client = support.local_client()
  let task_queue = "binary-" <> unique()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(echo_bytes(), fn(_, bytes) { Ok(bytes) }),
    ])
    |> worker.start
  let deadbeef = <<0xde, 0xad, 0xbe, 0xef>>
  assert client.execute(
      client,
      echo_bytes(),
      deadbeef,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
    == Ok(deadbeef)
  worker.stop(worker)
}

// ---- activity/retry_on_error ----

fn always_fail() {
  activity.Definition("always_fail", codec.nil(), codec.nil())
}

fn retrying() {
  workflow.Definition("retrying", codec.nil(), codec.nil())
}

/// Five attempts with no backoff, then the workflow fails with the last attempt's error.
fn retrying_workflow(ctx: Context, _: Nil) -> Result(Nil, Failure) {
  workflow.execute_activity(
    ctx,
    always_fail(),
    Nil,
    options()
      |> workflow.with_retry(
        retry.policy(maximum_attempts: 5)
        |> retry.with_initial_interval(duration.milliseconds(1))
        |> retry.with_backoff_coefficient(1.0),
      ),
  )
}

pub fn activity_retry_on_error_test() {
  let client = support.local_client()
  let task_queue = "retry-on-error-" <> unique()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows([
      workflow.handler(retrying(), retrying_workflow),
    ])
    |> worker.with_activities([
      activity.handler(always_fail(), fn(ctx, _) {
        Error(failure.new(
          "activity attempt "
          <> string.inspect(activity.info(ctx).attempt)
          <> " failed",
        ))
      }),
    ])
    |> worker.start
  let assert Error(client.WorkflowFailed(failed)) =
    client.execute(
      client,
      retrying(),
      Nil,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
  assert failure.root_cause(failed).message == "activity attempt 5 failed"
  worker.stop(worker)
}

/// The features repository keeps the histories its Go and Java SDKs recorded for this
/// feature. They are not copied here; `bin/features-histories` converts them from a
/// checkout into `build/features`, and this replays them when they are there.
///
/// Those SDKs name the workflow and the activity their own way and choose activity ids
/// differently. Core compares ids and types only in histories written by an SDK built on
/// it, so what is checked here is that the same kinds of command come in the same order.
pub fn activity_retry_on_error_replays_go_and_java_histories_test() {
  let cases = [#("go", "Workflow"), #("java", "SimpleWorkflow")]
  list.each(cases, fn(sdk) {
    let #(name, workflow_type) = sdk
    case support.read_file("build/features/retry_on_error." <> name <> ".bin") {
      Error(_) -> Nil
      Ok(encoded) -> {
        let history = client.History(workflow_id: "features", encoded:)
        let definition = workflow.Definition(..retrying(), name: workflow_type)
        let twin = workflow.handler(definition, retrying_workflow)
        assert replayer.replay([twin], history) == Ok(Nil)
        // A timer before the activity is not what the history records.
        let diverging =
          workflow.handler(definition, fn(ctx, _) {
            use _ <- result.try(workflow.sleep(ctx, duration.seconds(1)))
            retrying_workflow(ctx, Nil)
          })
        let assert Error(replayer.Nondeterminism(_)) =
          replayer.replay([diverging], history)
        Nil
      }
    }
  })
}

// ---- query/unexpected_arguments, query/unexpected_return_type ----

fn finish() {
  workflow.SignalDefinition("finish", codec.nil())
}

fn queried() {
  workflow.Definition("queried", codec.nil(), codec.nil())
}

fn queried_workflow(ctx: Context, _: Nil) -> Result(Nil, Failure) {
  query.set_handler(
    ctx,
    workflow.QueryDefinition("myq", codec.int(), codec.string()),
    fn(n) { "query yo " <> string.inspect(n) },
  )
  Ok(signal.receive(ctx, finish()))
}

/// The TypeScript SDK accepts anything here. With typed codecs a query whose argument or
/// result is of the wrong type fails instead, at the side that sees the mismatch.
pub fn query_with_unexpected_types_fails_test() {
  let client = support.local_client()
  let task_queue = "queried-" <> unique()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows([workflow.handler(queried(), queried_workflow)])
    |> worker.start
  let assert Ok(handle) =
    client.start(
      client,
      queried(),
      Nil,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
  assert client.query(
      handle,
      workflow.QueryDefinition("myq", codec.int(), codec.string()),
      123,
    )
    == Ok("query yo 123")
  // An argument of the wrong type: the workflow's codec refuses it.
  let assert Error(client.QueryFailed(_)) =
    client.query(
      handle,
      workflow.QueryDefinition("myq", codec.bool(), codec.string()),
      True,
    )
  // No argument at all.
  let assert Error(client.QueryFailed(_)) =
    client.query(
      handle,
      workflow.QueryDefinition("myq", codec.no_args(), codec.string()),
      Nil,
    )
  // An extra argument is ignored, as it is everywhere.
  assert client.query(
      handle,
      workflow.QueryDefinition(
        "myq",
        codec.args2(codec.int(), codec.bool()),
        codec.string(),
      ),
      #(123, True),
    )
    == Ok("query yo 123")
  // A result of the wrong type: the client's codec refuses it.
  let assert Error(client.DecodeFailed(_)) =
    client.query(
      handle,
      workflow.QueryDefinition("myq", codec.int(), codec.int()),
      123,
    )
  let assert Ok(Nil) = client.signal(handle, finish(), Nil)
  assert client.result(handle) == Ok(Nil)
  worker.stop(worker)
}

// ---- activity/shutdown ----

fn until_shutdown() {
  activity.Definition("until_shutdown", codec.bool(), codec.string())
}

/// Heartbeats until its worker shuts down, then finishes or fails as told.
fn until_shutdown_activity(ctx: activity.Context, succeed: Bool) {
  case activity.cancel_reason(ctx) {
    Some(activity.WorkerShutdown) ->
      case succeed {
        True -> Ok("stopped cleanly")
        False -> Error(failure.new("worker is shutting down"))
      }
    _ -> {
      process.sleep(50)
      until_shutdown_activity(ctx, succeed)
    }
  }
}

fn deaf() {
  activity.Definition("deaf_to_shutdown", codec.nil(), codec.nil())
}

fn closing() {
  workflow.Definition("closing", codec.nil(), codec.string())
}

/// Three activities caught by a worker shutdown: one finishes, one fails, one ignores it.
fn closing_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  let once =
    options()
    |> workflow.with_schedule_to_close(duration.seconds(30))
    |> workflow.with_retry(retry.policy(maximum_attempts: 1))
  let finishing = workflow.start_activity(ctx, until_shutdown(), True, once)
  let failing = workflow.start_activity(ctx, until_shutdown(), False, once)
  let ignoring =
    workflow.start_activity(
      ctx,
      deaf(),
      Nil,
      workflow.ActivityOptions(
        ..once,
        schedule_to_close: Some(duration.seconds(3)),
      ),
    )
  use finished <- result.try(workflow.await(finishing))
  let failed = case workflow.await(failing) {
    Error(failure) -> failure.root_cause(failure).message
    Ok(_) -> "did not fail"
  }
  let ignored = case workflow.await(ignoring) {
    Error(failure) -> failure.is_timeout(failure)
    Ok(Nil) -> False
  }
  Ok(finished <> "; " <> failed <> "; timed out: " <> string.inspect(ignored))
}

pub fn activity_shutdown_test() {
  let task_queue = "closing-" <> unique()
  let client = support.local_client()
  let workflows = fn() {
    worker.new(support.local_client(), task_queue)
    |> worker.with_workflows([workflow.handler(closing(), closing_workflow)])
  }
  // Activities on a worker of their own, so that stopping it leaves the workflow running.
  let assert Ok(activities) =
    worker.new(support.local_client(), task_queue)
    |> worker.with_activities([
      activity.handler(until_shutdown(), until_shutdown_activity),
      activity.handler(deaf(), fn(_, _) { Ok(process.sleep(60_000)) }),
    ])
    |> worker.with_shutdown_grace(duration.seconds(1))
    |> worker.start
  let assert Ok(workflow_worker) = worker.start(workflows())
  let assert Ok(handle) =
    client.start(
      client,
      closing(),
      Nil,
      client.start_options(workflow_id: task_queue, task_queue:),
    )
  // All three activities are running once the history shows them scheduled.
  assert eventually(fn() {
    let shown =
      support.cmd(
        "temporal workflow show --workflow-id " <> task_queue <> " 2>&1",
      )
    list.length(string.split(shown, "ActivityTaskScheduled")) == 4
  })
  process.sleep(500)
  worker.stop(activities)
  assert client.result(handle)
    == Ok("stopped cleanly; worker is shutting down; timed out: True")
  worker.stop(workflow_worker)
}

// ---- schedule/backfill ----

fn tick() {
  workflow.Definition("backfilled", codec.string(), codec.string())
}

pub fn schedule_backfill_test() {
  let client = support.local_client()
  let id = "backfill-" <> unique()
  let assert Ok(worker) =
    worker.new(client, id)
    |> worker.with_workflows([
      workflow.handler(tick(), fn(_, text) { Ok(text) }),
    ])
    |> worker.start
  // Paused, so that the only actions are the backfilled ones.
  let assert Ok(Nil) =
    schedule.create(
      client,
      id,
      schedule.new(
        schedule.every(duration.minutes(1)),
        schedule.action(
          tick(),
          "arg1",
          client.start_options(workflow_id: id <> "-run", task_queue: id),
        ),
      )
        |> schedule.with_paused(True),
    )
  // Half past the current minute, so that each two-minute window below holds exactly two
  // of the schedule's ticks whatever second the test runs at.
  let #(seconds, _) =
    timestamp.to_unix_seconds_and_nanoseconds(timestamp.system_time())
  let now = timestamp.from_unix_seconds(seconds - seconds % 60 + 30)
  let ago = fn(minutes) { timestamp.subtract(now, duration.minutes(minutes)) }
  // Two minutes of a one-minute schedule, twice: three years ago and half an hour ago.
  let three_years = 3 * 365 * 24 * 60
  let assert Ok(Nil) =
    schedule.backfill(
      client,
      id,
      from: ago(three_years + 2),
      to: ago(three_years),
      overlap: schedule.AllowAll,
    )
  let assert Ok(Nil) =
    schedule.backfill(
      client,
      id,
      from: ago(32),
      to: ago(30),
      overlap: schedule.AllowAll,
    )
  let action_count = fn() {
    let assert Ok(described) = schedule.describe(client, id)
    described.action_count
  }
  // The server's scheduler does not always get through both backfills in one pass, and
  // its next pass may be a minute away, at the schedule's next tick. A request to the
  // schedule wakes it, so nudge it rather than wait: the whole test has sixty seconds.
  // By the clock, not by a count of attempts: each look at the schedule is a query whose
  // own duration varies a great deal between machines.
  let reached = fn(seconds) {
    until(
      timestamp.add(timestamp.system_time(), duration.seconds(seconds)),
      fn() { action_count() >= 4 },
    )
  }
  let nudge = fn() {
    let assert Ok(Nil) = schedule.pause(client, id, note: "still paused")
    False
  }
  let _ = reached(8) || nudge() || reached(12) || nudge() || reached(12)
  process.sleep(500)
  assert action_count() == 4
  let assert Ok(Nil) = schedule.delete(client, id)
  worker.stop(worker)
}

/// Ask `check` every 200 ms until it holds or the clock passes `deadline`.
fn until(deadline: timestamp.Timestamp, check: fn() -> Bool) -> Bool {
  case check() {
    True -> True
    False ->
      case timestamp.compare(timestamp.system_time(), deadline) {
        order.Lt -> {
          process.sleep(200)
          until(deadline, check)
        }
        _ -> False
      }
  }
}
