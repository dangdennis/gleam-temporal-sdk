//// End-to-end tests of standalone activities: activities a client starts with no workflow
//// around them, against a real Temporal service, plus one exchange with the official
//// TypeScript SDK in each direction.

import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleam/time/duration
import support.{eventually, record, recorded, unique}
import temporal/activity
import temporal/client.{type Client}
import temporal/client/standalone_activity
import temporal/codec
import temporal/encryption
import temporal/failure
import temporal/retry
import temporal/worker

// ---- activities ----

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

/// Doubles its input and records what it was told about where it runs.
fn double_activity(ctx: activity.Context, n: Int) {
  let info = activity.info(ctx)
  record(
    info.activity_id,
    string.inspect(#(
      activity.is_standalone(ctx),
      info.workflow_id,
      info.run_id,
      info.workflow_type,
      info.activity_run_id,
      info.attempt,
    )),
  )
  Ok(n * 2)
}

fn declined() {
  activity.Definition("declined", codec.string(), codec.nil())
}

/// Fails every attempt, recording each one and whether it knew it was the last.
fn declined_activity(ctx: activity.Context, card: String) {
  let id = activity.info(ctx).activity_id
  record(
    id,
    recorded(id)
      <> int.to_string(activity.info(ctx).attempt)
      <> case activity.is_final_attempt(ctx) {
      True -> "! "
      False -> " "
    },
  )
  Error(
    failure.new("card " <> card <> " was declined")
    |> failure.with_type("Declined")
    |> failure.with_details([codec.encode(codec.string(), "code 51")]),
  )
}

fn patient() {
  activity.Definition("patient", codec.int(), codec.string())
}

/// Heartbeats its progress until cancelled.
fn patient_activity(ctx: activity.Context, progress: Int) {
  activity.heartbeat_with(ctx, codec.int(), progress)
  record(activity.info(ctx).activity_id, "running")
  case activity.cancel_reason(ctx) {
    Some(reason) -> {
      record(activity.info(ctx).activity_id, "saw " <> string.inspect(reason))
      Error(failure.cancelled())
    }
    None -> {
      process.sleep(50)
      patient_activity(ctx, progress)
    }
  }
}

fn approval() {
  activity.Definition("approval", codec.nil(), codec.string())
}

/// Leaves its completion to whoever holds its task token.
fn approval_activity(ctx: activity.Context, _: Nil) {
  let id = activity.info(ctx).activity_id
  record(id, bit_array.base64_encode(activity.task_token(ctx), True))
  Error(activity.complete_later(ctx))
}

// ---- helpers ----

fn local() -> Client {
  let assert Ok(client) = client.connect(client.local())
  client
}

/// Run `scenario` with a worker for the activities above on a queue of its own.
fn with_worker(client: Client, scenario: fn(String) -> Nil) -> Nil {
  let task_queue = "standalone-" <> unique()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_activities([
      activity.handler(double(), double_activity),
      activity.handler(declined(), declined_activity),
      activity.handler(patient(), patient_activity),
      activity.handler(approval(), approval_activity),
    ])
    |> worker.start
  scenario(task_queue)
  worker.stop(worker)
}

fn options_on(task_queue: String) -> standalone_activity.Options {
  standalone_activity.options(
    activity_id: task_queue <> "-" <> unique(),
    task_queue:,
    start_to_close: duration.seconds(10),
  )
}

// ---- tests ----

pub fn a_standalone_activity_runs_and_returns_its_typed_result_test() {
  let client = local()
  use task_queue <- with_worker(client)
  let options = options_on(task_queue)
  let assert Ok(handle) =
    standalone_activity.start(client, double(), 21, options)
  assert standalone_activity.result(handle) == Ok(42)
  assert standalone_activity.activity_id(handle) == options.activity_id

  // The activity was told the truth: no workflow, and a run of its own.
  let assert Some(run_id) = standalone_activity.run_id(handle)
  assert recorded(options.activity_id)
    == string.inspect(#(True, "", "", "", Some(run_id), 1))

  // The result can be fetched again, by id alone.
  let again = standalone_activity.handle(client, double(), options.activity_id)
  assert standalone_activity.run_id(again) == None
  assert standalone_activity.result(again) == Ok(42)

  assert standalone_activity.execute(
      client,
      double(),
      4,
      options_on(task_queue),
    )
    == Ok(8)
}

pub fn a_failing_standalone_activity_is_retried_then_surfaces_its_failure_test() {
  let client = local()
  use task_queue <- with_worker(client)
  let options =
    options_on(task_queue)
    |> standalone_activity.with_retry(
      retry.policy(maximum_attempts: 3)
      |> retry.with_initial_interval(duration.milliseconds(100))
      |> retry.with_backoff_coefficient(1.0),
    )
  let assert Error(client.ActivityFailed(failed)) =
    standalone_activity.execute(client, declined(), "4111", options)
  let cause = failure.root_cause(failed)
  assert cause.message == "card 4111 was declined"
  assert cause.kind == failure.Application
  assert cause.error_type == Some("Declined")
  assert list.map(cause.details, codec.decode(codec.string(), _))
    == [Ok("code 51")]
  // Three attempts ran, and only the third knew it was the last.
  assert recorded(options.activity_id) == "1 2 3! "

  let handle =
    standalone_activity.handle(client, declined(), options.activity_id)
  let assert Ok(description) = standalone_activity.describe(handle)
  assert description.status == standalone_activity.Failed
  assert description.attempt == 3
  assert description.run_state == None
  assert option.is_some(description.close_time)
  let assert Some(last) = description.last_failure
  assert failure.root_cause(last).error_type == Some("Declined")
}

pub fn a_non_retryable_failure_ends_a_standalone_activity_at_once_test() {
  let client = local()
  use task_queue <- with_worker(client)
  let options =
    options_on(task_queue)
    |> standalone_activity.with_retry(
      retry.policy(maximum_attempts: 5)
      |> retry.with_non_retryable_error_types(["Declined"]),
    )
  let assert Error(client.ActivityFailed(_)) =
    standalone_activity.execute(client, declined(), "4000", options)
  assert recorded(options.activity_id) == "1 "
}

pub fn a_heartbeating_standalone_activity_is_described_and_cancelled_test() {
  let client = local()
  use task_queue <- with_worker(client)
  let options =
    options_on(task_queue)
    |> standalone_activity.with_heartbeat_timeout(duration.milliseconds(1000))
  let assert Ok(handle) =
    standalone_activity.start(client, patient(), 7, options)
  assert eventually(fn() { recorded(options.activity_id) == "running" })

  // While it runs, the server shows the attempt and the progress it heartbeated.
  assert eventually(fn() {
    let assert Ok(description) = standalone_activity.describe(handle)
    description.run_state == Some(standalone_activity.Started)
    && list.map(description.heartbeat_details, codec.decode(codec.int(), _))
    == [Ok(7)]
  })
  let assert Ok(description) = standalone_activity.describe(handle)
  assert description.activity_id == options.activity_id
  assert Some(description.run_id) == standalone_activity.run_id(handle)
  assert description.activity_type == "patient"
  assert description.task_queue == task_queue
  assert description.status == standalone_activity.Running
  assert description.attempt == 1
  assert description.close_time == None
  assert description.last_failure == None
  assert description.last_worker_identity != ""

  let assert Ok(Nil) = standalone_activity.cancel(handle, "no longer needed")
  assert standalone_activity.result(handle) == Error(client.ActivityCancelled)
  assert recorded(options.activity_id) == "saw CancelRequested"
  let assert Ok(description) = standalone_activity.describe(handle)
  assert description.status == standalone_activity.Cancelled
}

pub fn standalone_activities_nobody_runs_are_cancelled_terminated_and_timed_out_test() {
  let client = local()
  // No worker polls this queue.
  let task_queue = "standalone-idle-" <> unique()
  let start = fn(options) {
    let assert Ok(handle) =
      standalone_activity.start(client, double(), 1, options)
    handle
  }

  let cancelled = start(options_on(task_queue))
  let assert Ok(Nil) = standalone_activity.cancel(cancelled, "changed my mind")
  assert standalone_activity.result(cancelled)
    == Error(client.ActivityCancelled)

  let terminated = start(options_on(task_queue))
  let assert Ok(Nil) = standalone_activity.terminate(terminated, "stuck")
  assert standalone_activity.result(terminated)
    == Error(client.ActivityTerminated)
  let assert Ok(description) = standalone_activity.describe(terminated)
  assert description.status == standalone_activity.Terminated

  let timed_out =
    start(
      options_on(task_queue)
      |> standalone_activity.with_schedule_to_close(duration.seconds(1)),
    )
  assert standalone_activity.result(timed_out) == Error(client.ActivityTimedOut)

  let missing =
    standalone_activity.handle(client, double(), "no-such-" <> unique())
  let assert Error(client.NotFound(_)) = standalone_activity.describe(missing)
}

pub fn a_standalone_activity_is_completed_from_outside_by_its_task_token_test() {
  let client = local()
  use task_queue <- with_worker(client)
  let options = options_on(task_queue)
  let assert Ok(handle) =
    standalone_activity.start(client, approval(), Nil, options)
  assert eventually(fn() { recorded(options.activity_id) != "" })
  let assert Ok(task_token) =
    bit_array.base64_decode(recorded(options.activity_id))
  let assert Ok(Nil) =
    client.complete_activity(client, task_token, approval(), "approved")
  assert standalone_activity.result(handle) == Ok("approved")
}

pub fn a_standalone_activity_id_is_taken_while_it_runs_test() {
  let client = local()
  let task_queue = "standalone-idle-" <> unique()
  let options = options_on(task_queue)
  let assert Ok(first) = standalone_activity.start(client, double(), 1, options)
  let assert Error(client.AlreadyExists(_)) =
    standalone_activity.start(client, double(), 1, options)
  let assert Ok(same) =
    standalone_activity.start(
      client,
      double(),
      1,
      options
        |> standalone_activity.with_id_conflict_policy(
          standalone_activity.UseExisting,
        ),
    )
  assert standalone_activity.run_id(same) == standalone_activity.run_id(first)
  let assert Ok(Nil) = standalone_activity.terminate(first, "done")

  // Closed, the id is free again unless the reuse policy says otherwise.
  let assert Error(client.AlreadyExists(_)) =
    standalone_activity.start(
      client,
      double(),
      1,
      options
        |> standalone_activity.with_id_reuse_policy(
          standalone_activity.RejectDuplicate,
        ),
    )
  let assert Ok(second) =
    standalone_activity.start(client, double(), 1, options)
  assert standalone_activity.run_id(second) != standalone_activity.run_id(first)
  let assert Ok(Nil) = standalone_activity.terminate(second, "done")
}

pub fn standalone_activities_are_listed_and_counted_test() {
  let client = local()
  use task_queue <- with_worker(client)
  let options = options_on(task_queue)
  assert standalone_activity.execute(client, double(), 2, options) == Ok(4)
  let query = "ActivityId = \"" <> options.activity_id <> "\""
  assert eventually(fn() { standalone_activity.count(client, query) == Ok(1) })
  let assert Ok([summary]) = standalone_activity.list(client, query, limit: 10)
  assert summary.activity_id == options.activity_id
  assert summary.activity_type == "double"
  assert summary.task_queue == task_queue
  assert summary.status == standalone_activity.Completed
  assert option.is_some(summary.close_time)
}

pub fn a_payload_codec_covers_a_standalone_activity_test() {
  let key =
    encryption.Key(id: "2026", secret: bit_array.concat(list.repeat(<<9>>, 32)))
  let assert Ok(encrypted) =
    client.connect(
      client.local() |> client.with_codec(encryption.codec(key, [])),
    )
  use task_queue <- with_worker(encrypted)
  let options = options_on(task_queue)
  assert standalone_activity.execute(encrypted, double(), 21, options) == Ok(42)

  // What the server holds is ciphertext: a client without the key cannot read the result.
  let outsider =
    standalone_activity.handle(local(), double(), options.activity_id)
  let assert Error(client.DecodeFailed(_)) =
    standalone_activity.result(outsider)

  // The same goes for a failure's details and for heartbeat details.
  let failing =
    options_on(task_queue)
    |> standalone_activity.with_retry(retry.policy(maximum_attempts: 1))
  let assert Error(client.ActivityFailed(failed)) =
    standalone_activity.execute(encrypted, declined(), "4111", failing)
  assert list.map(failure.root_cause(failed).details, codec.decode(
      codec.string(),
      _,
    ))
    == [Ok("code 51")]
  let outsider =
    standalone_activity.handle(local(), declined(), failing.activity_id)
  let assert Error(client.ActivityFailed(unread)) =
    standalone_activity.result(outsider)
  let assert [stored] = failure.root_cause(unread).details
  assert dict.get(stored.metadata, "encoding")
    == Ok(<<"binary/encrypted":utf8>>)
}

/// The TypeScript client starts an activity that this SDK's worker runs, and this SDK's
/// client starts one that a TypeScript worker runs.
pub fn standalone_activities_interoperate_with_the_typescript_sdk_test() {
  let typescript = "test/conformance/typescript"
  case
    support.cmd("command -v node >/dev/null && echo yes"),
    support.read_file(
      typescript <> "/node_modules/@temporalio/client/package.json",
    )
  {
    "yes\n", Ok(_) -> {
      let client = local()
      use task_queue <- with_worker(client)
      let typescript_queue = task_queue <> "-typescript"
      let triple = activity.Definition("triple", codec.int(), codec.int())
      let options =
        standalone_activity.options(
          activity_id: typescript_queue,
          task_queue: typescript_queue,
          start_to_close: duration.seconds(10),
        )
        |> standalone_activity.with_schedule_to_close(duration.seconds(60))
      let assert Ok(handle) =
        standalone_activity.start(client, triple, 14, options)
      let output =
        support.cmd(
          "cd "
          <> typescript
          <> " && node standalone.js "
          <> task_queue
          <> " "
          <> typescript_queue
          <> " "
          <> typescript_queue
          <> " 2>&1",
        )
      assert string.trim(output)
        == "{\"fromGleamWorker\":42,\"byGleamClient\":42,\"seen\":{\"activityId\":\""
        <> typescript_queue
        <> "\",\"inWorkflow\":false}}"
      assert standalone_activity.result(handle) == Ok(42)
      // The activity the TypeScript client started saw no workflow here either.
      assert string.starts_with(
        recorded(task_queue <> "-from-typescript"),
        "#(True, \"\", \"\", \"\", Some(",
      )
    }
    _, _ ->
      io.println(
        "\nskipped the standalone-activity interop test: it needs node and "
        <> typescript
        <> "/node_modules (bin/conformance installs them)",
      )
  }
}
