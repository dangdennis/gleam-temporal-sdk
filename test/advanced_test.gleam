//// End-to-end tests of payload codecs, updates, signalling and cancelling other workflows,
//// local activities, Nexus operations and worker versioning, against a real Temporal
//// service. Workflow scenarios run with and without the workflow cache and their histories
//// are replayed.

import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/string
import temporal/activity
import temporal/client.{type Client}
import temporal/codec
import temporal/encryption
import temporal/failure.{type Failure}
import temporal/nexus
import temporal/replayer
import temporal/worker
import temporal/workflow.{type Context}

@external(erlang, "temporal_test_ffi", "unique")
fn unique() -> String

@external(erlang, "temporal_test_ffi", "cmd")
fn cmd(command: String) -> String

// ---- activities ----

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

/// Fails until its attempt number reaches its input, and says whether it ran locally.
fn stubborn() {
  activity.Definition("stubborn", codec.int(), codec.string())
}

fn which_build() {
  activity.Definition("which_build", codec.nil(), codec.string())
}

fn activities(build: String) -> List(activity.Handler) {
  [
    activity.handler(double(), fn(_, n) { Ok(n * 2) }),
    activity.handler(stubborn(), fn(ctx, attempts) {
      case ctx.info.attempt >= attempts {
        True ->
          Ok(
            "attempt "
            <> int.to_string(ctx.info.attempt)
            <> " local="
            <> string.inspect(ctx.info.is_local),
          )
        False -> Error(failure.new("not yet"))
      }
    }),
    activity.handler(which_build(), fn(_, _) { Ok(build) }),
  ]
}

fn options() {
  workflow.activity_options(start_to_close_ms: 10_000)
}

// ---- updates ----

fn add() {
  workflow.UpdateDefinition("add", codec.int(), codec.int())
}

fn finish() {
  workflow.SignalDefinition("finish", codec.nil())
}

fn counter() {
  workflow.Definition("counter", codec.int(), codec.int())
}

/// A counter changed by updates. Each update runs an activity before it answers, and
/// negative amounts are refused by the validator.
fn counter_workflow(ctx: Context, total: Int) -> Result(Int, Failure) {
  workflow.set_update_validator(ctx, add(), fn(amount) {
    case amount < 0 {
      True -> Error("amount must not be negative")
      False -> Ok(Nil)
    }
  })
  let next =
    workflow.race([
      workflow.update(ctx, add()) |> workflow.map(Some),
      workflow.signal(ctx, finish()) |> workflow.map(fn(_) { None }),
    ])
  case workflow.await(next) {
    None -> Ok(total)
    Some(update) -> {
      let amount = workflow.update_argument(update)
      case amount {
        // An update the workflow accepts and then fails.
        13 -> {
          workflow.complete_update(
            ctx,
            update,
            Error(failure.new("unlucky") |> failure.with_type("Unlucky")),
          )
          counter_workflow(ctx, total)
        }
        _ -> {
          use doubled <- result.try(workflow.execute_activity(
            ctx,
            double(),
            amount,
            options(),
          ))
          let total = total + doubled
          workflow.complete_update(ctx, update, Ok(total))
          counter_workflow(ctx, total)
        }
      }
    }
  }
}

// ---- other workflows ----

fn poke() {
  workflow.SignalDefinition("poke", codec.string())
}

fn listener() {
  workflow.Definition("listener", codec.nil(), codec.string())
}

fn listener_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  case workflow.await_cancellable(ctx, workflow.signal(ctx, poke())) {
    Ok(text) -> Ok("poked: " <> text)
    Error(cancelled) -> Error(cancelled)
  }
}

fn messenger() {
  workflow.Definition("messenger", codec.string(), codec.string())
}

/// Signals one workflow, cancels another, and reports what happened with a third that does
/// not exist. Its input is the prefix of the three workflow ids.
fn messenger_workflow(ctx: Context, prefix: String) -> Result(String, Failure) {
  use _ <- result.try(
    workflow.await(workflow.signal_external(
      ctx,
      prefix <> "-signalled",
      poke(),
      "hello",
    )),
  )
  use _ <- result.try(
    workflow.await(workflow.cancel_external(ctx, prefix <> "-cancelled")),
  )
  let missing =
    workflow.await(workflow.signal_external(
      ctx,
      prefix <> "-missing",
      poke(),
      "anyone?",
    ))
  Ok("missing workflow: " <> string.inspect(result.is_error(missing)))
}

fn nanny() {
  workflow.Definition("nanny", codec.string(), codec.string())
}

/// Starts a child and signals it.
fn nanny_workflow(ctx: Context, child_id: String) -> Result(String, Failure) {
  use child <- result.try(workflow.start_child(
    ctx,
    listener(),
    Nil,
    workflow.child_options(workflow_id: child_id),
  ))
  use _ <- result.try(
    workflow.await(workflow.signal_child(ctx, child, poke(), "from parent")),
  )
  workflow.await(workflow.child_result(child))
}

// ---- local activities ----

fn quick() {
  workflow.Definition("quick", codec.int(), codec.string())
}

/// Local activities: one that succeeds at once, and one that fails `attempts - 1` times
/// with a retry interval long enough that core hands the wait back as a timer.
fn quick_workflow(ctx: Context, attempts: Int) -> Result(String, Failure) {
  let local = workflow.local_activity_options(start_to_close_ms: 5000)
  use doubled <- result.try(workflow.execute_local_activity(
    ctx,
    double(),
    21,
    local,
  ))
  let retrying =
    workflow.LocalActivityOptions(
      ..local,
      retry: Some(
        workflow.RetryPolicy(
          ..workflow.retry_policy(maximum_attempts: 5),
          initial_interval_ms: 1500,
          backoff_coefficient: 1.0,
        ),
      ),
      local_retry_threshold_ms: Some(1000),
    )
  use outcome <- result.map(workflow.execute_local_activity(
    ctx,
    stubborn(),
    attempts,
    retrying,
  ))
  int.to_string(doubled) <> ", " <> outcome
}

fn hasty() {
  workflow.Definition("hasty", codec.nil(), codec.string())
}

/// Cancels a local activity while it waits to be retried.
fn hasty_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  let retrying =
    workflow.LocalActivityOptions(
      ..workflow.local_activity_options(start_to_close_ms: 5000),
      retry: Some(
        workflow.RetryPolicy(
          ..workflow.retry_policy(maximum_attempts: 5),
          initial_interval_ms: 60_000,
        ),
      ),
      local_retry_threshold_ms: Some(1000),
    )
  let outcome =
    workflow.with_timeout(ctx, 1500, fn(scope) {
      workflow.execute_local_activity(scope, stubborn(), 2, retrying)
    })
  case outcome {
    Ok(text) -> Ok(text)
    Error(failure) ->
      Ok("stopped waiting: " <> string.inspect(failure.is_timeout(failure)))
  }
}

// ---- memo and search attributes ----

fn annotating() {
  workflow.Definition("annotating", codec.nil(), codec.string())
}

fn annotating_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  workflow.upsert_memo(ctx, "stage", codec.string(), "started")
  let assert Ok(Nil) = workflow.sleep(ctx, 10)
  workflow.upsert_memo(ctx, "stage", codec.string(), "finished")
  Ok(option.unwrap(workflow.memo(ctx, "stage", codec.string()), "none"))
}

// ---- nexus ----

fn shout() {
  workflow.NexusOperation("text", "shout", codec.string(), codec.string())
}

fn double_slowly() {
  workflow.NexusOperation("maths", "double", codec.int(), codec.int())
}

fn refuse() {
  workflow.NexusOperation("text", "refuse", codec.string(), codec.string())
}

fn doubling() {
  workflow.Definition("doubling", codec.int(), codec.int())
}

fn doubling_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  workflow.execute_activity(ctx, double(), n, options())
}

fn caller() {
  workflow.Definition("caller", codec.string(), codec.string())
}

/// Calls a synchronous operation, a workflow-backed one, and one that fails.
fn caller_workflow(ctx: Context, endpoint: String) -> Result(String, Failure) {
  let options = workflow.nexus_options()
  use shouted <- result.try(workflow.execute_nexus_operation(
    ctx,
    endpoint,
    shout(),
    "hello",
    options,
  ))
  use handle <- result.try(workflow.start_nexus_operation(
    ctx,
    endpoint,
    double_slowly(),
    21,
    options,
  ))
  let asynchronous = option.is_some(workflow.nexus_operation_token(handle))
  use doubled <- result.try(workflow.await(workflow.nexus_result(handle)))
  let refused =
    workflow.execute_nexus_operation(ctx, endpoint, refuse(), "this", options)
  let reason = case refused {
    Ok(_) -> "not refused"
    Error(failure) -> failure.root_cause(failure).message
  }
  Ok(
    shouted
    <> ", "
    <> int.to_string(doubled)
    <> " async="
    <> string.inspect(asynchronous)
    <> ", "
    <> reason,
  )
}

// ---- versioning ----

fn versioned() {
  workflow.Definition("versioned", codec.nil(), codec.string())
}

fn go() {
  workflow.SignalDefinition("go", codec.nil())
}

/// Reports the build that ran its activity before and after a new build became current.
fn versioned_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  use before <- result.try(workflow.execute_activity(
    ctx,
    which_build(),
    Nil,
    options(),
  ))
  workflow.receive_signal(ctx, go())
  use after <- result.map(workflow.execute_activity(
    ctx,
    which_build(),
    Nil,
    options(),
  ))
  before <> " then " <> after
}

fn workflows() -> List(workflow.Handler) {
  [
    workflow.handler(counter(), counter_workflow),
    workflow.handler(listener(), listener_workflow),
    workflow.handler(messenger(), messenger_workflow),
    workflow.handler(nanny(), nanny_workflow),
    workflow.handler(quick(), quick_workflow),
    workflow.handler(hasty(), hasty_workflow),
    workflow.handler(annotating(), annotating_workflow),
    workflow.handler(caller(), caller_workflow),
    workflow.handler(doubling(), doubling_workflow),
  ]
}

// ---- harness ----

type Env {
  Env(client: Client, task_queue: String)
}

fn in_both_modes_with(
  name: String,
  client: Client,
  scenario: fn(Env) -> Nil,
) -> Nil {
  list.each([1000, 0], fn(max_cached) {
    let task_queue = name <> "-" <> int.to_string(max_cached) <> "-" <> unique()
    let assert Ok(worker) =
      worker.new(client, task_queue)
      |> worker.with_workflows(workflows())
      |> worker.with_activities(activities("unversioned"))
      |> worker.with_max_cached_workflows(max_cached)
      |> worker.start
    scenario(Env(client, task_queue))
    worker.stop(worker)
  })
}

fn in_both_modes(name: String, scenario: fn(Env) -> Nil) -> Nil {
  let assert Ok(client) = client.connect(client.local())
  in_both_modes_with(name, client, scenario)
}

fn start_options(env: Env) -> client.StartOptions {
  client.start_options(
    workflow_id: env.task_queue <> "-" <> unique(),
    task_queue: env.task_queue,
  )
}

fn assert_replays(handle: client.Handle(output)) -> Nil {
  let assert Ok(history) = client.history(handle)
  assert replayer.replay(workflows(), history) == Ok(Nil)
}

// ---- updates ----

pub fn updates_change_the_workflow_and_return_a_value_test() {
  use env <- in_both_modes("counter")
  let assert Ok(handle) =
    client.start(env.client, counter(), 100, start_options(env))
  assert client.execute_update(handle, add(), 1) == Ok(102)
  assert client.execute_update(handle, add(), 10) == Ok(122)

  // Refused by the validator: nothing is recorded and the workflow is unchanged.
  let assert Error(client.UpdateFailed(rejected)) =
    client.execute_update(handle, add(), -5)
  assert rejected.message == "amount must not be negative"

  // Accepted, then failed by the workflow.
  let assert Error(client.UpdateFailed(failed)) =
    client.execute_update(handle, add(), 13)
  assert failed.error_type == "Unlucky"

  // Accepted first, result collected later.
  let assert Ok(update) = client.start_update(handle, add(), 4)
  assert client.update_result(update) == Ok(130)

  // The same id reaches the same update instead of applying it twice.
  let assert Ok(first) =
    client.start_update_with_id(handle, add(), 5, update_id: "once")
  let assert Ok(again) =
    client.start_update_with_id(handle, add(), 5, update_id: "once")
  assert client.update_result(first) == Ok(140)
  assert client.update_result(again) == Ok(140)

  let assert Ok(Nil) = client.signal(handle, finish(), Nil)
  assert client.result(handle) == Ok(140)
  assert_replays(handle)
}

pub fn an_update_nobody_handles_is_rejected_test() {
  use env <- in_both_modes("unhandled-update")
  let assert Ok(handle) =
    client.start(env.client, listener(), Nil, start_options(env))
  let assert Error(client.UpdateFailed(rejected)) =
    client.execute_update(handle, add(), 1)
  assert string.contains(rejected.message, "no handler registered")
  let assert Ok(Nil) = client.signal(handle, poke(), "done")
  assert client.result(handle) == Ok("poked: done")
  assert_replays(handle)
}

// ---- other workflows ----

pub fn a_workflow_signals_and_cancels_other_workflows_test() {
  use env <- in_both_modes("messenger")
  let prefix = env.task_queue
  let start = fn(suffix) {
    let assert Ok(handle) =
      client.start(
        env.client,
        listener(),
        Nil,
        client.start_options(
          workflow_id: prefix <> suffix,
          task_queue: env.task_queue,
        ),
      )
    handle
  }
  let signalled = start("-signalled")
  let cancelled = start("-cancelled")
  let assert Ok(messenger) =
    client.start(env.client, messenger(), prefix, start_options(env))
  assert client.result(messenger) == Ok("missing workflow: True")
  assert client.result(signalled) == Ok("poked: hello")
  assert client.result(cancelled) == Error(client.Cancelled)
  assert_replays(messenger)
}

pub fn a_parent_signals_its_child_test() {
  use env <- in_both_modes("nanny")
  let assert Ok(handle) =
    client.start(
      env.client,
      nanny(),
      env.task_queue <> "-child",
      start_options(env),
    )
  assert client.result(handle) == Ok("poked: from parent")
  assert_replays(handle)
}

// ---- local activities ----

pub fn local_activities_run_and_retry_across_a_backoff_test() {
  use env <- in_both_modes("quick")
  let assert Ok(handle) =
    client.start(env.client, quick(), 3, start_options(env))
  assert client.result(handle) == Ok("42, attempt 3 local=True")
  assert_replays(handle)
}

pub fn a_local_activity_waiting_to_retry_can_be_cancelled_test() {
  use env <- in_both_modes("hasty")
  let assert Ok(handle) =
    client.start(env.client, hasty(), Nil, start_options(env))
  assert client.result(handle) == Ok("stopped waiting: True")
  assert_replays(handle)
}

// ---- memo ----

pub fn a_workflow_can_change_its_memo_test() {
  use env <- in_both_modes("annotating")
  let assert Ok(handle) =
    client.start(env.client, annotating(), Nil, start_options(env))
  assert client.result(handle) == Ok("finished")
  let assert Ok(description) = client.describe(handle)
  let assert Ok(stage) = dict.get(description.memo, "stage")
  assert codec.string().decode(stage) == Ok("finished")
  assert_replays(handle)
}

// ---- payload codec ----

fn key(id: String, byte: Int) -> encryption.Key {
  encryption.Key(id:, secret: bit_array.concat(list.repeat(<<byte>>, 32)))
}

fn encrypted_client(current: encryption.Key, retired) -> Client {
  let assert Ok(client) =
    client.connect(
      client.Options(
        ..client.local(),
        codec: Some(encryption.codec(current, retired)),
      ),
    )
  client
}

pub fn an_encryption_codec_hides_payloads_from_the_server_test() {
  let old = key("2025", 7)
  let new = key("2026", 9)
  use env <- in_both_modes_with("encrypted", encrypted_client(old, []))
  let options =
    start_options(env)
    |> client.with_memo("note", codec.string(), "memo-secret")
  let assert Ok(handle) = client.start(env.client, counter(), 4100, options)
  assert client.execute_update(handle, add(), 21) == Ok(4142)
  let assert Ok(Nil) = client.signal(handle, finish(), Nil)
  assert client.result(handle) == Ok(4142)
  let assert Ok(description) = client.describe(handle)
  let assert Ok(note) = dict.get(description.memo, "note")
  assert codec.string().decode(note) == Ok("memo-secret")

  // What the server holds is ciphertext: a client without the key cannot read it, and
  // the plaintext appears nowhere in the stored history.
  let assert Ok(plain) = client.connect(client.local())
  let outsider = client.handle(plain, counter(), client.workflow_id(handle))
  let assert Error(client.ClientError(_)) = client.result(outsider)
  let assert Ok(seen) = client.describe(outsider)
  let assert Ok(stored) = dict.get(seen.memo, "note")
  assert dict.get(stored.metadata, "encoding")
    == Ok(<<"binary/encrypted":utf8>>)
  let assert Ok(history) = client.history(outsider)
  assert !contains_bytes(history.encoded, <<"4142":utf8>>)
  assert !contains_bytes(history.encoded, <<"memo-secret":utf8>>)
  assert contains_bytes(history.encoded, <<"binary/encrypted":utf8>>)

  // The history replays only with the codec.
  let with_codec =
    replayer.Options(
      ..replayer.options(),
      codec: Some(encryption.codec(old, [])),
    )
  assert replayer.replay_with(workflows(), history, with_codec) == Ok(Nil)
  let assert Error(_) = replayer.replay(workflows(), history)

  // After a key rotation the old key still reads what it wrote.
  let rotated =
    client.handle(
      encrypted_client(new, [old]),
      counter(),
      client.workflow_id(handle),
    )
  assert client.result(rotated) == Ok(4142)
  // Without the retired key it cannot.
  let forgetful =
    client.handle(
      encrypted_client(new, []),
      counter(),
      client.workflow_id(handle),
    )
  let assert Error(client.ClientError(_)) = client.result(forgetful)
  Nil
}

fn contains_bytes(haystack: BitArray, needle: BitArray) -> Bool {
  let size = bit_array.byte_size(needle)
  case haystack {
    <<candidate:bytes-size(size), _:bytes>> if candidate == needle -> True
    <<_, rest:bytes>> -> contains_bytes(rest, needle)
    _ -> False
  }
}

pub fn a_payload_survives_the_protobuf_round_trip_test() {
  let payload =
    codec.Payload(
      metadata: dict.from_list([
        #("encoding", <<"json/plain":utf8>>),
        #("type", <<"Keyword":utf8>>),
      ]),
      data: bit_array.concat(list.repeat(<<"0123456789":utf8>>, 40)),
    )
  assert codec.from_bytes(codec.to_bytes(payload)) == Ok(payload)
  let empty = codec.nil().encode(Nil)
  assert codec.from_bytes(codec.to_bytes(empty)) == Ok(empty)
}

// ---- nexus ----

pub fn nexus_operations_run_synchronously_and_as_workflows_test() {
  let assert Ok(client) = client.connect(client.local())
  list.each([1000, 0], fn(max_cached) {
    let task_queue = "nexus-" <> int.to_string(max_cached) <> "-" <> unique()
    let endpoint = "gleam-test-" <> unique()
    let created =
      cmd(
        "temporal operator nexus endpoint create --name "
        <> endpoint
        <> " --target-namespace default --target-task-queue "
        <> task_queue
        <> " 2>&1",
      )
    assert string.contains(created, endpoint)
    let assert Ok(worker) =
      worker.new(client, task_queue)
      |> worker.with_workflows(workflows())
      |> worker.with_activities(activities("unversioned"))
      |> worker.with_nexus_handlers(nexus_handlers(task_queue))
      |> worker.with_max_cached_workflows(max_cached)
      |> worker.start
    let assert Ok(handle) =
      client.start(
        client,
        caller(),
        endpoint,
        client.start_options(workflow_id: task_queue, task_queue:),
      )
    assert client.result(handle) == Ok("HELLO, 42 async=True, refused this")
    assert_replays(handle)
    worker.stop(worker)
  })
}

/// The workflow-backed operation starts its workflow on the handler's own task queue.
fn nexus_handlers(task_queue: String) -> List(nexus.Handler) {
  [
    nexus.sync(shout(), fn(_, text) { Ok(string.uppercase(text)) }),
    nexus.sync(refuse(), fn(_, text) {
      Error(failure.new("refused " <> text) |> failure.with_type("Refused"))
    }),
    nexus.workflow_run(double_slowly(), doubling(), fn(ctx, n) {
      #(
        n,
        client.start_options(
          workflow_id: "nexus-double-" <> ctx.request_id,
          task_queue:,
        ),
      )
    }),
  ]
}

// ---- versioning ----

pub fn a_pinned_workflow_stays_on_its_build_and_an_upgrading_one_moves_test() {
  let assert Ok(client) = client.connect(client.local())
  let task_queue = "versioned-" <> unique()
  let deployment = "gleam-" <> unique()
  let start_build = fn(build) {
    let assert Ok(worker) =
      worker.new(client, task_queue)
      |> worker.with_workflows([
        workflow.handler(versioned(), versioned_workflow)
          |> workflow.with_versioning(workflow.Pinned),
        workflow.Handler(
          ..workflow.handler(versioned(), versioned_workflow),
          name: "upgrading",
        )
          |> workflow.with_versioning(workflow.AutoUpgrade),
      ])
      |> worker.with_activities(activities(build))
      |> worker.with_deployment(worker.Deployment(
        name: deployment,
        build_id: build,
        default_behavior: workflow.Pinned,
      ))
      |> worker.start
    worker
  }
  let make_current = fn(build) {
    eventually(fn() {
      let output =
        cmd(
          "temporal worker deployment set-current-version --deployment-name "
          <> deployment
          <> " --build-id "
          <> build
          <> " --yes 2>&1",
        )
      !string.contains(string.lowercase(output), "error")
    })
  }
  let upgrading = workflow.Definition("upgrading", codec.nil(), codec.string())

  let v1 = start_build("v1")
  assert make_current("v1")
  let assert Ok(pinned) =
    client.start(
      client,
      versioned(),
      Nil,
      client.start_options(workflow_id: task_queue <> "-pinned", task_queue:),
    )
  let assert Ok(moving) =
    client.start(
      client,
      upgrading,
      Nil,
      client.start_options(workflow_id: task_queue <> "-moving", task_queue:),
    )
  // Both have run their first activity on v1 once their history shows it completed.
  assert eventually(fn() {
    list.all([task_queue <> "-pinned", task_queue <> "-moving"], fn(id) {
      string.contains(
        cmd("temporal workflow show --workflow-id " <> id <> " 2>&1"),
        "ActivityTaskCompleted",
      )
    })
  })

  let v2 = start_build("v2")
  assert make_current("v2")
  // The server applies a new current version to the task queue a few seconds after the
  // command returns; a signal sent sooner is still routed by the old one.
  process.sleep(5000)
  let assert Ok(Nil) = client.signal(pinned, go(), Nil)
  let assert Ok(Nil) = client.signal(moving, go(), Nil)
  assert client.result(pinned) == Ok("v1 then v1")
  assert client.result(moving) == Ok("v1 then v2")
  worker.stop(v1)
  worker.stop(v2)
}

fn eventually(check: fn() -> Bool) -> Bool {
  list.any(list.repeat(Nil, 100), fn(_) {
    case check() {
      True -> True
      False -> {
        process.sleep(200)
        False
      }
    }
  })
}

// ---- losing a worker in the middle of a local activity ----

@external(erlang, "temporal_test_ffi", "read_file")
fn read_file(path: String) -> Result(BitArray, String)

@external(erlang, "temporal_test_ffi", "write_file")
fn write_file(path: String, bytes: BitArray) -> Nil

fn fragile() {
  workflow.Definition("fragile", codec.nil(), codec.string())
}

/// Hangs the first time it runs for a workflow, as if its worker had frozen. Runs are
/// counted in a file, because the two runs happen in different operating system processes.
fn hang_once() {
  activity.Definition("hang_once", codec.nil(), codec.string())
}

fn runs_file(workflow_id: String) -> String {
  "build/scratch/" <> workflow_id <> ".runs"
}

fn runs_so_far(workflow_id: String) -> Int {
  case read_file(runs_file(workflow_id)) {
    Ok(bytes) -> bit_array.byte_size(bytes)
    Error(_) -> 0
  }
}

fn hang_once_activity(ctx: activity.Context, _: Nil) {
  let runs = runs_so_far(ctx.info.workflow_id) + 1
  write_file(
    runs_file(ctx.info.workflow_id),
    bit_array.concat(list.repeat(<<"x":utf8>>, runs)),
  )
  case runs {
    1 -> {
      process.sleep(600_000)
      Ok("the first run should have been lost")
    }
    _ -> Ok("finished on run " <> int.to_string(runs))
  }
}

fn fragile_handlers() -> List(workflow.Handler) {
  [
    workflow.handler(fragile(), fn(ctx, _) {
      workflow.execute_local_activity(
        ctx,
        hang_once(),
        Nil,
        workflow.local_activity_options(start_to_close_ms: 60_000),
      )
    }),
  ]
}

fn fragile_worker(task_queue: String) -> worker.Worker {
  let assert Ok(client) = client.connect(client.local())
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows(fragile_handlers())
    |> worker.with_activities([
      activity.handler(hang_once(), hang_once_activity),
    ])
    |> worker.start
  worker
}

/// Entry point of the operating system process the test starts and then kills.
pub fn doomed_worker(task_queue: String) -> Nil {
  let _ = fragile_worker(task_queue)
  process.sleep_forever()
}

/// A local activity is not recorded until it finishes. If the worker running it dies, the
/// workflow task times out, another worker replays the workflow and runs the activity
/// again from the start: at least once, never zero times, and the workflow is not harmed.
pub fn a_local_activity_runs_again_after_its_worker_is_killed_test() {
  let task_queue = "fragile-" <> unique()
  let assert Ok(client) = client.connect(client.local())
  // A worker in a process of its own, so that it can be killed outright.
  let os_pid =
    cmd(
      "erl -noshell -pa build/dev/erlang/*/ebin -eval 'advanced_test:doomed_worker(<<\""
      <> task_queue
      <> "\">>)' >/dev/null 2>&1 & echo $!",
    )
    |> string.trim
  let options =
    client.StartOptions(
      ..client.start_options(workflow_id: task_queue, task_queue:),
      task_timeout_ms: Some(2000),
    )
  let assert Ok(handle) = client.start(client, fragile(), Nil, options)
  assert eventually(fn() { runs_so_far(task_queue) == 1 })

  let _ = cmd("kill -9 " <> os_pid)
  let replacement = fragile_worker(task_queue)
  assert client.result(handle) == Ok("finished on run 2")
  assert runs_so_far(task_queue) == 2
  let assert Ok(history) = client.history(handle)
  assert replayer.replay(fragile_handlers(), history) == Ok(Nil)
  worker.stop(replacement)
}
