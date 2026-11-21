//// Cross-SDK conformance. Every feature here is a workflow written twice: in Gleam below,
//// and in TypeScript in `test/conformance/typescript/workflows.js`. A feature conforms
//// when histories replay in both directions:
////
//// 1. the history the official TypeScript SDK recorded (`test/fixtures/conformance`)
////    replays through the Gleam twin;
//// 2. a twin that diverges is rejected against that same history, so the replay cannot be
////    passing trivially;
//// 3. the Gleam twin runs against a real server, with and without the workflow cache, and
////    its own history replays;
//// 4. that history, written to `build/conformance`, replays through the TypeScript twin.
////
//// Steps 1 to 3 are this file. Step 4 is `bin/conformance`, which runs the TypeScript SDK
//// over what step 3 wrote. Re-record the TypeScript fixtures with `npm run record` in
//// `test/conformance/typescript`.

import gleam/bit_array
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/duration
import support.{type Env, Env, cmd, read_file, unique, write_file}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/encryption
import temporal/failure.{type Failure}
import temporal/nexus
import temporal/replayer
import temporal/retry
import temporal/schema
import temporal/worker
import temporal/workflow.{type Context}
import temporal/workflow/child
import temporal/workflow/continue_as_new
import temporal/workflow/local_activity
import temporal/workflow/operation
import temporal/workflow/signal
import temporal/workflow/update

// ---- activities ----

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn slow() {
  activity.Definition("slow", codec.int(), codec.nil())
}

fn stubborn() {
  activity.Definition("stubborn", codec.int(), codec.string())
}

/// Takes two arguments.
fn join() {
  activity.Definition(
    "join",
    codec.args2(codec.string(), codec.int()),
    codec.string(),
  )
}

/// Scheduled only by the diverging twins.
fn stray() {
  activity.Definition("stray", codec.nil(), codec.nil())
}

fn activities() -> List(activity.Handler) {
  [
    activity.handler(double(), fn(_, n) { Ok(n * 2) }),
    activity.handler(maybe(), fn(_, present) {
      Ok(case present {
        True -> Some("value")
        False -> None
      })
    }),
    activity.handler(nothing(), fn(_, _) { Ok(Nil) }),
    activity.handler(slow(), fn(_, ms) { Ok(process.sleep(ms)) }),
    activity.handler(join(), fn(_, arguments) {
      let #(text, times) = arguments
      Ok(string.join(list.repeat(text, times), " "))
    }),
    activity.handler(stubborn(), fn(ctx, attempts) {
      case activity.info(ctx).attempt >= attempts {
        True -> Ok("attempt " <> int.to_string(activity.info(ctx).attempt))
        False -> Error(failure.new("not yet"))
      }
    }),
  ]
}

fn options() {
  workflow.activity_options(start_to_close: duration.milliseconds(10_000))
}

// ---- twins ----

fn added() {
  workflow.SignalDefinition("added", codec.int())
}

fn basics() {
  workflow.Definition("basics", codec.int(), codec.int())
}

fn double_child() {
  workflow.Definition("double_child", codec.int(), codec.int())
}

/// Timers, activities in sequence and in parallel, a signal, a child workflow.
fn basics_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  use _ <- result.try(workflow.sleep(ctx, duration.milliseconds(10)))
  use a <- result.try(workflow.execute_activity(ctx, double(), n, options()))
  let assert [Ok(b), Ok(c)] =
    workflow.await_all([
      workflow.start_activity(ctx, double(), a, options()),
      workflow.start_activity(ctx, double(), a + 1, options()),
    ])
  let signalled = signal.receive(ctx, added())
  let child =
    child.options(workflow_id: workflow.info(ctx).workflow_id <> "-child")
  use doubled <- result.try(child.execute(ctx, double_child(), c, child))
  Ok(b + c + signalled + doubled)
}

fn double_child_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  workflow.execute_activity(ctx, double(), n, options())
}

fn scopes() {
  workflow.Definition("scopes", codec.int(), codec.int())
}

/// Cancellation: a deadline that cancels an activity, a scope that cancels a timer, and
/// shielded cleanup after the workflow itself is cancelled.
fn scopes_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  let _ =
    workflow.with_timeout(ctx, duration.milliseconds(200), fn(scope) {
      workflow.execute_activity(
        scope,
        slow(),
        2000,
        workflow.activity_options(start_to_close: duration.milliseconds(30_000)),
      )
    })
  use _ <- result.try(
    workflow.with_scope(ctx, fn(scope) {
      let pending = workflow.timer(scope, duration.milliseconds(60_000))
      use _ <- result.try(workflow.sleep(ctx, duration.milliseconds(10)))
      workflow.cancel(scope)
      let _ = workflow.await(pending)
      Ok(Nil)
    }),
  )
  case workflow.sleep(ctx, duration.milliseconds(60_000)) {
    Ok(Nil) -> Ok(n)
    Error(cancelled) -> {
      let _ =
        workflow.non_cancellable(ctx, fn(shielded) {
          workflow.execute_activity(shielded, double(), n, options())
        })
      Error(cancelled)
    }
  }
}

fn nudge() {
  workflow.SignalDefinition("poke", codec.nil())
}

fn deadline() {
  workflow.Definition("deadline", codec.int(), codec.string())
}

/// Waiting with a deadline: once for a signal that arrives, once for one that does not.
fn deadline_workflow(ctx: Context, n: Int) -> Result(String, Failure) {
  let wait = fn(ms) {
    workflow.await_timeout(
      ctx,
      signal.next(ctx, nudge()),
      duration.milliseconds(ms),
    )
    |> result.is_ok
    |> string.inspect
    |> string.lowercase
  }
  let first = wait(60_000)
  let second = wait(50)
  use doubled <- result.map(workflow.execute_activity(
    ctx,
    double(),
    n,
    options(),
  ))
  first <> " " <> second <> " " <> int.to_string(doubled)
}

fn add() {
  workflow.UpdateDefinition("add", codec.int(), codec.int())
}

fn finish() {
  workflow.SignalDefinition("finish", codec.nil())
}

fn updates() {
  workflow.Definition("updates", codec.int(), codec.int())
}

/// Updates: validated, answered after an activity, and one that fails after acceptance.
fn updates_workflow(ctx: Context, total: Int) -> Result(Int, Failure) {
  update.set_validator(ctx, add(), fn(amount) {
    case amount < 0 {
      True -> Error("amount must not be negative")
      False -> Ok(Nil)
    }
  })
  let next =
    workflow.race([
      update.next(ctx, add()) |> workflow.map(Some),
      signal.next(ctx, finish()) |> workflow.map(fn(_) { None }),
    ])
  case workflow.await(next) {
    None -> Ok(total)
    Some(update) ->
      case update.argument(update) {
        13 -> {
          update.complete(
            ctx,
            update,
            Error(failure.new("unlucky") |> failure.with_type("Unlucky")),
          )
          updates_workflow(ctx, total)
        }
        amount -> {
          use doubled <- result.try(workflow.execute_activity(
            ctx,
            double(),
            amount,
            options(),
          ))
          update.complete(ctx, update, Ok(total + doubled))
          updates_workflow(ctx, total + doubled)
        }
      }
  }
}

fn poke() {
  workflow.SignalDefinition("poke", codec.string())
}

fn listener() {
  workflow.Definition("listener", codec.nil(), codec.string())
}

fn listener_workflow(ctx: Context, _: Nil) -> Result(String, Failure) {
  use text <- result.map(workflow.await_cancellable(
    ctx,
    signal.next(ctx, poke()),
  ))
  "poked: " <> text
}

fn external() {
  workflow.Definition("external", codec.string(), codec.string())
}

/// Other workflows: signal one, cancel one, signal one that does not exist, then start a
/// child and signal it.
fn external_workflow(ctx: Context, prefix: String) -> Result(String, Failure) {
  use _ <- result.try(
    workflow.await(signal.send(ctx, prefix <> "-signalled", poke(), "hello")),
  )
  use _ <- result.try(
    workflow.await(workflow.cancel_external(ctx, prefix <> "-cancelled")),
  )
  let missing =
    workflow.await(signal.send(ctx, prefix <> "-missing", poke(), "anyone?"))
  let listening =
    child.start(
      ctx,
      listener(),
      Nil,
      child.options(workflow_id: prefix <> "-child"),
    )
  use _ <- result.try(workflow.await(child.started(listening)))
  use _ <- result.try(
    workflow.await(child.signal(ctx, listening, poke(), "from parent")),
  )
  use heard <- result.map(workflow.await(child.result(listening)))
  "missing="
  <> string.lowercase(string.inspect(result.is_error(missing)))
  <> " "
  <> heard
}

fn local() {
  workflow.Definition("local", codec.int(), codec.string())
}

/// Local activities: one that succeeds at once, and one whose retries are far enough apart
/// that the wait becomes a timer in history.
fn local_workflow(ctx: Context, attempts: Int) -> Result(String, Failure) {
  let once = local_activity.options(start_to_close: duration.milliseconds(5000))
  let retrying =
    once
    |> local_activity.with_retry(
      retry.policy(maximum_attempts: 5)
      |> retry.with_initial_interval(duration.milliseconds(1500))
      |> retry.with_backoff_coefficient(1.0),
    )
    |> local_activity.with_local_retry_threshold(duration.milliseconds(1000))
  use doubled <- result.try(local_activity.execute(ctx, double(), 21, once))
  use outcome <- result.map(local_activity.execute(
    ctx,
    stubborn(),
    attempts,
    retrying,
  ))
  int.to_string(doubled) <> ", " <> outcome
}

fn lifecycle() {
  workflow.Definition("lifecycle", codec.int(), codec.string())
}

/// Patching, memo changes and continue-as-new.
fn lifecycle_workflow(ctx: Context, remaining: Int) -> Result(String, Failure) {
  workflow.upsert_memo(
    ctx,
    "stage",
    codec.string(),
    "remaining " <> int.to_string(remaining),
  )
  use _ <- result.try(case workflow.patched(ctx, "double-first") {
    True ->
      workflow.execute_activity(ctx, double(), remaining, options())
      |> result.replace(Nil)
    False -> workflow.sleep(ctx, duration.milliseconds(5))
  })
  workflow.deprecate_patch(ctx, "retired")
  use _ <- result.try(workflow.sleep(ctx, duration.milliseconds(10)))
  case remaining > 0 {
    True -> workflow.continue_as_new(ctx, lifecycle(), remaining - 1)
    False -> Ok("done")
  }
}

fn shout() {
  workflow.NexusOperation("text", "shout", codec.string(), codec.string())
}

fn refuse() {
  workflow.NexusOperation("text", "refuse", codec.string(), codec.string())
}

fn double_operation() {
  workflow.NexusOperation("maths", "double", codec.int(), codec.int())
}

fn doubling() {
  workflow.Definition("doubling", codec.int(), codec.int())
}

fn nexus_caller() {
  workflow.Definition("nexus_caller", codec.string(), codec.string())
}

/// Nexus: a synchronous operation, a workflow-backed one, and one that fails.
fn nexus_caller_workflow(
  ctx: Context,
  endpoint: String,
) -> Result(String, Failure) {
  let options = operation.options()
  use shouted <- result.try(operation.execute(
    ctx,
    endpoint,
    shout(),
    "hello",
    options,
  ))
  let pending = operation.start(ctx, endpoint, double_operation(), 21, options)
  use _ <- result.try(workflow.await(operation.started(pending)))
  use doubled <- result.try(workflow.await(operation.result(pending)))
  let reason = case
    operation.execute(ctx, endpoint, refuse(), "this", options)
  {
    Ok(_) -> "not refused"
    Error(_) -> "refused"
  }
  Ok(shouted <> ", " <> int.to_string(doubled) <> ", " <> reason)
}

fn nexus_handlers(task_queue: String) -> List(nexus.Handler) {
  [
    nexus.sync(shout(), fn(_, text) { Ok(string.uppercase(text)) }),
    nexus.sync(refuse(), fn(_, text) { Error(failure.new("refused " <> text)) }),
    nexus.workflow_run(double_operation(), doubling(), fn(ctx, n) {
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

fn several_arguments() {
  workflow.Definition(
    "arguments",
    codec.args3(codec.string(), codec.int(), codec.bool()),
    codec.string(),
  )
}

/// Several arguments, to a workflow and to an activity.
fn several_arguments_workflow(
  ctx: Context,
  arguments: #(String, Int, Bool),
) -> Result(String, Failure) {
  let #(greeting, times, loud) = arguments
  use joined <- result.map(workflow.execute_activity(
    ctx,
    join(),
    #(greeting, times),
    options(),
  ))
  case loud {
    True -> string.uppercase(joined)
    False -> joined
  }
}

fn maybe() {
  activity.Definition(
    "maybe",
    codec.bool(),
    schema.codec(schema.optional(schema.string())),
  )
}

fn nothing() {
  activity.Definition("nothing", codec.no_args(), codec.nil())
}

fn silent() {
  workflow.Definition(
    "silent",
    codec.no_args(),
    schema.codec(schema.optional(schema.string())),
  )
}

fn optional() {
  workflow.Definition(
    "optional",
    codec.args2(codec.string(), schema.codec(schema.optional(schema.string()))),
    codec.string(),
  )
}

/// Values that are not there: an argument the caller left out, and activities and a child
/// workflow that return nothing. TypeScript sends each as a `binary/null` payload.
fn optional_workflow(
  ctx: Context,
  arguments: #(String, Option(String)),
) -> Result(String, Failure) {
  let #(label, suffix) = arguments
  use absent <- result.try(workflow.execute_activity(
    ctx,
    maybe(),
    False,
    options(),
  ))
  use present <- result.try(workflow.execute_activity(
    ctx,
    maybe(),
    True,
    options(),
  ))
  use _ <- result.try(workflow.execute_activity(ctx, nothing(), Nil, options()))
  let child =
    child.options(workflow_id: workflow.info(ctx).workflow_id <> "-silent")
  use said <- result.try(child.execute(ctx, silent(), Nil, child))
  Ok(
    label
    <> option.unwrap(suffix, "")
    <> ": "
    <> string.join(
      list.map([absent, present, said], option.unwrap(_, "none")),
      ", ",
    ),
  )
}

fn handover() {
  workflow.Definition("handover", codec.int(), codec.string())
}

/// Continue-as-new with options: the next run gets a memo and timeouts of its own.
fn handover_workflow(ctx: Context, remaining: Int) -> Result(String, Failure) {
  use _ <- result.try(workflow.sleep(ctx, duration.milliseconds(5)))
  case remaining > 0 {
    True ->
      continue_as_new.run(
        ctx,
        handover(),
        remaining - 1,
        continue_as_new.options()
          |> continue_as_new.with_memo(
            dict.from_list([
              #(
                "generation",
                codec.encode(
                  codec.string(),
                  int.to_string(remaining - 1) <> " to go",
                ),
              ),
            ]),
          )
          |> continue_as_new.with_run_timeout(duration.minutes(1))
          |> continue_as_new.with_task_timeout(duration.seconds(5)),
      )
    False ->
      Ok(option.unwrap(
        workflow.memo(ctx, "generation", codec.string()),
        "first",
      ))
  }
}

fn workflows() -> List(workflow.Handler) {
  [
    workflow.handler(optional(), optional_workflow),
    workflow.handler(silent(), fn(_, _) { Ok(None) }),
    workflow.handler(handover(), handover_workflow),
    workflow.handler(several_arguments(), several_arguments_workflow),
    workflow.handler(basics(), basics_workflow),
    workflow.handler(double_child(), double_child_workflow),
    workflow.handler(scopes(), scopes_workflow),
    workflow.handler(deadline(), deadline_workflow),
    workflow.handler(updates(), updates_workflow),
    workflow.handler(listener(), listener_workflow),
    workflow.handler(external(), external_workflow),
    workflow.handler(local(), local_workflow),
    workflow.handler(lifecycle(), lifecycle_workflow),
    workflow.handler(nexus_caller(), nexus_caller_workflow),
    workflow.handler(doubling(), double_child_workflow),
  ]
}

/// Every twin with a stray activity scheduled first: code that has diverged from what any
/// of the recorded histories contain.
fn diverging_workflows() -> List(workflow.Handler) {
  list.map(workflows(), fn(handler) {
    workflow.wrap(handler, fn(ctx, next) {
      let _ = workflow.execute_activity(ctx, stray(), Nil, options())
      next()
    })
  })
}

// ---- harness ----

/// The key the TypeScript harness encrypts with.
fn shared_codec() -> codec.PayloadCodec {
  let assert Ok(key) =
    encryption.key(
      id: "conformance",
      secret: bit_array.concat(list.repeat(<<7>>, 32)),
    )
  encryption.codec(key, [])
}

fn fixture(name: String) -> client.History {
  let path = "test/fixtures/conformance/" <> name <> ".bin"
  let assert Ok(encoded) = read_file(path)
  // Guards against a truncated fixture, which would replay trivially.
  assert bit_array.byte_size(encoded) > 500
  client.History(workflow_id: name <> "-typescript", encoded:)
}

fn replay_options(codec: Option(codec.PayloadCodec)) -> replayer.Options {
  case codec {
    Some(codec) -> replayer.options() |> replayer.with_codec(codec)
    None -> replayer.options()
  }
}

/// Steps 1 and 2: the TypeScript SDK's history replays through the Gleam twin, and is
/// refused by a twin that diverges.
fn check_typescript_history(
  name: String,
  codec: Option(codec.PayloadCodec),
) -> Nil {
  let history = fixture(name)
  assert replayer.replay_with(workflows(), history, replay_options(codec))
    == Ok(Nil)
  let assert Error(replayer.Nondeterminism(_)) =
    replayer.replay_with(diverging_workflows(), history, replay_options(codec))
  Nil
}

/// Step 3: run the Gleam twin on a real server in both cache modes, replay its history,
/// and leave the cached run's history where `bin/conformance` hands it to TypeScript.
/// `drive` starts the workflow, does whatever the feature needs done to it, and returns
/// its handle once it has finished.
fn record_gleam_history(
  name: String,
  codec: Option(codec.PayloadCodec),
  drive: fn(Env) -> client.Handle(output),
) -> Nil {
  let assert Ok(client) =
    client.connect(case codec {
      Some(codec) -> client.local() |> client.with_codec(codec)
      None -> client.local()
    })
  list.each([1000, 0], fn(max_cached) {
    let task_queue =
      "conformance-"
      <> name
      <> "-"
      <> int.to_string(max_cached)
      <> "-"
      <> unique()
    let assert Ok(worker) =
      worker.new(client, task_queue)
      |> worker.with_workflows(workflows())
      |> worker.with_activities(activities())
      |> worker.with_nexus_handlers(nexus_handlers(task_queue))
      |> worker.with_max_cached_workflows(max_cached)
      |> worker.start
    let handle = drive(Env(client, task_queue))
    let assert Ok(history) = client.history(handle)
    assert replayer.replay_with(workflows(), history, replay_options(codec))
      == Ok(Nil)
    case max_cached {
      0 -> Nil
      _ -> write_file("build/conformance/" <> name <> ".bin", history.encoded)
    }
    worker.stop(worker)
  })
}

fn start(
  env: Env,
  definition: workflow.Definition(input, output),
  input: input,
) -> client.Handle(output) {
  let assert Ok(handle) =
    client.start(
      env.client,
      definition,
      input,
      client.start_options(
        workflow_id: env.task_queue,
        task_queue: env.task_queue,
      ),
    )
  handle
}

// ---- features ----

pub fn basics_conform_test() {
  check_typescript_history("basics", None)
  use env <- record_gleam_history("basics", None)
  let handle = start(env, basics(), 5)
  let assert Ok(Nil) = client.signal(handle, added(), 1000)
  assert client.result(handle) == Ok(1086)
  handle
}

pub fn cancellation_scopes_conform_test() {
  check_typescript_history("scopes", None)
  // Cancelled while a scope's timer and a plain timer were both pending. Core accepts
  // those two cancellations in either order, so this shows that both are issued, not
  // their order; `runtime_test` pins the order.
  check_typescript_history("early_cancel", None)
  use env <- record_gleam_history("scopes", None)
  let handle = start(env, scopes(), 5)
  // Long enough for the workflow to reach its final sleep.
  process.sleep(5000)
  let assert Ok(Nil) = client.cancel(handle)
  assert client.result(handle) == Error(client.WorkflowCancelled)
  handle
}

pub fn waiting_with_a_deadline_conforms_test() {
  check_typescript_history("deadline", None)
  use env <- record_gleam_history("deadline", None)
  let handle = start(env, deadline(), 4)
  let assert Ok(Nil) = client.signal(handle, nudge(), Nil)
  assert client.result(handle) == Ok("true false 8")
  handle
}

pub fn updates_conform_test() {
  check_typescript_history("updates", None)
  use env <- record_gleam_history("updates", None)
  let handle = start(env, updates(), 100)
  assert client.execute_update(handle, add(), 1) == Ok(102)
  let assert Error(client.UpdateFailed(_)) =
    client.execute_update(handle, add(), -5)
  let assert Error(client.UpdateFailed(_)) =
    client.execute_update(handle, add(), 13)
  assert client.execute_update(handle, add(), 10) == Ok(122)
  let assert Ok(Nil) = client.signal(handle, finish(), Nil)
  assert client.result(handle) == Ok(122)
  handle
}

pub fn signalling_and_cancelling_other_workflows_conforms_test() {
  check_typescript_history("external", None)
  use env <- record_gleam_history("external", None)
  list.each(["-signalled", "-cancelled"], fn(suffix) {
    let assert Ok(_) =
      client.start(
        env.client,
        listener(),
        Nil,
        client.start_options(
          workflow_id: env.task_queue <> suffix,
          task_queue: env.task_queue,
        ),
      )
  })
  let handle = start(env, external(), env.task_queue)
  assert client.result(handle) == Ok("missing=true poked: from parent")
  handle
}

pub fn local_activities_conform_test() {
  check_typescript_history("local", None)
  use env <- record_gleam_history("local", None)
  let handle = start(env, local(), 3)
  assert client.result(handle) == Ok("42, attempt 3")
  handle
}

pub fn patching_memo_and_continue_as_new_conform_test() {
  check_typescript_history("lifecycle", None)
  use env <- record_gleam_history("lifecycle", None)
  let handle = start(env, lifecycle(), 2)
  assert client.result(handle) == Ok("done")
  // The handle names the first run, which is the one whose history is compared.
  handle
}

pub fn nexus_calls_conform_test() {
  check_typescript_history("nexus_caller", None)
  use env <- record_gleam_history("nexus_caller", None)
  let endpoint = "conformance-gleam-" <> unique()
  let created =
    cmd(
      "temporal operator nexus endpoint create --name "
      <> endpoint
      <> " --target-namespace default --target-task-queue "
      <> env.task_queue
      <> " 2>&1",
    )
  assert string.contains(created, endpoint)
  let handle = start(env, nexus_caller(), endpoint)
  assert client.result(handle) == Ok("HELLO, 42, refused")
  handle
}

pub fn encrypted_payloads_conform_test() {
  let codec = Some(shared_codec())
  check_typescript_history("encrypted", codec)
  // Without the key the TypeScript history cannot be read at all.
  let assert Error(_) =
    replayer.replay_with(
      workflows(),
      fixture("encrypted"),
      replay_options(None),
    )
  use env <- record_gleam_history("encrypted", codec)
  let handle = start(env, basics(), 5)
  let assert Ok(Nil) = client.signal(handle, added(), 1000)
  assert client.result(handle) == Ok(1086)
  handle
}

pub fn several_arguments_conform_test() {
  check_typescript_history("arguments", None)
  use env <- record_gleam_history("arguments", None)
  let handle = start(env, several_arguments(), #("hey", 3, True))
  assert client.result(handle) == Ok("HEY HEY HEY")
  handle
}

pub fn missing_values_conform_test() {
  // The TypeScript run was started with one argument and its activities and child
  // returned `undefined`: the Gleam twin has to read all of that to follow the history.
  check_typescript_history("optional", None)
  use env <- record_gleam_history("optional", None)
  let handle = start(env, optional(), #("hey", None))
  assert client.result(handle) == Ok("hey: none, value, none")
  handle
}

pub fn continue_as_new_with_options_conforms_test() {
  check_typescript_history("handover", None)
  use env <- record_gleam_history("handover", None)
  let handle = start(env, handover(), 1)
  assert client.result(handle) == Ok("0 to go")
  handle
}
