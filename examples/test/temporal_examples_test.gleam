//// One test per example: each calls the example's `run` against the Temporal service on
//// localhost:7233 and checks what it returns. `bin/examples` starts the service.

import examples/activity_retries
import examples/async_completion
import examples/cancellation
import examples/child_workflows
import examples/continue_as_new
import examples/data
import examples/encryption as encryption_example
import examples/fan_out
import examples/heartbeats
import examples/hello
import examples/interop
import examples/local_activities
import examples/logging_and_metrics
import examples/middleware
import examples/nexus
import examples/otp
import examples/queries
import examples/saga
import examples/schedules
import examples/search_attributes
import examples/signals
import examples/standalone_activities
import examples/testing as testing_example
import examples/timers
import examples/updates
import examples/versioning
import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import gleam/string
import gleeunit
import temporal/client.{type Client}
import temporal/encryption
import temporal/failure
import temporal/replayer
import temporal/schedule
import temporal/testing

pub fn main() -> Nil {
  gleeunit.main()
}

/// Run a shell command and return its output.
@external(erlang, "temporal_examples_test_ffi", "cmd")
fn cmd(command: String) -> String

@external(erlang, "temporal_examples_test_ffi", "test_server_path")
fn test_server_path() -> Result(String, Nil)

fn local_client() -> Client {
  let assert Ok(client) = client.connect(client.local())
  client
}

/// Poll, for up to twenty seconds, for something that happens with a delay.
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

pub fn hello_test() {
  assert hello.run(local_client()) == Ok("Hello, Gleam!")
}

pub fn activity_retries_test() {
  assert activity_retries.run(local_client())
    == Ok(
      "stock reserved on attempt 3; not charged: CardDeclined; "
      <> "no receipt: no email address on file",
    )
}

pub fn timers_test() {
  let assert Ok(report) = timers.run(local_client())
  assert report.slept_ms >= 200
  assert report.bounded == "timed out"
  assert report.awaited == "nobody called"
  assert report.winner == "fast"
}

pub fn cancellation_test() {
  assert cancellation.run(local_client())
    == Ok(cancellation.Outcome(
      scoped: "the scope was cancelled, the workflow was not",
      cancelled: True,
      released: "12A",
    ))
}

pub fn fan_out_test() {
  assert fan_out.run(local_client())
    == Ok(fan_out.Summary(total: 2100, resized: 4, failed: 2))
}

pub fn signals_test() {
  assert signals.run(local_client()) == Ok("report.pdf approved (looks good)")
}

pub fn queries_test() {
  assert queries.run(local_client())
    == Ok(queries.Outcome(total: 5, at_least_five: True, final: 5))
}

pub fn updates_test() {
  assert updates.run(local_client())
    == Ok(
      updates.Outcome(sizes: [1, 2], refused: "an item needs a name", items: [
        "apples",
        "pears",
      ]),
    )
}

pub fn child_workflows_test() {
  assert child_workflows.run(local_client())
    == Ok(child_workflows.Outcome(
      order: "packed examples-child-workflows, "
        <> "shipped examples-child-workflows to 1 Main Street",
      audit: "audited examples-child-workflows",
    ))
}

pub fn saga_test() {
  assert saga.run(local_client())
    == Ok(
      saga.Outcome(failed_with: "NoCars", steps: [
        "book_flight", "book_hotel", "book_car", "cancel_hotel", "cancel_flight",
      ]),
    )
}

pub fn continue_as_new_test() {
  assert continue_as_new.run(local_client()) == Ok(1000)
}

pub fn local_activities_test() {
  assert local_activities.run(local_client()) == Ok(920)
}

pub fn heartbeats_test() {
  assert heartbeats.run(local_client())
    == Ok("10 rows, the last attempt resumed from row 5; feed stopped")
}

pub fn standalone_activities_test() {
  assert standalone_activities.run(local_client())
    == Ok(
      "sent to ada@example.com on attempt 2; receipt is Completed after 2 attempts; feed was Running, then cancelled",
    )
}

pub fn async_completion_test() {
  assert async_completion.run(local_client()) == Ok("approved")
}

pub fn schedules_test() {
  let client = local_client()
  // Left behind if an earlier run was interrupted.
  let _ = schedule.delete(client, schedules.schedule_id)
  assert schedules.run(client)
    == Ok(schedules.Outcome(paused: True, note: "maintenance", actions: 3))
}

pub fn data_test() {
  // Four pens and one ink, less ten percent.
  assert data.run(local_client())
    == Ok(data.Invoice(
      order_id: "order-7",
      total_cents: 1350,
      sent_to: Some("ada@example.com"),
    ))
}

pub fn encryption_test() {
  // Stand-ins for keys from a secret store.
  let old_key = encryption.Key(id: "2025", secret: <<1:256>>)
  let new_key = encryption.Key(id: "2026", secret: <<2:256>>)
  assert encryption_example.run(client.local(), old_key, new_key)
    == Ok(encryption_example.Outcome(
      result: "the vault code is 1234!",
      readable_without_key: False,
    ))
}

pub fn middleware_test() {
  assert middleware.run(local_client())
    == Ok(
      middleware.Outcome(
        result: "workflow saw acme; activity saw acme for ada",
        activities: ["send_email_v2"],
      ),
    )
}

pub fn logging_test() {
  assert logging_and_metrics.run(local_client()) == Ok(3)
}

/// Metrics can only be set up before the SDK is first used in a VM, so this part of the
/// example runs in a VM of its own.
pub fn metrics_test() {
  let address = "127.0.0.1:19474"
  let os_pid =
    cmd(
      "erl -noshell -pa build/dev/erlang/*/ebin -eval '"
      <> "{ok, _} = examples@logging_and_metrics:run_with_metrics(<<\""
      <> address
      <> "\">>), timer:sleep(infinity).' >/dev/null 2>&1 & echo $!",
    )
    |> string.trim
  let served =
    eventually(fn() {
      cmd("curl -s http://" <> address <> "/metrics")
      |> string.contains("temporal_workflow_completed")
    })
  let _ = cmd("kill -9 " <> os_pid)
  assert served
}

pub fn search_attributes_test() {
  // Registering an attribute that exists is harmless.
  let _ =
    cmd(
      "temporal operator search-attribute create --name "
      <> search_attributes.attribute
      <> " --type Keyword 2>&1",
    )
  assert search_attributes.run(local_client())
    == Ok(search_attributes.Outcome(
      count: 2,
      workflow_ids: [
        "examples-search-attributes-ticket-1",
        "examples-search-attributes-ticket-2",
      ],
      note: "be kind",
      stage: "closed",
    ))
}

pub fn versioning_test() {
  let assert Ok(outcome) = versioning.run(local_client())
  assert outcome.guarded == Ok(Nil)
  let assert Error(replayer.Nondeterminism(_)) = outcome.unguarded
  assert outcome.deprecated == Ok(Nil)
}

pub fn testing_an_activity_test() {
  assert testing_example.activity_alone() == #(Ok(3), [2, 1, 0])
  let assert Error(failure.Failure(kind: failure.Cancelled, ..)) =
    testing_example.activity_cancelled()
}

pub fn testing_a_workflow_test() {
  let assert Ok(env) = case test_server_path() {
    Ok(path) -> testing.start_time_skipping_with(executable: path)
    Error(Nil) -> testing.start_time_skipping()
  }
  let assert Ok(#(reminder, history)) =
    testing_example.workflow_with_time_skipping(env)
  testing.stop(env)
  assert reminder == "renew passport: escalated after a week"
  assert testing_example.replay_history(history) == Ok(Nil)
}

pub fn otp_test() {
  assert otp.run(local_client()) == Ok(#("rendered home", 1))
}

pub fn interop_test() {
  assert interop.run(local_client()) == Ok("txn-customer-42")
}

pub fn nexus_test() {
  let endpoint = "examples-nexus"
  // Creating an endpoint that exists fails, which is harmless.
  let _ =
    cmd(
      "temporal operator nexus endpoint create --name "
      <> endpoint
      <> " --target-namespace default --target-task-queue "
      <> nexus.handler_task_queue
      <> " 2>&1",
    )
  assert nexus.run(local_client(), endpoint) == Ok("hello nexus world: 3 words")
}
