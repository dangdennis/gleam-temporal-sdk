//// Run one example against the Temporal service on localhost:7233 and print what it
//// returned:
////
////     gleam run -- hello
////
//// See the README for the list. `examples/test` runs them all and checks the results.

import argv
import examples/activity_retries
import examples/async_completion
import examples/cancellation
import examples/child_workflows
import examples/cloud
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
import gleam/io
import gleam/result
import gleam/string
import temporal/client.{type Client}
import temporal/encryption
import temporal/testing

pub fn main() -> Nil {
  case argv.load().arguments {
    // These two set up the SDK themselves, so they run before anything connects.
    ["cloud"] -> cloud.main()
    ["logging_and_metrics"] -> logging_and_metrics.main()
    [name] -> {
      let assert Ok(client) = client.connect(client.local())
        as "no Temporal service on localhost:7233: run `temporal server start-dev`"
      io.println(run(name, client))
    }
    _ ->
      io.println("usage: gleam run -- <example>, such as: gleam run -- hello")
  }
}

fn run(name: String, client: Client) -> String {
  case name {
    "hello" -> string.inspect(hello.run(client))
    "activity_retries" -> string.inspect(activity_retries.run(client))
    "timers" -> string.inspect(timers.run(client))
    "cancellation" -> string.inspect(cancellation.run(client))
    "fan_out" -> string.inspect(fan_out.run(client))
    "signals" -> string.inspect(signals.run(client))
    "queries" -> string.inspect(queries.run(client))
    "updates" -> string.inspect(updates.run(client))
    "child_workflows" -> string.inspect(child_workflows.run(client))
    "saga" -> string.inspect(saga.run(client))
    "continue_as_new" -> string.inspect(continue_as_new.run(client))
    "local_activities" -> string.inspect(local_activities.run(client))
    "heartbeats" -> string.inspect(heartbeats.run(client))
    "async_completion" -> string.inspect(async_completion.run(client))
    "standalone_activities" -> string.inspect(standalone_activities.run(client))
    "schedules" -> string.inspect(schedules.run(client))
    "data" -> string.inspect(data.run(client))
    "encryption" -> {
      // Stand-ins for keys from a secret store.
      let old_key = encryption.Key(id: "old", secret: <<1:256>>)
      let new_key = encryption.Key(id: "new", secret: <<2:256>>)
      string.inspect(encryption_example.run(client.local(), old_key, new_key))
    }
    "middleware" -> string.inspect(middleware.run(client))
    "search_attributes" -> string.inspect(search_attributes.run(client))
    "versioning" -> string.inspect(versioning.run(client))
    "testing" -> {
      let assert Ok(env) = testing.start_time_skipping()
      let outcome = testing_example.workflow_with_time_skipping(env)
      testing.stop(env)
      string.inspect(#(
        testing_example.activity_alone(),
        result.map(outcome, fn(run) { run.0 }),
        result.map(outcome, fn(run) { testing_example.replay_history(run.1) }),
      ))
    }
    "otp" -> string.inspect(otp.run(client))
    "interop" -> string.inspect(interop.run(client))
    "nexus" -> string.inspect(nexus.run(client, "examples-nexus"))
    _ -> "no example named " <> name
  }
}
