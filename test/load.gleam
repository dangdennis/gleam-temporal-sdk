//// A load test with faults: many workflows at once, on several workers, while workers are
//// killed and replaced. Not part of the test suite; run it with `bin/load`.
////
//// Every workflow must finish with the right answer however many times its worker died
//// under it, and the node must end with about as many processes as it started with.

import gleam/erlang/process.{type Pid}
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import support
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/worker
import temporal/workflow.{type Context}
import temporal/workflow/local_activity

@external(erlang, "erlang", "system_info")
fn system_info(item: Item) -> Int

@external(erlang, "temporal_test_ffi", "memory_mb")
fn memory_mb() -> Int

@external(erlang, "temporal_test_ffi", "env_int")
fn env_int(name: String, default: Int) -> Int

type Item {
  ProcessCount
}

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn order() {
  workflow.Definition("order", codec.int(), codec.int())
}

/// An activity, a timer, a local activity and a second activity: `n * 4 + 1`.
fn order_workflow(ctx: Context, n: Int) -> Result(Int, Failure) {
  let options = workflow.activity_options(start_to_close: duration.seconds(30))
  use doubled <- result.try(workflow.execute_activity(ctx, double(), n, options))
  use _ <- result.try(workflow.sleep(ctx, duration.milliseconds(200)))
  use again <- result.try(local_activity.execute(
    ctx,
    double(),
    doubled,
    local_activity.options(start_to_close: duration.seconds(30)),
  ))
  use _ <- result.map(workflow.execute_activity(ctx, double(), again, options))
  again + 1
}

fn config(task_queue: String) -> worker.Config {
  worker.new(support.local_client(), task_queue)
  |> worker.with_workflows([workflow.handler(order(), order_workflow)])
  |> worker.with_activities([activity.handler(double(), fn(_, n) { Ok(n * 2) })])
  // Small enough that runs are evicted and replayed throughout.
  |> worker.with_max_cached_workflows(100)
  |> worker.with_max_concurrent_workflow_task_polls(env_int("LOAD_POLLS", 10))
  |> worker.with_max_concurrent_activity_task_polls(env_int("LOAD_POLLS", 10))
}

/// Keep a worker running: start one, and start another whenever it dies. Each worker has
/// an owner of its own, because a worker takes its owner with it when it is killed.
fn keep_worker(task_queue: String, managers: process.Subject(Pid)) -> Nil {
  let owner =
    process.spawn_unlinked(fn() {
      let assert Ok(worker) = worker.start(config(task_queue))
      process.send(managers, worker.manager(worker))
      process.sleep_forever()
    })
  let monitor = process.monitor(owner)
  let _ =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive_forever
  keep_worker(task_queue, managers)
}

/// Split the work between `count` processes and wait for all of them.
fn in_parallel(items: List(a), count: Int, work: fn(a) -> b) -> List(b) {
  let done = process.new_subject()
  let chunks = list.sized_chunk(items, int.max(list.length(items) / count, 1))
  list.each(chunks, fn(chunk) {
    process.spawn(fn() { process.send(done, list.map(chunk, work)) })
  })
  list.flat_map(chunks, fn(_) { process.receive_forever(done) })
}

pub fn main() -> Nil {
  let workflows = env_int("LOAD_WORKFLOWS", 2000)
  let workers = env_int("LOAD_WORKERS", 3)
  let kill_every_ms = env_int("LOAD_KILL_EVERY_MS", 3000)
  let task_queue = "load-" <> support.unique()
  let client = support.local_client()
  let processes_before = system_info(ProcessCount)

  // The killer owns the list of running workers: it kills the one that has been up
  // longest, every so often, for as long as the test runs.
  let ready = process.new_subject()
  let killer =
    process.spawn_unlinked(fn() {
      let managers = process.new_subject()
      process.send(ready, managers)
      kill_in_turn(managers, kill_every_ms, 0)
    })
  let managers = process.receive_forever(ready)
  let keepers =
    list.map(list.repeat(Nil, workers), fn(_) {
      process.spawn_unlinked(fn() { keep_worker(task_queue, managers) })
    })

  io.println(
    int.to_string(workflows)
    <> " workflows on "
    <> int.to_string(workers)
    <> " workers, one killed every "
    <> int.to_string(kill_every_ms)
    <> "ms",
  )
  let numbers =
    int.range(from: workflows, to: 0, with: [], run: fn(seen, n) { [n, ..seen] })
  let started_at = timestamp.system_time()
  let handles =
    in_parallel(numbers, 40, fn(n) {
      let assert Ok(handle) =
        client.start(
          client,
          order(),
          n,
          client.start_options(
            workflow_id: task_queue <> "-" <> int.to_string(n),
            task_queue:,
          )
            |> client.with_task_timeout(duration.seconds(5)),
        )
      #(n, handle)
    })
  let wrong =
    in_parallel(handles, 40, fn(pair) {
      let #(n, handle) = pair
      case client.result(handle) {
        Ok(answer) if answer == n * 4 + 1 -> Ok(Nil)
        other -> Error(#(n, string.inspect(other)))
      }
    })
    |> list.filter_map(fn(outcome) {
      case outcome {
        Ok(Nil) -> Error(Nil)
        Error(problem) -> Ok(problem)
      }
    })
  let elapsed =
    timestamp.difference(started_at, timestamp.system_time())
    |> duration.to_milliseconds
  let peak_memory = memory_mb()

  // The keepers go first, so that nothing restarts the workers the killer then stops.
  list.each(keepers, process.kill)
  process.send(stop_all(killer), Nil)
  process.sleep(3000)
  let leaked = system_info(ProcessCount) - processes_before

  io.println(
    "finished in "
    <> int.to_string(elapsed / 1000)
    <> "s: "
    <> int.to_string(workflows * 1000 / int.max(elapsed, 1))
    <> " workflows/s, "
    <> int.to_string(peak_memory)
    <> " MB in use at the end, "
    <> int.to_string(leaked)
    <> " processes more than at the start",
  )
  case wrong {
    [] -> io.println("every workflow returned the right answer")
    _ -> {
      io.println(
        int.to_string(list.length(wrong))
        <> " workflows went wrong, the first: "
        <> string.inspect(list.first(wrong)),
      )
      panic as "load test failed"
    }
  }
  // Workflow runs, activities and pollers should all be gone with their workers.
  case leaked > 50 {
    True -> panic as "processes leaked"
    False -> Nil
  }
}

/// Ask the killer to stop every worker it knows of and then itself.
fn stop_all(killer: Pid) -> process.Subject(Nil) {
  let done = process.new_subject()
  process.send_abnormal_exit(killer, "stop")
  done
}

fn kill_in_turn(
  managers: process.Subject(Pid),
  every_ms: Int,
  killed: Int,
) -> Nil {
  process.trap_exits(True)
  let stopping =
    process.new_selector()
    |> process.select_trapped_exits(fn(_) { Nil })
    |> process.selector_receive(every_ms)
  case stopping, process.receive(managers, 0) {
    // Told to stop: take every remaining worker down first.
    Ok(Nil), _ -> drain(managers) |> list.each(process.kill)
    Error(Nil), Ok(manager) -> {
      process.kill(manager)
      io.println("  killed worker " <> int.to_string(killed + 1))
      kill_in_turn(managers, every_ms, killed + 1)
    }
    Error(Nil), Error(Nil) -> kill_in_turn(managers, every_ms, killed)
  }
}

fn drain(subject: process.Subject(a)) -> List(a) {
  case process.receive(subject, 0) {
    Ok(item) -> [item, ..drain(subject)]
    Error(Nil) -> []
  }
}
