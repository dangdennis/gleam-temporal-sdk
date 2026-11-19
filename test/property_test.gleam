//// Randomized workflows against a real Temporal service.
////
//// Each seed expands into a random program of timers, activities, fan-outs, races, random
//// draws and child workflows. A workflow interprets the program and returns a trace, which
//// must equal the trace a pure model computes without Temporal. The programs run
//// concurrently on a worker with caching disabled, so every workflow task replays the run
//// from the start, and each finished history is replayed once more afterwards.

import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/result
import gleam/time/duration
import support.{unique}
import temporal/activity
import temporal/client
import temporal/codec
import temporal/failure.{type Failure}
import temporal/replayer
import temporal/worker
import temporal/workflow.{type Context}

type Step {
  Sleep(ms: Int)
  Double(n: Int)
  FanOut(ns: List(Int))
  /// An activity raced against a timer. Which wins depends on real timing, so the trace
  /// does not record it, but the workflow branches on it: replay must take the same branch.
  Race(n: Int, ms: Int)
  RandomSleep
  Child(n: Int)
  /// An activity under a deadline it always meets: the deadline's timer is cancelled.
  Deadline(n: Int)
  /// A timer cancelled through its scope before it can fire.
  CancelledTimer
  /// An activity raced against a timer, each in a scope of its own, with the loser
  /// cancelled. Timing decides the winner, so the trace does not record it.
  CancellingRace(n: Int, ms: Int)
}

/// A small linear congruential generator, so programs depend only on their seed.
fn next(state: Int) -> Int {
  { state * 1_103_515_245 + 12_345 } % 2_147_483_648
}

fn program(seed: Int) -> List(Step) {
  let length = 3 + next(seed) % 6
  generate(next(next(seed)), length, [])
}

fn generate(state: Int, remaining: Int, steps: List(Step)) -> List(Step) {
  case remaining {
    0 -> list.reverse(steps)
    _ -> {
      let a = next(state)
      let b = next(a)
      let step = case a / 7 % 9 {
        0 -> Sleep(1 + b % 20)
        1 -> Double(b % 1000)
        2 ->
          FanOut(
            int.range(from: 0, to: 2 + b % 4, with: [], run: fn(ns, i) {
              [b % 100 + i, ..ns]
            }),
          )
        3 -> Race(b % 1000, 1 + b % 15)
        4 -> RandomSleep
        5 -> Child(b % 1000)
        6 -> Deadline(b % 1000)
        7 -> CancelledTimer
        _ -> CancellingRace(b % 1000, 1 + b % 15)
      }
      generate(next(b), remaining - 1, [step, ..steps])
    }
  }
}

/// What a program's trace must be, computed without Temporal.
fn model(steps: List(Step)) -> List(Int) {
  list.flat_map(steps, fn(step) {
    case step {
      Sleep(_) -> [0]
      Double(n) | Child(n) | Deadline(n) -> [n * 2]
      FanOut(ns) -> [int.sum(ns) * 2]
      CancelledTimer -> [-1]
      Race(..) | RandomSleep | CancellingRace(..) -> []
    }
  })
}

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn double_child() {
  workflow.Definition("double_child", codec.int(), codec.int())
}

fn interpreter() {
  workflow.Definition(
    "interpreter",
    codec.int(),
    codec.list(json.int, decode.int),
  )
}

fn options() {
  workflow.activity_options(start_to_close: duration.milliseconds(10_000))
}

fn interpret(ctx: Context, seed: Int) -> Result(List(Int), Failure) {
  list.try_map(program(seed), fn(step) {
    case step {
      Sleep(ms) -> {
        let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(ms))
        Ok([0])
      }
      Double(n) -> {
        use doubled <- result.try(workflow.execute_activity(
          ctx,
          double(),
          n,
          options(),
        ))
        Ok([doubled])
      }
      FanOut(ns) -> {
        let results =
          list.map(ns, workflow.start_activity(ctx, double(), _, options()))
          |> workflow.await_all
        use doubled <- result.try(list.try_map(results, fn(r) { r }))
        Ok([int.sum(doubled)])
      }
      Race(n, ms) -> {
        let activity =
          workflow.start_activity(ctx, double(), n, options())
          |> workflow.map(fn(_) { True })
        let timer =
          workflow.timer(ctx, duration.milliseconds(ms))
          |> workflow.map(fn(_) { False })
        case workflow.await(workflow.race([activity, timer])) {
          True -> Nil
          False -> {
            let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(1))
            Nil
          }
        }
        Ok([])
      }
      RandomSleep -> {
        let assert Ok(Nil) =
          workflow.sleep(
            ctx,
            duration.milliseconds(1 + workflow.random_int(ctx, 10)),
          )
        Ok([])
      }
      Child(n) -> {
        let child = workflow.child_options(workflow_id: workflow.uuid4(ctx))
        use doubled <- result.try(workflow.execute_child(
          ctx,
          double_child(),
          n,
          child,
        ))
        Ok([doubled])
      }
      Deadline(n) -> {
        use doubled <- result.try(
          workflow.with_timeout(ctx, duration.milliseconds(600_000), fn(scope) {
            workflow.execute_activity(scope, double(), n, options())
          }),
        )
        Ok([doubled])
      }
      CancelledTimer ->
        workflow.with_scope(ctx, fn(scope) {
          let pending = workflow.timer(scope, duration.milliseconds(600_000))
          workflow.cancel(scope)
          case workflow.await(pending) {
            Error(_) -> Ok([-1])
            Ok(Nil) -> Ok([0])
          }
        })
      CancellingRace(n, ms) -> {
        use work <- workflow.with_scope(ctx)
        use wait <- workflow.with_scope(ctx)
        let activity = workflow.start_activity(work, double(), n, options())
        let timer = workflow.timer(wait, duration.milliseconds(ms))
        let activity_won =
          workflow.await(
            workflow.race([
              workflow.map(activity, fn(_) { True }),
              workflow.map(timer, fn(_) { False }),
            ]),
          )
        // Cancel the loser and wait for it to settle, so its outcome is replayed too.
        case activity_won {
          True -> {
            workflow.cancel(wait)
            let _ = workflow.await(timer)
            Nil
          }
          False -> {
            workflow.cancel(work)
            let _ = workflow.await(activity)
            Nil
          }
        }
        Ok([])
      }
    }
  })
  |> result.map(list.flatten)
}

fn workflows() -> List(workflow.Handler) {
  [
    workflow.handler(interpreter(), interpret),
    workflow.handler(double_child(), fn(ctx, n) {
      workflow.execute_activity(ctx, double(), n, options())
    }),
  ]
}

fn check_seeds(seeds: List(Int), max_cached: Int) -> Nil {
  let assert Ok(client) = client.connect(client.local())
  let task_queue = "property-" <> unique()
  let assert Ok(worker) =
    worker.new(client, task_queue)
    |> worker.with_workflows(workflows())
    |> worker.with_activities([
      activity.handler(double(), fn(_, n) { Ok(n * 2) }),
    ])
    |> worker.with_max_cached_workflows(max_cached)
    |> worker.start
  let handles =
    list.map(seeds, fn(seed) {
      let workflow_id = task_queue <> "-" <> int.to_string(seed)
      let assert Ok(handle) =
        client.start(
          client,
          interpreter(),
          seed,
          client.start_options(workflow_id:, task_queue:),
        )
      #(seed, handle)
    })
  list.each(handles, fn(pair) {
    let #(seed, handle) = pair
    assert client.result(handle) == Ok(model(program(seed)))
    let assert Ok(history) = client.history(handle)
    assert replayer.replay(workflows(), history) == Ok(Nil)
  })
  worker.stop(worker)
}

pub fn random_programs_match_the_model_when_every_task_replays_test() {
  check_seeds(int.range(from: 20, to: 0, with: [], run: list.prepend), 0)
}

pub fn random_programs_match_the_model_with_caching_test() {
  check_seeds(int.range(from: 40, to: 20, with: [], run: list.prepend), 1000)
}

pub fn generated_programs_cover_every_kind_of_step_test() {
  let steps =
    int.range(from: 40, to: 0, with: [], run: list.prepend)
    |> list.flat_map(program)
  let has = fn(matches) { list.any(steps, matches) }
  assert has(fn(s) {
    case s {
      Sleep(_) -> True
      _ -> False
    }
  })
  assert has(fn(s) {
    case s {
      Double(_) -> True
      _ -> False
    }
  })
  assert has(fn(s) {
    case s {
      FanOut(_) -> True
      _ -> False
    }
  })
  assert has(fn(s) {
    case s {
      Race(..) -> True
      _ -> False
    }
  })
  assert has(fn(s) {
    case s {
      RandomSleep -> True
      _ -> False
    }
  })
  assert has(fn(s) {
    case s {
      Child(_) -> True
      _ -> False
    }
  })
}
