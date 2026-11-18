//// The workflow runtime driven by hand-written activations: no server and no NIF. The test
//// process plays the part of core, so each test states exactly which commands the runtime
//// must produce for a given sequence of jobs.

import gleam/dict
import gleam/erlang/process.{type Pid, type Subject}
import gleam/option.{None, Some}
import temporal/activity
import temporal/codec
import temporal/failure
import temporal/internal/bridge.{type Completion, Activation}
import temporal/internal/runtime
import temporal/workflow

fn double() {
  activity.Definition("double", codec.int(), codec.int())
}

fn added() {
  workflow.SignalDefinition("added", codec.int())
}

fn total() {
  workflow.QueryDefinition("total", codec.nil(), codec.int())
}

fn int_workflow(
  name: String,
  run: fn(workflow.Context, Int) -> Result(Int, failure.Failure),
) -> workflow.Handler {
  workflow.handler(workflow.Definition(name, codec.int(), codec.int()), run)
}

fn start(handler: workflow.Handler) -> #(Pid, Subject(Completion)) {
  let completions = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      bridge.rescue(fn() {
        runtime.run(
          dict.from_list([#(handler.name, handler.run)]),
          "queue",
          fn(completion) { process.send(completions, completion) },
        )
      })
    })
  #(pid, completions)
}

fn activate(
  run: #(Pid, Subject(Completion)),
  jobs: List(bridge.Job),
) -> List(bridge.Command) {
  bridge.send_activation(run.0, Activation("run", 1000, False, 3, jobs))
  let assert Ok(bridge.Success("run", commands)) = process.receive(run.1, 1000)
  commands
}

fn init(workflow_type: String, input: Int) -> bridge.Job {
  bridge.InitializeWorkflow(
    workflow_type,
    "wf",
    [codec.int().encode(input)],
    42,
    1,
  )
}

fn int_payload(value: Int) {
  Some(codec.int().encode(value))
}

pub fn timer_then_complete_test() {
  let run =
    start(
      int_workflow("sleeper", fn(ctx, n) {
        workflow.sleep(ctx, 500)
        Ok(n + 1)
      }),
    )
  assert activate(run, [init("sleeper", 1)]) == [bridge.StartTimer(1, 500)]
  assert activate(run, [bridge.FireTimer(1)])
    == [bridge.CompleteWorkflow(int_payload(2))]
}

pub fn activities_are_scheduled_together_test() {
  let options = workflow.activity_options(start_to_close_ms: 5000)
  let run =
    start(
      int_workflow("fan_out", fn(ctx, n) {
        let a = workflow.start_activity(ctx, double(), n, options)
        let b = workflow.start_activity(ctx, double(), n + 1, options)
        let assert [Ok(x), Ok(y)] = workflow.await_all([a, b])
        Ok(x + y)
      }),
    )
  let assert [
    bridge.ScheduleActivity(seq: 1, activity_type: "double", ..),
    bridge.ScheduleActivity(seq: 2, task_queue: "queue", ..),
  ] = activate(run, [init("fan_out", 1)])
  // Resolved out of order: the workflow still waits for both.
  assert activate(run, [
      bridge.ResolveActivity(2, bridge.Completed(int_payload(4))),
    ])
    == []
  assert activate(run, [
      bridge.ResolveActivity(1, bridge.Completed(int_payload(2))),
    ])
    == [bridge.CompleteWorkflow(int_payload(6))]
}

pub fn activity_failure_reaches_the_workflow_test() {
  let run =
    start(
      int_workflow("failing", fn(ctx, n) {
        workflow.execute_activity(
          ctx,
          double(),
          n,
          workflow.activity_options(start_to_close_ms: 5000),
        )
      }),
    )
  let _ = activate(run, [init("failing", 1)])
  let boom = failure.new("boom")
  assert activate(run, [bridge.ResolveActivity(1, bridge.Failed(boom))])
    == [bridge.FailWorkflow(boom)]
}

fn accumulator() -> workflow.Handler {
  int_workflow("accumulator", fn(ctx, start) { accumulate(ctx, start) })
}

fn accumulate(ctx: workflow.Context, sum: Int) -> Result(Int, failure.Failure) {
  workflow.set_query_handler(ctx, total(), fn(_) { sum })
  case workflow.receive_signal(ctx, added()) {
    0 -> Ok(sum)
    n -> accumulate(ctx, sum + n)
  }
}

fn signal(value: Int) -> bridge.Job {
  bridge.SignalWorkflow("added", [codec.int().encode(value)])
}

pub fn signals_are_buffered_and_delivered_in_order_test() {
  let run = start(accumulator())
  // Signals that arrive with the first task are there before the workflow first asks.
  assert activate(run, [init("accumulator", 10), signal(1), signal(2)]) == []
  assert activate(run, [signal(3), signal(0), signal(100)])
    == [bridge.CompleteWorkflow(int_payload(16))]
}

pub fn queries_see_the_state_after_the_jobs_delivered_with_them_test() {
  let run = start(accumulator())
  let _ = activate(run, [init("accumulator", 10)])
  assert activate(run, [signal(5), bridge.QueryWorkflow("q1", "total", [])])
    == [bridge.RespondToQuery("q1", Ok(codec.int().encode(15)))]
  let assert [bridge.RespondToQuery("q2", Error(_))] =
    activate(run, [bridge.QueryWorkflow("q2", "unknown", [])])
}

pub fn queries_are_answered_after_completion_test() {
  let run = start(accumulator())
  let _ = activate(run, [init("accumulator", 10), signal(0)])
  assert activate(run, [bridge.QueryWorkflow("q", "total", [])])
    == [bridge.RespondToQuery("q", Ok(codec.int().encode(10)))]
}

pub fn race_prefers_the_earlier_future_test() {
  let run =
    start(
      int_workflow("racer", fn(ctx, _) {
        let slow = workflow.timer(ctx, 1000) |> workflow.map(fn(_) { 1 })
        let fast = workflow.timer(ctx, 10) |> workflow.map(fn(_) { 2 })
        Ok(workflow.await(workflow.race([slow, fast])))
      }),
    )
  let _ = activate(run, [init("racer", 0)])
  // Both timers fire in one task; the first listed wins regardless of which was shorter.
  assert activate(run, [bridge.FireTimer(2), bridge.FireTimer(1)])
    == [bridge.CompleteWorkflow(int_payload(1))]
}

pub fn cancellation_test() {
  let run =
    start(
      int_workflow("cancellable", fn(ctx, _) {
        let work = workflow.timer(ctx, 60_000) |> workflow.map(fn(_) { Ok(1) })
        let cancelled =
          workflow.cancellation(ctx)
          |> workflow.map(fn(_) { Error(failure.cancelled()) })
        workflow.await(workflow.race([work, cancelled]))
      }),
    )
  let _ = activate(run, [init("cancellable", 0)])
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.CancelWorkflowExecution]
}

pub fn continue_as_new_test() {
  let definition = workflow.Definition("looper", codec.int(), codec.int())
  let run =
    start(
      workflow.handler(definition, fn(ctx, n) {
        workflow.continue_as_new(ctx, definition, n + 1)
      }),
    )
  assert activate(run, [init("looper", 1)])
    == [bridge.ContinueAsNew("looper", "queue", [codec.int().encode(2)])]
}

pub fn random_values_depend_only_on_the_seed_test() {
  let draw = fn() {
    let run =
      start(
        int_workflow("random", fn(ctx, _) {
          let a = workflow.random_int(ctx, 1_000_000)
          let b = workflow.random_int(ctx, 1_000_000)
          assert a != b
          Ok(a * 1_000_000 + b)
        }),
      )
    activate(run, [init("random", 0)])
  }
  assert draw() == draw()
}

pub fn uuid4_is_well_formed_test() {
  let run =
    start(
      workflow.handler(
        workflow.Definition("uuid", codec.nil(), codec.string()),
        fn(ctx, _) { Ok(workflow.uuid4(ctx)) },
      ),
    )
  let assert [bridge.CompleteWorkflow(Some(payload))] =
    activate(run, [bridge.InitializeWorkflow("uuid", "wf", [], 7, 1)])
  let assert Ok(<<
    _:bytes-size(14),
    "4",
    _:bytes-size(4),
    variant,
    _:bytes-size(16),
  >>) = codec.string().decode(payload) |> result_bits
  assert variant == 56 || variant == 57 || variant == 97 || variant == 98
}

fn result_bits(result: Result(String, String)) -> Result(BitArray, String) {
  case result {
    Ok(text) -> Ok(<<text:utf8>>)
    Error(e) -> Error(e)
  }
}

pub fn patch_takes_the_new_path_and_records_a_marker_test() {
  let handler =
    int_workflow("patched", fn(ctx, _) {
      case workflow.patched(ctx, "v2"), workflow.patched(ctx, "v2") {
        True, True -> Ok(2)
        _, _ -> Ok(1)
      }
    })
  let run = start(handler)
  assert activate(run, [init("patched", 0)])
    == [
      bridge.SetPatchMarker("v2", False),
      bridge.CompleteWorkflow(int_payload(2)),
    ]
}

pub fn replay_without_the_patch_marker_takes_the_old_path_test() {
  let handler =
    int_workflow("patched", fn(ctx, _) {
      case workflow.patched(ctx, "v2") {
        True -> Ok(2)
        False -> Ok(1)
      }
    })
  let replay = fn(jobs) {
    let run = start(handler)
    bridge.send_activation(run.0, Activation("run", 1000, True, 3, jobs))
    let assert Ok(bridge.Success("run", commands)) =
      process.receive(run.1, 1000)
    commands
  }
  assert replay([init("patched", 0)])
    == [bridge.CompleteWorkflow(int_payload(1))]
  assert replay([bridge.NotifyHasPatch("v2"), init("patched", 0)])
    == [
      bridge.SetPatchMarker("v2", False),
      bridge.CompleteWorkflow(int_payload(2)),
    ]
}

pub fn a_crash_fails_the_task_not_the_workflow_test() {
  let run = start(int_workflow("crasher", fn(_, _) { panic as "bug" }))
  bridge.send_activation(
    run.0,
    Activation("run", 1000, False, 3, [init("crasher", 0)]),
  )
  let assert Ok(bridge.TaskFailure("run", _)) = process.receive(run.1, 1000)
  // Core evicts the run after a task failure; the process acknowledges and exits.
  assert activate(run, [bridge.RemoveFromCache("task failed", 4)]) == []
  process.sleep(50)
  assert !process.is_alive(run.0)
}

pub fn an_unregistered_workflow_type_fails_the_task_test() {
  let run = start(accumulator())
  bridge.send_activation(
    run.0,
    Activation("run", 1000, False, 3, [init("other", 0)]),
  )
  let assert Ok(bridge.TaskFailure("run", _)) = process.receive(run.1, 1000)
}

pub fn eviction_while_blocked_stops_the_process_test() {
  let run = start(accumulator())
  let _ = activate(run, [init("accumulator", 0)])
  assert activate(run, [bridge.RemoveFromCache("cache full", 1)]) == []
  process.sleep(50)
  assert !process.is_alive(run.0)
}

pub fn invalid_input_fails_the_workflow_test() {
  let run = start(accumulator())
  let bad = codec.string().encode("not a number")
  let assert [
    bridge.FailWorkflow(failure.Failure(
      error_type: "InvalidArgument",
      cause: None,
      ..,
    )),
  ] =
    activate(run, [bridge.InitializeWorkflow("accumulator", "wf", [bad], 1, 1)])
}
