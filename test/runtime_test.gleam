//// The workflow runtime driven by hand-written activations: no server and no NIF. The test
//// process plays the part of core, so each test states exactly which commands the runtime
//// must produce for a given sequence of jobs.

import gleam/dict
import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import gleam/time/duration
import temporal/activity
import temporal/codec
import temporal/failure
import temporal/internal/bridge.{type Completion, Activation}
import temporal/internal/runtime
import temporal/middleware
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
          middleware.none(),
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
  bridge.send_activation(run.0, Activation("run", 1000, False, 3, jobs, False))
  let assert Ok(bridge.Success("run", commands, _)) =
    process.receive(run.1, 1000)
  commands
}

fn init(workflow_type: String, input: Int) -> bridge.Job {
  bridge.InitializeWorkflow(
    workflow_type,
    "wf",
    [codec.int().encode(input)],
    42,
    1,
    "run",
    "",
    dict.new(),
  )
}

fn int_payload(value: Int) {
  Some(codec.int().encode(value))
}

pub fn timer_then_complete_test() {
  let run =
    start(
      int_workflow("sleeper", fn(ctx, n) {
        let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(500))
        Ok(n + 1)
      }),
    )
  assert activate(run, [init("sleeper", 1)]) == [bridge.StartTimer(1, 500)]
  assert activate(run, [bridge.FireTimer(1)])
    == [bridge.CompleteWorkflow(int_payload(2))]
}

pub fn activities_are_scheduled_together_test() {
  let options =
    workflow.activity_options(start_to_close: duration.milliseconds(5000))
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
          workflow.activity_options(start_to_close: duration.milliseconds(5000)),
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
        let slow =
          workflow.timer(ctx, duration.milliseconds(1000))
          |> workflow.map(fn(_) { 1 })
        let fast =
          workflow.timer(ctx, duration.milliseconds(10))
          |> workflow.map(fn(_) { 2 })
        Ok(workflow.await(workflow.race([slow, fast])))
      }),
    )
  let _ = activate(run, [init("racer", 0)])
  // Both timers fire in one task; the first listed wins regardless of which was shorter.
  assert activate(run, [bridge.FireTimer(2), bridge.FireTimer(1)])
    == [bridge.CompleteWorkflow(int_payload(1))]
}

pub fn workflow_cancellation_cancels_a_pending_timer_test() {
  let run =
    start(
      int_workflow("cancellable", fn(ctx, _) {
        use _ <- result.map(workflow.sleep(ctx, duration.milliseconds(60_000)))
        1
      }),
    )
  let _ = activate(run, [init("cancellable", 0)])
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.CancelTimer(1), bridge.CancelWorkflowExecution]
}

fn cancelled_activity() -> bridge.Resolution {
  bridge.Cancelled(
    failure.Failure(
      ..failure.new("activity cancelled"),
      kind: failure.Activity,
      cause: Some(failure.cancelled()),
    ),
  )
}

pub fn workflow_cancellation_cancels_a_pending_activity_test() {
  let run =
    start(
      int_workflow("cancellable", fn(ctx, n) {
        workflow.execute_activity(ctx, double(), n, options())
      }),
    )
  let _ = activate(run, [init("cancellable", 0)])
  // The activity is asked to cancel; the workflow keeps waiting for its outcome.
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.RequestCancelActivity(1)]
  // A wrapped cancellation ends the workflow as cancelled, because it was asked to cancel.
  assert activate(run, [bridge.ResolveActivity(1, cancelled_activity())])
    == [bridge.CancelWorkflowExecution]
}

pub fn cancelled_activity_fails_a_workflow_that_was_not_cancelled_test() {
  let run =
    start(
      int_workflow("scoped", fn(ctx, n) {
        workflow.with_scope(ctx, fn(scope) {
          let pending = workflow.start_activity(scope, double(), n, options())
          workflow.cancel(scope)
          workflow.await(pending)
        })
      }),
    )
  let assert [
    bridge.ScheduleActivity(seq: 1, ..),
    bridge.RequestCancelActivity(1),
  ] = activate(run, [init("scoped", 0)])
  let assert [bridge.FailWorkflow(failure)] =
    activate(run, [bridge.ResolveActivity(1, cancelled_activity())])
  assert failure.is_cancellation(failure)
}

pub fn non_cancellable_cleanup_runs_after_cancellation_test() {
  let run =
    start(
      int_workflow("cleanup", fn(ctx, n) {
        case workflow.sleep(ctx, duration.milliseconds(60_000)) {
          Ok(Nil) -> Ok(n)
          Error(cancelled) -> {
            let assert Ok(_) =
              workflow.non_cancellable(ctx, fn(shielded) {
                workflow.execute_activity(shielded, double(), n, options())
              })
            Error(cancelled)
          }
        }
      }),
    )
  let _ = activate(run, [init("cleanup", 3)])
  let assert [bridge.CancelTimer(1), bridge.ScheduleActivity(seq: 1, ..)] =
    activate(run, [bridge.CancelWorkflow("stop")])
  assert activate(run, [
      bridge.ResolveActivity(1, bridge.Completed(int_payload(6))),
    ])
    == [bridge.CancelWorkflowExecution]
}

pub fn nothing_is_scheduled_in_a_cancelled_scope_test() {
  let run =
    start(
      int_workflow("late", fn(ctx, n) {
        let assert Error(_) =
          workflow.with_scope(ctx, fn(scope) {
            workflow.cancel(scope)
            assert workflow.is_cancelled(scope)
            let assert Error(_) =
              workflow.sleep(scope, duration.milliseconds(10))
            workflow.execute_activity(scope, double(), n, options())
          })
        // The skipped timer and activity still used sequence number 1.
        let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10))
        workflow.execute_activity(ctx, double(), n, options())
      }),
    )
  assert activate(run, [init("late", 1)]) == [bridge.StartTimer(2, 10)]
  let assert [bridge.ScheduleActivity(seq: 2, activity_id: "2", ..)] =
    activate(run, [bridge.FireTimer(2)])
}

pub fn with_timeout_cancels_the_body_when_the_deadline_passes_test() {
  let run =
    start(
      int_workflow("deadline", fn(ctx, n) {
        workflow.with_timeout(ctx, duration.milliseconds(5000), fn(scope) {
          workflow.execute_activity(scope, double(), n, options())
        })
      }),
    )
  let assert [bridge.StartTimer(1, 5000), bridge.ScheduleActivity(seq: 1, ..)] =
    activate(run, [init("deadline", 1)])
  assert activate(run, [bridge.FireTimer(1)])
    == [bridge.RequestCancelActivity(1)]
  let assert [
    bridge.FailWorkflow(failure.Failure(
      kind: failure.Timeout,
      cause: Some(_),
      ..,
    )),
  ] = activate(run, [bridge.ResolveActivity(1, cancelled_activity())])
}

pub fn with_timeout_cancels_its_timer_when_the_body_finishes_test() {
  let run =
    start(
      int_workflow("deadline", fn(ctx, n) {
        workflow.with_timeout(ctx, duration.milliseconds(5000), fn(scope) {
          workflow.execute_activity(scope, double(), n, options())
        })
      }),
    )
  let _ = activate(run, [init("deadline", 1)])
  assert activate(run, [
      bridge.ResolveActivity(1, bridge.Completed(int_payload(2))),
    ])
    == [bridge.CancelTimer(1), bridge.CompleteWorkflow(int_payload(2))]
}

pub fn await_timeout_returns_the_signal_or_times_out_test() {
  let waiter = fn() {
    start(
      int_workflow("waiter", fn(ctx, _) {
        case
          workflow.await_timeout(
            ctx,
            workflow.signal(ctx, added()),
            duration.milliseconds(5000),
          )
        {
          Ok(value) -> Ok(value)
          Error(failure.Failure(kind: failure.Timeout, ..)) -> Ok(-1)
          Error(other) -> Error(other)
        }
      }),
    )
  }
  let run = waiter()
  assert activate(run, [init("waiter", 0)]) == [bridge.StartTimer(1, 5000)]
  assert activate(run, [
      bridge.SignalWorkflow("added", [codec.int().encode(7)]),
    ])
    == [bridge.CancelTimer(1), bridge.CompleteWorkflow(int_payload(7))]

  let run = waiter()
  let _ = activate(run, [init("waiter", 0)])
  assert activate(run, [bridge.FireTimer(1)])
    == [bridge.CompleteWorkflow(int_payload(-1))]

  let run = waiter()
  let _ = activate(run, [init("waiter", 0)])
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.CancelTimer(1), bridge.CancelWorkflowExecution]
}

pub fn cancelling_a_scope_reaches_nested_scopes_but_not_shielded_ones_test() {
  let run =
    start(
      int_workflow("nested", fn(ctx, _) {
        workflow.with_scope(ctx, fn(outer) {
          let a = workflow.timer(outer, duration.milliseconds(100))
          workflow.with_scope(outer, fn(inner) {
            let b = workflow.timer(inner, duration.milliseconds(200))
            workflow.non_cancellable(inner, fn(shielded) {
              let c = workflow.timer(shielded, duration.milliseconds(300))
              workflow.cancel(outer)
              let assert Error(_) = workflow.await(a)
              let assert Error(_) = workflow.await(b)
              let assert Ok(Nil) = workflow.await(c)
              Ok(1)
            })
          })
        })
      }),
    )
  assert activate(run, [init("nested", 0)])
    == [
      bridge.StartTimer(1, 100),
      bridge.StartTimer(2, 200),
      bridge.StartTimer(3, 300),
      bridge.CancelTimer(1),
      bridge.CancelTimer(2),
    ]
  assert activate(run, [bridge.FireTimer(3)])
    == [bridge.CompleteWorkflow(int_payload(1))]
}

pub fn child_is_started_before_its_result_is_awaited_test() {
  let child = workflow.Definition("child", codec.int(), codec.int())
  let run =
    start(
      int_workflow("parent", fn(ctx, n) {
        let options =
          workflow.ChildOptions(
            ..workflow.child_options(workflow_id: "kid"),
            parent_close_policy: workflow.AbandonChild,
            id_reuse_policy: workflow.RejectDuplicate,
            run_timeout: Some(duration.seconds(9)),
          )
        use started <- result.try(workflow.start_child(ctx, child, n, options))
        assert workflow.child_run_id(started) == "child-run"
        assert workflow.child_workflow_id(started) == "kid"
        workflow.await(workflow.child_result(started))
      }),
    )
  let assert [
    bridge.StartChildWorkflow(
      seq: 1,
      workflow_id: "kid",
      workflow_type: "child",
      options: bridge.ChildStartOptions(
        parent_close_policy: 2,
        id_reuse_policy: 3,
        run_timeout_ms: Some(9000),
        cancellation_type: 2,
        ..,
      ),
      ..,
    ),
  ] = activate(run, [init("parent", 4)])
  assert activate(run, [
      bridge.ResolveChildWorkflowStart(1, bridge.ChildStarted("child-run")),
    ])
    == []
  assert activate(run, [
      bridge.ResolveChildWorkflow(1, bridge.Completed(int_payload(5))),
    ])
    == [bridge.CompleteWorkflow(int_payload(5))]
}

pub fn child_that_already_exists_is_a_typed_failure_test() {
  let child = workflow.Definition("child", codec.int(), codec.int())
  let run =
    start(
      int_workflow("parent", fn(ctx, n) {
        workflow.execute_child(
          ctx,
          child,
          n,
          workflow.child_options(workflow_id: "kid"),
        )
      }),
    )
  let _ = activate(run, [init("parent", 4)])
  let assert [
    bridge.FailWorkflow(failure.Failure(
      error_type: "WorkflowExecutionAlreadyStarted",
      ..,
    )),
  ] =
    activate(run, [
      bridge.ResolveChildWorkflowStart(1, bridge.ChildStartFailed("kid", 1)),
    ])
}

pub fn cancelling_the_workflow_cancels_a_running_child_test() {
  let child = workflow.Definition("child", codec.int(), codec.int())
  let run =
    start(
      int_workflow("parent", fn(ctx, n) {
        workflow.execute_child(
          ctx,
          child,
          n,
          workflow.child_options(workflow_id: "kid"),
        )
      }),
    )
  let _ = activate(run, [init("parent", 4)])
  let _ =
    activate(run, [
      bridge.ResolveChildWorkflowStart(1, bridge.ChildStarted("child-run")),
    ])
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.CancelChildWorkflow(1)]
}

pub fn info_and_memo_come_from_the_start_event_test() {
  let run =
    start(
      int_workflow("informed", fn(ctx, _) {
        let info = workflow.info(ctx)
        assert info.first_execution_run_id == "first-run"
        assert info.continued_from_run_id == Some("previous-run")
        assert info.continue_as_new_suggested
        assert workflow.memo(ctx, "trace", codec.string()) == Some("abc")
        assert workflow.memo(ctx, "missing", codec.string()) == None
        Ok(info.history_length)
      }),
    )
  bridge.send_activation(
    run.0,
    Activation(
      "run",
      1000,
      False,
      3,
      [
        bridge.InitializeWorkflow(
          "informed",
          "wf",
          [codec.int().encode(0)],
          42,
          1,
          "first-run",
          "previous-run",
          dict.from_list([#("trace", codec.string().encode("abc"))]),
        ),
      ],
      True,
    ),
  )
  let assert Ok(bridge.Success("run", [bridge.CompleteWorkflow(result)], _)) =
    process.receive(run.1, 1000)
  assert result == int_payload(3)
}

pub fn deprecated_patch_records_a_deprecated_marker_once_test() {
  let run =
    start(
      int_workflow("patched", fn(ctx, n) {
        workflow.deprecate_patch(ctx, "v2")
        workflow.deprecate_patch(ctx, "v2")
        Ok(n)
      }),
    )
  assert activate(run, [init("patched", 1)])
    == [
      bridge.SetPatchMarker("v2", True),
      bridge.CompleteWorkflow(int_payload(1)),
    ]
}

pub fn outbound_middleware_rewrites_scheduled_activities_test() {
  let handler =
    int_workflow("renamer", fn(ctx, n) {
      workflow.execute_activity(ctx, double(), n, options())
    })
  let completions = process.new_subject()
  let pid =
    process.spawn_unlinked(fn() {
      bridge.rescue(fn() {
        runtime.run(
          dict.from_list([#(handler.name, handler.run)]),
          "queue",
          middleware.Outbound(..middleware.none(), activity: fn(call) {
            middleware.ActivityCall(
              ..call,
              activity_type: "legacy_" <> call.activity_type,
            )
          }),
          fn(completion) { process.send(completions, completion) },
        )
      })
    })
  let assert [bridge.ScheduleActivity(activity_type: "legacy_double", ..)] =
    activate(#(pid, completions), [init("renamer", 1)])
}

pub fn workflow_middleware_wraps_the_workflow_test() {
  let handler =
    int_workflow("wrapped", fn(_, n) { Ok(n) })
    |> workflow.wrap(fn(_ctx, next) {
      case next() {
        Ok(_) -> Error(failure.new("replaced by middleware"))
        Error(failure) -> Error(failure)
      }
    })
  let assert [
    bridge.FailWorkflow(failure.Failure(message: "replaced by middleware", ..)),
  ] = activate(start(handler), [init("wrapped", 1)])
}

fn options() {
  workflow.activity_options(start_to_close: duration.milliseconds(5000))
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
    activate(run, [
      bridge.InitializeWorkflow("uuid", "wf", [], 7, 1, "run", "", dict.new()),
    ])
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
    bridge.send_activation(run.0, Activation("run", 1000, True, 3, jobs, False))
    let assert Ok(bridge.Success("run", commands, _)) =
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
    Activation("run", 1000, False, 3, [init("crasher", 0)], False),
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
    Activation("run", 1000, False, 3, [init("other", 0)], False),
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
    activate(run, [
      bridge.InitializeWorkflow(
        "accumulator",
        "wf",
        [bad],
        1,
        1,
        "run",
        "",
        dict.new(),
      ),
    ])
}

@external(erlang, "temporal_test_ffi", "capture_logs")
fn capture_logs(body: fn() -> Nil) -> List(#(bridge.LogLevel, String, String))

pub fn workflow_logs_once_and_not_on_replay_test() {
  let chatty = fn() {
    start(
      int_workflow("chatty", fn(ctx, n) {
        workflow.log_info(ctx, "starting")
        let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(10))
        workflow.log_warning(ctx, "slept")
        Ok(n)
      }),
    )
  }
  assert capture_logs(fn() {
      let run = chatty()
      let _ = activate(run, [init("chatty", 1)])
      let _ = activate(run, [bridge.FireTimer(1)])
      Nil
    })
    == [#(bridge.Info, "starting", "wf"), #(bridge.Warning, "slept", "wf")]

  // The first task is replayed from history; only the new progress logs.
  assert capture_logs(fn() {
      let run = chatty()
      bridge.send_activation(
        run.0,
        Activation("run", 1000, True, 3, [init("chatty", 1)], False),
      )
      let assert Ok(_) = process.receive(run.1, 1000)
      let _ = activate(run, [bridge.FireTimer(1)])
      Nil
    })
    == [#(bridge.Warning, "slept", "wf")]
}

fn add() {
  workflow.UpdateDefinition("add", codec.int(), codec.int())
}

fn update_job(protocol_id: String, amount: Int, validate: Bool) -> bridge.Job {
  bridge.DoUpdate(
    id: "update-" <> protocol_id,
    protocol_instance_id: protocol_id,
    name: "add",
    input: [codec.int().encode(amount)],
    run_validator: validate,
  )
}

fn adder() {
  start(
    int_workflow("adder", fn(ctx, total) {
      workflow.set_update_validator(ctx, add(), fn(amount) {
        case amount < 0 {
          True -> Error("negative")
          False -> Ok(Nil)
        }
      })
      let update = workflow.receive_update(ctx, add())
      let amount = workflow.update_argument(update)
      use doubled <- result.try(workflow.execute_activity(
        ctx,
        double(),
        amount,
        options(),
      ))
      workflow.complete_update(ctx, update, Ok(total + doubled))
      Ok(total + doubled)
    }),
  )
}

pub fn an_update_is_accepted_then_completed_after_its_work_test() {
  let run = adder()
  assert activate(run, [init("adder", 1)]) == []
  // Accepted as the job is applied, before the workflow's own commands.
  let assert [bridge.UpdateAccepted("p1"), bridge.ScheduleActivity(seq: 1, ..)] =
    activate(run, [update_job("p1", 5, True)])
  assert activate(run, [
      bridge.ResolveActivity(1, bridge.Completed(int_payload(10))),
    ])
    == [
      bridge.UpdateCompleted("p1", codec.int().encode(11)),
      bridge.CompleteWorkflow(int_payload(11)),
    ]
}

pub fn a_validator_rejects_an_update_without_waking_the_workflow_test() {
  let run = adder()
  let _ = activate(run, [init("adder", 1)])
  let assert [
    bridge.UpdateRejected("p1", failure.Failure(message: "negative", ..)),
  ] = activate(run, [update_job("p1", -1, True)])
  // The workflow is still waiting for an update it will accept.
  let assert [bridge.UpdateAccepted("p2"), bridge.ScheduleActivity(..)] =
    activate(run, [update_job("p2", 1, True)])
}

pub fn validators_do_not_run_on_replay_test() {
  let run = adder()
  let _ = activate(run, [init("adder", 1)])
  // History says this update was accepted, so it is, whatever the validator thinks now.
  let assert [bridge.UpdateAccepted("p1"), bridge.ScheduleActivity(..)] =
    activate(run, [update_job("p1", -1, False)])
}

pub fn an_update_for_an_unknown_name_is_rejected_once_the_workflow_waits_test() {
  let run =
    start(
      int_workflow("deaf", fn(ctx, n) {
        use _ <- result.map(workflow.sleep(ctx, duration.milliseconds(1000)))
        n
      }),
    )
  // Delivered with the first task: the workflow gets to run before it is judged.
  let assert [
    bridge.StartTimer(1, 1000),
    bridge.UpdateRejected(
      "p1",
      failure.Failure(error_type: "UpdateRejected", ..),
    ),
  ] = activate(run, [init("deaf", 1), update_job("p1", 1, True)])
}

pub fn an_update_that_arrives_with_the_first_task_is_taken_test() {
  let run = adder()
  let assert [bridge.UpdateAccepted("p1"), bridge.ScheduleActivity(..)] =
    activate(run, [init("adder", 1), update_job("p1", 2, True)])
}

pub fn a_local_activity_retries_after_the_backoff_core_asks_for_test() {
  let run =
    start(
      int_workflow("local", fn(ctx, n) {
        workflow.execute_local_activity(
          ctx,
          double(),
          n,
          workflow.local_activity_options(start_to_close: duration.milliseconds(
            1000,
          )),
        )
      }),
    )
  let assert [
    bridge.ScheduleLocalActivity(seq: 1, activity_id: "1", attempt: 1, ..),
  ] = activate(run, [init("local", 4)])
  // Core hands a long wait back: the runtime waits with a timer of its own.
  assert activate(run, [bridge.ResolveActivity(1, bridge.Backoff(2, 9000, 555))])
    == [bridge.StartTimer(1, 9000)]
  // The retry is a new activity as far as numbering goes, carrying core's attempt.
  let assert [
    bridge.ScheduleLocalActivity(
      seq: 2,
      activity_id: "2",
      attempt: 2,
      original_schedule_time_ms: Some(555),
      ..,
    ),
  ] = activate(run, [bridge.FireTimer(1)])
  assert activate(run, [
      bridge.ResolveActivity(2, bridge.Completed(int_payload(8))),
    ])
    == [bridge.CompleteWorkflow(int_payload(8))]
}

pub fn cancelling_a_local_activity_depends_on_what_it_is_waiting_for_test() {
  let scoped = fn() {
    start(
      int_workflow("local", fn(ctx, n) {
        workflow.execute_local_activity(
          ctx,
          double(),
          n,
          workflow.local_activity_options(start_to_close: duration.milliseconds(
            1000,
          )),
        )
      }),
    )
  }
  // Running: core is asked to cancel it.
  let run = scoped()
  let _ = activate(run, [init("local", 4)])
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.RequestCancelLocalActivity(1)]
  // Between attempts: the timer is cancelled and the workflow sees the cancellation.
  let run = scoped()
  let _ = activate(run, [init("local", 4)])
  let _ =
    activate(run, [bridge.ResolveActivity(1, bridge.Backoff(2, 9000, 555))])
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.CancelTimer(1), bridge.CancelWorkflowExecution]
}

pub fn signals_to_other_workflows_are_numbered_apart_from_cancellations_test() {
  let poke = workflow.SignalDefinition("poke", codec.int())
  let run =
    start(
      int_workflow("talker", fn(ctx, n) {
        let sent = workflow.signal_external(ctx, "other", poke, n)
        let stopped = workflow.cancel_external(ctx, "other")
        let assert Ok(Nil) = workflow.await(sent)
        let assert Error(_) = workflow.await(stopped)
        Ok(n)
      }),
    )
  let assert [
    bridge.SignalExternal(
      seq: 1,
      target: bridge.ExternalWorkflow("other", ""),
      signal_name: "poke",
      ..,
    ),
    bridge.CancelExternal(seq: 1, workflow_id: "other", ..),
  ] = activate(run, [init("talker", 1)])
  assert activate(run, [bridge.ResolveExternalSignal(1, None)]) == []
  assert activate(run, [
      bridge.ResolveExternalCancel(1, Some(failure.new("no such workflow"))),
    ])
    == [bridge.CompleteWorkflow(int_payload(1))]
}

pub fn a_pending_signal_to_another_workflow_is_cancelled_with_its_scope_test() {
  let poke = workflow.SignalDefinition("poke", codec.int())
  let run =
    start(
      int_workflow("talker", fn(ctx, n) {
        use _ <- result.map(
          workflow.await(workflow.signal_external(ctx, "other", poke, n)),
        )
        n
      }),
    )
  let _ = activate(run, [init("talker", 1)])
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.CancelSignalExternal(1)]
}

pub fn a_nexus_operation_starts_then_resolves_test() {
  let operation =
    workflow.NexusOperation("maths", "double", codec.int(), codec.int())
  let run =
    start(
      int_workflow("caller", fn(ctx, n) {
        use handle <- result.try(workflow.start_nexus_operation(
          ctx,
          "endpoint",
          operation,
          n,
          workflow.nexus_options(),
        ))
        assert workflow.nexus_operation_token(handle) == Some("token")
        workflow.await(workflow.nexus_result(handle))
      }),
    )
  let assert [
    bridge.ScheduleNexusOperation(
      seq: 1,
      endpoint: "endpoint",
      service: "maths",
      operation: "double",
      cancellation_type: 0,
      ..,
    ),
  ] = activate(run, [init("caller", 3)])
  assert activate(run, [
      bridge.ResolveNexusStart(1, bridge.NexusStartedAsync("token")),
    ])
    == []
  // Cancelling the workflow now asks for the operation to be cancelled.
  assert activate(run, [bridge.CancelWorkflow("stop")])
    == [bridge.CancelNexusOperation(1)]
  assert activate(run, [
      bridge.ResolveNexus(1, bridge.Completed(int_payload(6))),
    ])
    == [bridge.CompleteWorkflow(int_payload(6))]
}

pub fn a_workflow_reports_its_versioning_behaviour_test() {
  let handler =
    int_workflow("pinned", fn(_, n) { Ok(n) })
    |> workflow.with_versioning(workflow.Pinned)
  let run = start(handler)
  bridge.send_activation(
    run.0,
    Activation("run", 1000, False, 3, [init("pinned", 1)], False),
  )
  let assert Ok(bridge.Success("run", [bridge.CompleteWorkflow(_)], 1)) =
    process.receive(run.1, 1000)
}

pub fn a_future_keeps_the_value_it_resolved_with_test() {
  let run =
    start(
      int_workflow("twice", fn(ctx, _) {
        let next = workflow.signal(ctx, added())
        // Awaiting again must not take a second signal.
        let first = workflow.await(next)
        let again = workflow.await(next)
        let other = workflow.receive_signal(ctx, added())
        Ok(first * 100 + again * 10 + other)
      }),
    )
  let signal = fn(n) { bridge.SignalWorkflow("added", [codec.int().encode(n)]) }
  assert activate(run, [init("twice", 0), signal(1), signal(2)])
    == [bridge.CompleteWorkflow(int_payload(112))]
}

pub fn a_workflow_that_ends_fails_the_updates_it_did_not_answer_test() {
  let run =
    start(
      int_workflow("rude", fn(ctx, n) {
        // Takes one update and answers neither it nor the one still queued.
        let _ = workflow.receive_update(ctx, add())
        Ok(n)
      }),
    )
  // The first was taken, so it is accepted and then failed. The second was still waiting
  // to be looked at, so it is refused outright and leaves no trace in history.
  let assert [
    bridge.UpdateAccepted("p1"),
    bridge.UpdateRejected(
      "p1",
      failure.Failure(error_type: "WorkflowFinished", ..),
    ),
    bridge.UpdateRejected("p2", _),
    bridge.CompleteWorkflow(_),
  ] =
    activate(run, [
      init("rude", 1),
      update_job("p1", 1, True),
      update_job("p2", 2, True),
    ])
}

pub fn only_the_first_answer_to_an_update_counts_test() {
  let run =
    start(
      int_workflow("eager", fn(ctx, n) {
        let update = workflow.receive_update(ctx, add())
        workflow.complete_update(ctx, update, Ok(1))
        workflow.complete_update(ctx, update, Ok(2))
        Ok(n)
      }),
    )
  assert activate(run, [init("eager", 7), update_job("p1", 1, True)])
    == [
      bridge.UpdateAccepted("p1"),
      bridge.UpdateCompleted("p1", codec.int().encode(1)),
      bridge.CompleteWorkflow(int_payload(7)),
    ]
}

pub fn fail_on_crash_turns_a_crash_into_a_workflow_failure_test() {
  let handler =
    int_workflow("brittle", fn(_, _) { panic as "a bug" })
    |> workflow.fail_on_crash
  let assert [
    bridge.FailWorkflow(failure.Failure(
      error_type: "WorkflowCrashed",
      non_retryable: True,
      ..,
    )),
  ] = activate(start(handler), [init("brittle", 1)])
}

pub fn a_scope_forgets_operations_that_have_settled_test() {
  let timers = 200
  let run =
    start(
      int_workflow("busy", fn(ctx, _) {
        int.range(from: 0, to: timers, with: Nil, run: fn(_, _) {
          let assert Ok(Nil) = workflow.sleep(ctx, duration.milliseconds(1))
          Nil
        })
        let assert Ok(root) = dict.get(runtime.get().scopes, runtime.root_scope)
        Ok(list.length(root.ops))
      }),
    )
  let _ = activate(run, [init("busy", 0)])
  let remembered =
    int.range(from: 1, to: timers + 1, with: [], run: fn(_, seq) {
      activate(run, [bridge.FireTimer(seq)])
    })
  let assert [bridge.CompleteWorkflow(Some(remembered))] = remembered
  let assert Ok(remembered) = codec.int().decode(remembered)
  // Far fewer than the two hundred timers the workflow's scope has seen.
  assert remembered <= 64
}
