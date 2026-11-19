//// The layout of the values exchanged with the Rust NIF. `temporal/internal/bridge` and
//// `native/temporal_nif/src/convert.rs` agree on it by hand, with nothing to check them
//// at compile time, so these tests send every kind of value across in both directions
//// and compare what arrives with what was sent. Every field carries a value of its own,
//// so two fields swapped on one side show up as a mismatch.

import gleam/dict
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import temporal/codec.{type Payload}
import temporal/failure.{type Failure}
import temporal/internal/bridge

@external(erlang, "temporal_nif", "sample_activation")
fn sample_activation() -> bridge.Activation

@external(erlang, "temporal_nif", "sample_activity_task")
fn sample_activity_task() -> bridge.ActivityTask

@external(erlang, "temporal_nif", "debug_completion")
fn debug_completion(
  completion: bridge.Completion,
  namespace: String,
) -> Result(String, String)

@external(erlang, "temporal_nif", "debug_activity_result")
fn debug_activity_result(
  result: bridge.ActivityResult,
) -> Result(String, String)

@external(erlang, "temporal_nif", "debug_nexus_result")
fn debug_nexus_result(result: bridge.NexusResult) -> Result(String, String)

fn payload(text: String) -> Payload {
  codec.Payload(
    metadata: dict.from_list([#("encoding", <<"json/plain":utf8>>)]),
    data: <<text:utf8>>,
  )
}

fn sample_failure(message: String) -> Failure {
  failure.Failure(message, failure.Application, "SampleType", True, None, [])
}

pub fn every_job_arrives_as_the_gleam_type_describes_it_test() {
  assert sample_activation()
    == bridge.Activation(
      run_id: "run",
      timestamp_ms: 1234,
      is_replaying: True,
      history_length: 9,
      jobs: [
        bridge.InitializeWorkflow(
          workflow_type: "type",
          workflow_id: "id",
          arguments: [payload("1")],
          randomness_seed: 77,
          attempt: 3,
          first_execution_run_id: "first",
          continued_from_run_id: "previous",
          memo: dict.from_list([#("key", payload("2"))]),
        ),
        bridge.FireTimer(11),
        bridge.ResolveActivity(12, bridge.Completed(Some(payload("3")))),
        bridge.ResolveActivity(
          13,
          bridge.Backoff(
            attempt: 4,
            backoff_ms: 1500,
            original_schedule_time_ms: 5000,
          ),
        ),
        bridge.UpdateRandomSeed(78),
        bridge.QueryWorkflow("query-id", "query-type", [payload("4")]),
        bridge.CancelWorkflow("reason"),
        bridge.SignalWorkflow("signal", [payload("5")]),
        bridge.NotifyHasPatch("patch"),
        bridge.ResolveChildWorkflowStart(14, bridge.ChildStarted("child-run")),
        bridge.ResolveChildWorkflowStart(
          15,
          bridge.ChildStartFailed("child-id", 1),
        ),
        bridge.ResolveChildWorkflow(
          16,
          bridge.Failed(sample_failure("child failed")),
        ),
        bridge.DoUpdate(
          id: "update-id",
          protocol_instance_id: "protocol",
          name: "update",
          input: [payload("6")],
          run_validator: True,
        ),
        bridge.ResolveExternalSignal(17, None),
        bridge.ResolveExternalCancel(
          18,
          Some(sample_failure("no such workflow")),
        ),
        bridge.ResolveNexusStart(19, bridge.NexusStartedAsync("token")),
        bridge.ResolveNexus(20, bridge.Completed(Some(payload("7")))),
        bridge.RemoveFromCache("evicted", 3),
      ],
      continue_as_new_suggested: True,
    )
}

pub fn an_activity_task_arrives_as_the_gleam_type_describes_it_test() {
  assert sample_activity_task()
    == bridge.ActivityStart(
      task_token: <<"token":utf8>>,
      activity_type: "activity-type",
      activity_id: "activity-id",
      workflow_id: "workflow-id",
      run_id: "run-id",
      input: [payload("1")],
      attempt: 5,
      heartbeat_details: [payload("2")],
      workflow_type: "workflow-type",
      start_to_close_ms: Some(6000),
      heartbeat_timeout_ms: Some(700),
      scheduled_time_ms: 4321,
      is_local: True,
    )
}

fn retry() -> bridge.RetryPolicy {
  bridge.RetryPolicy(
    initial_interval_ms: 1000,
    backoff_coefficient: 2.5,
    maximum_interval_ms: Some(9000),
    maximum_attempts: 6,
    non_retryable_error_types: ["Fatal"],
  )
}

/// Every command, with fragments of how core must see it. Durations are in milliseconds
/// here and in seconds and nanoseconds there.
fn commands() -> List(#(bridge.Command, List(String))) {
  [
    #(bridge.StartTimer(1, 2500), [
      "StartTimer", "seq: 1", "seconds: 2, nanos: 500000000",
    ]),
    #(bridge.CancelTimer(2), ["CancelTimer", "seq: 2"]),
    #(
      bridge.ScheduleActivity(
        seq: 3,
        activity_id: "a-id",
        activity_type: "a-type",
        task_queue: "a-queue",
        arguments: [payload("1")],
        schedule_to_close_ms: Some(11_000),
        start_to_close_ms: Some(12_000),
        heartbeat_timeout_ms: Some(13_000),
        retry: Some(retry()),
        schedule_to_start_ms: Some(14_000),
        cancellation_type: 1,
      ),
      [
        "ScheduleActivity", "seq: 3", "activity_id: \"a-id\"",
        "activity_type: \"a-type\"", "task_queue: \"a-queue\"",
        "schedule_to_close_timeout: Some(Duration { seconds: 11",
        "start_to_close_timeout: Some(Duration { seconds: 12",
        "heartbeat_timeout: Some(Duration { seconds: 13",
        "schedule_to_start_timeout: Some(Duration { seconds: 14",
        "backoff_coefficient: 2.5", "maximum_attempts: 6",
        "initial_interval: Some(Duration { seconds: 1",
        "maximum_interval: Some(Duration { seconds: 9",
        "non_retryable_error_types: [\"Fatal\"]",
        "cancellation_type: WaitCancellationCompleted",
      ],
    ),
    #(bridge.RequestCancelActivity(4), ["RequestCancelActivity", "seq: 4"]),
    #(bridge.RespondToQuery("q-id", Ok(payload("2"))), [
      "query_id: \"q-id\"", "Succeeded",
    ]),
    #(bridge.RespondToQuery("q-id", Error(sample_failure("q failed"))), [
      "query_id: \"q-id\"", "Failed", "q failed",
    ]),
    #(bridge.CompleteWorkflow(Some(payload("3"))), ["CompleteWorkflowExecution"]),
    #(bridge.FailWorkflow(sample_failure("w failed")), [
      "FailWorkflowExecution", "message: \"w failed\"", "type: \"SampleType\"",
      "non_retryable: true",
    ]),
    #(bridge.ContinueAsNew("c-type", "c-queue", [payload("4")]), [
      "ContinueAsNewWorkflowExecution", "workflow_type: \"c-type\"",
      "task_queue: \"c-queue\"",
    ]),
    #(bridge.CancelWorkflowExecution, ["CancelWorkflowExecution"]),
    #(bridge.SetPatchMarker("p-id", True), [
      "SetPatchMarker", "patch_id: \"p-id\"", "deprecated: true",
    ]),
    #(
      bridge.StartChildWorkflow(
        seq: 5,
        workflow_id: "ch-id",
        workflow_type: "ch-type",
        task_queue: "ch-queue",
        input: [payload("5")],
        options: bridge.ChildStartOptions(
          execution_timeout_ms: Some(21_000),
          run_timeout_ms: Some(22_000),
          task_timeout_ms: Some(23_000),
          parent_close_policy: 2,
          id_reuse_policy: 3,
          retry: Some(retry()),
          memo: dict.from_list([#("m-key", payload("6"))]),
          search_attributes: dict.from_list([#("s-key", payload("7"))]),
          cancellation_type: 2,
        ),
      ),
      [
        "StartChildWorkflowExecution", "seq: 5", "workflow_id: \"ch-id\"",
        "workflow_type: \"ch-type\"", "task_queue: \"ch-queue\"",
        "workflow_execution_timeout: Some(Duration { seconds: 21",
        "workflow_run_timeout: Some(Duration { seconds: 22",
        "workflow_task_timeout: Some(Duration { seconds: 23",
        "parent_close_policy: Abandon",
        "workflow_id_reuse_policy: RejectDuplicate", "\"m-key\"", "\"s-key\"",
        "cancellation_type: WaitCancellationCompleted",
      ],
    ),
    #(bridge.CancelChildWorkflow(6), [
      "CancelChildWorkflowExecution", "child_workflow_seq: 6",
    ]),
    #(
      bridge.ScheduleLocalActivity(
        seq: 7,
        activity_id: "l-id",
        activity_type: "l-type",
        attempt: 2,
        original_schedule_time_ms: Some(31_000),
        arguments: [payload("8")],
        schedule_to_close_ms: Some(32_000),
        schedule_to_start_ms: Some(33_000),
        start_to_close_ms: Some(34_000),
        retry: Some(retry()),
        local_retry_threshold_ms: Some(35_000),
        cancellation_type: 2,
      ),
      [
        "ScheduleLocalActivity", "seq: 7", "activity_id: \"l-id\"",
        "activity_type: \"l-type\"", "attempt: 2",
        "original_schedule_time: Some(Timestamp { seconds: 31",
        "schedule_to_close_timeout: Some(Duration { seconds: 32",
        "schedule_to_start_timeout: Some(Duration { seconds: 33",
        "start_to_close_timeout: Some(Duration { seconds: 34",
        "local_retry_threshold: Some(Duration { seconds: 35",
        "cancellation_type: Abandon",
      ],
    ),
    #(bridge.RequestCancelLocalActivity(8), [
      "RequestCancelLocalActivity", "seq: 8",
    ]),
    #(
      bridge.SignalExternal(
        9,
        bridge.ExternalWorkflow("e-id", "e-run"),
        "e-signal",
        [payload("9")],
      ),
      [
        "SignalExternalWorkflowExecution", "seq: 9", "namespace: \"ns\"",
        "workflow_id: \"e-id\"", "run_id: \"e-run\"",
        "signal_name: \"e-signal\"",
      ],
    ),
    #(
      bridge.SignalExternal(
        10,
        bridge.ChildWorkflowTarget("kid"),
        "k-signal",
        [],
      ),
      ["seq: 10", "ChildWorkflowId(\"kid\")", "signal_name: \"k-signal\""],
    ),
    #(bridge.CancelSignalExternal(11), ["CancelSignalWorkflow", "seq: 11"]),
    #(bridge.CancelExternal(12, "x-id", "x-run", "x-reason"), [
      "RequestCancelExternalWorkflowExecution", "seq: 12", "namespace: \"ns\"",
      "workflow_id: \"x-id\"", "run_id: \"x-run\"", "reason: \"x-reason\"",
    ]),
    #(bridge.UpdateAccepted("u-1"), [
      "protocol_instance_id: \"u-1\"", "Accepted",
    ]),
    #(bridge.UpdateRejected("u-2", sample_failure("u failed")), [
      "protocol_instance_id: \"u-2\"", "Rejected", "u failed",
    ]),
    #(bridge.UpdateCompleted("u-3", payload("10")), [
      "protocol_instance_id: \"u-3\"", "Completed",
    ]),
    #(
      bridge.ScheduleNexusOperation(
        seq: 13,
        endpoint: "n-endpoint",
        service: "n-service",
        operation: "n-operation",
        input: payload("11"),
        schedule_to_close_ms: Some(41_000),
        cancellation_type: 2,
      ),
      [
        "ScheduleNexusOperation", "seq: 13", "endpoint: \"n-endpoint\"",
        "service: \"n-service\"", "operation: \"n-operation\"",
        "schedule_to_close_timeout: Some(Duration { seconds: 41",
        "cancellation_type: TryCancel",
      ],
    ),
    #(bridge.CancelNexusOperation(14), [
      "RequestCancelNexusOperation", "seq: 14",
    ]),
    #(
      bridge.UpsertSearchAttributes(dict.from_list([#("a-key", payload("12"))])),
      ["UpsertWorkflowSearchAttributes", "\"a-key\""],
    ),
    #(bridge.UpsertMemo(dict.from_list([#("b-key", payload("13"))])), [
      "ModifyWorkflowProperties", "\"b-key\"",
    ]),
  ]
}

pub fn every_command_reaches_core_with_its_fields_in_place_test() {
  list.each(commands(), fn(entry) {
    let #(command, fragments) = entry
    let assert Ok(seen) =
      debug_completion(bridge.Success("the-run", [command], 2), "ns")
    assert string.contains(seen, "run_id: \"the-run\"")
    assert string.contains(seen, "versioning_behavior: AutoUpgrade")
    list.each(fragments, fn(fragment) {
      case string.contains(seen, fragment) {
        True -> Nil
        False ->
          panic as { "core did not see `" <> fragment <> "` in: " <> seen }
      }
    })
  })
}

pub fn a_task_failure_reaches_core_test() {
  let assert Ok(seen) =
    debug_completion(
      bridge.TaskFailure("the-run", sample_failure("task failed")),
      "ns",
    )
  assert string.contains(seen, "run_id: \"the-run\"")
  assert string.contains(seen, "Failed")
  assert string.contains(seen, "task failed")
}

pub fn every_activity_result_reaches_core_test() {
  let seen = fn(result) {
    let assert Ok(seen) = debug_activity_result(result)
    seen
  }
  assert string.contains(
    seen(bridge.ActivityCompleted(Some(payload("1")))),
    "Completed",
  )
  let failed = seen(bridge.ActivityFailed(sample_failure("a failed")))
  assert string.contains(failed, "Failed")
    && string.contains(failed, "a failed")
  assert string.contains(
    seen(bridge.ActivityCancelled(failure.cancelled())),
    "Cancelled",
  )
}

pub fn every_nexus_result_reaches_core_test() {
  let seen = fn(result) {
    let assert Ok(seen) = debug_nexus_result(result)
    seen
  }
  assert string.contains(
    seen(bridge.NexusSyncSuccess(payload("1"))),
    "SyncSuccess",
  )
  assert string.contains(
    seen(bridge.NexusAsyncSuccess("the-token")),
    "operation_token: \"the-token\"",
  )
  assert string.contains(
    seen(bridge.NexusOperationFailed(sample_failure("o failed"))),
    "o failed",
  )
  let handler =
    seen(bridge.NexusHandlerFailed("NOT_FOUND", sample_failure("h failed")))
  assert string.contains(handler, "type: \"NOT_FOUND\"")
    && string.contains(handler, "h failed")
  assert string.contains(
    seen(bridge.NexusCancelAcknowledged),
    "CancelOperation",
  )
  assert string.contains(seen(bridge.NexusTaskCancelAcknowledged), "AckCancel")
}
