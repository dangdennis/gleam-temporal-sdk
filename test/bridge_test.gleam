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

@external(erlang, "temporal_nif", "sample_activity_failure")
fn sample_activity_failure() -> Failure

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
  failure.Failure(
    message,
    failure.Application,
    Some("SampleType"),
    True,
    None,
    [],
    None,
  )
}

/// `sample_failure` as it arrives from Rust: with the failure Temporal sent, which is the
/// `Failure` protobuf message. Field 1 is the message and field 5 the application
/// failure's type and non-retryable flag.
fn received_failure(message: String) -> Failure {
  let original = <<
    0x0a,
    string.byte_size(message),
    message:utf8,
    0x2a,
    14,
    0x0a,
    10,
    "SampleType":utf8,
    0x10,
    1,
  >>
  failure.Failure(..sample_failure(message), original: Some(original))
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
          headers: dict.from_list([#("trace", payload("8"))]),
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
          bridge.Failed(received_failure("child failed")),
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
          Some(received_failure("no such workflow")),
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
      headers: dict.from_list([#("trace", payload("3"))]),
      retry: Some(
        bridge.RetryPolicy(
          initial_interval_ms: 1000,
          backoff_coefficient: 2.5,
          maximum_interval_ms: Some(9000),
          maximum_attempts: 7,
          non_retryable_error_types: ["Fatal"],
        ),
      ),
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
        headers: dict.from_list([#("h-activity", payload("14"))]),
      ),
      [
        "ScheduleActivity", "seq: 3", "\"h-activity\"", "activity_id: \"a-id\"",
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
          headers: dict.from_list([#("h-child", payload("16"))]),
        ),
      ),
      [
        "StartChildWorkflowExecution", "seq: 5", "\"h-child\"",
        "workflow_id: \"ch-id\"", "workflow_type: \"ch-type\"",
        "task_queue: \"ch-queue\"",
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
        headers: dict.from_list([#("h-local", payload("15"))]),
      ),
      [
        "ScheduleLocalActivity", "seq: 7", "\"h-local\"",
        "activity_id: \"l-id\"", "activity_type: \"l-type\"", "attempt: 2",
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

/// What core is given when a workflow fails with `failure`.
fn sent(failure: Failure) -> String {
  let assert Ok(seen) =
    debug_completion(
      bridge.Success("run", [bridge.FailWorkflow(failure)], 0),
      "ns",
    )
  seen
}

/// Everything in `sample_activity_failure` that `failure.Failure` cannot hold.
const unmodelled = [
  "ActivityFailureInfo", "scheduled_event_id: 5", "started_event_id: 6",
  "identity: \"the-worker\"", "name: \"the-activity-type\"",
  "activity_id: \"the-activity-id\"", "retry_state: NonRetryableFailure",
  "source: \"Server\"",
]

const unmodelled_in_the_cause = [
  "TimeoutFailureInfo", "timeout_type: StartToClose", "last_heartbeat_details",
  "stack_trace: \"the-stack\"",
]

pub fn a_received_failure_shows_its_kind_and_cause_test() {
  let assert failure.Failure(
    message: "Activity task timed out",
    kind: failure.Activity,
    error_type: None,
    non_retryable: False,
    cause: Some(failure.Failure(
      message: "activity StartToClose timeout",
      kind: failure.Timeout,
      cause: None,
      original: Some(_),
      ..,
    )),
    details: [],
    original: Some(_),
  ) = sample_activity_failure()
}

pub fn a_failure_returned_unchanged_reaches_core_unchanged_test() {
  let seen = sent(sample_activity_failure())
  list.each(list.append(unmodelled, unmodelled_in_the_cause), fn(fragment) {
    assert string.contains(seen, fragment)
  })
  assert !string.contains(seen, "ApplicationFailureInfo")
  assert !string.contains(seen, "GleamSDK")
}

pub fn a_changed_failure_is_sent_as_built_and_its_cause_as_received_test() {
  let seen =
    sent(sample_activity_failure() |> failure.with_type("OrderAbandoned"))
  assert string.contains(seen, "ApplicationFailureInfo")
  assert string.contains(seen, "type: \"OrderAbandoned\"")
  assert string.contains(seen, "message: \"Activity task timed out\"")
  assert string.contains(seen, "source: \"GleamSDK\"")
  assert !string.contains(seen, "ActivityFailureInfo")
  // The cause was not touched, and goes back whole.
  list.each(unmodelled_in_the_cause, fn(fragment) {
    assert string.contains(seen, fragment)
  })
}

pub fn every_function_that_changes_a_failure_forgets_the_original_test() {
  let received = sample_activity_failure()
  assert failure.with_type(received, "T").original == None
  assert failure.non_retryable(received).original == None
  assert failure.with_details(received, [payload("1")]).original == None
  assert failure.new("built").original == None
  assert failure.cancelled().original == None
  assert failure.timeout("built").original == None
  // The cause is a failure of its own and keeps its own.
  let assert Some(failure.Failure(original: Some(_), ..)) =
    failure.with_type(received, "T").cause
}

pub fn a_failure_whose_fields_no_longer_match_its_original_is_sent_as_built_test() {
  // Changed with a record update, which leaves the original in place.
  let relabelled =
    failure.Failure(..sample_activity_failure(), message: "relabelled")
  let seen = sent(relabelled)
  assert string.contains(seen, "message: \"relabelled\"")
  assert string.contains(seen, "ApplicationFailureInfo")
  assert !string.contains(seen, "ActivityFailureInfo")
  let flagged =
    failure.Failure(..sample_activity_failure(), non_retryable: True)
  assert string.contains(sent(flagged), "non_retryable: true")
  let retyped =
    failure.Failure(..sample_activity_failure(), kind: failure.Application)
  assert !string.contains(sent(retyped), "ActivityFailureInfo")
  // An original that is not a failure at all is ignored.
  let garbled =
    failure.Failure(..failure.new("built"), original: Some(<<255, 255, 255>>))
  assert string.contains(sent(garbled), "message: \"built\"")
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

pub fn argument_list_codecs_carry_one_payload_per_argument_test() {
  let pair = codec.args2(codec.string(), codec.int())
  let payloads = codec.encode_arguments(pair, #("a", 2))
  assert payloads
    == [codec.encode(codec.string(), "a"), codec.encode(codec.int(), 2)]
  assert codec.decode_arguments(pair, payloads) == Ok(#("a", 2))

  // A caller that sent fewer arguments sent nulls for the rest, which only a codec that
  // accepts a null can read.
  let optional = codec.args2(codec.string(), codec.nil())
  assert codec.decode_arguments(optional, [codec.encode(codec.string(), "a")])
    == Ok(#("a", Nil))
  let assert Error(reason) =
    codec.decode_arguments(pair, [codec.encode(codec.string(), "a")])
  assert string.starts_with(reason, "argument 2:")

  // A single-value codec reads the first argument and ignores the rest.
  assert codec.decode_arguments(codec.string(), payloads) == Ok("a")
  assert codec.encode_arguments(codec.no_args(), Nil) == []
  assert codec.encode_arguments(codec.nil(), Nil)
    == [codec.encode(codec.nil(), Nil)]
}
