//! Checks of what crosses to and from `temporal/internal/bridge`, used by the test suite.
//! The term layout is generated, but which of core's fields goes into which Gleam field is
//! written by hand in `convert.rs`, so the tests send every kind of value across in both
//! directions and compare what arrives with what was sent.

use crate::{
    bridge,
    convert::{
        decode_activity_result, decode_completion, decode_nexus_result, duration, to_activation,
        to_activity_task,
    },
    err, ok,
    schedule::{overwrite, to_schedule},
    wire::{Bytes, Wire},
};
use rustler::{Encoder, Env, Term};
use temporalio_common::protos::{
    coresdk::{
        activity_result::{self as ar, ActivityResolution, activity_resolution},
        activity_task::{ActivityTask, Start, activity_task},
        child_workflow::{self as cw, ChildWorkflowResult, child_workflow_result},
        nexus::{NexusOperationResult, nexus_operation_result},
        workflow_activation::{
            self as wa, WorkflowActivation, WorkflowActivationJob,
            resolve_child_workflow_execution_start as child_start,
            resolve_nexus_operation_start as nexus_start, workflow_activation_job::Variant as Job,
        },
    },
    temporal::api::{
        common::v1::{ActivityType, Memo, Payload, Payloads, RetryPolicy, WorkflowExecution},
        enums::v1::{RetryState, TimeoutType},
        failure::v1::{
            ActivityFailureInfo, ApplicationFailureInfo, Failure, TimeoutFailureInfo,
            failure::FailureInfo,
        },
        workflowservice::v1::UpdateScheduleRequest,
    },
};

fn payload(text: &str) -> Payload {
    Payload {
        metadata: [("encoding".to_string(), b"json/plain".to_vec())].into(),
        data: text.as_bytes().to_vec(),
        ..Default::default()
    }
}

fn failure(message: &str) -> Failure {
    Failure {
        message: message.to_string(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "SampleType".to_string(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

/// An activity that timed out, as the server reports it to a workflow: full of fields
/// `failure.Failure` has no place for.
#[rustler::nif]
fn sample_activity_failure(env: Env) -> Term {
    Failure {
        message: "Activity task timed out".to_string(),
        source: "Server".to_string(),
        cause: Some(Box::new(Failure {
            message: "activity StartToClose timeout".to_string(),
            source: "Server".to_string(),
            stack_trace: "the-stack".to_string(),
            failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                timeout_type: TimeoutType::StartToClose as i32,
                last_heartbeat_details: Some(Payloads {
                    payloads: vec![payload("\"progress\"")],
                }),
            })),
            ..Default::default()
        })),
        failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo {
            scheduled_event_id: 5,
            started_event_id: 6,
            identity: "the-worker".to_string(),
            activity_type: Some(ActivityType {
                name: "the-activity-type".to_string(),
            }),
            activity_id: "the-activity-id".to_string(),
            retry_state: RetryState::NonRetryableFailure as i32,
        })),
        ..Default::default()
    }
    .to_term(env)
}

fn timestamp(ms: i64) -> prost_wkt_types::Timestamp {
    prost_wkt_types::Timestamp {
        seconds: ms / 1000,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    }
}

/// The debug form of a completion as core would receive it, or why it could not be read.
#[rustler::nif]
fn debug_completion<'a>(env: Env<'a>, completion: Term<'a>, namespace: String) -> Term<'a> {
    match decode_completion(completion, &namespace) {
        Ok(decoded) => ok(env, format!("{decoded:?}").encode(env)),
        Err(e) => err(env, e),
    }
}

/// The debug form of connection options, as any `{:?}` of them would print it: the API key
/// and the client's private key must not be in it.
#[rustler::nif]
fn debug_connect_options<'a>(env: Env<'a>, options: Term<'a>) -> Term<'a> {
    match bridge::ConnectOptions::from_term(options) {
        Ok(decoded) => ok(env, format!("{decoded:?}").encode(env)),
        Err(e) => err(env, e),
    }
}

#[rustler::nif]
fn debug_activity_result<'a>(env: Env<'a>, result: Term<'a>) -> Term<'a> {
    match decode_activity_result(result) {
        Ok(decoded) => ok(env, format!("{decoded:?}").encode(env)),
        Err(e) => err(env, e),
    }
}

#[rustler::nif]
fn debug_nexus_result<'a>(env: Env<'a>, result: Term<'a>) -> Term<'a> {
    match decode_nexus_result(result, b"token".to_vec()) {
        Ok(decoded) => ok(env, format!("{decoded:?}").encode(env)),
        Err(e) => err(env, e),
    }
}

/// An activation holding one job of every kind, each field with a value of its own.
#[rustler::nif]
fn sample_activation(env: Env) -> Term {
    let jobs = vec![
        Job::InitializeWorkflow(wa::InitializeWorkflow {
            workflow_type: "type".to_string(),
            workflow_id: "id".to_string(),
            arguments: vec![payload("1")],
            randomness_seed: 77,
            attempt: 3,
            first_execution_run_id: "first".to_string(),
            continued_from_execution_run_id: "previous".to_string(),
            memo: Some(Memo {
                fields: [("key".to_string(), payload("2"))].into(),
            }),
            headers: [("trace".to_string(), payload("8"))].into(),
            ..Default::default()
        }),
        Job::FireTimer(wa::FireTimer { seq: 11 }),
        Job::ResolveActivity(wa::ResolveActivity {
            seq: 12,
            result: Some(ActivityResolution {
                status: Some(activity_resolution::Status::Completed(ar::Success {
                    result: Some(payload("3")),
                })),
            }),
            ..Default::default()
        }),
        Job::ResolveActivity(wa::ResolveActivity {
            seq: 13,
            result: Some(ActivityResolution {
                status: Some(activity_resolution::Status::Backoff(ar::DoBackoff {
                    attempt: 4,
                    backoff_duration: Some(duration(1500)),
                    original_schedule_time: Some(timestamp(5000)),
                })),
            }),
            ..Default::default()
        }),
        Job::UpdateRandomSeed(wa::UpdateRandomSeed {
            randomness_seed: 78,
        }),
        Job::QueryWorkflow(wa::QueryWorkflow {
            query_id: "query-id".to_string(),
            query_type: "query-type".to_string(),
            arguments: vec![payload("4")],
            ..Default::default()
        }),
        Job::CancelWorkflow(wa::CancelWorkflow {
            reason: "reason".to_string(),
        }),
        Job::SignalWorkflow(wa::SignalWorkflow {
            signal_name: "signal".to_string(),
            input: vec![payload("5")],
            ..Default::default()
        }),
        Job::NotifyHasPatch(wa::NotifyHasPatch {
            patch_id: "patch".to_string(),
        }),
        Job::ResolveChildWorkflowExecutionStart(wa::ResolveChildWorkflowExecutionStart {
            seq: 14,
            status: Some(child_start::Status::Succeeded(
                wa::ResolveChildWorkflowExecutionStartSuccess {
                    run_id: "child-run".to_string(),
                },
            )),
        }),
        Job::ResolveChildWorkflowExecutionStart(wa::ResolveChildWorkflowExecutionStart {
            seq: 15,
            status: Some(child_start::Status::Failed(
                wa::ResolveChildWorkflowExecutionStartFailure {
                    workflow_id: "child-id".to_string(),
                    cause: 1,
                    ..Default::default()
                },
            )),
        }),
        Job::ResolveChildWorkflowExecution(wa::ResolveChildWorkflowExecution {
            seq: 16,
            result: Some(ChildWorkflowResult {
                status: Some(child_workflow_result::Status::Failed(cw::Failure {
                    failure: Some(failure("child failed")),
                })),
            }),
        }),
        Job::DoUpdate(wa::DoUpdate {
            id: "update-id".to_string(),
            protocol_instance_id: "protocol".to_string(),
            name: "update".to_string(),
            input: vec![payload("6")],
            run_validator: true,
            ..Default::default()
        }),
        Job::ResolveSignalExternalWorkflow(wa::ResolveSignalExternalWorkflow {
            seq: 17,
            failure: None,
            ..Default::default()
        }),
        Job::ResolveRequestCancelExternalWorkflow(wa::ResolveRequestCancelExternalWorkflow {
            seq: 18,
            failure: Some(failure("no such workflow")),
            ..Default::default()
        }),
        Job::ResolveNexusOperationStart(wa::ResolveNexusOperationStart {
            seq: 19,
            status: Some(nexus_start::Status::OperationToken("token".to_string())),
        }),
        Job::ResolveNexusOperation(wa::ResolveNexusOperation {
            seq: 20,
            result: Some(NexusOperationResult {
                status: Some(nexus_operation_result::Status::Completed(payload("7"))),
            }),
        }),
        Job::RemoveFromCache(wa::RemoveFromCache {
            message: "evicted".to_string(),
            reason: 3,
        }),
    ];
    to_activation(WorkflowActivation {
        run_id: "run".to_string(),
        timestamp: Some(timestamp(1234)),
        is_replaying: true,
        history_length: 9,
        jobs: jobs
            .into_iter()
            .map(|variant| WorkflowActivationJob {
                variant: Some(variant),
            })
            .collect(),
        continue_as_new_suggested: true,
        ..Default::default()
    })
    .to_term(env)
}

/// An activity task with each field set to a value of its own.
#[rustler::nif]
fn sample_activity_task(env: Env) -> Term {
    to_activity_task(ActivityTask {
        task_token: b"token".to_vec(),
        variant: Some(activity_task::Variant::Start(Start {
            workflow_type: "workflow-type".to_string(),
            workflow_execution: Some(WorkflowExecution {
                workflow_id: "workflow-id".to_string(),
                run_id: "run-id".to_string(),
            }),
            activity_id: "activity-id".to_string(),
            activity_type: "activity-type".to_string(),
            input: vec![payload("1")],
            heartbeat_details: vec![payload("2")],
            scheduled_time: Some(timestamp(4321)),
            attempt: 5,
            start_to_close_timeout: Some(duration(6000)),
            heartbeat_timeout: Some(duration(700)),
            is_local: true,
            header_fields: [("trace".to_string(), payload("3"))].into(),
            // A standalone activity's own run, not the workflow's. Core sets it on tasks
            // that have no workflow; here it only shows which field goes where.
            run_id: "activity-run-id".to_string(),
            retry_policy: Some(RetryPolicy {
                initial_interval: Some(duration(1000)),
                backoff_coefficient: 2.5,
                maximum_interval: Some(duration(9000)),
                maximum_attempts: 7,
                non_retryable_error_types: vec!["Fatal".to_string()],
            }),
            ..Default::default()
        })),
    })
    .to_term(env)
}

/// A standalone activity's first task as core hands it over: the workflow fields empty and
/// the activity's own run named.
#[rustler::nif]
fn sample_standalone_activity_task(env: Env) -> Term {
    use temporalio_common::protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;
    to_activity_task(ActivityTask::start_from_poll_resp(
        PollActivityTaskQueueResponse {
            task_token: b"token".to_vec(),
            activity_id: "activity-id".to_string(),
            activity_type: Some(ActivityType {
                name: "activity-type".to_string(),
            }),
            activity_run_id: "activity-run-id".to_string(),
            input: Some(Payloads {
                payloads: vec![payload("1")],
            }),
            scheduled_time: Some(timestamp(4321)),
            attempt: 2,
            start_to_close_timeout: Some(duration(6000)),
            retry_policy: Some(RetryPolicy {
                initial_interval: Some(duration(1000)),
                backoff_coefficient: 2.0,
                maximum_interval: Some(duration(9000)),
                maximum_attempts: 2,
                non_retryable_error_types: vec![],
            }),
            ..Default::default()
        },
    ))
    .to_term(env)
}

/// The request a `bridge.ActivityStartRequest` becomes, printed.
#[rustler::nif]
fn debug_activity_start<'a>(env: Env<'a>, request: Term<'a>) -> Term<'a> {
    match bridge::ActivityStartRequest::from_term(request).and_then(crate::activity::start_request)
    {
        Ok(request) => ok(env, format!("{request:?}").encode(env)),
        Err(e) => err(env, e),
    }
}

/// The description of a standalone activity that failed once, as Gleam is shown it.
#[rustler::nif]
fn sample_activity_execution(env: Env) -> Term {
    use temporalio_common::protos::temporal::api::{
        activity::v1::ActivityExecutionInfo, common::v1::SearchAttributes,
        workflowservice::v1::DescribeActivityExecutionResponse,
    };
    crate::activity::to_execution(DescribeActivityExecutionResponse {
        run_id: "run-id".to_string(),
        info: Some(ActivityExecutionInfo {
            activity_id: "activity-id".to_string(),
            activity_type: Some(ActivityType {
                name: "activity-type".to_string(),
            }),
            status: 1,
            run_state: 2,
            task_queue: "queue".to_string(),
            attempt: 2,
            schedule_time: Some(timestamp(4321)),
            close_time: Some(timestamp(8765)),
            last_failure: Some(failure("boom")),
            heartbeat_details: Some(Payloads {
                payloads: vec![payload("1")],
            }),
            last_worker_identity: "worker".to_string(),
            search_attributes: Some(SearchAttributes {
                indexed_fields: [("Key".to_string(), payload("2"))].into(),
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
    .to_term(env)
}

/// A schedule as another SDK or the CLI might have made it, with a value in every field
/// `bridge.Schedule` has no place for: as Gleam is shown it, and as the server has it.
#[rustler::nif]
fn sample_stored_schedule(env: Env) -> Term {
    use temporalio_common::protos::temporal::api::{
        common::v1::{Header, Payloads, WorkflowType},
        enums::v1::WorkflowIdReusePolicy,
        schedule::v1::{
            IntervalSpec, Range, Schedule, ScheduleAction, SchedulePolicies, ScheduleSpec,
            ScheduleState, StructuredCalendarSpec, schedule_action::Action,
        },
        taskqueue::v1::TaskQueue,
        workflow::v1::NewWorkflowExecutionInfo,
    };
    let stored = Schedule {
        spec: Some(ScheduleSpec {
            interval: vec![IntervalSpec {
                interval: Some(duration(3_600_000)),
                phase: Some(duration(60_000)),
            }],
            exclude_structured_calendar: vec![StructuredCalendarSpec {
                hour: vec![Range {
                    start: 3,
                    end: 4,
                    step: 1,
                }],
                comment: "maintenance".to_string(),
                ..Default::default()
            }],
            start_time: Some(timestamp(1_000_000)),
            end_time: Some(timestamp(2_000_000)),
            jitter: Some(duration(1500)),
            timezone_name: "Europe/Lisbon".to_string(),
            ..Default::default()
        }),
        action: Some(ScheduleAction {
            action: Some(Action::StartWorkflow(NewWorkflowExecutionInfo {
                workflow_id: "stored-id".to_string(),
                workflow_type: Some(WorkflowType {
                    name: "stored-type".to_string(),
                }),
                task_queue: Some(TaskQueue {
                    name: "stored-queue".to_string(),
                    ..Default::default()
                }),
                input: Some(Payloads {
                    payloads: vec![payload("1")],
                }),
                workflow_run_timeout: Some(duration(90_000)),
                workflow_id_reuse_policy: WorkflowIdReusePolicy::RejectDuplicate as i32,
                header: Some(Header {
                    fields: [("trace".to_string(), payload("2"))].into(),
                }),
                ..Default::default()
            })),
        }),
        policies: Some(SchedulePolicies {
            overlap_policy: 2,
            catchup_window: Some(duration(600_000)),
            pause_on_failure: true,
            keep_original_workflow_id: true,
        }),
        state: Some(ScheduleState {
            notes: "stored note".to_string(),
            paused: false,
            limited_actions: true,
            remaining_actions: 5,
        }),
    };
    let request = UpdateScheduleRequest {
        schedule: Some(stored.clone()),
        conflict_token: b"token".to_vec(),
        ..Default::default()
    };
    match to_schedule(stored) {
        Ok(described) => (described, Bytes(prost::Message::encode_to_vec(&request))).to_term(env),
        Err(status) => err(env, status.message()),
    }
}

/// The debug form of the schedule an update would send, or why it could not be made.
#[rustler::nif]
fn debug_schedule_update<'a>(
    env: Env<'a>,
    stored: Term<'a>,
    described: Term<'a>,
    changed: Term<'a>,
) -> Term<'a> {
    let updated = (|| {
        let stored = Bytes::from_term(stored)?;
        let mut request: UpdateScheduleRequest =
            prost::Message::decode(stored.0.as_slice()).map_err(|e| e.to_string())?;
        overwrite(
            request.schedule.get_or_insert_default(),
            bridge::Schedule::from_term(described)?,
            bridge::Schedule::from_term(changed)?,
        )?;
        Ok::<_, String>(format!("{request:?}"))
    })();
    match updated {
        Ok(updated) => ok(env, updated.encode(env)),
        Err(e) => err(env, e),
    }
}
