//! Checks of what crosses to and from `temporal/internal/bridge`, used by the test suite.
//! The term layout is generated, but which of core's fields goes into which Gleam field is
//! written by hand in `convert.rs`, so the tests send every kind of value across in both
//! directions and compare what arrives with what was sent.

use crate::{
    convert::{
        decode_activity_result, decode_completion, decode_nexus_result, duration, to_activation,
        to_activity_task,
    },
    err, ok,
    wire::Wire,
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
        common::v1::{Memo, Payload, WorkflowExecution},
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
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
                run_id: "ignored".to_string(),
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
            run_id: "run-id".to_string(),
            ..Default::default()
        })),
    })
    .to_term(env)
}
