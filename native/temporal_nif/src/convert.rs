//! Conversions between core's protobuf messages and the Gleam types in
//! `temporal/internal/bridge`.
//!
//! Nothing here builds or reads a bridge type's term by position. `bridge.rs`, generated
//! from the Gleam types, has a struct or enum for each with the same constructor and field
//! names, and `wire.rs` turns those into terms and back. Towards Gleam, a function here
//! builds the bridge value from core's message, and the caller writes it with `to_term`.
//! From Gleam, the term is read with `from_term` and then taken apart field by field, with
//! no `..`, so that a field added in Gleam does not compile here until it is handled.
//!
//! `temporal/codec.Payload` and `temporal/failure.Failure` are the exception: they are not
//! bridge types, and are read straight into core's messages by hand below.

use crate::{
    bridge,
    wire::{Bytes, DecodeResult, Wire, at, atom, binary, record, untag},
};
use prost::Message;
use rustler::{Encoder, Env, Term};
use std::collections::HashMap;
use temporalio_common::protos::{
    coresdk::{
        activity_result::{
            self as ar, ActivityExecutionResult, ActivityResolution, activity_execution_result,
            activity_resolution,
        },
        activity_task::{ActivityTask, activity_task},
        child_workflow::{ChildWorkflowResult, child_workflow_result},
        nexus::{NexusTask, NexusTaskCompletion},
        workflow_activation::{
            WorkflowActivation, resolve_child_workflow_execution_start as child_start,
            workflow_activation_job::Variant as Job,
        },
        workflow_commands::{self as wc, WorkflowCommand, workflow_command::Variant as Cmd},
        workflow_completion::{self, WorkflowActivationCompletion},
    },
    temporal::api::{
        common::v1::{Memo, Payload, Payloads, RetryPolicy, SearchAttributes},
        failure::v1::{ApplicationFailureInfo, CanceledFailureInfo, Failure, failure::FailureInfo},
    },
};

pub fn millis(ts: Option<impl Into<prost_wkt_types::Timestamp>>) -> i64 {
    ts.map(|t| {
        let t: prost_wkt_types::Timestamp = t.into();
        t.seconds * 1000 + i64::from(t.nanos) / 1_000_000
    })
    .unwrap_or(0)
}

pub fn duration_ms(d: Option<prost_wkt_types::Duration>) -> Option<i64> {
    d.map(|d| d.seconds * 1000 + i64::from(d.nanos) / 1_000_000)
}

pub fn duration(ms: u64) -> prost_wkt_types::Duration {
    prost_wkt_types::Duration {
        seconds: (ms / 1000) as i64,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    }
}

pub fn timestamp(ms: i64) -> prost_wkt_types::Timestamp {
    prost_wkt_types::Timestamp {
        seconds: ms.div_euclid(1000),
        nanos: (ms.rem_euclid(1000) * 1_000_000) as i32,
    }
}

/// A Gleam `Int` in the width core wants. Gleam's has no size, so one that does not fit is
/// reported by the name of its field.
pub fn n<T: TryFrom<i64>>(value: i64, name: &str) -> DecodeResult<T> {
    T::try_from(value).map_err(|_| format!("{name}: {value} is out of range"))
}

/// A length of time in milliseconds, which cannot be negative.
pub fn ms(value: i64, name: &str) -> DecodeResult<prost_wkt_types::Duration> {
    let value: u64 = value
        .try_into()
        .map_err(|_| format!("{name}: a duration must be a non-negative Int"))?;
    Ok(duration(value))
}

pub fn opt_ms(value: Option<i64>, name: &str) -> DecodeResult<Option<prost_wkt_types::Duration>> {
    value.map(|value| ms(value, name)).transpose()
}

/// A count from core as a Gleam `Int`. Core's unsigned counts never get near the limit.
pub fn count(value: impl TryInto<i64>) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

// A memo or set of search attributes with nothing in it is sent as none at all.

pub fn memo(fields: HashMap<String, Payload>) -> Option<Memo> {
    (!fields.is_empty()).then_some(Memo { fields })
}

pub fn search_attributes(indexed_fields: HashMap<String, Payload>) -> Option<SearchAttributes> {
    (!indexed_fields.is_empty()).then_some(SearchAttributes { indexed_fields })
}

// ---- payloads and failures, by hand ----

pub fn encode_payload<'a>(env: Env<'a>, p: &Payload) -> Term<'a> {
    let mut metadata = Term::map_new(env);
    for (k, v) in &p.metadata {
        metadata = metadata
            .map_put(k.as_str(), binary(env, v))
            .expect("map_put on a map");
    }
    record(env, "payload", &[metadata, binary(env, &p.data)])
}

pub fn decode_payload(term: Term) -> DecodeResult<Payload> {
    let (_, f) = untag(term)?;
    if f.len() != 2 {
        return Err("expected a Payload".to_string());
    }
    let metadata: HashMap<String, Bytes> = at(&f, 0, "Payload.metadata")?;
    let data: Bytes = at(&f, 1, "Payload.data")?;
    Ok(Payload {
        metadata: metadata.into_iter().map(|(k, v)| (k, v.0)).collect(),
        data: data.0,
        ..Default::default()
    })
}

/// The kind, error type and non-retryable flag Gleam sees for a failure.
fn failure_summary(f: &Failure) -> (&'static str, Option<&str>, bool) {
    let (kind, error_type, non_retryable) = match &f.failure_info {
        Some(FailureInfo::ApplicationFailureInfo(i)) => {
            ("application", i.r#type.as_str(), i.non_retryable)
        }
        Some(FailureInfo::TimeoutFailureInfo(_)) => ("timeout", "", false),
        Some(FailureInfo::CanceledFailureInfo(_)) => ("cancelled", "", false),
        Some(FailureInfo::TerminatedFailureInfo(_)) => ("terminated", "", false),
        Some(FailureInfo::ServerFailureInfo(i)) => ("server", "", i.non_retryable),
        Some(FailureInfo::ActivityFailureInfo(_)) => ("activity", "", false),
        Some(FailureInfo::ChildWorkflowExecutionFailureInfo(_)) => ("child_workflow", "", false),
        Some(FailureInfo::NexusOperationExecutionFailureInfo(_)) => ("nexus_operation", "", false),
        Some(FailureInfo::NexusHandlerFailureInfo(i)) => {
            ("nexus_handler", i.r#type.as_str(), false)
        }
        _ => ("other", "", false),
    };
    let error_type = (!error_type.is_empty()).then_some(error_type);
    (kind, error_type, non_retryable)
}

/// The last field, `original`, is the failure itself, encoded: `decode_failure` sends it
/// back as it is when Gleam returns the failure unchanged.
pub fn encode_failure<'a>(env: Env<'a>, f: &Failure) -> Term<'a> {
    let (kind, error_type, non_retryable) = failure_summary(f);
    let details = match &f.failure_info {
        Some(FailureInfo::ApplicationFailureInfo(i)) => i.details.as_ref(),
        Some(FailureInfo::CanceledFailureInfo(i)) => i.details.as_ref(),
        _ => None,
    };
    let details: Vec<Term> = details
        .map(|d| d.payloads.as_slice())
        .unwrap_or(&[])
        .iter()
        .map(|p| encode_payload(env, p))
        .collect();
    record(
        env,
        "failure",
        &[
            f.message.to_term(env),
            atom(env, kind),
            error_type.map(str::to_string).to_term(env),
            non_retryable.to_term(env),
            f.cause.to_term(env),
            details.encode(env),
            Some(Bytes(f.encode_to_vec())).to_term(env),
        ],
    )
}

/// A failure Gleam received and hands back untouched is sent as Temporal sent it, with
/// everything `failure.Failure` does not model: whether what Gleam shows of it still matches
/// its `original` tells. Any other failure is built from the fields, as an application
/// failure or a cancellation, and its cause is read by the same rule.
pub fn decode_failure(term: Term) -> DecodeResult<Failure> {
    let (_, f) = untag(term)?;
    if f.len() != 7 {
        return Err("expected a Failure".to_string());
    }
    let message: String = at(&f, 0, "Failure.message")?;
    let (kind, _) = untag(f[1])?;
    let error_type: Option<String> = at(&f, 2, "Failure.error_type")?;
    let non_retryable: bool = at(&f, 3, "Failure.non_retryable")?;
    let original: Option<Bytes> = at(&f, 6, "Failure.original")?;
    if let Some(original) = original.and_then(|bytes| Failure::decode(bytes.0.as_slice()).ok())
        && original.message == message
        && failure_summary(&original) == (kind.as_str(), error_type.as_deref(), non_retryable)
    {
        return Ok(original);
    }
    let details: Vec<Payload> = at(&f, 5, "Failure.details")?;
    let details = (!details.is_empty()).then_some(Payloads { payloads: details });
    let info = if kind == "cancelled" {
        FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
            details,
            ..Default::default()
        })
    } else {
        FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo {
            r#type: error_type.unwrap_or_default(),
            non_retryable,
            details,
            ..Default::default()
        })
    };
    Ok(Failure {
        message,
        source: "GleamSDK".to_string(),
        cause: at(&f, 4, "Failure.cause")?,
        failure_info: Some(info),
        ..Default::default()
    })
}

// ---- core -> Gleam ----

fn to_activity_resolution(r: Option<ActivityResolution>) -> bridge::Resolution {
    match r.and_then(|r| r.status) {
        Some(activity_resolution::Status::Completed(s)) => {
            bridge::Resolution::Completed { result: s.result }
        }
        Some(activity_resolution::Status::Failed(f)) => bridge::Resolution::Failed {
            failure: f.failure.unwrap_or_default(),
        },
        Some(activity_resolution::Status::Cancelled(c)) => bridge::Resolution::Cancelled {
            failure: c.failure.unwrap_or_default(),
        },
        Some(activity_resolution::Status::Backoff(b)) => bridge::Resolution::Backoff {
            attempt: i64::from(b.attempt),
            backoff_ms: duration_ms(b.backoff_duration).unwrap_or(0),
            original_schedule_time_ms: millis(b.original_schedule_time),
        },
        None => bridge::Resolution::Failed {
            failure: Failure::default(),
        },
    }
}

fn to_child_resolution(r: Option<ChildWorkflowResult>) -> bridge::Resolution {
    match r.and_then(|r| r.status) {
        Some(child_workflow_result::Status::Completed(s)) => {
            bridge::Resolution::Completed { result: s.result }
        }
        Some(child_workflow_result::Status::Failed(f)) => bridge::Resolution::Failed {
            failure: f.failure.unwrap_or_default(),
        },
        Some(child_workflow_result::Status::Cancelled(c)) => bridge::Resolution::Cancelled {
            failure: c.failure.unwrap_or_default(),
        },
        None => bridge::Resolution::Failed {
            failure: Failure::default(),
        },
    }
}

fn to_job(job: Job) -> bridge::Job {
    match job {
        Job::InitializeWorkflow(j) => bridge::Job::InitializeWorkflow {
            workflow_type: j.workflow_type,
            workflow_id: j.workflow_id,
            arguments: j.arguments,
            randomness_seed: j.randomness_seed,
            attempt: i64::from(j.attempt),
            first_execution_run_id: j.first_execution_run_id,
            continued_from_run_id: j.continued_from_execution_run_id,
            memo: j.memo.map(|m| m.fields).unwrap_or_default(),
            headers: j.headers,
        },
        Job::FireTimer(j) => bridge::Job::FireTimer {
            seq: i64::from(j.seq),
        },
        Job::ResolveActivity(j) => bridge::Job::ResolveActivity {
            seq: i64::from(j.seq),
            result: to_activity_resolution(j.result),
        },
        Job::UpdateRandomSeed(j) => bridge::Job::UpdateRandomSeed {
            seed: j.randomness_seed,
        },
        Job::QueryWorkflow(j) => bridge::Job::QueryWorkflow {
            query_id: j.query_id,
            query_type: j.query_type,
            arguments: j.arguments,
        },
        Job::CancelWorkflow(j) => bridge::Job::CancelWorkflow { reason: j.reason },
        Job::SignalWorkflow(j) => bridge::Job::SignalWorkflow {
            signal_name: j.signal_name,
            input: j.input,
        },
        Job::NotifyHasPatch(j) => bridge::Job::NotifyHasPatch {
            patch_id: j.patch_id,
        },
        Job::ResolveChildWorkflowExecutionStart(j) => {
            let result = match j.status {
                Some(child_start::Status::Succeeded(s)) => {
                    bridge::ChildStart::ChildStarted { run_id: s.run_id }
                }
                Some(child_start::Status::Failed(f)) => bridge::ChildStart::ChildStartFailed {
                    workflow_id: f.workflow_id,
                    cause: i64::from(f.cause),
                },
                Some(child_start::Status::Cancelled(c)) => {
                    bridge::ChildStart::ChildStartCancelled {
                        failure: c.failure.unwrap_or_default(),
                    }
                }
                None => bridge::ChildStart::ChildStartFailed {
                    workflow_id: String::new(),
                    cause: 0,
                },
            };
            bridge::Job::ResolveChildWorkflowStart {
                seq: i64::from(j.seq),
                result,
            }
        }
        Job::ResolveChildWorkflowExecution(j) => bridge::Job::ResolveChildWorkflow {
            seq: i64::from(j.seq),
            result: to_child_resolution(j.result),
        },
        Job::RemoveFromCache(j) => bridge::Job::RemoveFromCache {
            message: j.message,
            reason: i64::from(j.reason),
        },
        Job::ResolveSignalExternalWorkflow(j) => bridge::Job::ResolveExternalSignal {
            seq: i64::from(j.seq),
            failure: j.failure,
        },
        Job::ResolveRequestCancelExternalWorkflow(j) => bridge::Job::ResolveExternalCancel {
            seq: i64::from(j.seq),
            failure: j.failure,
        },
        Job::DoUpdate(j) => bridge::Job::DoUpdate {
            id: j.id,
            protocol_instance_id: j.protocol_instance_id,
            name: j.name,
            input: j.input,
            run_validator: j.run_validator,
        },
        Job::ResolveNexusOperationStart(j) => {
            use temporalio_common::protos::coresdk::workflow_activation::resolve_nexus_operation_start::Status;
            let result = match j.status {
                Some(Status::OperationToken(operation_token)) => {
                    bridge::NexusStart::NexusStartedAsync { operation_token }
                }
                Some(Status::StartedSync(_)) => bridge::NexusStart::NexusStartedSync,
                Some(Status::Failed(failure)) => bridge::NexusStart::NexusStartFailed { failure },
                None => bridge::NexusStart::NexusStartFailed {
                    failure: Failure::default(),
                },
            };
            bridge::Job::ResolveNexusStart {
                seq: i64::from(j.seq),
                result,
            }
        }
        Job::ResolveNexusOperation(j) => {
            use temporalio_common::protos::coresdk::nexus::nexus_operation_result::Status;
            let result = match j.result.and_then(|r| r.status) {
                Some(Status::Completed(p)) => bridge::Resolution::Completed { result: Some(p) },
                Some(Status::Cancelled(failure)) => bridge::Resolution::Cancelled { failure },
                Some(Status::Failed(failure)) | Some(Status::TimedOut(failure)) => {
                    bridge::Resolution::Failed { failure }
                }
                None => bridge::Resolution::Failed {
                    failure: Failure::default(),
                },
            };
            bridge::Job::ResolveNexus {
                seq: i64::from(j.seq),
                result,
            }
        }
    }
}

pub fn to_activation(a: WorkflowActivation) -> bridge::Activation {
    bridge::Activation {
        run_id: a.run_id,
        timestamp_ms: millis(a.timestamp),
        is_replaying: a.is_replaying,
        history_length: i64::from(a.history_length),
        jobs: a
            .jobs
            .into_iter()
            .filter_map(|j| j.variant)
            .map(to_job)
            .collect(),
        continue_as_new_suggested: a.continue_as_new_suggested,
    }
}

pub fn to_activity_task(t: ActivityTask) -> bridge::ActivityTask {
    let task_token = Bytes(t.task_token);
    match t.variant {
        Some(activity_task::Variant::Start(s)) => {
            let execution = s.workflow_execution.unwrap_or_default();
            bridge::ActivityTask::ActivityStart {
                task_token,
                activity_type: s.activity_type,
                activity_id: s.activity_id,
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
                input: s.input,
                attempt: i64::from(s.attempt),
                heartbeat_details: s.heartbeat_details,
                workflow_type: s.workflow_type,
                start_to_close_ms: duration_ms(s.start_to_close_timeout),
                heartbeat_timeout_ms: duration_ms(s.heartbeat_timeout).filter(|ms| *ms > 0),
                scheduled_time_ms: millis(s.scheduled_time),
                is_local: s.is_local,
                headers: s.header_fields,
                retry: s.retry_policy.map(to_retry_policy),
            }
        }
        Some(activity_task::Variant::Cancel(c)) => bridge::ActivityTask::ActivityCancel {
            task_token,
            reason: i64::from(c.reason),
        },
        None => bridge::ActivityTask::ActivityCancel {
            task_token,
            reason: 0,
        },
    }
}

pub fn to_nexus_task(task: NexusTask) -> bridge::NexusTask {
    use temporalio_common::protos::{
        coresdk::nexus::nexus_task::Variant, temporal::api::nexus::v1::request,
    };
    match task.variant {
        Some(Variant::Task(t)) => {
            let task_token = Bytes(t.task_token);
            let request = t.request.unwrap_or_default();
            match request.variant {
                Some(request::Variant::StartOperation(s)) => bridge::NexusTask::NexusStartTask {
                    task_token,
                    service: s.service,
                    operation: s.operation,
                    request_id: s.request_id,
                    input: s.payload,
                    callback_url: s.callback,
                    callback_headers: s.callback_header,
                    headers: request.header,
                },
                Some(request::Variant::CancelOperation(c)) => bridge::NexusTask::NexusCancelTask {
                    task_token,
                    service: c.service,
                    operation: c.operation,
                    operation_token: c.operation_token,
                },
                None => bridge::NexusTask::NexusTaskCancelled { task_token },
            }
        }
        Some(Variant::CancelTask(c)) => bridge::NexusTask::NexusTaskCancelled {
            task_token: Bytes(c.task_token),
        },
        None => bridge::NexusTask::NexusTaskCancelled {
            task_token: Bytes(vec![]),
        },
    }
}

pub fn to_retry_policy(r: RetryPolicy) -> bridge::RetryPolicy {
    bridge::RetryPolicy {
        initial_interval_ms: duration_ms(r.initial_interval).unwrap_or(0),
        backoff_coefficient: r.backoff_coefficient,
        // The server reports a length of time that was never set as zero.
        maximum_interval_ms: duration_ms(r.maximum_interval).filter(|ms| *ms > 0),
        maximum_attempts: i64::from(r.maximum_attempts),
        non_retryable_error_types: r.non_retryable_error_types,
    }
}

// ---- Gleam -> core ----

pub fn retry_policy(policy: bridge::RetryPolicy) -> DecodeResult<RetryPolicy> {
    let bridge::RetryPolicy {
        initial_interval_ms,
        backoff_coefficient,
        maximum_interval_ms,
        maximum_attempts,
        non_retryable_error_types,
    } = policy;
    Ok(RetryPolicy {
        initial_interval: Some(ms(initial_interval_ms, "RetryPolicy.initial_interval_ms")?),
        backoff_coefficient,
        maximum_interval: opt_ms(maximum_interval_ms, "RetryPolicy.maximum_interval_ms")?,
        maximum_attempts: n(maximum_attempts, "RetryPolicy.maximum_attempts")?,
        non_retryable_error_types,
    })
}

fn command(command: bridge::Command, namespace: &str) -> DecodeResult<WorkflowCommand> {
    use bridge::Command as C;
    use temporalio_common::protos::coresdk::common::NamespacedWorkflowExecution;
    let variant = match command {
        C::StartTimer { seq, ms: fire_in } => Cmd::StartTimer(wc::StartTimer {
            seq: n(seq, "StartTimer.seq")?,
            start_to_fire_timeout: Some(ms(fire_in, "StartTimer.ms")?),
        }),
        C::CancelTimer { seq } => Cmd::CancelTimer(wc::CancelTimer {
            seq: n(seq, "CancelTimer.seq")?,
        }),
        C::ScheduleActivity {
            seq,
            activity_id,
            activity_type,
            task_queue,
            arguments,
            schedule_to_close_ms,
            start_to_close_ms,
            heartbeat_timeout_ms,
            retry,
            schedule_to_start_ms,
            cancellation_type,
            headers,
        } => Cmd::ScheduleActivity(wc::ScheduleActivity {
            seq: n(seq, "ScheduleActivity.seq")?,
            activity_id,
            activity_type,
            task_queue,
            arguments,
            schedule_to_close_timeout: opt_ms(
                schedule_to_close_ms,
                "ScheduleActivity.schedule_to_close_ms",
            )?,
            start_to_close_timeout: opt_ms(
                start_to_close_ms,
                "ScheduleActivity.start_to_close_ms",
            )?,
            heartbeat_timeout: opt_ms(
                heartbeat_timeout_ms,
                "ScheduleActivity.heartbeat_timeout_ms",
            )?,
            retry_policy: retry.map(retry_policy).transpose()?,
            schedule_to_start_timeout: opt_ms(
                schedule_to_start_ms,
                "ScheduleActivity.schedule_to_start_ms",
            )?,
            cancellation_type: n(cancellation_type, "ScheduleActivity.cancellation_type")?,
            headers,
            ..Default::default()
        }),
        C::RequestCancelActivity { seq } => Cmd::RequestCancelActivity(wc::RequestCancelActivity {
            seq: n(seq, "RequestCancelActivity.seq")?,
        }),
        C::RespondToQuery { query_id, result } => Cmd::RespondToQuery(wc::QueryResult {
            query_id,
            variant: Some(match result {
                Ok(response) => wc::query_result::Variant::Succeeded(wc::QuerySuccess {
                    response: Some(response),
                }),
                Err(failure) => wc::query_result::Variant::Failed(failure),
            }),
        }),
        C::CompleteWorkflow { result } => {
            Cmd::CompleteWorkflowExecution(wc::CompleteWorkflowExecution { result })
        }
        C::FailWorkflow { failure } => Cmd::FailWorkflowExecution(wc::FailWorkflowExecution {
            failure: Some(failure),
        }),
        C::ContinueAsNew {
            workflow_type,
            task_queue,
            arguments,
            options,
        } => {
            let bridge::ContinueAsNewOptions {
                run_timeout_ms,
                task_timeout_ms,
                memo,
                search_attributes: attributes,
                headers,
                retry,
            } = options;
            Cmd::ContinueAsNewWorkflowExecution(wc::ContinueAsNewWorkflowExecution {
                workflow_type,
                task_queue,
                arguments,
                workflow_run_timeout: opt_ms(
                    run_timeout_ms,
                    "ContinueAsNewOptions.run_timeout_ms",
                )?,
                workflow_task_timeout: opt_ms(
                    task_timeout_ms,
                    "ContinueAsNewOptions.task_timeout_ms",
                )?,
                memo,
                search_attributes: search_attributes(attributes),
                headers,
                retry_policy: retry.map(retry_policy).transpose()?,
                ..Default::default()
            })
        }
        C::CancelWorkflowExecution => {
            Cmd::CancelWorkflowExecution(wc::CancelWorkflowExecution::default())
        }
        C::SetPatchMarker {
            patch_id,
            deprecated,
        } => Cmd::SetPatchMarker(wc::SetPatchMarker {
            patch_id,
            deprecated,
        }),
        C::StartChildWorkflow {
            seq,
            workflow_id,
            workflow_type,
            task_queue,
            input,
            options,
        } => {
            let bridge::ChildStartOptions {
                execution_timeout_ms,
                run_timeout_ms,
                task_timeout_ms,
                parent_close_policy,
                id_reuse_policy,
                retry,
                memo,
                search_attributes: attributes,
                cancellation_type,
                headers,
            } = options;
            Cmd::StartChildWorkflowExecution(wc::StartChildWorkflowExecution {
                seq: n(seq, "StartChildWorkflow.seq")?,
                workflow_id,
                workflow_type,
                task_queue,
                input,
                workflow_execution_timeout: opt_ms(
                    execution_timeout_ms,
                    "ChildStartOptions.execution_timeout_ms",
                )?,
                workflow_run_timeout: opt_ms(run_timeout_ms, "ChildStartOptions.run_timeout_ms")?,
                workflow_task_timeout: opt_ms(
                    task_timeout_ms,
                    "ChildStartOptions.task_timeout_ms",
                )?,
                parent_close_policy: n(
                    parent_close_policy,
                    "ChildStartOptions.parent_close_policy",
                )?,
                workflow_id_reuse_policy: n(id_reuse_policy, "ChildStartOptions.id_reuse_policy")?,
                retry_policy: retry.map(retry_policy).transpose()?,
                memo,
                search_attributes: search_attributes(attributes),
                cancellation_type: n(cancellation_type, "ChildStartOptions.cancellation_type")?,
                headers,
                ..Default::default()
            })
        }
        C::CancelChildWorkflow { seq } => {
            Cmd::CancelChildWorkflowExecution(wc::CancelChildWorkflowExecution {
                child_workflow_seq: n(seq, "CancelChildWorkflow.seq")?,
                reason: String::new(),
            })
        }
        C::ScheduleLocalActivity {
            seq,
            activity_id,
            activity_type,
            attempt,
            original_schedule_time_ms,
            arguments,
            schedule_to_close_ms,
            schedule_to_start_ms,
            start_to_close_ms,
            retry,
            local_retry_threshold_ms,
            cancellation_type,
            headers,
        } => Cmd::ScheduleLocalActivity(wc::ScheduleLocalActivity {
            seq: n(seq, "ScheduleLocalActivity.seq")?,
            activity_id,
            activity_type,
            attempt: n(attempt, "ScheduleLocalActivity.attempt")?,
            original_schedule_time: original_schedule_time_ms.map(timestamp),
            arguments,
            schedule_to_close_timeout: opt_ms(
                schedule_to_close_ms,
                "ScheduleLocalActivity.schedule_to_close_ms",
            )?,
            schedule_to_start_timeout: opt_ms(
                schedule_to_start_ms,
                "ScheduleLocalActivity.schedule_to_start_ms",
            )?,
            start_to_close_timeout: opt_ms(
                start_to_close_ms,
                "ScheduleLocalActivity.start_to_close_ms",
            )?,
            retry_policy: retry.map(retry_policy).transpose()?,
            local_retry_threshold: opt_ms(
                local_retry_threshold_ms,
                "ScheduleLocalActivity.local_retry_threshold_ms",
            )?,
            cancellation_type: n(cancellation_type, "ScheduleLocalActivity.cancellation_type")?,
            headers,
            ..Default::default()
        }),
        C::RequestCancelLocalActivity { seq } => {
            Cmd::RequestCancelLocalActivity(wc::RequestCancelLocalActivity {
                seq: n(seq, "RequestCancelLocalActivity.seq")?,
            })
        }
        C::SignalExternal {
            seq,
            target,
            signal_name,
            input,
        } => {
            use wc::signal_external_workflow_execution::Target;
            let target = match target {
                bridge::SignalTarget::ChildWorkflowTarget { workflow_id } => {
                    Target::ChildWorkflowId(workflow_id)
                }
                bridge::SignalTarget::ExternalWorkflow {
                    workflow_id,
                    run_id,
                } => Target::WorkflowExecution(NamespacedWorkflowExecution {
                    namespace: namespace.to_string(),
                    workflow_id,
                    run_id,
                }),
            };
            Cmd::SignalExternalWorkflowExecution(wc::SignalExternalWorkflowExecution {
                seq: n(seq, "SignalExternal.seq")?,
                target: Some(target),
                signal_name,
                args: input,
                ..Default::default()
            })
        }
        C::CancelSignalExternal { seq } => Cmd::CancelSignalWorkflow(wc::CancelSignalWorkflow {
            seq: n(seq, "CancelSignalExternal.seq")?,
        }),
        C::CancelExternal {
            seq,
            workflow_id,
            run_id,
            reason,
        } => Cmd::RequestCancelExternalWorkflowExecution(
            wc::RequestCancelExternalWorkflowExecution {
                seq: n(seq, "CancelExternal.seq")?,
                workflow_execution: Some(NamespacedWorkflowExecution {
                    namespace: namespace.to_string(),
                    workflow_id,
                    run_id,
                }),
                reason,
            },
        ),
        C::UpdateAccepted {
            protocol_instance_id,
        } => Cmd::UpdateResponse(wc::UpdateResponse {
            protocol_instance_id,
            response: Some(wc::update_response::Response::Accepted(())),
        }),
        C::UpdateRejected {
            protocol_instance_id,
            failure,
        } => Cmd::UpdateResponse(wc::UpdateResponse {
            protocol_instance_id,
            response: Some(wc::update_response::Response::Rejected(failure)),
        }),
        C::UpdateCompleted {
            protocol_instance_id,
            result,
        } => Cmd::UpdateResponse(wc::UpdateResponse {
            protocol_instance_id,
            response: Some(wc::update_response::Response::Completed(result)),
        }),
        C::ScheduleNexusOperation {
            seq,
            endpoint,
            service,
            operation,
            input,
            schedule_to_close_ms,
            cancellation_type,
        } => Cmd::ScheduleNexusOperation(wc::ScheduleNexusOperation {
            seq: n(seq, "ScheduleNexusOperation.seq")?,
            endpoint,
            service,
            operation,
            input: Some(input),
            schedule_to_close_timeout: opt_ms(
                schedule_to_close_ms,
                "ScheduleNexusOperation.schedule_to_close_ms",
            )?,
            cancellation_type: n(
                cancellation_type,
                "ScheduleNexusOperation.cancellation_type",
            )?,
            ..Default::default()
        }),
        C::CancelNexusOperation { seq } => {
            Cmd::RequestCancelNexusOperation(wc::RequestCancelNexusOperation {
                seq: n(seq, "CancelNexusOperation.seq")?,
            })
        }
        C::UpsertSearchAttributes { attributes } => {
            Cmd::UpsertWorkflowSearchAttributes(wc::UpsertWorkflowSearchAttributes {
                search_attributes: search_attributes(attributes),
            })
        }
        C::UpsertMemo { memo: fields } => {
            Cmd::ModifyWorkflowProperties(wc::ModifyWorkflowProperties {
                upserted_memo: memo(fields),
            })
        }
    };
    Ok(WorkflowCommand {
        variant: Some(variant),
        ..Default::default()
    })
}

/// `namespace` is the worker's: commands that name another workflow are addressed to it.
pub fn decode_completion(
    term: Term,
    namespace: &str,
) -> DecodeResult<WorkflowActivationCompletion> {
    use workflow_completion::workflow_activation_completion::Status;
    let (run_id, status) = match bridge::Completion::from_term(term)? {
        bridge::Completion::Success {
            run_id,
            commands,
            versioning_behavior,
        } => (
            run_id,
            Status::Successful(workflow_completion::Success {
                commands: commands
                    .into_iter()
                    .map(|c| command(c, namespace))
                    .collect::<DecodeResult<_>>()?,
                versioning_behavior: n(versioning_behavior, "Success.versioning_behavior")?,
                ..Default::default()
            }),
        ),
        bridge::Completion::TaskFailure { run_id, failure } => (
            run_id,
            Status::Failed(workflow_completion::Failure {
                failure: Some(failure),
                ..Default::default()
            }),
        ),
    };
    Ok(WorkflowActivationCompletion {
        run_id,
        status: Some(status),
        ..Default::default()
    })
}

/// The server takes only an application failure as an activity's. An activity that returns
/// another kind as it received it, a timeout or a child workflow's failure, fails with an
/// application failure that has it for a cause.
fn application_failure(failure: Failure) -> Failure {
    if let Some(FailureInfo::ApplicationFailureInfo(_)) = failure.failure_info {
        return failure;
    }
    Failure {
        message: failure.message.clone(),
        source: "GleamSDK".to_string(),
        cause: Some(Box::new(failure)),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(Default::default())),
        ..Default::default()
    }
}

pub fn decode_activity_result(term: Term) -> DecodeResult<ActivityExecutionResult> {
    use activity_execution_result::Status;
    let status = match bridge::ActivityResult::from_term(term)? {
        bridge::ActivityResult::ActivityCompleted { result } => {
            Status::Completed(ar::Success { result })
        }
        bridge::ActivityResult::ActivityFailed { failure } => Status::Failed(ar::Failure {
            failure: Some(application_failure(failure)),
            ..Default::default()
        }),
        bridge::ActivityResult::ActivityCancelled { failure } => {
            Status::Cancelled(ar::Cancellation {
                failure: Some(failure),
            })
        }
        bridge::ActivityResult::ActivityWillCompleteAsync => {
            Status::WillCompleteAsync(ar::WillCompleteAsync {})
        }
    };
    Ok(ActivityExecutionResult {
        status: Some(status),
    })
}

pub fn decode_nexus_result(term: Term, task_token: Vec<u8>) -> DecodeResult<NexusTaskCompletion> {
    use temporalio_common::protos::{
        coresdk::nexus::nexus_task_completion::Status,
        temporal::api::{
            failure::v1::NexusHandlerFailureInfo,
            nexus::v1::{
                CancelOperationResponse, Response, StartOperationResponse, response,
                start_operation_response,
            },
        },
    };
    let start = |variant| {
        Status::Completed(Response {
            variant: Some(response::Variant::StartOperation(StartOperationResponse {
                variant: Some(variant),
            })),
        })
    };
    let status = match bridge::NexusResult::from_term(term)? {
        bridge::NexusResult::NexusSyncSuccess { result } => start(
            start_operation_response::Variant::SyncSuccess(start_operation_response::Sync {
                payload: Some(result),
                ..Default::default()
            }),
        ),
        bridge::NexusResult::NexusAsyncSuccess { operation_token } => start(
            start_operation_response::Variant::AsyncSuccess(start_operation_response::Async {
                operation_token,
                ..Default::default()
            }),
        ),
        bridge::NexusResult::NexusOperationFailed { failure } => {
            start(start_operation_response::Variant::Failure(failure))
        }
        bridge::NexusResult::NexusHandlerFailed {
            error_type,
            failure: cause,
        } => Status::Failure(Failure {
            message: cause.message.clone(),
            source: "GleamSDK".to_string(),
            cause: Some(Box::new(cause)),
            failure_info: Some(FailureInfo::NexusHandlerFailureInfo(
                NexusHandlerFailureInfo {
                    r#type: error_type,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }),
        bridge::NexusResult::NexusCancelAcknowledged => Status::Completed(Response {
            variant: Some(response::Variant::CancelOperation(
                CancelOperationResponse {},
            )),
        }),
        bridge::NexusResult::NexusTaskCancelAcknowledged => Status::AckCancel(true),
    };
    Ok(NexusTaskCompletion {
        task_token,
        status: Some(status),
    })
}
