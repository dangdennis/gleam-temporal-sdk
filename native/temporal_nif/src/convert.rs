//! Conversions between core's protobuf messages and the Erlang representation of the Gleam
//! types in `temporal/internal/bridge`. A Gleam record `Foo(a, b)` is the tuple `{foo, A, B}`,
//! a field-less constructor is a bare atom, and `Option` is `{some, X}` or `none`.

use crate::atoms;
use rustler::{
    Atom, Binary, Encoder, Env, MapIterator, NewBinary, Term,
    types::tuple::{get_tuple, make_tuple},
};
use std::collections::HashMap;
use temporalio_common::protos::{
    coresdk::{
        activity_result::{
            self as ar, ActivityExecutionResult, ActivityResolution, activity_execution_result,
            activity_resolution,
        },
        activity_task::{ActivityTask, activity_task},
        child_workflow::{ChildWorkflowResult, child_workflow_result},
        workflow_activation::{
            WorkflowActivation, resolve_child_workflow_execution_start as child_start,
            workflow_activation_job::Variant as Job,
        },
        workflow_commands::{self as wc, WorkflowCommand, workflow_command::Variant as Cmd},
        workflow_completion::{self, WorkflowActivationCompletion},
    },
    temporal::api::{
        common::v1::{Payload, RetryPolicy},
        failure::v1::{ApplicationFailureInfo, CanceledFailureInfo, Failure, failure::FailureInfo},
    },
};

pub type DecodeResult<T> = Result<T, String>;

fn atom<'a>(env: Env<'a>, name: &str) -> Term<'a> {
    Atom::from_str(env, name)
        .expect("atom names are short ASCII")
        .to_term(env)
}

/// A Gleam record: `{tag, Fields...}`.
fn rec<'a>(env: Env<'a>, tag: &str, fields: &[Term<'a>]) -> Term<'a> {
    let mut terms = Vec::with_capacity(fields.len() + 1);
    terms.push(atom(env, tag));
    terms.extend_from_slice(fields);
    make_tuple(env, &terms)
}

pub fn bin<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut b = NewBinary::new(env, bytes.len());
    b.as_mut_slice().copy_from_slice(bytes);
    b.into()
}

fn option<'a>(env: Env<'a>, value: Option<Term<'a>>) -> Term<'a> {
    match value {
        Some(t) => (atoms::some(), t).encode(env),
        None => atoms::none().encode(env),
    }
}

/// Decode a Gleam `Option`, yielding the inner term for `Some`.
pub fn opt(term: Term) -> Option<Term> {
    let parts = get_tuple(term).ok()?;
    (parts.len() == 2).then(|| parts[1])
}

fn millis(ts: Option<impl Into<prost_wkt_types::Timestamp>>) -> i64 {
    ts.map(|t| {
        let t: prost_wkt_types::Timestamp = t.into();
        t.seconds * 1000 + i64::from(t.nanos) / 1_000_000
    })
    .unwrap_or(0)
}

fn duration(ms: u64) -> prost_wkt_types::Duration {
    prost_wkt_types::Duration {
        seconds: (ms / 1000) as i64,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    }
}

// ---- encoding (core -> Gleam) ----

pub fn encode_payload<'a>(env: Env<'a>, p: &Payload) -> Term<'a> {
    let mut metadata = Term::map_new(env);
    for (k, v) in &p.metadata {
        metadata = metadata
            .map_put(k.as_str(), bin(env, v))
            .expect("map_put on a map");
    }
    rec(env, "payload", &[metadata, bin(env, &p.data)])
}

fn encode_payloads<'a>(env: Env<'a>, ps: &[Payload]) -> Term<'a> {
    ps.iter()
        .map(|p| encode_payload(env, p))
        .collect::<Vec<_>>()
        .encode(env)
}

pub fn encode_failure<'a>(env: Env<'a>, f: &Failure) -> Term<'a> {
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
        _ => ("other", "", false),
    };
    let cause = option(env, f.cause.as_deref().map(|c| encode_failure(env, c)));
    rec(
        env,
        "failure",
        &[
            f.message.encode(env),
            atom(env, kind),
            error_type.encode(env),
            non_retryable.encode(env),
            cause,
        ],
    )
}

fn failure_or_default<'a>(env: Env<'a>, f: &Option<Failure>) -> Term<'a> {
    match f {
        Some(f) => encode_failure(env, f),
        None => encode_failure(env, &Failure::default()),
    }
}

fn encode_activity_resolution<'a>(env: Env<'a>, r: Option<ActivityResolution>) -> Term<'a> {
    match r.and_then(|r| r.status) {
        Some(activity_resolution::Status::Completed(s)) => rec(
            env,
            "completed",
            &[option(
                env,
                s.result.as_ref().map(|p| encode_payload(env, p)),
            )],
        ),
        Some(activity_resolution::Status::Failed(f)) => {
            rec(env, "failed", &[failure_or_default(env, &f.failure)])
        }
        Some(activity_resolution::Status::Cancelled(c)) => {
            rec(env, "cancelled", &[failure_or_default(env, &c.failure)])
        }
        // Backoff is only issued for local activities, which this SDK never schedules.
        Some(activity_resolution::Status::Backoff(_)) | None => {
            rec(env, "failed", &[failure_or_default(env, &None)])
        }
    }
}

fn encode_child_result<'a>(env: Env<'a>, r: Option<ChildWorkflowResult>) -> Term<'a> {
    match r.and_then(|r| r.status) {
        Some(child_workflow_result::Status::Completed(s)) => rec(
            env,
            "completed",
            &[option(
                env,
                s.result.as_ref().map(|p| encode_payload(env, p)),
            )],
        ),
        Some(child_workflow_result::Status::Failed(f)) => {
            rec(env, "failed", &[failure_or_default(env, &f.failure)])
        }
        Some(child_workflow_result::Status::Cancelled(c)) => {
            rec(env, "cancelled", &[failure_or_default(env, &c.failure)])
        }
        None => rec(env, "failed", &[failure_or_default(env, &None)]),
    }
}

fn encode_job<'a>(env: Env<'a>, job: Job) -> Term<'a> {
    match job {
        Job::InitializeWorkflow(j) => rec(
            env,
            "initialize_workflow",
            &[
                j.workflow_type.encode(env),
                j.workflow_id.encode(env),
                encode_payloads(env, &j.arguments),
                j.randomness_seed.encode(env),
                j.attempt.encode(env),
            ],
        ),
        Job::FireTimer(j) => rec(env, "fire_timer", &[j.seq.encode(env)]),
        Job::ResolveActivity(j) => rec(
            env,
            "resolve_activity",
            &[j.seq.encode(env), encode_activity_resolution(env, j.result)],
        ),
        Job::UpdateRandomSeed(j) => {
            rec(env, "update_random_seed", &[j.randomness_seed.encode(env)])
        }
        Job::QueryWorkflow(j) => rec(
            env,
            "query_workflow",
            &[
                j.query_id.encode(env),
                j.query_type.encode(env),
                encode_payloads(env, &j.arguments),
            ],
        ),
        Job::CancelWorkflow(j) => rec(env, "cancel_workflow", &[j.reason.encode(env)]),
        Job::SignalWorkflow(j) => rec(
            env,
            "signal_workflow",
            &[j.signal_name.encode(env), encode_payloads(env, &j.input)],
        ),
        Job::NotifyHasPatch(j) => rec(env, "notify_has_patch", &[j.patch_id.encode(env)]),
        Job::ResolveChildWorkflowExecutionStart(j) => {
            let status = match j.status {
                Some(child_start::Status::Succeeded(s)) => {
                    rec(env, "child_started", &[s.run_id.encode(env)])
                }
                Some(child_start::Status::Failed(f)) => rec(
                    env,
                    "child_start_failed",
                    &[f.workflow_id.encode(env), f.cause.encode(env)],
                ),
                Some(child_start::Status::Cancelled(c)) => rec(
                    env,
                    "child_start_cancelled",
                    &[failure_or_default(env, &c.failure)],
                ),
                None => rec(env, "child_start_failed", &["".encode(env), 0.encode(env)]),
            };
            rec(
                env,
                "resolve_child_workflow_start",
                &[j.seq.encode(env), status],
            )
        }
        Job::ResolveChildWorkflowExecution(j) => rec(
            env,
            "resolve_child_workflow",
            &[j.seq.encode(env), encode_child_result(env, j.result)],
        ),
        Job::RemoveFromCache(j) => rec(
            env,
            "remove_from_cache",
            &[j.message.encode(env), j.reason.encode(env)],
        ),
        Job::ResolveSignalExternalWorkflow(_) => unsupported(env, "resolve_signal_external"),
        Job::ResolveRequestCancelExternalWorkflow(_) => {
            unsupported(env, "resolve_request_cancel_external")
        }
        Job::DoUpdate(_) => unsupported(env, "do_update"),
        Job::ResolveNexusOperationStart(_) => unsupported(env, "resolve_nexus_operation_start"),
        Job::ResolveNexusOperation(_) => unsupported(env, "resolve_nexus_operation"),
    }
}

fn unsupported<'a>(env: Env<'a>, name: &str) -> Term<'a> {
    rec(env, "unsupported_job", &[name.encode(env)])
}

pub fn encode_activation<'a>(env: Env<'a>, a: WorkflowActivation) -> Term<'a> {
    let jobs: Vec<Term> = a
        .jobs
        .into_iter()
        .filter_map(|j| j.variant)
        .map(|j| encode_job(env, j))
        .collect();
    rec(
        env,
        "activation",
        &[
            a.run_id.encode(env),
            millis(a.timestamp).encode(env),
            a.is_replaying.encode(env),
            a.history_length.encode(env),
            jobs.encode(env),
        ],
    )
}

pub fn encode_activity_task<'a>(env: Env<'a>, t: ActivityTask) -> Term<'a> {
    let token = bin(env, &t.task_token);
    match t.variant {
        Some(activity_task::Variant::Start(s)) => {
            let workflow_id = s
                .workflow_execution
                .as_ref()
                .map(|e| e.workflow_id.as_str())
                .unwrap_or("");
            rec(
                env,
                "activity_start",
                &[
                    token,
                    s.activity_type.encode(env),
                    s.activity_id.encode(env),
                    workflow_id.encode(env),
                    s.run_id.encode(env),
                    encode_payloads(env, &s.input),
                    s.attempt.encode(env),
                    encode_payloads(env, &s.heartbeat_details),
                ],
            )
        }
        Some(activity_task::Variant::Cancel(c)) => {
            rec(env, "activity_cancel", &[token, c.reason.encode(env)])
        }
        None => rec(env, "activity_cancel", &[token, 0.encode(env)]),
    }
}

// ---- decoding (Gleam -> core) ----

/// Split a record into its tag and fields; a bare atom is a record without fields.
fn untag(term: Term) -> DecodeResult<(String, Vec<Term>)> {
    if term.is_atom() {
        return Ok((term.atom_to_string().map_err(|_| "bad atom")?, vec![]));
    }
    let parts = get_tuple(term).map_err(|_| "expected a record".to_string())?;
    let tag = parts
        .first()
        .and_then(|t| t.atom_to_string().ok())
        .ok_or("record without a tag")?;
    Ok((tag, parts[1..].to_vec()))
}

fn get<'a, T: rustler::Decoder<'a>>(fields: &[Term<'a>], i: usize) -> DecodeResult<T> {
    fields
        .get(i)
        .ok_or_else(|| format!("missing field {i}"))?
        .decode()
        .map_err(|_| format!("field {i} has the wrong type"))
}

fn field<'a>(fields: &[Term<'a>], i: usize) -> DecodeResult<Term<'a>> {
    fields
        .get(i)
        .copied()
        .ok_or_else(|| format!("missing field {i}"))
}

pub fn decode_payload(term: Term) -> DecodeResult<Payload> {
    let (_, f) = untag(term)?;
    let mut metadata = HashMap::new();
    let iter = MapIterator::new(field(&f, 0)?).ok_or("payload metadata is not a map")?;
    for (k, v) in iter {
        let k: String = k.decode().map_err(|_| "bad metadata key")?;
        let v: Binary = v.decode().map_err(|_| "bad metadata value")?;
        metadata.insert(k, v.as_slice().to_vec());
    }
    let data: Binary = get(&f, 1)?;
    Ok(Payload {
        metadata,
        data: data.as_slice().to_vec(),
        ..Default::default()
    })
}

pub fn decode_payloads(term: Term) -> DecodeResult<Vec<Payload>> {
    let list: Vec<Term> = term.decode().map_err(|_| "expected a list of payloads")?;
    list.into_iter().map(decode_payload).collect()
}

fn decode_opt_payload(term: Term) -> DecodeResult<Option<Payload>> {
    opt(term).map(decode_payload).transpose()
}

pub fn decode_failure(term: Term) -> DecodeResult<Failure> {
    let (_, f) = untag(term)?;
    let (kind, _) = untag(field(&f, 1)?)?;
    let info = if kind == "cancelled" {
        FailureInfo::CanceledFailureInfo(CanceledFailureInfo::default())
    } else {
        FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo {
            r#type: get(&f, 2)?,
            non_retryable: get(&f, 3)?,
            ..Default::default()
        })
    };
    let cause = opt(field(&f, 4)?)
        .map(decode_failure)
        .transpose()?
        .map(Box::new);
    Ok(Failure {
        message: get(&f, 0)?,
        source: "GleamSDK".to_string(),
        cause,
        failure_info: Some(info),
        ..Default::default()
    })
}

fn decode_opt_ms(term: Term) -> DecodeResult<Option<prost_wkt_types::Duration>> {
    match opt(term) {
        Some(t) => {
            let ms: u64 = t
                .decode()
                .map_err(|_| "duration must be a non-negative Int")?;
            Ok(Some(duration(ms)))
        }
        None => Ok(None),
    }
}

fn decode_retry(term: Term) -> DecodeResult<RetryPolicy> {
    let (_, f) = untag(term)?;
    Ok(RetryPolicy {
        initial_interval: Some(duration(get(&f, 0)?)),
        backoff_coefficient: get(&f, 1)?,
        maximum_interval: decode_opt_ms(field(&f, 2)?)?,
        maximum_attempts: get(&f, 3)?,
        non_retryable_error_types: get(&f, 4)?,
    })
}

fn decode_command(term: Term) -> DecodeResult<WorkflowCommand> {
    let (tag, f) = untag(term)?;
    let variant = match tag.as_str() {
        "start_timer" => Cmd::StartTimer(wc::StartTimer {
            seq: get(&f, 0)?,
            start_to_fire_timeout: Some(duration(get(&f, 1)?)),
        }),
        "cancel_timer" => Cmd::CancelTimer(wc::CancelTimer { seq: get(&f, 0)? }),
        "schedule_activity" => Cmd::ScheduleActivity(wc::ScheduleActivity {
            seq: get(&f, 0)?,
            activity_id: get(&f, 1)?,
            activity_type: get(&f, 2)?,
            task_queue: get(&f, 3)?,
            arguments: decode_payloads(field(&f, 4)?)?,
            schedule_to_close_timeout: decode_opt_ms(field(&f, 5)?)?,
            start_to_close_timeout: decode_opt_ms(field(&f, 6)?)?,
            heartbeat_timeout: decode_opt_ms(field(&f, 7)?)?,
            retry_policy: opt(field(&f, 8)?).map(decode_retry).transpose()?,
            ..Default::default()
        }),
        "request_cancel_activity" => {
            Cmd::RequestCancelActivity(wc::RequestCancelActivity { seq: get(&f, 0)? })
        }
        "respond_to_query" => {
            let (result_tag, r) = untag(field(&f, 1)?)?;
            let variant = if result_tag == "ok" {
                wc::query_result::Variant::Succeeded(wc::QuerySuccess {
                    response: Some(decode_payload(field(&r, 0)?)?),
                })
            } else {
                wc::query_result::Variant::Failed(decode_failure(field(&r, 0)?)?)
            };
            Cmd::RespondToQuery(wc::QueryResult {
                query_id: get(&f, 0)?,
                variant: Some(variant),
            })
        }
        "complete_workflow" => Cmd::CompleteWorkflowExecution(wc::CompleteWorkflowExecution {
            result: decode_opt_payload(field(&f, 0)?)?,
        }),
        "fail_workflow" => Cmd::FailWorkflowExecution(wc::FailWorkflowExecution {
            failure: Some(decode_failure(field(&f, 0)?)?),
        }),
        "continue_as_new" => {
            Cmd::ContinueAsNewWorkflowExecution(wc::ContinueAsNewWorkflowExecution {
                workflow_type: get(&f, 0)?,
                task_queue: get(&f, 1)?,
                arguments: decode_payloads(field(&f, 2)?)?,
                ..Default::default()
            })
        }
        "cancel_workflow_execution" => {
            Cmd::CancelWorkflowExecution(wc::CancelWorkflowExecution::default())
        }
        "set_patch_marker" => Cmd::SetPatchMarker(wc::SetPatchMarker {
            patch_id: get(&f, 0)?,
            deprecated: get(&f, 1)?,
        }),
        "start_child_workflow" => {
            Cmd::StartChildWorkflowExecution(wc::StartChildWorkflowExecution {
                seq: get(&f, 0)?,
                workflow_id: get(&f, 1)?,
                workflow_type: get(&f, 2)?,
                task_queue: get(&f, 3)?,
                input: decode_payloads(field(&f, 4)?)?,
                ..Default::default()
            })
        }
        "cancel_child_workflow" => {
            Cmd::CancelChildWorkflowExecution(wc::CancelChildWorkflowExecution {
                child_workflow_seq: get(&f, 0)?,
                reason: String::new(),
            })
        }
        other => return Err(format!("unknown command {other}")),
    };
    Ok(WorkflowCommand {
        variant: Some(variant),
        ..Default::default()
    })
}

pub fn decode_completion(term: Term) -> DecodeResult<WorkflowActivationCompletion> {
    use workflow_completion::workflow_activation_completion::Status;
    let (tag, f) = untag(term)?;
    let status = match tag.as_str() {
        "success" => {
            let commands: Vec<Term> = get(&f, 1)?;
            Status::Successful(workflow_completion::Success {
                commands: commands
                    .into_iter()
                    .map(decode_command)
                    .collect::<DecodeResult<_>>()?,
                ..Default::default()
            })
        }
        "task_failure" => Status::Failed(workflow_completion::Failure {
            failure: Some(decode_failure(field(&f, 1)?)?),
            ..Default::default()
        }),
        other => return Err(format!("unknown completion {other}")),
    };
    Ok(WorkflowActivationCompletion {
        run_id: get(&f, 0)?,
        status: Some(status),
        ..Default::default()
    })
}

pub fn decode_activity_result(term: Term) -> DecodeResult<ActivityExecutionResult> {
    use activity_execution_result::Status;
    let (tag, f) = untag(term)?;
    let status = match tag.as_str() {
        "activity_completed" => Status::Completed(ar::Success {
            result: decode_opt_payload(field(&f, 0)?)?,
        }),
        "activity_failed" => Status::Failed(ar::Failure {
            failure: Some(decode_failure(field(&f, 0)?)?),
            ..Default::default()
        }),
        "activity_cancelled" => Status::Cancelled(ar::Cancellation {
            failure: Some(decode_failure(field(&f, 0)?)?),
        }),
        other => return Err(format!("unknown activity result {other}")),
    };
    Ok(ActivityExecutionResult {
        status: Some(status),
    })
}
