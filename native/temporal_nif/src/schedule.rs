//! Schedules: the `WorkflowService` schedule calls, exchanging `bridge.Schedule`.

use crate::{
    ConnectionRes, atoms,
    client::{StartRequest, decode_start_request, invalid, rpc_err},
    convert::*,
    ok, spawn_reply,
};
use rustler::{Atom, Encoder, Env, LocalPid, ResourceArc, Term};
use temporalio_common::protos::temporal::api::{
    common::v1::{Payloads, WorkflowType},
    schedule::v1::{
        IntervalSpec, Range, Schedule, ScheduleAction, ScheduleInfo, SchedulePatch,
        SchedulePolicies, ScheduleSpec, ScheduleState, StructuredCalendarSpec,
        TriggerImmediatelyRequest, schedule_action::Action,
    },
    taskqueue::v1::TaskQueue,
    workflow::v1::NewWorkflowExecutionInfo,
    workflowservice::v1::{
        CreateScheduleRequest, DeleteScheduleRequest, DescribeScheduleRequest,
        ListSchedulesRequest, PatchScheduleRequest, UpdateScheduleRequest,
    },
};
use tonic::IntoRequest;

fn decode_ranges(term: Term) -> DecodeResult<Vec<Range>> {
    let ranges: Vec<(i32, i32, i32)> = term.decode().map_err(|_| "expected a list of ranges")?;
    Ok(ranges
        .into_iter()
        .map(|(start, end, step)| Range { start, end, step })
        .collect())
}

fn encode_ranges<'a>(env: Env<'a>, ranges: &[Range]) -> Term<'a> {
    ranges
        .iter()
        // The server leaves out an end equal to the start, and a step of one.
        .map(|r| (r.start, r.end.max(r.start), r.step.max(1)))
        .collect::<Vec<_>>()
        .encode(env)
}

fn decode_calendar(term: Term) -> DecodeResult<StructuredCalendarSpec> {
    let (_, f) = untag(term)?;
    Ok(StructuredCalendarSpec {
        second: decode_ranges(field(&f, 0)?)?,
        minute: decode_ranges(field(&f, 1)?)?,
        hour: decode_ranges(field(&f, 2)?)?,
        day_of_month: decode_ranges(field(&f, 3)?)?,
        month: decode_ranges(field(&f, 4)?)?,
        year: decode_ranges(field(&f, 5)?)?,
        day_of_week: decode_ranges(field(&f, 6)?)?,
        comment: get(&f, 7)?,
    })
}

fn encode_calendar<'a>(env: Env<'a>, c: &StructuredCalendarSpec) -> Term<'a> {
    rec(
        env,
        "schedule_calendar",
        &[
            encode_ranges(env, &c.second),
            encode_ranges(env, &c.minute),
            encode_ranges(env, &c.hour),
            encode_ranges(env, &c.day_of_month),
            encode_ranges(env, &c.month),
            encode_ranges(env, &c.year),
            encode_ranges(env, &c.day_of_week),
            c.comment.encode(env),
        ],
    )
}

fn decode_spec(term: Term) -> DecodeResult<ScheduleSpec> {
    let (_, f) = untag(term)?;
    let intervals: Vec<(u64, u64)> = get(&f, 0)?;
    let calendars: Vec<Term> = get(&f, 1)?;
    Ok(ScheduleSpec {
        interval: intervals
            .into_iter()
            .map(|(every, offset)| IntervalSpec {
                interval: Some(duration(every)),
                phase: Some(duration(offset)),
            })
            .collect(),
        structured_calendar: calendars
            .into_iter()
            .map(decode_calendar)
            .collect::<DecodeResult<_>>()?,
        cron_string: get(&f, 2)?,
        timezone_name: get(&f, 3)?,
        jitter: decode_opt_ms(field(&f, 4)?)?,
        ..Default::default()
    })
}

fn encode_spec<'a>(env: Env<'a>, s: &ScheduleSpec) -> Term<'a> {
    let intervals: Vec<(i64, i64)> = s
        .interval
        .iter()
        .map(|i| {
            (
                duration_ms(i.interval).unwrap_or(0),
                duration_ms(i.phase).unwrap_or(0),
            )
        })
        .collect();
    let calendars: Vec<Term> = s
        .structured_calendar
        .iter()
        .map(|c| encode_calendar(env, c))
        .collect();
    rec(
        env,
        "schedule_spec",
        &[
            intervals.encode(env),
            calendars.encode(env),
            s.cron_string.encode(env),
            s.timezone_name.encode(env),
            option(
                env,
                duration_ms(s.jitter)
                    .filter(|ms| *ms > 0)
                    .map(|ms| ms.encode(env)),
            ),
        ],
    )
}

fn action(r: StartRequest) -> ScheduleAction {
    ScheduleAction {
        action: Some(Action::StartWorkflow(NewWorkflowExecutionInfo {
            workflow_id: r.workflow_id,
            workflow_type: Some(WorkflowType {
                name: r.workflow_type,
            }),
            task_queue: Some(TaskQueue {
                name: r.task_queue,
                ..Default::default()
            }),
            input: Some(Payloads { payloads: r.input }),
            workflow_execution_timeout: r.execution_timeout,
            workflow_run_timeout: r.run_timeout,
            workflow_task_timeout: r.task_timeout,
            workflow_id_reuse_policy: r.id_reuse_policy,
            retry_policy: r.retry,
            memo: r.memo,
            search_attributes: r.search_attributes,
            header: r.header,
            ..Default::default()
        })),
    }
}

/// `bridge.StartRequest` from a schedule's action.
fn encode_action<'a>(env: Env<'a>, a: Option<ScheduleAction>) -> Term<'a> {
    let Some(Action::StartWorkflow(w)) = a.and_then(|a| a.action) else {
        return atoms::nil().encode(env);
    };
    let ms = |d| {
        option(
            env,
            duration_ms(d).filter(|ms| *ms > 0).map(|ms| ms.encode(env)),
        )
    };
    let retry = w.retry_policy.map(|r| {
        rec(
            env,
            "retry_policy",
            &[
                duration_ms(r.initial_interval).unwrap_or(0).encode(env),
                r.backoff_coefficient.encode(env),
                ms(r.maximum_interval),
                r.maximum_attempts.encode(env),
                r.non_retryable_error_types.encode(env),
            ],
        )
    });
    rec(
        env,
        "start_request",
        &[
            w.workflow_id.encode(env),
            w.workflow_type
                .map(|t| t.name)
                .unwrap_or_default()
                .encode(env),
            w.task_queue.map(|t| t.name).unwrap_or_default().encode(env),
            encode_payloads(env, &w.input.map(|p| p.payloads).unwrap_or_default()),
            w.workflow_id_reuse_policy.encode(env),
            0.encode(env),
            ms(w.workflow_execution_timeout),
            ms(w.workflow_run_timeout),
            ms(w.workflow_task_timeout),
            option(env, retry),
            encode_payload_map(env, &w.memo.map(|m| m.fields).unwrap_or_default()),
            encode_payload_map(
                env,
                &w.search_attributes
                    .map(|a| a.indexed_fields)
                    .unwrap_or_default(),
            ),
            atoms::none().encode(env),
            "".encode(env),
            "".encode(env),
            Vec::<Term>::new().encode(env),
            encode_payload_map(env, &w.header.map(|h| h.fields).unwrap_or_default()),
        ],
    )
}

fn decode_schedule(term: Term) -> DecodeResult<Schedule> {
    let (_, f) = untag(term)?;
    Ok(Schedule {
        spec: Some(decode_spec(field(&f, 0)?)?),
        action: Some(action(decode_start_request(field(&f, 1)?)?)),
        policies: Some(SchedulePolicies {
            overlap_policy: get(&f, 2)?,
            catchup_window: decode_opt_ms(field(&f, 3)?)?,
            pause_on_failure: get(&f, 4)?,
            ..Default::default()
        }),
        state: Some(ScheduleState {
            paused: get(&f, 5)?,
            notes: get(&f, 6)?,
            ..Default::default()
        }),
    })
}

fn encode_schedule<'a>(env: Env<'a>, s: Schedule) -> Term<'a> {
    let policies = s.policies.unwrap_or_default();
    let state = s.state.unwrap_or_default();
    rec(
        env,
        "schedule",
        &[
            encode_spec(env, &s.spec.unwrap_or_default()),
            encode_action(env, s.action),
            policies.overlap_policy.encode(env),
            option(
                env,
                duration_ms(policies.catchup_window)
                    .filter(|ms| *ms > 0)
                    .map(|ms| ms.encode(env)),
            ),
            policies.pause_on_failure.encode(env),
            state.paused.encode(env),
            state.notes.encode(env),
        ],
    )
}

fn encode_info<'a>(env: Env<'a>, info: ScheduleInfo) -> Term<'a> {
    let recent: Vec<Term> = info
        .recent_actions
        .iter()
        .map(|a| {
            (
                millis(a.schedule_time),
                a.start_workflow_result
                    .as_ref()
                    .map(|w| w.workflow_id.as_str())
                    .unwrap_or(""),
            )
                .encode(env)
        })
        .collect();
    let upcoming: Vec<i64> = info
        .future_action_times
        .into_iter()
        .map(|t| millis(Some(t)))
        .collect();
    rec(
        env,
        "schedule_info",
        &[
            info.action_count.encode(env),
            recent.encode(env),
            upcoming.encode(env),
            (info.running_workflows.len() as i64).encode(env),
        ],
    )
}

fn unit<'a>(env: Env<'a>, r: Result<(), tonic::Status>) -> Term<'a> {
    match r {
        Ok(()) => ok(env, atoms::nil().encode(env)),
        Err(s) => rpc_err(env, &s),
    }
}

/// Create the schedule, or, given a conflict token from `schedule_describe`, replace it.
#[rustler::nif]
fn schedule_put<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    schedule_id: String,
    schedule: Term<'a>,
    conflict_token: Term<'a>,
    trigger_immediately: bool,
) -> Term<'a> {
    let schedule = match decode_schedule(schedule) {
        Ok(s) => s,
        Err(e) => return invalid(env, format!("invalid schedule: {e}")),
    };
    let conflict_token: Option<Vec<u8>> = opt(conflict_token)
        .and_then(|t| t.decode::<rustler::Binary>().ok())
        .map(|b| b.as_slice().to_vec());
    let fut = async move {
        let mut service = conn.connection.workflow_service();
        let request_id = uuid::Uuid::new_v4().to_string();
        match conflict_token {
            None => service
                .create_schedule(
                    CreateScheduleRequest {
                        namespace: conn.namespace.clone(),
                        schedule_id,
                        schedule: Some(schedule),
                        initial_patch: trigger_immediately.then(|| SchedulePatch {
                            trigger_immediately: Some(TriggerImmediatelyRequest::default()),
                            ..Default::default()
                        }),
                        identity: conn.identity.clone(),
                        request_id,
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .map(|_| ()),
            Some(conflict_token) => service
                .update_schedule(
                    UpdateScheduleRequest {
                        namespace: conn.namespace.clone(),
                        schedule_id,
                        schedule: Some(schedule),
                        conflict_token,
                        identity: conn.identity.clone(),
                        request_id,
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .map(|_| ()),
        }
    };
    spawn_reply(pid, reference, fut, unit);
    atoms::ok().encode(env)
}

/// Answers `{Schedule, ScheduleInfo, ConflictToken}`.
#[rustler::nif]
fn schedule_describe<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    schedule_id: String,
) -> Atom {
    let fut = async move {
        conn.connection
            .workflow_service()
            .describe_schedule(
                DescribeScheduleRequest {
                    namespace: conn.namespace.clone(),
                    schedule_id,
                }
                .into_request(),
            )
            .await
            .map(|r| r.into_inner())
    };
    spawn_reply(pid, reference, fut, |env, r| match r {
        Ok(d) => ok(
            env,
            (
                encode_schedule(env, d.schedule.unwrap_or_default()),
                encode_info(env, d.info.unwrap_or_default()),
                bin(env, &d.conflict_token),
            )
                .encode(env),
        ),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}

#[rustler::nif]
fn schedule_delete<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    schedule_id: String,
) -> Atom {
    let fut = async move {
        conn.connection
            .workflow_service()
            .delete_schedule(
                DeleteScheduleRequest {
                    namespace: conn.namespace.clone(),
                    schedule_id,
                    identity: conn.identity.clone(),
                }
                .into_request(),
            )
            .await
            .map(|_| ())
    };
    spawn_reply(pid, reference, fut, unit);
    atoms::ok()
}

/// `patch` is `{pause, Note}`, `{unpause, Note}` or `{trigger, OverlapPolicy}`.
#[rustler::nif]
fn schedule_patch<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    schedule_id: String,
    patch: Term<'a>,
) -> Term<'a> {
    let patch = match untag(patch) {
        Ok((tag, f)) => match tag.as_str() {
            "pause" => get(&f, 0).map(|note| SchedulePatch {
                pause: note,
                ..Default::default()
            }),
            "unpause" => get(&f, 0).map(|note| SchedulePatch {
                unpause: note,
                ..Default::default()
            }),
            "trigger" => get(&f, 0).map(|overlap_policy| SchedulePatch {
                trigger_immediately: Some(TriggerImmediatelyRequest {
                    overlap_policy,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            other => Err(format!("unknown patch {other}")),
        },
        Err(e) => Err(e),
    };
    let patch = match patch {
        Ok(p) => p,
        Err(e) => return invalid(env, e),
    };
    let fut = async move {
        conn.connection
            .workflow_service()
            .patch_schedule(
                PatchScheduleRequest {
                    namespace: conn.namespace.clone(),
                    schedule_id,
                    patch: Some(patch),
                    identity: conn.identity.clone(),
                    request_id: uuid::Uuid::new_v4().to_string(),
                }
                .into_request(),
            )
            .await
            .map(|_| ())
    };
    spawn_reply(pid, reference, fut, unit);
    atoms::ok().encode(env)
}

/// Every schedule in the namespace as `{Id, WorkflowType, Paused, Note}`.
#[rustler::nif]
fn schedule_list<'a>(pid: LocalPid, reference: Term<'a>, conn: ResourceArc<ConnectionRes>) -> Atom {
    let fut = async move {
        let mut found = vec![];
        let mut next_page_token = vec![];
        loop {
            let response = conn
                .connection
                .workflow_service()
                .list_schedules(
                    ListSchedulesRequest {
                        namespace: conn.namespace.clone(),
                        maximum_page_size: 1000,
                        next_page_token,
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await?
                .into_inner();
            found.extend(response.schedules);
            next_page_token = response.next_page_token;
            if next_page_token.is_empty() {
                return Ok::<_, tonic::Status>(found);
            }
        }
    };
    spawn_reply(pid, reference, fut, |env, r| match r {
        Ok(found) => {
            let terms: Vec<Term> = found
                .into_iter()
                .map(|entry| {
                    let info = entry.info.unwrap_or_default();
                    (
                        entry.schedule_id,
                        info.workflow_type.map(|t| t.name).unwrap_or_default(),
                        info.paused,
                        info.notes,
                    )
                        .encode(env)
                })
                .collect();
            ok(env, terms.encode(env))
        }
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}
