//! Schedules: the `WorkflowService` schedule calls, exchanging `bridge.Schedule`.

use crate::{
    ConnectionRes, atoms, bridge,
    client::{StartRequest, invalid, rpc_err},
    convert::{count, duration_ms, millis, ms, n, opt_ms, timestamp, to_retry_policy},
    ok, spawn_reply,
    wire::{Bytes, DecodeResult, Wire},
};
use prost::Message;
use rustler::{Atom, Encoder, Env, LocalPid, ResourceArc, Term};
use temporalio_common::protos::temporal::api::{
    common::v1::{Payloads, WorkflowType},
    schedule::v1::{
        BackfillRequest, IntervalSpec, Range, Schedule, ScheduleAction, ScheduleInfo,
        SchedulePatch, SchedulePolicies, ScheduleSpec, ScheduleState, StructuredCalendarSpec,
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

/// `{Start, End, Step}` ranges of one calendar field, named for the errors.
fn ranges(ranges: Vec<(i64, i64, i64)>, name: &str) -> DecodeResult<Vec<Range>> {
    ranges
        .into_iter()
        .map(|(start, end, step)| {
            Ok(Range {
                start: n(start, name)?,
                end: n(end, name)?,
                step: n(step, name)?,
            })
        })
        .collect()
}

fn to_ranges(ranges: Vec<Range>) -> Vec<(i64, i64, i64)> {
    ranges
        .into_iter()
        // The server leaves out an end equal to the start, and a step of one.
        .map(|r| {
            (
                i64::from(r.start),
                i64::from(r.end.max(r.start)),
                i64::from(r.step.max(1)),
            )
        })
        .collect()
}

fn calendar(calendar: bridge::ScheduleCalendar) -> DecodeResult<StructuredCalendarSpec> {
    let bridge::ScheduleCalendar {
        second,
        minute,
        hour,
        day_of_month,
        month,
        year,
        day_of_week,
        comment,
    } = calendar;
    Ok(StructuredCalendarSpec {
        second: ranges(second, "ScheduleCalendar.second")?,
        minute: ranges(minute, "ScheduleCalendar.minute")?,
        hour: ranges(hour, "ScheduleCalendar.hour")?,
        day_of_month: ranges(day_of_month, "ScheduleCalendar.day_of_month")?,
        month: ranges(month, "ScheduleCalendar.month")?,
        year: ranges(year, "ScheduleCalendar.year")?,
        day_of_week: ranges(day_of_week, "ScheduleCalendar.day_of_week")?,
        comment,
    })
}

fn to_calendar(c: StructuredCalendarSpec) -> bridge::ScheduleCalendar {
    bridge::ScheduleCalendar {
        second: to_ranges(c.second),
        minute: to_ranges(c.minute),
        hour: to_ranges(c.hour),
        day_of_month: to_ranges(c.day_of_month),
        month: to_ranges(c.month),
        year: to_ranges(c.year),
        day_of_week: to_ranges(c.day_of_week),
        comment: c.comment,
    }
}

fn spec(spec: bridge::ScheduleSpec) -> DecodeResult<ScheduleSpec> {
    let bridge::ScheduleSpec {
        intervals,
        calendars,
        cron_expressions,
        timezone,
        jitter_ms,
    } = spec;
    Ok(ScheduleSpec {
        interval: intervals
            .into_iter()
            .map(|(every, offset)| {
                Ok(IntervalSpec {
                    interval: Some(ms(every, "ScheduleSpec.intervals")?),
                    phase: Some(ms(offset, "ScheduleSpec.intervals")?),
                })
            })
            .collect::<DecodeResult<_>>()?,
        structured_calendar: calendars
            .into_iter()
            .map(calendar)
            .collect::<DecodeResult<_>>()?,
        cron_string: cron_expressions,
        timezone_name: timezone,
        jitter: opt_ms(jitter_ms, "ScheduleSpec.jitter_ms")?,
        ..Default::default()
    })
}

/// A length of time the server reports as zero when it was never set.
fn set_ms(d: Option<prost_wkt_types::Duration>) -> Option<i64> {
    duration_ms(d).filter(|ms| *ms > 0)
}

fn to_spec(s: ScheduleSpec) -> bridge::ScheduleSpec {
    bridge::ScheduleSpec {
        intervals: s
            .interval
            .into_iter()
            .map(|i| {
                (
                    duration_ms(i.interval).unwrap_or(0),
                    duration_ms(i.phase).unwrap_or(0),
                )
            })
            .collect(),
        calendars: s.structured_calendar.into_iter().map(to_calendar).collect(),
        cron_expressions: s.cron_string,
        timezone: s.timezone_name,
        jitter_ms: set_ms(s.jitter),
    }
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

/// `bridge.StartRequest` from a schedule's action. A schedule stores less than a start
/// request holds, and the rest is left empty.
fn to_start_request(a: Option<ScheduleAction>) -> Result<bridge::StartRequest, tonic::Status> {
    let Some(Action::StartWorkflow(w)) = a.and_then(|a| a.action) else {
        return Err(tonic::Status::internal(
            "the schedule's action does not start a workflow",
        ));
    };
    Ok(bridge::StartRequest {
        workflow_id: w.workflow_id,
        workflow_type: w.workflow_type.map(|t| t.name).unwrap_or_default(),
        task_queue: w.task_queue.map(|t| t.name).unwrap_or_default(),
        input: w.input.map(|p| p.payloads).unwrap_or_default(),
        id_reuse_policy: i64::from(w.workflow_id_reuse_policy),
        id_conflict_policy: 0,
        execution_timeout_ms: set_ms(w.workflow_execution_timeout),
        run_timeout_ms: set_ms(w.workflow_run_timeout),
        task_timeout_ms: set_ms(w.workflow_task_timeout),
        retry: w.retry_policy.map(to_retry_policy),
        memo: w.memo.map(|m| m.fields).unwrap_or_default(),
        search_attributes: w
            .search_attributes
            .map(|a| a.indexed_fields)
            .unwrap_or_default(),
        start_delay_ms: None,
        cron_schedule: String::new(),
        request_id: String::new(),
        callbacks: vec![],
        headers: w.header.map(|h| h.fields).unwrap_or_default(),
    })
}

fn schedule(schedule: bridge::Schedule) -> DecodeResult<Schedule> {
    let bridge::Schedule {
        spec: schedule_spec,
        action: start,
        overlap_policy,
        catchup_window_ms,
        pause_on_failure,
        paused,
        note,
    } = schedule;
    Ok(Schedule {
        spec: Some(spec(schedule_spec)?),
        action: Some(action(start.try_into()?)),
        policies: Some(SchedulePolicies {
            overlap_policy: n(overlap_policy, "Schedule.overlap_policy")?,
            catchup_window: opt_ms(catchup_window_ms, "Schedule.catchup_window_ms")?,
            pause_on_failure,
            ..Default::default()
        }),
        state: Some(ScheduleState {
            paused,
            notes: note,
            ..Default::default()
        }),
    })
}

pub fn to_schedule(s: Schedule) -> Result<bridge::Schedule, tonic::Status> {
    let policies = s.policies.unwrap_or_default();
    let state = s.state.unwrap_or_default();
    Ok(bridge::Schedule {
        spec: to_spec(s.spec.unwrap_or_default()),
        action: to_start_request(s.action)?,
        overlap_policy: i64::from(policies.overlap_policy),
        catchup_window_ms: set_ms(policies.catchup_window),
        pause_on_failure: policies.pause_on_failure,
        paused: state.paused,
        note: state.notes,
    })
}

fn to_info(info: ScheduleInfo) -> bridge::ScheduleInfo {
    bridge::ScheduleInfo {
        action_count: info.action_count,
        recent_actions: info
            .recent_actions
            .into_iter()
            .map(|a| {
                (
                    millis(a.schedule_time),
                    a.start_workflow_result
                        .map(|w| w.workflow_id)
                        .unwrap_or_default(),
                )
            })
            .collect(),
        next_action_times_ms: info
            .future_action_times
            .into_iter()
            .map(|t| millis(Some(t)))
            .collect(),
        running_workflows: count(info.running_workflows.len()),
    }
}

fn patch(patch: bridge::SchedulePatch) -> DecodeResult<SchedulePatch> {
    Ok(match patch {
        bridge::SchedulePatch::Pause { note } => SchedulePatch {
            pause: note,
            ..Default::default()
        },
        bridge::SchedulePatch::Unpause { note } => SchedulePatch {
            unpause: note,
            ..Default::default()
        },
        bridge::SchedulePatch::Trigger { overlap_policy } => SchedulePatch {
            trigger_immediately: Some(TriggerImmediatelyRequest {
                overlap_policy: n(overlap_policy, "Trigger.overlap_policy")?,
                ..Default::default()
            }),
            ..Default::default()
        },
        bridge::SchedulePatch::Backfill {
            from_ms,
            to_ms,
            overlap_policy,
        } => SchedulePatch {
            backfill_request: vec![BackfillRequest {
                start_time: Some(timestamp(from_ms)),
                end_time: Some(timestamp(to_ms)),
                overlap_policy: n(overlap_policy, "Backfill.overlap_policy")?,
            }],
            ..Default::default()
        },
    })
}

fn unit<'a>(env: Env<'a>, r: Result<(), tonic::Status>) -> Term<'a> {
    match r {
        Ok(()) => ok(env, atoms::nil().encode(env)),
        Err(s) => rpc_err(env, &s),
    }
}

/// Write onto `stored`, the schedule as the server has it, the fields in which `changed`
/// differs from `described`, which is what Gleam was shown of `stored`. A field the caller
/// left alone keeps the server's value exactly, and so does everything `bridge.Schedule`
/// does not model: the start and end time, excluded calendars, the remaining-actions limit,
/// the action's headers and id reuse policy, and whatever the server adds later.
pub fn overwrite(
    stored: &mut Schedule,
    described: bridge::Schedule,
    changed: bridge::Schedule,
) -> DecodeResult<()> {
    let bridge::Schedule {
        spec: new_spec,
        action: new_action,
        overlap_policy,
        catchup_window_ms,
        pause_on_failure,
        paused,
        note,
    } = changed;

    if new_spec != described.spec {
        let old = described.spec;
        let bridge::ScheduleSpec {
            intervals,
            calendars,
            cron_expressions,
            timezone,
            jitter_ms,
        } = new_spec.clone();
        let new = spec(new_spec)?;
        let stored = stored.spec.get_or_insert_default();
        if intervals != old.intervals {
            stored.interval = new.interval;
        }
        if calendars != old.calendars {
            stored.structured_calendar = new.structured_calendar;
        }
        if cron_expressions != old.cron_expressions {
            stored.cron_string = new.cron_string;
        }
        if timezone != old.timezone {
            stored.timezone_name = new.timezone_name;
            // The server prefers the zone's rules, when given, to its name.
            stored.timezone_data = vec![];
        }
        if jitter_ms != old.jitter_ms {
            stored.jitter = new.jitter;
        }
    }

    if new_action != described.action {
        let old = described.action;
        let new: StartRequest = new_action.clone().try_into()?;
        let action = stored.action.get_or_insert_default();
        if !matches!(action.action, Some(Action::StartWorkflow(_))) {
            action.action = Some(Action::StartWorkflow(Default::default()));
        }
        let Some(Action::StartWorkflow(stored)) = &mut action.action else {
            unreachable!("set above")
        };
        // Only what `schedule.Action` has: the rest of a start request is not a schedule's.
        if new_action.workflow_id != old.workflow_id {
            stored.workflow_id = new.workflow_id;
        }
        if new_action.workflow_type != old.workflow_type {
            stored.workflow_type = Some(WorkflowType {
                name: new.workflow_type,
            });
        }
        if new_action.task_queue != old.task_queue {
            stored.task_queue.get_or_insert_default().name = new.task_queue;
        }
        if new_action.input != old.input {
            stored.input = Some(Payloads {
                payloads: new.input,
            });
        }
        if new_action.execution_timeout_ms != old.execution_timeout_ms {
            stored.workflow_execution_timeout = new.execution_timeout;
        }
        if new_action.run_timeout_ms != old.run_timeout_ms {
            stored.workflow_run_timeout = new.run_timeout;
        }
        if new_action.task_timeout_ms != old.task_timeout_ms {
            stored.workflow_task_timeout = new.task_timeout;
        }
        if new_action.retry != old.retry {
            stored.retry_policy = new.retry;
        }
        if new_action.memo != old.memo {
            stored.memo = new.memo;
        }
        if new_action.search_attributes != old.search_attributes {
            stored.search_attributes = new.search_attributes;
        }
    }

    let policies = stored.policies.get_or_insert_default();
    if overlap_policy != described.overlap_policy {
        policies.overlap_policy = n(overlap_policy, "Schedule.overlap_policy")?;
    }
    if catchup_window_ms != described.catchup_window_ms {
        policies.catchup_window = opt_ms(catchup_window_ms, "Schedule.catchup_window_ms")?;
    }
    if pause_on_failure != described.pause_on_failure {
        policies.pause_on_failure = pause_on_failure;
    }

    let state = stored.state.get_or_insert_default();
    if paused != described.paused {
        state.paused = paused;
    }
    if note != described.note {
        state.notes = note;
    }
    Ok(())
}

#[rustler::nif]
fn schedule_create<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    schedule_id: String,
    schedule: Term<'a>,
    trigger_immediately: bool,
) -> Term<'a> {
    let schedule = match bridge::Schedule::from_term(schedule).and_then(self::schedule) {
        Ok(s) => s,
        Err(e) => return invalid(env, format!("invalid schedule: {e}")),
    };
    let fut = async move {
        conn.connection
            .workflow_service()
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
                    request_id: uuid::Uuid::new_v4().to_string(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .map(|_| ())
    };
    spawn_reply(pid, reference, fut, unit);
    atoms::ok().encode(env)
}

/// Replace the schedule `schedule_describe` answered. `stored` is the third element of its
/// answer; `described` the first, and `changed` what the caller made of it. See `overwrite`.
#[rustler::nif]
fn schedule_update<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    schedule_id: String,
    stored: Term<'a>,
    described: Term<'a>,
    changed: Term<'a>,
) -> Term<'a> {
    let request = (|| {
        let stored = Bytes::from_term(stored)?;
        let mut request = UpdateScheduleRequest::decode(stored.0.as_slice())
            .map_err(|e| format!("not a schedule from schedule_describe: {e}"))?;
        overwrite(
            request.schedule.get_or_insert_default(),
            bridge::Schedule::from_term(described)?,
            bridge::Schedule::from_term(changed)?,
        )?;
        Ok::<_, String>(request)
    })();
    let request = match request {
        Ok(request) => request,
        Err(e) => return invalid(env, format!("invalid schedule: {e}")),
    };
    let fut = async move {
        conn.connection
            .workflow_service()
            .update_schedule(
                UpdateScheduleRequest {
                    namespace: conn.namespace.clone(),
                    schedule_id,
                    identity: conn.identity.clone(),
                    request_id: uuid::Uuid::new_v4().to_string(),
                    ..request
                }
                .into_request(),
            )
            .await
            .map(|_| ())
    };
    spawn_reply(pid, reference, fut, unit);
    atoms::ok().encode(env)
}

/// Answers `{Schedule, ScheduleInfo, Stored}`. `Stored` is for `schedule_update`: the
/// schedule exactly as the server has it and the token that tells whether it has changed
/// since, encoded as the update request they will be sent back in.
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
    spawn_reply(pid, reference, fut, |env, r| {
        let described = r.and_then(|d| {
            let stored = UpdateScheduleRequest {
                schedule: d.schedule.clone(),
                conflict_token: d.conflict_token,
                ..Default::default()
            };
            Ok((
                to_schedule(d.schedule.unwrap_or_default())?,
                to_info(d.info.unwrap_or_default()),
                Bytes(stored.encode_to_vec()),
            ))
        });
        match described {
            Ok(described) => ok(env, described.to_term(env)),
            Err(s) => rpc_err(env, &s),
        }
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

/// `patch` is `{pause, Note}`, `{unpause, Note}`, `{trigger, OverlapPolicy}` or
/// `{backfill, FromMs, ToMs, OverlapPolicy}`.
#[rustler::nif]
fn schedule_patch<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    schedule_id: String,
    patch: Term<'a>,
) -> Term<'a> {
    let patch = match bridge::SchedulePatch::from_term(patch).and_then(self::patch) {
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
