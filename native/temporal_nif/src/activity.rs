//! Standalone activities: activities a client starts directly, with no workflow around
//! them. Raw `WorkflowService` calls over the shared connection.

use crate::{
    ConnectionRes, atoms, bridge,
    client::{invalid, rpc_err, unit_reply},
    convert::{millis, n, opt_ms, retry_policy, search_attributes},
    ok, spawn_reply,
    wire::Wire,
};
use rustler::{Atom, Encoder, Env, LocalPid, ResourceArc, Term};
use std::time::Duration;
use temporalio_common::protos::temporal::api::{
    activity::v1::{ActivityExecutionInfo, ActivityExecutionListInfo, activity_execution_outcome},
    common::v1::{ActivityType, Header, Payloads},
    taskqueue::v1::TaskQueue,
    workflowservice::v1::{
        CountActivityExecutionsRequest, DescribeActivityExecutionRequest,
        DescribeActivityExecutionResponse, ListActivityExecutionsRequest,
        PollActivityExecutionRequest, RequestCancelActivityExecutionRequest,
        StartActivityExecutionRequest, TerminateActivityExecutionRequest,
    },
};
use tonic::{Code, IntoRequest};

/// `bridge.ActivityStartRequest` as the request the server takes. The namespace, identity
/// and request id are the connection's to add.
pub fn start_request(
    request: bridge::ActivityStartRequest,
) -> Result<StartActivityExecutionRequest, String> {
    let bridge::ActivityStartRequest {
        activity_id,
        activity_type,
        task_queue,
        input,
        schedule_to_close_ms,
        schedule_to_start_ms,
        start_to_close_ms,
        heartbeat_timeout_ms,
        retry,
        id_reuse_policy,
        id_conflict_policy,
        search_attributes: attributes,
        start_delay_ms,
        headers,
    } = request;
    let name = "ActivityStartRequest";
    Ok(StartActivityExecutionRequest {
        activity_id,
        activity_type: Some(ActivityType {
            name: activity_type,
        }),
        task_queue: Some(TaskQueue {
            name: task_queue,
            ..Default::default()
        }),
        schedule_to_close_timeout: opt_ms(
            schedule_to_close_ms,
            &format!("{name}.schedule_to_close_ms"),
        )?,
        schedule_to_start_timeout: opt_ms(
            schedule_to_start_ms,
            &format!("{name}.schedule_to_start_ms"),
        )?,
        start_to_close_timeout: opt_ms(start_to_close_ms, &format!("{name}.start_to_close_ms"))?,
        heartbeat_timeout: opt_ms(
            heartbeat_timeout_ms,
            &format!("{name}.heartbeat_timeout_ms"),
        )?,
        retry_policy: retry.map(retry_policy).transpose()?,
        input: Some(Payloads { payloads: input }),
        id_reuse_policy: n(id_reuse_policy, &format!("{name}.id_reuse_policy"))?,
        id_conflict_policy: n(id_conflict_policy, &format!("{name}.id_conflict_policy"))?,
        search_attributes: search_attributes(attributes),
        header: (!headers.is_empty()).then_some(Header { fields: headers }),
        start_delay: opt_ms(start_delay_ms, &format!("{name}.start_delay_ms"))?,
        ..Default::default()
    })
}

/// `bridge.ActivityExecution` from the server's description.
pub fn to_execution(response: DescribeActivityExecutionResponse) -> bridge::ActivityExecution {
    let info: ActivityExecutionInfo = response.info.unwrap_or_default();
    bridge::ActivityExecution {
        activity_id: info.activity_id,
        // The response names the run even when the info inside it does not.
        run_id: if info.run_id.is_empty() {
            response.run_id
        } else {
            info.run_id
        },
        activity_type: info.activity_type.map(|t| t.name).unwrap_or_default(),
        task_queue: info.task_queue,
        status: i64::from(info.status),
        run_state: i64::from(info.run_state),
        attempt: i64::from(info.attempt),
        schedule_time_ms: millis(info.schedule_time),
        close_time_ms: info.close_time.map(|t| millis(Some(t))),
        last_failure: info.last_failure,
        heartbeat_details: info.heartbeat_details.unwrap_or_default().payloads,
        last_worker_identity: info.last_worker_identity,
        search_attributes: info
            .search_attributes
            .map(|a| a.indexed_fields)
            .unwrap_or_default(),
    }
}

fn to_summary(info: ActivityExecutionListInfo) -> bridge::ActivitySummary {
    bridge::ActivitySummary {
        activity_id: info.activity_id,
        run_id: info.run_id,
        activity_type: info.activity_type.map(|t| t.name).unwrap_or_default(),
        task_queue: info.task_queue,
        status: i64::from(info.status),
        schedule_time_ms: millis(info.schedule_time),
        close_time_ms: info.close_time.map(|t| millis(Some(t))),
        search_attributes: info
            .search_attributes
            .map(|a| a.indexed_fields)
            .unwrap_or_default(),
    }
}

/// Start a standalone activity and answer its run id.
#[rustler::nif]
fn start_activity<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    request: Term<'a>,
) -> Term<'a> {
    let request = match bridge::ActivityStartRequest::from_term(request).and_then(start_request) {
        Ok(request) => request,
        Err(e) => return invalid(env, format!("invalid activity start request: {e}")),
    };
    let fut = async move {
        let request = StartActivityExecutionRequest {
            namespace: conn.namespace.clone(),
            identity: conn.identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            ..request
        };
        conn.connection
            .workflow_service()
            .start_activity_execution(request.into_request())
            .await
            .map(|r| r.into_inner().run_id)
    };
    spawn_reply(pid, reference, fut, |env, r| match r {
        Ok(run_id) => ok(env, run_id.encode(env)),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok().encode(env)
}

/// Wait for a standalone activity to close.
#[rustler::nif]
fn activity_result<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    activity_id: String,
    run_id: String,
) -> Atom {
    let fut = async move {
        loop {
            let mut request = PollActivityExecutionRequest {
                namespace: conn.namespace.clone(),
                activity_id: activity_id.clone(),
                run_id: run_id.clone(),
            }
            .into_request();
            request.set_timeout(Duration::from_secs(60));
            match conn
                .connection
                .workflow_service()
                .poll_activity_execution(request)
                .await
            {
                // The server's long poll ran out with the activity still open; ask again.
                Ok(r) => match r.into_inner().outcome.and_then(|o| o.value) {
                    Some(outcome) => return Ok(outcome),
                    None => continue,
                },
                Err(s) if matches!(s.code(), Code::DeadlineExceeded | Code::Cancelled) => continue,
                Err(s) => return Err(s),
            }
        }
    };
    spawn_reply(pid, reference, fut, |env, r| match r {
        Ok(activity_execution_outcome::Value::Result(result)) => ok(
            env,
            bridge::ActivityOutcome::ActivitySucceeded {
                result: result.payloads,
            }
            .to_term(env),
        ),
        Ok(activity_execution_outcome::Value::Failure(failure)) => ok(
            env,
            bridge::ActivityOutcome::ActivityEnded { failure }.to_term(env),
        ),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}

#[rustler::nif]
fn describe_activity<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    activity_id: String,
    run_id: String,
) -> Atom {
    let fut = async move {
        let request = DescribeActivityExecutionRequest {
            namespace: conn.namespace.clone(),
            activity_id,
            run_id,
            include_heartbeat_details: true,
            include_last_failure: true,
            ..Default::default()
        };
        conn.connection
            .workflow_service()
            .describe_activity_execution(request.into_request())
            .await
            .map(|r| r.into_inner())
    };
    spawn_reply(pid, reference, fut, |env, r| match r {
        Ok(response) => ok(env, to_execution(response).to_term(env)),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}

#[rustler::nif]
fn cancel_activity<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    activity_id: String,
    run_id: String,
    reason: String,
) -> Atom {
    let fut = async move {
        let request = RequestCancelActivityExecutionRequest {
            namespace: conn.namespace.clone(),
            activity_id,
            run_id,
            identity: conn.identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            reason,
        };
        conn.connection
            .workflow_service()
            .request_cancel_activity_execution(request.into_request())
            .await
            .map(|_| ())
    };
    spawn_reply(pid, reference, fut, unit_reply);
    atoms::ok()
}

#[rustler::nif]
fn terminate_activity<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    activity_id: String,
    run_id: String,
    reason: String,
) -> Atom {
    let fut = async move {
        let request = TerminateActivityExecutionRequest {
            namespace: conn.namespace.clone(),
            activity_id,
            run_id,
            identity: conn.identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            reason,
        };
        conn.connection
            .workflow_service()
            .terminate_activity_execution(request.into_request())
            .await
            .map(|_| ())
    };
    spawn_reply(pid, reference, fut, unit_reply);
    atoms::ok()
}

/// List up to `limit` standalone activities matching a visibility query.
#[rustler::nif]
fn list_activities<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    query: String,
    limit: usize,
) -> Atom {
    let fut = async move {
        let mut found = vec![];
        let mut next_page_token = vec![];
        loop {
            let request = ListActivityExecutionsRequest {
                namespace: conn.namespace.clone(),
                page_size: limit.saturating_sub(found.len()).min(1000) as i32,
                next_page_token,
                query: query.clone(),
            };
            let response = conn
                .connection
                .workflow_service()
                .list_activity_executions(request.into_request())
                .await?
                .into_inner();
            found.extend(response.executions);
            next_page_token = response.next_page_token;
            if next_page_token.is_empty() || found.len() >= limit {
                found.truncate(limit);
                return Ok::<_, tonic::Status>(found);
            }
        }
    };
    spawn_reply(pid, reference, fut, |env, r| match r {
        Ok(found) => {
            let found: Vec<bridge::ActivitySummary> = found.into_iter().map(to_summary).collect();
            ok(env, found.to_term(env))
        }
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}

#[rustler::nif]
fn count_activities<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    query: String,
) -> Atom {
    let fut = async move {
        let request = CountActivityExecutionsRequest {
            namespace: conn.namespace.clone(),
            query,
        };
        conn.connection
            .workflow_service()
            .count_activity_executions(request.into_request())
            .await
            .map(|r| r.into_inner().count)
    };
    spawn_reply(pid, reference, fut, |env, r| match r {
        Ok(count) => ok(env, count.encode(env)),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}
