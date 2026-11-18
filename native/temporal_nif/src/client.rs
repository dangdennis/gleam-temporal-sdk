//! Client operations: raw `WorkflowService` calls over the shared connection.

use crate::{ConnectionRes, atoms, convert::*, ok, spawn_reply};
use rustler::{Atom, Encoder, Env, LocalPid, ResourceArc, Term};
use std::time::Duration;
use temporalio_common::protos::temporal::api::{
    common::v1::{
        Memo, Payload, Payloads, RetryPolicy, SearchAttributes, WorkflowExecution, WorkflowType,
    },
    enums::v1::HistoryEventFilterType,
    failure::v1::Failure,
    history::v1::{History, history_event::Attributes},
    query::v1::WorkflowQuery,
    taskqueue::v1::TaskQueue,
    workflow::v1::WorkflowExecutionInfo,
    workflowservice::v1::{
        CountWorkflowExecutionsRequest, DescribeWorkflowExecutionRequest,
        GetWorkflowExecutionHistoryRequest, ListWorkflowExecutionsRequest, QueryWorkflowRequest,
        RequestCancelWorkflowExecutionRequest, SignalWithStartWorkflowExecutionRequest,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
        TerminateWorkflowExecutionRequest,
    },
};
use tonic::{Code, IntoRequest};

fn execution(workflow_id: String, run_id: String) -> Option<WorkflowExecution> {
    Some(WorkflowExecution {
        workflow_id,
        run_id,
    })
}

fn payloads_arg<'a>(env: Env<'a>, term: Term<'a>) -> Result<Vec<Payload>, Term<'a>> {
    decode_payloads(term).map_err(|e| invalid(env, format!("invalid payloads: {e}")))
}

fn unit_reply<'a>(env: Env<'a>, r: Result<(), tonic::Status>) -> Term<'a> {
    match r {
        Ok(()) => ok(env, atoms::nil().encode(env)),
        Err(s) => rpc_err(env, &s),
    }
}

/// A gRPC failure as `bridge.RpcError`.
pub fn rpc_err<'a>(env: Env<'a>, s: &tonic::Status) -> Term<'a> {
    (
        atoms::error(),
        rec(
            env,
            "rpc_error",
            &[(s.code() as i32).encode(env), s.message().encode(env)],
        ),
    )
        .encode(env)
}

/// A request rejected before it was sent, reported like the server's `InvalidArgument`.
pub fn invalid<'a>(env: Env<'a>, message: impl AsRef<str>) -> Term<'a> {
    rpc_err(env, &tonic::Status::invalid_argument(message.as_ref()))
}

/// Mirrors `bridge.StartRequest`.
pub struct StartRequest {
    pub workflow_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub input: Vec<Payload>,
    pub id_reuse_policy: i32,
    pub id_conflict_policy: i32,
    pub execution_timeout: Option<prost_wkt_types::Duration>,
    pub run_timeout: Option<prost_wkt_types::Duration>,
    pub task_timeout: Option<prost_wkt_types::Duration>,
    pub retry: Option<RetryPolicy>,
    pub memo: Option<Memo>,
    pub search_attributes: Option<SearchAttributes>,
    pub start_delay: Option<prost_wkt_types::Duration>,
    pub cron_schedule: String,
}

pub fn decode_start_request(term: Term) -> DecodeResult<StartRequest> {
    let (_, f) = untag(term)?;
    Ok(StartRequest {
        workflow_id: get(&f, 0)?,
        workflow_type: get(&f, 1)?,
        task_queue: get(&f, 2)?,
        input: decode_payloads(field(&f, 3)?)?,
        id_reuse_policy: get(&f, 4)?,
        id_conflict_policy: get(&f, 5)?,
        execution_timeout: decode_opt_ms(field(&f, 6)?)?,
        run_timeout: decode_opt_ms(field(&f, 7)?)?,
        task_timeout: decode_opt_ms(field(&f, 8)?)?,
        retry: opt(field(&f, 9)?).map(decode_retry).transpose()?,
        memo: decode_memo(field(&f, 10)?)?,
        search_attributes: decode_search_attributes(field(&f, 11)?)?,
        start_delay: decode_opt_ms(field(&f, 12)?)?,
        cron_schedule: get(&f, 13)?,
    })
}

/// Start a workflow, or, when `signal` is `{some, {Name, Input}}`, signal it and start it
/// first if it is not running.
#[rustler::nif]
fn start_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    request: Term<'a>,
    signal: Term<'a>,
) -> Term<'a> {
    let r = match decode_start_request(request) {
        Ok(r) => r,
        Err(e) => return invalid(env, format!("invalid start request: {e}")),
    };
    let signal: Option<(String, Vec<Payload>)> = match opt(signal) {
        None => None,
        Some(t) => match t
            .decode::<(String, Term)>()
            .map_err(|_| "bad signal".to_string())
            .and_then(|(name, input)| Ok((name, decode_payloads(input)?)))
        {
            Ok(s) => Some(s),
            Err(e) => return invalid(env, format!("invalid signal: {e}")),
        },
    };
    let fut = async move {
        let mut service = conn.connection.workflow_service();
        let workflow_type = Some(WorkflowType {
            name: r.workflow_type,
        });
        let task_queue = Some(TaskQueue {
            name: r.task_queue,
            ..Default::default()
        });
        let input = Some(Payloads { payloads: r.input });
        let request_id = uuid::Uuid::new_v4().to_string();
        match signal {
            None => {
                let request = StartWorkflowExecutionRequest {
                    namespace: conn.namespace.clone(),
                    workflow_id: r.workflow_id,
                    workflow_type,
                    task_queue,
                    input,
                    identity: conn.identity.clone(),
                    request_id,
                    workflow_id_reuse_policy: r.id_reuse_policy,
                    workflow_id_conflict_policy: r.id_conflict_policy,
                    workflow_execution_timeout: r.execution_timeout,
                    workflow_run_timeout: r.run_timeout,
                    workflow_task_timeout: r.task_timeout,
                    retry_policy: r.retry,
                    memo: r.memo,
                    search_attributes: r.search_attributes,
                    workflow_start_delay: r.start_delay,
                    cron_schedule: r.cron_schedule,
                    ..Default::default()
                };
                service
                    .start_workflow_execution(request.into_request())
                    .await
                    .map(|r| r.into_inner().run_id)
            }
            Some((signal_name, signal_input)) => {
                let request = SignalWithStartWorkflowExecutionRequest {
                    namespace: conn.namespace.clone(),
                    workflow_id: r.workflow_id,
                    workflow_type,
                    task_queue,
                    input,
                    identity: conn.identity.clone(),
                    request_id,
                    workflow_id_reuse_policy: r.id_reuse_policy,
                    workflow_id_conflict_policy: r.id_conflict_policy,
                    workflow_execution_timeout: r.execution_timeout,
                    workflow_run_timeout: r.run_timeout,
                    workflow_task_timeout: r.task_timeout,
                    retry_policy: r.retry,
                    memo: r.memo,
                    search_attributes: r.search_attributes,
                    workflow_start_delay: r.start_delay,
                    cron_schedule: r.cron_schedule,
                    signal_name,
                    signal_input: Some(Payloads {
                        payloads: signal_input,
                    }),
                    ..Default::default()
                };
                service
                    .signal_with_start_workflow_execution(request.into_request())
                    .await
                    .map(|r| r.into_inner().run_id)
            }
        }
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(run_id) => ok(env, run_id.encode(env)),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok().encode(env)
}

/// `bridge.Execution` from the server's description of a run.
fn encode_execution<'a>(env: Env<'a>, info: WorkflowExecutionInfo) -> Term<'a> {
    let execution = info.execution.unwrap_or_default();
    let close = info.close_time.map(|t| millis(Some(t)).encode(env));
    rec(
        env,
        "execution",
        &[
            execution.workflow_id.encode(env),
            execution.run_id.encode(env),
            info.r#type.map(|t| t.name).unwrap_or_default().encode(env),
            info.task_queue.encode(env),
            info.status.encode(env),
            millis(info.start_time).encode(env),
            option(env, close),
            info.history_length.encode(env),
            encode_payload_map(env, &info.memo.map(|m| m.fields).unwrap_or_default()),
            encode_payload_map(
                env,
                &info
                    .search_attributes
                    .map(|a| a.indexed_fields)
                    .unwrap_or_default(),
            ),
            option(
                env,
                info.parent_execution.map(|p| p.workflow_id.encode(env)),
            ),
        ],
    )
}

#[rustler::nif]
fn describe_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    workflow_id: String,
    run_id: String,
) -> Atom {
    let fut = async move {
        let request = DescribeWorkflowExecutionRequest {
            namespace: conn.namespace.clone(),
            execution: execution(workflow_id, run_id),
        };
        conn.connection
            .workflow_service()
            .describe_workflow_execution(request.into_request())
            .await
            .map(|r| r.into_inner().workflow_execution_info.unwrap_or_default())
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(info) => ok(env, encode_execution(env, info)),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}

/// List up to `limit` executions matching a visibility query.
#[rustler::nif]
fn list_workflows<'a>(
    env: Env<'a>,
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
            let request = ListWorkflowExecutionsRequest {
                namespace: conn.namespace.clone(),
                page_size: limit.saturating_sub(found.len()).min(1000) as i32,
                next_page_token,
                query: query.clone(),
            };
            let response = conn
                .connection
                .workflow_service()
                .list_workflow_executions(request.into_request())
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
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(found) => {
            let terms: Vec<Term> = found
                .into_iter()
                .map(|info| encode_execution(env, info))
                .collect();
            ok(env, terms.encode(env))
        }
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}

#[rustler::nif]
fn count_workflows<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    query: String,
) -> Atom {
    let fut = async move {
        let request = CountWorkflowExecutionsRequest {
            namespace: conn.namespace.clone(),
            query,
        };
        conn.connection
            .workflow_service()
            .count_workflow_executions(request.into_request())
            .await
            .map(|r| r.into_inner().count)
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(count) => ok(env, count.encode(env)),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}

#[rustler::nif]
fn signal_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    workflow_id: String,
    run_id: String,
    signal_name: String,
    input: Term<'a>,
) -> Term<'a> {
    let input = match payloads_arg(env, input) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let fut = async move {
        let request = SignalWorkflowExecutionRequest {
            namespace: conn.namespace.clone(),
            workflow_execution: execution(workflow_id, run_id),
            signal_name,
            input: Some(Payloads { payloads: input }),
            identity: conn.identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        };
        conn.connection
            .workflow_service()
            .signal_workflow_execution(request.into_request())
            .await
            .map(|_| ())
    };
    spawn_reply(env, pid, reference, fut, unit_reply);
    atoms::ok().encode(env)
}

#[rustler::nif]
fn query_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    workflow_id: String,
    run_id: String,
    query_type: String,
    args: Term<'a>,
) -> Term<'a> {
    let args = match payloads_arg(env, args) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let fut = async move {
        let request = QueryWorkflowRequest {
            namespace: conn.namespace.clone(),
            execution: execution(workflow_id, run_id),
            query: Some(WorkflowQuery {
                query_type,
                query_args: Some(Payloads { payloads: args }),
                ..Default::default()
            }),
            ..Default::default()
        };
        conn.connection
            .workflow_service()
            .query_workflow(request.into_request())
            .await
            .map(|r| r.into_inner().query_result.unwrap_or_default().payloads)
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(payloads) => {
            let terms: Vec<Term> = payloads.iter().map(|p| encode_payload(env, p)).collect();
            ok(env, terms.encode(env))
        }
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok().encode(env)
}

#[rustler::nif]
fn cancel_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    workflow_id: String,
    run_id: String,
) -> Atom {
    let fut = async move {
        let request = RequestCancelWorkflowExecutionRequest {
            namespace: conn.namespace.clone(),
            workflow_execution: execution(workflow_id, run_id),
            identity: conn.identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        };
        conn.connection
            .workflow_service()
            .request_cancel_workflow_execution(request.into_request())
            .await
            .map(|_| ())
    };
    spawn_reply(env, pid, reference, fut, unit_reply);
    atoms::ok()
}

#[rustler::nif]
fn terminate_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    workflow_id: String,
    run_id: String,
    reason: String,
) -> Atom {
    let fut = async move {
        let request = TerminateWorkflowExecutionRequest {
            namespace: conn.namespace.clone(),
            workflow_execution: execution(workflow_id, run_id),
            reason,
            identity: conn.identity.clone(),
            ..Default::default()
        };
        conn.connection
            .workflow_service()
            .terminate_workflow_execution(request.into_request())
            .await
            .map(|_| ())
    };
    spawn_reply(env, pid, reference, fut, unit_reply);
    atoms::ok()
}

/// How one run ended. `next_run_id` is set when the server started a successor run (continue as
/// new, retry or cron), which the caller follows to reach the final result.
enum Outcome {
    Completed(Vec<Payload>),
    Failed(Box<Failure>),
    Cancelled,
    Terminated,
    TimedOut,
    Continued,
}

async fn close_event(
    conn: &ConnectionRes,
    workflow_id: String,
    run_id: String,
) -> Result<(Outcome, String), tonic::Status> {
    let mut page_token = vec![];
    loop {
        let request = GetWorkflowExecutionHistoryRequest {
            namespace: conn.namespace.clone(),
            execution: execution(workflow_id.clone(), run_id.clone()),
            next_page_token: page_token.clone(),
            wait_new_event: true,
            history_event_filter_type: HistoryEventFilterType::CloseEvent as i32,
            skip_archival: true,
            ..Default::default()
        };
        let mut request = request.into_request();
        request.set_timeout(Duration::from_secs(60));
        let response = match conn
            .connection
            .workflow_service()
            .get_workflow_execution_history(request)
            .await
        {
            Ok(r) => r.into_inner(),
            // The long poll expired with the workflow still running; ask again.
            Err(s) if matches!(s.code(), Code::DeadlineExceeded | Code::Cancelled) => continue,
            Err(s) => return Err(s),
        };
        let event = response
            .history
            .and_then(|h| h.events.into_iter().next())
            .and_then(|e| e.attributes);
        let Some(attributes) = event else {
            page_token = response.next_page_token;
            continue;
        };
        return Ok(match attributes {
            Attributes::WorkflowExecutionCompletedEventAttributes(a) => (
                Outcome::Completed(a.result.unwrap_or_default().payloads),
                a.new_execution_run_id,
            ),
            Attributes::WorkflowExecutionFailedEventAttributes(a) => (
                Outcome::Failed(Box::new(a.failure.unwrap_or_default())),
                a.new_execution_run_id,
            ),
            Attributes::WorkflowExecutionCanceledEventAttributes(_) => {
                (Outcome::Cancelled, String::new())
            }
            Attributes::WorkflowExecutionTerminatedEventAttributes(_) => {
                (Outcome::Terminated, String::new())
            }
            Attributes::WorkflowExecutionTimedOutEventAttributes(a) => {
                (Outcome::TimedOut, a.new_execution_run_id)
            }
            Attributes::WorkflowExecutionContinuedAsNewEventAttributes(a) => {
                (Outcome::Continued, a.new_execution_run_id)
            }
            other => {
                return Err(tonic::Status::internal(format!(
                    "unexpected close event: {other:?}"
                )));
            }
        });
    }
}

/// Wait for the workflow chain starting at `run_id` to finish, following successor runs.
#[rustler::nif]
fn workflow_result<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    workflow_id: String,
    run_id: String,
) -> Atom {
    let fut = async move {
        let mut run_id = run_id;
        loop {
            let (outcome, next) = close_event(&conn, workflow_id.clone(), run_id).await?;
            if next.is_empty() {
                return Ok::<_, tonic::Status>(outcome);
            }
            run_id = next;
        }
    };
    spawn_reply(env, pid, reference, fut, |env, r| {
        let tag = |name: &str| Atom::from_str(env, name).unwrap();
        match r {
            Ok(Outcome::Completed(payloads)) => {
                let terms: Vec<Term> = payloads.iter().map(|p| encode_payload(env, p)).collect();
                ok(env, (tag("outcome_completed"), terms).encode(env))
            }
            Ok(Outcome::Failed(f)) => ok(
                env,
                (tag("outcome_failed"), encode_failure(env, &f)).encode(env),
            ),
            Ok(Outcome::Cancelled) => ok(env, tag("outcome_cancelled").encode(env)),
            Ok(Outcome::Terminated) => ok(env, tag("outcome_terminated").encode(env)),
            Ok(Outcome::TimedOut) => ok(env, tag("outcome_timed_out").encode(env)),
            Ok(Outcome::Continued) => invalid(env, "continued run has no successor run id"),
            Err(s) => rpc_err(env, &s),
        }
    });
    atoms::ok()
}

/// Fetch a run's complete history as an encoded `temporal.api.history.v1.History`.
#[rustler::nif]
fn fetch_history<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    workflow_id: String,
    run_id: String,
) -> Atom {
    let fut = async move {
        let mut events = vec![];
        let mut page_token = vec![];
        loop {
            let request = GetWorkflowExecutionHistoryRequest {
                namespace: conn.namespace.clone(),
                execution: execution(workflow_id.clone(), run_id.clone()),
                next_page_token: page_token,
                ..Default::default()
            };
            let response = conn
                .connection
                .workflow_service()
                .get_workflow_execution_history(request.into_request())
                .await?
                .into_inner();
            events.extend(response.history.unwrap_or_default().events);
            page_token = response.next_page_token;
            if page_token.is_empty() {
                return Ok::<_, tonic::Status>(prost::Message::encode_to_vec(&History { events }));
            }
        }
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(bytes) => ok(env, bin(env, &bytes)),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok()
}
