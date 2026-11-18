//! Client operations: raw `WorkflowService` calls over the shared connection.

use crate::{ConnectionRes, atoms, convert::*, err, ok, spawn_reply};
use rustler::{Atom, Encoder, Env, LocalPid, ResourceArc, Term};
use std::time::Duration;
use temporalio_common::protos::temporal::api::{
    common::v1::{Payload, Payloads, WorkflowExecution, WorkflowType},
    enums::v1::HistoryEventFilterType,
    failure::v1::Failure,
    history::v1::{History, history_event::Attributes},
    query::v1::WorkflowQuery,
    taskqueue::v1::TaskQueue,
    workflowservice::v1::{
        GetWorkflowExecutionHistoryRequest, QueryWorkflowRequest,
        RequestCancelWorkflowExecutionRequest, SignalWorkflowExecutionRequest,
        StartWorkflowExecutionRequest, TerminateWorkflowExecutionRequest,
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
    decode_payloads(term).map_err(|e| err(env, format!("invalid payloads: {e}")))
}

fn unit_reply<'a>(env: Env<'a>, r: Result<(), tonic::Status>) -> Term<'a> {
    match r {
        Ok(()) => ok(env, atoms::nil().encode(env)),
        Err(s) => err(env, s.message()),
    }
}

#[rustler::nif]
fn start_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    input: Term<'a>,
) -> Term<'a> {
    let input = match payloads_arg(env, input) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let fut = async move {
        let request = StartWorkflowExecutionRequest {
            namespace: conn.namespace.clone(),
            workflow_id,
            workflow_type: Some(WorkflowType {
                name: workflow_type,
            }),
            task_queue: Some(TaskQueue {
                name: task_queue,
                ..Default::default()
            }),
            input: Some(Payloads { payloads: input }),
            identity: conn.identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        };
        conn.connection
            .workflow_service()
            .start_workflow_execution(request.into_request())
            .await
            .map(|r| r.into_inner().run_id)
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(run_id) => ok(env, run_id.encode(env)),
        Err(s) if s.code() == Code::AlreadyExists => (
            atoms::error(),
            Atom::from_str(env, "already_started").unwrap(),
        )
            .encode(env),
        Err(s) => {
            let failed = Atom::from_str(env, "start_failed").unwrap();
            (atoms::error(), (failed, s.message())).encode(env)
        }
    });
    atoms::ok().encode(env)
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
        Err(s) => err(env, s.message()),
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
            Ok(Outcome::Continued) => err(env, "continued run has no successor run id"),
            Err(s) => err(env, s.message()),
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
        Err(s) => err(env, s.message()),
    });
    atoms::ok()
}
