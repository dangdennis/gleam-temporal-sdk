//! Erlang NIF bridging Gleam to Temporal's `sdk-core`.
//!
//! Every blocking operation is spawned on core's Tokio runtime and answers the calling process
//! with `{temporal_nif, Ref, Result}`; `temporal_nif.erl` turns that back into a blocking call.
//! Terms are encoded directly in the shape of the Gleam types in `temporal/internal/bridge`.

// NIF arguments are positional and mirror the Erlang call, so they cannot be grouped.
#![allow(clippy::too_many_arguments)]

mod client;
mod convert;

use convert::*;
use futures_util::stream;
use prost::Message;
use rustler::{Atom, Binary, Encoder, Env, LocalPid, OwnedEnv, ResourceArc, Term};
use std::{
    str::FromStr,
    sync::{Arc, OnceLock},
    time::Duration,
};
use temporalio_client::{Connection, ConnectionOptions};
use temporalio_common::{
    protos::{
        coresdk::{ActivityHeartbeat, ActivityTaskCompletion},
        temporal::api::history::v1::History,
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk_core::{
    CoreRuntime, PollError, PollerBehavior, RuntimeOptions, TokioRuntimeBuilder, Url, Worker,
    WorkerConfig, WorkerVersioningStrategy,
    replay::{HistoryForReplay, ReplayWorkerInput},
};

mod atoms {
    rustler::atoms! { ok, error, nil, temporal_nif, shutdown, some, none }
}

pub struct ConnectionRes {
    pub connection: Connection,
    pub namespace: String,
    pub identity: String,
}

pub struct WorkerRes {
    pub worker: Arc<Worker>,
}

#[rustler::resource_impl]
impl rustler::Resource for ConnectionRes {}
#[rustler::resource_impl]
impl rustler::Resource for WorkerRes {}

fn runtime() -> &'static CoreRuntime {
    static RT: OnceLock<CoreRuntime> = OnceLock::new();
    RT.get_or_init(|| {
        CoreRuntime::new(RuntimeOptions::default(), TokioRuntimeBuilder::default())
            .expect("failed to start the Temporal core runtime")
    })
}

/// Run `fut` on the core runtime, then send `{temporal_nif, Ref, reply}` to `pid`. `reply`
/// builds the term on a Tokio thread, which is the only place an `OwnedEnv` may send from.
pub fn spawn_reply<F, T, R>(env: Env, pid: LocalPid, reference: Term, fut: F, reply: R)
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
    R: for<'a> FnOnce(Env<'a>, T) -> Term<'a> + Send + 'static,
{
    let _ = env;
    let mut owned = OwnedEnv::new();
    let saved = owned.save(reference);
    runtime().tokio_handle().spawn(async move {
        let out = fut.await;
        let _ = owned.send_and_clear(&pid, |env| {
            (atoms::temporal_nif(), saved.load(env), reply(env, out)).encode(env)
        });
    });
}

pub fn ok<'a>(env: Env<'a>, t: Term<'a>) -> Term<'a> {
    (atoms::ok(), t).encode(env)
}

pub fn err<'a>(env: Env<'a>, msg: impl AsRef<str>) -> Term<'a> {
    (atoms::error(), msg.as_ref()).encode(env)
}

fn unit_result<'a, E: std::fmt::Display>(env: Env<'a>, r: Result<(), E>) -> Term<'a> {
    match r {
        Ok(()) => ok(env, atoms::nil().encode(env)),
        Err(e) => err(env, e.to_string()),
    }
}

#[rustler::nif]
fn connect<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    target: String,
    namespace: String,
    identity: String,
    api_key: Term<'a>,
) -> Atom {
    let api_key: Option<String> = opt(api_key).and_then(|t| t.decode().ok());
    let fut = async move {
        let url = Url::from_str(&target).map_err(|e| format!("invalid target url: {e}"))?;
        let options = ConnectionOptions::new(url)
            .identity(identity.clone())
            .maybe_api_key(api_key)
            .build();
        let connection = Connection::connect(options)
            .await
            .map_err(|e| format!("connection failed: {e}"))?;
        Ok::<_, String>(ConnectionRes {
            connection,
            namespace,
            identity,
        })
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(res) => ok(env, ResourceArc::new(res).encode(env)),
        Err(e) => err(env, e),
    });
    atoms::ok()
}

fn worker_config(
    namespace: String,
    task_queue: String,
    max_cached_workflows: usize,
    task_types: WorkerTaskTypes,
) -> Result<WorkerConfig, String> {
    WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .max_cached_workflows(max_cached_workflows)
        .task_types(task_types)
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
        .max_outstanding_workflow_tasks(100usize)
        .max_outstanding_activities(100usize)
        .max_outstanding_local_activities(100usize)
        .max_outstanding_nexus_tasks(1usize)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: "temporal-gleam".to_string(),
        })
        .build()
}

#[rustler::nif]
fn worker_new<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    task_queue: String,
    max_cached_workflows: usize,
) -> Atom {
    let fut = async move {
        let task_types = WorkerTaskTypes {
            enable_workflows: true,
            enable_local_activities: false,
            enable_remote_activities: true,
            enable_nexus: false,
        };
        let config = worker_config(
            conn.namespace.clone(),
            task_queue,
            max_cached_workflows,
            task_types,
        )?;
        let worker = temporalio_sdk_core::init_worker(runtime(), config, conn.connection.clone())
            .map_err(|e| format!("worker init failed: {e}"))?;
        worker
            .validate()
            .await
            .map_err(|e| format!("worker validation failed: {e}"))?;
        Ok::<_, String>(WorkerRes {
            worker: Arc::new(worker),
        })
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(res) => ok(env, ResourceArc::new(res).encode(env)),
        Err(e) => err(env, e),
    });
    atoms::ok()
}

/// A worker that replays the given `(workflow_id, encoded History)` pairs and then shuts down.
#[rustler::nif]
fn replay_worker_new<'a>(
    env: Env<'a>,
    task_queue: String,
    histories: Vec<(String, Binary<'a>)>,
) -> Term<'a> {
    let mut decoded = Vec::new();
    for (workflow_id, bytes) in histories {
        match History::decode(bytes.as_slice()) {
            Ok(history) => decoded.push(HistoryForReplay::new(history, workflow_id)),
            Err(e) => return err(env, format!("invalid history: {e}")),
        }
    }
    let config = match worker_config(
        "replay".to_string(),
        task_queue,
        1,
        WorkerTaskTypes::workflow_only(),
    ) {
        Ok(c) => c,
        Err(e) => return err(env, e),
    };
    let _guard = runtime().tokio_handle().enter();
    match temporalio_sdk_core::init_replay_worker(ReplayWorkerInput::new(
        config,
        stream::iter(decoded),
    )) {
        Ok(worker) => ok(
            env,
            ResourceArc::new(WorkerRes {
                worker: Arc::new(worker),
            })
            .encode(env),
        ),
        Err(e) => err(env, format!("replay worker init failed: {e}")),
    }
}

fn poll_error<'a>(env: Env<'a>, e: PollError) -> Term<'a> {
    match e {
        PollError::ShutDown => (atoms::error(), atoms::shutdown()).encode(env),
        other => (atoms::error(), (atoms::error(), other.to_string())).encode(env),
    }
}

#[rustler::nif]
fn worker_poll_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    w: ResourceArc<WorkerRes>,
) -> Atom {
    let fut = async move { w.worker.poll_workflow_activation().await };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(activation) => ok(env, encode_activation(env, activation)),
        Err(e) => poll_error(env, e),
    });
    atoms::ok()
}

#[rustler::nif]
fn worker_complete_workflow<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    w: ResourceArc<WorkerRes>,
    completion: Term<'a>,
) -> Term<'a> {
    let completion = match decode_completion(completion) {
        Ok(c) => c,
        Err(e) => return err(env, format!("invalid completion: {e}")),
    };
    let fut = async move { w.worker.complete_workflow_activation(completion).await };
    spawn_reply(env, pid, reference, fut, unit_result);
    atoms::ok().encode(env)
}

#[rustler::nif]
fn worker_poll_activity<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    w: ResourceArc<WorkerRes>,
) -> Atom {
    let fut = async move { w.worker.poll_activity_task().await };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(task) => ok(env, encode_activity_task(env, task)),
        Err(e) => poll_error(env, e),
    });
    atoms::ok()
}

#[rustler::nif]
fn worker_complete_activity<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    w: ResourceArc<WorkerRes>,
    task_token: Binary<'a>,
    result: Term<'a>,
) -> Term<'a> {
    let result = match decode_activity_result(result) {
        Ok(r) => r,
        Err(e) => return err(env, format!("invalid activity result: {e}")),
    };
    let completion = ActivityTaskCompletion {
        task_token: task_token.as_slice().to_vec(),
        result: Some(result),
    };
    let fut = async move { w.worker.complete_activity_task(completion).await };
    spawn_reply(env, pid, reference, fut, unit_result);
    atoms::ok().encode(env)
}

#[rustler::nif]
fn worker_heartbeat<'a>(
    w: ResourceArc<WorkerRes>,
    task_token: Binary<'a>,
    details: Term<'a>,
) -> Atom {
    if let Ok(details) = decode_payloads(details) {
        let _guard = runtime().tokio_handle().enter();
        w.worker.record_activity_heartbeat(ActivityHeartbeat {
            task_token: task_token.as_slice().to_vec(),
            details,
        });
    }
    atoms::nil()
}

/// Stop polling. Outstanding polls resolve with `{error, shutdown}` once in-flight tasks finish.
#[rustler::nif]
fn worker_shutdown<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    w: ResourceArc<WorkerRes>,
) -> Atom {
    let fut = async move {
        w.worker.initiate_shutdown();
        // Bounded so a workflow that never answers its eviction cannot hang the caller.
        let _ = tokio::time::timeout(Duration::from_secs(10), w.worker.shutdown()).await;
    };
    spawn_reply(env, pid, reference, fut, |env, ()| atoms::nil().encode(env));
    atoms::ok()
}

rustler::init!("temporal_nif");
