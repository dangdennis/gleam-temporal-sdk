//! Erlang NIF bridging Gleam to Temporal's `sdk-core`.
//!
//! Every blocking operation is spawned on core's Tokio runtime and answers the calling process
//! with `{temporal_nif, Ref, Result}`; `temporal_nif.erl` turns that back into a blocking call.
//! Terms are encoded directly in the shape of the Gleam types in `temporal/internal/bridge`.

// NIF arguments are positional and mirror the Erlang call, so they cannot be grouped.
#![allow(clippy::too_many_arguments)]

mod client;
mod convert;
mod schedule;
mod testing;

use convert::*;
use futures_util::stream;
use prost::Message;
use rustler::{Atom, Binary, Encoder, Env, LocalPid, OwnedEnv, ResourceArc, Term};
use std::{
    str::FromStr,
    sync::{Arc, OnceLock},
    time::Duration,
};
use temporalio_client::{ClientTlsOptions, Connection, ConnectionOptions, TlsOptions};
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

pub fn runtime() -> &'static CoreRuntime {
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
    tls: Term<'a>,
) -> Term<'a> {
    let api_key: Option<String> = opt(api_key).and_then(|t| t.decode().ok());
    let tls = match opt(tls).map(decode_tls).transpose() {
        Ok(tls) => tls,
        Err(e) => return err(env, format!("invalid TLS options: {e}")),
    };
    let fut = async move {
        let url = Url::from_str(&target).map_err(|e| format!("invalid target url: {e}"))?;
        let options = ConnectionOptions::new(url)
            .identity(identity.clone())
            .maybe_api_key(api_key)
            .maybe_tls_options(tls)
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
    atoms::ok().encode(env)
}

/// `bridge.Tls`: an optional root CA, an optional server name and an optional client
/// certificate and key, all PEM.
fn decode_tls(term: Term) -> DecodeResult<TlsOptions> {
    let (_, f) = untag(term)?;
    let bytes = |t: Term| -> DecodeResult<Vec<u8>> {
        let b: Binary = t.decode().map_err(|_| "expected a BitArray".to_string())?;
        Ok(b.as_slice().to_vec())
    };
    let root = opt(field(&f, 0)?).map(bytes).transpose()?;
    let domain: Option<String> = opt(field(&f, 1)?)
        .map(|t| t.decode().map_err(|_| "domain must be a String".to_string()))
        .transpose()?;
    let client = opt(field(&f, 2)?)
        .map(|t| {
            let (cert, key): (Term, Term) =
                t.decode().map_err(|_| "bad client certificate".to_string())?;
            Ok::<_, String>(
                ClientTlsOptions::builder()
                    .client_cert(bytes(cert)?)
                    .client_private_key(bytes(key)?)
                    .build(),
            )
        })
        .transpose()?;
    Ok(TlsOptions::builder()
        .maybe_server_root_ca_cert(root)
        .maybe_domain(domain)
        .maybe_client_tls_options(client)
        .build())
}

/// Mirrors `bridge.WorkerOptions`.
struct WorkerOptions {
    max_cached_workflows: usize,
    max_workflow_tasks: usize,
    max_activities: usize,
    task_queue_activities_per_second: Option<f64>,
    worker_activities_per_second: Option<f64>,
    task_types: WorkerTaskTypes,
}

fn decode_worker_options(term: Term) -> DecodeResult<WorkerOptions> {
    let (_, f) = untag(term)?;
    let rate = |i: usize| -> DecodeResult<Option<f64>> {
        opt(field(&f, i)?)
            .map(|t| t.decode().map_err(|_| "rate must be a Float".to_string()))
            .transpose()
    };
    Ok(WorkerOptions {
        max_cached_workflows: get(&f, 0)?,
        max_workflow_tasks: get(&f, 1)?,
        max_activities: get(&f, 2)?,
        task_queue_activities_per_second: rate(3)?,
        worker_activities_per_second: rate(4)?,
        task_types: WorkerTaskTypes {
            enable_workflows: get(&f, 5)?,
            enable_local_activities: false,
            enable_remote_activities: get(&f, 6)?,
            enable_nexus: false,
        },
    })
}

fn worker_config(
    namespace: String,
    task_queue: String,
    options: WorkerOptions,
) -> Result<WorkerConfig, String> {
    WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .max_cached_workflows(options.max_cached_workflows)
        .task_types(options.task_types)
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(2))
        .max_outstanding_workflow_tasks(options.max_workflow_tasks)
        .max_outstanding_activities(options.max_activities)
        .maybe_max_task_queue_activities_per_second(options.task_queue_activities_per_second)
        .maybe_max_worker_activities_per_second(options.worker_activities_per_second)
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
    options: Term<'a>,
) -> Term<'a> {
    let options = match decode_worker_options(options) {
        Ok(o) => o,
        Err(e) => return err(env, format!("invalid worker options: {e}")),
    };
    let fut = async move {
        let config = worker_config(conn.namespace.clone(), task_queue, options)?;
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
    atoms::ok().encode(env)
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
    let options = WorkerOptions {
        max_cached_workflows: 1,
        max_workflow_tasks: 100,
        max_activities: 100,
        task_queue_activities_per_second: None,
        worker_activities_per_second: None,
        task_types: WorkerTaskTypes::workflow_only(),
    };
    let config = match worker_config("replay".to_string(), task_queue, options) {
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
