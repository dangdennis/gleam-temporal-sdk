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
mod selftest;
mod testing;

use convert::*;
use futures_util::stream;
use prost::Message;
use rustler::{Atom, Binary, Encoder, Env, LocalPid, OwnedEnv, ResourceArc, Term};
use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use temporalio_client::{ClientTlsOptions, Connection, ConnectionOptions, TlsOptions};
use temporalio_common::telemetry::{PrometheusExporterOptions, start_prometheus_metric_exporter};
use temporalio_common::{
    protos::{
        coresdk::{
            ActivityHeartbeat, ActivityTaskCompletion,
            activity_result::{self, ActivityExecutionResult, activity_execution_result},
            activity_task::activity_task,
            nexus::{NexusTask, NexusTaskCompletion, nexus_task, nexus_task_completion},
            workflow_activation::workflow_activation_job::Variant as Job,
            workflow_completion::{
                self, WorkflowActivationCompletion, workflow_activation_completion::Status,
            },
        },
        temporal::api::{failure::v1::Failure, history::v1::History},
    },
    worker::{
        VersioningBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes,
    },
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
    /// The namespace commands that name another workflow are addressed to.
    pub namespace: String,
}

#[rustler::resource_impl]
impl rustler::Resource for ConnectionRes {}
#[rustler::resource_impl]
impl rustler::Resource for WorkerRes {
    const IMPLEMENTS_DOWN: bool = true;

    /// The process that owns the worker died without stopping it. Shut the worker down, so
    /// that its polls end and a replacement can register on the same task queue.
    fn down<'a>(&'a self, _env: Env<'a>, _pid: LocalPid, _monitor: rustler::Monitor) {
        let worker = self.worker.clone();
        runtime().tokio_handle().spawn(async move {
            worker.initiate_shutdown();
            let _ = tokio::time::timeout(Duration::from_secs(10), worker.shutdown()).await;
        });
    }
}

/// Where to serve Prometheus metrics, if asked before the runtime started.
static METRICS_ADDRESS: Mutex<Option<SocketAddr>> = Mutex::new(None);
/// Where they are being served.
static METRICS_BOUND: OnceLock<Result<SocketAddr, String>> = OnceLock::new();
static RUNTIME: OnceLock<CoreRuntime> = OnceLock::new();

pub fn runtime() -> &'static CoreRuntime {
    RUNTIME.get_or_init(|| {
        let mut runtime =
            CoreRuntime::new(RuntimeOptions::default(), TokioRuntimeBuilder::default())
                .expect("failed to start the Temporal core runtime");
        if let Some(socket_addr) = METRICS_ADDRESS.lock().ok().and_then(|a| *a) {
            let started = {
                let _guard = runtime.tokio_handle().enter();
                start_prometheus_metric_exporter(
                    PrometheusExporterOptions::builder()
                        .socket_addr(socket_addr)
                        .build(),
                )
            };
            let bound = match started {
                Ok(server) => {
                    runtime
                        .telemetry_mut()
                        .attach_late_init_metrics(server.meter);
                    Ok(server.bound_addr)
                }
                Err(e) => Err(e.to_string()),
            };
            let _ = METRICS_BOUND.set(bound);
        }
        runtime
    })
}

/// Serve the worker's and client's metrics in Prometheus format on `address`. Has to be
/// called before anything else here, because the runtime reads it once, when it starts.
/// Answers the address being served, which differs from the one asked for when that named
/// port 0.
#[rustler::nif]
fn start_metrics<'a>(env: Env<'a>, address: String) -> Term<'a> {
    if RUNTIME.get().is_some() {
        return err(
            env,
            "metrics must be configured before the first connection or worker is made",
        );
    }
    let address: SocketAddr = match address.parse() {
        Ok(a) => a,
        Err(e) => return err(env, format!("invalid address: {e}")),
    };
    if let Ok(mut wanted) = METRICS_ADDRESS.lock() {
        *wanted = Some(address);
    }
    runtime();
    match METRICS_BOUND.get() {
        Some(Ok(bound)) => ok(env, Encoder::encode(&bound.to_string(), env)),
        Some(Err(e)) => err(env, e),
        None => err(env, "metrics could not be started"),
    }
}

/// Run `fut` on the core runtime, then send `{temporal_nif, Ref, reply}` to `pid`. `reply`
/// builds the term on a Tokio thread, which is the only place an `OwnedEnv` may send from.
pub fn spawn_reply<F, T, R>(pid: LocalPid, reference: Term, fut: F, reply: R)
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
    R: for<'a> FnOnce(Env<'a>, T) -> Term<'a> + Send + 'static,
{
    spawn_reply_or(pid, reference, fut, reply, |_| (), |()| async {});
}

/// Like `spawn_reply`, for results that must not be lost. If `pid` has died by the time the
/// result is ready, `undelivered` runs on what `salvage` kept of it. A polling loop that
/// crashes leaves its poll running, so without this the next task would vanish and the
/// server would wait out its timeout.
pub fn spawn_reply_or<F, T, R, S, K, U, UF>(
    pid: LocalPid,
    reference: Term,
    fut: F,
    reply: R,
    salvage: S,
    undelivered: U,
) where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
    R: for<'a> FnOnce(Env<'a>, T) -> Term<'a> + Send + 'static,
    S: FnOnce(&T) -> K + Send + 'static,
    K: Send + 'static,
    U: FnOnce(K) -> UF + Send + 'static,
    UF: Future<Output = ()> + Send + 'static,
{
    let mut owned = OwnedEnv::new();
    let saved = owned.save(reference);
    runtime().tokio_handle().spawn(async move {
        let out = fut.await;
        let kept = salvage(&out);
        let sent = owned.send_and_clear(&pid, |env| {
            (atoms::temporal_nif(), saved.load(env), reply(env, out)).encode(env)
        });
        if sent.is_err() {
            undelivered(kept).await;
        }
    });
}

fn lost_task_failure() -> Failure {
    Failure {
        message: "the worker process that asked for this task died before receiving it".to_string(),
        source: "GleamSDK".to_string(),
        ..Default::default()
    }
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
    spawn_reply(pid, reference, fut, |env, r| match r {
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
        .map(|t| {
            t.decode()
                .map_err(|_| "domain must be a String".to_string())
        })
        .transpose()?;
    let client = opt(field(&f, 2)?)
        .map(|t| {
            let (cert, key): (Term, Term) = t
                .decode()
                .map_err(|_| "bad client certificate".to_string())?;
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
    versioning: WorkerVersioningStrategy,
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
            // Local activities run in the worker that runs the workflow.
            enable_local_activities: get(&f, 5)?,
            enable_remote_activities: get(&f, 6)?,
            enable_nexus: get(&f, 7)?,
        },
        versioning: match opt(field(&f, 8)?) {
            None => unversioned(),
            Some(deployment) => {
                let (_, d) = untag(deployment)?;
                let behavior = match get::<i32>(&d, 3)? {
                    1 => Some(VersioningBehavior::Pinned),
                    2 => Some(VersioningBehavior::AutoUpgrade),
                    _ => None,
                };
                WorkerVersioningStrategy::WorkerDeploymentBased(
                    WorkerDeploymentOptions::new(
                        WorkerDeploymentVersion::builder()
                            .deployment_name(get::<String>(&d, 0)?)
                            .build_id(get::<String>(&d, 1)?)
                            .build(),
                    )
                    .use_worker_versioning(get(&d, 2)?)
                    .maybe_default_versioning_behavior(behavior)
                    .build(),
                )
            }
        },
    })
}

fn unversioned() -> WorkerVersioningStrategy {
    WorkerVersioningStrategy::None {
        build_id: "temporal-gleam".to_string(),
    }
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
        .versioning_strategy(options.versioning)
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
        let conn = conn.clone();
        let worker = temporalio_sdk_core::init_worker(runtime(), config, conn.connection.clone())
            .map_err(|e| format!("worker init failed: {e}"))?;
        worker
            .validate()
            .await
            .map_err(|e| format!("worker validation failed: {e}"))?;
        Ok::<_, String>(WorkerRes {
            worker: Arc::new(worker),
            namespace: conn.namespace.clone(),
        })
    };
    // The worker belongs to the process that asked for it and is shut down if that dies.
    let owner = pid;
    spawn_reply(pid, reference, fut, move |env, r| match r {
        Ok(res) => {
            let worker = ResourceArc::new(res);
            worker.monitor(None, &owner);
            ok(env, worker.encode(env))
        }
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
        versioning: unversioned(),
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
                namespace: "replay".to_string(),
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
fn worker_poll_workflow<'a>(pid: LocalPid, reference: Term<'a>, w: ResourceArc<WorkerRes>) -> Atom {
    let poller = w.clone();
    let fut = async move { poller.worker.poll_workflow_activation().await };
    spawn_reply_or(
        pid,
        reference,
        fut,
        |env, r| match r {
            Ok(activation) => ok(env, encode_activation(env, activation)),
            Err(e) => poll_error(env, e),
        },
        // The run id, and whether the activation only evicts the run.
        |r| {
            r.as_ref().ok().map(|a| {
                let eviction = a
                    .jobs
                    .iter()
                    .all(|j| matches!(j.variant, Some(Job::RemoveFromCache(_))));
                (a.run_id.clone(), eviction)
            })
        },
        |lost| async move {
            let Some((run_id, eviction)) = lost else {
                return;
            };
            // An eviction is acknowledged. Anything else fails the task, which makes core
            // evict the run and the server send the task again.
            let status = if eviction {
                Status::Successful(Default::default())
            } else {
                Status::Failed(workflow_completion::Failure {
                    failure: Some(lost_task_failure()),
                    ..Default::default()
                })
            };
            let _ = w
                .worker
                .complete_workflow_activation(WorkflowActivationCompletion {
                    run_id,
                    status: Some(status),
                    ..Default::default()
                })
                .await;
        },
    );
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
    let completion = match decode_completion(completion, &w.namespace) {
        Ok(c) => c,
        Err(e) => return err(env, format!("invalid completion: {e}")),
    };
    let fut = async move { w.worker.complete_workflow_activation(completion).await };
    spawn_reply(pid, reference, fut, unit_result);
    atoms::ok().encode(env)
}

#[rustler::nif]
fn worker_poll_activity<'a>(pid: LocalPid, reference: Term<'a>, w: ResourceArc<WorkerRes>) -> Atom {
    let poller = w.clone();
    let fut = async move { poller.worker.poll_activity_task().await };
    spawn_reply_or(
        pid,
        reference,
        fut,
        |env, r| match r {
            Ok(task) => ok(env, encode_activity_task(env, task)),
            Err(e) => poll_error(env, e),
        },
        // The token of a task that starts an activity.
        |r| match r {
            Ok(task) if matches!(task.variant, Some(activity_task::Variant::Start(_))) => {
                Some(task.task_token.clone())
            }
            _ => None,
        },
        |lost| async move {
            let Some(task_token) = lost else { return };
            // Failed, not cancelled, so that the activity's retry policy applies.
            let _ = w
                .worker
                .complete_activity_task(ActivityTaskCompletion {
                    task_token,
                    result: Some(ActivityExecutionResult {
                        status: Some(activity_execution_result::Status::Failed(
                            activity_result::Failure {
                                failure: Some(lost_task_failure()),
                                ..Default::default()
                            },
                        )),
                    }),
                })
                .await;
        },
    );
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
    spawn_reply(pid, reference, fut, unit_result);
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

#[rustler::nif]
fn worker_poll_nexus<'a>(pid: LocalPid, reference: Term<'a>, w: ResourceArc<WorkerRes>) -> Atom {
    let poller = w.clone();
    let fut = async move { poller.worker.poll_nexus_task().await };
    spawn_reply_or(
        pid,
        reference,
        fut,
        |env, r| match r {
            Ok(task) => ok(env, encode_nexus_task(env, task)),
            Err(e) => poll_error(env, e),
        },
        |r| match r {
            Ok(NexusTask {
                variant: Some(nexus_task::Variant::Task(task)),
                ..
            }) => Some(task.task_token.clone()),
            _ => None,
        },
        |lost| async move {
            let Some(task_token) = lost else { return };
            // The caller's server retries a request its handler could not take.
            let _ = w
                .worker
                .complete_nexus_task(NexusTaskCompletion {
                    task_token,
                    status: Some(nexus_task_completion::Status::Failure(lost_task_failure())),
                })
                .await;
        },
    );
    atoms::ok()
}

#[rustler::nif]
fn worker_complete_nexus<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    w: ResourceArc<WorkerRes>,
    task_token: Binary<'a>,
    result: Term<'a>,
) -> Term<'a> {
    let completion = match decode_nexus_result(result, task_token.as_slice().to_vec()) {
        Ok(c) => c,
        Err(e) => return err(env, format!("invalid nexus result: {e}")),
    };
    let fut = async move { w.worker.complete_nexus_task(completion).await };
    spawn_reply(pid, reference, fut, unit_result);
    atoms::ok().encode(env)
}

/// Stop polling. Outstanding polls resolve with `{error, shutdown}` once in-flight tasks finish.
#[rustler::nif]
fn worker_shutdown<'a>(pid: LocalPid, reference: Term<'a>, w: ResourceArc<WorkerRes>) -> Atom {
    let fut = async move {
        w.worker.initiate_shutdown();
        // Bounded so a workflow that never answers its eviction cannot hang the caller.
        let _ = tokio::time::timeout(Duration::from_secs(10), w.worker.shutdown()).await;
    };
    spawn_reply(pid, reference, fut, |env, ()| atoms::nil().encode(env));
    atoms::ok()
}

rustler::init!("temporal_nif");
