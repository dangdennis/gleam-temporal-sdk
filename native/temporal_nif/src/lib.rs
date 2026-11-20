//! Erlang NIF bridging Gleam to Temporal's `sdk-core`.
//!
//! Every blocking operation is spawned on core's Tokio runtime and answers the calling process
//! with `{temporal_nif, Ref, Result}`; `temporal_nif.erl` turns that back into a blocking call.
//! What crosses is a type from `temporal/internal/bridge`: `bridge.rs` has the same types,
//! generated from the Gleam ones, and `convert.rs` maps them to and from core's messages.

// NIF arguments are positional and mirror the Erlang call, so they cannot be grouped.
#![allow(clippy::too_many_arguments)]

mod bridge;
mod client;
mod convert;
mod schedule;
mod selftest;
mod testing;
mod wire;

use convert::{
    decode_activity_result, decode_completion, decode_nexus_result, n, to_activation,
    to_activity_task, to_nexus_task,
};
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
        temporal::api::{common::v1::Payload, failure::v1::Failure, history::v1::History},
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
use wire::{DecodeResult, Wire};

mod atoms {
    rustler::atoms! { ok, error, nil, temporal_nif }
}

pub struct ConnectionRes {
    pub connection: Connection,
    pub namespace: String,
    pub identity: String,
}

pub struct WorkerRes {
    pub worker: Arc<Worker>,
    /// How long shutting down waits for tasks in flight.
    pub shutdown_grace: Duration,
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
    let api_key = match Option::<String>::from_term(api_key) {
        Ok(key) => key,
        Err(e) => return err(env, format!("invalid API key: {e}")),
    };
    let tls = match Option::<bridge::Tls>::from_term(tls) {
        Ok(tls) => tls.map(tls_options),
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
fn tls_options(tls: bridge::Tls) -> TlsOptions {
    let bridge::Tls {
        server_root_ca,
        domain,
        client_certificate,
    } = tls;
    let client = client_certificate.map(|(certificate, key)| {
        ClientTlsOptions::builder()
            .client_cert(certificate.0)
            .client_private_key(key.0)
            .build()
    });
    TlsOptions::builder()
        .maybe_server_root_ca_cert(server_root_ca.map(|ca| ca.0))
        .maybe_domain(domain)
        .maybe_client_tls_options(client)
        .build()
}

/// What core is told from `bridge.WorkerOptions`.
struct WorkerOptions {
    max_cached_workflows: usize,
    max_workflow_tasks: usize,
    max_activities: usize,
    task_queue_activities_per_second: Option<f64>,
    worker_activities_per_second: Option<f64>,
    task_types: WorkerTaskTypes,
    versioning: WorkerVersioningStrategy,
    shutdown_grace: Duration,
}

fn worker_options(options: bridge::WorkerOptions) -> DecodeResult<WorkerOptions> {
    let bridge::WorkerOptions {
        max_cached_workflows,
        max_concurrent_workflow_tasks,
        max_concurrent_activities,
        max_task_queue_activities_per_second,
        max_activities_per_second,
        enable_workflows,
        enable_activities,
        enable_nexus,
        deployment,
        shutdown_grace_ms,
    } = options;
    Ok(WorkerOptions {
        max_cached_workflows: n(max_cached_workflows, "WorkerOptions.max_cached_workflows")?,
        max_workflow_tasks: n(
            max_concurrent_workflow_tasks,
            "WorkerOptions.max_concurrent_workflow_tasks",
        )?,
        max_activities: n(
            max_concurrent_activities,
            "WorkerOptions.max_concurrent_activities",
        )?,
        task_queue_activities_per_second: max_task_queue_activities_per_second,
        worker_activities_per_second: max_activities_per_second,
        task_types: WorkerTaskTypes {
            enable_workflows,
            // Local activities run in the worker that runs the workflow.
            enable_local_activities: enable_workflows,
            enable_remote_activities: enable_activities,
            enable_nexus,
        },
        versioning: match deployment {
            None => unversioned(),
            Some(deployment) => {
                let bridge::Deployment {
                    name,
                    build_id,
                    use_versioning,
                    default_behavior,
                } = deployment;
                let behavior = match n::<i32>(default_behavior, "Deployment.default_behavior")? {
                    1 => Some(VersioningBehavior::Pinned),
                    2 => Some(VersioningBehavior::AutoUpgrade),
                    _ => None,
                };
                WorkerVersioningStrategy::WorkerDeploymentBased(
                    WorkerDeploymentOptions::new(
                        WorkerDeploymentVersion::builder()
                            .deployment_name(name)
                            .build_id(build_id)
                            .build(),
                    )
                    .use_worker_versioning(use_versioning)
                    .maybe_default_versioning_behavior(behavior)
                    .build(),
                )
            }
        },
        shutdown_grace: Duration::from_millis(n(
            shutdown_grace_ms,
            "WorkerOptions.shutdown_grace_ms",
        )?),
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
        .graceful_shutdown_period(options.shutdown_grace)
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
    let options = match bridge::WorkerOptions::from_term(options).and_then(worker_options) {
        Ok(o) => o,
        Err(e) => return err(env, format!("invalid worker options: {e}")),
    };
    let fut = async move {
        let shutdown_grace = options.shutdown_grace;
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
            shutdown_grace,
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
        shutdown_grace: Duration::from_secs(1),
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
                shutdown_grace: Duration::from_secs(1),
                namespace: "replay".to_string(),
            })
            .encode(env),
        ),
        Err(e) => err(env, format!("replay worker init failed: {e}")),
    }
}

fn poll_error<'a>(env: Env<'a>, e: PollError) -> Term<'a> {
    let error = match e {
        PollError::ShutDown => bridge::PollError::Shutdown,
        other => bridge::PollError::PollFailed {
            message: other.to_string(),
        },
    };
    (atoms::error(), error.to_term(env)).encode(env)
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
            Ok(activation) => ok(env, to_activation(activation).to_term(env)),
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
            Ok(task) => ok(env, to_activity_task(task).to_term(env)),
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
    if let Ok(details) = Vec::<Payload>::from_term(details) {
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
            Ok(task) => ok(env, to_nexus_task(task).to_term(env)),
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
        // Bounded so a task that never finishes cannot hang the caller: the grace period
        // activities are given, and a little more for core to wind down.
        let limit = w.shutdown_grace + Duration::from_secs(5);
        let _ = tokio::time::timeout(limit, w.worker.shutdown()).await;
    };
    spawn_reply(pid, reference, fut, |env, ()| atoms::nil().encode(env));
    atoms::ok()
}

rustler::init!("temporal_nif");
