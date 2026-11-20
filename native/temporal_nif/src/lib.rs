//! Erlang NIF bridging Gleam to Temporal's `sdk-core`.
//!
//! Every blocking operation is spawned on core's Tokio runtime and answers the calling process
//! with `{temporal_nif, Ref, Result}`; `temporal_nif.erl` turns that back into a blocking call.
//! What crosses is a type from `temporal/internal/bridge`: `bridge.rs` has the same types,
//! generated from the Gleam ones, and `convert.rs` maps them to and from core's messages.

// NIF arguments are positional and mirror the Erlang call, so they cannot be grouped.
#![allow(clippy::too_many_arguments)]

mod activity;
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
use temporalio_client::{
    ClientKeepAliveOptions, ClientTlsOptions, Connection, ConnectionOptions, TlsOptions,
};
use temporalio_common::telemetry::{
    PrometheusExporterOptions, TelemetryOptions, construct_filter_string,
    start_prometheus_metric_exporter,
};
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
        temporal::api::{
            common::v1::Payload,
            failure::v1::{Failure, failure::FailureInfo},
            history::v1::History,
        },
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
use tracing::{Level, Subscriber};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt};
use wire::{DecodeResult, Wire};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        temporal_nif,
        already_started,
        invalid_address,
        bind_failed
    }
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
/// The least severe of core's log lines to print, if asked before the runtime started.
static CORE_LOG_LEVEL: Mutex<Option<bridge::CoreLogLevel>> = Mutex::new(None);
static RUNTIME: OnceLock<CoreRuntime> = OnceLock::new();

/// Prints the log lines of core and its client at `level` and above on standard error.
/// Lines from the libraries beneath them are printed from warnings up.
fn core_logger(level: bridge::CoreLogLevel) -> Option<Arc<dyn Subscriber + Send + Sync>> {
    let level = match level {
        bridge::CoreLogLevel::CoreLogOff => return None,
        bridge::CoreLogLevel::CoreLogError => Level::ERROR,
        bridge::CoreLogLevel::CoreLogWarning => Level::WARN,
        bridge::CoreLogLevel::CoreLogInfo => Level::INFO,
        bridge::CoreLogLevel::CoreLogDebug => Level::DEBUG,
    };
    let others = level.min(Level::WARN);
    let lines = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_target(false)
        .with_filter(EnvFilter::new(construct_filter_string(level, others)));
    Some(Arc::new(tracing_subscriber::registry().with(lines)))
}

pub fn runtime() -> &'static CoreRuntime {
    RUNTIME.get_or_init(|| {
        let level = CORE_LOG_LEVEL
            .lock()
            .ok()
            .and_then(|level| level.clone())
            .unwrap_or(bridge::CoreLogLevel::CoreLogWarning);
        let telemetry = TelemetryOptions::builder()
            .maybe_subscriber_override(core_logger(level))
            .build();
        let options = RuntimeOptions::builder()
            .telemetry_options(telemetry)
            .build()
            .expect("the default runtime options are valid");
        let mut runtime = CoreRuntime::new(options, TokioRuntimeBuilder::default())
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
/// port 0. The errors are the variants of Gleam's `telemetry.Error`.
#[rustler::nif]
fn start_metrics<'a>(env: Env<'a>, address: String) -> Term<'a> {
    let already_started = || (atoms::error(), atoms::already_started()).encode(env);
    if RUNTIME.get().is_some() {
        return already_started();
    }
    let address: SocketAddr = match address.parse() {
        Ok(a) => a,
        Err(e) => {
            let reason = (atoms::invalid_address(), format!("\"{address}\": {e}"));
            return (atoms::error(), reason).encode(env);
        }
    };
    if let Ok(mut wanted) = METRICS_ADDRESS.lock() {
        *wanted = Some(address);
    }
    runtime();
    match METRICS_BOUND.get() {
        Some(Ok(bound)) => ok(env, Encoder::encode(&bound.to_string(), env)),
        Some(Err(e)) => (atoms::error(), (atoms::bind_failed(), e)).encode(env),
        // Another process started the runtime between the check above and here.
        None => already_started(),
    }
}

/// Choose which of core's log lines are printed. Like `start_metrics`, it has to be called
/// before the runtime starts.
#[rustler::nif]
fn set_core_log_level<'a>(env: Env<'a>, level: Term<'a>) -> rustler::NifResult<Term<'a>> {
    let level = bridge::CoreLogLevel::from_term(level).map_err(|_| rustler::Error::BadArg)?;
    let already_started = (atoms::error(), atoms::already_started()).encode(env);
    // Held while checking, so that a runtime starting now either reads the new level or
    // has already made this call fail.
    let Ok(mut wanted) = CORE_LOG_LEVEL.lock() else {
        return Ok(already_started);
    };
    if RUNTIME.get().is_some() {
        return Ok(already_started);
    }
    *wanted = Some(level);
    Ok(ok(env, atoms::nil().encode(env)))
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
        // The server takes no other kind of failure from an activity.
        failure_info: Some(FailureInfo::ApplicationFailureInfo(Default::default())),
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

/// What the client crate is told from `bridge.ConnectOptions`.
struct ConnectOptions {
    target: String,
    namespace: String,
    identity: String,
    api_key: Option<String>,
    tls: Option<TlsOptions>,
    keep_alive: Option<ClientKeepAliveOptions>,
    connect_timeout: Duration,
}

fn connect_options(options: bridge::ConnectOptions) -> DecodeResult<ConnectOptions> {
    let bridge::ConnectOptions {
        target,
        namespace,
        identity,
        api_key,
        tls,
        keep_alive,
        connect_timeout_ms,
    } = options;
    let positive = |value: i64, name: &str| match n::<u64>(value, name)? {
        0 => Err(format!("{name}: must be longer than zero")),
        ms => Ok(Duration::from_millis(ms)),
    };
    Ok(ConnectOptions {
        target,
        namespace,
        identity,
        api_key,
        tls: tls.map(tls_options),
        keep_alive: keep_alive
            .map(|(interval, timeout)| {
                Ok::<_, String>(
                    ClientKeepAliveOptions::builder()
                        .interval(positive(interval, "the keep-alive interval")?)
                        .timeout(positive(timeout, "the keep-alive timeout")?)
                        .build(),
                )
            })
            .transpose()?,
        connect_timeout: positive(connect_timeout_ms, "the connect timeout")?,
    })
}

#[rustler::nif]
fn connect<'a>(env: Env<'a>, pid: LocalPid, reference: Term<'a>, options: Term<'a>) -> Term<'a> {
    let options = match bridge::ConnectOptions::from_term(options).and_then(connect_options) {
        Ok(options) => options,
        Err(e) => return err(env, format!("invalid connection options: {e}")),
    };
    let fut = async move {
        let ConnectOptions {
            target,
            namespace,
            identity,
            api_key,
            tls,
            keep_alive,
            connect_timeout,
        } = options;
        let url = Url::from_str(&target).map_err(|e| format!("invalid target url: {e}"))?;
        // The timeout is given twice: the option bounds every attempt to open a socket, now
        // and when reconnecting, and the outer one bounds the whole handshake, which also
        // waits for the server's first answer.
        let options = ConnectionOptions::new(url)
            .identity(identity.clone())
            .maybe_api_key(api_key)
            .maybe_tls_options(tls)
            .keep_alive(keep_alive)
            .connect_timeout(connect_timeout)
            .build();
        let connection = tokio::time::timeout(connect_timeout, Connection::connect(options))
            .await
            .map_err(|_| {
                format!("could not connect to {target} within {connect_timeout:?}: timed out")
            })?
            .map_err(|e| format!("could not connect to {target}: {e}"))?;
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
    max_nexus_tasks: usize,
    workflow_task_polls: usize,
    activity_task_polls: usize,
    nexus_task_polls: usize,
    task_queue_activities_per_second: Option<f64>,
    worker_activities_per_second: Option<f64>,
    task_types: WorkerTaskTypes,
    versioning: WorkerVersioningStrategy,
    shutdown_grace: Duration,
}

/// How many polls to keep open: what was asked for, but no more than there are slots to
/// run what they return, and no fewer than core accepts.
fn polls(asked: i64, slots: usize, least: usize, name: &str) -> DecodeResult<usize> {
    Ok(n::<usize>(asked, name)?.min(slots).max(least))
}

fn worker_options(options: bridge::WorkerOptions) -> DecodeResult<WorkerOptions> {
    let bridge::WorkerOptions {
        max_cached_workflows,
        max_concurrent_workflow_tasks,
        max_concurrent_activities,
        max_concurrent_nexus_tasks,
        max_concurrent_workflow_task_polls,
        max_concurrent_activity_task_polls,
        max_concurrent_nexus_task_polls,
        max_task_queue_activities_per_second,
        max_activities_per_second,
        enable_workflows,
        enable_activities,
        enable_nexus,
        deployment,
        shutdown_grace_ms,
    } = options;
    let max_cached_workflows = n(max_cached_workflows, "WorkerOptions.max_cached_workflows")?;
    let max_workflow_tasks = n(
        max_concurrent_workflow_tasks,
        "WorkerOptions.max_concurrent_workflow_tasks",
    )?;
    let max_activities = n(
        max_concurrent_activities,
        "WorkerOptions.max_concurrent_activities",
    )?;
    let max_nexus_tasks = n(
        max_concurrent_nexus_tasks,
        "WorkerOptions.max_concurrent_nexus_tasks",
    )?;
    Ok(WorkerOptions {
        max_cached_workflows,
        max_workflow_tasks,
        max_activities,
        max_nexus_tasks,
        workflow_task_polls: polls(
            max_concurrent_workflow_task_polls,
            max_workflow_tasks,
            // With a cache, core polls the worker's sticky queue as well as the shared one.
            if max_cached_workflows > 0 { 2 } else { 1 },
            "WorkerOptions.max_concurrent_workflow_task_polls",
        )?,
        activity_task_polls: polls(
            max_concurrent_activity_task_polls,
            max_activities,
            1,
            "WorkerOptions.max_concurrent_activity_task_polls",
        )?,
        nexus_task_polls: polls(
            max_concurrent_nexus_task_polls,
            max_nexus_tasks,
            1,
            "WorkerOptions.max_concurrent_nexus_task_polls",
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
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(options.workflow_task_polls))
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(options.activity_task_polls))
        .nexus_task_poller_behavior(PollerBehavior::SimpleMaximum(options.nexus_task_polls))
        .max_outstanding_workflow_tasks(options.max_workflow_tasks)
        .max_outstanding_activities(options.max_activities)
        .maybe_max_task_queue_activities_per_second(options.task_queue_activities_per_second)
        .maybe_max_worker_activities_per_second(options.worker_activities_per_second)
        .max_outstanding_local_activities(100usize)
        .max_outstanding_nexus_tasks(options.max_nexus_tasks)
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
        max_nexus_tasks: 1,
        workflow_task_polls: 2,
        activity_task_polls: 1,
        nexus_task_polls: 1,
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

/// Stop polling and wait until core's worker has shut down, which it has once every task in
/// flight has been completed and every poll has returned `{error, shutdown}`. `limit_ms`
/// bounds the wait; zero waits for as long as it takes.
#[rustler::nif]
fn worker_shutdown<'a>(
    pid: LocalPid,
    reference: Term<'a>,
    w: ResourceArc<WorkerRes>,
    limit_ms: u64,
) -> Atom {
    let fut = async move {
        w.worker.initiate_shutdown();
        if limit_ms == 0 {
            w.worker.shutdown().await;
        } else {
            let limit = Duration::from_millis(limit_ms);
            let _ = tokio::time::timeout(limit, w.worker.shutdown()).await;
        }
    };
    spawn_reply(pid, reference, fut, |env, ()| atoms::nil().encode(env));
    atoms::ok()
}

rustler::init!("temporal_nif");
