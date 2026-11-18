//! Test servers: start and stop one, and control the time-skipping server's clock.

use crate::{ConnectionRes, atoms, client::rpc_err, convert::*, err, ok, runtime, spawn_reply};
use rustler::{Atom, Encoder, Env, LocalPid, ResourceArc, Term};
use std::{process::Stdio, time::Duration};
use temporalio_common::protos::temporal::api::testservice::v1::{
    LockTimeSkippingRequest, SleepRequest, UnlockTimeSkippingRequest,
};
use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, EphemeralServer, TemporalDevServerConfig, TestServerConfig,
};
use tokio::sync::Mutex;
use tonic::IntoRequest;

/// A server process owned by the BEAM. Dropping the resource kills the process.
pub struct ServerRes {
    server: Mutex<Option<EphemeralServer>>,
}

#[rustler::resource_impl]
impl rustler::Resource for ServerRes {}

/// Start a server. `kind` is `time_skipping` for the test server, whose clock can be
/// advanced, or `dev` for the Temporal CLI's development server. `exe` is the path of the
/// executable; without one it is downloaded once and cached in the temporary directory.
#[rustler::nif]
fn server_start<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    kind: Atom,
    exe: Term<'a>,
) -> Atom {
    let time_skipping = kind == Atom::from_str(env, "time_skipping").unwrap();
    let exe = match opt(exe).and_then(|t| t.decode::<String>().ok()) {
        Some(path) => EphemeralExe::ExistingPath(path),
        None => EphemeralExe::CachedDownload {
            version: EphemeralExeVersion::SDKDefault {
                sdk_name: "sdk-rust".to_string(),
                sdk_version: "0.1.0".to_string(),
            },
            dest_dir: None,
            ttl: None,
        },
    };
    let fut = async move {
        let started = if time_skipping {
            TestServerConfig::builder()
                .exe(exe)
                .startup_timeout(Duration::from_secs(30))
                .build()
                .start_server_with_output(Stdio::null(), Stdio::null())
                .await
        } else {
            TemporalDevServerConfig::builder()
                .exe(exe)
                .startup_timeout(Duration::from_secs(30))
                .log(("pretty".to_string(), "error".to_string()))
                .build()
                .start_server_with_output(Stdio::null(), Stdio::null())
                .await
        };
        started.map_err(|e| format!("could not start the server: {e}"))
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(server) => {
            let target = format!("http://{}", server.target);
            let resource = ResourceArc::new(ServerRes {
                server: Mutex::new(Some(server)),
            });
            ok(env, (resource, target).encode(env))
        }
        Err(e) => err(env, e),
    });
    let _ = runtime();
    atoms::ok()
}

#[rustler::nif]
fn server_stop<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    server: ResourceArc<ServerRes>,
) -> Atom {
    let fut = async move {
        if let Some(mut running) = server.server.lock().await.take() {
            let _ = running.shutdown().await;
        }
    };
    spawn_reply(env, pid, reference, fut, |env, ()| atoms::nil().encode(env));
    atoms::ok()
}

enum TimeOp {
    Lock,
    Unlock,
    Sleep(u64),
    Now,
}

/// Control a time-skipping server's clock: `lock`, `unlock`, `{sleep, Ms}` (let that much
/// time pass at once, then stop skipping again) or `now`. Answers the server's time in
/// milliseconds for `now` and 0 otherwise.
#[rustler::nif]
fn time_control<'a>(
    env: Env<'a>,
    pid: LocalPid,
    reference: Term<'a>,
    conn: ResourceArc<ConnectionRes>,
    op: Term<'a>,
) -> Term<'a> {
    let op = match untag(op) {
        Ok((tag, f)) => match (tag.as_str(), get::<u64>(&f, 0)) {
            ("lock", _) => TimeOp::Lock,
            ("unlock", _) => TimeOp::Unlock,
            ("now", _) => TimeOp::Now,
            ("sleep", Ok(ms)) => TimeOp::Sleep(ms),
            _ => return err(env, "unknown time operation"),
        },
        Err(e) => return err(env, e),
    };
    let fut = async move {
        let mut service = conn.connection.test_service();
        match op {
            TimeOp::Lock => service
                .lock_time_skipping(LockTimeSkippingRequest {}.into_request())
                .await
                .map(|_| 0),
            TimeOp::Unlock => service
                .unlock_time_skipping(UnlockTimeSkippingRequest {}.into_request())
                .await
                .map(|_| 0),
            TimeOp::Sleep(ms) => service
                .unlock_time_skipping_with_sleep(
                    SleepRequest {
                        duration: Some(duration(ms)),
                    }
                    .into_request(),
                )
                .await
                .map(|_| 0),
            TimeOp::Now => service
                .get_current_time(().into_request())
                .await
                .map(|r| millis(r.into_inner().time)),
        }
    };
    spawn_reply(env, pid, reference, fut, |env, r| match r {
        Ok(ms) => ok(env, ms.encode(env)),
        Err(s) => rpc_err(env, &s),
    });
    atoms::ok().encode(env)
}
