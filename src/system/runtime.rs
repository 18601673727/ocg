//! One process run: start the components, wait to be told to stop, then stop
//! them in the reverse order.
//!
//! Every stop step is attempted even when an earlier one failed, and every
//! failure, startup or shutdown, is kept with the phase it happened in. The
//! database is closed last, after everything that may still write to it.

use std::future::Future;
use std::net::SocketAddr;

use crate::api::v1::{ControlPlane, ControlPlaneConfig};
use crate::db::{DatabaseHandle, DatabaseWorker};
use crate::system::data_directory::DataDirectory;
use crate::system::log_cleanup;
use crate::system::logger::RuntimeLogger;
use crate::system::monitor::report_protection;
use crate::system::policy::SystemPolicy;
use crate::system::scheduler::{ExecutionRuntime, SchedulerHandle};

/// The variable that named the one Workspace of a host before Projects.
const REMOVED_WORKSPACE_ROOT_VARIABLE: &str = "OCG_WORKSPACE_ROOT";

/// Where in a run something happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Startup,
    Logger,
    LogCleanup,
    ExecutionRuntime,
    ControlPlane,
    Database,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Startup => "startup",
            Self::Logger => "logger",
            Self::LogCleanup => "log cleanup",
            Self::ExecutionRuntime => "execution runtime",
            Self::ControlPlane => "control plane",
            Self::Database => "database",
        })
    }
}

#[derive(Debug)]
pub struct HostFailure {
    pub phase: Phase,
    pub error: String,
}

/// Every failure of a run, in the order they happened.
#[derive(Debug, thiserror::Error)]
#[error("{}", .failures.iter().map(|failure| format!("{}: {}", failure.phase, failure.error)).collect::<Vec<_>>().join("; "))]
pub struct HostError {
    pub failures: Vec<HostFailure>,
}

/// What a run did: the phases it attempted and the failures it met.
#[derive(Debug, Default)]
pub(crate) struct HostReport {
    pub(crate) attempted: Vec<Phase>,
    pub(crate) failures: Vec<HostFailure>,
}

impl HostReport {
    fn attempt<T, E: std::fmt::Display>(
        &mut self,
        phase: Phase,
        result: Result<T, E>,
    ) -> Option<T> {
        self.attempted.push(phase);
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                self.fail(phase, error);
                None
            }
        }
    }

    fn fail(&mut self, phase: Phase, error: impl std::fmt::Display) {
        self.failures.push(HostFailure {
            phase,
            error: error.to_string(),
        });
    }

    pub(crate) fn into_result(self) -> Result<(), HostError> {
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(HostError {
                failures: self.failures,
            })
        }
    }
}

/// What a caller running inside the host gets: the database and the
/// scheduler of the host's own execution runtime. The host keeps owning
/// both. Once the host begins to stop, the scheduler refuses every
/// submission, however many clones of it exist.
#[derive(Clone)]
pub struct HostContext {
    database: DatabaseHandle,
    scheduler: SchedulerHandle,
    control_plane: Option<SocketAddr>,
}

impl HostContext {
    pub fn database(&self) -> &DatabaseHandle {
        &self.database
    }

    /// Submits work to the host's execution runtime; see [`SchedulerHandle`].
    pub fn scheduler(&self) -> &SchedulerHandle {
        &self.scheduler
    }

    /// Where the Control Plane listens, when the run was given one; it
    /// answers from before the caller is called until the caller returns.
    pub fn control_plane(&self) -> Option<SocketAddr> {
        self.control_plane
    }
}

/// Runs the process until `until_stopped` finishes, then stops it. With
/// `signals`, the host's runtime owns the process signals.
pub fn run<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
) -> Result<(), HostError>
where
    F: Future<Output = ()> + 'static,
{
    run_controlled(data_directory, signals, None, move |context| {
        until_stopped(context.database)
    })
}

/// Like [`run`], giving `until_stopped` the whole [`HostContext`].
pub fn run_hosted<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(HostContext) -> F + 'static,
) -> Result<(), HostError>
where
    F: Future<Output = ()> + 'static,
{
    run_hosted_reporting(data_directory, signals, until_stopped).into_result()
}

pub(crate) fn run_hosted_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(HostContext) -> F + 'static,
) -> HostReport
where
    F: Future<Output = ()> + 'static,
{
    run_scoped_reporting(
        data_directory,
        signals,
        None,
        crate::system::scheduler::ClaimScope::Process,
        until_stopped,
    )
}

/// Like [`run_hosted`], also serving the local Control Plane `control`
/// describes, when given, for as long as `until_stopped` runs; see
/// [`crate::api::v1`]. It starts only once every service the host
/// waits for runs; a listener that cannot start fails the run, and
/// `until_stopped` is then not called. It is stopped first, before the
/// execution runtime.
pub fn run_controlled<F>(
    data_directory: &DataDirectory,
    signals: bool,
    control: Option<ControlPlaneConfig>,
    until_stopped: impl FnOnce(HostContext) -> F + 'static,
) -> Result<(), HostError>
where
    F: Future<Output = ()> + 'static,
{
    run_scoped_reporting(
        data_directory,
        signals,
        control,
        crate::system::scheduler::ClaimScope::Stored,
        until_stopped,
    )
    .into_result()
}

fn run_scoped_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    control: Option<ControlPlaneConfig>,
    scope: crate::system::scheduler::ClaimScope,
    until_stopped: impl FnOnce(HostContext) -> F + 'static,
) -> HostReport
where
    F: Future<Output = ()> + 'static,
{
    let mut report = HostReport::default();
    if let Err(error) = crate::system::http::initialize() {
        report.fail(Phase::Startup, error);
        return report;
    }
    let policy = match SystemPolicy::load_for_startup(data_directory) {
        Ok(policy) => policy,
        Err(error) => {
            report.fail(Phase::Startup, error);
            return report;
        }
    };
    let database = match DatabaseWorker::start_with_policy(data_directory, policy) {
        Ok(database) => database,
        Err(error) => {
            report.fail(Phase::Startup, error);
            return report;
        }
    };
    let workspace_tools =
        crate::system::executor::execution_runtime::WorkspaceTools::new(data_directory.clone());
    let served = ntex::rt::System::build()
        .name("ocg")
        .signals(signals)
        .build(ntex::rt::DefaultRuntime)
        .block_on(serve(
            database.handle(),
            scope,
            control,
            workspace_tools,
            until_stopped,
        ));
    report.attempted.extend(served.attempted);
    report.failures.extend(served.failures);
    report.attempt(Phase::Database, database.shutdown());
    report
}

/// Runs the components that need the host's async runtime, waits for
/// `until_stopped`, then stops everything it was given, in this order: the
/// Control Plane, if any, so no request reads or submits past this point;
/// the execution runtime, so no submission starts once stopping begins; log
/// cleanup; the logger. The caller closes the
/// database after it.
async fn serve<F>(
    handle: DatabaseHandle,
    scope: crate::system::scheduler::ClaimScope,
    control: Option<ControlPlaneConfig>,
    workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    until_stopped: impl FnOnce(HostContext) -> F,
) -> HostReport
where
    F: Future<Output = ()>,
{
    let mut report = HostReport::default();
    let execution = match ExecutionRuntime::start(handle.clone(), scope) {
        Ok(execution) => execution,
        Err(error) => {
            report.fail(Phase::Startup, error);
            return report;
        }
    };
    // Started first: a lock also blocks the file log, so it is reported on
    // standard error for as long as the process runs.
    ntex::rt::spawn(report_protection(handle.watch_disk_protection()));
    let context = HostContext {
        database: handle.clone(),
        scheduler: execution.scheduler(),
        control_plane: None,
    };
    let logger = match RuntimeLogger::start(handle.clone()) {
        Ok(logger) => logger,
        Err(error) => {
            report.attempt::<(), _>(Phase::Logger, Err(error));
            report.attempt(Phase::ExecutionRuntime, execution.shutdown().await);
            return report;
        }
    };
    let log_handle = logger.handle();
    match log::set_boxed_logger(Box::new(log_handle.clone())) {
        Ok(()) => {
            log::set_max_level(log_handle.active_config().level.filter());
            log::info!("OCG foundation initialized; storage is ready");
            if std::env::var_os(REMOVED_WORKSPACE_ROOT_VARIABLE).is_some() {
                log::warn!(
                    "{REMOVED_WORKSPACE_ROOT_VARIABLE} is ignored; a Project names the Workspace of a Job"
                );
            }
            let abandoned = handle.abandoned_at_open();
            if !abandoned.is_empty() {
                let jobs = abandoned
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                log::warn!(
                    "Jobs left running by an earlier run were recorded as unknown and stopped: {jobs}"
                );
            }
        }
        Err(error) => report.fail(Phase::Logger, error),
    }

    crate::system::http::log_configuration();
    report.attempted.push(Phase::LogCleanup);
    let cleanup = match log_cleanup::start_retention(&handle).await {
        Ok(cleanup) => Some(cleanup),
        Err(error) => {
            log::error!("log retention did not start: {error}");
            report.fail(Phase::LogCleanup, error);
            None
        }
    };
    serve_control_plane(
        &mut report,
        context,
        control,
        workspace_tools,
        until_stopped,
    )
    .await;

    report.attempt(Phase::ExecutionRuntime, execution.shutdown().await);
    // The cleanup worker is joined while the database and the logger it
    // uses are still open; a run in flight stops at its next file.
    if let Some(cleanup) = cleanup
        && let Err(error) = cleanup.shutdown()
    {
        report.fail(Phase::LogCleanup, error);
    }
    log::info!("OCG foundation shutting down");
    report.attempted.push(Phase::Logger);
    match logger.shutdown() {
        Ok(status) => {
            if let Some(failure) = status.last_failure {
                report.fail(Phase::Logger, failure);
            }
        }
        Err(error) => report.fail(Phase::Logger, error),
    }
    report
}

async fn serve_control_plane<F>(
    report: &mut HostReport,
    mut context: HostContext,
    control: Option<ControlPlaneConfig>,
    workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    until_stopped: impl FnOnce(HostContext) -> F,
) where
    F: Future<Output = ()>,
{
    let listener = match control {
        None => None,
        Some(config) => {
            report.attempted.push(Phase::ControlPlane);
            match ControlPlane::start_with_workspace(
                config,
                context.database.clone(),
                context.scheduler.clone(),
                workspace_tools,
            ) {
                Ok(listener) => Some(listener),
                Err(error) => {
                    log::error!("{error}");
                    report.fail(Phase::ControlPlane, error);
                    return;
                }
            }
        }
    };
    context.control_plane = listener.as_ref().map(ControlPlane::address);
    // Announced only once everything runs, right before waiting to stop.
    if let Some(address) = context.control_plane {
        log::info!("control plane listening on http://{address}");
        eprintln!("OCG control plane listening on http://{address}");
    }
    // The sign-in code is for a remote control client of a headless
    // listener, on this machine: it is not the bearer token and
    // reveals none of it. A served frontend signs itself in and
    // prints no code. Each run prints its own, and only that one
    // signs a browser in.
    if let Some(pairing) = listener.as_ref().and_then(ControlPlane::pairing_code) {
        eprintln!("OCG local browser pairing code: {pairing}");
    }
    eprintln!("OCG host running");
    until_stopped(context).await;
    if let Some(listener) = listener
        && let Err(error) = listener.stop().await
    {
        report.fail(Phase::ControlPlane, error);
    }
}
