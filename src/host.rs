//! One process run: start the components, wait to be told to stop, then stop
//! them in the reverse order.
//!
//! Every stop step is attempted even when an earlier one failed, and every
//! failure, startup or shutdown, is kept with the phase it happened in. The
//! database is closed last, after everything that may still write to it.

use std::future::Future;
use std::net::SocketAddr;

use crate::control_plane::{ControlPlane, ControlPlaneConfig};
use crate::data_directory::DataDirectory;
use crate::database::{DatabaseHandle, DatabaseWorker};
use crate::disk_protection::{ProtectionLock, ProtectionWatch};
use crate::log_cleanup;
use crate::logger::RuntimeLogger;
use crate::orchestration::execution_runtime::{ExecutionRuntime, SchedulerHandle};
use crate::system_policy::SystemPolicy;

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
    run_reporting(data_directory, signals, until_stopped).into_result()
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

pub(crate) fn run_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
) -> HostReport
where
    F: Future<Output = ()> + 'static,
{
    run_hosted_reporting(data_directory, signals, move |context: HostContext| {
        until_stopped(context.database)
    })
}

pub(crate) fn run_hosted_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(HostContext) -> F + 'static,
) -> HostReport
where
    F: Future<Output = ()> + 'static,
{
    run_controlled_reporting(data_directory, signals, None, until_stopped)
}

/// Like [`run_hosted`], also serving the local Control Plane `control`
/// describes, when given, for as long as `until_stopped` runs; see
/// [`crate::control_plane`]. It starts only once every service the host
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
    run_controlled_reporting(data_directory, signals, control, until_stopped).into_result()
}

pub(crate) fn run_controlled_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    control: Option<ControlPlaneConfig>,
    until_stopped: impl FnOnce(HostContext) -> F + 'static,
) -> HostReport
where
    F: Future<Output = ()> + 'static,
{
    let mut report = HostReport::default();
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
    let served = ntex::rt::System::build()
        .name("ocg")
        .signals(signals)
        .build(ntex::rt::DefaultRuntime)
        .block_on(serve(database.handle(), control, until_stopped));
    report.attempted.extend(served.attempted);
    report.failures.extend(served.failures);
    report.attempt(Phase::Database, database.shutdown());
    report
}

/// Writes every change between locked and unlocked storage to standard
/// error, which needs neither the database nor the file log. Ends when the
/// database worker stops.
async fn report_protection(watch: ProtectionWatch) {
    let mut lock = watch.status().lock;
    if let Some(notice) = protection_notice(None, lock.as_ref()) {
        eprintln!("{notice}");
    }
    while let Some(status) = watch.changed().await {
        if let Some(notice) = protection_notice(lock.as_ref(), status.lock.as_ref()) {
            eprintln!("{notice}");
        }
        lock = status.lock;
    }
}

/// What changed between two protection states, if admission changed.
fn protection_notice(
    before: Option<&ProtectionLock>,
    after: Option<&ProtectionLock>,
) -> Option<String> {
    match (before, after) {
        (None, Some(lock)) => Some(format!(
            "OCG storage protection is active; commands are refused and the file log is paused: {lock}"
        )),
        (Some(_), None) => {
            Some("OCG storage protection was lifted; commands are accepted again".to_owned())
        }
        _ => None,
    }
}

/// Runs the components that need the host's async runtime, waits for
/// `until_stopped`, then stops everything it was given, in this order: the
/// Control Plane, if any, so no request reads or submits past this point;
/// the execution runtime, so no submission starts once stopping begins; log
/// cleanup; the logger. The caller closes the
/// database after it.
async fn serve<F>(
    handle: DatabaseHandle,
    control: Option<ControlPlaneConfig>,
    until_stopped: impl FnOnce(HostContext) -> F,
) -> HostReport
where
    F: Future<Output = ()>,
{
    let mut report = HostReport::default();
    let execution = match ExecutionRuntime::start(handle.clone()) {
        Ok(execution) => execution,
        Err(error) => {
            report.fail(Phase::Startup, error);
            return report;
        }
    };
    // Started first: a lock also blocks the file log, so it is reported on
    // standard error for as long as the process runs.
    ntex::rt::spawn(report_protection(handle.watch_disk_protection()));
    let mut context = HostContext {
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

    report.attempted.push(Phase::LogCleanup);
    let cleanup = match log_cleanup::start_retention(&handle).await {
        Ok(cleanup) => Some(cleanup),
        Err(error) => {
            log::error!("log retention did not start: {error}");
            report.fail(Phase::LogCleanup, error);
            None
        }
    };
    if cleanup.is_some() {
        // Started only beside the running retention, and the run does not
        // wait to be stopped when it cannot start.
        let listener = match control {
            None => Some(None),
            Some(config) => {
                report.attempted.push(Phase::ControlPlane);
                match ControlPlane::start(config, handle.clone(), execution.scheduler()) {
                    Ok(listener) => Some(Some(listener)),
                    Err(error) => {
                        log::error!("{error}");
                        report.fail(Phase::ControlPlane, error);
                        None
                    }
                }
            }
        };
        if let Some(listener) = listener {
            context.control_plane = listener.as_ref().map(ControlPlane::address);
            // Announced only once everything runs, right before waiting to stop.
            if let Some(address) = context.control_plane {
                log::info!("control plane listening on http://{address}");
                eprintln!("OCG control plane listening on http://{address}");
            }
            // The sign-in code is for the operator alone, on this machine:
            // it is not the bearer token and reveals none of it. Each run
            // prints its own, and only that one signs a browser in.
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
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_protection::{ProtectedArea, ProtectionReason, SpaceShortage};
    use crate::persistence::Timestamp;

    #[test]
    fn only_a_change_between_locked_and_unlocked_storage_is_reported() {
        let lock = ProtectionLock {
            reasons: vec![ProtectionReason::InsufficientSpace(SpaceShortage {
                area: ProtectedArea::Database,
                path: "ocg_db".into(),
                available_bytes: 10,
                expected_write_bytes: 0,
                threshold_bytes: 20,
            })],
            locked_at: Timestamp::from_unix_micros(0),
        };
        let locked = protection_notice(None, Some(&lock)).expect("a new lock is reported");
        assert!(locked.contains("ocg_db has 10 bytes available"), "{locked}");
        assert!(protection_notice(Some(&lock), None).is_some_and(|text| text.contains("lifted")));
        assert_eq!(protection_notice(Some(&lock), Some(&lock)), None);
        assert_eq!(protection_notice(None, None), None);
    }
}
