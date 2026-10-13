//! One process run: start the components, wait to be told to stop, then stop
//! them in the reverse order.
//!
//! Every stop step is attempted even when an earlier one failed, and every
//! failure, startup or shutdown, is kept with the phase it happened in. The
//! database is closed last, after everything that may still write to it.

use std::future::Future;

use crate::data_directory::DataDirectory;
use crate::database::{DatabaseHandle, DatabaseWorker};
use crate::disk_protection::{ProtectionLock, ProtectionWatch};
use crate::log_cleanup;
use crate::logger::RuntimeLogger;
use crate::orchestration::aggregation_service::{
    AggregationService, AggregationServiceConfig, ServiceState,
};
use crate::orchestration::execution_runtime::{ExecutionRuntime, SchedulerHandle};
use crate::system_policy::SystemPolicy;

/// Where in a run something happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Startup,
    Logger,
    AggregationService,
    LogCleanup,
    ExecutionRuntime,
    Database,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Startup => "startup",
            Self::Logger => "logger",
            Self::AggregationService => "aggregation service",
            Self::LogCleanup => "log cleanup",
            Self::ExecutionRuntime => "execution runtime",
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
}

impl HostContext {
    pub fn database(&self) -> &DatabaseHandle {
        &self.database
    }

    /// Submits work to the host's execution runtime; see [`SchedulerHandle`].
    pub fn scheduler(&self) -> &SchedulerHandle {
        &self.scheduler
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
    let execution = match ExecutionRuntime::start(database.handle()) {
        Ok(execution) => execution,
        Err(error) => {
            // The database still has to be closed, and its failure is kept
            // beside the startup failure.
            report.fail(Phase::Startup, error);
            report.attempt(Phase::Database, database.shutdown());
            return report;
        }
    };
    let served = ntex::rt::System::build()
        .name("ocg")
        .signals(signals)
        .build(ntex::rt::DefaultRuntime)
        .block_on(serve(database.handle(), execution, until_stopped));
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
/// execution runtime, so no submission starts once stopping begins; log
/// cleanup; the aggregation service; the logger. The caller closes the
/// database after it.
async fn serve<F>(
    handle: DatabaseHandle,
    execution: ExecutionRuntime,
    until_stopped: impl FnOnce(HostContext) -> F,
) -> HostReport
where
    F: Future<Output = ()>,
{
    let mut report = HostReport::default();
    // Started first: a lock also blocks the file log, so it is reported on
    // standard error for as long as the process runs.
    ntex::rt::spawn(report_protection(handle.watch_disk_protection()));
    let context = HostContext {
        database: handle.clone(),
        scheduler: execution.scheduler(),
    };
    let logger = match RuntimeLogger::start(handle.clone()) {
        Ok(logger) => logger,
        Err(error) => {
            report.attempt::<(), _>(Phase::Logger, Err(error));
            report.attempt(Phase::ExecutionRuntime, execution.shutdown());
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

    report.attempted.push(Phase::AggregationService);
    let service = match AggregationService::start(
        handle.clone(),
        AggregationServiceConfig::default(),
    )
    .await
    {
        Ok(service) => {
            log::info!("aggregation service {} is running", service.job());
            match service.state() {
                ServiceState::Failed(reason) => {
                    log::error!("aggregation service {} failed: {reason}", service.job());
                }
                ServiceState::Degraded(reason) => {
                    eprintln!(
                        "OCG aggregation service {} is degraded: {reason}",
                        service.job()
                    );
                    log::error!(
                        "aggregation service {} is degraded: {reason}",
                        service.job()
                    );
                }
                ServiceState::Running | ServiceState::Paused => {}
            }
            Some(service)
        }
        // A service that failed before it was ready is not reported as
        // running, and the process does not wait for a stop signal.
        Err(error) => {
            report.fail(Phase::AggregationService, error);
            None
        }
    };
    // Retention runs only beside a running service, and a retention that
    // cannot start fails the run the same way.
    let cleanup = match &service {
        Some(_) => {
            report.attempted.push(Phase::LogCleanup);
            match log_cleanup::start_retention(&handle).await {
                Ok(cleanup) => Some(cleanup),
                Err(error) => {
                    log::error!("log retention did not start: {error}");
                    report.fail(Phase::LogCleanup, error);
                    None
                }
            }
        }
        None => None,
    };
    if let (Some(service), Some(_)) = (&service, &cleanup) {
        // Announced only once everything runs, right before waiting to stop.
        eprintln!("OCG aggregation service running as {}", service.job());
        until_stopped(context).await;
    }

    report.attempt(Phase::ExecutionRuntime, execution.shutdown());
    // The cleanup worker is joined while the database and the logger it
    // uses are still open; a run in flight stops at its next file.
    if let Some(cleanup) = cleanup
        && let Err(error) = cleanup.shutdown()
    {
        report.fail(Phase::LogCleanup, error);
    }
    // The loop is joined before its Job is ended, and both before the
    // logger and the database are closed.
    if let Some(service) = service
        && let Err(error) = service.shutdown().await.into_result()
    {
        report.fail(Phase::AggregationService, error);
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
