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
use crate::logger::RuntimeLogger;
use crate::orchestration::aggregation_service::{
    AggregationService, AggregationServiceConfig, ServiceState,
};
use crate::orchestration::execution_runtime::ExecutionRuntime;
use crate::system_policy::SystemPolicy;

/// Where in a run something happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Startup,
    Logger,
    AggregationService,
    ExecutionRuntime,
    Database,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Startup => "startup",
            Self::Logger => "logger",
            Self::AggregationService => "aggregation service",
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

pub(crate) fn run_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
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
        .block_on(serve(database.handle(), until_stopped));
    report.attempted.extend(served.attempted);
    report.failures.extend(served.failures);
    report.attempt(Phase::ExecutionRuntime, execution.shutdown());
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

async fn serve<F>(
    handle: DatabaseHandle,
    until_stopped: impl FnOnce(DatabaseHandle) -> F,
) -> HostReport
where
    F: Future<Output = ()>,
{
    let mut report = HostReport::default();
    // Started first: a lock also blocks the file log, so it is reported on
    // standard error for as long as the process runs.
    ntex::rt::spawn(report_protection(handle.watch_disk_protection()));
    let logger = match RuntimeLogger::start(handle.clone()) {
        Ok(logger) => logger,
        Err(error) => {
            report.attempt::<(), _>(Phase::Logger, Err(error));
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
    match AggregationService::start(handle.clone(), AggregationServiceConfig::default()).await {
        Ok(service) => {
            eprintln!("OCG aggregation service running as {}", service.job());
            log::info!("aggregation service {} is running", service.job());
            if let ServiceState::Failed(reason) = service.state() {
                log::error!("aggregation service {} failed: {reason}", service.job());
            }
            until_stopped(handle).await;
            // The loop is joined before its Job is ended, and both before the
            // logger and the database are closed.
            if let Err(error) = service.shutdown().await.into_result() {
                report.fail(Phase::AggregationService, error);
            }
        }
        // A service that failed before it was ready is not reported as
        // running, and the process does not wait for a stop signal.
        Err(error) => report.fail(Phase::AggregationService, error),
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
