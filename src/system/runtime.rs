use std::future::Future;
use std::sync::Arc;

use crate::api::v1::{ApiServer, ServerConfig};
use crate::db::{DatabaseHandle, DatabaseWorker};
use crate::system::data_directory::DataDirectory;
use crate::system::logger::{LogStore, RuntimeLogger};
use crate::system::monitor::report_protection;
use crate::system::native_scheduler::NativeStops;
use crate::system::policy::SystemPolicy;
use crate::system::scheduler::ExecutionRuntime;

const REMOVED_WORKSPACE_ROOT_VARIABLE: &str = "OCG_WORKSPACE_ROOT";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Startup,
    Logger,
    LogCleanup,
    ExecutionRuntime,
    ApiServer,
    Database,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Startup => "startup",
            Self::Logger => "logger",
            Self::LogCleanup => "log cleanup",
            Self::ExecutionRuntime => "execution runtime",
            Self::ApiServer => "API server",
            Self::Database => "database",
        })
    }
}

#[derive(Debug)]
pub struct HostFailure {
    pub phase: Phase,
    pub error: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{}", .failures.iter().map(|failure| format!("{}: {}", failure.phase, failure.error)).collect::<Vec<_>>().join("; "))]
pub struct HostError {
    pub failures: Vec<HostFailure>,
}

#[derive(Debug, Default)]
pub(crate) struct HostReport {
    pub(crate) failures: Vec<HostFailure>,
}

impl HostReport {
    fn record(&mut self, phase: Phase, result: Result<(), impl std::fmt::Display>) {
        if let Err(error) = result {
            self.fail(phase, error);
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

pub(crate) fn run_hosted<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
) -> Result<(), HostError>
where
    F: Future<Output = ()> + 'static,
{
    run_hosted_reporting(data_directory, signals, until_stopped).into_result()
}

pub(crate) fn run_hosted_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
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

pub(crate) fn run_server<F>(
    data_directory: &DataDirectory,
    signals: bool,
    server_config: Option<ServerConfig>,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
) -> Result<(), HostError>
where
    F: Future<Output = ()> + 'static,
{
    run_scoped_reporting(
        data_directory,
        signals,
        server_config,
        crate::system::scheduler::ClaimScope::Stored,
        until_stopped,
    )
    .into_result()
}

fn run_scoped_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    server_config: Option<ServerConfig>,
    scope: crate::system::scheduler::ClaimScope,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
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
            server_config,
            workspace_tools,
            until_stopped,
        ));
    report.failures.extend(served.failures);
    report.record(Phase::Database, database.shutdown());
    report
}

async fn serve<F>(
    handle: DatabaseHandle,
    scope: crate::system::scheduler::ClaimScope,
    server_config: Option<ServerConfig>,
    workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    until_stopped: impl FnOnce(DatabaseHandle) -> F,
) -> HostReport
where
    F: Future<Output = ()>,
{
    let mut report = HostReport::default();
    let execution = match ExecutionRuntime::start_host(handle.clone(), scope, &workspace_tools) {
        Ok(execution) => execution,
        Err(error) => {
            report.fail(Phase::Startup, error);
            return report;
        }
    };
    ntex::rt::spawn(report_protection(handle.watch_disk_protection()));
    let logs = LogStore::new(handle.data_directory().logs_path());
    let logger = match RuntimeLogger::start(handle.clone(), logs.clone()) {
        Ok(logger) => logger,
        Err(error) => {
            report.fail(Phase::Logger, error);
            report.record(Phase::ExecutionRuntime, execution.shutdown().await);
            return report;
        }
    };
    let log_handle = logger.handle();
    match log::set_boxed_logger(Box::new(log_handle.clone())) {
        Ok(()) => {
            log::set_max_level(log_handle.active_config().level.filter());
            log::info!("OCG foundation initialized; storage is ready");
            for (name, enabled) in crate::system::execution_features::EXECUTION_FEATURES {
                let state = if enabled { "Enabled" } else { "Disabled" };
                log::info!("execution feature {name}: {state}");
            }
            log::info!(
                "autonomous scheduling: {}",
                if execution.is_autonomous() {
                    "enabled for stored jobs using their configured Provider endpoints"
                } else {
                    "disabled"
                }
            );
            handle.log_open_warnings();
            if std::env::var_os(REMOVED_WORKSPACE_ROOT_VARIABLE).is_some() {
                log::warn!(
                    "{REMOVED_WORKSPACE_ROOT_VARIABLE} is ignored; a Project names the Workspace of a Job"
                );
            }
        }
        Err(error) => report.fail(Phase::Logger, error),
    }

    crate::system::http::log_configuration();
    let cleanup = match crate::system::logger::start_retention(&handle, logs.clone()) {
        Ok(cleanup) => Some(cleanup),
        Err(error) => {
            log::error!("log retention did not start: {error}");
            report.fail(Phase::LogCleanup, error);
            None
        }
    };
    serve_api(
        &mut report,
        handle,
        execution.native_stops(),
        logs,
        server_config,
        workspace_tools,
        until_stopped,
    )
    .await;

    report.record(Phase::ExecutionRuntime, execution.shutdown().await);
    if let Some(cleanup) = cleanup
        && let Err(error) = cleanup.shutdown()
    {
        report.fail(Phase::LogCleanup, error);
    }
    log::info!("OCG foundation shutting down");
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

async fn serve_api<F>(
    report: &mut HostReport,
    handle: DatabaseHandle,
    native_stops: NativeStops,
    logs: Arc<LogStore>,
    server_config: Option<ServerConfig>,
    workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    until_stopped: impl FnOnce(DatabaseHandle) -> F,
) where
    F: Future<Output = ()>,
{
    let listener = match server_config {
        None => None,
        Some(config) => {
            match ApiServer::start(config, handle.clone(), logs, native_stops, workspace_tools) {
                Ok(listener) => Some(listener),
                Err(error) => {
                    log::error!("{error}");
                    report.fail(Phase::ApiServer, error);
                    return;
                }
            }
        }
    };
    if let Some(address) = listener.as_ref().map(ApiServer::address) {
        log::info!("API server listening on http://{address}");
        eprintln!("OCG API server listening on http://{address}");
    }
    eprintln!("OCG host running");
    until_stopped(handle).await;
    if let Some(listener) = listener
        && let Err(error) = listener.stop().await
    {
        report.fail(Phase::ApiServer, error);
    }
}
