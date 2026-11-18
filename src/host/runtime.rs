use std::future::Future;

use crate::api::v1::{ActiveSystemSettingsView, ApiServer, ServerConfig};
use crate::db::{DatabaseHandle, DatabaseWorker};
use crate::host::data_directory::DataDirectory;
use crate::host::job_runtime::ExecutionRuntime;
use crate::host::logger::{LogStore, RuntimeLogger};
use crate::host::policy::SystemPolicy;
use crate::host::protection_notice::report_protection;

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
        crate::host::job_runtime::ClaimScope::Process,
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
        crate::host::job_runtime::ClaimScope::Stored,
        until_stopped,
    )
    .into_result()
}

fn run_scoped_reporting<F>(
    data_directory: &DataDirectory,
    signals: bool,
    server_config: Option<ServerConfig>,
    scope: crate::host::job_runtime::ClaimScope,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
) -> HostReport
where
    F: Future<Output = ()> + 'static,
{
    let mut report = HostReport::default();
    if let Err(error) = crate::network::http::initialize() {
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
    let filesystem_config = policy.filesystem_config;
    let database = match DatabaseWorker::start_with_config(data_directory, filesystem_config) {
        Ok(database) => database,
        Err(error) => {
            report.fail(Phase::Startup, error);
            return report;
        }
    };
    let workspace_tools =
        crate::jobs::execution::runtime::WorkspaceTools::new(data_directory.clone());
    let served = ntex::rt::System::build()
        .name("ocg")
        .signals(signals)
        .build(ntex::rt::DefaultRuntime)
        .block_on(serve(
            data_directory.clone(),
            policy,
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
    data_directory: DataDirectory,
    policy: SystemPolicy,
    handle: DatabaseHandle,
    scope: crate::host::job_runtime::ClaimScope,
    server_config: Option<ServerConfig>,
    workspace_tools: crate::jobs::execution::runtime::WorkspaceTools,
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
    let logs = LogStore::new(data_directory.logs_path());
    let logger = match RuntimeLogger::start(handle.clone(), logs.clone(), policy.logger_config) {
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
            for (name, enabled) in crate::host::execution_features::COMPILED_EXECUTION_FEATURES {
                let enabled = if name == crate::host::execution_features::AUTONOMOUS_SCHEDULING {
                    execution.is_autonomous()
                } else {
                    enabled
                };
                let state = if enabled { "Enabled" } else { "Disabled" };
                log::info!("execution feature {name}: {state}");
            }
            handle.log_open_warnings();
            if std::env::var_os(REMOVED_WORKSPACE_ROOT_VARIABLE).is_some() {
                log::warn!(
                    "{REMOVED_WORKSPACE_ROOT_VARIABLE} is ignored; a Project names the Workspace of a Job"
                );
            }
        }
        Err(error) => report.fail(Phase::Logger, error),
    }

    crate::network::http::log_configuration();
    let cleanup = match crate::host::logger::start_retention(
        &handle,
        logs.clone(),
        policy.logger_config.retention_days,
        policy
            .filesystem_config
            .cleanup_config
            .max_log_files_per_run,
    ) {
        Ok(cleanup) => Some(cleanup),
        Err(error) => {
            log::error!("log retention did not start: {error}");
            report.fail(Phase::LogCleanup, error);
            None
        }
    };
    let listener = server_config
        .map(|config| {
            ApiServer::start(
                config,
                handle.clone(),
                ActiveSystemSettingsView::new(&data_directory, &policy),
                logs,
                execution.native_stops(),
                workspace_tools,
            )
        })
        .transpose();
    match listener {
        Ok(listener) => serve_api(&mut report, handle, listener, until_stopped).await,
        Err(error) => {
            log::error!("{error}");
            report.fail(Phase::ApiServer, error);
        }
    }

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
    listener: Option<ApiServer>,
    until_stopped: impl FnOnce(DatabaseHandle) -> F,
) where
    F: Future<Output = ()>,
{
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
