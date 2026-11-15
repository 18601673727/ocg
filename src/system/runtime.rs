use std::future::Future;
use std::net::SocketAddr;

use crate::api::v1::{ApiServer, ServerConfig};
use crate::db::{DatabaseHandle, DatabaseWorker};
use crate::system::data_directory::DataDirectory;
use crate::system::log_cleanup;
use crate::system::logger::RuntimeLogger;
use crate::system::monitor::report_protection;
use crate::system::policy::SystemPolicy;
use crate::system::scheduler::{ExecutionRuntime, SchedulerHandle};

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

#[derive(Clone)]
pub struct HostContext {
    database: DatabaseHandle,
    scheduler: SchedulerHandle,
    api_address: Option<SocketAddr>,
}

impl HostContext {
    pub fn database(&self) -> &DatabaseHandle {
        &self.database
    }

    pub fn scheduler(&self) -> &SchedulerHandle {
        &self.scheduler
    }

    pub fn api_address(&self) -> Option<SocketAddr> {
        self.api_address
    }
}

pub fn run<F>(
    data_directory: &DataDirectory,
    signals: bool,
    until_stopped: impl FnOnce(DatabaseHandle) -> F + 'static,
) -> Result<(), HostError>
where
    F: Future<Output = ()> + 'static,
{
    run_server(data_directory, signals, None, move |context| {
        until_stopped(context.database)
    })
}

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

pub fn run_server<F>(
    data_directory: &DataDirectory,
    signals: bool,
    server_config: Option<ServerConfig>,
    until_stopped: impl FnOnce(HostContext) -> F + 'static,
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
            server_config,
            workspace_tools,
            until_stopped,
        ));
    report.attempted.extend(served.attempted);
    report.failures.extend(served.failures);
    report.attempt(Phase::Database, database.shutdown());
    report
}

async fn serve<F>(
    handle: DatabaseHandle,
    scope: crate::system::scheduler::ClaimScope,
    server_config: Option<ServerConfig>,
    workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    until_stopped: impl FnOnce(HostContext) -> F,
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
    let context = HostContext {
        database: handle.clone(),
        scheduler: execution.scheduler(),
        api_address: None,
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
            for feature in crate::system::execution_features::ExecutionFeature::ALL {
                log::info!(
                    "execution feature {}: {:?}",
                    feature.name(),
                    feature.state()
                );
            }
            log::info!(
                "autonomous scheduling: {}",
                if execution.is_autonomous() {
                    "enabled for stored jobs (loopback endpoints direct; external endpoints require an active scheduled external policy and grant)"
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
    report.attempted.push(Phase::LogCleanup);
    let cleanup = match log_cleanup::start_retention(&handle).await {
        Ok(cleanup) => Some(cleanup),
        Err(error) => {
            log::error!("log retention did not start: {error}");
            report.fail(Phase::LogCleanup, error);
            None
        }
    };
    serve_api(
        &mut report,
        context,
        server_config,
        workspace_tools,
        until_stopped,
    )
    .await;

    report.attempt(Phase::ExecutionRuntime, execution.shutdown().await);
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

async fn serve_api<F>(
    report: &mut HostReport,
    mut context: HostContext,
    server_config: Option<ServerConfig>,
    workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    until_stopped: impl FnOnce(HostContext) -> F,
) where
    F: Future<Output = ()>,
{
    let listener = match server_config {
        None => None,
        Some(config) => {
            report.attempted.push(Phase::ApiServer);
            match ApiServer::start_with_workspace(
                config,
                context.database.clone(),
                context.scheduler.clone(),
                workspace_tools,
            ) {
                Ok(listener) => Some(listener),
                Err(error) => {
                    log::error!("{error}");
                    report.fail(Phase::ApiServer, error);
                    return;
                }
            }
        }
    };
    context.api_address = listener.as_ref().map(ApiServer::address);
    if let Some(address) = context.api_address {
        log::info!("API server listening on http://{address}");
        eprintln!("OCG API server listening on http://{address}");
    }
    if let Some(pairing) = listener.as_ref().and_then(ApiServer::pairing_code) {
        eprintln!("OCG local browser pairing code: {pairing}");
    }
    eprintln!("OCG host running");
    until_stopped(context).await;
    if let Some(listener) = listener
        && let Err(error) = listener.stop().await
    {
        report.fail(Phase::ApiServer, error);
    }
}
