use std::process::ExitCode;

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseHandle, DatabaseWorker};
use ocg::logger::RuntimeLogger;
use ocg::orchestration::aggregation_service::{
    AggregationService, AggregationServiceConfig, ServiceState,
};
use ocg::orchestration::execution_runtime::ExecutionRuntime;
use ocg::system_policy::SystemPolicy;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("OCG failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Runs until a graceful shutdown signal (SIGINT, SIGTERM, SIGQUIT or SIGHUP),
/// then stops in the reverse order of starting. Every step is attempted even
/// when an earlier one failed, and the database is closed last, after every
/// component that may still write to it has stopped.
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let data_directory = DataDirectory::from_environment()?;
    eprintln!("OCG data directory: {}", data_directory.root().display());
    let policy = SystemPolicy::load_for_startup(&data_directory)?;
    let database = DatabaseWorker::start_with_policy(&data_directory, policy)?;
    let execution = match ExecutionRuntime::start(database.handle()) {
        Ok(execution) => execution,
        Err(error) => {
            database.shutdown().ok();
            return Err(error.into());
        }
    };
    let handle = database.handle();
    // The system owns the process signals; its thread is this one.
    let operation = ntex::rt::System::build()
        .name("ocg")
        .signals(true)
        .build(ntex::rt::DefaultRuntime)
        .block_on(serve(handle));
    let execution_closed = execution.shutdown();
    let closed = database.shutdown();
    operation?;
    execution_closed?;
    closed?;
    Ok(())
}

async fn serve(handle: DatabaseHandle) -> Result<(), Box<dyn std::error::Error>> {
    let logger = RuntimeLogger::start(handle.clone())?;
    let log_handle = logger.handle();
    log::set_boxed_logger(Box::new(log_handle.clone()))?;
    log::set_max_level(log_handle.active_config().level.filter());
    log::info!("OCG foundation initialized; storage is ready");

    let mut errors: Vec<String> = Vec::new();
    match AggregationService::start(handle, AggregationServiceConfig::default()).await {
        Ok(service) => {
            eprintln!("OCG aggregation service running as {}", service.job());
            log::info!("aggregation service {} is running", service.job());
            if let ServiceState::Failed(reason) = service.state() {
                log::error!("aggregation service {} failed: {reason}", service.job());
            }
            // Stay alive until a graceful shutdown signal arrives.
            let signals = ntex::rt::signals::signal().await;
            log::info!("OCG received {signals:?}; shutting down");
            // The loop is joined before its Job is ended, and both before the
            // logger and the database are closed.
            if let Err(error) = service.shutdown().await.into_result() {
                errors.push(error.to_string());
            }
        }
        Err(error) => errors.push(format!("the aggregation service did not start: {error}")),
    }

    log::info!("OCG foundation shutting down");
    match logger.shutdown() {
        Ok(status) => {
            if let Some(failure) = status.last_failure {
                errors.push(failure.to_string());
            }
        }
        Err(error) => errors.push(error.to_string()),
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; ").into())
    }
}
