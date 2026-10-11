use std::process::ExitCode;

use ocg::data_directory::DataDirectory;
use ocg::database::DatabaseWorker;
use ocg::logger::RuntimeLogger;
use ocg::system_policy::SystemPolicy;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("OCG startup failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let data_directory = DataDirectory::from_environment()?;
    eprintln!("OCG data directory: {}", data_directory.root().display());
    let policy = SystemPolicy::load_for_startup(&data_directory)?;
    let database = DatabaseWorker::start_with_policy(&data_directory, policy)?;
    let operation = run_with_logging(&database);
    let closed = database.shutdown();
    operation?;
    closed?;
    Ok(())
}

fn run_with_logging(database: &DatabaseWorker) -> Result<(), Box<dyn std::error::Error>> {
    let logger = RuntimeLogger::start(database.handle())?;
    let handle = logger.handle();
    log::set_boxed_logger(Box::new(handle.clone()))?;
    log::set_max_level(handle.active_config().level.filter());
    log::info!("OCG foundation initialized; storage is ready");
    log::info!("OCG foundation shutting down");
    let status = logger.shutdown()?;
    if let Some(failure) = status.last_failure {
        return Err(Box::new(failure.as_ref().clone()));
    }
    Ok(())
}
