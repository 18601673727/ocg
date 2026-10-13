use std::process::ExitCode;

use ocg::data_directory::DataDirectory;

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
/// then stops in the reverse order of starting; see [`ocg::host`].
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let data_directory = DataDirectory::from_environment()?;
    eprintln!("OCG data directory: {}", data_directory.root().display());
    ocg::host::run(&data_directory, true, |_database| async {
        let signals = ntex::rt::signals::signal().await;
        log::info!("OCG received {signals:?}; shutting down");
    })?;
    Ok(())
}
