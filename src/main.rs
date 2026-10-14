use std::process::ExitCode;

use ocg::data_directory::DataDirectory;

#[cfg(unix)]
mod workspace_job;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("OCG failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Without arguments, runs until a graceful shutdown signal (SIGINT,
/// SIGTERM, SIGQUIT or SIGHUP), then stops in the reverse order of starting;
/// see [`ocg::host`]. `workspace-read <objective>` instead runs one Job that
/// may read the authorized Workspace, `workspace-edit <objective>` one
/// that may also create and conditionally update its files,
/// `workspace-code <objective>` one that may also run its build and tests,
/// and `workspace-vision <image-path> <objective>` one that sends an image
/// of it with the objective, see `workspace_job`.
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next();
    if let Some(command) = command.as_deref() {
        #[cfg(unix)]
        if let Some(mode) = workspace_job::Mode::of(command) {
            let image = if mode.takes_image() {
                arguments.next()
            } else {
                None
            };
            let objective = arguments.next();
            if arguments.next().is_some() {
                return Err(mode.usage().into());
            }
            let config = workspace_job::Config::from_environment(mode, image, objective)?;
            let data_directory = DataDirectory::from_environment()?;
            eprintln!("OCG data directory: {}", data_directory.root().display());
            return Ok(workspace_job::run(&data_directory, config)?);
        }
        return Err(format!("unknown command `{command}`").into());
    }
    let data_directory = DataDirectory::from_environment()?;
    eprintln!("OCG data directory: {}", data_directory.root().display());
    ocg::host::run(&data_directory, true, |_database| async {
        let signals = ntex::rt::signals::signal().await;
        log::info!("OCG received {signals:?}; shutting down");
    })?;
    Ok(())
}
