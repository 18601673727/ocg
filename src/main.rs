use std::process::ExitCode;

use ocg::control_plane::ControlPlaneConfig;
use ocg::data_directory::DataDirectory;

mod llm_target;
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
/// and `workspace-vision <image-path> <objective>` one that stores an image
/// of it as an Attachment and sends it with the objective, see
/// `workspace_job`. `register-llm` registers the LLM target that Control
/// Plane submissions name, prints its identities and exits, see
/// `llm_target`. Without arguments, the local Control Plane is served too
/// when it is configured, see [`ocg::control_plane`].
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
        if command == llm_target::COMMAND {
            if arguments.next().is_some() {
                return Err(llm_target::Error::Usage.into());
            }
            let target = llm_target::control_plane_target()?;
            let data_directory = DataDirectory::from_environment()?;
            eprintln!("OCG data directory: {}", data_directory.root().display());
            return Ok(llm_target::register(&data_directory, target)?);
        }
        return Err(format!("unknown command `{command}`").into());
    }
    // Refused before anything starts when it is given but invalid.
    let control = ControlPlaneConfig::from_environment()?;
    let data_directory = DataDirectory::from_environment()?;
    eprintln!("OCG data directory: {}", data_directory.root().display());
    match control {
        None => ocg::host::run(&data_directory, true, |_database| until_signalled())?,
        Some(control) => {
            ocg::host::run_controlled(&data_directory, true, Some(control), |_context| {
                until_signalled()
            })?;
        }
    }
    Ok(())
}

async fn until_signalled() {
    let signals = ntex::rt::signals::signal().await;
    log::info!("OCG received {signals:?}; shutting down");
}
