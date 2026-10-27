use crate::api::v1::ControlPlaneConfig;
use crate::system::data_directory::DataDirectory;

mod llm_target;
mod pool;
mod release_slot;
pub mod typescript;
#[cfg(unix)]
mod workspace_job;

/// Without arguments, runs until a graceful shutdown signal (SIGINT,
/// SIGTERM, SIGQUIT or SIGHUP), then stops in the reverse order of starting;
/// see [`crate::system::runtime`]. `workspace-read <objective>` instead runs one Job that
/// may read the authorized Workspace, `workspace-edit <objective>` one
/// that may also create and conditionally update its files,
/// `workspace-code <objective>` one that may also run its build and tests,
/// and `workspace-vision <image-path> <objective>` one that stores an image
/// of it as an Attachment and sends it with the objective, see
/// `workspace_job`. `register-llm` registers the LLM target that Control
/// Plane submissions name, prints its identities and exits, see
/// `llm_target`. Without arguments, the local Control Plane is served too
/// when it is configured, see [`crate::api::v1`].
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next();
    if let Some(command) = command.as_deref() {
        if command == "--version" || command == "-V" {
            println!("ocg {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        if command == "help" || command == "--help" || command == "-h" {
            println!("{}", usage());
            return Ok(());
        }
        if command == "pool" {
            let directory = DataDirectory::from_environment()?;
            return pool::run(&directory, arguments.collect());
        }
        if command == "release-slot" {
            let directory = DataDirectory::from_environment()?;
            return release_slot::run(&directory, arguments.collect());
        }
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
        #[cfg(not(unix))]
        if matches!(
            command,
            "workspace-read" | "workspace-edit" | "workspace-code" | "workspace-vision"
        ) {
            return Err(format!("command `{command}` is supported only on Unix").into());
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
        return Err(format!("unknown command `{command}`; see `ocg help`").into());
    }
    // Refused before anything starts when it is given but invalid.
    let control = ControlPlaneConfig::from_environment()?;
    let data_directory = DataDirectory::from_environment()?;
    eprintln!("OCG data directory: {}", data_directory.root().display());
    match control {
        None => crate::system::runtime::run(&data_directory, true, |_database| until_signalled())?,
        Some(control) => {
            crate::system::runtime::run_controlled(
                &data_directory,
                true,
                Some(control),
                |_context| until_signalled(),
            )?;
        }
    }
    Ok(())
}

async fn until_signalled() {
    let signals = ntex::rt::signals::signal().await;
    log::info!("OCG received {signals:?}; shutting down");
}

fn usage() -> &'static str {
    "usage: ocg [command] [arguments]\n\
     \n\
     Without command: run the OCG host (reads OCG_DATA_DIR, OCG_CONTROL_PORT, OCG_CONTROL_TOKEN, OCG_UI_DIR)\n\
     \n\
     Commands:\n\
       pool list | create <name> [nonzero-concurrency] | enable <pool> | disable <pool> | set-limit <pool> <nonzero-concurrency>\n\
       release-slot <job> [--yes]\n\
       workspace-read <objective>               (unix; OCG_PROJECT, OCG_PROVIDER_*)\n\
       workspace-edit <objective>               (unix; OCG_PROJECT, OCG_PROVIDER_*)\n\
       workspace-code <objective>               (unix; OCG_PROJECT, OCG_PROVIDER_*)\n\
       workspace-vision <image-path> <objective> (unix; OCG_PROJECT, OCG_PROVIDER_*)\n\
       register-llm\n\
       help, --help, -h\n\
       --version, -V"
}
