use crate::api::v1::ControlPlaneConfig;
use crate::system::data_directory::DataDirectory;

mod external_trial;
mod inspect_held;
mod llm_target;
mod manual_host;
#[cfg(unix)]
mod reconcile_reserved;
mod release_slot;
mod run_loopback_job;
pub mod typescript;
#[cfg(unix)]
mod update;
mod verify;

/// Without arguments, runs until a graceful shutdown signal (SIGINT,
/// SIGTERM, SIGQUIT or SIGHUP), then stops in the reverse order of starting;
/// see [`crate::system::runtime`]. `register-llm` registers the LLM target that Control
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
        if command == "inspect-held" {
            return inspect_held::run(arguments.collect());
        }
        #[cfg(unix)]
        if command == "reconcile-reserved" {
            return reconcile_reserved::run(arguments.collect());
        }
        if command == "external-trial" {
            return external_trial::run(arguments.collect());
        }
        if command == "run-loopback-job" {
            return run_loopback_job::run(arguments.collect());
        }
        if command == "verify" {
            return verify::run(arguments.collect());
        }
        #[cfg(unix)]
        if command == "update" {
            return update::run(arguments.collect());
        }
        if command == "release-slot" {
            let directory = DataDirectory::from_environment()?;
            return release_slot::run(&directory, arguments.collect());
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
       update [--check] [--version X.Y.Z] [--yes]\n\
       release-slot <job> [--yes]\n\
       run-loopback-job <job> --pool <id> --executor <id> --timeout-secs <n> --confirm-loopback-only\n\
         (runs one named submitted Generative Job once against its own literal 127.0.0.1 endpoint; never an external Provider)\n\
       inspect-held <data-directory>            (read-only)\n\
       verify <data-directory> [--pages]        (offline integrity check; never writes the original)\n\
       reconcile-reserved <data-directory> <job> --confirm-no-start\n\
         (offline: ends one crash-surviving Reserved Allocation that never started; works on a copy, keeps a rollback artifact)\n\
       register-llm\n\
       help, --help, -h\n\
       --version, -V"
}
