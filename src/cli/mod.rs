use crate::api::v1::ControlPlaneConfig;
use crate::system::data_directory::DataDirectory;

mod inspect_held;
mod llm_target;
mod migrate;
#[cfg(unix)]
mod reconcile_reserved;
mod scheduled_external_policy;
pub mod typescript;
#[cfg(unix)]
mod update;
mod verify;

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
        #[cfg(unix)]
        if command == "reconcile-held" {
            return reconcile_reserved::run_held(arguments.collect());
        }
        if command == "verify" {
            return verify::run(arguments.collect());
        }
        #[cfg(unix)]
        if command == "update" {
            return update::run(arguments.collect());
        }
        if command == "migrate" {
            return migrate::run(arguments.collect());
        }
        if command == scheduled_external_policy::COMMAND {
            let directory = DataDirectory::from_environment()?;
            return scheduled_external_policy::run(&directory, arguments.collect());
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
       inspect-held <data-directory>            (read-only)\n\
       verify <data-directory> [--pages]\n\
       reconcile-reserved <data-directory> <job> --confirm-no-start | --finalize\n\
       reconcile-held <data-directory> --dry-run | --apply <plan-digest> | --finalize\n\
       migrate [--check | --to <directory>]\n\
       scheduled-external-policy <create|list|show|update|enable|disable|revoke> ...\n\
       register-llm\n\
       help, --help, -h\n\
       --version, -V"
}
