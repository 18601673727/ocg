use crate::api::v1::ServerConfig;
use crate::system::data_directory::DataDirectory;

mod llm_target;
#[cfg(unix)]
mod reconcile_reserved;
mod scheduled_external_policy;
pub mod typescript;
#[cfg(unix)]
mod update;

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
            return inspect_held(arguments.collect());
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
            return verify(arguments.collect());
        }
        #[cfg(unix)]
        if command == "update" {
            return update::run(arguments.collect());
        }
        if command == scheduled_external_policy::COMMAND {
            let directory = DataDirectory::from_environment()?;
            return scheduled_external_policy::run(&directory, arguments.collect());
        }
        if command == llm_target::COMMAND {
            if arguments.next().is_some() {
                return Err(llm_target::Error::Usage.into());
            }
            let target = llm_target::default_target()?;
            let data_directory = DataDirectory::from_environment()?;
            eprintln!("OCG data directory: {}", data_directory.root().display());
            return Ok(llm_target::register(&data_directory, target)?);
        }
        return Err(format!("unknown command `{command}`; see `ocg help`").into());
    }
    let server_config = ServerConfig::from_environment()?;
    let data_directory = DataDirectory::from_environment()?;
    eprintln!("OCG data directory: {}", data_directory.root().display());
    crate::system::runtime::run_server(&data_directory, true, server_config, |_| {
        until_signalled()
    })?;
    Ok(())
}

async fn until_signalled() {
    let receiver = ntex::rt::signals::signal();
    // ntex queues signal subscription on the primary runtime. A subsequent task
    // on that same FIFO queue completes only after the subscription is installed.
    let barrier = ntex::rt::System::current().handle().spawn(async {});
    if barrier.await.is_err() {
        log::error!("signal subscription barrier failed");
        return;
    }
    eprintln!("OCG signal listener ready");
    let signals = receiver.await;
    log::info!("OCG received {signals:?}; shutting down");
}

fn usage() -> &'static str {
    "usage: ocg [command] [arguments]\n\
     \n\
     Without command: run the OCG host (reads OCG_DATA_DIR, OCG_API_PORT, OCG_API_TOKEN, OCG_UI_DIR)\n\
     \n\
     Commands:\n\
       update [--check] [--version X.Y.Z] [--yes]\n\
       inspect-held <data-directory>            (read-only)\n\
       verify <data-directory> [--pages]\n\
       reconcile-reserved <data-directory> <job> --confirm-no-start | --finalize\n\
       reconcile-held <data-directory> --dry-run | --apply <plan-digest> | --finalize\n\
       scheduled-external-policy <create|list|show|update|enable|disable|revoke> ...\n\
       register-llm\n\
       help, --help, -h\n\
       --version, -V"
}

fn inspect_held(arguments: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let [directory] = arguments.as_slice() else {
        return Err("usage: ocg inspect-held <data-directory>".into());
    };
    let report = crate::db::inspect_held_allocations_offline(std::path::Path::new(directory))
        .map_err(crate::db::DatabaseError::Read)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn verify(arguments: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let (directory, pages) = match arguments.as_slice() {
        [directory] => (directory, false),
        [directory, flag] if flag == "--pages" => (directory, true),
        _ => return Err("usage: ocg verify <data-directory> [--pages]".into()),
    };
    let verification = crate::db::verify_offline(std::path::Path::new(directory), pages)
        .map_err(crate::db::DatabaseError::Read)?;
    println!("{}", serde_json::to_string_pretty(&verification.report)?);
    if !verification.passed {
        return Err("offline verification failed".into());
    }
    Ok(())
}
