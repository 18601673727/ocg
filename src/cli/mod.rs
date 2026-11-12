use crate::api::v1::ControlPlaneConfig;
use crate::system::data_directory::DataDirectory;

mod external_trial;
mod inspect_held;
mod llm_target;
mod loopback_scheduling;
mod manual_host;
mod migrate;
#[cfg(unix)]
mod reconcile_reserved;
mod release_slot;
mod run_external_job;
mod run_loopback_job;
mod scheduled_external_policy;
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
        if command == "reconcile-held" {
            return reconcile_reserved::run_held(arguments.collect());
        }
        if command == "external-trial" {
            return external_trial::run(arguments.collect());
        }
        if command == run_external_job::COMMAND {
            return run_external_job::run(arguments.collect());
        }
        if command == "run-loopback-job" {
            return run_loopback_job::run(arguments.collect());
        }
        if command == loopback_scheduling::COMMAND {
            return loopback_scheduling::run(arguments.collect());
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
       run-external-job <job> --pool <id> --executor <id> --endpoint <https-url> --credential-ref <ref> --model <id>\n\
         --trusted-http-proxy <http://127.0.0.1:port> --max-request-bytes <n> --max-output-tokens <n> --timeout-secs <n>\n\
         --confirm-external-request\n\
         (runs one named submitted Generative Job once against its approved HTTPS endpoint through the named proxy;\n\
          refused unless the manual_external_execution control is enabled in the build)\n\
       loopback-scheduling preflight --data-dir <path> [--database <device:inode>]\n\
         (read-only activation preflight of an existing database: identity, queues, recurrences, controls, eligibility,\n\
          and the challenge a serve of exactly this database state must be given; opens nothing, creates nothing, arms nothing)\n\
       loopback-scheduling serve --data-dir <path> --database <device:inode> --confirm-first-activation --confirm-loopback-only\n\
         [--confirm-challenge <ocg1-...>]\n\
         (the one host armed for autonomous loopback scheduling over exactly that database; the flags alone authorize\n\
          nothing: serve needs the challenge a preflight of this state printed, and it names one opening, so a static\n\
          challenge cannot be replayed after any opening, its own included. That is a state-bound, per-opening\n\
          confirmation, not proof of a person: any local process that can run preflight can obtain a fresh challenge\n\
          and pass it to serve, so a local wrapper can re-arm unattended, and the authority involved is the local\n\
          ability to run this command as this user; refused unless the autonomous_scheduling control is enabled in\n\
          the build and the database holds no queued work, no Allocation and no loop that can create work)\n\
       inspect-held <data-directory>            (read-only)\n\
       verify <data-directory> [--pages]        (offline integrity check; never writes the original)\n\
       reconcile-reserved <data-directory> <job> --confirm-no-start | --finalize\n\
         (offline: ends one crash-surviving Reserved Allocation that never started; durably finalizes publication evidence)\n\
       reconcile-held <data-directory> --dry-run | --apply <plan-digest> | --finalize\n\
         (offline: releases only eligible stopped literal-loopback Allocations; preserves Unknown outcomes and evidence; finalizes without reapplying)\n\
       migrate [--check | --to <directory>]\n\
         (offline: migrates one stopped format 22 database to format 23 on a private copy; the source is never written)\n\
       scheduled-external-policy <create|list|show|update|enable|disable|revoke> ...\n\
         (durable scheduled external policy management; creating a policy never enables scheduled_external_transport or autonomous_scheduling)\n\
       register-llm\n\
       help, --help, -h\n\
       --version, -V"
}
