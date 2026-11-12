//! `ocg loopback-scheduling preflight --data-dir <path> [--database <id>]`
//! `ocg loopback-scheduling serve --data-dir <path> --database <id>
//!  --confirm-first-activation --confirm-loopback-only
//!  [--confirm-challenge <ocg1-...>]`
//!
//! The operator's explicit, per-run activation of autonomous loopback
//! scheduling; see [`crate::system::loopback_activation`].
//!
//! `preflight` reads the named existing database **without opening it for
//! writing**: no host, no worker, no `open_epoch` bump, no `clean_shutdown`
//! change, no directory, lock file or log created, no derived index rebuilt.
//! It prints the activation preflight report, including the challenge a
//! `serve` of exactly this database state must be given, and exits. It arms
//! nothing.
//!
//! `serve` refuses on its own read of the database whenever that read already
//! proves a first activation impossible, so such a refusal never opens
//! anything. Only then does it open the host, and only the preflight inside
//! that host, under its exclusive lock, arms it.
//!
//! A static command line is not an authorization. The confirmation flags say
//! what the operator intends; the challenge says which database state, opened
//! which time, this process confirmed. The challenge covers `open_epoch`,
//! which every opening increments, so it names one opening and cannot be
//! reused by the next one: a unit file, alias or script that carries a
//! challenge printed earlier is refused after any opening, including its own.
//! When stdin is a terminal the challenge is asked for there; otherwise it
//! must be passed with `--confirm-challenge`, and the operator has to read it
//! from a `preflight` of the state the host is about to open.
//!
//! What this does not do is identify the operator, or prove that a person
//! gave the confirmation. `preflight` prints the challenge for the state it
//! reads, so any process able to run these commands over this data directory
//! can obtain a fresh challenge and pass it to `serve`, and a noninteractive
//! caller can read the expected challenge out of a refusal and retry with it.
//! The gate therefore records a per-opening, state-bound confirmation, not
//! human presence, and it does not stop a local wrapper from confirming and
//! re-arming unattended. What it authorizes is a local command-execution
//! authority: whoever can run `serve` this way could already run arbitrary
//! commands as this user over these files.
//!
//! Neither command creates a database. A build without the
//! `autonomous_scheduling` control refuses `serve` before opening anything.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use super::manual_host::{Failure, refused};
use crate::api::v1::ControlPlaneConfig;
use crate::system::data_directory::DataDirectory;
use crate::system::execution_controls::ExecutionControl;
use crate::system::loopback_activation::{
    ActivationRequest, Confirmation, DatabaseIdentity, Preflight, preflight, record_state_reread,
};

pub(super) const COMMAND: &str = "loopback-scheduling";

const USAGE: &str = "usage: ocg loopback-scheduling preflight --data-dir <path> [--database <device:inode>]\n       ocg loopback-scheduling serve --data-dir <path> --database <device:inode> --confirm-first-activation --confirm-loopback-only [--confirm-challenge <ocg1-...>]";

enum Command {
    Preflight {
        data_directory: PathBuf,
        database: Option<DatabaseIdentity>,
    },
    Serve {
        data_directory: PathBuf,
        database: DatabaseIdentity,
        challenge: Option<String>,
    },
}

fn parse(arguments: &[String]) -> Result<Command, Failure> {
    let mut iterator = arguments.iter();
    let verb = iterator.next().ok_or(USAGE)?;
    let (mut data_directory, mut database) = (None, None);
    let (mut first, mut loopback) = (false, false);
    let mut challenge = None;
    while let Some(flag) = iterator.next() {
        match flag.as_str() {
            "--confirm-first-activation" => {
                if std::mem::replace(&mut first, true) {
                    return Err(USAGE.into());
                }
            }
            "--confirm-loopback-only" => {
                if std::mem::replace(&mut loopback, true) {
                    return Err(USAGE.into());
                }
            }
            "--confirm-challenge" => {
                let value = iterator.next().ok_or(USAGE)?;
                if challenge.replace(value.to_owned()).is_some() || value.is_empty() {
                    return Err(USAGE.into());
                }
            }
            "--data-dir" => {
                let value = iterator.next().ok_or(USAGE)?;
                if data_directory.replace(PathBuf::from(value)).is_some() || value.is_empty() {
                    return Err(USAGE.into());
                }
            }
            "--database" => {
                let value = iterator.next().ok_or(USAGE)?;
                let identity = DatabaseIdentity::parse(value).ok_or(
                    "the database identity must be <device>:<inode> as `loopback-scheduling preflight` prints it",
                )?;
                if database.replace(identity).is_some() {
                    return Err(USAGE.into());
                }
            }
            _ => return Err(USAGE.into()),
        }
    }
    let data_directory = data_directory.ok_or(USAGE)?;
    match verb.as_str() {
        "preflight" if !first && !loopback && challenge.is_none() => Ok(Command::Preflight {
            data_directory,
            database,
        }),
        "serve" if first && loopback => Ok(Command::Serve {
            data_directory,
            database: database.ok_or(USAGE)?,
            challenge,
        }),
        _ => Err(USAGE.into()),
    }
}

/// The named directory must already hold a database: a mistyped path must
/// not become a new one, and a preflight over nothing proves nothing. Nothing
/// here creates, chmods or otherwise touches the directory.
fn existing(named: &Path) -> Result<DataDirectory, Failure> {
    if !named.is_dir() {
        return Err(refused(
            "no_database",
            &format!(
                "{} is not an existing data directory; loopback-scheduling never creates one",
                named.display()
            ),
        ));
    }
    if !named.join(crate::db::DATABASE_FILE_NAME).is_file() {
        return Err(refused(
            "no_database",
            &format!(
                "{} holds no database; loopback-scheduling never creates one",
                named.display()
            ),
        ));
    }
    Ok(DataDirectory::existing(named.to_path_buf())?)
}

pub(super) fn run(arguments: Vec<String>) -> Result<(), Failure> {
    match parse(&arguments)? {
        Command::Preflight {
            data_directory,
            database,
        } => run_preflight(&data_directory, database),
        Command::Serve {
            data_directory,
            database,
            challenge,
        } => run_serve(data_directory, database, challenge),
    }
}

fn print_report(report: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(report).unwrap_or_default()
    );
}

fn run_preflight(named: &Path, database: Option<DatabaseIdentity>) -> Result<(), Failure> {
    let data_directory = existing(named)?;
    let request = database.map(|database| ActivationRequest {
        data_directory: named.to_path_buf(),
        database,
        confirmation: Confirmation {
            challenge: String::new(),
            open_epoch: 0,
            clean_shutdown: false,
            state: String::new(),
        },
    });
    // No host, no worker, no opening: a read of the file, nothing else.
    let Preflight { report, .. } = preflight(&data_directory, request.as_ref());
    print_report(&report);
    Ok(())
}

fn run_serve(
    named: PathBuf,
    database: DatabaseIdentity,
    supplied_challenge: Option<String>,
) -> Result<(), Failure> {
    if !ExecutionControl::AutonomousScheduling.is_enabled() {
        return Err(refused(
            "control_disabled",
            &ExecutionControl::AutonomousScheduling.refusal(),
        ));
    }
    // Refused before anything opens when it is given but invalid.
    let control = ControlPlaneConfig::from_environment()?;
    // Refused without opening anything when the directory or the database
    // cannot even be read read-only.
    let data_directory = existing(&named)?;
    let mut request = ActivationRequest {
        data_directory: named.clone(),
        database,
        confirmation: Confirmation {
            challenge: String::new(),
            open_epoch: 0,
            clean_shutdown: false,
            state: String::new(),
        },
    };

    // The advisory reading: it decides nothing, but a refusal it already
    // proves refuses here, before this process writes anything to the
    // database or creates any directory, lock file or log.
    let mut preliminary = preflight(&data_directory, Some(&request));
    if let Some(refusal) = preliminary.refusal.take() {
        print_report(&preliminary.report);
        return Err(refused(refusal.code, &refusal.detail));
    }

    // The confirmation: a challenge for exactly this state, at exactly this
    // open_epoch, produced in this process.
    let challenge = preliminary
        .report
        .get("confirmation_challenge")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let state = preliminary
        .report
        .get("confirmation_state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let confirmed = match supplied_challenge {
        Some(supplied) => supplied,
        None => ask(&challenge)?,
    };
    if confirmed.trim() != challenge || state.is_empty() {
        let mut report = preliminary.report;
        report["verdict"] = serde_json::Value::String("refused".to_owned());
        report["refusal"] = serde_json::json!({
            "code": "activation_confirmation",
            "detail": format!(
                "the confirmation did not name this database state: the challenge for {} at open_epoch {} is {challenge}; a first activation needs the operator to confirm the state a preflight just printed, in this process",
                named.display(),
                report.get("database_epoch").cloned().unwrap_or(serde_json::Value::Null),
            ),
        });
        report["checks"] = serde_json::json!([{
            "check": "activation_confirmation",
            "pass": false,
            "detail": "the supplied confirmation is not the challenge for this state",
        }]);
        print_report(&report);
        return Err(refused(
            "activation_confirmation",
            "the confirmation does not name this database state",
        ));
    }
    request.confirmation = Confirmation {
        challenge,
        open_epoch: preliminary
            .report
            .get("database_epoch")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        clean_shutdown: preliminary
            .report
            .get("clean_shutdown")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        state: state.clone(),
    };

    eprintln!(
        "OCG data directory: {} (autonomous loopback activation requested)",
        data_directory.root().display()
    );
    let captured: std::rc::Rc<std::cell::RefCell<Option<serde_json::Value>>> = Default::default();
    let slot = captured.clone();
    let ran = crate::system::runtime::run_activated(
        &data_directory,
        control,
        request,
        move |report| {
            // An armed run reports before it serves; a refusal reports after
            // its host has stopped, with the scope it actually changed.
            if report["verdict"] == "armed" {
                print_report(report);
            } else {
                *slot.borrow_mut() = Some(report.clone());
            }
        },
        |_context| async {
            let signals = ntex::rt::signals::signal().await;
            log::info!("OCG received {signals:?}; shutting down the armed loopback host");
        },
    );
    // The host has closed the database, so read it again: the report says
    // what the refusal did and did not change, verified rather than assumed.
    if let Some(mut report) = captured.borrow_mut().take() {
        let reread = preflight(&data_directory, None).report;
        let unchanged = reread
            .get("confirmation_state")
            .and_then(serde_json::Value::as_str)
            .map(|after| after == state);
        record_state_reread(&mut report, Some(&reread), unchanged);
        print_report(&report);
    }
    if let Err(error) = ran {
        return Err(refused("host_failed", &error.to_string()));
    }
    Ok(())
}

/// Asks for the challenge on the terminal. An interactive terminal is where
/// an operator usually is; it is not proof that one is there, and this does
/// not claim otherwise: any process can write to standard input, and the
/// challenge, not the terminal, is what binds the confirmation to a database
/// state and one opening.
fn ask(challenge: &str) -> Result<String, Failure> {
    if !std::io::stdin().is_terminal() {
        return Err(refused(
            "confirmation_required",
            &format!(
                "no terminal to confirm on: pass --confirm-challenge {challenge} (read from `ocg loopback-scheduling preflight` for this database, whose state this run must still find unchanged), or run serve from a terminal"
            ),
        ));
    }
    eprintln!(
        "Confirm the first autonomous loopback activation by typing this challenge:\n\n  {challenge}\n"
    );
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|error| {
        refused(
            "confirmation_required",
            &format!("the challenge could not be read: {error}"),
        )
    })?;
    Ok(line.trim().to_owned())
}
