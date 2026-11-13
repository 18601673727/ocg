use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use super::manual_host::{Failure, refused};
use crate::api::v1::ControlPlaneConfig;
use crate::system::data_directory::DataDirectory;
use crate::system::execution_controls::ExecutionControl;
use crate::system::loopback_activation::{
    ActivationMode, ActivationRequest, Confirmation, DatabaseIdentity, Preflight, QueuedAllowlist,
    preflight, record_state_reread,
};

pub(super) const COMMAND: &str = "loopback-scheduling";

const USAGE: &str = "usage: ocg loopback-scheduling preflight --data-dir <path> [--database <device:inode> [--allow-job <job-id>]...]\n       ocg loopback-scheduling serve --data-dir <path> --database <device:inode> --confirm-first-activation --confirm-loopback-only [--confirm-challenge <ocg1-...>]\n       ocg loopback-scheduling serve --data-dir <path> --database <device:inode> --confirm-queued-work --allow-job <job-id> [--allow-job <job-id>]... (1 to 16) --confirm-loopback-only [--confirm-challenge <ocg1-...>]";

enum Command {
    Preflight {
        data_directory: PathBuf,
        database: Option<DatabaseIdentity>,
        mode: ActivationMode,
    },
    Serve {
        data_directory: PathBuf,
        database: DatabaseIdentity,
        challenge: Option<String>,
        mode: ActivationMode,
    },
}

fn parse(arguments: &[String]) -> Result<Command, Failure> {
    let mut iterator = arguments.iter();
    let verb = iterator.next().ok_or(USAGE)?;
    let (mut data_directory, mut database) = (None, None);
    let (mut first, mut queued_work, mut loopback) = (false, false, false);
    let mut allowed: Vec<String> = Vec::new();
    let mut challenge = None;
    while let Some(flag) = iterator.next() {
        match flag.as_str() {
            "--confirm-first-activation" => {
                if std::mem::replace(&mut first, true) {
                    return Err(USAGE.into());
                }
            }
            "--confirm-queued-work" => {
                if std::mem::replace(&mut queued_work, true) {
                    return Err(USAGE.into());
                }
            }
            "--allow-job" => {
                let value = iterator.next().ok_or(USAGE)?;
                if value.is_empty() {
                    return Err(USAGE.into());
                }
                allowed.push(value.to_owned());
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
    let allowlist = |allowed: &[String]| {
        QueuedAllowlist::parse(allowed)
            .map(ActivationMode::QueuedWork)
            .map_err(|detail| refused("queued_work_allowlist", &detail))
    };
    match verb.as_str() {
        "preflight" if !first && !queued_work && !loopback && challenge.is_none() => {
            let mode = if allowed.is_empty() {
                ActivationMode::FirstActivation
            } else if database.is_some() {
                allowlist(&allowed)?
            } else {
                return Err(USAGE.into());
            };
            Ok(Command::Preflight {
                data_directory,
                database,
                mode,
            })
        }
        "serve" if loopback && first && !queued_work && allowed.is_empty() => Ok(Command::Serve {
            data_directory,
            database: database.ok_or(USAGE)?,
            challenge,
            mode: ActivationMode::FirstActivation,
        }),
        "serve" if loopback && queued_work && !first => Ok(Command::Serve {
            data_directory,
            database: database.ok_or(USAGE)?,
            challenge,
            mode: allowlist(&allowed)?,
        }),
        _ => Err(USAGE.into()),
    }
}

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
            mode,
        } => run_preflight(&data_directory, database, mode),
        Command::Serve {
            data_directory,
            database,
            challenge,
            mode,
        } => run_serve(data_directory, database, challenge, mode),
    }
}

fn print_report(report: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(report).unwrap_or_default()
    );
}

fn run_preflight(
    named: &Path,
    database: Option<DatabaseIdentity>,
    mode: ActivationMode,
) -> Result<(), Failure> {
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
        mode,
    });
    let Preflight { report, .. } = preflight(&data_directory, request.as_ref());
    print_report(&report);
    Ok(())
}

fn run_serve(
    named: PathBuf,
    database: DatabaseIdentity,
    supplied_challenge: Option<String>,
    mode: ActivationMode,
) -> Result<(), Failure> {
    if !ExecutionControl::AutonomousScheduling.is_enabled() {
        return Err(refused(
            "control_disabled",
            &ExecutionControl::AutonomousScheduling.refusal(),
        ));
    }
    let control = ControlPlaneConfig::from_environment()?;
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
        mode,
    };

    let mut preliminary = preflight(&data_directory, Some(&request));
    if let Some(refusal) = preliminary.refusal.take() {
        print_report(&preliminary.report);
        return Err(refused(refusal.code, &refusal.detail));
    }

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
        None => ask(&challenge, &request.mode)?,
    };
    if confirmed.trim() != challenge || state.is_empty() {
        let mut report = preliminary.report;
        report["verdict"] = serde_json::Value::String("refused".to_owned());
        report["refusal"] = serde_json::json!({
            "code": "activation_confirmation",
            "detail": format!(
                "the confirmation did not name this database state: the challenge for {} at open_epoch {} is {challenge}; {} needs the operator to confirm the state a preflight just printed, in this process",
                named.display(),
                report.get("database_epoch").cloned().unwrap_or(serde_json::Value::Null),
                request.mode.subject(),
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

fn ask(challenge: &str, mode: &ActivationMode) -> Result<String, Failure> {
    if !std::io::stdin().is_terminal() {
        return Err(refused(
            "confirmation_required",
            &format!(
                "no terminal to confirm on: pass --confirm-challenge {challenge} (read from `ocg loopback-scheduling preflight` for this database, whose state this run must still find unchanged), or run serve from a terminal"
            ),
        ));
    }
    eprintln!(
        "Confirm {} of autonomous loopback scheduling by typing this challenge:\n\n  {challenge}\n",
        mode.subject()
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
