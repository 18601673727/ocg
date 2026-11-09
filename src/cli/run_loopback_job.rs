//! `ocg run-loopback-job <job> --pool <id> --executor <id> --timeout-secs <n>
//!  --confirm-loopback-only`
//!
//! Runs one explicitly named, submitted, tool-free Generative Job once,
//! against the literal `127.0.0.1` endpoint the Job itself froze. It is the
//! operator's manual loopback execution and nothing else: it cannot reach an
//! external Provider (the Job's endpoint must be a literal loopback address,
//! and there is no flag to name another), does not discover, schedule, retry
//! or recover, and never selects a Job for the operator. It is refused unless
//! the `manual_loopback_execution` control is enabled in the build, and it
//! runs against the existing database in `OCG_DATA_DIR` as a host of its own,
//! so it cannot run beside another host. Every flag is required; nothing is
//! defaulted.
//!
//! Interrupting it (SIGINT or SIGTERM) cancels the local exchange. A request
//! that may have reached the Provider then stays held as Uncertain.

use std::time::Duration;

use serde_json::json;

use super::manual_host::{Failure, existing_data_directory, host_once, refused, uuid};
use crate::db::entities::{AllocationPhase, ExecutionPoolId, ExecutorId, JobId, OutcomeKind};
use crate::system::execution_controls::ExecutionControl;
use crate::system::manual_execution::{ExecutionReport, Stop, Target, outcome_name, phase_name};
use crate::system::manual_loopback::{LoopbackRequest, run_loopback_job};

const USAGE: &str = "usage: ocg run-loopback-job <job-id> --pool <id> --executor <id> --timeout-secs <n> --confirm-loopback-only";

struct Arguments {
    job: JobId,
    pool: ExecutionPoolId,
    executor: ExecutorId,
    timeout_secs: u64,
}

fn parse(arguments: &[String]) -> Result<Arguments, Failure> {
    let mut iterator = arguments.iter();
    let job = JobId::new(uuid(iterator.next().ok_or(USAGE)?, "Job")?);
    let (mut pool, mut executor, mut timeout) = (None, None, None);
    let mut confirmed = false;
    while let Some(flag) = iterator.next() {
        if flag == "--confirm-loopback-only" {
            if std::mem::replace(&mut confirmed, true) {
                return Err(USAGE.into());
            }
            continue;
        }
        let value = iterator.next().ok_or(USAGE)?;
        let slot = |present: bool| if present { Err(USAGE) } else { Ok(()) };
        match flag.as_str() {
            "--pool" => {
                slot(pool.is_some())?;
                pool = Some(ExecutionPoolId::new(uuid(value, "Pool")?));
            }
            "--executor" => {
                slot(executor.is_some())?;
                executor = Some(ExecutorId::new(uuid(value, "Executor")?));
            }
            "--timeout-secs" => {
                slot(timeout.is_some())?;
                timeout = Some(value.parse::<u64>().map_err(|_| USAGE)?);
            }
            _ => return Err(USAGE.into()),
        }
    }
    if !confirmed {
        return Err(USAGE.into());
    }
    Ok(Arguments {
        job,
        pool: pool.ok_or(USAGE)?,
        executor: executor.ok_or(USAGE)?,
        timeout_secs: timeout.ok_or(USAGE)?,
    })
}

fn report_json(report: &ExecutionReport) -> serde_json::Value {
    json!({
        "job": report.job.to_string(),
        "destination": "literal loopback only",
        "allocation_phase": phase_name(report.allocation_phase),
        "capacity_still_held": report.allocation_phase != AllocationPhase::Released,
        "started": report.started,
        "job_outcome": outcome_name(report.outcome),
        "outcome_detail": report.outcome_code,
        "exchange_returned_result": report.exchange_returned_result,
        "exchange_detail": report.exchange_detail,
        "next": if report.allocation_phase == AllocationPhase::Released {
            "the Allocation is released; inspect the Job's outputs and events"
        } else {
            "the Allocation stays held by design; run `ocg inspect-held` and adjudicate manually; nothing is retried or released automatically"
        },
    })
}

pub fn run(arguments: Vec<String>) -> Result<(), Failure> {
    let arguments = parse(&arguments)?;
    if !ExecutionControl::ManualLoopbackExecution.is_enabled() {
        return Err(refused(
            "control_disabled",
            &ExecutionControl::ManualLoopbackExecution.refusal(),
        ));
    }
    let request = LoopbackRequest::new(
        Target {
            job: arguments.job,
            pool: arguments.pool,
            executor: arguments.executor,
        },
        Duration::from_secs(arguments.timeout_secs),
    )
    .map_err(|stop| match stop {
        Stop::Refused { code, detail } => refused(code, &detail),
        Stop::Held { stage, .. } => format!("held at {stage}").into(),
    })?;
    let data_directory = existing_data_directory("a manual loopback execution")?;
    let (finished, hosted) = host_once(&data_directory, move |database, cancellation| async move {
        run_loopback_job(&database, request, cancellation).await
    });
    let Some(finished) = finished else {
        // It never ran: the host did not open, so nothing was sent.
        return Err(match hosted {
            Err(error) => refused(
                "host_failed",
                &format!("the database could not be opened or closed cleanly: {error}"),
            ),
            Ok(()) => "the execution did not run".into(),
        });
    };
    if let Err(error) = hosted {
        // It ran and the host then failed to close cleanly. The result below
        // is what it recorded; the close failure is reported with it, never
        // instead of it.
        eprintln!("the database could not be closed cleanly after the execution: {error}");
    }
    match finished {
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&report_json(&report))?);
            if report.outcome == Some(OutcomeKind::Completed) {
                Ok(())
            } else {
                Err("the execution did not complete; see the report".into())
            }
        }
        Err(Stop::Refused { code, detail }) => Err(refused(code, &detail)),
        Err(Stop::Held { stage, detail }) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "outcome": "held",
                    "stage": stage,
                    "detail": detail,
                    "next": "ownership may be held; run `ocg inspect-held` before anything else; nothing is retried or released automatically",
                }))?
            );
            Err(format!("held at {stage}").into())
        }
    }
}
