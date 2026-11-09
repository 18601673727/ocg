//! `ocg external-trial <job> --pool <id> --executor <id> --endpoint <https-url>
//!  --credential-ref <ref> --max-request-bytes <n> --max-output-tokens <n>
//!  --timeout-secs <n> [--model <id>] [--trusted-http-proxy <http://loopback:port>]
//!  --confirm-external-request`
//!
//! The one operator-invoked path that may send one Job to an approved
//! external HTTPS endpoint. It is not an HTTP route, is refused unless the
//! `manual_external_trial` control is enabled in the build, runs against the
//! database in `OCG_DATA_DIR` as a host of its own (so it cannot run beside
//! another host), and runs exactly one Job once: no retry, no recovery, no
//! scheduling. Every flag but `--model` and `--trusted-http-proxy` is required;
//! nothing is defaulted. Without `--trusted-http-proxy` the connection is
//! direct; with it, the connection goes through that one loopback HTTP CONNECT
//! proxy and never falls back to a direct one. The environment's proxy
//! variables and the system proxy are never used.
//!
//! Interrupting it (SIGINT or SIGTERM) cancels the local exchange. A request
//! that may have reached the Provider then stays held as Uncertain: stopping
//! the local transport says nothing about the Provider.

use std::time::Duration;

use serde_json::json;

use super::manual_host::{Failure, existing_data_directory, host_once, refused, uuid};
use crate::db::entities::{AllocationPhase, ExecutionPoolId, ExecutorId, JobId, OutcomeKind};
use crate::service::ai::providers::{ApprovedRoute, LoopbackProxy};
use crate::system::execution_controls::ExecutionControl;
use crate::system::external_trial::{
    TrialApproval, TrialError, TrialLimits, TrialRequest, parse_trusted_proxy, run_external_trial,
};
use crate::system::manual_execution::{ExecutionReport, outcome_name, phase_name};

const USAGE: &str = "usage: ocg external-trial <job-id> --pool <id> --executor <id> --endpoint <https-url> --credential-ref <ref> --max-request-bytes <n> --max-output-tokens <n> --timeout-secs <n> [--model <id>] [--trusted-http-proxy <http://127.0.0.1:port>] --confirm-external-request";

struct Arguments {
    job: JobId,
    pool: ExecutionPoolId,
    executor: ExecutorId,
    endpoint: String,
    credential_ref: String,
    max_request_bytes: usize,
    max_output_tokens: u32,
    timeout_secs: u64,
    model: Option<String>,
    proxy: Option<String>,
}

fn parse(arguments: &[String]) -> Result<Arguments, Failure> {
    let mut iterator = arguments.iter();
    let job = iterator.next().ok_or(USAGE)?;
    let job = JobId::new(uuid(job, "Job")?);
    let (mut pool, mut executor, mut endpoint, mut credential_ref) = (None, None, None, None);
    let (mut request_bytes, mut output_tokens, mut timeout) = (None, None, None);
    let (mut model, mut proxy) = (None, None);
    let mut confirmed = false;
    while let Some(flag) = iterator.next() {
        if flag == "--confirm-external-request" {
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
            "--endpoint" => {
                slot(endpoint.is_some())?;
                endpoint = Some(value.clone());
            }
            "--credential-ref" => {
                slot(credential_ref.is_some())?;
                credential_ref = Some(value.clone());
            }
            "--max-request-bytes" => {
                slot(request_bytes.is_some())?;
                request_bytes = Some(value.parse::<usize>().map_err(|_| USAGE)?);
            }
            "--max-output-tokens" => {
                slot(output_tokens.is_some())?;
                output_tokens = Some(value.parse::<u32>().map_err(|_| USAGE)?);
            }
            "--timeout-secs" => {
                slot(timeout.is_some())?;
                timeout = Some(value.parse::<u64>().map_err(|_| USAGE)?);
            }
            "--model" => {
                slot(model.is_some())?;
                model = Some(value.clone());
            }
            "--trusted-http-proxy" => {
                slot(proxy.is_some())?;
                proxy = Some(value.clone());
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
        endpoint: endpoint.ok_or(USAGE)?,
        credential_ref: credential_ref.ok_or(USAGE)?,
        max_request_bytes: request_bytes.ok_or(USAGE)?,
        max_output_tokens: output_tokens.ok_or(USAGE)?,
        timeout_secs: timeout.ok_or(USAGE)?,
        model,
        proxy,
    })
}

/// What the connection guarantees, stated for the path this build uses. The
/// ambient proxy, if any is configured, is never used.
fn network_json(approval: &TrialApproval) -> serde_json::Value {
    let ambient = crate::system::http::ambient_proxy_configured();
    match approval.route() {
        ApprovedRoute::Direct => json!({
            "path": "direct",
            "destination": approval.destination_text(),
            "ambient_proxy_configured": ambient,
            "ambient_proxy_used": false,
            "ocg_validated_destination_address": "yes: OCG resolves the name itself, requires every address to be public and not a fake-IP substitute, and connects only to those addresses",
            "tls": "certificate verified against the bundled public roots for exactly the approved host; redirects are not followed",
            "not_established": "that the resolved address belongs to the Provider you intend: the certificate check binds the host name, not the operator",
        }),
        ApprovedRoute::TrustedProxy(proxy) => json!({
            "path": "trusted_http_connect_proxy",
            "destination": approval.destination_text(),
            "proxy": proxy.address().to_string(),
            "proxy_listener": observe_listener(*proxy),
            "ambient_proxy_configured": ambient,
            "ambient_proxy_used": false,
            "direct_fallback": false,
            "local_lookup_of_provider_name": false,
            "connect_request": "CONNECT <approved host>:<approved port> only; no credential, header or body",
            "ocg_validated_destination_address": "no: the proxy resolves the name and chooses the outbound route; OCG cannot see or prove the address reached",
            "tls": "after a 2xx CONNECT answer, TLS is negotiated inside the tunnel with the approved host name and verified against the bundled public roots; redirects are not followed",
            "not_established": "the final destination address, and that the proxy is the program you intend: a listener name is an observation, not proof",
        }),
    }
}

/// What the operating system reports about the process listening on the
/// proxy port. It is an observation for the operator, never an authorization:
/// a process can use any name, and a listener can change between this look
/// and the connection.
fn observe_listener(proxy: LoopbackProxy) -> serde_json::Value {
    let port = proxy.address().port();
    let output = std::process::Command::new("/usr/sbin/lsof")
        .args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-Fpc"])
        .output();
    let Ok(output) = output else {
        return json!({"status": "unavailable", "detail": "lsof could not be run"});
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let mut listeners: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        if let Some(pid) = line.strip_prefix('p') {
            listeners.push((pid.to_owned(), String::new()));
        } else if let (Some(name), Some(last)) = (line.strip_prefix('c'), listeners.last_mut()) {
            last.1 = name.chars().take(64).collect();
        }
    }
    listeners.sort();
    listeners.dedup();
    if listeners.is_empty() {
        return json!({
            "status": "no_listener_visible",
            "detail": "no listening process on this port is visible to this user",
        });
    }
    json!({
        "status": "observed",
        "processes": listeners
            .iter()
            .map(|(pid, name)| json!({"pid": pid, "name": name}))
            .collect::<Vec<_>>(),
        "note": "reported by the operating system; not proof that the process is Surge",
    })
}

fn report_json(report: &ExecutionReport, network: &serde_json::Value) -> serde_json::Value {
    let remote = if report.started {
        "unknown: OCG cannot prove from a returned, interrupted or refused exchange whether the Provider did work or charged"
    } else {
        "none: no request was sent"
    };
    json!({
        "job": report.job.to_string(),
        "allocation_phase": phase_name(report.allocation_phase),
        "capacity_still_held": report.allocation_phase != AllocationPhase::Released,
        "started": report.started,
        "job_outcome": outcome_name(report.outcome),
        "outcome_detail": report.outcome_code,
        "exchange_returned_result": report.exchange_returned_result,
        "exchange_detail": report.exchange_detail,
        "remote_work": remote,
        "network": network,
        "next": if report.allocation_phase == AllocationPhase::Released {
            "the Allocation is released; inspect the Job's outputs and events"
        } else {
            "the Allocation stays held by design; run `ocg inspect-held` and adjudicate manually; nothing is retried or released automatically"
        },
    })
}

pub fn run(arguments: Vec<String>) -> Result<(), Failure> {
    let arguments = parse(&arguments)?;
    if !ExecutionControl::ManualExternalTrial.is_enabled() {
        return Err(refused(
            "control_disabled",
            &ExecutionControl::ManualExternalTrial.refusal(),
        ));
    }
    let limits = TrialLimits::new(
        arguments.max_request_bytes,
        arguments.max_output_tokens,
        Duration::from_secs(arguments.timeout_secs),
    )
    .map_err(|error| refused(error.code, &error.detail))?;
    let mut approval = TrialApproval::new(&arguments.endpoint, &arguments.credential_ref, limits)
        .map_err(|error| refused(error.code, &error.detail))?;
    if let Some(model) = arguments.model.as_deref() {
        if model.is_empty()
            || model.len() > 128
            || !model.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(refused(
                "model_invalid",
                "the model id must be 1..=128 visible ASCII characters",
            ));
        }
        approval = approval.with_model(model);
    }
    if let Some(proxy) = arguments.proxy.as_deref() {
        let proxy =
            parse_trusted_proxy(proxy).map_err(|error| refused(error.code, &error.detail))?;
        approval = approval.with_trusted_proxy(proxy);
    }

    let data_directory = existing_data_directory("a trial")?;
    let network = network_json(&approval);
    let request = TrialRequest {
        job: arguments.job,
        pool: arguments.pool,
        executor: arguments.executor,
        approval,
    };
    let (finished, hosted) = host_once(&data_directory, move |database, cancellation| async move {
        run_external_trial(&database, request, cancellation).await
    });
    let Some(finished) = finished else {
        // The trial never ran: the host did not open, so nothing was sent.
        return Err(match hosted {
            Err(error) => refused(
                "host_failed",
                &format!("the database could not be opened or closed cleanly: {error}"),
            ),
            Ok(()) => "the trial did not run".into(),
        });
    };
    if let Err(error) = hosted {
        // The trial ran and the host then failed to close cleanly. The result
        // below is what the trial recorded; the close failure is reported with
        // it, never instead of it.
        eprintln!("the database could not be closed cleanly after the trial: {error}");
    }
    match finished {
        Ok(report) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report_json(&report, &network))?
            );
            if report.outcome == Some(OutcomeKind::Completed) {
                Ok(())
            } else {
                Err("the trial did not complete; see the report".into())
            }
        }
        Err(TrialError::Refused(refusal)) => Err(refused(refusal.code, &refusal.detail)),
        Err(TrialError::Held { stage, detail }) => {
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
