use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use serde_json::json;

use crate::db::DatabaseHandle;
use crate::db::entities::{AllocationPhase, OutcomeKind};
use crate::service::ai::providers::{ApprovedRoute, CancellationToken, LoopbackProxy};
use crate::system::data_directory::{DATA_DIRECTORY_VARIABLE, DataDirectory};
use crate::system::external_approval::ExternalApproval;
use crate::system::manual_execution::{ExecutionReport, Stop, outcome_name, phase_name};
use crate::system::runtime::HostError;

pub(super) type Failure = Box<dyn std::error::Error>;

pub(super) fn uuid(text: &str, what: &str) -> Result<u128, Failure> {
    uuid::Uuid::parse_str(text)
        .map(|id| id.as_u128())
        .map_err(|_| format!("the {what} must be a UUID").into())
}

pub(super) fn refused(code: &str, detail: &str) -> Failure {
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "outcome": "refused",
            "code": code,
            "detail": detail,
            "request_sent": false,
            "job_state_changed": false,
        }))
        .unwrap_or_default()
    );
    format!("refused: {code}").into()
}

pub(super) fn existing_data_directory(never_creates: &str) -> Result<DataDirectory, Failure> {
    let root = std::env::var_os(DATA_DIRECTORY_VARIABLE)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| refused("data_directory", "OCG_DATA_DIR must be set"))?;
    if !std::path::Path::new(&root)
        .join(crate::db::DATABASE_FILE_NAME)
        .is_file()
    {
        return Err(refused(
            "no_database",
            &format!("OCG_DATA_DIR holds no database; {never_creates} never creates one"),
        ));
    }
    Ok(DataDirectory::bootstrap(root)?)
}

pub(super) fn host_once<T, F>(
    data_directory: &DataDirectory,
    run: impl FnOnce(DatabaseHandle, CancellationToken) -> F + 'static,
) -> (Option<T>, Result<(), HostError>)
where
    T: 'static,
    F: Future<Output = T> + 'static,
{
    let result: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
    let slot = result.clone();
    let hosted =
        crate::system::runtime::run_hosted(data_directory, true, move |context| async move {
            use ntex::util::{Either, select};
            let cancellation = CancellationToken::default();
            let database = context.database().clone();
            let execution = run(database, cancellation.clone());
            let mut execution = std::pin::pin!(execution);
            let finished = match select(execution.as_mut(), ntex::rt::signals::signal()).await {
                Either::Left(finished) => finished,
                Either::Right(_) => {
                    cancellation.cancel();
                    execution.await
                }
            };
            *slot.borrow_mut() = Some(finished);
        });
    let finished = result.borrow_mut().take();
    (finished, hosted)
}

pub(super) fn external_network_json(approval: &ExternalApproval) -> serde_json::Value {
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
        "note": "reported by the operating system; not proof of which program the process is",
    })
}

pub(super) fn external_report_json(
    report: &ExecutionReport,
    network: &serde_json::Value,
) -> serde_json::Value {
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

pub(super) fn conclude_external(
    finished: Option<Result<ExecutionReport, Stop>>,
    hosted: Result<(), HostError>,
    what: &str,
    render: impl FnOnce(&ExecutionReport) -> serde_json::Value,
) -> Result<(), Failure> {
    let Some(finished) = finished else {
        return Err(match hosted {
            Err(error) => refused(
                "host_failed",
                &format!("the database could not be opened or closed cleanly: {error}"),
            ),
            Ok(()) => format!("the {what} did not run").into(),
        });
    };
    if let Err(error) = hosted {
        eprintln!("the database could not be closed cleanly after the {what}: {error}");
    }
    match finished {
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&render(&report))?);
            if report.outcome == Some(OutcomeKind::Completed) {
                Ok(())
            } else {
                Err(format!("the {what} did not complete; see the report").into())
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
