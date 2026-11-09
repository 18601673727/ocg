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

use super::manual_host::{
    Failure, conclude_external, existing_data_directory, external_network_json,
    external_report_json, host_once, refused, uuid,
};
use crate::db::entities::{ExecutionPoolId, ExecutorId, JobId};
use crate::system::execution_controls::ExecutionControl;
use crate::system::external_approval::{ApprovalLimits, ExternalApproval, parse_trusted_proxy};
use crate::system::external_trial::{TrialRequest, run_external_trial};
use crate::system::manual_execution::Target;

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

pub fn run(arguments: Vec<String>) -> Result<(), Failure> {
    let arguments = parse(&arguments)?;
    if !ExecutionControl::ManualExternalTrial.is_enabled() {
        return Err(refused(
            "control_disabled",
            &ExecutionControl::ManualExternalTrial.refusal(),
        ));
    }
    let limits = ApprovalLimits::new(
        arguments.max_request_bytes,
        arguments.max_output_tokens,
        Duration::from_secs(arguments.timeout_secs),
    )
    .map_err(|error| refused(error.code, &error.detail))?;
    let mut approval =
        ExternalApproval::trial(&arguments.endpoint, &arguments.credential_ref, limits)
            .map_err(|error| refused(error.code, &error.detail))?;
    if let Some(model) = arguments.model.as_deref() {
        approval = approval
            .with_model(model)
            .map_err(|error| refused(error.code, &error.detail))?;
    }
    if let Some(proxy) = arguments.proxy.as_deref() {
        let proxy =
            parse_trusted_proxy(proxy).map_err(|error| refused(error.code, &error.detail))?;
        approval = approval.with_trusted_proxy(proxy);
    }

    let data_directory = existing_data_directory("a trial")?;
    let network = external_network_json(&approval);
    let request = TrialRequest {
        target: Target {
            job: arguments.job,
            pool: arguments.pool,
            executor: arguments.executor,
        },
        approval,
    };
    let (finished, hosted) = host_once(&data_directory, move |database, cancellation| async move {
        run_external_trial(&database, request, cancellation).await
    });
    conclude_external(finished, hosted, "trial", |report| {
        external_report_json(report, &network)
    })
}
