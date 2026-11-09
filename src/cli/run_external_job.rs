//! `ocg run-external-job <job> --pool <id> --executor <id> --endpoint <https-url>
//!  --credential-ref <ref> --model <id> --trusted-http-proxy <http://loopback:port>
//!  --max-request-bytes <n> --max-output-tokens <n> --timeout-secs <n>
//!  --confirm-external-request`
//!
//! Runs one explicitly named, ordinary, submitted, tool-free native Generative
//! Job once against one approved HTTPS Provider endpoint, through one named
//! trusted loopback HTTP CONNECT proxy. It is not an HTTP route, is refused
//! unless the `manual_external_execution` control is enabled in the build
//! (the `manual_external_trial` and `manual_loopback_execution` controls do
//! not stand in for it), runs against the existing database in `OCG_DATA_DIR`
//! as a host of its own (so it cannot run beside another host), and runs
//! exactly one Job once: no retry, no recovery, no scheduling, no tools.
//!
//! Every flag is required and nothing is defaulted or selected for the
//! operator. `--endpoint`, `--credential-ref` and `--model` are assertions
//! against the Job's frozen configuration, not overrides: a mismatch is
//! refused before anything is reserved. The API key is never an argument; the
//! Job's frozen credential reference names the stored secret. The connection
//! goes through the named proxy only and never falls back to a direct one;
//! the environment's proxy variables and the system proxy are never used.
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
use crate::system::manual_execution::Target;
use crate::system::manual_external::{ExternalJobRequest, run_external_job};

pub(super) const COMMAND: &str = "run-external-job";

const USAGE: &str = "usage: ocg run-external-job <job-id> --pool <id> --executor <id> --endpoint <https-url> --credential-ref <ref> --model <id> --trusted-http-proxy <http://127.0.0.1:port> --max-request-bytes <n> --max-output-tokens <n> --timeout-secs <n> --confirm-external-request";

struct Arguments {
    job: JobId,
    pool: ExecutionPoolId,
    executor: ExecutorId,
    endpoint: String,
    credential_ref: String,
    model: String,
    proxy: Option<String>,
    max_request_bytes: usize,
    max_output_tokens: u32,
    timeout_secs: u64,
}

fn parse(arguments: &[String]) -> Result<Arguments, Failure> {
    let mut iterator = arguments.iter();
    let job = JobId::new(uuid(iterator.next().ok_or(USAGE)?, "Job")?);
    let (mut pool, mut executor, mut endpoint, mut credential_ref) = (None, None, None, None);
    let (mut model, mut proxy) = (None, None);
    let (mut request_bytes, mut output_tokens, mut timeout) = (None, None, None);
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
            "--model" => {
                slot(model.is_some())?;
                model = Some(value.clone());
            }
            "--trusted-http-proxy" => {
                slot(proxy.is_some())?;
                proxy = Some(value.clone());
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
        model: model.ok_or(USAGE)?,
        proxy,
        max_request_bytes: request_bytes.ok_or(USAGE)?,
        max_output_tokens: output_tokens.ok_or(USAGE)?,
        timeout_secs: timeout.ok_or(USAGE)?,
    })
}

pub fn run(arguments: Vec<String>) -> Result<(), Failure> {
    let arguments = parse(&arguments)?;
    if !ExecutionControl::ManualExternalExecution.is_enabled() {
        return Err(refused(
            "control_disabled",
            &ExecutionControl::ManualExternalExecution.refusal(),
        ));
    }
    let Some(proxy) = arguments.proxy.as_deref() else {
        return Err(refused(
            "proxy_required",
            "--trusted-http-proxy is required: manual external execution connects only through a named trusted HTTP proxy, never directly",
        ));
    };
    let proxy = parse_trusted_proxy(proxy).map_err(|error| refused(error.code, &error.detail))?;
    let limits = ApprovalLimits::new(
        arguments.max_request_bytes,
        arguments.max_output_tokens,
        Duration::from_secs(arguments.timeout_secs),
    )
    .map_err(|error| refused(error.code, &error.detail))?;
    let approval = ExternalApproval::manual_execution(
        &arguments.endpoint,
        &arguments.credential_ref,
        &arguments.model,
        proxy,
        limits,
    )
    .map_err(|error| refused(error.code, &error.detail))?;

    let data_directory = existing_data_directory("a manual external execution")?;
    let mut network = external_network_json(&approval);
    if let Some(network) = network.as_object_mut() {
        network.insert(
            "authority".to_owned(),
            ExecutionControl::ManualExternalExecution.name().into(),
        );
    }
    let request = ExternalJobRequest {
        target: Target {
            job: arguments.job,
            pool: arguments.pool,
            executor: arguments.executor,
        },
        approval,
    };
    let (finished, hosted) = host_once(&data_directory, move |database, cancellation| async move {
        run_external_job(&database, request, cancellation).await
    });
    conclude_external(finished, hosted, "execution", |report| {
        external_report_json(report, &network)
    })
}
