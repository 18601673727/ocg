use std::time::{Duration, Instant};

use crate::ai::providers::{CancellationToken, ProviderExecutionConfig};
use crate::db::entities::{
    AllocationPhase, AllocationToken, ExecutionOperation, JobId, OutcomeKind, PoolAllocation,
    SCHEDULED_EXTERNAL_MAX_TIMEOUT, UnstartedCause,
};
use crate::db::{DatabaseError, DatabaseHandle, DevelopmentHandoffError};
use crate::jobs::authorization::scheduled::ScheduledExternalGrant;
use crate::jobs::execution::runtime::{
    EndpointPolicy, WorkspaceTools, supervise_development_decision,
    supervise_development_generative,
};
use crate::jobs::execution::stops::NativeStops;
use crate::jobs::execution::tools::native::NativeTooling;

const DISPATCH_TIMEOUT: Duration = SCHEDULED_EXTERNAL_MAX_TIMEOUT;
const MAX_RESPONSE_BYTES: usize = 1 << 20;

#[derive(Debug)]
pub(crate) struct Dispatched {
    pub(crate) job: JobId,
    pub(crate) allocation_phase: Option<AllocationPhase>,
    pub(crate) started: bool,
    pub(crate) outcome: Option<OutcomeKind>,
    pub(crate) detail: String,
}

impl Dispatched {
    pub(crate) fn resolved(&self) -> bool {
        match self.allocation_phase {
            Some(AllocationPhase::Released) => self.outcome.is_some(),
            Some(AllocationPhase::Uncertain) => true,
            Some(
                AllocationPhase::Reserved
                | AllocationPhase::Running
                | AllocationPhase::StopRequested,
            )
            | None => false,
        }
    }

    pub(super) fn log(&self) {
        let phase = self.allocation_phase.map_or("unknown", |p| match p {
            AllocationPhase::Reserved => "reserved",
            AllocationPhase::Running => "running",
            AllocationPhase::StopRequested => "stop_requested",
            AllocationPhase::Uncertain => "uncertain",
            AllocationPhase::Released => "released",
        });
        let outcome = match self.outcome {
            None => "none",
            Some(OutcomeKind::Completed) => "completed",
            Some(OutcomeKind::Failed) => "failed",
            Some(OutcomeKind::Cancelled) => "cancelled",
            Some(OutcomeKind::Unknown) => "unknown",
        };
        let started = if self.started {
            "started"
        } else {
            "never started"
        };
        match self.allocation_phase {
            Some(AllocationPhase::Released) => log::info!(
                "native scheduler: Job {} {started}, {outcome}, Allocation released ({})",
                self.job,
                self.detail
            ),
            _ => log::warn!(
                "native scheduler: Job {} {started}, {outcome}, Allocation {phase} and held ({})",
                self.job,
                self.detail
            ),
        }
    }
}

fn prepared_native_policy(
    prepared: &crate::db::PreparedDevelopmentHandoff,
) -> Result<EndpointPolicy<'static>, &'static str> {
    if let Some(decision) = prepared.decision_configuration() {
        let endpoint = match decision {
            crate::db::entities::FrozenDecisionConfig::Native(native) => native.endpoint.as_deref(),
            crate::db::entities::FrozenDecisionConfig::Generative(generative, _) => {
                generative.llm.provider.endpoint.as_deref()
            }
        };
        return EndpointPolicy::from_model_endpoint(endpoint.ok_or("endpoint_missing")?)
            .ok_or("endpoint_not_supported");
    }
    let Some(frozen) = prepared.generative_configuration() else {
        return Err("generative_required");
    };
    let refusal = if prepared.is_summary_producer() {
        frozen.provider_refusal()
    } else {
        frozen.native_execution_refusal()
    };
    if refusal.is_some() {
        return Err("not_single_request_generative");
    }
    let endpoint = frozen
        .llm
        .provider
        .endpoint
        .as_deref()
        .ok_or("endpoint_missing")?;
    let policy = EndpointPolicy::from_model_endpoint(endpoint).ok_or("endpoint_not_supported")?;
    let request = prepared.request();
    if request.get("tool_choice").is_some()
        || request.get("stream") != Some(&serde_json::Value::Bool(true))
    {
        return Err("request_not_tool_free_stream");
    }
    Ok(policy)
}

async fn within_deadline<F: std::future::Future>(
    cancellation: &CancellationToken,
    timeout: Duration,
    exchange: F,
) -> F::Output {
    let deadline = Instant::now() + timeout;
    let done = CancellationToken::default();
    let timer = ntex::rt::spawn({
        let cancellation = cancellation.clone();
        let done = done.clone();
        async move {
            use ntex::util::{Either, select};
            let wait_for_deadline = async {
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    // The timer wheel rounds long sleeps up by tens of seconds.
                    ntex::time::sleep(remaining.min(Duration::from_secs(5))).await;
                }
            };
            if let Either::Right(()) = select(done.cancelled(), wait_for_deadline).await {
                cancellation.cancel();
            }
        }
    });
    let output = exchange.await;
    done.cancel();
    drop(timer);
    output
}

fn protection_refused(error: &DatabaseError) -> bool {
    matches!(
        error,
        DatabaseError::DiskProtected(_) | DatabaseError::RefusedByDiskProtection { .. }
    )
}

async fn wait_for_protection_recovery(
    database: &DatabaseHandle,
    cancellation: &CancellationToken,
) -> bool {
    use ntex::util::{Either, select};

    let protection = database.watch_disk_protection();
    loop {
        if cancellation.is_cancelled() {
            return false;
        }
        if protection.status().lock.is_none() {
            return true;
        }
        match select(cancellation.cancelled(), protection.changed()).await {
            Either::Left(()) | Either::Right(None) => return false,
            Either::Right(Some(_)) => {}
        }
    }
}

pub(super) async fn dispatch(
    database: &DatabaseHandle,
    allocation: PoolAllocation,
    cancellation: &CancellationToken,
    workspace_tools: &WorkspaceTools,
    stops: &NativeStops,
) -> Dispatched {
    let token = allocation.token;
    let admitted = if cancellation.is_cancelled() {
        Err((
            UnstartedCause::Cancelled,
            "cancelled before the start".to_owned(),
        ))
    } else {
        match ScheduledExternalGrant::load(database, token.job).await {
            Ok(Some(_)) if allocation.operation == ExecutionOperation::Decision => Err((
                UnstartedCause::StartRefused,
                "Decision execution is not permitted under a scheduled external grant".to_owned(),
            )),
            Ok(grant) => Ok(grant),
            Err(error) => Err((
                UnstartedCause::StartRefused,
                format!(
                    "the scheduled external authorization was refused before the start ({error})"
                ),
            )),
        }
    };
    let detail = match admitted {
        Ok(grant) => {
            let config = grant.as_ref().map_or_else(
                || ProviderExecutionConfig {
                    timeout: DISPATCH_TIMEOUT,
                    first_delta_timeout: Some(DISPATCH_TIMEOUT),
                    max_output_bytes: MAX_RESPONSE_BYTES,
                    cancellation: cancellation.clone(),
                    ..ProviderExecutionConfig::default()
                },
                |grant| grant.provider_config(cancellation.clone()),
            );
            start_and_exchange(
                database,
                token,
                allocation.operation,
                config,
                grant.as_ref(),
                workspace_tools.data_directory(),
                stops,
            )
            .await
        }
        Err((cause, prefix)) => match database
            .fail_unstarted_development_reservation(token, cause)
            .await
        {
            Ok(_) => format!("{prefix}; the reservation was ended and nothing was sent"),
            Err(settle) => format!("{prefix}; the reservation was not ended here ({settle})"),
        },
    };
    let dispatched = reported(database, token, detail).await;
    if dispatched
        .outcome
        .is_some_and(|outcome| outcome != OutcomeKind::Unknown)
        && let Err(error) = database.settle_child_turns(vec![token.job]).await
    {
        log::warn!(
            "native scheduler: the settling of Job {} was not passed on ({error})",
            token.job
        );
    }
    dispatched
}

async fn start_and_exchange(
    database: &DatabaseHandle,
    token: AllocationToken,
    operation: ExecutionOperation,
    config: ProviderExecutionConfig,
    grant: Option<&ScheduledExternalGrant>,
    data_directory: &crate::host::data_directory::DataDirectory,
    stops: &NativeStops,
) -> String {
    let cancellation = &config.cancellation;
    let mut tooling = NativeTooling::new(data_directory, stops.clone(), cancellation.clone());
    let handoff = loop {
        let result = async {
            use crate::db::classify_unstarted;
            use crate::db::entities::{JobQueuePriority, Violation};
            use crate::jobs::{
                DevelopmentResolvedCandidate, DevelopmentToolContext, prepare_development_handoff,
                resolve_development_candidate,
            };
            let invalid = |reason| {
                classify_unstarted(DatabaseError::Rejected(Violation::InvalidValue(
                    "handoff", reason,
                )))
            };
            let snapshot = database
                .development_execution_context(token)
                .await
                .map_err(classify_unstarted)?;
            let resolved = resolve_development_candidate(database, snapshot).await?;
            let offer = if let DevelopmentResolvedCandidate::Generative(context) = &resolved
                && !context.snapshot.is_summary_work()
            {
                let context = &context.snapshot;
                tooling
                    .offer(
                        &context.configuration,
                        context.project.as_ref(),
                        DevelopmentToolContext {
                            decider: context.job.input.priority == JobQueuePriority::Foreground,
                            nested: context.conversation.parent_id.is_some(),
                            plan_step: context.plan_step,
                        },
                    )
                    .await
                    .map_err(invalid)?
            } else {
                None
            };
            let mut prepared = prepare_development_handoff(resolved, offer.as_ref())
                .map_err(classify_unstarted)?;
            database.load_handoff_credentials(&mut prepared).await?;
            let policy = match grant {
                None => prepared_native_policy(&prepared),
                Some(grant) => grant
                    .check_prepared(&prepared)
                    .map(|()| EndpointPolicy::ScheduledExternal(grant))
                    .map_err(|refusal| {
                        log::warn!(
                            "native scheduler: Job {} scheduled external start refused ({}): {}",
                            token.job,
                            refusal.code,
                            refusal.detail,
                        );
                        refusal.code
                    }),
            }
            .map_err(invalid)?;
            let handoff = database.start_development_handoff(prepared).await?;
            Ok((handoff, policy))
        }
        .await;
        match result {
            Err(DevelopmentHandoffError::Database(error)) if protection_refused(&error) => {
                if !wait_for_protection_recovery(database, cancellation).await {
                    return "protection interrupted the start; the reservation remains held".into();
                }
            }
            result => break result,
        }
    };
    match handoff {
        Ok((handoff, policy)) => {
            let tools = tooling.into_execution();
            let exchange = within_deadline(cancellation, DISPATCH_TIMEOUT, async {
                if operation == ExecutionOperation::Decision || handoff.decision_inputs().is_some()
                {
                    supervise_development_decision(database, handoff, &config, policy).await
                } else {
                    supervise_development_generative(database, handoff, &config, policy, tools)
                        .await
                }
            })
            .await;
            match exchange {
                Ok(_) => "the exchange returned and was recorded".to_owned(),
                Err(error) => error.to_string(),
            }
        }
        Err(DevelopmentHandoffError::NotStarted { cause, detail }) => {
            let cause = if cancellation.is_cancelled() {
                UnstartedCause::Cancelled
            } else {
                cause
            };
            let settled = loop {
                match database
                    .fail_unstarted_development_reservation(token, cause)
                    .await
                {
                    Err(error) if protection_refused(&error) => {
                        if !wait_for_protection_recovery(database, cancellation).await {
                            return "protection interrupted settlement; the reservation remains held".into();
                        }
                    }
                    result => break result,
                }
            };
            match settled {
                Ok(_) => format!(
                    "the start was refused before any request ({detail}); the reservation was ended"
                ),
                Err(settle) => format!(
                    "the start was refused before any request ({detail}); the reservation was not ended here ({settle})"
                ),
            }
        }
        Err(error) => format!("the start may have committed without a reply ({error:?})"),
    }
}

async fn reported(database: &DatabaseHandle, token: AllocationToken, detail: String) -> Dispatched {
    match database.development_trial_job(token.job).await {
        Ok(Some((record, Some(allocation)))) if allocation.token == token => {
            if allocation.phase == AllocationPhase::Released
                && record
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind != OutcomeKind::Unknown)
            {
                crate::workspace::process::stop_settled(token.job);
            }
            Dispatched {
                job: token.job,
                allocation_phase: Some(allocation.phase),
                started: allocation.started_at.is_some(),
                outcome: record.outcome.as_ref().map(|outcome| outcome.kind),
                detail,
            }
        }
        Ok(_) => Dispatched {
            job: token.job,
            allocation_phase: None,
            started: false,
            outcome: None,
            detail: format!("{detail}; the Allocation could not be found afterwards"),
        },
        Err(error) => Dispatched {
            job: token.job,
            allocation_phase: None,
            started: false,
            outcome: None,
            detail: format!("{detail}; the result could not be read ({error})"),
        },
    }
}
