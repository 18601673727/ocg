use crate::db::ObservationChange;
use crate::db::commands::Command;
use crate::db::commands::{
    byte_count, debug_bytes, json_bytes, output_bytes, search_index_bytes, text_bytes,
};
use crate::db::entities::JobId;
use crate::db::entities::{
    AppendedOutput, JobFinalization, JobOutput, Metrics, OperationKind, ProviderAnswer,
    ProviderRequestRecord, RequestNumber, ToolCallRef, ToolEvidence, ToolEvidenceRecord,
    ToolOutcome, Violation,
};
use crate::db::errors::DatabaseError;
use crate::db::handoff::{
    DevelopmentHandoff, DevelopmentHandoffError, DevelopmentOutputGuard,
    DevelopmentUsageCheckpoint, DevelopmentWriteProof, PreparedDevelopmentHandoff,
    classify_unstarted, development_contains_secret,
};
use crate::db::redb::StorageErrorKind;
use crate::db::worker::{DatabaseHandle, run};
use crate::db::{entities, mutations, redb};
use crate::support::cancellation::CancellationToken;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError};

const CANCELLED_BEFORE_CLAIM: &str = "execution cancelled before claim";

fn finalization_bytes(finalization: &JobFinalization) -> u64 {
    let result = finalization.result();
    output_bytes(
        result.summary.as_deref(),
        result.code.as_deref(),
        result.value.as_ref(),
    )
}

impl DatabaseHandle {
    pub(crate) async fn provider_responses(
        &self,
        job: JobId,
        after: Option<RequestNumber>,
        limit: usize,
        max_text_bytes: usize,
    ) -> Result<crate::db::entities::ProviderResponsesPage, DatabaseError> {
        self.read(move |database| {
            redb::read_provider_responses(database, job, after, limit, max_text_bytes)
        })
        .await
    }

    pub async fn provider_request(
        &self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<Option<ProviderRequestRecord>, DatabaseError> {
        self.read(move |database| redb::read_provider_request(database, job, request))
            .await
    }

    pub async fn tool_evidence(
        &self,
        job: JobId,
        after: Option<(RequestNumber, u32)>,
        limit: usize,
    ) -> Result<Vec<ToolEvidenceRecord>, DatabaseError> {
        self.read(move |database| redb::read_tool_evidence(database, job, after, limit))
            .await
    }

    pub(crate) async fn development_trial_job(
        &self,
        job: entities::JobId,
    ) -> Result<Option<(entities::MutableJobRecord, Option<entities::PoolAllocation>)>, DatabaseError>
    {
        self.read(move |database| redb::read_trial_job(database, job))
            .await
    }

    pub(crate) async fn load_handoff_credentials(
        &self,
        prepared: &mut PreparedDevelopmentHandoff,
    ) -> Result<(), DevelopmentHandoffError> {
        let references: Vec<String> = prepared
            .credential_references()
            .into_iter()
            .map(str::to_owned)
            .collect();
        for reference in references {
            if !crate::db::entities::valid_credential_ref(&reference) {
                return Err(classify_unstarted(DatabaseError::Rejected(
                    Violation::InvalidValue("credential", "invalid credential reference"),
                )));
            }
            let key = self.shared.credential_key();
            let lookup = reference.clone();
            let resolved = self
                .read(move |database| {
                    redb::read_handoff_credential(database, &lookup, key.as_deref())
                })
                .await
                .map_err(|error| match &error {
                    DatabaseError::Read(storage)
                        if storage.kind == StorageErrorKind::Other
                            && storage.os_error.is_none() =>
                    {
                        DevelopmentHandoffError::NotStarted {
                            cause: entities::UnstartedCause::FrozenSecretUnavailable,
                            detail: error.to_string(),
                        }
                    }
                    _ => classify_unstarted(error),
                })?;
            let Some((secret, encrypted)) = resolved else {
                return Err(classify_unstarted(DatabaseError::Rejected(
                    Violation::InvalidValue("credential", "stored credential is unavailable"),
                )));
            };
            prepared.credentials.push(reference, secret, encrypted);
        }
        Ok(())
    }

    pub(crate) async fn start_development_handoff(
        &self,
        prepared: PreparedDevelopmentHandoff,
        cancellation: &CancellationToken,
    ) -> Result<DevelopmentHandoff, DevelopmentHandoffError> {
        let owner = self.shared.clone();
        let result = self
            .write_sized(
                (prepared, owner, cancellation.clone()),
                |(prepared, _, _)| {
                    byte_count(crate::db::serialized_len(&prepared.request).unwrap_or(usize::MAX))
                        .saturating_add(64 * 1024)
                },
                OperationKind::ClaimExecution,
                |batch, (prepared, owner, cancellation)| {
                    if cancellation.is_cancelled() {
                        return batch
                            .reject(Violation::InvalidValue("handoff", CANCELLED_BEFORE_CLAIM));
                    }
                    let started = batch.start_mutable_job(&prepared)?;
                    Ok(DevelopmentHandoff {
                        prepared,
                        started,
                        owner,
                        output_busy: AtomicBool::new(false),
                        output_uncertain: AtomicBool::new(false),
                        usage_checkpoint: std::sync::Mutex::new(
                            DevelopmentUsageCheckpoint::default(),
                        ),
                    })
                },
            )
            .await;
        match result {
            Ok(handoff) => Ok(handoff),
            Err(error) => match &error {
                DatabaseError::Rejected(Violation::InvalidValue(
                    "handoff",
                    CANCELLED_BEFORE_CLAIM,
                )) => Err(DevelopmentHandoffError::CancelledBeforeClaim),
                DatabaseError::OutcomeUnknown { operation, .. } => {
                    let reconciliation =
                        self.reconcile_operation(*operation).await.map_err(Box::new);
                    Err(DevelopmentHandoffError::OutcomeUnknown {
                        error: Box::new(error),
                        reconciliation,
                    })
                }
                _ => Err(classify_unstarted(error)),
            },
        }
    }

    pub(crate) async fn append_development_output(
        &self,
        handoff: &DevelopmentHandoff,
        expected_seq: u32,
        output: JobOutput,
    ) -> Result<AppendedOutput, DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write_output(
                proof.token.job,
                (proof, expected_seq, output),
                |(_, _, output)| {
                    output_bytes(
                        output.summary.as_deref(),
                        output.code.as_deref(),
                        output.value.as_ref(),
                    )
                },
                OperationKind::AppendJobOutput,
                |batch, (proof, expected_seq, output)| {
                    batch.append_development_output(&proof, expected_seq, output)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    pub(crate) async fn record_development_request<S>(
        &self,
        handoff: &DevelopmentHandoff,
        body: &serde_json::Value,
        cancellation: &CancellationToken,
        prepare: impl FnOnce() -> Result<(entities::SentRequest, S), DatabaseError>,
        remember: impl FnOnce(S),
    ) -> Result<(), DatabaseError> {
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        if handoff
            .prepared
            .credentials
            .spellings
            .as_ref()
            .is_some_and(|secret| development_contains_secret(body, secret))
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "request",
                "request evidence contains credential material",
            )));
        }
        let (sent, state) = prepare()?;
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let attachments = match &handoff.prepared.candidate {
            crate::db::entities::DevelopmentCandidate::Generative(context) => context
                .covered_inputs
                .iter()
                .flat_map(|input| input.attachments.iter())
                .chain(
                    context
                        .history
                        .iter()
                        .flat_map(|turn| turn.attachments.iter()),
                )
                .chain(context.attachments.iter())
                .cloned()
                .collect::<Vec<_>>(),
            crate::db::entities::DevelopmentCandidate::Decision(_) => Vec::new(),
        };
        let result = self
            .write_sized(
                (proof, sent, attachments, cancellation.clone()),
                |(_, sent, _, _)| {
                    sent.bodies.iter().fold(0u64, |size, body| {
                        size.saturating_add(byte_count(body.byte_length()))
                    })
                },
                OperationKind::RecordSentRequest,
                |batch, (proof, sent, attachments, cancellation)| {
                    if cancellation.is_cancelled() {
                        return batch.reject(Violation::InvalidValue(
                            "execution",
                            "execution cancelled before recording the Provider request",
                        ));
                    }
                    batch.record_development_sent_request(&proof, sent, &attachments)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        if result.is_ok() {
            remember(state);
        }
        result
    }

    pub(crate) async fn record_development_partial_usage(
        &self,
        handoff: &DevelopmentHandoff,
        request: entities::RequestNumber,
        usages: Vec<Metrics>,
    ) -> Result<(), DatabaseError> {
        if usages.is_empty() {
            return Ok(());
        }
        if let Some(secret) = &handoff.prepared.credentials.spellings
            && usages.iter().any(|metrics| {
                metrics
                    .measurements
                    .usage
                    .raw
                    .as_ref()
                    .is_some_and(|raw| development_contains_secret(raw, secret))
            })
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "usage",
                "usage evidence contains credential material",
            )));
        }
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let checkpoint = handoff
            .usage_checkpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .for_request(request);
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, request, usages, checkpoint),
                    reply,
                    |(_, _, usages, _)| {
                        usages.iter().fold(0u64, |size, metrics| {
                            size.saturating_add(json_bytes(metrics.measurements.usage.raw.as_ref()))
                        })
                    },
                    |(proof, request, usages, checkpoint), reply, database, operation, commit| {
                        let ids = std::cell::Cell::new(None);
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            OperationKind::ConsumeTokenUsage,
                            reply,
                            |batch| {
                                let (usage_ids, next_checkpoint) = batch
                                    .record_development_partial_usage(
                                        &proof, request, usages, checkpoint,
                                    )?;
                                ids.set(Some(usage_ids));
                                Ok(next_checkpoint)
                            },
                        );
                        if result.committed
                            && let Some(usage_ids) = ids.take()
                        {
                            for id in usage_ids {
                                result
                                    .observations
                                    .push(ObservationChange::Metrics(proof.token.job, id));
                            }
                        }
                        result
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result.map(|next_checkpoint| {
            *handoff
                .usage_checkpoint
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = next_checkpoint.of_request(request);
        })
    }

    pub(crate) async fn record_development_provider_answer(
        &self,
        handoff: &DevelopmentHandoff,
        answer: ProviderAnswer,
        usages: Vec<(u32, Metrics)>,
        remember: impl FnOnce(entities::ContentDigest),
    ) -> Result<(), DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let response: serde_json::Value =
            serde_json::from_slice(&answer.response.bytes).map_err(|_| {
                DatabaseError::Rejected(Violation::InvalidValue(
                    "response",
                    "invalid response JSON",
                ))
            })?;
        if let Some(secret) = &handoff.prepared.credentials.spellings {
            let observation = &answer.stat.observation;
            let failure_has_secret = observation.failure.as_ref().is_some_and(|f| {
                [
                    Some(f.message.as_str()),
                    f.provider_code.as_deref(),
                    f.response_body.as_deref(),
                    f.request_id.as_deref(),
                ]
                .into_iter()
                .flatten()
                .any(|s| secret.found_in(s))
            });
            if development_contains_secret(&response, secret)
                || answer
                    .measurements
                    .usage
                    .raw
                    .as_ref()
                    .is_some_and(|v| development_contains_secret(v, secret))
                || observation
                    .raw_finish_reason
                    .as_deref()
                    .is_some_and(|s| secret.found_in(s))
                || failure_has_secret
            {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "answer",
                    "answer evidence contains credential material",
                )));
            }
            if usages.iter().any(|(_, metrics)| {
                metrics
                    .measurements
                    .usage
                    .raw
                    .as_ref()
                    .is_some_and(|raw| development_contains_secret(raw, secret))
            }) {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "usage",
                    "usage evidence contains credential material",
                )));
            }
        }
        let request = answer.stat.request;
        let has_usages = !usages.is_empty();
        let checkpoint = handoff
            .usage_checkpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .for_request(request);
        let response_digest = answer.response.digest();
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, answer, usages, checkpoint),
                    reply,
                    |(_, answer, usages, _)| {
                        let base = byte_count(answer.response.byte_length())
                            .saturating_add(json_bytes(answer.measurements.usage.raw.as_ref()));
                        usages.iter().fold(base, |acc, (_, metrics)| {
                            acc.saturating_add(json_bytes(metrics.measurements.usage.raw.as_ref()))
                        })
                    },
                    |(proof, answer, usages, checkpoint), reply, database, operation, commit| {
                        let ids = std::cell::Cell::new(None);
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            OperationKind::RecordProviderAnswer,
                            reply,
                            |batch| {
                                let (metrics, availability, usage_ids, next_checkpoint) = batch
                                    .record_development_provider_answer(
                                        &proof, answer, usages, checkpoint,
                                    )?;
                                ids.set(Some((metrics, availability, usage_ids)));
                                Ok(next_checkpoint)
                            },
                        );
                        if result.committed
                            && let Some((metrics, availability, usage_ids)) = ids.take()
                        {
                            result
                                .observations
                                .push(ObservationChange::Metrics(proof.token.job, metrics));
                            result.observations.push(ObservationChange::Availability(
                                proof.token.job,
                                availability,
                            ));
                            for id in usage_ids {
                                result
                                    .observations
                                    .push(ObservationChange::Metrics(proof.token.job, id));
                            }
                        }
                        result
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result.map(|next_checkpoint| {
            remember(response_digest);
            if has_usages {
                *handoff
                    .usage_checkpoint
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = next_checkpoint.of_request(request);
            }
        })
    }

    pub(crate) async fn finalize_development_job(
        &self,
        handoff: &DevelopmentHandoff,
        expected_seq: u32,
        finalization: JobFinalization,
    ) -> Result<AppendedOutput, DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        if let Some(secret) = &handoff.prepared.credentials.spellings {
            let result = finalization.result();
            if result
                .summary
                .as_deref()
                .is_some_and(|s| secret.found_in(s))
                || result.code.as_deref().is_some_and(|s| secret.found_in(s))
                || result
                    .value
                    .as_ref()
                    .is_some_and(|v| development_contains_secret(v, secret))
            {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "outcome",
                    "result contains credential material",
                )));
            }
        }
        let result = self
            .write_output(
                proof.token.job,
                (proof, expected_seq, finalization),
                |(_, _, finalization)| finalization_bytes(finalization),
                OperationKind::FinalizeJob,
                |batch, (proof, expected_seq, finalization)| {
                    batch.finalize_development_job(&proof, expected_seq, finalization)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    pub(crate) async fn finalize_development_decision(
        &self,
        handoff: &DevelopmentHandoff,
        expected_seq: u32,
        result: entities::DecisionResult,
    ) -> Result<AppendedOutput, DatabaseError> {
        if !matches!(
            &handoff.prepared.candidate,
            crate::db::entities::DevelopmentCandidate::Decision(_)
        ) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "Decision handoff required",
            )));
        }
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let value = serde_json::to_value(&result).map_err(|_| {
            DatabaseError::Rejected(Violation::InvalidValue(
                "decision",
                "result cannot be serialized",
            ))
        })?;
        if handoff
            .prepared
            .credentials
            .spellings
            .as_ref()
            .is_some_and(|secret| development_contains_secret(&value, secret))
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "decision",
                "result contains credential material",
            )));
        }
        let result = self
            .write_output(
                proof.token.job,
                (proof, expected_seq, result),
                |(_, _, result)| debug_bytes(result),
                OperationKind::FinalizeJob,
                |batch, (proof, expected_seq, decision)| {
                    batch.finalize_development_decision(&proof, expected_seq, decision)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    pub(crate) async fn finalize_development_failure(
        &self,
        handoff: &DevelopmentHandoff,
        expected_seq: u32,
        finalization: JobFinalization,
        rejection: Option<(
            entities::RequestNumber,
            entities::Timestamp,
            entities::AvailabilityObservation,
        )>,
    ) -> Result<AppendedOutput, DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let token = proof.token;
        if let Some(secret) = &handoff.prepared.credentials.spellings {
            let result = finalization.result();
            let failure = rejection
                .as_ref()
                .and_then(|(_, _, observation)| observation.failure.as_ref());
            if [
                result.summary.as_deref(),
                result.code.as_deref(),
                failure.map(|failure| failure.message.as_str()),
                failure.and_then(|failure| failure.provider_code.as_deref()),
                failure.and_then(|failure| failure.response_body.as_deref()),
                failure.and_then(|failure| failure.request_id.as_deref()),
            ]
            .into_iter()
            .flatten()
            .any(|text| secret.found_in(text))
                || result
                    .value
                    .as_ref()
                    .is_some_and(|value| development_contains_secret(value, secret))
            {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "outcome",
                    "failure evidence contains credential material",
                )));
            }
        }
        let rejection =
            rejection.map(
                |(request, observed_at, observation)| entities::AvailabilityStat {
                    job: token.job,
                    request,
                    observed_at,
                    observation,
                },
            );
        let result = self
            .write_output(
                proof.token.job,
                (proof, expected_seq, finalization, rejection),
                |(_, _, finalization, rejection)| {
                    finalization_bytes(finalization).saturating_add(
                        rejection
                            .as_ref()
                            .map_or(0, |stat| debug_bytes(&stat.observation)),
                    )
                },
                OperationKind::FinalizeJob,
                |batch, (proof, expected_seq, finalization, rejection)| {
                    batch.finalize_development_failure(
                        &proof,
                        expected_seq,
                        finalization,
                        rejection,
                    )
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    pub(crate) async fn fail_unstarted_development_reservation(
        &self,
        token: entities::AllocationToken,
        cause: entities::UnstartedCause,
    ) -> Result<AppendedOutput, DatabaseError> {
        self.write_output(
            token.job,
            (token, cause),
            |(_, cause)| output_bytes(Some(cause.summary()), Some(cause.code()), None),
            OperationKind::FinalizeJob,
            |batch, (token, cause)| batch.fail_unstarted_reservation(token, cause),
        )
        .await
    }

    pub(crate) async fn record_development_uncertain(
        &self,
        handoff: &DevelopmentHandoff,
        cause: entities::UncertaintyReason,
    ) -> Result<(), DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write(
                0,
                OperationKind::FinalizeJob,
                (proof, cause),
                |batch, (proof, cause)| batch.record_development_uncertain(&proof, cause),
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    pub(crate) async fn development_execution_context(
        &self,
        token: entities::AllocationToken,
        requirements: &'static dyn entities::DevelopmentContextRequirements,
    ) -> Result<crate::db::entities::DevelopmentCandidate, DatabaseError> {
        self.read(move |database| {
            redb::read_development_execution_context(database, token, requirements)
        })
        .await
    }

    pub(crate) async fn apply_development_job_tool(
        &self,
        handoff: &DevelopmentHandoff,
        call: ToolCallRef,
        tool: entities::JobToolCall,
        retain_read_only_result: bool,
    ) -> Result<redb::JobToolApplied, DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write_sized(
                (proof, call, tool, retain_read_only_result),
                |(_, _, tool, _)| {
                    use entities::JobToolCall;
                    let search = match tool {
                        JobToolCall::JobPlan { steps, .. } => steps
                            .iter()
                            .map(|step| {
                                search_index_bytes(&step.objective).saturating_add(64 * 1024)
                            })
                            .sum(),
                        JobToolCall::JobAssign { objective, .. }
                        | JobToolCall::JobDispatch { objective, .. } => {
                            search_index_bytes(objective)
                        }
                        JobToolCall::JobMessage { text, .. } => search_index_bytes(text),
                        JobToolCall::JobLimits { .. }
                        | JobToolCall::JobAgents { .. }
                        | JobToolCall::JobWorkers { .. }
                        | JobToolCall::JobStop { .. }
                        | JobToolCall::JobClose { .. }
                        | JobToolCall::JobHold { .. }
                        | JobToolCall::JobUnhold { .. }
                        | JobToolCall::JobRetry { .. } => 0,
                    };
                    debug_bytes(tool)
                        .saturating_add(4096)
                        .saturating_add(search)
                        .saturating_add(match tool {
                            JobToolCall::JobAssign { objective, .. }
                            | JobToolCall::JobDispatch { objective, .. }
                                if objective.len() <= 64 * 1024 =>
                            {
                                byte_count(objective.len())
                                    .saturating_mul(6)
                                    .saturating_add(64 * 1024)
                            }
                            JobToolCall::JobMessage { text, .. } if text.len() <= 64 * 1024 => {
                                byte_count(text.len())
                                    .saturating_mul(6)
                                    .saturating_add(64 * 1024)
                            }
                            _ => 0,
                        })
                },
                OperationKind::ApplyJobTool,
                |batch, (proof, call, tool, retain)| {
                    batch.apply_job_tool(&proof, call, tool, retain)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    fn begin_development_write<'a>(
        &self,
        handoff: &'a DevelopmentHandoff,
    ) -> Result<(DevelopmentOutputGuard<'a>, DevelopmentWriteProof), DatabaseError> {
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        let token = handoff.prepared.candidate.allocation().token;
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write is pending or requires reconciliation",
            )));
        }
        let guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        Ok((guard, proof))
    }

    pub(crate) async fn record_development_tool_decision(
        &self,
        handoff: &DevelopmentHandoff,
        evidence: ToolEvidence,
    ) -> Result<(), DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write_sized(
                (proof, Box::new(evidence)),
                |(_, evidence)| {
                    let identity = byte_count(evidence.tool_call_id.len())
                        .saturating_add(byte_count(evidence.name.len()));
                    match evidence.decision.stored_admission() {
                        None => identity,
                        Some(admission) => identity
                            .saturating_add(byte_count(admission.handler_version.len()))
                            .saturating_add(byte_count(admission.parameters.len()))
                            .saturating_add(byte_count(admission.arguments.len())),
                    }
                },
                OperationKind::RecordToolDecision,
                |batch, (proof, evidence)| {
                    mutations::record_tool_decision(batch, &proof, *evidence)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    pub(crate) async fn record_development_tool_outcome(
        &self,
        handoff: &DevelopmentHandoff,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write_sized(
                (proof, call, Box::new(outcome)),
                |(_, _, outcome)| match outcome.as_ref() {
                    ToolOutcome::Completed { result } => byte_count(result.len()),
                    ToolOutcome::NotApplied { message } => byte_count(message.len()),
                    ToolOutcome::Uncertain { message, .. } => text_bytes(message.as_deref()),
                    ToolOutcome::CompletedWithheld { .. }
                    | ToolOutcome::CompletedElided { .. }
                    | ToolOutcome::NotInvoked { .. } => 0,
                },
                OperationKind::RecordToolOutcome,
                |batch, (proof, call, outcome)| {
                    mutations::record_tool_outcome(batch, &proof, call, *outcome)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }
}
