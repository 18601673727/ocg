use crate::db::entities::{
    ChatFinishReason, ExecutionOperation, JobId, OutcomeKind, OutputKind, RequestNumber,
};
use crate::db::redb::batch::{Batch, BatchError, Failure};
use crate::db::redb::jobs;
use crate::db::redb::schema::{
    CHILDREN, JOB_OUTPUTS, JOBS, LIVE_CHILDREN, MESSAGES, POOL_ALLOCATIONS, REQUEST_EVENTS,
    TOOL_CALLS,
};
use crate::db::redb::{StorageError, TxRead};
use crate::db::redb::{allocations, rows, state};
use ::redb::ReadableTable;

pub(crate) fn confirmed_answer(
    tx: &impl TxRead,
    allocation: &crate::db::entities::PoolAllocation,
) -> Result<Option<crate::db::entities::FinalResult>, StorageError> {
    use crate::db::entities::{
        AllocationPhase, MutableFrozenConfiguration, MutableJobPhase, RequestPayload,
    };
    if allocation.phase != AllocationPhase::Running
        || allocation.operation != ExecutionOperation::LlmRequest
        || allocation.stop_confirmed_at.is_some()
        || allocation.stop_proof.is_some()
    {
        return Ok(None);
    }
    let job_id = allocation.token.job;
    let job = {
        let jobs = tx.table(JOBS)?;
        let Some(row) = jobs.get(job_id.get())? else {
            return Ok(None);
        };
        jobs::decode_mutable_job(job_id.get(), row.value())?
    };
    if job.phase != MutableJobPhase::Executing || job.revision != allocation.job_revision {
        return Ok(None);
    }
    let Some(MutableFrozenConfiguration::Generative(_)) = &job.frozen else {
        return Ok(None);
    };
    let Some(runtime) = state::read(tx, job_id)? else {
        return Ok(None);
    };
    if runtime.state.outcome.is_some() || runtime.pending != Some((0, 0)) {
        return Ok(None);
    }
    for entry in tx
        .table(TOOL_CALLS)?
        .range((job_id.get(), 0, 0)..=(job_id.get(), u32::MAX, u32::MAX))?
    {
        let (key, value) = entry?;
        let call = rows::tool_call_ref(key.value())?;
        let evidence = rows::decode_tool_evidence(call, value.value())?;
        if !evidence
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.fact.is_definite())
        {
            return Ok(None);
        }
    }
    for entry in tx
        .table(JOB_OUTPUTS)?
        .range((job_id.get(), 0)..=(job_id.get(), u32::MAX))?
    {
        let (_, value) = entry?;
        if rows::output_kind(value.value())? == OutputKind::Result {
            return Ok(None);
        }
    }
    let events = tx.table(REQUEST_EVENTS)?;
    let Some((last_key, _)) = events
        .range((job_id.get(), 0, 0)..=(job_id.get(), u32::MAX, u32::MAX))?
        .next_back()
        .transpose()?
    else {
        return Ok(None);
    };
    let last_request = last_key.value().1;
    let mut digest = None;
    let mut finish = None;
    let mut current_sent = false;
    let mut prior_request = None;
    let mut prior_received = false;
    let mut prior_success = false;
    for entry in events.range((job_id.get(), 0, 0)..=(job_id.get(), u32::MAX, u32::MAX))? {
        let (key, value) = entry?;
        let (_, number, seq) = key.value();
        let request = RequestNumber::new(number)
            .ok_or_else(|| StorageError::corrupted("zero request identity"))?;
        let event = rows::decode_request_event(job_id, request, seq, value.value())?;
        match event.payload {
            RequestPayload::Sent { .. } if seq == 1 && number == last_request => {
                current_sent = true;
            }
            RequestPayload::Sent { .. } if seq == 1 && number < last_request => {
                if prior_request.is_some() && (!prior_received || !prior_success) {
                    return Ok(None);
                }
                prior_request = Some(number);
                prior_received = false;
                prior_success = false;
            }
            RequestPayload::Received { .. }
                if number < last_request && prior_request == Some(number) && !prior_received =>
            {
                prior_received = true;
            }
            RequestPayload::Availability {
                status_code: Some(status),
                failure: None,
                ..
            } if number < last_request
                && prior_request == Some(number)
                && !prior_success
                && (200..300).contains(&status) =>
            {
                prior_success = true;
            }
            RequestPayload::Received { digest: value }
                if number == last_request && digest.is_none() =>
            {
                digest = Some(value);
            }
            RequestPayload::Availability {
                status_code: Some(status),
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason,
                failure: None,
            } if number == last_request && (200..300).contains(&status) && finish.is_none() => {
                finish = Some(raw_finish_reason);
            }
            RequestPayload::Measurement { .. } | RequestPayload::Token { .. } => {}
            RequestPayload::Sent { .. }
            | RequestPayload::Received { .. }
            | RequestPayload::Availability { .. } => return Ok(None),
        }
    }
    if prior_request.is_some() && (!prior_received || !prior_success) {
        return Ok(None);
    }
    let (Some(digest), Some(raw_finish), true) = (digest, finish, current_sent) else {
        return Ok(None);
    };
    let messages = tx.table(MESSAGES)?;
    let Some(body) = messages.get(digest.as_bytes().as_slice())? else {
        return Err(StorageError::corrupted(
            "confirmed Provider response is missing",
        ));
    };
    let value: serde_json::Value = serde_json::from_slice(body.value())
        .map_err(|_| StorageError::corrupted("invalid confirmed Provider response"))?;
    if value.get("role").and_then(serde_json::Value::as_str) != Some("assistant")
        || value.get("tool_calls").is_some_and(|calls| {
            !calls.is_null() && calls.as_array().is_none_or(|calls| !calls.is_empty())
        })
    {
        return Ok(None);
    }
    let text = match value.get("content") {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(_) => return Ok(None),
    };
    if let Some(source) = job.summary_source {
        let Ok(summary) = serde_json::from_str::<crate::db::entities::SummarizationResult>(&text)
        else {
            return Ok(None);
        };
        if summary.validate().is_err() || summary.source != source {
            return Ok(None);
        }
    }
    let mut result = serde_json::Map::new();
    result.insert("text".to_owned(), serde_json::Value::String(text.clone()));
    if let Some(reason) = raw_finish {
        result.insert(
            "finish_reason".to_owned(),
            serde_json::Value::String(reason),
        );
    }
    Ok(Some(crate::db::entities::FinalResult {
        summary: Some(text),
        code: None,
        goal_reached: None,
        value: Some(serde_json::Value::Object(result)),
    }))
}

pub(super) enum StartupSettlementError {
    Storage(StorageError),
    ClosingGroupRejected(JobId, crate::db::entities::Violation),
}

fn settlement_failure(batch: &Batch, error: BatchError) -> Failure {
    // Prefer any latched storage failure over the returned business rejection.
    batch
        .check_failure()
        .err()
        .filter(|error| matches!(&error.0, Failure::Storage(_)))
        .unwrap_or(error)
        .0
}

pub(super) fn settle_startup(
    batch: &mut Batch,
    deferred_groups: &std::collections::BTreeSet<JobId>,
) -> Result<(), StartupSettlementError> {
    // Every held Allocation must be released by confirmed-answer settlement before
    // Group recovery can defer queued work. Retrying a Group repeats these releases.
    settle_confirmed_answers(batch)?;
    finish_closing_groups(batch, deferred_groups)?;
    batch.check_failure().map_err(|error| match error.0 {
        Failure::Storage(error) => StartupSettlementError::Storage(error),
        Failure::Rejected(reason) => StartupSettlementError::Storage(StorageError::corrupted(
            format!("startup settlement latched a rejection without an owning object: {reason}"),
        )),
    })
}

fn settle_confirmed_answers(batch: &mut Batch) -> Result<(), StartupSettlementError> {
    let candidates = batch
        .step(|tx| {
            let mut found = Vec::new();
            for entry in tx.open_table(POOL_ALLOCATIONS)?.iter()? {
                let (key, value) = entry?;
                let allocation = allocations::decode_pool_allocation(key.value(), value.value())?;
                if !allocation.holds_capacity() {
                    continue;
                }
                let job = allocation.token.job;
                let result = confirmed_answer(tx, &allocation)
                    .map_err(|mut error| {
                        error.detail = format!("read confirmed answer for {job}: {}", error.detail);
                        error
                    })?
                    .ok_or_else(|| {
                        StorageError::corrupted(format!(
                            "{job} still holds capacity without a confirmed answer; inspect with `ocg inspect-held <data-directory>`"
                        ))
                    })?;
                found.push((allocation, result));
            }
            Ok(found)
        })
        .map_err(|error| startup_read_error("scan confirmed answers", error))?;
    for (allocation, result) in candidates {
        let job = allocation.token.job;
        if let Err(error) = batch.settle_confirmed_answer(allocation, result) {
            let error = match settlement_failure(batch, error) {
                Failure::Storage(mut error) => {
                    error.detail = format!(
                        "startup settlement of confirmed answer for {job}: {}",
                        error.detail
                    );
                    error
                }
                Failure::Rejected(reason) => StorageError::corrupted(format!(
                    "startup settlement of confirmed answer for {job} was rejected: {reason}. Capacity remains held; inspect with `ocg inspect-held <data-directory>`"
                )),
            };
            return Err(StartupSettlementError::Storage(error));
        }
    }
    Ok(())
}

fn startup_read_error(operation: &str, error: BatchError) -> StartupSettlementError {
    let error = match error.0 {
        Failure::Storage(mut error) => {
            error.detail = format!("startup settlement {operation}: {}", error.detail);
            error
        }
        Failure::Rejected(reason) => StorageError::corrupted(format!(
            "startup settlement {operation} unexpectedly rejected a read: {reason}"
        )),
    };
    StartupSettlementError::Storage(error)
}

impl Batch {
    fn settle_confirmed_answer(
        &mut self,
        allocation: crate::db::entities::PoolAllocation,
        mut result: crate::db::entities::FinalResult,
    ) -> Result<(), BatchError> {
        let job_id = allocation.token.job;
        let Some(job) = self.mutable_job(job_id)? else {
            return self.corrupted(format!("confirmed answer Job {job_id} is missing"));
        };
        self.normalize_summary_output(&job, &mut result)?;
        self.settle_mutable_outcome(allocation.token, &job, OutcomeKind::Completed, result)?;
        let mut released = allocation.clone();
        released.phase = crate::db::entities::AllocationPhase::Released;
        self.put_pool_allocation(&released, Some(&allocation))?;
        #[cfg(test)]
        if INJECT_CONFIRMED_ANSWER_REJECTION.with(std::cell::Cell::get) {
            return self.reject(crate::db::entities::Violation::InvalidValue(
                "confirmed_answer",
                "injected settlement rejection",
            ));
        }
        self.settle_summary_membership(&job)?;
        self.settle_standalone_decision(&job)?;
        self.freed_capacity = true;
        Ok(())
    }
}

fn finish_closing_groups(
    batch: &mut Batch,
    deferred_groups: &std::collections::BTreeSet<JobId>,
) -> Result<(), StartupSettlementError> {
    use std::ops::Bound::{Excluded, Unbounded};
    let mut after = None;
    loop {
        let next = batch
            .step(|tx| {
                let groups = tx.open_table(LIVE_CHILDREN)?;
                let entry = groups
                    .range((after.map_or(Unbounded, Excluded), Unbounded))?
                    .next()
                    .transpose()?;
                Ok(entry.map(|(key, _)| key.value()))
            })
            .map_err(|error| startup_read_error("scan closing Groups", error))?;
        let Some(key) = next else {
            return Ok(());
        };
        after = Some(key);
        let parent = JobId::new(key.0);
        let group = JobId::new(key.1);
        if deferred_groups.contains(&group) {
            continue;
        }
        let spec = batch
            .job(group)
            .map_err(|error| startup_read_error("load closing Group", error))?
            .ok_or_else(|| {
                StartupSettlementError::Storage(StorageError::corrupted(format!(
                    "LIVE_CHILDREN of Group {parent} references missing Group {group}"
                )))
            })?;
        if spec.parent_id != Some(parent) {
            return Err(StartupSettlementError::Storage(StorageError::corrupted(
                format!("LIVE_CHILDREN of Group {parent} disagrees with parent of Group {group}"),
            )));
        }
        if spec.is_chunk_group()
            && let Err(error) = batch.recover_chunk_group(&spec)
        {
            return Err(match settlement_failure(batch, error) {
                Failure::Storage(mut error) => {
                    error.detail = format!(
                        "startup settlement of closing Chunk Group {group}: {}",
                        error.detail
                    );
                    StartupSettlementError::Storage(error)
                }
                Failure::Rejected(reason) => {
                    // A deferred Group is retried on the next start, so it may only
                    // wait while it owns no capacity. Confirmed answers were settled
                    // earlier in this transaction and startup refused every other
                    // held Allocation, so this can only fail if an invariant broke.
                    if group_holds_capacity(&batch.tx, group).map_err(|error| {
                        StartupSettlementError::Storage(StorageError::corrupted(format!(
                            "cannot prove closing Chunk Group {group} holds no capacity: {error}"
                        )))
                    })? {
                        return Err(StartupSettlementError::Storage(StorageError::corrupted(
                            format!(
                                "closing Chunk Group {group} was rejected and still holds capacity: {reason}. Capacity remains held; inspect with `ocg inspect-held <data-directory>`"
                            ),
                        )));
                    }
                    StartupSettlementError::ClosingGroupRejected(group, reason)
                }
            });
        }
    }
}

/// Whether any member of `group` still owns capacity in this transaction.
fn group_holds_capacity(tx: &::redb::WriteTransaction, group: JobId) -> Result<bool, StorageError> {
    let children = tx.open_table(CHILDREN)?;
    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
    for entry in children.range((group.get(), 0)..=(group.get(), u128::MAX))? {
        let (key, _) = entry?;
        let member = key.value().1;
        if let Some(row) = allocations.get(member)?
            && allocations::decode_pool_allocation(member, row.value())?.holds_capacity()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
thread_local! {
    static INJECT_CONFIRMED_ANSWER_REJECTION: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;
    use std::path::PathBuf;

    use ::redb::ReadableDatabase;
    use uuid::Uuid;

    use super::INJECT_CONFIRMED_ANSWER_REJECTION;
    use crate::db::StorageErrorKind;
    use crate::db::entities::{
        AgentConfig, AllocationExecutor, AllocationPool, ChatFinishReason, ComposerContextId,
        ConfigBody, ConnectionConfig, ContentDigest, DevelopmentReservation,
        DevelopmentReservationRequest, ExecutionConfiguration, ExecutionOperation, ExecutionPoolId,
        ExecutorId, JobQueuePriority, LlmParameters, LlmTransport, ModelConfig, MutableJobContent,
        MutableJobInput, RequestEventId, RequestEventRecord, RequestFingerprint, RequestNumber,
        RequestPayload, ReservationRequestId, Submission, SubmissionKey,
    };
    use crate::db::redb::Commit;
    use crate::db::redb::allocations;
    use crate::db::redb::batch::run_batch_with;
    use crate::db::redb::recovery::{database_file, open};
    use crate::db::redb::rows;
    use crate::db::redb::schema::{JOB_OUTPUTS, MESSAGES, POOL_ALLOCATIONS, REQUEST_EVENTS};
    use crate::db::redb::state;
    use crate::host::data_directory::DataDirectory;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn data_directory(&self) -> DataDirectory {
            DataDirectory::bootstrap(self.0.clone().into_os_string()).expect("bootstrap data dir")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct FaultGuard;

    impl FaultGuard {
        fn enable() -> Self {
            INJECT_CONFIRMED_ANSWER_REJECTION.with(|flag| flag.set(true));
            Self
        }
    }

    impl Drop for FaultGuard {
        fn drop(&mut self) {
            INJECT_CONFIRMED_ANSWER_REJECTION.with(|flag| flag.set(false));
        }
    }

    #[test]
    fn confirmed_answer_settlement_rejection_refuses_startup_and_rolls_back() {
        let temp = TempDir::new("ocg-confirmed-reject");
        let _directory = temp.data_directory();

        let opened = open(&temp.0).expect("initial open");
        let outcome = run_batch_with(&opened.database, opened.epoch, Commit::Durable, |batch| {
            let pool_id = ExecutionPoolId::new(Uuid::now_v7().as_u128());
            let executor_id = ExecutorId::new(Uuid::now_v7().as_u128());
            batch.register_allocation_pool(AllocationPool {
                id: pool_id,
                name: "default".to_owned(),
                capacity: NonZeroU32::new(1).expect("non-zero capacity"),
                enabled: true,
                archived: false,
                revision: 1,
            })?;
            batch.register_allocation_executor(AllocationExecutor {
                id: executor_id,
                pool: pool_id,
                name: "exec-1".to_owned(),
                enabled: true,
                archived: false,
                revision: 1,
                capabilities: BTreeSet::from([(
                    ExecutionOperation::LlmRequest,
                    Some(crate::db::entities::LlmTransport::OpenAiChatCompletions),
                )]),
            })?;
            let connection_id = batch.create_config(
                "conn-1".to_owned(),
                ConfigBody::Connection(ConnectionConfig {
                    protocol: LlmTransport::OpenAiChatCompletions,
                    endpoint: Some("https://example.invalid/v1".to_owned()),
                    credential_ref: None,
                    headers: Vec::new(),
                }),
            )?;
            let model_id = batch.create_config(
                "model-1".to_owned(),
                ConfigBody::Model(ModelConfig {
                    reasoning_efforts: None,
                    connection_id,
                    model: "gpt-test".to_owned(),
                    context_window: Some(8192),
                    parameters: LlmParameters::default(),
                    declared_capabilities: BTreeSet::from([
                        crate::db::entities::ModelCapability::Chat,
                    ]),
                }),
            )?;
            let agent_id = batch.create_config(
                "agent-1".to_owned(),
                ConfigBody::Agent(AgentConfig {
                    instructions: None,
                    capabilities: BTreeSet::from([crate::db::entities::AgentCapability::Chat]),
                    models: vec![model_id],
                    compaction_policy: None,
                }),
            )?;
            let job_id = batch.create_unsubmitted_job(
                ComposerContextId::new(100),
                MutableJobInput {
                    priority: JobQueuePriority::Foreground,
                    content: MutableJobContent::Generative {
                        text: "hello".to_owned(),
                        attachments: Vec::new(),
                        configuration: Some(ExecutionConfiguration {
                            reasoning_effort: None,
                            agent_profile: Some(agent_id.into()),
                            llm_profile: model_id.into(),
                        }),
                    },
                },
                Submission {
                    key: SubmissionKey::from_digest([1; 32]),
                    fingerprint: RequestFingerprint::from_digest([2; 32]),
                },
                None,
                None,
            )?;
            batch.submit_mutable_job(
                job_id,
                1,
                Submission {
                    key: SubmissionKey::from_digest([5; 32]),
                    fingerprint: RequestFingerprint::from_digest([6; 32]),
                },
            )?;
            let reservation = batch.reserve_next_development_job(
                DevelopmentReservationRequest {
                    id: ReservationRequestId::new(Uuid::now_v7().as_u128()),
                    pool: pool_id,
                    executor: executor_id,
                    after: None,
                },
                |_, _| true,
            )?;
            assert!(matches!(reservation, DevelopmentReservation::Reserved(_)));
            Ok(job_id)
        });
        let job_id = outcome.result.expect("setup reservation batch failed");
        let outcome = run_batch_with(&opened.database, opened.epoch, Commit::Durable, |batch| {
            let now = batch.now;
            let mut job = batch.mutable_job(job_id)?.expect("job exists");
            let mut allocation = batch.pool_allocation(job_id)?.expect("allocation exists");
            let previous = allocation.clone();
            job.phase = crate::db::entities::MutableJobPhase::Executing;
            allocation.phase = crate::db::entities::AllocationPhase::Running;
            allocation.started_at = Some(now);
            batch.store_mutable_job(&job)?;
            batch.put_pool_allocation(&allocation, Some(&previous))?;

            let body = br#"{"role":"assistant","content":"confirmed"}"#;
            let digest = ContentDigest::from_bytes([3; 32]);
            let req = RequestNumber::new(1).expect("non-zero request");
            let sent = rows::encode_request_event(&RequestEventRecord {
                id: RequestEventId::new(1),
                job_id,
                request: req,
                seq: 1,
                observed_at: now,
                recorded_at: now,
                payload: RequestPayload::Sent {
                    messages: Vec::new(),
                    tools: None,
                    parameters: ContentDigest::from_bytes([4; 32]),
                    dropped_turns: None,
                },
            });
            let received = rows::encode_request_event(&RequestEventRecord {
                id: RequestEventId::new(2),
                job_id,
                request: req,
                seq: 2,
                observed_at: now,
                recorded_at: now,
                payload: RequestPayload::Received { digest },
            });
            let avail = rows::encode_request_event(&RequestEventRecord {
                id: RequestEventId::new(3),
                job_id,
                request: req,
                seq: 3,
                observed_at: now,
                recorded_at: now,
                payload: RequestPayload::Availability {
                    status_code: Some(200),
                    finish_reason: Some(ChatFinishReason::Stop),
                    raw_finish_reason: Some("stop".to_owned()),
                    failure: None,
                },
            });
            batch.step(|tx| {
                tx.open_table(MESSAGES)?
                    .insert(digest.as_bytes().as_slice(), body.as_slice())?;
                let mut events = tx.open_table(REQUEST_EVENTS)?;
                events.insert((job_id.get(), 1, 1), sent.as_slice())?;
                events.insert((job_id.get(), 1, 2), received.as_slice())?;
                events.insert((job_id.get(), 1, 3), avail.as_slice())?;
                Ok(())
            })?;
            batch.record_request_reference(job_id, req, 1)?;
            batch.record_request_reference(job_id, req, 2)?;
            batch.record_request_reference(job_id, req, 3)?;
            Ok(job_id)
        });
        let job_id = outcome.result.expect("setup batch failed");
        drop(opened);

        // Inject rejection inside settle_confirmed_answer after partial writes.
        let guard = FaultGuard::enable();
        let err = match open(&temp.0) {
            Ok(_) => panic!("startup must refuse when confirmed answer settlement rejects"),
            Err(error) => error,
        };
        assert_eq!(err.kind, StorageErrorKind::Corrupted);
        assert!(
            err.detail.contains("was rejected") && err.detail.contains("Capacity remains held"),
            "unexpected error detail: {}",
            err.detail
        );
        drop(guard);

        // Verify complete transaction rollback and preserved capacity ownership.
        let raw = ::redb::Database::open(database_file(&temp.0)).expect("open raw redb");
        let tx = raw.begin_read().expect("begin read");
        let allocation_row = tx
            .open_table(POOL_ALLOCATIONS)
            .expect("open POOL_ALLOCATIONS")
            .get(job_id.get())
            .expect("read allocation")
            .expect("allocation still exists");
        let allocation = allocations::decode_pool_allocation(job_id.get(), allocation_row.value())
            .expect("decode allocation");
        assert!(
            allocation.holds_capacity(),
            "capacity ownership must remain intact after rollback"
        );
        let job_state = state::require(&tx, job_id).expect("read job state");
        assert!(
            job_state.state.outcome.is_none(),
            "partial outcome write must be rolled back"
        );
        assert!(
            state::pending(&tx, job_id)
                .expect("read pending counts")
                .is_some(),
            "partial pending count update must be rolled back"
        );
        let has_outputs = tx
            .open_table(JOB_OUTPUTS)
            .expect("open JOB_OUTPUTS")
            .range((job_id.get(), 0)..=(job_id.get(), u32::MAX))
            .expect("range JOB_OUTPUTS")
            .next()
            .is_some();
        assert!(!has_outputs, "partial output write must be rolled back");
        drop(tx);
        drop(raw);

        // Clearing fault injection allows startup to settle the confirmed answer cleanly.
        let recovered = open(&temp.0).expect("startup succeeds once fault is cleared");
        let tx = recovered.database.begin_read().expect("begin read");
        let settled_allocation = tx
            .open_table(POOL_ALLOCATIONS)
            .expect("open POOL_ALLOCATIONS")
            .get(job_id.get())
            .expect("read allocation")
            .expect("allocation row exists");
        let settled_allocation =
            allocations::decode_pool_allocation(job_id.get(), settled_allocation.value())
                .expect("decode allocation");
        assert!(!settled_allocation.holds_capacity());
    }
}
