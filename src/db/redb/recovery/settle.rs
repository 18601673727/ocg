use crate::db::entities::{
    ChatFinishReason, ExecutionOperation, JobId, OutcomeKind, OutputKind, RequestNumber,
};
use crate::db::redb::batch::{Batch, BatchError, Failure};
use crate::db::redb::jobs;
use crate::db::redb::schema::{
    JOB_OUTPUTS, JOBS, LIVE_CHILDREN, MESSAGES, POOL_ALLOCATIONS, REQUEST_EVENTS, TOOL_CALLS,
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

pub(super) fn settlement_failure(
    error: BatchError,
    operation: &str,
    job: Option<JobId>,
    group: Option<JobId>,
) -> StorageError {
    let job = job.map_or_else(|| "unidentified".to_owned(), |id| id.to_string());
    let group = group.map_or_else(|| "unidentified".to_owned(), |id| id.to_string());
    let context = format!("recovery {operation} (Job {job}, Group {group})");
    match error.0 {
        Failure::Storage(mut error) => {
            error.detail = format!("{context}: {}", error.detail);
            error
        }
        Failure::Rejected(violation) => StorageError {
            kind: super::StorageErrorKind::Other,
            os_error: None,
            detail: format!(
                "{context}: business rejection; recovery cannot safely continue: {violation}"
            ),
        },
    }
}

pub(crate) fn settle_confirmed_answers(batch: &mut Batch) -> Result<(), StorageError> {
    let candidates = batch
        .step(|tx| {
            let mut found = Vec::new();
            for entry in tx.open_table(POOL_ALLOCATIONS)?.iter()? {
                let (key, value) = entry?;
                let allocation = allocations::decode_pool_allocation(key.value(), value.value())
                    .map_err(|error| {
                        settlement_failure(
                            BatchError(Failure::Storage(error)),
                            "decode confirmed answer Allocation",
                            Some(JobId::new(key.value())),
                            None,
                        )
                    })?;
                let result = confirmed_answer(tx, &allocation).map_err(|error| {
                    settlement_failure(
                        BatchError(Failure::Storage(error)),
                        "verify confirmed answer",
                        Some(allocation.token.job),
                        None,
                    )
                })?;
                if let Some(result) = result {
                    found.push((allocation, result));
                }
            }
            Ok(found)
        })
        .map_err(|error| settlement_failure(error, "scan confirmed answers", None, None))?;
    for (allocation, result) in candidates {
        batch.settle_confirmed_answer(allocation, result)?;
    }
    Ok(())
}

impl Batch {
    fn settle_confirmed_answer(
        &mut self,
        allocation: crate::db::entities::PoolAllocation,
        mut result: crate::db::entities::FinalResult,
    ) -> Result<(), StorageError> {
        let job_id = allocation.token.job;
        let Some(job) = self.mutable_job(job_id).map_err(|error| {
            settlement_failure(error, "load confirmed answer Job", Some(job_id), None)
        })?
        else {
            return Err(StorageError::corrupted(format!(
                "confirmed answer Job {job_id} is missing"
            )));
        };
        let failure = |error, operation| {
            settlement_failure(error, operation, Some(job_id), Some(job.parent_id))
        };
        self.normalize_summary_output(&job, &mut result)
            .map_err(|error| failure(error, "normalize confirmed summary"))?;
        self.settle_mutable_outcome(allocation.token, &job, OutcomeKind::Completed, result)
            .map_err(|error| failure(error, "settle confirmed outcome"))?;
        let mut released = allocation.clone();
        released.phase = crate::db::entities::AllocationPhase::Released;
        self.put_pool_allocation(&released, Some(&allocation))
            .map_err(|error| failure(error, "release confirmed Allocation"))?;
        self.settle_summary_membership(&job)
            .map_err(|error| failure(error, "settle confirmed summary membership"))?;
        self.settle_standalone_decision(&job)
            .map_err(|error| failure(error, "settle confirmed standalone Decision"))?;
        self.freed_capacity = true;
        Ok(())
    }
}

pub(crate) fn finish_closing_groups(batch: &mut Batch) -> Result<(), StorageError> {
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
            .map_err(|error| {
                settlement_failure(
                    error,
                    "scan closing Groups",
                    None,
                    after.map(|key: (u128, u128)| JobId::new(key.1)),
                )
            })?;
        let Some(key) = next else {
            return Ok(());
        };
        after = Some(key);
        let group = JobId::new(key.1);
        let spec = batch.job(group).map_err(|error| {
            settlement_failure(error, "load closing Group", None, Some(group))
        })?.ok_or_else(|| StorageError::corrupted(format!(
            "recovery load closing Group {group}: LIVE_CHILDREN of Group {} references a missing Group", JobId::new(key.0)
        )))?;
        if spec.is_chunk_group() {
            batch.recover_chunk_group(&spec).map_err(|error| {
                settlement_failure(error, "recover closing Chunk Group", None, Some(group))
            })?;
        }
    }
}
