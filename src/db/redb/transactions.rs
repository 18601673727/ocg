//! The transactions of section 8 of the design: T1 submit and chat
//! continuation, the claim batch, tool decision and outcome, finalize,
//! confirm stop, settlement, placement and promotion.
//!
//! Every function runs inside the write transaction of one [`Batch`]. The
//! derived tables change in the same transaction as the facts they derive
//! from, so no commit leaves one without the other.

use ::redb::ReadableTableMetadata;

use super::*;
use crate::db::entities::{
    ClaimGrant, ClaimRecord, ClaimRefusal, ConfigBody, ConfigId, ConfigKind, DependencyCondition,
    DependencyState, ExecutionConfiguration, FrozenConfiguration, FrozenDecisionConfig,
    FrozenPoolBinding, GLOBAL_EXECUTION_LIMIT, JobExecution, OutcomeCause, OutcomeKind,
    OutcomeRecord, PoolConfig, Recorded, RequestFingerprint, SubmissionKey, SubmissionRecord,
    ToolCallRef, ToolDecision, ToolEvidenceRecord, ToolOutcome, Violation,
};
use crate::system::execution_controls::ExecutionControl;

pub(super) const TAG_ALLOCATION_POOL: u8 = 40;
pub(super) const TAG_ALLOCATION_EXECUTOR: u8 = 41;

#[allow(dead_code)]
pub(super) fn encode_allocation_pool(pool: &crate::db::entities::AllocationPool) -> Vec<u8> {
    let mut e = Enc::new(TAG_ALLOCATION_POOL);
    e.u128(pool.id.get());
    e.str(&pool.name);
    e.u32(pool.capacity.get());
    e.bool(pool.enabled);
    e.bool(pool.archived);
    e.u32(pool.revision);
    e.0
}

#[allow(dead_code)]
pub(super) fn decode_allocation_pool(
    key: u128,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::AllocationPool> {
    let mut d = Dec::new(bytes, TAG_ALLOCATION_POOL)?;
    let id = ExecutionPoolId::new(d.u128()?);
    let name = d.str()?;
    let capacity = std::num::NonZeroU32::new(d.u32()?)
        .ok_or_else(|| StorageError::corrupted("zero Pool capacity"))?;
    let enabled = d.bool()?;
    let archived = d.bool()?;
    let revision = d.u32()?;
    d.end()?;
    if id.get() != key || name.is_empty() || name.len() > 255 || revision == 0 {
        return Err(StorageError::corrupted("invalid allocation Pool"));
    }
    Ok(crate::db::entities::AllocationPool {
        id,
        name,
        capacity,
        enabled,
        archived,
        revision,
    })
}

#[allow(dead_code)]
pub(super) fn encode_allocation_executor(
    executor: &crate::db::entities::AllocationExecutor,
) -> Vec<u8> {
    let mut e = Enc::new(TAG_ALLOCATION_EXECUTOR);
    e.u128(executor.id.get());
    e.u128(executor.pool.get());
    e.str(&executor.name);
    e.bool(executor.enabled);
    e.bool(executor.archived);
    e.u32(executor.revision);
    e.u32(executor.capabilities.len() as u32);
    for (operation, protocol) in &executor.capabilities {
        e.u8(operation.code());
        e.opt(protocol.as_ref(), |e, protocol| e.u8(protocol.code()));
    }
    e.0
}

#[allow(dead_code)]
pub(super) fn decode_allocation_executor(
    key: u128,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::AllocationExecutor> {
    let mut d = Dec::new(bytes, TAG_ALLOCATION_EXECUTOR)?;
    let id = ExecutorId::new(d.u128()?);
    let pool = ExecutionPoolId::new(d.u128()?);
    let name = d.str()?;
    let enabled = d.bool()?;
    let archived = d.bool()?;
    let revision = d.u32()?;
    let length = d.u32()?;
    if length > 8 {
        return Err(StorageError::corrupted("too many Executor capabilities"));
    }
    let mut capabilities = BTreeSet::new();
    for _ in 0..length {
        let operation = d.code(ExecutionOperation::from_code)?;
        let protocol = d.opt(|d| d.code(LlmTransport::from_code))?;
        if !capabilities.insert((operation, protocol)) {
            return Err(StorageError::corrupted("duplicate Executor capability"));
        }
    }
    d.end()?;
    if id.get() != key
        || id.get() == pool.get()
        || name.is_empty()
        || name.len() > 255
        || revision == 0
    {
        return Err(StorageError::corrupted("invalid allocation Executor"));
    }
    Ok(crate::db::entities::AllocationExecutor {
        id,
        pool,
        name,
        enabled,
        archived,
        revision,
        capabilities,
    })
}

#[allow(dead_code)]
fn encode_pool_allocation(allocation: &crate::db::entities::PoolAllocation) -> Vec<u8> {
    use crate::db::entities::AllocationPhase;
    let mut e = Enc::new(42);
    e.u128(allocation.token.job.get());
    e.u128(allocation.token.pool.get());
    e.u128(allocation.token.executor.get());
    e.u32(allocation.token.generation.get());
    e.u64(allocation.token.epoch);
    e.u64(allocation.job_revision);
    e.u8(match allocation.phase {
        AllocationPhase::Reserved => 1,
        AllocationPhase::Running => 2,
        AllocationPhase::StopRequested => 3,
        AllocationPhase::Uncertain => 4,
        AllocationPhase::Released => 5,
    });
    e.ts(allocation.claimed_at);
    e.ostr(allocation.host_id.as_deref());
    e.ostr(allocation.boot_id.as_deref());
    e.u32(allocation.pool_revision);
    e.u32(allocation.executor_revision);
    e.u8(allocation.operation.code());
    e.opt(allocation.protocol.as_ref(), |e, p| e.u8(p.code()));
    e.ots(allocation.stop_confirmed_at);
    e.opt(allocation.stop_proof.as_ref(), |e, p| e.u8(p.code()));
    e.u128(allocation.request_id.get());
    e.opt(allocation.request_after.as_ref(), |e, cursor| {
        e.u8(cursor.0.0);
        e.i64(cursor.0.1);
        e.u128(cursor.0.2);
        e.u16(cursor.0.3);
    });
    e.ots(allocation.started_at);
    e.0
}

#[allow(dead_code)]
pub(super) fn decode_pool_allocation(
    key: u128,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::PoolAllocation> {
    use crate::db::entities::{AllocationPhase, AllocationToken, PoolAllocation};
    let mut d = Dec::new(bytes, 42)?;
    let job = JobId::new(d.u128()?);
    let pool = ExecutionPoolId::new(d.u128()?);
    let executor = ExecutorId::new(d.u128()?);
    let generation = std::num::NonZeroU32::new(d.u32()?)
        .ok_or_else(|| StorageError::corrupted("zero allocation generation"))?;
    let epoch = d.u64()?;
    let job_revision = d.u64()?;
    let phase = match d.u8()? {
        1 => AllocationPhase::Reserved,
        2 => AllocationPhase::Running,
        3 => AllocationPhase::StopRequested,
        4 => AllocationPhase::Uncertain,
        5 => AllocationPhase::Released,
        _ => return Err(StorageError::corrupted("invalid allocation phase")),
    };
    let claimed_at = d.ts()?;
    let host_id = d.ostr()?;
    let boot_id = d.ostr()?;
    let pool_revision = d.u32()?;
    let executor_revision = d.u32()?;
    let operation = d.code(ExecutionOperation::from_code)?;
    let protocol = d.opt(|d| d.code(LlmTransport::from_code))?;
    let stop_confirmed_at = d.ots()?;
    let stop_proof = d.opt(|d| d.code(StopProof::from_code))?;
    let request_id = crate::db::entities::ReservationRequestId::new(d.u128()?);
    let request_after = d.opt(|d| {
        Ok(crate::db::entities::DevelopmentReadyCursor((
            d.u8()?,
            d.i64()?,
            d.u128()?,
            d.u16()?,
        )))
    })?;
    let started_at = d.ots()?;
    d.end()?;
    if job.get() != key
        || pool.get() == executor.get()
        || epoch == 0
        || job_revision == 0
        || pool_revision == 0
        || executor_revision == 0
        || !valid_reservation_request_id(request_id)
        || request_after.is_some_and(|cursor| cursor.0.0 > 2)
        || stop_confirmed_at.is_some() != stop_proof.is_some()
        || (phase != AllocationPhase::Released && stop_proof.is_some())
        || (phase == AllocationPhase::Reserved && started_at.is_some())
        || (phase == AllocationPhase::Running && started_at.is_none())
        || started_at.is_some_and(|at| at < claimed_at)
    {
        return Err(StorageError::corrupted("invalid Pool allocation"));
    }
    Ok(PoolAllocation {
        request_id,
        request_after,
        token: AllocationToken {
            job,
            pool,
            executor,
            generation,
            epoch,
        },
        job_revision,
        phase,
        claimed_at,
        started_at,
        host_id,
        boot_id,
        pool_revision,
        executor_revision,
        operation,
        protocol,
        stop_confirmed_at,
        stop_proof,
    })
}

pub(crate) fn valid_reservation_request_id(id: crate::db::entities::ReservationRequestId) -> bool {
    let value = uuid::Uuid::from_u128(id.get());
    value.get_version_num() == 7 && value.get_variant() == uuid::Variant::RFC4122
}

/// The Allocation a reservation request made, by the request index. The index
/// row, the Allocation row and the request identity inside it must agree, or
/// the database is corrupt.
pub(super) fn allocation_by_request(
    requests: &impl ReadableTable<u128, u128>,
    allocations: &impl ReadableTable<u128, &'static [u8]>,
    id: crate::db::entities::ReservationRequestId,
) -> Result<Option<crate::db::entities::PoolAllocation>, StorageError> {
    let Some(job) = requests.get(id.get())?.map(|job| job.value()) else {
        return Ok(None);
    };
    let Some(bytes) = allocations.get(job)? else {
        return Err(StorageError::corrupted(
            "reservation request index references a missing Allocation",
        ));
    };
    let allocation = decode_pool_allocation(job, bytes.value())?;
    if allocation.request_id != id {
        return Err(StorageError::corrupted(
            "reservation request index disagrees with its Allocation",
        ));
    }
    Ok(Some(allocation))
}

/// The Jobs holding capacity on `pool`, or on one Executor of it, read from
/// the active index. At most the Pool's capacity rows exist, so the range is
/// bounded; more is corruption, reported by the caller.
pub(super) fn active_allocations(
    active: &impl ReadableTable<(u128, u128, u128), ()>,
    pool: u128,
    executor: Option<u128>,
) -> Result<Vec<(u128, u128)>, StorageError> {
    let (low, high) = match executor {
        Some(executor) => ((pool, executor, 0), (pool, executor, u128::MAX)),
        None => ((pool, 0, 0), (pool, u128::MAX, u128::MAX)),
    };
    active
        .range(low..=high)?
        .map(|entry| {
            let (key, _) = entry?;
            let (_, executor, job) = key.value();
            Ok((executor, job))
        })
        .collect()
}

#[allow(dead_code)]
pub(super) fn mutable_priority(priority: crate::db::entities::JobQueuePriority) -> u8 {
    use crate::db::entities::JobQueuePriority;
    match priority {
        JobQueuePriority::Foreground => 0,
        JobQueuePriority::Automatic => 1,
        JobQueuePriority::Speculative => 2,
    }
}

#[allow(dead_code)]
fn encode_mutable_job(job: &crate::db::entities::MutableJob) -> Vec<u8> {
    use crate::db::entities::{MutableContextSelection, MutableJobPhase};
    let mut e = Enc::new(TAG_JOB);
    e.u128(job.id.get());
    e.u128(job.parent_id.get());
    e.ou128(job.previous.map(JobId::get));
    e.u128(job.composer_context.get());
    e.ts(job.created_at);
    e.ots(job.submitted_at);
    e.u64(job.revision);
    e.u8(match job.phase {
        MutableJobPhase::Unsubmitted => 0,
        MutableJobPhase::Queued => 1,
        MutableJobPhase::Executing => 2,
    });
    encode_mutable_input(&mut e, &job.input);
    e.opt(job.frozen.as_ref(), |e, frozen| match frozen {
        crate::db::entities::MutableFrozenConfiguration::Generative(configuration) => {
            e.u8(0);
            e.frozen_execution(configuration);
        }
        crate::db::entities::MutableFrozenConfiguration::Decision(configuration) => {
            e.u8(1);
            e.native_decision(configuration);
        }
    });
    match job.context_selection {
        MutableContextSelection::FullHistory => e.u8(0),
        MutableContextSelection::AdoptedSummary { producer, source } => {
            e.u8(1);
            e.u128(producer.get());
            for byte in source.as_bytes() {
                e.u8(*byte);
            }
        }
    }
    e.opt(job.summary_source.as_ref(), |e, source| {
        for byte in source.as_bytes() {
            e.u8(*byte);
        }
    });
    e.0
}

fn encode_mutable_input(e: &mut Enc, input: &crate::db::entities::MutableJobInput) {
    use crate::db::entities::{DecisionQuestion, MutableJobContent};
    e.u8(mutable_priority(input.priority));
    match &input.content {
        MutableJobContent::Generative {
            text,
            attachments,
            configuration,
        } => {
            e.u8(0);
            e.str(text);
            e.u32(attachments.len() as u32);
            for attachment in attachments {
                e.u128(attachment.get());
            }
            e.opt(configuration.as_ref(), |e, config| {
                e.ostr(config.reasoning_effort.map(|effort| effort.as_str()));
                e.ou128(config.agent_profile.map(ProfileId::get));
                e.u128(config.llm_profile.get());
            });
        }
        MutableJobContent::Decision {
            request,
            profile,
            references,
        } => {
            e.u8(1);
            e.u128(profile.get());
            e.u32(references.len() as u32);
            for reference in references {
                e.config_ref(reference);
            }
            e.u32(request.schema_version);
            e.str(&request.state.to_string());
            match &request.question {
                DecisionQuestion::Choice { prompt, options } => {
                    e.u8(0);
                    e.str(prompt);
                    e.u32(options.len() as u32);
                    for option in options {
                        e.str(&option.id);
                        e.str(&option.description);
                    }
                }
                DecisionQuestion::BinaryProbability { prompt } => {
                    e.u8(1);
                    e.str(prompt);
                }
                DecisionQuestion::OrdinalScore { prompt, levels } => {
                    e.u8(2);
                    e.str(prompt);
                    e.u32(levels.len() as u32);
                    for level in levels {
                        e.u32(level.value as u32);
                        e.str(&level.description);
                    }
                }
            }
        }
    }
}

fn decode_mutable_input(d: &mut Dec<'_>) -> DecodeResult<crate::db::entities::MutableJobInput> {
    use crate::db::entities::{
        ChoiceOption, DecisionCapability, DecisionQuestion, DecisionRequest, JobQueuePriority,
        MutableJobContent, MutableJobInput, ReasoningEffort, ScoreLevel,
    };
    let priority = match d.u8()? {
        0 => JobQueuePriority::Foreground,
        1 => JobQueuePriority::Automatic,
        2 => JobQueuePriority::Speculative,
        _ => return Err(StorageError::corrupted("invalid Job priority")),
    };
    let content = match d.u8()? {
        0 => {
            let text = d.str()?;
            let count = d.u32()?;
            if count > 8 {
                return Err(StorageError::corrupted("too many Job attachments"));
            }
            let mut attachments = Vec::new();
            for _ in 0..count {
                attachments.push(AttachmentId::new(d.u128()?));
            }
            let configuration = d.opt(|d| {
                let effort = d.ostr()?;
                let reasoning_effort = effort
                    .map(|v| {
                        ReasoningEffort::parse(&v)
                            .ok_or_else(|| StorageError::corrupted("invalid reasoning effort"))
                    })
                    .transpose()?;
                Ok(ExecutionConfiguration {
                    reasoning_effort,
                    agent_profile: d.ou128()?.map(ProfileId::new),
                    llm_profile: ProfileId::new(d.u128()?),
                })
            })?;
            if text.len() > 32 * 1024
                || text
                    .chars()
                    .any(|c| c.is_control() && !matches!(c, '\t' | '\n' | '\r'))
                || attachments.iter().copied().collect::<BTreeSet<_>>().len() != attachments.len()
            {
                return Err(StorageError::corrupted("invalid generative Job input"));
            }
            MutableJobContent::Generative {
                text,
                attachments,
                configuration,
            }
        }
        1 => {
            let profile = ConfigId::new(d.u128()?);
            let count = d.u32()?;
            if !(2..=3).contains(&count) {
                return Err(StorageError::corrupted(
                    "invalid Decision configuration references",
                ));
            }
            let mut references = Vec::new();
            for _ in 0..count {
                references.push(d.config_ref()?);
            }
            if references
                .iter()
                .any(|reference| reference.id.get() == 0 || reference.edit_seq == 0)
                || references
                    .iter()
                    .map(|reference| reference.id)
                    .collect::<BTreeSet<_>>()
                    .len()
                    != references.len()
                || !references.iter().any(|reference| reference.id == profile)
            {
                return Err(StorageError::corrupted(
                    "invalid Decision configuration references",
                ));
            }
            let schema_version = d.u32()?;
            let state = serde_json::from_str(&d.str()?)
                .map_err(|_| StorageError::corrupted("invalid Decision state"))?;
            let kind = d.u8()?;
            let prompt = d.str()?;
            let question = match kind {
                0 => {
                    let count = d.u32()?;
                    if count > 256 {
                        return Err(StorageError::corrupted("too many choice options"));
                    }
                    let mut options = Vec::new();
                    for _ in 0..count {
                        options.push(ChoiceOption {
                            id: d.str()?,
                            description: d.str()?,
                        });
                    }
                    DecisionQuestion::Choice { prompt, options }
                }
                1 => DecisionQuestion::BinaryProbability { prompt },
                2 => {
                    let count = d.u32()?;
                    if count > 256 {
                        return Err(StorageError::corrupted("too many score levels"));
                    }
                    let mut levels = Vec::new();
                    for _ in 0..count {
                        levels.push(ScoreLevel {
                            value: d.u32()? as i32,
                            description: d.str()?,
                        });
                    }
                    DecisionQuestion::OrdinalScore { prompt, levels }
                }
                _ => return Err(StorageError::corrupted("invalid Decision question")),
            };
            let request = DecisionRequest {
                schema_version,
                state,
                question,
            };
            let capabilities = BTreeSet::from([
                DecisionCapability::Choice,
                DecisionCapability::BinaryProbability,
                DecisionCapability::OrdinalScore,
            ]);
            request
                .validate(&capabilities)
                .map_err(StorageError::corrupted)?;
            if profile.get() == 0 {
                return Err(StorageError::corrupted("invalid Decision profile"));
            }
            MutableJobContent::Decision {
                request,
                profile,
                references,
            }
        }
        _ => return Err(StorageError::corrupted("invalid mutable Job input kind")),
    };
    Ok(MutableJobInput { priority, content })
}

#[allow(dead_code)]
pub(super) fn decode_mutable_job(
    key: u128,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::MutableJob> {
    use crate::db::entities::{MutableContextSelection, MutableJob, MutableJobPhase};
    let mut d = Dec::new(bytes, TAG_JOB)?;
    let id = JobId::new(d.u128()?);
    let parent_id = JobId::new(d.u128()?);
    let previous = d.ou128()?.map(JobId::new);
    let composer_context = crate::db::entities::ComposerContextId::new(d.u128()?);
    let created_at = d.ts()?;
    let submitted_at = d.ots()?;
    let revision = d.u64()?;
    let phase = match d.u8()? {
        0 => MutableJobPhase::Unsubmitted,
        1 => MutableJobPhase::Queued,
        2 => MutableJobPhase::Executing,
        _ => return Err(StorageError::corrupted("invalid mutable Job phase")),
    };
    let input = decode_mutable_input(&mut d)?;
    let frozen = d.opt(|d| match d.u8()? {
        0 => d
            .frozen_execution()
            .map(crate::db::entities::MutableFrozenConfiguration::Generative),
        1 => d
            .native_decision()
            .map(crate::db::entities::MutableFrozenConfiguration::Decision),
        _ => Err(StorageError::corrupted(
            "invalid frozen mutable Job configuration kind",
        )),
    })?;
    let context_selection = match d.u8()? {
        0 => MutableContextSelection::FullHistory,
        1 => {
            let producer = JobId::new(d.u128()?);
            let mut source = [0u8; 32];
            for byte in &mut source {
                *byte = d.u8()?;
            }
            if producer.get() == 0 {
                return Err(StorageError::corrupted("invalid context summary producer"));
            }
            MutableContextSelection::AdoptedSummary {
                producer,
                source: crate::db::entities::ContentDigest::from_bytes(source),
            }
        }
        _ => return Err(StorageError::corrupted("invalid context selection")),
    };
    let summary_source = d.opt(|d| {
        Ok(crate::db::entities::ContentDigest::from_bytes(
            d.take_array()?,
        ))
    })?;
    d.end()?;
    if id.get() != key
        || parent_id.get() == 0
        || parent_id >= id
        || previous.is_some_and(|previous| previous >= id)
        || revision == 0
        || (phase != MutableJobPhase::Unsubmitted
            && matches!(
                &input.content,
                crate::db::entities::MutableJobContent::Generative {
                    configuration: None,
                    ..
                }
            ))
        || (phase == MutableJobPhase::Unsubmitted) != submitted_at.is_none()
        || (phase == MutableJobPhase::Unsubmitted) == frozen.is_some()
        || matches!(
            (&input.content, &frozen),
            (
                crate::db::entities::MutableJobContent::Generative { .. },
                Some(crate::db::entities::MutableFrozenConfiguration::Decision(_))
            ) | (
                crate::db::entities::MutableJobContent::Decision { .. },
                Some(crate::db::entities::MutableFrozenConfiguration::Generative(
                    _
                ))
            )
        )
    {
        return Err(StorageError::corrupted("invalid mutable Job"));
    }
    if let (
        crate::db::entities::MutableJobContent::Decision {
            request,
            references,
            ..
        },
        Some(crate::db::entities::MutableFrozenConfiguration::Decision(configuration)),
    ) = (&input.content, &frozen)
    {
        let decision = FrozenDecisionConfig::Native(configuration.clone());
        decision.validate().map_err(StorageError::corrupted)?;
        request
            .validate(&configuration.model.declared_capabilities)
            .map_err(StorageError::corrupted)?;
        if decision.references().collect::<Vec<_>>() != *references {
            return Err(StorageError::corrupted(
                "frozen Decision configuration disagrees with Job input",
            ));
        }
    }
    if let (
        crate::db::entities::MutableJobContent::Generative { configuration, .. },
        Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)),
    ) = (&input.content, &frozen)
    {
        let Some(selection) = configuration else {
            return Err(StorageError::corrupted(
                "frozen generative Job has no configuration selection",
            ));
        };
        if frozen.llm.model.id.get() != selection.llm_profile.get()
            || frozen.agent_ref.map(|reference| reference.id.get())
                != selection.agent_profile.map(|id| id.get())
        {
            return Err(StorageError::corrupted(
                "frozen generative configuration disagrees with Job input",
            ));
        }
    }
    Ok(MutableJob {
        id,
        parent_id,
        previous,
        composer_context,
        created_at,
        submitted_at,
        revision,
        phase,
        input,
        context_selection,
        summary_source,
        frozen,
    })
}

#[allow(dead_code)]
fn encode_mutable_input_revision(
    id: JobId,
    revision: u32,
    input: &crate::db::entities::MutableJobInput,
) -> Vec<u8> {
    let mut e = Enc::new(52);
    e.u128(id.get());
    e.u32(revision);
    encode_mutable_input(&mut e, input);
    e.0
}

#[allow(dead_code)]
fn decode_mutable_input_revision(
    id: JobId,
    revision: u32,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::MutableJobInput> {
    let mut d = Dec::new(bytes, 52)?;
    if d.u128()? != id.get() || d.u32()? != revision || revision == 0 {
        return Err(StorageError::corrupted(
            "invalid Job input revision identity",
        ));
    }
    let input = decode_mutable_input(&mut d)?;
    d.end()?;
    Ok(input)
}

// Native execution operations. They must only run in the database worker's
// write transaction; returned tokens are tentative until commit.
#[allow(dead_code)]
impl Batch {
    pub(super) fn require_generative_predecessors(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<(), BatchError> {
        use crate::db::entities::MutableJobContent;
        let mut previous = job.previous;
        let mut seen = std::collections::BTreeSet::new();
        while let Some(id) = previous {
            if seen.len() == 4096 {
                return self.reject(Violation::InvalidValue(
                    "execution_context",
                    "conversation history exceeds the supported admission limit",
                ));
            }
            if !seen.insert(id) || id == job.id {
                return self.corrupted("conversation predecessor cycle".to_owned());
            }
            let Some(prior) = self.mutable_job(id)? else {
                return self.corrupted("conversation predecessor missing".to_owned());
            };
            if prior.parent_id != job.parent_id || prior.submitted_at.is_none() {
                return self.corrupted("invalid conversation predecessor".to_owned());
            }
            if matches!(prior.input.content, MutableJobContent::Decision { .. }) {
                return self.reject(Violation::InvalidValue(
                    "execution_context",
                    "Generative Jobs cannot follow Decision history",
                ));
            }
            previous = prior.previous;
        }
        Ok(())
    }

    pub(crate) fn unsupported_queued_context(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<bool, BatchError> {
        use crate::db::entities::{MutableJobContent, MutableJobPhase};
        if job.phase != MutableJobPhase::Queued
            || self.require_job_state(job.id)?.outcome.is_some()
            || !matches!(&job.input.content, MutableJobContent::Generative { .. })
        {
            return Ok(false);
        }
        match self.require_generative_predecessors(job) {
            Err(BatchError(Failure::Rejected(Violation::InvalidValue(
                "execution_context",
                "Generative Jobs cannot follow Decision history",
            )))) => Ok(true),
            Ok(()) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn mutable_predecessor_ready(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<bool, BatchError> {
        let Some(id) = job.previous else {
            return Ok(true);
        };
        let Some(previous) = self.mutable_job(id)? else {
            return self.corrupted("conversation predecessor missing".to_owned());
        };
        if previous.parent_id != job.parent_id || previous.submitted_at.is_none() {
            return self.corrupted("invalid conversation predecessor".to_owned());
        }
        let state = self.require_job_state(id)?;
        if !state.locally_stopped()
            || state
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            return Ok(false);
        }
        Ok(!self
            .pool_allocation(id)?
            .is_some_and(|allocation| allocation.holds_capacity()))
    }

    pub(super) fn mutable_context_valid(
        &mut self,
        job: &crate::db::entities::MutableJob,
        selection: crate::db::entities::MutableContextSelection,
    ) -> Result<bool, BatchError> {
        if matches!(
            &job.input.content,
            crate::db::entities::MutableJobContent::Decision { .. }
        ) {
            return Ok(matches!(
                selection,
                crate::db::entities::MutableContextSelection::FullHistory
            ));
        }
        use crate::db::entities::MutableContextSelection;
        let MutableContextSelection::AdoptedSummary { producer, source } = selection else {
            return Ok(true);
        };
        self.step(|tx| {
            let tables = reader::NativeSummaryTables {
                jobs: tx.open_table(JOBS)?,
                states: tx.open_table(JOB_STATES)?,
                allocations: tx.open_table(POOL_ALLOCATIONS)?,
                outputs: tx.open_table(JOB_OUTPUTS)?,
                attachments: tx.open_table(ATTACHMENTS)?,
                tools: tx.open_table(TOOL_CALLS)?,
                messages: tx.open_table(MESSAGES)?,
                configs: tx.open_table(CONFIGS)?,
            };
            match reader::verify_native_summary_tables(
                &tables,
                job.parent_id,
                job.id,
                producer,
                source,
            ) {
                Ok(_) => Ok(true),
                Err(error)
                    if matches!(
                        error.kind,
                        StorageErrorKind::Other | StorageErrorKind::Corrupted
                    ) =>
                {
                    Ok(false)
                }
                Err(error) => Err(error),
            }
        })
    }

    fn put_mutable_job_state(&mut self, id: JobId, state: &JobState) -> Result<(), BatchError> {
        if state.claim.is_some() {
            return self.reject(Violation::InvalidValue(
                "job_state",
                "legacy claims are not written by native storage",
            ));
        }
        let encoded = state.encode();
        self.insert(JOB_STATES, id.get(), encoded.as_slice())
    }

    fn record_mutable_submission(&mut self, record: &SubmissionRecord) -> Result<(), BatchError> {
        let encoded = encode_submission(record);
        self.insert(
            SUBMISSIONS,
            record.key.as_bytes().as_slice(),
            encoded.as_slice(),
        )?;
        self.probe = Some(Probe::Submission(record.key));
        Ok(())
    }

    fn require_allocation_durability(&mut self) -> Result<(), BatchError> {
        // The worker withholds staged group results until its durable group
        // commit succeeds. A Batch result is never itself an authorization to
        // dispatch or acknowledge the command.
        if !matches!(self.commit_mode, Some(Commit::Durable | Commit::Staged)) {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "allocation changes require a durable transaction",
            ));
        }
        Ok(())
    }

    fn validate_mutable_input(
        &mut self,
        input: &crate::db::entities::MutableJobInput,
    ) -> Result<(), BatchError> {
        if let crate::db::entities::MutableJobContent::Decision { request, .. } = &input.content {
            use crate::db::entities::DecisionCapability;
            let capabilities = BTreeSet::from([
                DecisionCapability::Choice,
                DecisionCapability::BinaryProbability,
                DecisionCapability::OrdinalScore,
            ]);
            if let Err(reason) = request.validate(&capabilities) {
                return self.reject(Violation::InvalidValue("decision", reason));
            }
            return Ok(());
        }
        if input.text().len() > 32 * 1024
            || input
                .text()
                .chars()
                .any(|c| c.is_control() && !matches!(c, '\t' | '\n' | '\r'))
            || input.attachments().len() > 8
        {
            return self.reject(Violation::InvalidValue("input", "invalid Job input"));
        }
        let mut seen = BTreeSet::new();
        for attachment in input.attachments() {
            if !seen.insert(*attachment) {
                return self.reject(Violation::InvalidValue(
                    "attachments",
                    "duplicate attachment",
                ));
            }
            if !self.require_attachment(*attachment)?.is_available() {
                return self.reject(Violation::AttachmentCleanedUp(*attachment));
            }
        }
        Ok(())
    }

    /// A submitted Job's frozen configuration as the resolver's own type.
    /// Nothing is re-resolved: the stored record is the only authority.
    fn frozen_configuration(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<FrozenConfiguration, BatchError> {
        use crate::db::entities::MutableFrozenConfiguration;
        match &job.frozen {
            Some(MutableFrozenConfiguration::Generative(configuration)) => {
                Ok(FrozenConfiguration::Generative(configuration.clone()))
            }
            Some(MutableFrozenConfiguration::Decision(configuration)) => Ok(
                FrozenConfiguration::Decision(FrozenDecisionConfig::Native(configuration.clone())),
            ),
            None => self.corrupted("submitted Job has no frozen configuration".to_owned()),
        }
    }

    /// The part of a freshly resolved configuration a Job stores.
    fn storable_frozen(
        &mut self,
        frozen: &FrozenConfiguration,
    ) -> Result<crate::db::entities::MutableFrozenConfiguration, BatchError> {
        use crate::db::entities::MutableFrozenConfiguration;
        match frozen {
            FrozenConfiguration::Generative(configuration) => Ok(
                MutableFrozenConfiguration::Generative(configuration.clone()),
            ),
            FrozenConfiguration::Decision(FrozenDecisionConfig::Native(configuration)) => {
                Ok(MutableFrozenConfiguration::Decision(configuration.clone()))
            }
            FrozenConfiguration::Decision(FrozenDecisionConfig::Generative(_, _)) => self.reject(
                Violation::InvalidValue("decision", "a native Decision Model is required"),
            ),
        }
    }

    fn mutable_execution(
        &mut self,
        input: &crate::db::entities::MutableJobInput,
    ) -> Result<FrozenConfiguration, BatchError> {
        if let crate::db::entities::MutableJobContent::Decision {
            request,
            profile,
            references,
        } = &input.content
        {
            let frozen = self.freeze_decision_profile(*profile)?;
            let FrozenDecisionConfig::Native(ref native) = frozen else {
                return self.reject(Violation::InvalidValue(
                    "decision",
                    "a native Decision Model is required",
                ));
            };
            if let Err(reason) = request.validate(&native.model.declared_capabilities) {
                return self.reject(Violation::InvalidValue("decision", reason));
            }
            if frozen.references().collect::<Vec<_>>() != *references {
                return self.reject(Violation::InvalidValue(
                    "decision",
                    "Decision configuration changed",
                ));
            }
            return Ok(FrozenConfiguration::Decision(frozen));
        }
        if input.text().trim().is_empty() && input.attachments().is_empty() {
            return self.reject(Violation::InvalidValue("input", "Job input is empty"));
        }
        let Some(configuration) = input.configuration() else {
            return self.reject(Violation::InvalidValue(
                "configuration",
                "Job configuration is required",
            ));
        };
        match self.freeze_configuration(configuration)? {
            FrozenConfiguration::Generative(frozen) => {
                // Static facts of the frozen configuration that no later step
                // can repair are refused here, before an Allocation exists.
                if let Some(reason) = frozen.single_request_refusal() {
                    return self.reject(Violation::InvalidValue("configuration", reason));
                }
                Ok(FrozenConfiguration::Generative(frozen))
            }
            FrozenConfiguration::Decision(_) => self.reject(Violation::InvalidValue(
                "configuration",
                "typed decision work is not supported here",
            )),
        }
    }

    fn bind_mutable_decision_input(
        &mut self,
        input: &mut crate::db::entities::MutableJobInput,
    ) -> Result<(), BatchError> {
        if let crate::db::entities::MutableJobContent::Decision {
            request,
            profile,
            references,
        } = &mut input.content
        {
            let frozen = self.freeze_decision_profile(*profile)?;
            let FrozenDecisionConfig::Native(ref native) = frozen else {
                return self.reject(Violation::InvalidValue(
                    "decision",
                    "a native Decision Model is required",
                ));
            };
            if let Err(reason) = request.validate(&native.model.declared_capabilities) {
                return self.reject(Violation::InvalidValue("decision", reason));
            }
            *references = frozen.references().collect();
        }
        Ok(())
    }

    pub(crate) fn mutable_job(
        &mut self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::MutableJob>, BatchError> {
        self.allocation_format_gate()?;
        self.get(JOBS, job.get(), |bytes| {
            decode_mutable_job(job.get(), bytes)
        })
    }

    pub(crate) fn read_mutable_job(
        &mut self,
        id: JobId,
    ) -> Result<Option<crate::db::entities::MutableJobRecord>, BatchError> {
        let Some(job) = self.mutable_job(id)? else {
            return Ok(None);
        };
        let outcome = self.require_job_state(id)?.outcome;
        let parent = self.require_mutable_conversation(job.parent_id)?;
        let JobKind::Group { project, .. } = parent.kind else {
            return self.corrupted("mutable Job parent is not a conversation".to_owned());
        };
        Ok(Some(crate::db::entities::MutableJobRecord {
            job,
            outcome,
            project,
        }))
    }

    /// The one unsubmitted, unsettled Job drafted in `context`, by the draft
    /// index. The index, the Job row and its state must agree, or the
    /// database is corrupt.
    pub(crate) fn mutable_job_for_context(
        &mut self,
        context: crate::db::entities::ComposerContextId,
    ) -> Result<Option<crate::db::entities::MutableJobRecord>, BatchError> {
        self.allocation_format_gate()?;
        let Some(job) = self.step(|tx| {
            Ok(tx
                .open_table(DRAFT_CONTEXTS)?
                .get(context.get())?
                .map(|job| JobId::new(job.value())))
        })?
        else {
            return Ok(None);
        };
        let Some(record) = self.read_mutable_job(job)? else {
            return self.corrupted("draft index references a missing Job".to_owned());
        };
        if record.job.composer_context != context
            || record.job.phase != crate::db::entities::MutableJobPhase::Unsubmitted
            || record.outcome.is_some()
        {
            return self.corrupted("draft index disagrees with its Job".to_owned());
        }
        Ok(Some(record))
    }

    pub(crate) fn require_mutable_job_context(
        &mut self,
        context: crate::db::entities::ComposerContextId,
        job: JobId,
    ) -> Result<(), BatchError> {
        if self
            .mutable_job(job)?
            .is_some_and(|record| record.composer_context == context)
        {
            return Ok(());
        }
        self.reject(Violation::NotFound(RecordRef::Job(job)))
    }

    pub(crate) fn mutable_job_input_revision(
        &mut self,
        id: JobId,
        revision: u64,
    ) -> Result<Option<crate::db::entities::MutableJobInput>, BatchError> {
        let Some(job) = self.mutable_job(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(id)));
        };
        if revision == job.revision {
            return Ok(Some(job.input));
        }
        let Ok(revision) = u32::try_from(revision) else {
            return Ok(None);
        };
        let input = self.get(JOB_INPUTS, (id.get(), revision), |bytes| {
            decode_mutable_input_revision(id, revision, bytes)
        })?;
        if revision > 0 && u64::from(revision) < job.revision && input.is_none() {
            return self.corrupted("Job input revision is missing".to_owned());
        }
        Ok(input)
    }

    pub(crate) fn create_unsubmitted_job(
        &mut self,
        context: crate::db::entities::ComposerContextId,
        mut input: crate::db::entities::MutableJobInput,
        submission: crate::db::entities::Submission,
        conversation: Option<JobId>,
        project: Option<ProjectId>,
    ) -> Result<JobId, BatchError> {
        use crate::db::entities::{MutableJob, MutableJobPhase};
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        if let Some(record) = self.submission(submission.key)? {
            if record.fingerprint != submission.fingerprint || record.seq != 0 {
                return self.reject(Violation::SubmissionKeyUsed {
                    job: record.job,
                    same_request: false,
                });
            }
            self.require_mutable_job_context(context, record.job)?;
            let existing = self.mutable_job(record.job)?.ok_or_else(|| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "created Job missing",
                )))
            })?;
            if conversation.is_some_and(|parent| parent != existing.parent_id) {
                return self.reject(Violation::InvalidValue(
                    "conversation",
                    "Job belongs to another conversation",
                ));
            }
            if let Some(project) = project {
                let parent = self.require_mutable_conversation(existing.parent_id)?;
                if !matches!(parent.kind, JobKind::Group { project: Some(current), .. } if current == project)
                {
                    return self.reject(Violation::InvalidValue(
                        "project",
                        "conversation belongs to another project",
                    ));
                }
            }
            return Ok(record.job);
        }
        if self.mutable_job_for_context(context)?.is_some() {
            return self.reject(Violation::InvalidValue(
                "context",
                "Composer already has an unsubmitted Job",
            ));
        }
        self.bind_mutable_decision_input(&mut input)?;
        self.validate_mutable_input(&input)?;
        let parent_id = match conversation {
            Some(parent) => {
                let row = self.require_mutable_conversation(parent)?;
                if let Some(project) = project {
                    self.require_project(project)?;
                    if !matches!(&row.kind, JobKind::Group { project: Some(current), .. } if *current == project)
                    {
                        return self.reject(Violation::InvalidValue(
                            "project",
                            "conversation belongs to another project",
                        ));
                    }
                }
                if !row.kind.is_group()
                    || self.require_job_state(parent)?.outcome.is_some()
                    || self.job_control(parent)?.closing.is_some()
                {
                    return self.reject(Violation::InvalidValue(
                        "conversation",
                        "conversation is not open",
                    ));
                }
                parent
            }
            None => self.create_mutable_conversation(project)?,
        };
        let previous = self.group_head(parent_id)?;
        let id = JobId::new(self.next_id()?);
        let job = MutableJob {
            id,
            parent_id,
            previous,
            composer_context: context,
            created_at: self.now,
            submitted_at: None,
            revision: 1,
            phase: MutableJobPhase::Unsubmitted,
            input,
            context_selection: crate::db::entities::MutableContextSelection::FullHistory,
            summary_source: None,
            frozen: None,
        };
        self.insert(JOBS, id.get(), encode_mutable_job(&job).as_slice())?;
        self.put_mutable_job_state(id, &JobState::new(id))?;
        self.insert(DRAFT_CONTEXTS, context.get(), id.get())?;
        self.record_mutable_submission(&SubmissionRecord {
            key: submission.key,
            fingerprint: submission.fingerprint,
            job: id,
            seq: 0,
            recorded_at: self.now,
        })?;
        Ok(id)
    }

    pub(crate) fn update_mutable_job(
        &mut self,
        id: JobId,
        expected_revision: u64,
        mut input: crate::db::entities::MutableJobInput,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        self.bind_mutable_decision_input(&mut input)?;
        self.update_mutable_job_with_context(id, expected_revision, input, None)
    }

    fn update_mutable_job_with_context(
        &mut self,
        id: JobId,
        expected_revision: u64,
        input: crate::db::entities::MutableJobInput,
        context_selection: Option<crate::db::entities::MutableContextSelection>,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        use crate::db::entities::MutableJobPhase;
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        let Some(mut job) = self.mutable_job(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(id)));
        };
        if job.revision != expected_revision {
            return self.reject(Violation::InvalidValue("revision", "stale Job revision"));
        }
        if job.phase == MutableJobPhase::Executing || self.require_job_state(id)?.outcome.is_some()
        {
            return self.reject(Violation::InvalidValue("job", "Job input is immutable"));
        }
        if self.pool_allocation(id)?.is_some() {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "Job has an allocation that has not been safely revoked",
            ));
        }
        self.validate_mutable_input(&input)?;
        let next_context = context_selection.unwrap_or(job.context_selection);
        if !self.mutable_context_valid(&job, next_context)? {
            return self.reject(Violation::InvalidValue(
                "context_selection",
                "summary provenance is invalid or unavailable",
            ));
        }
        if input == job.input && next_context == job.context_selection {
            return Ok(job);
        }
        if job.phase == MutableJobPhase::Queued {
            if matches!(
                &input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            ) {
                self.require_generative_predecessors(&job)?;
            }
            if matches!(
                &job.input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            ) && matches!(
                &input.content,
                crate::db::entities::MutableJobContent::Decision { .. }
            ) && self.step(|tx| Ok(tx.open_table(SUCCESSORS)?.get(id.get())?.is_some()))?
            {
                return self.reject(Violation::InvalidValue(
                    "execution_context",
                    "a queued Job with successors cannot be changed to Decision",
                ));
            }
            let parent = self.require_mutable_conversation(job.parent_id)?;
            let JobKind::Group { agent, .. } = parent.kind else {
                return self.corrupted("mutable Job parent is not a conversation".to_owned());
            };
            let compatible = match &input.content {
                crate::db::entities::MutableJobContent::Generative { configuration, .. } => {
                    configuration.and_then(|config| config.agent_profile.map(Into::into)) == agent
                }
                crate::db::entities::MutableJobContent::Decision { .. } => {
                    let FrozenConfiguration::Decision(frozen) = self.mutable_execution(&input)?
                    else {
                        return self.corrupted("Decision input froze as Generative".to_owned());
                    };
                    agent.is_none() || frozen.agent_ref().map(|reference| reference.id) == agent
                }
            };
            if !compatible {
                return self.reject(Violation::InvalidValue(
                    "conversation",
                    "Agent differs from the conversation",
                ));
            }
        }
        let next = job
            .revision
            .checked_add(1)
            .ok_or(BatchError(Failure::Rejected(Violation::ValueOutOfRange(
                "revision",
            ))))?;
        let historical_revision = u32::try_from(job.revision).map_err(|_| {
            BatchError(Failure::Rejected(Violation::ValueOutOfRange(
                "input revision",
            )))
        })?;
        let historical = encode_mutable_input_revision(id, historical_revision, &job.input);
        self.step(|tx| {
            let mut inputs = tx.open_table(JOB_INPUTS)?;
            if inputs.get((id.get(), historical_revision))?.is_some() {
                return Err(StorageError::corrupted("Job input revision already exists"));
            }
            inputs.insert((id.get(), historical_revision), historical.as_slice())?;
            Ok(())
        })?;
        if job.phase == MutableJobPhase::Queued {
            // An edited input is submitted again and freezes again; a change
            // that leaves the input alone keeps the configuration it froze.
            let new = if input == job.input {
                self.frozen_configuration(&job)?
            } else {
                self.mutable_execution(&input)?
            };
            job.frozen = Some(self.storable_frozen(&new)?);
            self.step(|tx| {
                indexes::update_mutable_turn_search(tx, job.parent_id, id, Some(&job.input), &input)
            })?;
            let submitted = job.submitted_at.ok_or_else(|| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "queued Job has no submission time",
                )))
            })?;
            let new_key = (
                mutable_priority(input.priority),
                submitted.as_unix_micros(),
                id.get(),
                mutable_configuration_requirement(&new),
            );
            self.step(|tx| {
                let mut ready = tx.open_table(MUTABLE_READY)?;
                let prefix = (
                    mutable_priority(job.input.priority),
                    submitted.as_unix_micros(),
                    id.get(),
                );
                let keys = ready
                    .range(
                        (prefix.0, prefix.1, prefix.2, 0)
                            ..=(prefix.0, prefix.1, prefix.2, u16::MAX),
                    )?
                    .map(|entry| Ok(entry?.0.value()))
                    .collect::<Result<Vec<_>, StorageError>>()?;
                if keys.len() != 1 {
                    return Err(StorageError::corrupted(
                        "queued Job has invalid ready entries",
                    ));
                }
                ready.remove(keys[0])?;
                ready.insert(new_key, ())?;
                Ok(())
            })?;
        }
        job.input = input;
        job.context_selection = next_context;
        job.revision = next;
        self.insert(JOBS, id.get(), encode_mutable_job(&job).as_slice())?;
        Ok(job)
    }

    /// Change Job-owned context selection under the same revision and
    /// allocation checks as an input edit.
    pub(crate) fn set_mutable_context_selection(
        &mut self,
        id: JobId,
        expected_revision: u64,
        selection: crate::db::entities::MutableContextSelection,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        self.allocation_format_gate()?;
        let Some(job) = self.mutable_job(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(id)));
        };
        if job.revision != expected_revision {
            return self.reject(Violation::InvalidValue("revision", "stale Job revision"));
        }
        if job.context_selection == selection {
            if !self.mutable_context_valid(&job, selection)? {
                return self.reject(Violation::InvalidValue(
                    "context_selection",
                    "summary provenance is invalid or unavailable",
                ));
            }
            return Ok(job);
        }
        self.update_mutable_job_with_context(id, expected_revision, job.input, Some(selection))
    }

    pub(crate) fn discard_unsubmitted_job(
        &mut self,
        id: JobId,
        expected_revision: u64,
    ) -> Result<(), BatchError> {
        use crate::db::entities::MutableJobPhase;
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        let Some(job) = self.mutable_job(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(id)));
        };
        if job.phase != MutableJobPhase::Unsubmitted || job.revision != expected_revision {
            return self.reject(Violation::InvalidValue(
                "job",
                "Job is not the expected unsubmitted revision",
            ));
        }
        let mut state = self.require_job_state(id)?;
        if state.outcome.is_some() || self.pool_allocation(id)?.is_some() {
            return self.reject(Violation::InvalidValue("job", "Job cannot be discarded"));
        }
        state.outcome = Some(OutcomeRecord {
            kind: OutcomeKind::Cancelled,
            at: self.now,
            cause: Some(OutcomeCause::Text("discarded before submission".to_owned())),
        });
        self.put_mutable_job_state(id, &state)?;
        self.release_draft_context(&job)?;
        self.probe = Some(Probe::Outcome(id));
        Ok(())
    }

    /// Removes the draft index entry of a Job leaving the unsubmitted state.
    /// The entry must exist and name this Job.
    fn release_draft_context(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<(), BatchError> {
        let removed = self.step(|tx| {
            Ok(tx
                .open_table(DRAFT_CONTEXTS)?
                .remove(job.composer_context.get())?
                .map(|owner| owner.value()))
        })?;
        if removed != Some(job.id.get()) {
            return self.corrupted("draft index is missing or names another Job".to_owned());
        }
        Ok(())
    }

    /// Explicitly cancel an old queued Job which cannot execute its Decision
    /// history. Its submitted input and conversation membership stay intact.
    pub(crate) fn cancel_unsupported_queued_job(
        &mut self,
        id: JobId,
        expected_revision: u64,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{MutableJobContent, MutableJobPhase};
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        let Some(job) = self.mutable_job(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(id)));
        };
        if job.revision != expected_revision {
            return self.reject(Violation::InvalidValue("revision", "stale Job revision"));
        }
        let mut state = self.require_job_state(id)?;
        if job.phase != MutableJobPhase::Queued
            || state.outcome.is_some()
            || state.claim.is_some()
            || self.pool_allocation(id)?.is_some()
            || !matches!(job.input.content, MutableJobContent::Generative { .. })
        {
            return self.reject(Violation::InvalidValue(
                "job",
                "Job cannot be cancelled by this operation",
            ));
        }
        match self.require_generative_predecessors(&job) {
            Err(BatchError(Failure::Rejected(Violation::InvalidValue(
                "execution_context",
                "Generative Jobs cannot follow Decision history",
            )))) => {}
            Ok(()) => {
                return self.reject(Violation::InvalidValue(
                    "job",
                    "Job does not have unsupported Decision history",
                ));
            }
            Err(error) => return Err(error),
        }
        let submitted = job.submitted_at.ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "queued Job has no submission time",
            )))
        })?;
        self.step(|tx| {
            let mut ready = tx.open_table(MUTABLE_READY)?;
            let prefix = (
                mutable_priority(job.input.priority),
                submitted.as_unix_micros(),
                id.get(),
            );
            let keys = ready
                .range(
                    (prefix.0, prefix.1, prefix.2, 0)..=(prefix.0, prefix.1, prefix.2, u16::MAX),
                )?
                .map(|entry| Ok(entry?.0.value()))
                .collect::<Result<Vec<_>, StorageError>>()?;
            if keys.len() > 1 {
                return Err(StorageError::corrupted(
                    "queued Job has duplicate ready entries",
                ));
            }
            if let Some(key) = keys.first() {
                ready.remove(*key)?;
            }
            tx.open_table(PENDING)?.remove(id.get())?;
            let mut unsettled = tx.open_table(UNSETTLED)?;
            if unsettled.get(id.get())?.map(|v| v.value()) != Some((0, 0)) {
                return Err(StorageError::corrupted(
                    "queued Job has unsettled dependents",
                ));
            }
            unsettled.remove(id.get())?;
            Ok(())
        })?;
        self.adjust_open_children(job.parent_id, -1)?;
        state.outcome = Some(OutcomeRecord {
            kind: OutcomeKind::Cancelled,
            at: self.now,
            cause: Some(OutcomeCause::Text(
                "cancelled before execution: unsupported Decision history".to_owned(),
            )),
        });
        self.put_mutable_job_state(id, &state)?;
        self.probe = Some(Probe::Outcome(id));
        Ok(())
    }

    pub(crate) fn submit_mutable_job(
        &mut self,
        id: JobId,
        expected_revision: u64,
        submission: crate::db::entities::Submission,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        use crate::db::entities::MutableJobPhase;
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        if let Some(record) = self.submission(submission.key)? {
            if record.fingerprint != submission.fingerprint || record.job != id || record.seq != 1 {
                return self.reject(Violation::SubmissionKeyUsed {
                    job: record.job,
                    same_request: false,
                });
            }
            return match self.mutable_job(id)? {
                Some(job) => Ok(job),
                None => self.corrupted("submission references a missing Job".to_owned()),
            };
        }
        let Some(mut job) = self.mutable_job(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(id)));
        };
        if job.revision != expected_revision {
            return self.reject(Violation::InvalidValue("revision", "stale Job revision"));
        }
        if job.phase != MutableJobPhase::Unsubmitted
            || self.require_job_state(id)?.outcome.is_some()
        {
            return self.reject(Violation::InvalidValue("job", "Job is not unsubmitted"));
        }
        if !self.mutable_context_valid(&job, job.context_selection)? {
            return self.reject(Violation::InvalidValue(
                "context_selection",
                "summary provenance is invalid or unavailable",
            ));
        }
        let mut parent = self.require_mutable_conversation(job.parent_id)?;
        if !parent.kind.is_group()
            || self.require_job_state(job.parent_id)?.outcome.is_some()
            || self.job_control(job.parent_id)?.closing.is_some()
        {
            return self.reject(Violation::InvalidValue(
                "conversation",
                "conversation is not open",
            ));
        }
        if self.group_head(job.parent_id)? != job.previous {
            return self.reject(Violation::InvalidValue(
                "conversation",
                "conversation has changed since Job creation",
            ));
        }
        self.validate_mutable_input(&job.input)?;
        if matches!(
            &job.input.content,
            crate::db::entities::MutableJobContent::Generative { .. }
        ) {
            self.require_generative_predecessors(&job)?;
        }
        // The freeze point: the configuration, request headers and secret
        // references the Job will run with are fixed here, and nothing
        // resolves them again from current settings.
        let frozen = self.mutable_execution(&job.input)?;
        job.frozen = Some(self.storable_frozen(&frozen)?);
        let JobKind::Group { agent, .. } = &mut parent.kind else {
            return self.corrupted("mutable Job parent is not a conversation".to_owned());
        };
        match &frozen {
            FrozenConfiguration::Generative(frozen) => {
                let selected = frozen.agent_ref.map(|reference| reference.id);
                if job.previous.is_none() && agent.is_none() {
                    *agent = selected;
                    self.insert(
                        JOBS,
                        parent.id.get(),
                        parent.encode_mutable_conversation().as_slice(),
                    )?;
                } else if *agent != selected {
                    return self.reject(Violation::InvalidValue(
                        "conversation",
                        "Agent differs from the conversation",
                    ));
                }
            }
            FrozenConfiguration::Decision(frozen) => {
                if agent.is_some() && frozen.agent_ref().map(|reference| reference.id) != *agent {
                    return self.reject(Violation::InvalidValue(
                        "conversation",
                        "Agent differs from the conversation",
                    ));
                }
            }
        }
        let submitted = self.now;
        job.phase = MutableJobPhase::Queued;
        job.submitted_at = Some(submitted);
        self.step(|tx| {
            tx.open_table(MUTABLE_READY)?.insert(
                (
                    mutable_priority(job.input.priority),
                    submitted.as_unix_micros(),
                    id.get(),
                    mutable_configuration_requirement(&frozen),
                ),
                (),
            )?;
            tx.open_table(PENDING)?.insert(id.get(), ())?;
            tx.open_table(TURNS)?
                .insert((job.parent_id.get(), id.get()), ())?;
            tx.open_table(CHILDREN)?
                .insert((job.parent_id.get(), id.get()), ())?;
            tx.open_table(UNSETTLED)?.insert(id.get(), (0, 0))?;
            if let Some(previous) = job.previous {
                tx.open_table(SUCCESSORS)?.insert(previous.get(), ())?;
            }
            Ok(())
        })?;
        self.adjust_open_children(job.parent_id, 1)?;
        self.step(|tx| {
            indexes::update_mutable_turn_search(tx, job.parent_id, id, None, &job.input)
        })?;
        self.insert(JOBS, id.get(), encode_mutable_job(&job).as_slice())?;
        self.release_draft_context(&job)?;
        self.record_mutable_submission(&SubmissionRecord {
            key: submission.key,
            fingerprint: submission.fingerprint,
            job: id,
            seq: 1,
            recorded_at: self.now,
        })?;
        Ok(job)
    }

    pub(crate) fn submit_mutable_job_with_input(
        &mut self,
        id: JobId,
        expected_revision: u64,
        input: crate::db::entities::MutableJobInput,
        submission: crate::db::entities::Submission,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        if self.submission(submission.key)?.is_some() {
            return self.submit_mutable_job(id, expected_revision, submission);
        }
        let updated = self.update_mutable_job(id, expected_revision, input)?;
        self.submit_mutable_job(id, updated.revision, submission)
    }

    /// Every native write starts here: the database must carry this build's
    /// format and schema fingerprint and belong to the open epoch holding the
    /// write lock. O(1); the legacy-claim invariant is enforced where Job
    /// states are written (`put_job_state`) and checked at open.
    fn allocation_format_gate(&mut self) -> Result<(), BatchError> {
        let current_epoch = self.epoch;
        let ready = self.step(|tx| {
            let meta = tx.open_table(META)?;
            let version = meta.get(DATABASE_FORMAT_KEY)?;
            let fingerprint = meta.get(SCHEMA_FINGERPRINT_KEY)?;
            let epoch = meta.get("open_epoch")?;
            Ok(
                version.is_some_and(|v| v.value() == DATABASE_FORMAT_VERSION.to_le_bytes())
                    && fingerprint.is_some_and(|v| v.value() == SCHEMA_FINGERPRINT)
                    && epoch.is_some_and(|v| v.value() == current_epoch.to_le_bytes()),
            )
        })?;
        if !ready {
            return self.reject(Violation::InvalidValue(
                "allocation_format",
                "allocation storage is not active",
            ));
        }
        Ok(())
    }

    pub(crate) fn register_allocation_pool(
        &mut self,
        pool: crate::db::entities::AllocationPool,
    ) -> Result<(), BatchError> {
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        if pool.id.get() == 0
            || crate::db::entities::parse_entity_id(&pool.id.to_string()).is_none()
            || pool.name.trim().is_empty()
            || pool.name.chars().any(char::is_control)
            || pool.name.len() > 255
            || pool.revision != 1
        {
            return self.reject(Violation::InvalidValue(
                "pool",
                "invalid Pool configuration",
            ));
        }
        let existing = self.step(|tx| Ok(tx.open_table(CONFIGS)?.get(pool.id.get())?.is_some()))?;
        if existing {
            return self.reject(Violation::InvalidValue(
                "pool",
                "configuration identity already exists",
            ));
        }
        self.observe_configuration_id(pool.id.get())?;
        self.insert(
            CONFIGS,
            pool.id.get(),
            encode_allocation_pool(&pool).as_slice(),
        )
    }

    pub(crate) fn register_allocation_executor(
        &mut self,
        executor: crate::db::entities::AllocationExecutor,
    ) -> Result<(), BatchError> {
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        if executor.id.get() == 0
            || executor.id.get() == executor.pool.get()
            || executor.name.trim().is_empty()
            || executor.name.chars().any(char::is_control)
            || executor.name.len() > 255
            || executor.revision != 1
            || executor.capabilities.len() > 8
            || executor.capabilities.iter().any(|(operation, protocol)| {
                !matches!(
                    (operation, protocol),
                    (ExecutionOperation::LlmRequest, Some(_))
                        | (ExecutionOperation::Decision, None)
                )
            })
            || crate::db::entities::parse_entity_id(&executor.id.to_string()).is_none()
        {
            return self.reject(Violation::InvalidValue(
                "executor",
                "invalid Executor configuration",
            ));
        }
        let pool = self
            .get(CONFIGS, executor.pool.get(), |bytes| match bytes.first() {
                Some(&TAG_ALLOCATION_POOL) => {
                    decode_allocation_pool(executor.pool.get(), bytes).map(Some)
                }
                Some(&TAG_ALLOCATION_EXECUTOR) => {
                    decode_allocation_executor(executor.pool.get(), bytes)?;
                    Ok(None)
                }
                _ => {
                    ConfigRow::decode_at(executor.pool.get(), bytes)?;
                    Ok(None)
                }
            })?
            .flatten();
        if pool.is_none_or(|pool| pool.archived) {
            return self.reject(Violation::InvalidValue("executor", "Pool does not exist"));
        }
        let existing =
            self.step(|tx| Ok(tx.open_table(CONFIGS)?.get(executor.id.get())?.is_some()))?;
        if existing {
            return self.reject(Violation::InvalidValue(
                "executor",
                "configuration identity already exists",
            ));
        }
        self.observe_configuration_id(executor.id.get())?;
        self.insert(
            CONFIGS,
            executor.id.get(),
            encode_allocation_executor(&executor).as_slice(),
        )
    }

    fn observe_configuration_id(&mut self, id: u128) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut sequence = tx.open_table(SEQUENCES)?;
            if sequence
                .get("entity")?
                .is_none_or(|previous| previous.value() < id)
            {
                sequence.insert("entity", id)?;
            }
            Ok(())
        })
    }

    pub(crate) fn update_native_pool(
        &mut self,
        id: ExecutionPoolId,
        capacity: Option<std::num::NonZeroU32>,
        enabled: Option<bool>,
    ) -> Result<crate::db::entities::AllocationPool, BatchError> {
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        let Some(mut pool) = self.get(CONFIGS, id.get(), |bytes| {
            decode_allocation_pool(id.get(), bytes)
        })?
        else {
            return self.reject(Violation::NotFound(RecordRef::ExecutionPool(id)));
        };
        if pool.archived {
            return self.reject(Violation::NotFound(RecordRef::ExecutionPool(id)));
        }
        pool.revision = pool
            .revision
            .checked_add(1)
            .ok_or(BatchError(Failure::Rejected(Violation::ValueOutOfRange(
                "revision",
            ))))?;
        if let Some(capacity) = capacity {
            pool.capacity = capacity;
        }
        if let Some(enabled) = enabled {
            pool.enabled = enabled;
        }
        self.insert(CONFIGS, id.get(), encode_allocation_pool(&pool).as_slice())?;
        Ok(pool)
    }

    pub(crate) fn require_native_capability(
        &mut self,
        operation: ExecutionOperation,
        protocol: Option<LlmTransport>,
    ) -> Result<(), BatchError> {
        let available = self.step(|tx| {
            let configs = tx.open_table(CONFIGS)?;
            let mut available = false;
            for entry in configs.iter()? {
                let (key, value) = entry?;
                match value.value().first() {
                    Some(&TAG_ALLOCATION_EXECUTOR) => {
                        let executor = decode_allocation_executor(key.value(), value.value())?;
                        let pool_bytes = configs.get(executor.pool.get())?.ok_or_else(|| {
                            StorageError::corrupted("Executor references a missing Pool")
                        })?;
                        let pool = decode_allocation_pool(executor.pool.get(), pool_bytes.value())?;
                        if pool.enabled
                            && !pool.archived
                            && executor.enabled
                            && !executor.archived
                            && executor.capabilities.contains(&(operation, protocol))
                        {
                            available = true;
                        }
                    }
                    Some(&TAG_ALLOCATION_POOL) => {
                        decode_allocation_pool(key.value(), value.value())?;
                    }
                    _ => {
                        ConfigRow::decode_at(key.value(), value.value())?;
                    }
                }
            }
            Ok(available)
        })?;
        if !available {
            return self.reject(Violation::InvalidValue(
                "executor",
                "no enabled native Executor supports this operation and protocol",
            ));
        }
        Ok(())
    }

    /// Select and reserve in one writer transaction. The cursor limits work per
    /// request; a new traversal starts at the head of the authoritative index.
    /// `selection` says which ready Jobs this pass may take; see
    /// [`crate::db::entities::ReadySelection`].
    pub(crate) fn reserve_next_development_job(
        &mut self,
        request: crate::db::entities::DevelopmentReservationRequest,
        is_new: bool,
        selection: crate::db::entities::ReadySelection,
    ) -> Result<crate::db::entities::DevelopmentReservation, BatchError> {
        use crate::db::entities::{
            DevelopmentReadyCursor, DevelopmentReservation, MutableJobPhase, ReadySelection,
        };
        use std::ops::Bound::{Excluded, Unbounded};

        const SCAN_BUDGET: usize = 128;
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        if !valid_reservation_request_id(request.id) {
            return self.reject(Violation::InvalidValue(
                "reservation_request",
                "expected a UUIDv7 request identity",
            ));
        }
        let existing = self.step(|tx| {
            allocation_by_request(
                &tx.open_table(ALLOCATION_REQUESTS)?,
                &tx.open_table(POOL_ALLOCATIONS)?,
                request.id,
            )
        })?;
        if let Some(allocation) = existing {
            if !allocation.matches_request(request) {
                return self.reject(Violation::InvalidValue(
                    "reservation_request",
                    "request identity has different parameters",
                ));
            }
            self.probe = Some(Probe::PoolAllocation(allocation.token));
            return Ok(DevelopmentReservation::Replayed(allocation));
        }
        if !is_new {
            return Ok(DevelopmentReservation::Unresolved);
        }
        let pool_id = request.pool;
        let executor_id = request.executor;
        let after = request.after;
        let Some(pool) = self.get(CONFIGS, pool_id.get(), |bytes| {
            decode_allocation_pool(pool_id.get(), bytes)
        })?
        else {
            return self.reject(Violation::InvalidValue("pool", "Pool does not exist"));
        };
        let Some(executor) = self.get(CONFIGS, executor_id.get(), |bytes| {
            decode_allocation_executor(executor_id.get(), bytes)
        })?
        else {
            return self.reject(Violation::InvalidValue(
                "executor",
                "Executor does not exist",
            ));
        };
        if !pool.enabled
            || pool.archived
            || !executor.enabled
            || executor.archived
            || executor.pool != pool_id
        {
            return self.reject(Violation::InvalidValue(
                "executor",
                "Executor is not eligible",
            ));
        }
        if matches!(selection, ReadySelection::SingleRequestGenerative { .. }) {
            // The scheduler never waits on capacity inside a transaction and
            // never scans for an Executor that could not take a Job: a busy
            // Executor, a full Pool or the global limit refuses the pass
            // here, before any ready entry is read. A refusal, unlike
            // `Exhausted`, says nothing about the ready index.
            let room = self.step(|tx| {
                let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
                if active.len()? >= u64::from(GLOBAL_EXECUTION_LIMIT) {
                    return Ok(false);
                }
                let held = active_allocations(&active, pool_id.get(), None)?;
                if held.len() as u64 > u64::from(pool.capacity.get()) {
                    return Err(StorageError::corrupted("Pool occupancy exceeds capacity"));
                }
                Ok((held.len() as u64) < u64::from(pool.capacity.get())
                    && !held
                        .iter()
                        .any(|(executor, _)| *executor == executor_id.get()))
            })?;
            if !room {
                return self.reject(Violation::InvalidValue(
                    "allocation",
                    "the Executor, its Pool or the global limit has no room",
                ));
            }
        }

        let keys = self.step(|tx| {
            let ready = tx.open_table(MUTABLE_READY)?;
            let start = after.map_or(Unbounded, |cursor| Excluded(cursor.0));
            ready
                .range((start, Unbounded))?
                .take(SCAN_BUDGET)
                .map(|entry| Ok(entry?.0.value()))
                .collect::<Result<Vec<_>, StorageError>>()
        })?;
        for key in &keys {
            let id = JobId::new(key.2);
            // A manual trial names its one Job: any other ready entry is
            // passed over untouched, never reserved.
            if let ReadySelection::Only(only) = selection
                && only != id
            {
                continue;
            }
            let Some(job) =
                self.get(JOBS, id.get(), |bytes| decode_mutable_job(id.get(), bytes))?
            else {
                return self.corrupted("ready entry references a missing Job".to_owned());
            };
            let state = self.require_job_state(id)?;
            let submitted = job.submitted_at.ok_or_else(|| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "ready Job has no submission time",
                )))
            })?;
            if job.phase != MutableJobPhase::Queued
                || state.claim.is_some()
                || state.outcome.is_some()
                || key.0 != mutable_priority(job.input.priority)
                || key.1 != submitted.as_unix_micros()
                || self
                    .get(POOL_ALLOCATIONS, id.get(), |bytes| {
                        decode_pool_allocation(id.get(), bytes)
                    })?
                    .is_some()
            {
                return self.corrupted("invalid mutable ready entry".to_owned());
            }
            if !self.mutable_context_valid(&job, job.context_selection)? {
                continue;
            }
            if matches!(
                &job.input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            ) {
                match self.require_generative_predecessors(&job) {
                    Ok(()) => {}
                    Err(BatchError(Failure::Rejected(Violation::InvalidValue(
                        "execution_context",
                        "Generative Jobs cannot follow Decision history",
                    )))) => {
                        // The scheduler's pass writes nothing unless it
                        // reserves; the entry stays for a manual pass to
                        // retire, and the Job reports the blocked history.
                        if !matches!(selection, ReadySelection::SingleRequestGenerative { .. }) {
                            self.remove(MUTABLE_READY, *key)?;
                        }
                        continue;
                    }
                    Err(BatchError(Failure::Rejected(_))) => continue,
                    Err(error) => return Err(error),
                }
            }
            if !self.mutable_predecessor_ready(&job)? {
                continue;
            }
            if matches!(&job.input.content, crate::db::entities::MutableJobContent::Generative { configuration, .. } if configuration.is_none_or(|config| config.agent_profile.is_none()))
            {
                continue;
            }
            let mut attachments_available = true;
            for attachment in job.input.attachments() {
                let record = self.get(ATTACHMENTS, attachment.get(), |bytes| {
                    decode_attachment(attachment.get(), bytes)
                })?;
                if !record.is_some_and(|record| record.is_available()) {
                    attachments_available = false;
                    break;
                }
            }
            if !attachments_available {
                continue;
            }
            let frozen = self.frozen_configuration(&job)?;
            let mut scheduled_external = None;
            if let ReadySelection::SingleRequestGenerative { policy } = selection {
                // Only the frozen facts decide, so the pass and the checked
                // start cannot disagree; a Decision, a Job that needs tools,
                // a Workspace or another protocol, and any configuration the
                // scheduled policy does not admit are left queued, never
                // reserved. The durable scheduled external policy is read in
                // this same transaction; its identity and revision become the
                // binding the start and send transactions revalidate.
                let FrozenConfiguration::Generative(frozen) = &frozen else {
                    continue;
                };
                if frozen.single_request_refusal().is_some() || !policy(frozen) {
                    continue;
                }
                let loopback = frozen
                    .llm
                    .provider
                    .endpoint
                    .as_deref()
                    .is_some_and(crate::system::http::direct_diagnostic_loopback);
                scheduled_external = match self.scheduled_external_policy_for(frozen)? {
                    Some(record) => Some((record.id, record.revision)),
                    None if !loopback => continue,
                    None => None,
                };
            }
            let (operation, protocol) = match &frozen {
                FrozenConfiguration::Generative(frozen) => (
                    ExecutionOperation::LlmRequest,
                    Some(frozen.llm.provider.transport),
                ),
                FrozenConfiguration::Decision(FrozenDecisionConfig::Native(_)) => {
                    (ExecutionOperation::Decision, None)
                }
                FrozenConfiguration::Decision(FrozenDecisionConfig::Generative(_, _)) => continue,
            };
            if key.3 != requirement(Some(operation), protocol)
                || !executor.capabilities.contains(&(operation, protocol))
            {
                continue;
            }
            return self
                .reserve_pool_allocation(
                    id,
                    job.revision,
                    request,
                    operation,
                    protocol,
                    scheduled_external,
                )
                .map(DevelopmentReservation::Reserved);
        }
        if keys.len() == SCAN_BUDGET
            && let Some(key) = keys.last()
        {
            return Ok(DevelopmentReservation::Continue(DevelopmentReadyCursor(
                *key,
            )));
        }
        Ok(DevelopmentReservation::Exhausted)
    }

    /// Read the executor's current owner after a lost reservation reply. The
    /// query is serialized behind earlier commands on the same worker.
    pub(crate) fn development_executor_allocation(
        &mut self,
        executor: ExecutorId,
    ) -> Result<Option<crate::db::entities::PoolAllocation>, BatchError> {
        self.allocation_format_gate()?;
        let Some(config) = self.get(CONFIGS, executor.get(), |bytes| {
            decode_allocation_executor(executor.get(), bytes)
        })?
        else {
            return Ok(None);
        };
        self.step(|tx| {
            let held = active_allocations(
                &tx.open_table(ACTIVE_ALLOCATIONS)?,
                config.pool.get(),
                Some(executor.get()),
            )?;
            let allocations = tx.open_table(POOL_ALLOCATIONS)?;
            let mut found = None;
            for (_, job) in held {
                let Some(bytes) = allocations.get(job)? else {
                    return Err(StorageError::corrupted(
                        "active Allocation index references a missing Allocation",
                    ));
                };
                let allocation = decode_pool_allocation(job, bytes.value())?;
                if !allocation.holds_capacity()
                    || allocation.token.executor != executor
                    || found.is_some()
                {
                    return Err(StorageError::corrupted(
                        "Executor has an inconsistent active Allocation",
                    ));
                }
                found = Some(allocation);
            }
            Ok(found)
        })
    }

    pub(crate) fn reserve_pool_allocation(
        &mut self,
        job: JobId,
        expected_revision: u64,
        request: crate::db::entities::DevelopmentReservationRequest,
        operation: ExecutionOperation,
        protocol: Option<LlmTransport>,
        scheduled_external: Option<(crate::db::entities::ScheduledExternalPolicyId, u32)>,
    ) -> Result<crate::db::entities::PoolAllocation, BatchError> {
        use crate::db::entities::{AllocationPhase, AllocationToken, PoolAllocation};
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        if !valid_reservation_request_id(request.id) {
            return self.reject(Violation::InvalidValue(
                "reservation_request",
                "expected a UUIDv7 request identity",
            ));
        }
        if self
            .step(|tx| {
                allocation_by_request(
                    &tx.open_table(ALLOCATION_REQUESTS)?,
                    &tx.open_table(POOL_ALLOCATIONS)?,
                    request.id,
                )
            })?
            .is_some()
        {
            return self.reject(Violation::InvalidValue(
                "reservation_request",
                "request identity is already used",
            ));
        }
        let pool_id = request.pool;
        let executor_id = request.executor;
        let Some(pool) = self.get(CONFIGS, pool_id.get(), |bytes| {
            decode_allocation_pool(pool_id.get(), bytes)
        })?
        else {
            return self.reject(Violation::InvalidValue("pool", "Pool does not exist"));
        };
        let Some(executor) = self.get(CONFIGS, executor_id.get(), |bytes| {
            decode_allocation_executor(executor_id.get(), bytes)
        })?
        else {
            return self.reject(Violation::InvalidValue(
                "executor",
                "Executor does not exist",
            ));
        };
        if !pool.enabled
            || pool.archived
            || !executor.enabled
            || executor.archived
            || executor.pool != pool_id
            || !executor.capabilities.contains(&(operation, protocol))
        {
            return self.reject(Violation::InvalidValue(
                "executor",
                "Executor is not eligible",
            ));
        }
        let Some(row) = self.mutable_job(job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job)));
        };
        let state = self.require_job_state(job)?;
        if row.phase != crate::db::entities::MutableJobPhase::Queued
            || row.revision != expected_revision
            || state.claim.is_some()
            || state.outcome.is_some()
        {
            return self.reject(Violation::InvalidValue(
                "job",
                "Job is not eligible for this Executor",
            ));
        }
        if !self.mutable_context_valid(&row, row.context_selection)? {
            return self.reject(Violation::InvalidValue(
                "context_selection",
                "summary provenance is invalid or unavailable",
            ));
        }
        if !self.mutable_predecessor_ready(&row)? {
            return self.reject(Violation::InvalidValue(
                "job",
                "conversation predecessor is not settled",
            ));
        }
        if matches!(
            &row.input.content,
            crate::db::entities::MutableJobContent::Generative { .. }
        ) {
            self.require_generative_predecessors(&row)?;
        }
        let frozen = self.frozen_configuration(&row)?;
        if mutable_configuration_requirement(&frozen) != requirement(Some(operation), protocol) {
            return self.reject(Violation::InvalidValue("job", "Job requirements changed"));
        }
        let submitted = row.submitted_at.ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "queued Job has no submission time",
            )))
        })?;
        let ready_key = (
            mutable_priority(row.input.priority),
            submitted.as_unix_micros(),
            job.get(),
            requirement(Some(operation), protocol),
        );
        let available = self.step(|tx| {
            let ready = tx.open_table(MUTABLE_READY)?;
            let allocations = tx.open_table(POOL_ALLOCATIONS)?;
            if ready.get(ready_key)?.is_none() || allocations.get(job.get())?.is_some() {
                return Ok((false, false, 0u64));
            }
            let held =
                active_allocations(&tx.open_table(ACTIVE_ALLOCATIONS)?, pool_id.get(), None)?;
            if held.len() as u64 > u64::from(pool.capacity.get()) {
                return Err(StorageError::corrupted("Pool occupancy exceeds capacity"));
            }
            let occupied = held.len() as u64;
            let executor_free = !held
                .iter()
                .any(|(executor, _)| *executor == executor_id.get());
            Ok((true, executor_free, occupied))
        })?;
        if !available.0 || !available.1 || available.2 >= u64::from(pool.capacity.get()) {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "Job, Executor or Pool capacity is unavailable",
            ));
        }
        let generation = std::num::NonZeroU32::MIN;
        let allocation = PoolAllocation {
            request_id: request.id,
            request_after: request.after,
            token: AllocationToken {
                job,
                pool: pool_id,
                executor: executor_id,
                generation,
                epoch: self.epoch,
            },
            job_revision: row.revision,
            phase: AllocationPhase::Reserved,
            claimed_at: self.now,
            started_at: None,
            host_id: self.host_id.clone(),
            boot_id: self.boot_id.clone(),
            pool_revision: pool.revision,
            executor_revision: executor.revision,
            operation,
            protocol,
            stop_confirmed_at: None,
            stop_proof: None,
        };
        self.put_pool_allocation(&allocation, None)?;
        self.step(|tx| {
            tx.open_table(MUTABLE_READY)?.remove(ready_key)?;
            tx.open_table(PENDING)?.remove(job.get())?;
            let mut bindings = tx.open_table(SCHEDULED_EXTERNAL_BINDING)?;
            match scheduled_external {
                Some((policy, policy_revision)) => {
                    let binding = crate::db::entities::ScheduledExternalBinding {
                        job,
                        policy,
                        policy_revision,
                        allocation_epoch: allocation.token.epoch,
                        allocation_generation: allocation.token.generation,
                    };
                    bindings.insert(
                        job.get(),
                        encode_scheduled_external_binding(&binding).as_slice(),
                    )?;
                }
                None => {
                    bindings.remove(job.get())?;
                }
            }
            Ok(())
        })?;
        self.probe = Some(Probe::PoolAllocation(allocation.token));
        Ok(allocation)
    }

    fn allocation_for_token(
        &mut self,
        token: crate::db::entities::AllocationToken,
    ) -> Result<crate::db::entities::PoolAllocation, BatchError> {
        self.allocation_format_gate()?;
        let Some(allocation) = self.get(POOL_ALLOCATIONS, token.job.get(), |bytes| {
            decode_pool_allocation(token.job.get(), bytes)
        })?
        else {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "allocation does not exist",
            ));
        };
        if allocation.token != token {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "stale allocation token",
            ));
        }
        Ok(allocation)
    }

    pub(crate) fn pool_allocation(
        &mut self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::PoolAllocation>, BatchError> {
        self.allocation_format_gate()?;
        self.get(POOL_ALLOCATIONS, job.get(), |bytes| {
            decode_pool_allocation(job.get(), bytes)
        })
    }

    /// Compare snapshot facts inside the writer transaction. This is only a
    /// building block: it is not a start authorization or a handoff permit.
    /// In particular it does not resolve the runtime tool and OS policy.
    pub(super) fn revalidate_development_snapshot(
        &mut self,
        candidate: &reader::DevelopmentCandidate,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase};
        let (allocation, job, pool, executor, conversation, control, project) = match candidate {
            reader::DevelopmentCandidate::Generative(context) => (
                &context.allocation,
                &context.job,
                &context.pool,
                &context.executor,
                &context.conversation,
                &context.conversation_control,
                &context.project,
            ),
            reader::DevelopmentCandidate::Decision(context) => (
                &context.allocation,
                &context.job,
                &context.pool,
                &context.executor,
                &context.conversation,
                &context.conversation_control,
                &context.project,
            ),
        };
        let stale = || {
            BatchError(Failure::Rejected(Violation::InvalidValue(
                "handoff",
                "execution snapshot changed",
            )))
        };
        let token = allocation.token;
        let metadata_matches = self.step(|tx| {
            let meta = tx.open_table(META)?;
            Ok(meta
                .get(DATABASE_FORMAT_KEY)?
                .is_some_and(|value| value.value() == DATABASE_FORMAT_VERSION.to_le_bytes())
                && meta
                    .get(SCHEMA_FINGERPRINT_KEY)?
                    .is_some_and(|value| value.value() == SCHEMA_FINGERPRINT)
                && meta
                    .get("open_epoch")?
                    .is_some_and(|value| value.value() == token.epoch.to_le_bytes()))
        })?;
        if !metadata_matches {
            return Err(stale());
        }
        let bounded = self.step(|tx| {
            Ok(tx
                .open_table(JOBS)?
                .get(job.id.get())?
                .is_some_and(|value| value.value().len() <= 8 * 1024 * 1024))
        })?;
        if !bounded {
            return Err(stale());
        }
        let state = self.require_job_state(job.id)?;
        if token.epoch != self.epoch
            || allocation.phase != AllocationPhase::Reserved
            || self.pool_allocation(token.job)?.as_ref() != Some(allocation)
            || self.mutable_job(token.job)?.as_ref() != Some(job)
            || job.phase != MutableJobPhase::Queued
            || job.revision != allocation.job_revision
            || state.outcome.is_some()
            || state.claim.is_some()
            || state.stop_confirmed_at.is_some()
            || state.stop_confirmed_by.is_some()
        {
            return Err(stale());
        }
        let current_pool = self.get(CONFIGS, token.pool.get(), |bytes| {
            decode_allocation_pool(token.pool.get(), bytes)
        })?;
        let current_executor = self.get(CONFIGS, token.executor.get(), |bytes| {
            decode_allocation_executor(token.executor.get(), bytes)
        })?;
        if current_pool.as_ref() != Some(pool)
            || current_executor.as_ref() != Some(executor)
            || !pool.enabled
            || pool.archived
            || !executor.enabled
            || executor.archived
            || executor.pool != pool.id
            || pool.id != token.pool
            || executor.id != token.executor
            || pool.revision != allocation.pool_revision
            || executor.revision != allocation.executor_revision
            || !executor
                .capabilities
                .contains(&(allocation.operation, allocation.protocol))
        {
            return Err(stale());
        }
        let occupancy_valid = self.step(|tx| {
            let held =
                active_allocations(&tx.open_table(ACTIVE_ALLOCATIONS)?, token.pool.get(), None)?;
            let executor_exclusive = held
                .iter()
                .filter(|(owner, _)| *owner == token.executor.get())
                .all(|(_, owner_job)| *owner_job == token.job.get());
            let own = held.iter().any(|(owner, owner_job)| {
                *owner == token.executor.get() && *owner_job == token.job.get()
            });
            Ok(executor_exclusive && own && held.len() as u64 <= u64::from(pool.capacity.get()))
        })?;
        if !occupancy_valid {
            return Err(stale());
        }
        if self.require_mutable_conversation(job.parent_id)? != *conversation
            || self.job_control(job.parent_id)? != *control
        {
            return Err(stale());
        }
        if let Some(project) = project
            && self.require_project_record(project.id)? != *project
        {
            return Err(stale());
        }
        let no_execution_evidence = self.step(|tx| {
            Ok(tx
                .open_table(JOB_OUTPUTS)?
                .range((job.id.get(), 0)..=(job.id.get(), u32::MAX))?
                .next()
                .transpose()?
                .is_none()
                && tx
                    .open_table(REQUEST_EVENTS)?
                    .range((job.id.get(), 0, 0)..=(job.id.get(), u32::MAX, u32::MAX))?
                    .next()
                    .transpose()?
                    .is_none()
                && tx
                    .open_table(TOOL_CALLS)?
                    .range((job.id.get(), 0, 0)..=(job.id.get(), u32::MAX, u32::MAX))?
                    .next()
                    .transpose()?
                    .is_none())
        })?;
        if !no_execution_evidence {
            return Err(stale());
        }
        let frozen = self.frozen_configuration(job)?;
        if mutable_configuration_requirement(&frozen)
            != requirement(Some(allocation.operation), allocation.protocol)
        {
            return Err(stale());
        }
        match (candidate, frozen) {
            (
                reader::DevelopmentCandidate::Decision(context),
                FrozenConfiguration::Decision(FrozenDecisionConfig::Native(configuration)),
            ) => {
                if context.configuration != configuration {
                    return Err(stale());
                }
            }
            (
                reader::DevelopmentCandidate::Generative(context),
                FrozenConfiguration::Generative(configuration),
            ) => {
                if context.configuration != configuration {
                    return Err(stale());
                }
                let compaction =
                    self.step(|tx| reader::global_compaction_settings(&tx.open_table(META)?))?;
                if context.compaction != compaction {
                    return Err(stale());
                }
                if let Some(summary) = &context.adopted_summary {
                    if !matches!(job.context_selection, crate::db::entities::MutableContextSelection::AdoptedSummary { producer, source } if producer == summary.producer && source == summary.source)
                    {
                        return Err(stale());
                    }
                    let current = self.step(|tx| {
                        let tables = reader::NativeSummaryTables {
                            jobs: tx.open_table(JOBS)?,
                            states: tx.open_table(JOB_STATES)?,
                            allocations: tx.open_table(POOL_ALLOCATIONS)?,
                            outputs: tx.open_table(JOB_OUTPUTS)?,
                            attachments: tx.open_table(ATTACHMENTS)?,
                            tools: tx.open_table(TOOL_CALLS)?,
                            messages: tx.open_table(MESSAGES)?,
                            configs: tx.open_table(CONFIGS)?,
                        };
                        match reader::verify_native_summary_tables(
                            &tables,
                            job.parent_id,
                            job.id,
                            summary.producer,
                            summary.source,
                        ) {
                            Ok(value) => Ok(Some(value)),
                            Err(error)
                                if matches!(
                                    error.kind,
                                    StorageErrorKind::Other | StorageErrorKind::Corrupted
                                ) =>
                            {
                                Ok(None)
                            }
                            Err(error) => Err(error),
                        }
                    })?;
                    let Some(current) = current else {
                        return Err(stale());
                    };
                    if current.payload != summary.payload
                        || current.producer_spec != summary.producer_spec
                        || current.producer_revision != summary.producer_revision
                        || current.producer_allocation != summary.producer_allocation
                        || current.manifest != summary.manifest
                        || current.summary != summary.summary
                    {
                        return Err(stale());
                    }
                } else if !matches!(
                    job.context_selection,
                    crate::db::entities::MutableContextSelection::FullHistory
                ) {
                    return Err(stale());
                }
                let mut previous = job.previous;
                for turn in context.history.iter().rev() {
                    let bounded = self.step(|tx| {
                        Ok(tx
                            .open_table(JOBS)?
                            .get(turn.record.job.id.get())?
                            .is_some_and(|value| value.value().len() <= 8 * 1024 * 1024))
                    })?;
                    if !bounded {
                        return Err(stale());
                    }
                    if previous != Some(turn.record.job.id)
                        || self.read_mutable_job(turn.record.job.id)?.as_ref() != Some(&turn.record)
                        || turn.record.job.input.attachments()
                            != turn
                                .attachments
                                .iter()
                                .map(|attachment| attachment.id)
                                .collect::<Vec<_>>()
                    {
                        return Err(stale());
                    }
                    let outputs = self.step(|tx| {
                        let mut outputs = Vec::new();
                        let mut bytes = 0usize;
                        let table = tx.open_table(JOB_OUTPUTS)?;
                        for entry in table
                            .range(
                                (turn.record.job.id.get(), 0)
                                    ..=(turn.record.job.id.get(), u32::MAX),
                            )?
                            .take(turn.outputs.len().saturating_add(1))
                        {
                            let (key, value) = entry?;
                            bytes = bytes.saturating_add(value.value().len());
                            if bytes > 8 * 1024 * 1024 {
                                return Ok(None);
                            }
                            outputs.push(decode_output(key.value(), value.value())?);
                        }
                        Ok(Some(outputs))
                    })?;
                    if outputs.as_ref() != Some(&turn.outputs) {
                        return Err(stale());
                    }
                    previous = turn.record.job.previous;
                }
                for input in context.covered_inputs.iter().rev() {
                    if previous != Some(input.job) {
                        return Err(stale());
                    }
                    let bounded = self.step(|tx| {
                        Ok(tx
                            .open_table(JOBS)?
                            .get(input.job.get())?
                            .is_some_and(|value| value.value().len() <= 8 * 1024 * 1024))
                    })?;
                    if !bounded {
                        return Err(stale());
                    }
                    let row = self.read_mutable_job(input.job)?.ok_or_else(stale)?;
                    if row.job.input.text() != input.text
                        || row.job.input.attachments()
                            != input
                                .attachments
                                .iter()
                                .map(|attachment| attachment.id)
                                .collect::<Vec<_>>()
                    {
                        return Err(stale());
                    }
                    previous = row.job.previous;
                }
                if previous.is_some() {
                    return Err(stale());
                }
                if job.input.attachments()
                    != context
                        .attachments
                        .iter()
                        .map(|attachment| attachment.id)
                        .collect::<Vec<_>>()
                {
                    return Err(stale());
                }
                for attachment in context
                    .covered_inputs
                    .iter()
                    .flat_map(|input| &input.attachments)
                    .chain(context.history.iter().flat_map(|turn| &turn.attachments))
                    .chain(&context.attachments)
                {
                    let current = self.get(ATTACHMENTS, attachment.id.get(), |bytes| {
                        decode_attachment(attachment.id.get(), bytes)
                    })?;
                    if current.as_ref() != Some(attachment) || !attachment.is_available() {
                        return Err(stale());
                    }
                }
            }
            _ => return Err(stale()),
        }
        Ok(())
    }

    /// Start only the exact candidate checked in this same writer transaction.
    /// The caller must retain the prepared, owned request until durable commit.
    pub(crate) fn start_mutable_job(
        &mut self,
        prepared: &crate::db::handle::PreparedDevelopmentHandoff,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase};
        self.require_allocation_durability()?;
        if !prepared.has_bounded_tool_free_inputs() {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "prepared inputs are incomplete",
            ));
        }
        let candidate = prepared.candidate();
        self.revalidate_development_snapshot(candidate)?;
        // A Job reserved under a durable scheduled external policy may start
        // only while that same policy revision is still live and still binds
        // the Job's frozen configuration. This transaction is the start's
        // linearization point; the send revalidates again atomically.
        let job = match candidate {
            reader::DevelopmentCandidate::Generative(context) => context.job.id,
            reader::DevelopmentCandidate::Decision(context) => context.job.id,
        };
        self.scheduled_external_authorization(job)?;
        let (snapshot, allocation, conversation) = match candidate {
            reader::DevelopmentCandidate::Generative(context) => {
                if context.configuration.single_request_refusal().is_some() {
                    return self.reject(Violation::InvalidValue(
                        "handoff",
                        "execution policy or provider requires unsupported preparation",
                    ));
                }
                (&context.job, &context.allocation, &context.conversation)
            }
            reader::DevelopmentCandidate::Decision(context) => {
                (&context.job, &context.allocation, &context.conversation)
            }
        };
        if !self.mutable_predecessor_ready(snapshot)? {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "conversation predecessor is not settled",
            ));
        }
        let expected = prepared.expected_credential_references();
        let evidence: Vec<(&str, &[u8])> = prepared.credential_evidence().collect();
        let mut credential_matches = expected.len() == evidence.len()
            && expected
                .iter()
                .all(|reference| evidence.iter().any(|(held, _)| held == reference));
        if credential_matches {
            for (reference, encrypted) in &evidence {
                let unchanged = self.step(|tx| {
                    Ok(tx
                        .open_table(CREDENTIALS)?
                        .get(*reference)?
                        .is_some_and(|row| row.value() == *encrypted))
                })?;
                if !unchanged {
                    credential_matches = false;
                    break;
                }
            }
        }
        if !credential_matches {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "credential changed or is unavailable",
            ));
        }
        if conversation.parent_id.is_some() || snapshot.summary_source.is_some() {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "nested and summary-producing Jobs are not supported by checked handoff",
            ));
        }
        let unsupported_policy = self.step(|tx| {
            let table = tx.open_table(JOBS)?;
            for (index, row) in table.iter()?.enumerate() {
                if index == 100_000 {
                    return Ok(true);
                }
                let (key, value) = row?;
                match value.value().first() {
                    Some(&JobSpec::MUTABLE_CONVERSATION_TAG) => {
                        let group = JobSpec::decode_at(key.value(), value.value())?;
                        if group.parent_id == Some(snapshot.parent_id) {
                            return Ok(true);
                        }
                    }
                    Some(&TAG_JOB) => {}
                    _ => {
                        return Err(StorageError::corrupted(
                            "invalid Job record during handoff policy validation",
                        ));
                    }
                }
            }
            Ok(tx
                .open_table(NOTICES)?
                .range((snapshot.parent_id.get(), 0)..=(snapshot.parent_id.get(), u32::MAX))?
                .next()
                .transpose()?
                .is_some()
                || tx
                    .open_table(LIVE_CHILDREN)?
                    .range((snapshot.parent_id.get(), 0)..=(snapshot.parent_id.get(), u128::MAX))?
                    .next()
                    .transpose()?
                    .is_some()
                || tx
                    .open_table(APPROVALS)?
                    .iter()?
                    .next()
                    .transpose()?
                    .is_some()
                || tx
                    .open_table(TOOL_CALLS)?
                    .iter()?
                    .next()
                    .transpose()?
                    .is_some())
        })?;
        if unsupported_policy {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "notices, child work, approvals or tool evidence require unsupported preparation",
            ));
        }
        // The configuration was frozen when the Job was submitted; starting
        // reads it back and never resolves current settings.
        if snapshot.frozen.is_none() {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "the Job has no frozen configuration",
            ));
        }
        let mut job = snapshot.clone();
        let mut allocation = allocation.clone();
        job.phase = MutableJobPhase::Executing;
        allocation.phase = AllocationPhase::Running;
        allocation.started_at = Some(self.now);
        self.insert(JOBS, job.id.get(), encode_mutable_job(&job).as_slice())?;
        let previous = self.allocation_for_token(allocation.token)?;
        self.put_pool_allocation(&allocation, Some(&previous))?;
        self.probe = Some(Probe::MutableJobStarted(allocation.token, job.revision));
        Ok(job)
    }

    pub(crate) fn validate_allocation_token(
        &mut self,
        token: crate::db::entities::AllocationToken,
    ) -> Result<(), BatchError> {
        use crate::db::entities::AllocationPhase;
        let allocation = self.allocation_for_token(token)?;
        if token.epoch != self.epoch
            || !matches!(
                allocation.phase,
                AllocationPhase::Running | AllocationPhase::StopRequested
            )
        {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "allocation is no longer authoritative",
            ));
        }
        if self.require_job_state(token.job)?.outcome.is_some() {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "Job already has an outcome",
            ));
        }
        let Some(job) = self.mutable_job(token.job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(token.job)));
        };
        if job.phase != crate::db::entities::MutableJobPhase::Executing
            || job.revision != allocation.job_revision
            || job.frozen.is_none()
        {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "Job revision is not executing under this allocation",
            ));
        }
        Ok(())
    }

    /// Validate execution ownership in the transaction that will perform a write.
    pub(super) fn require_development_write(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
    ) -> Result<(), BatchError> {
        self.require_development_write_for(proof, ExecutionOperation::LlmRequest)
    }

    pub(super) fn require_development_write_for(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        operation: ExecutionOperation,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{AllocationPhase, MutableFrozenConfiguration, MutableJobPhase};
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        let token = proof.token();
        let rejected =
            || Violation::InvalidValue("execution", "stale or unauthorized execution ownership");
        let allocation = self.pool_allocation(token.job)?;
        let job = self.mutable_job(token.job)?;
        let Some(allocation) = allocation else {
            return self.reject(rejected());
        };
        let Some(job) = job else {
            return self.reject(rejected());
        };
        let state = self.require_job_state(token.job)?;
        // A stop request bars only a new Provider request (see
        // `record_development_sent_request`); the execution it stops still
        // owns the Allocation and records how it ended.
        if token.epoch != self.epoch
            || allocation.token != token
            || !matches!(
                allocation.phase,
                AllocationPhase::Running | AllocationPhase::StopRequested
            )
            || allocation.operation != operation
            || allocation.job_revision != proof.revision()
            || allocation.stop_confirmed_at.is_some()
            || allocation.stop_proof.is_some()
            || job.id != token.job
            || job.revision != proof.revision()
            || job.phase != MutableJobPhase::Executing
            || !matches!(
                (operation, &job.frozen),
                (
                    ExecutionOperation::LlmRequest,
                    Some(MutableFrozenConfiguration::Generative(_))
                ) | (
                    ExecutionOperation::Decision,
                    Some(MutableFrozenConfiguration::Decision(_))
                )
            )
            || state.claim.is_some()
            || state.outcome.is_some()
            || state.stop_confirmed_at.is_some()
            || state.stop_confirmed_by.is_some()
            || self.job_control(job.parent_id)?.closing.is_some()
        {
            return self.reject(rejected());
        }
        let pool = self.get(CONFIGS, token.pool.get(), |bytes| {
            decode_allocation_pool(token.pool.get(), bytes)
        })?;
        let executor = self.get(CONFIGS, token.executor.get(), |bytes| {
            decode_allocation_executor(token.executor.get(), bytes)
        })?;
        if pool.as_ref().is_none_or(|pool| pool.id != token.pool)
            || executor.as_ref().is_none_or(|executor| {
                executor.id != token.executor
                    || executor.pool != token.pool
                    || !executor
                        .capabilities
                        .contains(&(allocation.operation, allocation.protocol))
            })
        {
            return self.reject(rejected());
        }
        let epoch_matches = self.step(|tx| {
            Ok(tx
                .open_table(META)?
                .get("open_epoch")?
                .is_some_and(|value| value.value() == token.epoch.to_le_bytes()))
        })?;
        if !epoch_matches {
            return self.reject(rejected());
        }
        Ok(())
    }

    pub(super) fn require_development_write_any(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
    ) -> Result<ExecutionOperation, BatchError> {
        let Some(allocation) = self.pool_allocation(proof.token().job)? else {
            return self.reject(Violation::InvalidValue(
                "execution",
                "allocation is missing",
            ));
        };
        self.require_development_write_for(proof, allocation.operation)?;
        Ok(allocation.operation)
    }

    /// Atomically publish a result, settle the whole Job and release its Pool
    /// allocation. The caller must invoke this only after local execution has
    /// actually returned; the database transition is not stop evidence.
    pub(crate) fn finalize_development_job(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        expected_seq: u32,
        finalization: crate::db::entities::JobFinalization,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        self.require_development_write(proof)?;
        self.finish_development_job(proof, expected_seq, finalization)
    }

    /// Records that a supervised exchange ended with an outcome this process
    /// cannot classify: the Job becomes Unknown with the typed reason and the
    /// Allocation becomes Uncertain, still holding its capacity. Nothing is
    /// released, retried or redispatched, and the record never claims that
    /// remote work stopped. Requires the same ownership proof as a result.
    pub(crate) fn record_development_uncertain(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        reason: crate::db::entities::UncertaintyReason,
    ) -> Result<(), BatchError> {
        self.require_development_write_any(proof)?;
        self.transition_pool_allocation(
            proof.token(),
            crate::db::entities::AllocationPhase::Uncertain,
            Some(reason),
        )?;
        self.probe = Some(Probe::Outcome(proof.token().job));
        Ok(())
    }

    /// Settles a Job Failed or Cancelled without an answer. A Failed settlement
    /// may carry the Availability observation of a Provider response that
    /// rejected the request; it is recorded in this same transaction. The
    /// caller supplies only facts it observed: no answer, usage or success
    /// evidence is ever created here, and Cancelled requires that no request
    /// was recorded at all.
    pub(crate) fn finalize_development_failure(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        expected_seq: u32,
        finalization: crate::db::entities::JobFinalization,
        rejection: Option<crate::db::entities::AvailabilityStat>,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        use crate::db::entities::JobFinalization;
        self.require_development_write_any(proof)?;
        match (&finalization, rejection) {
            (JobFinalization::Failed(_), Some(stat)) => {
                self.record_development_provider_failure(proof, stat)?;
            }
            (JobFinalization::Failed(_) | JobFinalization::Cancelled(_), None) => {}
            (JobFinalization::Completed(_) | JobFinalization::Unknown, None) | (_, Some(_)) => {
                return self.reject(Violation::InvalidValue(
                    "outcome",
                    "only an unanswered failure can be settled here",
                ));
            }
        }
        self.finish_development_job(proof, expected_seq, finalization)
    }

    pub(crate) fn finalize_development_decision(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        expected_seq: u32,
        result: crate::db::entities::DecisionResult,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        use crate::db::entities::{
            FrozenDecisionConfig, MutableFrozenConfiguration, MutableJobContent, RequestRef,
        };
        self.require_development_write_for(proof, ExecutionOperation::Decision)?;
        let job_id = proof.token().job;
        let Some(job) = self.mutable_job(job_id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job_id)));
        };
        let (
            Some(MutableFrozenConfiguration::Decision(frozen)),
            MutableJobContent::Decision { request, .. },
        ) = (job.frozen, job.input.content)
        else {
            return self.reject(Violation::InvalidValue(
                "decision",
                "frozen Decision request required",
            ));
        };
        if result
            .validate(
                &request,
                &FrozenDecisionConfig::Native(frozen),
                RequestRef {
                    job: job_id,
                    request: RequestNumber::FIRST,
                },
            )
            .is_err()
        {
            return self.reject(Violation::InvalidValue(
                "decision",
                "result does not match the frozen Decision request",
            ));
        }
        let value = serde_json::to_value(result).map_err(|_| {
            BatchError(Failure::Rejected(Violation::InvalidValue(
                "decision",
                "result cannot be serialized",
            )))
        })?;
        let value = serde_json::json!({"decision": value});
        if crate::db::entities::output_filter::value(&value).as_ref() != &value {
            return self.reject(Violation::InvalidValue(
                "decision",
                "result cannot be preserved by the output filter",
            ));
        }
        self.finish_development_job(
            proof,
            expected_seq,
            crate::db::entities::JobFinalization::Completed(crate::db::entities::FinalResult {
                summary: None,
                code: None,
                goal_reached: None,
                value: Some(value),
            }),
        )
    }

    fn finish_development_job(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        expected_seq: u32,
        finalization: crate::db::entities::JobFinalization,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        use super::rows::decode_request_event;
        use crate::db::entities::RequestPayload;
        let (kind, result) = match finalization {
            crate::db::entities::JobFinalization::Completed(result) => {
                (OutcomeKind::Completed, result)
            }
            crate::db::entities::JobFinalization::Failed(result) => (OutcomeKind::Failed, result),
            crate::db::entities::JobFinalization::Cancelled(result) => {
                (OutcomeKind::Cancelled, result)
            }
            crate::db::entities::JobFinalization::Unknown => {
                return self.reject(Violation::InvalidValue(
                    "outcome",
                    "cancellation or uncertain execution cannot be finalized here",
                ));
            }
        };
        let token = proof.token();
        let Some(job) = self.mutable_job(token.job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(token.job)));
        };
        let valid = self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            let mut prefix = Vec::new();
            for seq in 1..=4 {
                match events.get((token.job.get(), 1, seq))? {
                    Some(row) => prefix.push(decode_request_event(
                        token.job,
                        RequestNumber::FIRST,
                        seq,
                        row.value(),
                    )?),
                    None => break,
                }
            }
            let last = events
                .range((token.job.get(), 0, 0)..=(token.job.get(), u32::MAX, u32::MAX))?
                .next_back()
                .transpose()?
                .map(|(key, _)| key.value());
            let full = prefix.len() == 4
                && matches!(prefix[0].payload, RequestPayload::Sent { .. })
                && matches!(prefix[1].payload, RequestPayload::Measurement { .. })
                && matches!(prefix[2].payload, RequestPayload::Received { .. })
                && matches!(prefix[3].payload, RequestPayload::Availability { .. });
            let failed_before_answer = kind == OutcomeKind::Failed
                && ((prefix.is_empty() && last.is_none())
                    || (prefix.len() == 1
                        && matches!(prefix[0].payload, RequestPayload::Sent { .. })
                        && last == Some((token.job.get(), 1, 1)))
                    || (prefix.len() == 2
                        && matches!(prefix[0].payload, RequestPayload::Sent { .. })
                        && matches!(
                            prefix[1].payload,
                            RequestPayload::Availability {
                                status_code: Some(_),
                                failure: Some(_),
                                ..
                            }
                        )
                        && last == Some((token.job.get(), 1, 2))));
            let cancelled_before_request = kind == OutcomeKind::Cancelled
                && prefix.is_empty()
                && last.is_none()
                && expected_seq == 1;
            if (kind == OutcomeKind::Cancelled && !cancelled_before_request)
                || (!full && !failed_before_answer && !cancelled_before_request)
            {
                return Ok(false);
            }
            drop(events);
            if full {
                indexes::native_usage_sample(tx, token.job)?;
            }
            if tx
                .open_table(UNSETTLED)?
                .get(token.job.get())?
                .map(|v| v.value())
                != Some((0, 0))
            {
                return Ok(false);
            }
            if tx
                .open_table(LIVE_CHILDREN)?
                .range((token.job.get(), 0)..=(token.job.get(), u128::MAX))?
                .next()
                .transpose()?
                .is_some()
            {
                return Ok(false);
            }
            if tx
                .open_table(TOOL_CALLS)?
                .range((token.job.get(), 0, 0)..=(token.job.get(), u32::MAX, u32::MAX))?
                .next()
                .transpose()?
                .is_some()
            {
                return Ok(false);
            }
            let mut last = 0u32;
            for entry in tx
                .open_table(JOB_OUTPUTS)?
                .range((token.job.get(), 0)..=(token.job.get(), u32::MAX))?
            {
                let (key, value) = entry?;
                if rows::output_kind(value.value())? == OutputKind::Result {
                    return Ok(false);
                }
                last = key.value().1;
            }
            Ok(last.checked_add(1) == Some(expected_seq))
        })?;
        if !valid {
            return self.reject(Violation::InvalidValue(
                "outcome",
                "required evidence or output sequence is invalid",
            ));
        }
        let (seq, id) = self.settle_mutable_outcome(token, &job, kind, result)?;
        self.transition_pool_allocation(
            token,
            crate::db::entities::AllocationPhase::Released,
            None,
        )?;
        self.freed_capacity = true;
        self.probe = Some(Probe::Outcome(token.job));
        Ok(crate::db::entities::AppendedOutput {
            output: id,
            seq: i64::from(seq),
        })
    }

    /// Records a terminal outcome and its Result output for a mutable Job and
    /// settles it in the conversation indexes. The Allocation is the caller's.
    fn settle_mutable_outcome(
        &mut self,
        token: crate::db::entities::AllocationToken,
        job: &crate::db::entities::MutableJob,
        kind: OutcomeKind,
        result: crate::db::entities::FinalResult,
    ) -> Result<(u32, crate::db::entities::JobOutputId), BatchError> {
        let mut state = self.require_job_state(token.job)?;
        state.outcome = Some(OutcomeRecord {
            kind,
            at: self.now,
            cause: None,
        });
        self.put_mutable_job_state(token.job, &state)?;
        let (seq, id) = self.write_output(
            token.job,
            OutputKind::Result,
            result.summary,
            result.code,
            result.goal_reached,
            result.value,
        )?;
        self.step(|tx| {
            let mut unsettled = tx.open_table(UNSETTLED)?;
            if unsettled.remove(token.job.get())?.is_none() {
                return Err(StorageError::corrupted("native Job unsettled row missing"));
            }
            let (dependencies, children) = unsettled
                .get(job.parent_id.get())?
                .map(|v| v.value())
                .ok_or_else(|| {
                StorageError::corrupted("native conversation unsettled row missing")
            })?;
            let children = children.checked_sub(1).ok_or_else(|| {
                StorageError::corrupted("native conversation child count underflow")
            })?;
            unsettled.insert(job.parent_id.get(), (dependencies, children))?;
            let mut results = tx.open_table(GROUP_RESULTS)?;
            let previous = results
                .get(job.parent_id.get())?
                .map_or((0, 0), |v| v.value());
            // A cancelled turn is not an answer: the same rule recovery
            // applies when it rebuilds this index from the Job rows.
            let answered = if kind == OutcomeKind::Cancelled {
                previous.0
            } else {
                previous.0.max(token.job.get())
            };
            results.insert(
                job.parent_id.get(),
                (answered, previous.1.max(token.job.get())),
            )?;
            Ok(())
        })?;
        Ok((seq, id))
    }

    /// Ends a Reserved Allocation whose checked start never committed, and
    /// the Job it was reserved for, with a typed failure that preserves the
    /// reason. This, and the stop of a Job that never started (see
    /// [`Self::request_native_stop`]), which shares its checks, are the only
    /// ways a Reserved Allocation is released.
    ///
    /// A Reserved Allocation proves that no execution authority exists: the
    /// start transaction is the only writer that makes an Allocation Running
    /// and a Job Executing, and the in-memory handoff, the only thing that can
    /// send a Provider request, is created after that transaction commits.
    /// Everything below is checked in this one writer transaction, so a start
    /// racing this call either committed first (the phase is no longer
    /// Reserved and this refuses) or finds the Allocation gone and refuses.
    /// Anything that is not exactly an untouched reservation is left held.
    pub(crate) fn fail_unstarted_reservation(
        &mut self,
        token: crate::db::entities::AllocationToken,
        cause: crate::db::entities::UnstartedCause,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        self.end_untouched_reservation(
            token,
            OutcomeKind::Failed,
            crate::db::entities::FinalResult {
                summary: Some(cause.summary().to_owned()),
                code: Some(cause.code().to_owned()),
                goal_reached: Some(false),
                value: None,
            },
        )
    }

    /// Ends an untouched reservation and its Job with `kind` and `result`,
    /// under every check [`Self::fail_unstarted_reservation`] documents: a
    /// reservation that is not exactly untouched is refused and left held.
    /// The stop of a Job that was reserved but never started ends it here as
    /// Cancelled; see [`Self::request_native_stop`].
    fn end_untouched_reservation(
        &mut self,
        token: crate::db::entities::AllocationToken,
        kind: OutcomeKind,
        result: crate::db::entities::FinalResult,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase};
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        let refused = |reason: &'static str| {
            BatchError(Failure::Rejected(Violation::InvalidValue(
                "reservation",
                reason,
            )))
        };
        let mut allocation = self.allocation_for_token(token)?;
        if token.epoch != self.epoch {
            return Err(refused("the Allocation belongs to another database epoch"));
        }
        if allocation.phase != AllocationPhase::Reserved
            || allocation.stop_confirmed_at.is_some()
            || allocation.stop_proof.is_some()
        {
            return Err(refused("the Allocation is not an untouched reservation"));
        }
        let Some(job) = self.mutable_job(token.job)? else {
            return Err(refused("the reserved Job does not exist"));
        };
        let state = self.require_job_state(token.job)?;
        if job.id != token.job
            || job.phase != MutableJobPhase::Queued
            || job.revision != allocation.job_revision
            || job.frozen.is_none()
            || state.outcome.is_some()
            || state.claim.is_some()
            || state.stop_confirmed_at.is_some()
            || state.stop_confirmed_by.is_some()
        {
            return Err(refused(
                "the Job is not the queued revision that was reserved",
            ));
        }
        let pool = self.get(CONFIGS, token.pool.get(), |bytes| {
            decode_allocation_pool(token.pool.get(), bytes)
        })?;
        let executor = self.get(CONFIGS, token.executor.get(), |bytes| {
            decode_allocation_executor(token.executor.get(), bytes)
        })?;
        if pool.as_ref().is_none_or(|pool| pool.id != token.pool)
            || executor
                .as_ref()
                .is_none_or(|executor| executor.id != token.executor || executor.pool != token.pool)
        {
            return Err(refused("the reserved Pool or Executor binding changed"));
        }
        let consistent = self.step(|tx| {
            let held =
                active_allocations(&tx.open_table(ACTIVE_ALLOCATIONS)?, token.pool.get(), None)?;
            let indexed = held.iter().any(|(owner, owner_job)| {
                *owner == token.executor.get() && *owner_job == token.job.get()
            });
            let in_ready = match job.submitted_at {
                Some(submitted) => {
                    let prefix = (
                        mutable_priority(job.input.priority),
                        submitted.as_unix_micros(),
                        token.job.get(),
                    );
                    tx.open_table(MUTABLE_READY)?
                        .range(
                            (prefix.0, prefix.1, prefix.2, 0)
                                ..=(prefix.0, prefix.1, prefix.2, u16::MAX),
                        )?
                        .next()
                        .transpose()?
                        .is_some()
                }
                None => true,
            };
            let evidence = tx
                .open_table(JOB_OUTPUTS)?
                .range((token.job.get(), 0)..=(token.job.get(), u32::MAX))?
                .next()
                .transpose()?
                .is_some()
                || tx
                    .open_table(REQUEST_EVENTS)?
                    .range((token.job.get(), 0, 0)..=(token.job.get(), u32::MAX, u32::MAX))?
                    .next()
                    .transpose()?
                    .is_some()
                || tx
                    .open_table(TOOL_CALLS)?
                    .range((token.job.get(), 0, 0)..=(token.job.get(), u32::MAX, u32::MAX))?
                    .next()
                    .transpose()?
                    .is_some()
                || tx
                    .open_table(LIVE_CHILDREN)?
                    .range((token.job.get(), 0)..=(token.job.get(), u128::MAX))?
                    .next()
                    .transpose()?
                    .is_some();
            Ok(indexed && !in_ready && !evidence)
        })?;
        if !consistent {
            return Err(refused("the reservation has other state; it is left held"));
        }
        let (seq, id) = self.settle_mutable_outcome(token, &job, kind, result)?;
        let previous = allocation.clone();
        allocation.phase = AllocationPhase::Released;
        self.put_pool_allocation(&allocation, Some(&previous))?;
        self.freed_capacity = true;
        self.probe = Some(Probe::Outcome(token.job));
        Ok(crate::db::entities::AppendedOutput {
            output: id,
            seq: i64::from(seq),
        })
    }

    /// Records the stop of one native Job, decided against its Allocation in
    /// this one transaction:
    ///
    /// - Reserved: no execution authority exists yet, so the untouched
    ///   reservation is ended here and the Job is Cancelled; nothing was or
    ///   will be sent. A start racing this either committed first (the
    ///   Allocation is Running) or is refused.
    /// - Running: the Allocation becomes StopRequested. From then on no
    ///   Provider request may be recorded as sent, so one that was not yet
    ///   sent never is; an exchange already sent settles as it ends, held as
    ///   Uncertain unless it returned a result. Ownership is not released here.
    /// - StopRequested: already requested; nothing is written.
    ///
    /// A Job without an Allocation is not running, and a Job with an outcome
    /// has ended; both are refused and nothing changes.
    pub(crate) fn request_native_stop(
        &mut self,
        job: JobId,
    ) -> Result<crate::db::entities::NativeStopRequest, BatchError> {
        use crate::db::entities::{AllocationPhase, NativeStopRequest};
        self.allocation_format_gate()?;
        self.require_allocation_durability()?;
        if self.mutable_job(job)?.is_none() {
            return self.reject(Violation::NotFound(RecordRef::Job(job)));
        }
        let ended = Violation::InvalidValue("stop", crate::db::entities::STOP_JOB_ENDED);
        if self.require_job_state(job)?.outcome.is_some() {
            return self.reject(ended);
        }
        let Some(allocation) = self.pool_allocation(job)? else {
            return self.reject(Violation::InvalidValue(
                "stop",
                crate::db::entities::STOP_JOB_NOT_RUNNING,
            ));
        };
        let token = allocation.token;
        match allocation.phase {
            AllocationPhase::Reserved => {
                self.end_untouched_reservation(
                    token,
                    OutcomeKind::Cancelled,
                    crate::db::entities::FinalResult {
                        summary: Some(
                            "Stopped before it started. Nothing was sent to the Provider."
                                .to_owned(),
                        ),
                        code: Some("stopped_before_start".to_owned()),
                        goal_reached: Some(false),
                        value: None,
                    },
                )?;
                Ok(NativeStopRequest::EndedBeforeStart(token))
            }
            AllocationPhase::Running => {
                self.transition_pool_allocation(token, AllocationPhase::StopRequested, None)?;
                self.probe = Some(Probe::PoolAllocation(token));
                Ok(NativeStopRequest::Requested(token))
            }
            AllocationPhase::StopRequested => Ok(NativeStopRequest::AlreadyRequested(token)),
            AllocationPhase::Uncertain | AllocationPhase::Released => self.reject(ended),
        }
    }

    pub(crate) fn transition_pool_allocation(
        &mut self,
        token: crate::db::entities::AllocationToken,
        next: crate::db::entities::AllocationPhase,
        uncertainty: Option<crate::db::entities::UncertaintyReason>,
    ) -> Result<(), BatchError> {
        use crate::db::entities::AllocationPhase;
        self.require_allocation_durability()?;
        if (next == AllocationPhase::Uncertain) != uncertainty.is_some() {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "an Uncertain transition carries exactly one reason",
            ));
        }
        let mut allocation = self.allocation_for_token(token)?;
        if token.epoch != self.epoch {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "stale database epoch",
            ));
        }
        if allocation.phase == AllocationPhase::Reserved && next == AllocationPhase::Running {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "execution start requires a verified Job revision",
            ));
        }
        let allowed = matches!(
            (allocation.phase, next),
            (
                AllocationPhase::Reserved,
                AllocationPhase::Running
                    | AllocationPhase::StopRequested
                    | AllocationPhase::Uncertain
            ) | (
                AllocationPhase::Running,
                AllocationPhase::StopRequested
                    | AllocationPhase::Uncertain
                    | AllocationPhase::Released
            ) | (
                // A stopped execution that still returned a confirmed
                // outcome (cancelled before its request was sent, or a
                // result that arrived first) is released like a Running
                // one: the Released branch below requires that outcome.
                AllocationPhase::StopRequested,
                AllocationPhase::Uncertain | AllocationPhase::Released
            )
        );
        if !allowed {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "invalid allocation transition",
            ));
        }
        let mut state = self.require_job_state(token.job)?;
        if next == AllocationPhase::Released {
            if !state.locally_stopped()
                || state
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
            {
                return self.reject(Violation::InvalidValue(
                    "allocation",
                    "Job has no confirmed outcome",
                ));
            }
        } else if state.outcome.is_some() {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "Job already has an outcome",
            ));
        } else if next == AllocationPhase::Uncertain {
            state.outcome = Some(OutcomeRecord {
                kind: OutcomeKind::Unknown,
                at: self.now,
                cause: uncertainty.map(OutcomeCause::Uncertain),
            });
            self.put_mutable_job_state(token.job, &state)?;
        }
        let previous = allocation.clone();
        allocation.phase = next;
        self.put_pool_allocation(&allocation, Some(&previous))
    }

    pub(crate) fn confirm_allocation_stop(
        &mut self,
        token: crate::db::entities::AllocationToken,
        proof: StopProof,
    ) -> Result<(), BatchError> {
        use crate::db::entities::AllocationPhase;
        self.require_allocation_durability()?;
        let mut allocation = self.allocation_for_token(token)?;
        if !matches!(
            allocation.phase,
            AllocationPhase::Uncertain | AllocationPhase::StopRequested
        ) {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "stop confirmation is not applicable",
            ));
        }
        if !self
            .require_job_state(token.job)?
            .outcome
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "stop confirmation requires an unknown Job outcome",
            ));
        }
        let previous = allocation.clone();
        allocation.phase = AllocationPhase::Released;
        allocation.stop_confirmed_at = Some(self.now);
        allocation.stop_proof = Some(proof);
        self.put_pool_allocation(&allocation, Some(&previous))
    }

    /// The only writer of `POOL_ALLOCATIONS`. It keeps the request and active
    /// indexes in the same transaction: a new row adds both (a reused request
    /// identity or a second Allocation for the Job is corruption, not
    /// overwritten), a change keeps identity and token and moves the active
    /// entry out when capacity is released, and a Released row never changes.
    fn put_pool_allocation(
        &mut self,
        allocation: &crate::db::entities::PoolAllocation,
        previous: Option<&crate::db::entities::PoolAllocation>,
    ) -> Result<(), BatchError> {
        let token = allocation.token;
        let active_key = (token.pool.get(), token.executor.get(), token.job.get());
        if let Some(previous) = previous
            && (previous.token != token
                || previous.request_id != allocation.request_id
                || !previous.holds_capacity())
        {
            return self
                .corrupted("Allocation identity changed or Released row rewritten".to_owned());
        }
        let outcome = self.step(|tx| {
            let mut allocations = tx.open_table(POOL_ALLOCATIONS)?;
            let mut requests = tx.open_table(ALLOCATION_REQUESTS)?;
            let mut active = tx.open_table(ACTIVE_ALLOCATIONS)?;
            let stored = allocations.get(token.job.get())?.is_some();
            if stored != previous.is_some() {
                return Ok(Some("Allocation presence disagrees with the writer's view"));
            }
            if previous.is_none() {
                if requests
                    .insert(allocation.request_id.get(), token.job.get())?
                    .is_some()
                {
                    return Ok(Some("duplicate reservation request identity"));
                }
            } else if requests
                .get(allocation.request_id.get())?
                .is_none_or(|job| job.value() != token.job.get())
            {
                return Ok(Some("reservation request index is missing or wrong"));
            }
            let indexed = active.get(active_key)?.is_some();
            match (previous.is_some(), allocation.holds_capacity()) {
                (false, true) => {
                    if indexed {
                        return Ok(Some("active Allocation index already has this Job"));
                    }
                    active.insert(active_key, ())?;
                }
                (false, false) => return Ok(Some("a new Allocation cannot be Released")),
                (true, true) => {
                    if !indexed {
                        return Ok(Some("active Allocation index is missing a holding row"));
                    }
                }
                (true, false) => {
                    if !indexed {
                        return Ok(Some("active Allocation index is missing a holding row"));
                    }
                    active.remove(active_key)?;
                }
            }
            allocations.insert(
                token.job.get(),
                encode_pool_allocation(allocation).as_slice(),
            )?;
            Ok(None)
        })?;
        match outcome {
            Some(detail) => self.corrupted(detail.to_owned()),
            None => Ok(()),
        }
    }
}

/// The key of a Job in `ready`: what a Pool must offer to run it. The high
/// byte is the operation (0 for work no Pool is asked to run) and the low byte
/// is the protocol (0 for none).
pub(super) fn requirement(
    operation: Option<ExecutionOperation>,
    protocol: Option<LlmTransport>,
) -> u16 {
    let operation = operation.map_or(0, |operation| u16::from(operation.code()));
    let protocol = protocol.map_or(0, |protocol| u16::from(protocol.code()));
    (operation << 8) | protocol
}

/// What a Pool must offer to run the Job: an LLM request over the protocol
/// of the Connection it froze. A Job without an execution needs nothing a
/// Pool offers.
pub(super) fn execution_requirement(execution: &FrozenExecutionConfig) -> u16 {
    requirement(
        Some(ExecutionOperation::LlmRequest),
        Some(execution.llm.provider.transport),
    )
}

pub(super) fn mutable_configuration_requirement(configuration: &FrozenConfiguration) -> u16 {
    match configuration {
        FrozenConfiguration::Generative(execution) => execution_requirement(execution),
        FrozenConfiguration::Decision(decision) => decision_work_requirement(decision),
    }
}

/// What a Pool must offer to run one decision Job. The native protocol is
/// chosen from the frozen Decision Model; a Pool that offers `Decision` can
/// run any typed protocol its executor adapter implements.
pub(super) fn decision_work_requirement(frozen: &FrozenDecisionConfig) -> u16 {
    match frozen {
        FrozenDecisionConfig::Native(_) => decision_requirement(),
        FrozenDecisionConfig::Generative(frozen, _) => execution_requirement(frozen),
    }
}

pub(super) fn decision_requirement() -> u16 {
    requirement(Some(ExecutionOperation::Decision), None)
}
/// A Pool as the claim batch sees it.
struct OpenPool {
    id: ConfigId,
    edit_seq: u32,
    body: PoolConfig,
    held: u32,
}

impl OpenPool {
    fn has_room(&self) -> bool {
        self.body.enabled && self.held < self.body.concurrency_limit
    }

    fn offers(&self, requirement_key: u16) -> bool {
        self.body.capabilities.iter().any(|(operation, protocol)| {
            requirement(Some(*operation), *protocol) == requirement_key
        })
    }
}

impl Batch {
    pub(crate) fn with_admission_candidate<T>(
        &mut self,
        candidate: Option<crate::db::AdmissionCandidate>,
        submission: Option<crate::db::entities::Submission>,
        create: impl FnOnce(&mut Self) -> Result<T, BatchError>,
    ) -> Result<T, BatchError> {
        let replay = match (candidate, submission) {
            (Some(_), Some(submission)) => match self.submission(submission.key)? {
                Some(record) if record.fingerprint != submission.fingerprint => {
                    return self.reject(Violation::SubmissionKeyUsed {
                        job: record.job,
                        same_request: false,
                    });
                }
                Some(_) => true,
                None => false,
            },
            (Some(_), None) | (None, _) => false,
        };
        let value = create(self)?;
        if !replay && let Some(candidate) = candidate {
            let (executor, model, decision) = match candidate {
                crate::db::AdmissionCandidate::Decision(executor, model) => (executor, model, true),
            };
            if !self
                .config_record(executor.into())?
                .is_some_and(|row| matches!(row.body, ConfigBody::Pool(_)))
            {
                return self.reject(Violation::NotFound(RecordRef::Executor(executor)));
            }
            let native = decision
                && self
                    .config_record(model.into())?
                    .is_some_and(|row| matches!(row.body, ConfigBody::DecisionModel(_)));
            let compatible = self.step(|tx| {
                if native {
                    reader::decision_executors(tx)
                } else {
                    reader::compatible_executors(tx, model)
                }
            })?;
            if !compatible.contains(&executor)
                && (compatible.len() < MAX_PAGE
                    || !self.step(|tx| reader::candidate_compatible(tx, candidate))?)
            {
                return self.reject(Violation::InvalidValue(
                    "executor_incompatible",
                    "the Executor cannot run this request",
                ));
            }
        }
        Ok(value)
    }

    // ---- T1 submit (8.1) and chat continuation (8.2) -------------------------------

    /// T1_SUBMIT. `spec` is the immutable Job spec; its `id`, `created_at`
    /// and `submission` are set here. Returns the new Job.
    pub(crate) fn t1_submit(
        &mut self,
        submission: Option<(SubmissionKey, RequestFingerprint)>,
        mut spec: JobSpec,
    ) -> Result<JobId, BatchError> {
        if let Some((key, fingerprint)) = submission
            && let Some(recorded) = self.submission(key)?
        {
            return self.reject(Violation::SubmissionKeyUsed {
                job: recorded.job,
                same_request: recorded.fingerprint == fingerprint,
            });
        }
        self.validate_spec(&spec)?;

        let job = JobId::new(self.next_id()?);
        spec.id = job;
        spec.created_at = self.now;
        self.insert_job(&spec)?;
        self.probe = Some(Probe::Job(job));
        self.put_job_state(job, &JobState::new(job))?;

        let mut open_dependencies = 0u32;
        self.step(|tx| {
            if !spec.kind.is_group() {
                tx.open_table(PENDING)?.insert(job.get(), ())?;
            }
            Ok(())
        })?;
        for (dependency, condition) in spec.kind.waits_on() {
            if !self.is_subtree_settled(dependency)? {
                open_dependencies += 1;
                self.insert(
                    DEPENDENCY_WAITERS,
                    (dependency.get(), job.get()),
                    condition.code(),
                )?;
            }
        }
        self.insert(UNSETTLED, job.get(), (open_dependencies, 0))?;
        if let Some(parent) = spec.parent_id {
            self.adjust_open_children(parent, 1)?;
        }
        if open_dependencies == 0 && !spec.kind.is_group() {
            self.place(job)?;
        }
        let parent_key = spec.parent_id.map_or(0, JobId::get);
        self.step(|tx| {
            tx.open_table(CHILDREN)?
                .insert((parent_key, job.get()), ())?;
            if let Some(previous) = spec.kind.previous() {
                tx.open_table(SUCCESSORS)?.insert(previous.get(), ())?;
            }
            Ok(())
        })?;
        if let Some((key, fingerprint)) = submission {
            let record = SubmissionRecord {
                key,
                fingerprint,
                job,
                seq: 0,
                recorded_at: self.now,
            };
            self.insert(
                SUBMISSIONS,
                key.as_bytes().as_slice(),
                encode_submission(&record).as_slice(),
            )?;
            self.probe = Some(Probe::Submission(key));
        }
        Ok(job)
    }

    /// All T1 refusals occur before allocating or writing any rows. Event
    /// appends can keep their source transaction and fall back to a notice.
    pub(crate) fn validate_spec(&mut self, spec: &JobSpec) -> Result<(), BatchError> {
        if let JobKind::Execution {
            execution: JobExecution::Decision(work),
            ..
        } = &spec.kind
        {
            if let Err(message) = work.validate() {
                return self.reject(Violation::InvalidValue("execution", message));
            }
            if work.prediction.is_some()
                && !self.step(|tx| {
                    reader::prediction_current(
                        &tx.open_table(META)?,
                        &tx.open_table(JOBS)?,
                        &tx.open_table(CONFIGS)?,
                        &tx.open_table(PROJECTS)?,
                        &tx.open_table(JOB_CONTROLS)?,
                        reader::Turns(&tx.open_table(TURNS)?),
                        spec,
                    )
                })?
            {
                return self.reject(Violation::InvalidValue(
                    "execution",
                    "prediction binding is no longer current",
                ));
            }
            if !self.open_pools()?.iter().any(|pool| {
                pool.body.enabled && pool.offers(decision_work_requirement(&work.frozen))
            }) {
                return self.reject(Violation::InvalidValue(
                    "execution",
                    "no enabled Executor offers decision work",
                ));
            }
        }
        if let JobKind::Execution {
            execution: JobExecution::Summarization(work),
            ..
        } = &spec.kind
        {
            if let Err(reason) = work.validate_stage() {
                return self.reject(Violation::InvalidValue("execution", reason));
            }
            if work.chain.is_some() {
                self.step(|tx| reader::summary_root(&tx.open_table(JOBS)?, work))?;
            }
            let manifest = self
                .step(|tx| reader::load_context_manifest(&tx.open_table(MESSAGES)?, work.source))?;
            let parent = spec
                .parent_id
                .map(|parent| self.require_job(parent))
                .transpose()?;
            if !parent.is_some_and(|parent| {
                parent.parent_id == Some(manifest.conversation)
                    && matches!(
                        parent.kind,
                        JobKind::Group {
                            origin: Origin::System,
                            ..
                        }
                    )
            }) || !self.source_still_matches(&manifest)?
            {
                return self.reject(Violation::InvalidValue(
                    "execution",
                    "summarization must name a valid sealed source in its parent conversation",
                ));
            }
        }
        self.check_spec_references(spec)?;
        self.validate_job_allocation(spec)?;
        for (dependency, _) in spec.kind.dependencies() {
            if Some(*dependency) == spec.kind.previous() {
                continue;
            }
            let sibling = self.require_job(*dependency)?;
            let valid = self.dependency_is_allowed(spec, &sibling)?;
            if !valid {
                return self.reject(Violation::InvalidValue(
                    "dependencies",
                    "must name the previous turn or an earlier sibling conversation",
                ));
            }
        }
        if let Some(parent) = spec.parent_id {
            let state = self.require_job_state(parent)?;
            if state.outcome.is_some() {
                return self.reject(Violation::ParentClosed(parent));
            }
            let parent_job = self.require_job(parent)?;
            if parent_job.kind.is_group() && self.job_control(parent)?.closing.is_some() {
                return self.reject(Violation::ParentClosed(parent));
            }
        }
        let mut previous = None;
        for (dependency, condition) in spec.kind.waits_on() {
            if Some(dependency) != spec.kind.previous() {
                if previous.is_some_and(|previous| dependency <= previous) {
                    return self.reject(Violation::InvalidValue(
                        "dependencies",
                        "must be sorted by Job and unique",
                    ));
                }
                previous = Some(dependency);
            } else if spec
                .kind
                .dependencies()
                .iter()
                .any(|(id, _)| *id == dependency)
            {
                return self.reject(Violation::InvalidValue(
                    "dependencies",
                    "must not repeat the previous turn",
                ));
            }
            self.require_job(dependency)?;
            if self.is_subtree_settled(dependency)? {
                let completed = self
                    .require_job_state(dependency)?
                    .outcome
                    .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
                if condition == DependencyCondition::Success && !completed {
                    return self.reject(Violation::UnsatisfiedDependency {
                        job: JobId::new(0),
                        prerequisite: dependency,
                        state: DependencyState::Impossible,
                    });
                }
            }
        }
        for attachment in spec.kind.attachments() {
            if !self.require_attachment(*attachment)?.is_available() {
                return self.reject(Violation::AttachmentCleanedUp(*attachment));
            }
        }
        let stored_tip = if spec.kind.is_persistent()
            && let Some(conversation) = spec.parent_id
        {
            let tip = self.group_head(conversation)?;
            if tip != spec.kind.previous() {
                return self.reject(Violation::InvalidValue(
                    "previous",
                    "must be the tip at creation",
                ));
            }
            Some(tip)
        } else {
            None
        };
        if let Some(previous) = spec.kind.previous() {
            let prior = self.require_job(previous)?;
            if prior.parent_id != spec.parent_id || !prior.kind.is_persistent() {
                return self.reject(Violation::InvalidValue(
                    "previous",
                    "must name the prior turn of this conversation",
                ));
            }
            let tip = match stored_tip {
                Some(tip) => tip,
                None => self.group_head(spec.parent_id.ok_or_else(|| {
                    BatchError(Failure::Storage(StorageError::corrupted(
                        "missing conversation",
                    )))
                })?)?,
            };
            if tip != Some(previous) {
                return self.reject(Violation::InvalidValue("previous", "must be the tip"));
            }
            if self.step(|tx| Ok(tx.open_table(SUCCESSORS)?.get(previous.get())?.is_some()))? {
                return self.reject(Violation::InvalidValue(
                    "previous",
                    "already has a successor",
                ));
            }
        }
        let pending = self.step(|tx| Ok(tx.open_table(PENDING)?.len()?))?;
        if !spec.kind.is_group() && pending + 1 > P_MAX && !self.event_append {
            return self.reject(Violation::PendingJobsLimit {
                max_pending_jobs: P_MAX,
            });
        }

        Ok(())
    }

    fn check_spec_references(&mut self, spec: &JobSpec) -> Result<(), BatchError> {
        match &spec.kind {
            JobKind::Group { agent, project, .. } => {
                if let Some(agent) = agent {
                    // A conversation is held by an Agent Profile, or by a
                    // Decision Profile when it carries typed decision work.
                    let row = self.require_config(*agent)?;
                    if row.archived_at.is_some()
                        || !matches!(row.kind, ConfigKind::Agent | ConfigKind::DecisionAgent)
                    {
                        return self.reject(Violation::WrongConfigKind {
                            config: *agent,
                            expected: ConfigKind::Agent,
                            found: row.kind,
                        });
                    }
                }
                if let Some(project) = project {
                    self.require_project(*project)?;
                }
            }
            JobKind::Execution { .. } => {
                if let JobKind::Execution {
                    execution: JobExecution::Ref(owner),
                    ..
                } = &spec.kind
                {
                    let owner = self.require_job(*owner)?;
                    if !matches!(
                        owner.kind,
                        JobKind::Execution {
                            execution: JobExecution::Frozen(_),
                            ..
                        }
                    ) || owner.parent_id != spec.parent_id
                    {
                        return self.reject(Violation::InvalidValue(
                            "execution",
                            "reference must name a Frozen owner in this conversation",
                        ));
                    }
                }
                if let JobKind::Execution {
                    execution: JobExecution::Decision(work),
                    ..
                } = &spec.kind
                {
                    match &work.frozen {
                        FrozenDecisionConfig::Native(native) => {
                            if let Some(agent) = native.agent_ref {
                                self.require_config_kind(
                                    agent.id,
                                    native
                                        .agent
                                        .as_ref()
                                        .map_or(ConfigKind::DecisionAgent, |profile| {
                                            profile.kind()
                                        }),
                                )?;
                            }
                            self.require_config_kind(
                                native.model_ref.id,
                                ConfigKind::DecisionModel,
                            )?;
                        }
                        FrozenDecisionConfig::Generative(generative, profile) => {
                            self.require_config_kind(generative.llm.model.id, ConfigKind::Model)?;
                            if let Some((_, reference)) = profile {
                                self.require_config_kind(reference.id, ConfigKind::DecisionAgent)?;
                            }
                            if let Some(agent) = generative.agent_ref {
                                self.require_config_kind(agent.id, ConfigKind::Agent)?;
                            }
                        }
                    }
                    self.require_config_kind(
                        work.frozen.connection_ref().id,
                        ConfigKind::Connection,
                    )?;
                    return Ok(());
                }
                let (execution, _, agent) = self.job_execution(spec)?;
                if spec.kind.is_persistent() && execution.agent_ref.map(|agent| agent.id) != agent {
                    return self.reject(Violation::InvalidValue(
                        "agent",
                        "frozen Agent differs from the conversation",
                    ));
                }
                self.require_config_kind(execution.llm.model.id, ConfigKind::Model)?;
                self.require_config_kind(execution.llm.provider.id.into(), ConfigKind::Connection)?;
            }
        }
        Ok(())
    }

    /// The global request fields and their revision.
    pub(crate) fn global_headers(
        &mut self,
    ) -> Result<crate::db::entities::GlobalHeaders, BatchError> {
        self.step(|tx| reader::global_outbound_headers(&tx.open_table(META)?))
    }

    /// Replaces the global request fields. `expected_revision` is the
    /// revision the caller read; a stale one is refused, so two editors never
    /// overwrite each other. A Job already frozen keeps its own copy, and one
    /// still waiting to start is refused at its checked handoff.
    pub(crate) fn save_global_headers(
        &mut self,
        expected_revision: u32,
        fields: Vec<crate::db::entities::OutboundHeader>,
    ) -> Result<crate::db::entities::GlobalHeaders, BatchError> {
        use crate::db::entities::{HeaderScope, validate_fields};
        let current = self.global_headers()?;
        if current.revision != expected_revision {
            return self.reject(Violation::InvalidValue(
                "revision",
                "global headers changed; reload before saving",
            ));
        }
        if let Err(error) = validate_fields(&fields, HeaderScope::Global) {
            return self.reject(Violation::InvalidValue("headers", error.reason()));
        }
        let Some(revision) = current.revision.checked_add(1) else {
            return self.corrupted("global header revision exhausted".into());
        };
        let saved = crate::db::entities::GlobalHeaders { revision, fields };
        self.step(|tx| {
            let mut e = Enc::new(TAG_GLOBAL_HEADERS);
            e.u32(saved.revision);
            e.outbound_headers(&saved.fields);
            tx.open_table(META)?
                .insert("outbound_headers", e.0.as_slice())?;
            Ok(())
        })?;
        Ok(saved)
    }

    /// Reads the configs a Job is submitted with and freezes their values
    /// (section 4.1.4): the Model, its Connection and, when named, the Agent.
    /// The Agent must list the Model.
    pub(crate) fn freeze_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<FrozenConfiguration, BatchError> {
        if configuration.agent_profile.is_none() {
            return self.reject(Violation::InvalidValue(
                "agent_profile_required",
                "name an Agent that lists this Model",
            ));
        }
        let global = self.global_headers()?;
        crate::service::jobs::ConfigurationResolver::new(|id| {
            self.config(id).map(|row| row.map(reader::config_record))
        })
        .with_global_headers(global)
        .freeze_configuration(configuration)
    }

    /// Freezes a configuration without requiring an Agent.
    ///
    /// The only callers of this method are:
    /// - the compaction summarizer (`job_records.rs:229-240`);
    /// - CLI process turns, through `spec_of` (`db/mutations.rs:85`).
    ///
    /// Title jobs (`job_records.rs:2195-2257`) reuse the parent turn's Model
    /// with the Agent stripped, and never freeze; they are a derivative the
    /// Agent already validated.
    pub(crate) fn freeze_system_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<FrozenConfiguration, BatchError> {
        let global = self.global_headers()?;
        crate::service::jobs::ConfigurationResolver::new(|id| {
            self.config(id).map(|row| row.map(reader::config_record))
        })
        .with_global_headers(global)
        .freeze_configuration(configuration)
    }

    // ---- placement (8.10) ---------------------------------------------------------

    /// What a Job's ready row requires. A decision turn names its typed
    /// operation; every other turn derives its requirement from the frozen
    /// generative transport.
    pub(super) fn execution_key(&mut self, row: &JobSpec) -> Result<u16, BatchError> {
        match &row.kind {
            JobKind::Execution {
                execution: JobExecution::Decision(work),
                ..
            } => Ok(decision_work_requirement(&work.frozen)),
            JobKind::Group { .. } | JobKind::Execution { .. } => {
                Ok(execution_requirement(&self.job_execution(row)?.0))
            }
        }
    }

    /// PLACE: a pending Job with no open dependency becomes `ready`, or
    /// `delayed` until its `eligible_at`.
    pub(crate) fn place(&mut self, job: JobId) -> Result<(), BatchError> {
        let row = self.require_job(job)?;
        let requirement = self.execution_key(&row)?;
        let blocked = self.is_held(&row)?
            || match (row.kind.is_persistent(), row.parent_id) {
                (true, Some(parent)) => {
                    self.active_job(parent)?.is_some_and(|active| active != job)
                }
                _ => false,
            };
        if blocked {
            return Ok(());
        }
        let eligible_at = row.kind.eligible_at().map(Timestamp::as_unix_micros);
        let clock = recovery::promotion_clock();
        self.touched_ready_or_delayed = true;
        self.step(|tx| {
            match eligible_at {
                Some(eligible_at) if eligible_at > clock => {
                    tx.open_table(DELAYED)?
                        .insert((eligible_at, job.get()), ())?;
                }
                _ => {
                    tx.open_table(READY)?.insert((requirement, job.get()), ())?;
                }
            }
            Ok(())
        })
    }

    /// PROMOTE: the `delayed` rows whose time has come become `ready`.
    pub(crate) fn promote(&mut self) -> Result<(), BatchError> {
        let clock = recovery::promotion_clock();
        let due = self.step(|tx| {
            let delayed = tx.open_table(DELAYED)?;
            let mut due = Vec::new();
            for entry in delayed.range((i64::MIN, 0)..=(clock, u128::MAX))? {
                due.push(entry?.0.value());
            }
            Ok(due)
        })?;
        for (eligible_at, job) in due {
            let row = self.require_job(JobId::new(job))?;
            let requirement = self.execution_key(&row)?;
            self.touched_ready_or_delayed = true;
            self.step(|tx| {
                tx.open_table(DELAYED)?.remove((eligible_at, job))?;
                tx.open_table(READY)?.insert((requirement, job), ())?;
                Ok(())
            })?;
        }
        Ok(())
    }

    // ---- T2 claim batch (8.3) -------------------------------------------------------

    fn open_pools(&mut self) -> Result<Vec<OpenPool>, BatchError> {
        self.step(|tx| {
            let configs = tx.open_table(CONFIGS)?;
            let held = tx.open_table(HELD_SLOTS)?;
            let mut pools = Vec::new();
            for entry in configs.iter()? {
                let (key, bytes) = entry?;
                let row = ConfigRow::decode_at(key.value(), bytes.value())?;
                if row.archived_at.is_some() {
                    continue;
                }
                let ConfigBody::Pool(body) = row.body else {
                    continue;
                };
                let id = row.id.get();
                let mut count = 0u32;
                for slot in held.range((id, 0)..=(id, u128::MAX))? {
                    slot?;
                    count += 1;
                }
                pools.push(OpenPool {
                    id: row.id,
                    edit_seq: row.edit_seq,
                    body,
                    held: count,
                });
            }
            Ok(pools)
        })
    }

    fn held_background(&mut self) -> Result<(u32, u32), BatchError> {
        self.step(|tx| {
            let jobs = tx.open_table(JOBS)?;
            let mut counts = (0u32, 0u32);
            for entry in tx
                .open_table(HELD_SLOTS)?
                .iter()?
                .take(usize::from(GLOBAL_EXECUTION_LIMIT) + 1)
            {
                let id = entry?.0.value().1;
                let Some(bytes) = jobs.get(id)? else {
                    return Err(StorageError::corrupted("held Job missing"));
                };
                match reader::work_priority(&jobs, &JobSpec::decode_at(id, bytes.value())?)? {
                    2 => counts.0 += 1,
                    1 => counts.1 += 1,
                    _ => {}
                }
            }
            Ok(counts)
        })
    }

    pub(super) fn has_executor_for(
        &mut self,
        operation: ExecutionOperation,
        protocol: Option<LlmTransport>,
    ) -> Result<bool, BatchError> {
        let key = requirement(Some(operation), protocol);
        Ok(self
            .open_pools()?
            .iter()
            .any(|pool| pool.body.enabled && pool.offers(key)))
    }

    fn held_slot_count(&mut self) -> Result<u32, BatchError> {
        let count = self.step(|tx| Ok(tx.open_table(HELD_SLOTS)?.len()?))?;
        Ok(u32::try_from(count).unwrap_or(u32::MAX))
    }

    /// CLAIM_BATCH: promotes due Jobs, then claims ready Jobs, smallest Job
    /// id first, while a global slot and a compatible Pool are free.
    pub(crate) fn claim_batch(&mut self) -> Result<Vec<JobId>, BatchError> {
        self.promote()?;
        self.cancel_stale_context_work()?;
        self.recover_predictions()?;
        let mut free = u32::from(GLOBAL_EXECUTION_LIMIT).saturating_sub(self.held_slot_count()?);
        let mut pools: Vec<OpenPool> = self
            .open_pools()?
            .into_iter()
            .filter(OpenPool::has_room)
            .collect();
        let (mut speculative, mut automatic) = self.held_background()?;
        let mut grants = Vec::new();
        let mut deferred = BTreeSet::new();
        while free > 0 {
            let offered: BTreeSet<u16> = pools
                .iter()
                .flat_map(|pool| {
                    pool.body
                        .capabilities
                        .iter()
                        .map(|(operation, protocol)| requirement(Some(*operation), *protocol))
                })
                .collect();
            let mut candidate: Option<(u8, u128, u16)> = None;
            for key in offered {
                let first = self.step(|tx| {
                    let ready = tx.open_table(READY)?;
                    let jobs = tx.open_table(JOBS)?;
                    let mut first = None;
                    for entry in ready.range((key, 0)..=(key, u128::MAX))?.take(256) {
                        let id = entry?.0.value().1;
                        if deferred.contains(&id) {
                            continue;
                        }
                        let Some(bytes) = jobs.get(id)? else {
                            return Err(StorageError::corrupted("ready Job missing"));
                        };
                        let row = JobSpec::decode_at(id, bytes.value())?;
                        if !row.kind.is_persistent() {
                            continue;
                        }
                        let priority = reader::work_priority(&jobs, &row)?;
                        if priority == 2 && (speculative >= 1 || free < 2)
                            || priority == 1 && automatic >= 2
                        {
                            continue;
                        }
                        let chosen = (priority, id, key);
                        if first.is_none_or(|best| chosen < best) {
                            first = Some(chosen);
                        }
                        if priority == 0 {
                            break;
                        }
                    }
                    Ok(first)
                })?;
                if let Some(first) = first
                    && candidate.is_none_or(|best| first < best)
                {
                    candidate = Some(first);
                }
            }
            let Some((priority, job, key)) = candidate else {
                break;
            };
            // Policy is resolved before a slot is reserved. A turn that still
            // needs preparation stays pending; reserving its pool room first
            // would leave that room held with no grant.
            if self.prepare_if_needed(JobId::new(job))? {
                deferred.insert(job);
                continue;
            }
            let Some(index) = pools
                .iter()
                .enumerate()
                .filter(|(_, pool)| pool.offers(key))
                .min_by_key(|(_, pool)| (pool.body.capabilities.len(), pool.id))
                .map(|(index, _)| index)
            else {
                break;
            };
            let (pool, edit_seq, capabilities) = {
                let pool = &mut pools[index];
                pool.held += 1;
                (pool.id, pool.edit_seq, pool.body.capabilities.clone())
            };
            self.write_claim(JobId::new(job), key, pool, edit_seq, capabilities)?;
            if priority == 2 {
                speculative += 1;
            } else if priority == 1 {
                automatic += 1;
            }
            grants.push(JobId::new(job));
            free -= 1;
            if !pools[index].has_room() {
                pools.remove(index);
            }
        }
        if let Some(first) = grants.first() {
            self.probe = Some(Probe::Claim(*first));
        }
        Ok(grants)
    }

    /// Prepares when the resolved policy says the turn is over its trigger,
    /// and reports that the turn should stay pending. Disabled, an unknown
    /// window, and a missing summarizer do not block the turn. The preset
    /// numbers are the uncalibrated proposal.
    fn prepare_if_needed(&mut self, job: JobId) -> Result<bool, BatchError> {
        let row = self.require_job(job)?;
        if !self.step(|tx| {
            reader::prediction_current(
                &tx.open_table(META)?,
                &tx.open_table(JOBS)?,
                &tx.open_table(CONFIGS)?,
                &tx.open_table(PROJECTS)?,
                &tx.open_table(JOB_CONTROLS)?,
                reader::Turns(&tx.open_table(TURNS)?),
                &row,
            )
        })? {
            self.finalize(job, OutcomeKind::Cancelled, None, Some(FinalResult {
                summary: Some("context prediction opt-in, assignment or activity changed before execution".into()),
                code: Some("prediction_stale".into()), goal_reached: Some(false), value: None,
            }), false)?;
            return Ok(true);
        }
        if !matches!(
            row.kind,
            JobKind::Execution {
                execution: JobExecution::Frozen(_) | JobExecution::Ref(_),
                plan_source: PlanSource::Stored { .. },
                ..
            }
        ) {
            return Ok(false);
        }
        let Some(conversation) = row.parent_id else {
            return Ok(false);
        };
        self.select_context(job)?;
        let (policy, revision, profile) =
            self.step(|tx| reader::effective_context_policy(&tx.open_table(META)?))?;
        if matches!(policy.mode, crate::db::entities::CompactionMode::Off) {
            return Ok(false);
        }
        let (frozen, _, _) = self.job_execution(&row)?;
        let model = &frozen.llm.config;
        let output = model
            .parameters
            .max_completion_tokens
            .or(model.parameters.max_tokens)
            .unwrap_or(4096);
        let (mut occupancy, growth, _) = self.step(|tx| {
            reader::raw_context_usage(
                &tx.open_table(JOBS)?,
                &tx.open_table(JOB_STATES)?,
                &tx.open_table(JOB_INPUTS)?,
                &tx.open_table(JOB_OUTPUTS)?,
                job,
            )
        })?;
        if occupancy == usize::MAX {
            return Ok(false);
        }
        if let Some(adopted) = self.adopt_context(job)? {
            let reducible = self.step(|tx| reader::source_reducible(tx, &adopted.source))?;
            occupancy = occupancy.saturating_sub(reducible);
            occupancy =
                occupancy.saturating_add(adopted.summary.len().saturating_add(128).div_ceil(3));
        }
        let safety = u32::try_from(
            frozen
                .agent
                .as_ref()
                .and_then(|agent| agent.instructions.as_ref())
                .map_or(0, String::len)
                .div_ceil(3),
        )
        .unwrap_or(u32::MAX)
        .saturating_add(256);
        let Ok(recipe) = policy.recipe(model.context_window, output, safety, growth.max(output))
        else {
            return Ok(false);
        };
        if recipe
            .trigger
            .is_none_or(|trigger| occupancy <= trigger as usize)
        {
            return Ok(false);
        }
        let summarizer = policy.summarization_model.unwrap_or(frozen.llm.model.id);
        if !self.config(summarizer)?.is_some_and(|record| {
            record.archived_at.is_none() && matches!(record.body, ConfigBody::Model(_))
        }) {
            return Ok(false);
        }
        let summarizer_row = self.require_config(summarizer)?;
        let ConfigBody::Model(summarizer_body) = summarizer_row.body else {
            return Ok(false);
        };
        let Some(connection) = self.config(summarizer_body.connection_id)? else {
            return Ok(false);
        };
        let ConfigBody::Connection(connection_body) = connection.body else {
            return Ok(false);
        };
        if connection.archived_at.is_some()
            || !self.open_pools()?.iter().any(|pool| {
                pool.body.enabled
                    && pool.offers(requirement(
                        Some(ExecutionOperation::LlmRequest),
                        Some(connection_body.protocol),
                    ))
            })
        {
            return Ok(false);
        }
        let mut identity = [0u8; 32];
        identity[..16].copy_from_slice(&conversation.get().to_le_bytes());
        identity[16..].copy_from_slice(&job.get().to_le_bytes());
        let key = crate::db::entities::SubmissionKey::from_digest(identity);
        if let Some(submission) = self.submission(key)? {
            return Ok(self.require_job_state(submission.job)?.outcome.is_none());
        }
        for producer in self.context_compaction_jobs(conversation)? {
            let producer_row = self.require_job(producer)?;
            let JobKind::Execution {
                execution: JobExecution::Summarization(work),
                ..
            } = producer_row.kind
            else {
                continue;
            };
            let binding = self.step(|tx| reader::summary_root(&tx.open_table(JOBS)?, &work))?;
            if binding
                .policy_revision
                .is_some_and(|bound| bound != revision || binding.policy_profile != profile)
            {
                continue;
            }
            let state = self.require_job_state(producer)?;
            if binding.prediction.is_some()
                && state
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind != OutcomeKind::Completed)
            {
                continue;
            }
            if state.outcome.is_none() {
                return Ok(true);
            }
            if state.outcome.is_some_and(|outcome| {
                self.now
                    .as_unix_micros()
                    .saturating_sub(outcome.at.as_unix_micros())
                    < i64::from(recipe.cooldown_seconds).saturating_mul(1_000_000)
            }) {
                return Ok(false);
            }
        }
        if !self.background_room(false)? {
            return Ok(false);
        }
        let submission = crate::db::entities::Submission {
            key,
            fingerprint: crate::db::entities::RequestFingerprint::from_digest(identity),
        };
        match self.request_compaction_for(conversation, summarizer, submission, Some(recipe))? {
            crate::db::entities::CompactionRequest::Prepared { job, .. } => {
                self.insert_turn_input(
                    row.id,
                    Entry::ContextPreparation {
                        job,
                        policy_revision: revision,
                    },
                )?;
                Ok(self.require_job_state(job)?.outcome.is_none())
            }
            crate::db::entities::CompactionRequest::Noop { .. } => Ok(false),
        }
    }

    /// Writes the claim of `job` into `pool`: the claim itself, the Job's
    /// leaving `ready` and `pending`, and its held slot.
    fn write_claim(
        &mut self,
        job: JobId,
        requirement_key: u16,
        pool: ConfigId,
        pool_edit_seq: u32,
        capabilities: BTreeSet<(ExecutionOperation, Option<LlmTransport>)>,
    ) -> Result<(), BatchError> {
        let mut state = self.require_job_state(job)?;
        let row = self.require_job(job)?;
        let conversation = row.kind.is_persistent().then_some(row.parent_id).flatten();
        let notice_seq = match conversation {
            Some(conversation) => self.last_notice_seq(conversation)?,
            None => 0,
        };
        if let Some(conversation) = conversation {
            self.step(|tx| {
                tx.open_table(ACTIVE_JOBS)?
                    .insert(conversation.get(), job.get())?;
                tx.open_table(NOTICE_READ)?
                    .insert(conversation.get(), notice_seq)?;
                Ok(())
            })?;
            // Adoption is checked in this claim. A mismatch leaves the
            // original records in the prompt; it does not refuse the claim.
            self.select_context(job)?;
            let _adopted = self.adopt_context(job)?;
        }
        state.claim = Some(ClaimRecord {
            started_at: self.now,
            pool_id: pool,
            pool_edit_seq,
            capabilities,
            epoch: self.epoch,
            host_id: self.host_id.clone(),
            boot_id: self.boot_id.clone(),
            notice_seq,
        });
        self.put_job_state(job, &state)?;
        let epoch = self.epoch;
        self.step(|tx| {
            tx.open_table(READY)?.remove((requirement_key, job.get()))?;
            tx.open_table(PENDING)?.remove(job.get())?;
            tx.open_table(HELD_SLOTS)?
                .insert((pool.get(), job.get()), (epoch, false))?;
            Ok(())
        })
    }

    /// Claims one named ready Job into the first of `allowed` that is
    /// enabled, offers what the Job needs and has room. The same limits as
    /// [`Self::claim_batch`] apply. Context preparation may be admitted while
    /// the foreground turn remains pending.
    pub(crate) fn claim_job(
        &mut self,
        job: JobId,
        allowed: &BTreeSet<ConfigId>,
    ) -> Result<Result<ClaimGrant, ClaimRefusal>, BatchError> {
        let Some(row) = self.job(job)? else {
            return Ok(Err(ClaimRefusal::NotFound));
        };
        let state = self.require_job_state(job)?;
        if state.claim.is_some() || state.outcome.is_some() {
            return Ok(Err(ClaimRefusal::NotPending));
        }
        if allowed.is_empty() {
            return Ok(Err(ClaimRefusal::EmptyAllowedPools));
        }
        self.promote()?;
        let requirement_key = self.execution_key(&row)?;
        let (ready, delayed) = self.step(|tx| {
            let ready = tx
                .open_table(READY)?
                .get((requirement_key, job.get()))?
                .is_some();
            let mut delayed = None;
            if let Some(eligible_at) = row.kind.eligible_at().map(Timestamp::as_unix_micros)
                && tx
                    .open_table(DELAYED)?
                    .get((eligible_at, job.get()))?
                    .is_some()
            {
                delayed = Some(eligible_at);
            }
            Ok((ready, delayed))
        })?;
        if !ready {
            if let Some(eligible_at) = delayed {
                return Ok(Err(ClaimRefusal::NotYetEligible {
                    eligible_at: Timestamp::from_unix_micros(eligible_at),
                }));
            }
            let prerequisite = self.first_open_dependency(&row)?;
            return Ok(Err(match prerequisite {
                Some(prerequisite) => ClaimRefusal::DependencyUnsatisfied {
                    prerequisite,
                    state: DependencyState::Waiting,
                },
                None => ClaimRefusal::NotPending,
            }));
        }
        let held = self.held_slot_count()?;
        if held >= u32::from(GLOBAL_EXECUTION_LIMIT) {
            return Ok(Err(ClaimRefusal::NoCapacity(CapacityScope::Global)));
        }
        let pools = self.open_pools()?;
        let mut compatible = false;
        let mut chosen = None;
        for pool in pools.iter().filter(|pool| allowed.contains(&pool.id)) {
            if !pool.body.enabled || !pool.offers(requirement_key) {
                continue;
            }
            compatible = true;
            if pool.held < pool.body.concurrency_limit {
                chosen = Some(pool);
                break;
            }
        }
        let Some(pool) = chosen else {
            return Ok(Err(if compatible {
                ClaimRefusal::NoCapacity(CapacityScope::Pools(
                    allowed
                        .iter()
                        .map(|pool| ExecutionPoolId::from(*pool))
                        .collect(),
                ))
            } else {
                ClaimRefusal::NoCompatiblePool
            }));
        };
        let (pool_id, edit_seq, pool_capabilities) =
            (pool.id, pool.edit_seq, pool.body.capabilities.clone());
        if self.prepare_if_needed(job)? {
            return Ok(Err(ClaimRefusal::NotPending));
        }
        let priority = self.step(|tx| reader::work_priority(&tx.open_table(JOBS)?, &row))?;
        let (speculative, automatic) = self.held_background()?;
        if priority == 2
            && (speculative >= 1
                || self.held_slot_count()?.saturating_add(1) >= u32::from(GLOBAL_EXECUTION_LIMIT))
            || priority == 1 && automatic >= 2
        {
            return Ok(Err(ClaimRefusal::NoCapacity(CapacityScope::Global)));
        }
        self.write_claim(
            job,
            requirement_key,
            pool_id,
            edit_seq,
            pool_capabilities.clone(),
        )?;
        self.probe = Some(Probe::Claim(job));
        Ok(Ok(ClaimGrant {
            job,
            binding: FrozenPoolBinding {
                pool: pool_id.into(),
                edit_seq,
                capabilities: pool_capabilities,
            },
            epoch: self.epoch,
            global_free_after: u8::try_from(
                u32::from(GLOBAL_EXECUTION_LIMIT).saturating_sub(held + 1),
            )
            .unwrap_or(0),
        }))
    }

    fn first_open_dependency(&mut self, row: &JobSpec) -> Result<Option<JobId>, BatchError> {
        for (dependency, _) in row.kind.waits_on() {
            if !self.is_subtree_settled(dependency)? {
                return Ok(Some(dependency));
            }
        }
        Ok(None)
    }

    fn adjust_open_children(&mut self, job: JobId, delta: i64) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut unsettled = tx.open_table(UNSETTLED)?;
            let counts = unsettled.get(job.get())?.map(|counts| counts.value());
            let Some((dependencies, children)) = counts else {
                return Ok(());
            };
            let children = i64::from(children)
                .checked_add(delta)
                .and_then(|children| u32::try_from(children).ok())
                .ok_or_else(|| {
                    StorageError::corrupted(format!(
                        "{job} has {children} open children, which cannot change by {delta}"
                    ))
                })?;
            unsettled.insert(job.get(), (dependencies, children))?;
            Ok(())
        })
    }

    // ---- tool decision and outcome (8.4) --------------------------------------------

    /// TOOL_DECIDE: records OCG's decision about a call before anything about
    /// it can run (I12). `spawns` says whether the admitted handler can start
    /// an OS process.
    pub(crate) fn tool_decide(
        &mut self,
        call: ToolCallRef,
        tool_call_id: &str,
        name: &str,
        decision: ToolDecision,
        spawns: bool,
        approval: Option<ToolCallRef>,
    ) -> Result<(), BatchError> {
        self.require_executing_here(call.job)?;
        let turn = self.require_job(call.job)?;
        let conversation = if turn.kind.is_persistent() {
            turn.parent_id
        } else {
            None
        };
        if let Some(original) = approval {
            let Some(conversation) = conversation else {
                return self.reject(Violation::InvalidValue(
                    "approval_executed",
                    "no matching approval in this conversation",
                ));
            };
            if self.job_control(conversation)?.closing.is_some() {
                return self.reject(Violation::InvalidValue(
                    "conversation_closing",
                    "approved calls cannot run while closing",
                ));
            }
            let eligible = self.step(|tx| {
                Ok(tx
                    .open_table(OPEN_APPROVALS)?
                    .get(approvals::open_key(conversation, original))?
                    .is_some_and(|v| v.value() == 1)
                    && tx
                        .open_table(APPROVAL_CALLS)?
                        .get(tool_call_key(original))?
                        .is_none())
            })?;
            if !eligible {
                return self.reject(Violation::InvalidValue(
                    "approval_executed",
                    "approval is unavailable or already executed",
                ));
            }
            let Some(original) = self.tool_evidence(original)? else {
                return self.reject(Violation::InvalidValue(
                    "approval_not_found",
                    "awaiting call missing",
                ));
            };
            if original.name != name {
                return self.reject(Violation::InvalidValue(
                    "name",
                    "must equal the approved tool name",
                ));
            }
            let (ToolDecision::AwaitingApproval(awaited) | ToolDecision::AwaitingAnswer(awaited)) =
                &original.decision.fact
            else {
                return self.reject(Violation::InvalidValue(
                    "approval_executed",
                    "the approved call was not awaiting a decision",
                ));
            };
            match &decision {
                ToolDecision::Admitted(admitted) if admitted != awaited => {
                    return self.reject(Violation::InvalidValue(
                        "admission",
                        "must equal the approved admission",
                    ));
                }
                ToolDecision::AwaitingApproval(_) | ToolDecision::AwaitingAnswer(_) => {
                    return self.reject(Violation::InvalidValue(
                        "decision",
                        "an approved call cannot await another decision",
                    ));
                }
                ToolDecision::Admitted(_) | ToolDecision::Refused(_) => {}
            }
            if call.request == RequestNumber::FIRST
                && !self.request_observed(call.job, call.request)?
            {
                self.insert_request_event(
                    call.job,
                    call.request,
                    self.now,
                    crate::db::entities::RequestPayload::ApprovedCalls,
                )?;
            }
        }
        if matches!(
            decision,
            ToolDecision::AwaitingApproval(_) | ToolDecision::AwaitingAnswer(_)
        ) && conversation.is_none()
        {
            return self.reject(Violation::InvalidValue(
                "approve",
                "awaiting calls require a conversation",
            ));
        }
        if !self.request_observed(call.job, call.request)? {
            return self.reject(Violation::RequestNotObserved {
                job: call.job,
                request: call.request,
            });
        }
        let key = tool_call_key(call);
        if self.tool_evidence(call)?.is_some() {
            return self.reject(Violation::DuplicateToolCall(call));
        }
        let existing = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_CALL_IDS)?
                .get((call.job.get(), tool_call_id))?
                .map(|found| found.value()))
        })?;
        if let Some((request, index)) = existing {
            let existing = tool_call_ref((call.job.get(), request, index))
                .map_err(|error| BatchError(Failure::Storage(error)))?;
            return self.reject(Violation::DuplicateToolCallId { call, existing });
        }
        let current_stats = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_STATS)?
                .get(name)?
                .map(|found| found.value()))
        })?;
        let mut stats_changed = false;
        let mut updated_stats = current_stats.unwrap_or((0, 0, 0, 0, 0, 0));
        match &decision {
            ToolDecision::Admitted(_) => {
                updated_stats.0 = updated_stats.0.saturating_add(1);
                stats_changed = true;
            }
            ToolDecision::Refused(ToolCallRefusal::InvalidArguments) => {
                updated_stats.1 = updated_stats.1.saturating_add(1);
                stats_changed = true;
            }
            ToolDecision::AwaitingApproval(_) | ToolDecision::AwaitingAnswer(_) => {}
            ToolDecision::Refused(
                ToolCallRefusal::Malformed
                | ToolCallRefusal::ConflictingIdentity
                | ToolCallRefusal::AmbiguousIdentity
                | ToolCallRefusal::LimitExceeded
                | ToolCallRefusal::Incomplete
                | ToolCallRefusal::Unauthorized
                | ToolCallRefusal::MissingSchema
                | ToolCallRefusal::UnsupportedSchema
                | ToolCallRefusal::NoDecider,
            ) => {}
        }
        let record = ToolEvidenceRecord {
            call,
            approval,
            tool_call_id: tool_call_id.to_owned(),
            name: name.to_owned(),
            spawns: spawns && matches!(decision, ToolDecision::Admitted(_)),
            decision: Recorded {
                fact: decision,
                at: self.now,
            },
            outcome: None,
            reconciliation: None,
        };
        self.step(|tx| {
            tx.open_table(TOOL_CALLS)?
                .insert(key, encode_tool_evidence(&record).as_slice())?;
            tx.open_table(TOOL_CALL_IDS)?.insert(
                (call.job.get(), tool_call_id),
                (call.request.get(), call.index),
            )?;
            if stats_changed || current_stats.is_some() {
                tx.open_table(TOOL_STATS)?.insert(name, updated_stats)?;
            }
            Ok(())
        })?;
        if let Some(original) = approval
            && let Some(conversation) = conversation
        {
            self.step(|tx| {
                tx.open_table(OPEN_APPROVALS)?
                    .remove(approvals::open_key(conversation, original))?;
                tx.open_table(APPROVAL_CALLS)?
                    .insert(tool_call_key(original), tool_call_key(call))?;
                Ok(())
            })?;
        }
        if matches!(
            record.decision.fact,
            ToolDecision::AwaitingApproval(_) | ToolDecision::AwaitingAnswer(_)
        ) && let Some(conversation) = conversation
        {
            if let Some(closing) = self.job_control(conversation)?.closing {
                self.put_approval(call, ApprovalDecision::Voided, closing.by)?;
            } else {
                self.insert(OPEN_APPROVALS, approvals::open_key(conversation, call), 0)?;
                self.notify(conversation, NoticeEvent::Requested, call.job)?;
            }
        }
        self.probe = Some(Probe::ToolCall(call));
        Ok(())
    }

    /// TOOL_OUTCOME: records what is known of an admitted call's execution,
    /// once.
    pub(crate) fn tool_outcome(
        &mut self,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), BatchError> {
        self.require_executing_here(call.job)?;
        let Some(mut record) = self.tool_evidence(call)? else {
            return self.reject(Violation::ToolCallNotFound(call));
        };
        if record.admission().is_none() {
            return self.reject(Violation::ToolCallNotAdmitted(call));
        }
        if record.outcome.is_some() {
            return self.reject(Violation::ToolOutcomeAlreadyRecorded(call));
        }
        let current_stats = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_STATS)?
                .get(record.name.as_str())?
                .map(|found| found.value())
                .unwrap_or((0, 0, 0, 0, 0, 0)))
        })?;
        let mut updated_stats = current_stats;
        // Duration semantics: admission commit -> outcome commit (includes the
        // admission's durable commit), never spanning a restart.
        let duration_us = self
            .now
            .as_unix_micros()
            .saturating_sub(record.decision.at.as_unix_micros())
            .max(0) as u64;
        updated_stats.4 = updated_stats.4.saturating_add(duration_us);
        updated_stats.5 = updated_stats.5.max(duration_us);
        match &outcome {
            ToolOutcome::Completed { .. } | ToolOutcome::CompletedWithheld { .. } => {
                updated_stats.2 = updated_stats.2.saturating_add(1);
            }
            ToolOutcome::NotApplied { .. }
            | ToolOutcome::NotInvoked { .. }
            | ToolOutcome::Uncertain { .. } => {
                updated_stats.3 = updated_stats.3.saturating_add(1);
            }
        }
        record.outcome = Some(Recorded {
            fact: outcome,
            at: self.now,
        });
        self.put_tool_evidence(&record)?;
        self.insert(TOOL_STATS, record.name.as_str(), updated_stats)?;
        self.probe = Some(Probe::ToolOutcome(call));
        Ok(())
    }

    /// Refuses unless the Job is claimed by this opening of the database and
    /// has no outcome (I13).
    pub(crate) fn require_executing_here(&mut self, job: JobId) -> Result<(), BatchError> {
        self.require_legacy_evidence_job(job)?;
        let state = self.require_job_state(job)?;
        match &state.claim {
            Some(claim) if claim.epoch == self.epoch && state.outcome.is_none() => Ok(()),
            _ => self.reject(Violation::NotExecutingLocally(job)),
        }
    }

    // ---- finalize (8.5), confirm stop (8.6) ------------------------------------------

    /// FINALIZE. A confirmed outcome of a claimed Job whose direct children
    /// have not all settled, or whose admitted tool calls are not all
    /// resolved, is recorded as Unknown (I6). Returns the outcome kind that
    /// was recorded and the sequence number of the Result output, if any.
    pub(crate) fn finalize(
        &mut self,
        job: JobId,
        mut kind: OutcomeKind,
        mut cause: Option<OutcomeCause>,
        result: Option<FinalResult>,
        ignore_stop_cause: bool,
    ) -> Result<(OutcomeKind, Option<(u32, JobOutputId)>), BatchError> {
        let mut state = self.require_job_state(job)?;
        if state.outcome.is_some() {
            return self.reject(Violation::JobAlreadyFinalized(job));
        }
        let row = self.require_job(job)?;
        if kind == OutcomeKind::Completed || ignore_stop_cause {
            cause = None;
        } else if row.kind.is_persistent()
            && let Some(conversation) = row.parent_id
        {
            if cause.is_none()
                && self
                    .job_control(conversation)?
                    .closing
                    .is_some_and(|closing| closing.running)
            {
                cause = Some(OutcomeCause::Closed);
            }
            if matches!(
                cause,
                Some(OutcomeCause::Stopped { .. } | OutcomeCause::Closed)
            ) {
                let sent = self.step(|tx| {
                    for entry in tx
                        .open_table(REQUEST_EVENTS)?
                        .range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))?
                    {
                        let (key, bytes) = entry?;
                        let (_, request, seq) = key.value();
                        let Some(request) = RequestNumber::new(request) else {
                            continue;
                        };
                        if matches!(
                            rows::decode_request_event(job, request, seq, bytes.value())?.payload,
                            crate::db::entities::RequestPayload::Sent { .. }
                                | crate::db::entities::RequestPayload::ApprovedCalls
                        ) {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })?;
                kind = if sent || !self.no_live_process(job)? {
                    OutcomeKind::Unknown
                } else {
                    OutcomeKind::Cancelled
                };
            }
        }
        if state.claim.is_none() && kind != OutcomeKind::Cancelled {
            return self.reject(Violation::InvalidValue(
                "outcome",
                "a Job without a claim can only be Cancelled",
            ));
        }
        if state.claim.is_some() && kind != OutcomeKind::Unknown {
            let open_children = self.step(|tx| {
                Ok(tx
                    .open_table(UNSETTLED)?
                    .get(job.get())?
                    .map_or(0, |counts| counts.value().1))
            })?;
            if open_children > 0 || !self.tool_effects_resolved(job)? {
                kind = OutcomeKind::Unknown;
            }
        }
        state.outcome = Some(OutcomeRecord {
            kind,
            at: self.now,
            cause,
        });
        if kind == OutcomeKind::Completed
            && let Some(target) = self.title_target(&row)?
            && let Some(title) = result
                .as_ref()
                .and_then(|result| result.value.as_ref())
                .and_then(|value| value.get("text"))
                .and_then(serde_json::Value::as_str)
                .and_then(crate::db::entities::parse_suggested_title)
        {
            crate::db::mutations::set_conversation(
                self,
                target,
                crate::db::entities::JobEdit::SuggestedTitle(title),
            )?;
        }
        if kind == OutcomeKind::Unknown && self.no_live_process(job)? {
            state.stop_confirmed_at = Some(self.now);
            state.stop_confirmed_by = Some(StopProof::Proof);
        }
        if let Some(claim) = &state.claim {
            let pool = claim.pool_id.get();
            let epoch = claim.epoch;
            if kind != OutcomeKind::Unknown || state.stop_confirmed_at.is_some() {
                self.freed_capacity = true;
                self.remove(HELD_SLOTS, (pool, job.get()))?;
            } else {
                self.insert(HELD_SLOTS, (pool, job.get()), (epoch, true))?;
            }
        } else {
            self.cancel_pending_writes(job)?;
        }
        if row.kind.is_persistent()
            && let Some(conversation) = row.parent_id
            && (kind != OutcomeKind::Unknown || state.stop_confirmed_at.is_some())
        {
            self.step(|tx| {
                let mut active = tx.open_table(ACTIVE_JOBS)?;
                if active
                    .get(conversation.get())?
                    .is_some_and(|active| active.value() == job.get())
                {
                    active.remove(conversation.get())?;
                }
                Ok(())
            })?;
        }
        self.put_job_state(job, &state)?;
        if kind == OutcomeKind::Completed
            && let Some(conversation) = row.parent_id
        {
            self.step(|tx| indexes::complete_turn_search_index(tx, conversation, job))?;
        }
        if kind == OutcomeKind::Completed
            && let JobKind::Execution {
                execution: JobExecution::Decision(work),
                ..
            } = &row.kind
        {
            self.apply_command_assessment(job, work, result.as_ref())?;
        }
        let mut appended = None;
        if let Some(result) = result
            && kind != OutcomeKind::Unknown
        {
            appended = Some(self.append_output(
                job,
                OutputKind::Result,
                result.summary,
                result.code,
                result.goal_reached,
                result.value,
            )?);
        }
        self.index_answer(&row, kind, appended.is_some())?;
        if kind == OutcomeKind::Completed {
            self.schedule_title_job(&row)?;
        }
        self.settle(job)?;
        self.turn_ended(job)?;
        self.probe = Some(Probe::Outcome(job));
        Ok((kind, appended))
    }

    /// CONFIRM_STOP: the stop of an Unknown Job is proven, so its slot is
    /// released.
    pub(crate) fn confirm_stop(&mut self, job: JobId, proof: StopProof) -> Result<(), BatchError> {
        let mut state = self.require_job_state(job)?;
        match &state.outcome {
            None => return self.reject(Violation::StopNeedsOutcome(job)),
            Some(outcome) if outcome.kind != OutcomeKind::Unknown => {
                return self.reject(Violation::StopNeedsUnknownOutcome(job));
            }
            Some(_) => {}
        }
        if state.stop_confirmed_at.is_some() {
            return self.reject(Violation::StopAlreadyConfirmed(job));
        }
        state.stop_confirmed_at = Some(self.now);
        state.stop_confirmed_by = Some(proof);
        if let Some(claim) = &state.claim {
            let pool = claim.pool_id.get();
            self.freed_capacity = true;
            self.remove(HELD_SLOTS, (pool, job.get()))?;
        }
        let row = self.require_job(job)?;
        if row.kind.is_persistent()
            && let Some(conversation) = row.parent_id
        {
            self.remove(ACTIVE_JOBS, conversation.get())?;
        }
        self.put_job_state(job, &state)?;
        self.settle(job)?;
        if let Some(conversation) = self.require_job(job)?.parent_id {
            self.auto_close_check(conversation)?;
        }
        Ok(())
    }

    /// CANCEL_PENDING_WRITES: a Job cancelled before its claim leaves every
    /// readiness table.
    pub(crate) fn cancel_pending_writes(&mut self, job: JobId) -> Result<(), BatchError> {
        let row = self.require_job(job)?;
        let requirement_key = self.execution_key(&row)?;
        self.step(|tx| {
            tx.open_table(PENDING)?.remove(job.get())?;
            tx.open_table(READY)?.remove((requirement_key, job.get()))?;
            if let Some(eligible_at) = row.kind.eligible_at() {
                tx.open_table(DELAYED)?
                    .remove((eligible_at.as_unix_micros(), job.get()))?;
            }
            let mut waiters = tx.open_table(DEPENDENCY_WAITERS)?;
            for (dependency, _) in row.kind.waits_on() {
                waiters.remove((dependency.get(), job.get()))?;
            }
            let mut unsettled = tx.open_table(UNSETTLED)?;
            let open_children = unsettled.get(job.get())?.map(|counts| counts.value().1);
            if let Some(open_children) = open_children {
                unsettled.insert(job.get(), (0, open_children))?;
            }
            Ok(())
        })
    }

    /// SETTLE: from `start`, deletes the `unsettled` row of every Job that has
    /// become SubtreeSettled, releases or cancels its waiters and moves up to
    /// its parent.
    pub(crate) fn settle(&mut self, start: JobId) -> Result<(), BatchError> {
        let mut work = vec![start];
        while let Some(job) = work.pop() {
            let counts = self.step(|tx| {
                Ok(tx
                    .open_table(UNSETTLED)?
                    .get(job.get())?
                    .map(|counts| counts.value()))
            })?;
            let Some((_, open_children)) = counts else {
                continue;
            };
            let row = self.require_job(job)?;
            if row.kind.is_group() && open_children == 0 {
                self.settle_group(job)?;
            }
            let state = self.require_job_state(job)?;
            if !state.locally_stopped() || open_children > 0 {
                if row.kind.is_group() {
                    self.place_waiting_job(job)?;
                }
                continue;
            }
            self.remove(UNSETTLED, job.get())?;
            let completed = state
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
            let waiters = self.step(|tx| {
                let table = tx.open_table(DEPENDENCY_WAITERS)?;
                let mut waiters = Vec::new();
                for entry in table.range((job.get(), 0)..=(job.get(), u128::MAX))? {
                    let (key, condition) = entry?;
                    waiters.push((key.value().1, condition.value()));
                }
                Ok(waiters)
            })?;
            for (waiter, condition) in waiters {
                self.remove(DEPENDENCY_WAITERS, (job.get(), waiter))?;
                let waiter = JobId::new(waiter);
                let condition = DependencyCondition::from_code(condition)
                    .ok_or_else(|| StorageError::corrupted("invalid dependency condition"));
                let condition = self.decoded(condition)?;
                if condition == DependencyCondition::Settled || completed {
                    let remaining = self.step(|tx| {
                        let mut unsettled = tx.open_table(UNSETTLED)?;
                        let counts = unsettled.get(waiter.get())?.map(|counts| counts.value());
                        let Some((dependencies, children)) = counts else {
                            return Ok(None);
                        };
                        let dependencies = dependencies.saturating_sub(1);
                        unsettled.insert(waiter.get(), (dependencies, children))?;
                        Ok(Some(dependencies))
                    })?;
                    if remaining == Some(0) {
                        self.place(waiter)?;
                    }
                } else {
                    let mut cancelled = self.require_job_state(waiter)?;
                    cancelled.outcome = Some(OutcomeRecord {
                        kind: OutcomeKind::Cancelled,
                        at: self.now,
                        cause: Some(OutcomeCause::DependencyImpossible),
                    });
                    self.put_job_state(waiter, &cancelled)?;
                    self.cancel_pending_writes(waiter)?;
                    work.push(waiter);
                    self.turn_ended(waiter)?;
                }
            }
            if let Some(parent) = row.parent_id {
                if row.kind.is_group() {
                    self.remove(LIVE_CHILDREN, (parent.get(), job.get()))?;
                    self.auto_close_check(parent)?;
                }
                self.adjust_open_children(parent, -1)?;
                work.push(parent);
            }
        }
        Ok(())
    }

    // ---- predicates over a Job's tool calls ----------------------------------------------

    /// NO_LIVE_PROCESS: every admitted call that can spawn has an
    /// outcome, so no OS process of the Job can remain (section 11.4).
    pub(crate) fn no_live_process(&mut self, job: JobId) -> Result<bool, BatchError> {
        let calls = self.job_tool_evidence(job)?;
        Ok(calls
            .iter()
            .all(|record| !record.spawns || record.outcome.is_some()))
    }

    /// Whether every admitted call has a definite outcome or a reconciliation.
    fn tool_effects_resolved(&mut self, job: JobId) -> Result<bool, BatchError> {
        let calls = self.job_tool_evidence(job)?;
        Ok(calls.iter().all(|record| !record.is_unresolved()))
    }
}

impl Batch {
    pub(crate) fn decision_executor_available(
        &mut self,
        frozen: &FrozenDecisionConfig,
    ) -> Result<bool, BatchError> {
        self.step(|tx| reader::decision_executor_available(&tx.open_table(CONFIGS)?, frozen))
    }
}

/// The durable scheduled external policy and its binding to one Allocation.
/// Creation takes an already validated draft; the store never infers authorization
/// from a Job's frozen configuration or from another authority. Every mutation
/// bumps the policy revision (except a no-op enable/disable), so a binding made
/// under an older revision can never authorize a start or a send.
impl Batch {
    fn scheduled_external_policy_by_id(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
    ) -> Result<Option<crate::db::entities::ScheduledExternalPolicyRecord>, BatchError> {
        self.step(|tx| {
            let table = tx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
            table
                .get(id.get())?
                .map(|row| decode_scheduled_external_policy(id.get(), row.value()))
                .transpose()
        })
    }

    fn scheduled_external_binding_by_job(
        &mut self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::ScheduledExternalBinding>, BatchError> {
        self.step(|tx| {
            let table = tx.open_table(SCHEDULED_EXTERNAL_BINDING)?;
            table
                .get(job.get())?
                .map(|row| decode_scheduled_external_binding(job.get(), row.value()))
                .transpose()
        })
    }

    pub(crate) fn create_scheduled_external_policy(
        &mut self,
        draft: &crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        use crate::db::entities::{ScheduledExternalPolicyId, ScheduledExternalPolicyRecord};
        let id = ScheduledExternalPolicyId::new(self.next_id()?);
        let now = self.now;
        let record = ScheduledExternalPolicyRecord {
            id,
            endpoint: draft.endpoint().to_owned(),
            credential_ref: draft.credential_ref().to_owned(),
            model: draft.model().to_owned(),
            proxy: draft.proxy().to_owned(),
            max_request_bytes: u32::try_from(draft.limits().max_request_bytes)
                .map_err(|_| self.invalid("scheduled_external_policy"))?,
            max_output_tokens: draft.limits().max_output_tokens,
            timeout_secs: u32::try_from(draft.limits().timeout.as_secs())
                .map_err(|_| self.invalid("scheduled_external_policy"))?,
            enabled: false,
            revoked: false,
            revision: 1,
            created_at: now,
            updated_at: now,
        };
        let bytes = encode_scheduled_external_policy(&record);
        self.step(|tx| {
            tx.open_table(SCHEDULED_EXTERNAL_POLICY)?
                .insert(id.get(), bytes.as_slice())?;
            Ok(())
        })?;
        Ok(record)
    }

    pub(crate) fn update_scheduled_external_policy(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        draft: &crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        use crate::db::entities::ScheduledExternalPolicyRecord;
        let Some(mut record) = self.scheduled_external_policy_by_id(id)? else {
            return self.reject(Violation::NotFound(
                crate::db::entities::RecordRef::ScheduledExternalPolicy(id),
            ));
        };
        if record.revoked {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "a revoked policy is immutable; create a new one",
            ));
        }
        if record.revision != expected_revision {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the policy revision changed; reload before saving",
            ));
        }
        let revision = record.revision.checked_add(1).ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "scheduled external policy revision exhausted",
            )))
        })?;
        record = ScheduledExternalPolicyRecord {
            endpoint: draft.endpoint().to_owned(),
            credential_ref: draft.credential_ref().to_owned(),
            model: draft.model().to_owned(),
            proxy: draft.proxy().to_owned(),
            max_request_bytes: u32::try_from(draft.limits().max_request_bytes)
                .map_err(|_| self.invalid("scheduled_external_policy"))?,
            max_output_tokens: draft.limits().max_output_tokens,
            timeout_secs: u32::try_from(draft.limits().timeout.as_secs())
                .map_err(|_| self.invalid("scheduled_external_policy"))?,
            revision,
            updated_at: self.now,
            ..record
        };
        let bytes = encode_scheduled_external_policy(&record);
        self.step(|tx| {
            tx.open_table(SCHEDULED_EXTERNAL_POLICY)?
                .insert(id.get(), bytes.as_slice())?;
            Ok(())
        })?;
        Ok(record)
    }

    pub(crate) fn set_scheduled_external_policy_enabled(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        enabled: bool,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        let Some(record) = self.scheduled_external_policy_by_id(id)? else {
            return self.reject(Violation::NotFound(
                crate::db::entities::RecordRef::ScheduledExternalPolicy(id),
            ));
        };
        if record.revoked {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "a revoked policy cannot be enabled",
            ));
        }
        if record.revision != expected_revision {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the policy revision changed; reload before saving",
            ));
        }
        if record.enabled == enabled {
            return Ok(record);
        }
        let revision = record.revision.checked_add(1).ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "scheduled external policy revision exhausted",
            )))
        })?;
        let updated = crate::db::entities::ScheduledExternalPolicyRecord {
            enabled,
            revision,
            updated_at: self.now,
            ..record
        };
        let bytes = encode_scheduled_external_policy(&updated);
        self.step(|tx| {
            tx.open_table(SCHEDULED_EXTERNAL_POLICY)?
                .insert(id.get(), bytes.as_slice())?;
            Ok(())
        })?;
        Ok(updated)
    }

    pub(crate) fn revoke_scheduled_external_policy(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        let Some(record) = self.scheduled_external_policy_by_id(id)? else {
            return self.reject(Violation::NotFound(
                crate::db::entities::RecordRef::ScheduledExternalPolicy(id),
            ));
        };
        if record.revision != expected_revision {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the policy revision changed; reload before revoking",
            ));
        }
        if record.revoked {
            return Ok(record);
        }
        let revision = record.revision.checked_add(1).ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "scheduled external policy revision exhausted",
            )))
        })?;
        let updated = crate::db::entities::ScheduledExternalPolicyRecord {
            enabled: false,
            revoked: true,
            revision,
            updated_at: self.now,
            ..record
        };
        let bytes = encode_scheduled_external_policy(&updated);
        self.step(|tx| {
            tx.open_table(SCHEDULED_EXTERNAL_POLICY)?
                .insert(id.get(), bytes.as_slice())?;
            Ok(())
        })?;
        Ok(updated)
    }

    /// The one live policy that binds this frozen configuration, if any. It
    /// refuses when the build's control is disabled, and when more than one
    /// live policy would bind the same Job: choosing between them is not the
    /// transaction's to make.
    fn scheduled_external_policy_for(
        &mut self,
        frozen: &FrozenExecutionConfig,
    ) -> Result<Option<crate::db::entities::ScheduledExternalPolicyRecord>, BatchError> {
        if !ExecutionControl::ScheduledExternalTransport.is_enabled() {
            return Ok(None);
        }
        let candidates = self.step(|tx| {
            let table = tx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
            table
                .iter()?
                .map(|entry| {
                    let (key, row) = entry?;
                    decode_scheduled_external_policy(key.value(), row.value())
                })
                .collect::<Result<Vec<_>, StorageError>>()
        })?;
        let matching: Vec<_> = candidates
            .into_iter()
            .filter(|record| {
                record.is_live() && crate::system::scheduled_external::record_admits(record, frozen)
            })
            .collect();
        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.into_iter().next()),
            _ => self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "more than one live policy binds this Job; refuse rather than choose",
            )),
        }
    }

    /// The policy authorizing this Job's current Allocation, revalidated in
    /// this write transaction. `Ok(None)` means no scheduled binding exists,
    /// which is every loopback and manual execution. Any binding that no longer
    /// matches its live policy, the Allocation, or the Job's frozen
    /// configuration is a refusal, never a warning.
    pub(crate) fn scheduled_external_authorization(
        &mut self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::ScheduledExternalPolicyRecord>, BatchError> {
        let Some(binding) = self.scheduled_external_binding_by_job(job)? else {
            return Ok(None);
        };
        if !ExecutionControl::ScheduledExternalTransport.is_enabled() {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the scheduled external transport control is disabled",
            ));
        }
        let Some(record) = self.scheduled_external_policy_by_id(binding.policy)? else {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the bound policy no longer exists",
            ));
        };
        if !record.is_live() || record.revision != binding.policy_revision {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the bound policy is revoked, disabled or changed since reservation",
            ));
        }
        let allocation = self.pool_allocation(job)?;
        if allocation.is_none_or(|allocation| {
            allocation.token.epoch != binding.allocation_epoch
                || allocation.token.generation != binding.allocation_generation
        }) {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the binding does not name this Allocation",
            ));
        }
        let Some(row) = self.mutable_job(job)? else {
            return self.reject(Violation::NotFound(crate::db::entities::RecordRef::Job(
                job,
            )));
        };
        let frozen = self.frozen_configuration(&row)?;
        let FrozenConfiguration::Generative(frozen) = &frozen else {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "a scheduled external Job must be Generative",
            ));
        };
        if !crate::system::scheduled_external::record_admits(&record, frozen) {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the Job's frozen configuration no longer matches the bound policy",
            ));
        }
        Ok(Some(record))
    }

    fn invalid(&self, field: &'static str) -> BatchError {
        BatchError(Failure::Rejected(Violation::InvalidValue(
            field,
            "value out of range",
        )))
    }
}
