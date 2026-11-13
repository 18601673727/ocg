use super::*;
use crate::db::entities::{
    ConfigBody, ConfigId, DependencyCondition, ExecutionConfiguration, FrozenConfiguration,
    FrozenDecisionConfig, OutcomeCause, OutcomeKind, OutcomeRecord, Recorded, SubmissionRecord,
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

pub(super) const POOL_ARCHIVE_SCAN_LIMIT: usize = 4096;

enum NativeConfigRow {
    Pool(crate::db::entities::AllocationPool),
    Executor(crate::db::entities::AllocationExecutor),
    Other,
}

fn decode_native_config_row(key: u128, bytes: &[u8]) -> DecodeResult<NativeConfigRow> {
    match bytes.first() {
        Some(&TAG_ALLOCATION_POOL) => decode_allocation_pool(key, bytes).map(NativeConfigRow::Pool),
        Some(&TAG_ALLOCATION_EXECUTOR) => {
            decode_allocation_executor(key, bytes).map(NativeConfigRow::Executor)
        }
        _ => {
            ConfigRow::decode_at(key, bytes)?;
            Ok(NativeConfigRow::Other)
        }
    }
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

enum ScheduledPolicyMatch {
    None,
    One(crate::db::entities::ScheduledExternalPolicyRecord),
    Ambiguous,
}

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
pub(super) const STOPPED_BEFORE_START_CODE: &str = "stopped_before_start";
pub(super) const STOPPED_BEFORE_START_SUMMARY: &str =
    "Stopped before it started. Nothing was sent to the Provider.";

pub(super) fn mutable_priority(priority: crate::db::entities::JobQueuePriority) -> u8 {
    use crate::db::entities::JobQueuePriority;
    match priority {
        JobQueuePriority::Foreground => 0,
        JobQueuePriority::Automatic => 1,
        JobQueuePriority::Speculative => 2,
    }
}

#[allow(dead_code)]
pub(super) fn encode_mutable_job(job: &crate::db::entities::MutableJob) -> Vec<u8> {
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
        crate::db::entities::MutableJobContent::Decision {
            request,
            references,
            ..
        },
        Some(crate::db::entities::MutableFrozenConfiguration::Generative(configuration)),
    ) = (&input.content, &frozen)
    {
        let decision = FrozenDecisionConfig::Generative(configuration.clone(), None);
        decision.validate().map_err(StorageError::corrupted)?;
        request
            .validate(&decision.capabilities())
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

    pub(crate) fn unsupported_start_context(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<Option<&'static str>, BatchError> {
        use crate::db::entities::MutableContextSelection;
        const CHAIN_LIMIT: usize = 4096;
        const UNSUPPORTED: &str = "child work or tool evidence require unsupported preparation";
        let mut context = vec![job.id];
        if let MutableContextSelection::AdoptedSummary { producer, .. } = job.context_selection {
            context.push(producer);
        }
        let mut previous = job.previous;
        let mut predecessors = 0usize;
        while let Some(id) = previous {
            if predecessors == CHAIN_LIMIT {
                return Ok(Some(UNSUPPORTED));
            }
            let Some(prior) = self.mutable_job(id)? else {
                return self.corrupted("conversation predecessor missing".to_owned());
            };
            predecessors += 1;
            context.push(id);
            previous = prior.previous;
        }
        self.step(|tx| {
            let tools = tx.open_table(TOOL_CALLS)?;
            for id in &context {
                let range = (id.get(), 0, 0)..=(id.get(), u32::MAX, u32::MAX);
                if *id == job.id && tools.range(range)?.next().transpose()?.is_some() {
                    return Ok(Some(UNSUPPORTED));
                }
            }
            Ok(None)
        })
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
            match reader::verify_native_summary_tables(
                &tx.open_table(JOBS)?,
                &tx.open_table(JOB_STATES)?,
                &tx.open_table(JOB_OUTPUTS)?,
                &tx.open_table(MESSAGES)?,
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
        let encoded = state.encode();
        self.insert(JOB_STATES, id.get(), encoded.as_slice())
    }

    pub(super) fn record_mutable_submission(
        &mut self,
        record: &SubmissionRecord,
    ) -> Result<(), BatchError> {
        let encoded = encode_submission(record);
        self.insert(
            SUBMISSIONS,
            record.key.as_bytes().as_slice(),
            encoded.as_slice(),
        )?;
        self.probe = Some(Probe::Submission(record.key));
        Ok(())
    }

    pub(super) fn require_allocation_durability(&mut self) -> Result<(), BatchError> {
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

    fn frozen_configuration(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<FrozenConfiguration, BatchError> {
        use crate::db::entities::{MutableFrozenConfiguration, MutableJobContent};
        match (&job.input.content, &job.frozen) {
            (
                MutableJobContent::Decision { .. },
                Some(MutableFrozenConfiguration::Generative(configuration)),
            ) => Ok(FrozenConfiguration::Decision(
                FrozenDecisionConfig::Generative(configuration.clone(), None),
            )),
            (_, Some(MutableFrozenConfiguration::Generative(configuration))) => {
                Ok(FrozenConfiguration::Generative(configuration.clone()))
            }
            (_, Some(MutableFrozenConfiguration::Decision(configuration))) => Ok(
                FrozenConfiguration::Decision(FrozenDecisionConfig::Native(configuration.clone())),
            ),
            (_, None) => self.corrupted("submitted Job has no frozen configuration".to_owned()),
        }
    }

    fn storable_frozen(
        &mut self,
        frozen: &FrozenConfiguration,
    ) -> Result<crate::db::entities::MutableFrozenConfiguration, BatchError> {
        use crate::db::entities::{AgentCapability, AgentConfig, MutableFrozenConfiguration};
        match frozen {
            FrozenConfiguration::Generative(configuration) => Ok(
                MutableFrozenConfiguration::Generative(configuration.clone()),
            ),
            FrozenConfiguration::Decision(FrozenDecisionConfig::Native(configuration)) => {
                Ok(MutableFrozenConfiguration::Decision(configuration.clone()))
            }
            FrozenConfiguration::Decision(FrozenDecisionConfig::Generative(
                configuration,
                decision_profile,
            )) => {
                let mut stored = configuration.clone();
                if let Some((agent, reference)) = decision_profile {
                    stored.agent = Some(AgentConfig {
                        instructions: agent.instructions.clone(),
                        capabilities: BTreeSet::from([AgentCapability::Decision]),
                        models: agent.models.clone(),
                        compaction_policy: None,
                    });
                    stored.agent_ref = Some(*reference);
                }
                Ok(MutableFrozenConfiguration::Generative(stored))
            }
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
            if let Err(reason) = request.validate(&frozen.capabilities()) {
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
                if let Some(reason) = frozen.native_execution_refusal() {
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
            if let Err(reason) = request.validate(&frozen.capabilities()) {
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

    pub(crate) fn mutable_job_for_context(
        &mut self,
        context: crate::db::entities::ComposerContextId,
    ) -> Result<Option<crate::db::entities::MutableJobRecord>, BatchError> {
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

    pub(crate) fn create_unsubmitted_job(
        &mut self,
        context: crate::db::entities::ComposerContextId,
        input: crate::db::entities::MutableJobInput,
        submission: crate::db::entities::Submission,
        conversation: Option<JobId>,
        project: Option<ProjectId>,
    ) -> Result<JobId, BatchError> {
        self.create_job_as(context, input, submission, conversation, project, true)
    }

    pub(crate) fn create_job_as(
        &mut self,
        context: crate::db::entities::ComposerContextId,
        mut input: crate::db::entities::MutableJobInput,
        submission: crate::db::entities::Submission,
        conversation: Option<JobId>,
        project: Option<ProjectId>,
        by_user: bool,
    ) -> Result<JobId, BatchError> {
        use crate::db::entities::{MutableJob, MutableJobPhase};
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
        if by_user {
            let mut control = self.job_control(parent_id)?;
            if !control.user_input || control.loop_count != 0 {
                control.user_input = true;
                control.loop_count = 0;
                self.put_job_control(parent_id, &control)?;
            }
        }
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

    pub(crate) fn discard_unsubmitted_job(
        &mut self,
        id: JobId,
        expected_revision: u64,
    ) -> Result<(), BatchError> {
        use crate::db::entities::MutableJobPhase;
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

    pub(crate) fn cancel_unsupported_queued_job(
        &mut self,
        id: JobId,
        expected_revision: u64,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{MutableJobContent, MutableJobPhase};
        self.require_allocation_durability()?;
        let Some(job) = self.mutable_job(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(id)));
        };
        if job.revision != expected_revision {
            return self.reject(Violation::InvalidValue("revision", "stale Job revision"));
        }
        let state = self.require_job_state(id)?;
        if job.phase != MutableJobPhase::Queued
            || state.outcome.is_some()
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
        self.end_queued_native_job(
            &job,
            OutcomeCause::Text(
                "cancelled before execution: unsupported Decision history".to_owned(),
            ),
        )
    }

    pub(crate) fn end_queued_native_job(
        &mut self,
        job: &crate::db::entities::MutableJob,
        cause: OutcomeCause,
    ) -> Result<(), BatchError> {
        let id = job.id;
        let mut state = self.require_job_state(id)?;
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
            cause: Some(cause),
        });
        self.put_mutable_job_state(id, &state)?;
        self.probe = Some(Probe::Outcome(id));
        Ok(())
    }

    fn enqueue_mutable_job(
        &mut self,
        job: &crate::db::entities::MutableJob,
        req_tag: u16,
        planning_wait: bool,
        create_state: bool,
    ) -> Result<(), BatchError> {
        let submitted_micros = job.submitted_at.unwrap_or(self.now).as_unix_micros();
        self.step(|tx| {
            if !planning_wait {
                tx.open_table(MUTABLE_READY)?.insert(
                    (
                        mutable_priority(job.input.priority),
                        submitted_micros,
                        job.id.get(),
                        req_tag,
                    ),
                    (),
                )?;
            }
            tx.open_table(PENDING)?.insert(job.id.get(), ())?;
            tx.open_table(TURNS)?
                .insert((job.parent_id.get(), job.id.get()), ())?;
            tx.open_table(CHILDREN)?
                .insert((job.parent_id.get(), job.id.get()), ())?;
            tx.open_table(UNSETTLED)?
                .insert(job.id.get(), (u32::from(planning_wait), 0))?;
            if let Some(previous) = job.previous {
                tx.open_table(SUCCESSORS)?.insert(previous.get(), ())?;
            }
            Ok(())
        })?;
        self.adjust_open_children(job.parent_id, 1)?;
        self.insert(JOBS, job.id.get(), encode_mutable_job(job).as_slice())?;
        if create_state {
            self.put_job_state(job.id, &JobState::new(job.id))?;
        }
        Ok(())
    }

    pub(crate) fn submit_mutable_job(
        &mut self,
        id: JobId,
        expected_revision: u64,
        submission: crate::db::entities::Submission,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        use crate::db::entities::MutableJobPhase;
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
        if job.context_selection == crate::db::entities::MutableContextSelection::FullHistory
            && job.previous.is_some()
            && let Some((producer, source)) = self.latest_verified_summary(job.parent_id, id)?
        {
            job.context_selection =
                crate::db::entities::MutableContextSelection::AdoptedSummary { producer, source };
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
        let planning_wait = self.maybe_attach_planning_decision(&parent, &job, &frozen)?;
        let req_tag = mutable_configuration_requirement(&frozen);
        self.enqueue_mutable_job(&job, req_tag, planning_wait, false)?;
        self.step(|tx| {
            indexes::update_mutable_turn_search(tx, job.parent_id, id, None, &job.input)
        })?;
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

    pub(crate) fn submit_native_decision(
        &mut self,
        parent: Option<JobId>,
        spec: JobGroup,
        configuration: ExecutionConfiguration,
        request: crate::db::entities::DecisionRequest,
        submission: Option<crate::db::entities::Submission>,
    ) -> Result<(JobId, Appended), BatchError> {
        use crate::db::entities::{
            ComposerContextId, JobQueuePriority, MutableContextSelection, MutableJob,
            MutableJobContent, MutableJobInput, MutableJobPhase,
        };
        self.require_allocation_durability()?;
        if let Some(sub) = submission
            && let Some(record) = self.submission(sub.key)?
        {
            return self.reject(Violation::SubmissionKeyUsed {
                job: record.job,
                same_request: record.fingerprint == sub.fingerprint,
            });
        }
        let frozen = match self.freeze_configuration(configuration)? {
            FrozenConfiguration::Decision(frozen) => frozen,
            FrozenConfiguration::Generative(frozen) => {
                FrozenDecisionConfig::Generative(frozen, None)
            }
        };
        if let Err(reason) = frozen.validate() {
            return self.reject(Violation::InvalidValue("decision", reason));
        }
        if let Err(reason) = request.validate(&frozen.capabilities()) {
            return self.reject(Violation::InvalidValue("decision", reason));
        }
        let storable = self.storable_frozen(&FrozenConfiguration::Decision(frozen.clone()))?;
        let conversation_id = JobId::new(self.next_id()?);
        let conversation_row = JobSpec {
            id: conversation_id,
            created_at: self.now,
            parent_id: parent,
            allocation: None,
            kind: JobKind::Group {
                agent: spec.agent,
                project: spec.project,
                origin: spec.origin,
                keep_open: spec.keep_open,
            },
        };
        self.insert(
            JOBS,
            conversation_id.get(),
            conversation_row.encode_mutable_conversation().as_slice(),
        )?;
        self.put_job_state(conversation_id, &JobState::new(conversation_id))?;
        self.put_job_control(
            conversation_id,
            &JobControl {
                title: spec.name,
                ..JobControl::default()
            },
        )?;
        self.insert(CONVERSATIONS, conversation_id.get(), ())?;
        self.insert(UNSETTLED, conversation_id.get(), (0, 0))?;
        self.insert(
            CHILDREN,
            (parent.map_or(0, JobId::get), conversation_id.get()),
            (),
        )?;

        let job_id = JobId::new(self.next_id()?);
        let profile = frozen.profile_ref().id;
        let input = MutableJobInput {
            priority: JobQueuePriority::Foreground,
            content: MutableJobContent::Decision {
                request,
                profile,
                references: frozen.references().collect(),
            },
        };
        let submitted = self.now;
        let job = MutableJob {
            id: job_id,
            parent_id: conversation_id,
            previous: None,
            composer_context: ComposerContextId::new(self.next_id()?),
            created_at: submitted,
            submitted_at: Some(submitted),
            revision: 1,
            phase: MutableJobPhase::Queued,
            input: input.clone(),
            context_selection: MutableContextSelection::FullHistory,
            summary_source: None,
            frozen: Some(storable),
        };
        let req_tag = mutable_configuration_requirement(&FrozenConfiguration::Decision(frozen));
        self.enqueue_mutable_job(&job, req_tag, false, true)?;
        self.step(|tx| {
            indexes::update_mutable_turn_search(tx, conversation_id, job_id, None, &input)
        })?;
        if let Some(sub) = submission {
            self.record_mutable_submission(&SubmissionRecord {
                key: sub.key,
                fingerprint: sub.fingerprint,
                job: job_id,
                seq: 1,
                recorded_at: self.now,
            })?;
        }
        Ok((
            conversation_id,
            Appended {
                turn: job_id,
                seq: 1,
                joined: false,
            },
        ))
    }

    pub(crate) fn submit_mutable_job_with_input(
        &mut self,
        id: JobId,
        expected_revision: u64,
        input: crate::db::entities::MutableJobInput,
        submission: crate::db::entities::Submission,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        self.require_allocation_durability()?;
        if self.submission(submission.key)?.is_some() {
            return self.submit_mutable_job(id, expected_revision, submission);
        }
        let updated = self.update_mutable_job(id, expected_revision, input)?;
        self.submit_mutable_job(id, updated.revision, submission)
    }

    pub(crate) fn register_allocation_pool(
        &mut self,
        pool: crate::db::entities::AllocationPool,
    ) -> Result<(), BatchError> {
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

    fn mutate_native_pool(
        &mut self,
        id: ExecutionPoolId,
        expected_revision: u32,
        archived_violation: Violation,
        apply: impl FnOnce(
            &mut Self,
            &mut crate::db::entities::AllocationPool,
        ) -> Result<(), BatchError>,
    ) -> Result<crate::db::entities::AllocationPool, BatchError> {
        self.require_allocation_durability()?;
        let Some(mut pool) = self.step(|tx| {
            let configs = tx.open_table(CONFIGS)?;
            let Some(bytes) = configs.get(id.get())? else {
                return Ok(None);
            };
            Ok(match decode_native_config_row(id.get(), bytes.value())? {
                NativeConfigRow::Pool(pool) => Some(pool),
                NativeConfigRow::Executor(_) | NativeConfigRow::Other => None,
            })
        })?
        else {
            return self.reject(Violation::NotFound(RecordRef::ExecutionPool(id)));
        };
        if pool.archived {
            return self.reject(archived_violation);
        }
        if pool.revision != expected_revision {
            return self.reject(Violation::InvalidValue("pool", "revision conflict"));
        }
        apply(self, &mut pool)?;
        pool.revision = pool
            .revision
            .checked_add(1)
            .ok_or(BatchError(Failure::Rejected(Violation::ValueOutOfRange(
                "revision",
            ))))?;
        self.insert(CONFIGS, id.get(), encode_allocation_pool(&pool).as_slice())?;
        Ok(pool)
    }

    pub(crate) fn update_native_pool(
        &mut self,
        id: ExecutionPoolId,
        expected_revision: u32,
        capacity: Option<std::num::NonZeroU32>,
        enabled: Option<bool>,
    ) -> Result<crate::db::entities::AllocationPool, BatchError> {
        self.mutate_native_pool(
            id,
            expected_revision,
            Violation::NotFound(RecordRef::ExecutionPool(id)),
            |_, pool| {
                if let Some(capacity) = capacity {
                    pool.capacity = capacity;
                }
                if let Some(enabled) = enabled {
                    pool.enabled = enabled;
                }
                Ok(())
            },
        )
    }

    fn mutate_native_executor(
        &mut self,
        id: ExecutorId,
        expected_revision: u32,
        archived_violation: Violation,
        apply: impl FnOnce(
            &mut Self,
            &mut crate::db::entities::AllocationExecutor,
        ) -> Result<(), BatchError>,
    ) -> Result<crate::db::entities::AllocationExecutor, BatchError> {
        self.require_allocation_durability()?;
        let Some(mut executor) = self.native_executor_row(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Executor(id)));
        };
        if executor.archived {
            return self.reject(archived_violation);
        }
        if executor.revision != expected_revision {
            return self.reject(Violation::InvalidValue("executor", "revision conflict"));
        }
        apply(self, &mut executor)?;
        executor.revision =
            executor
                .revision
                .checked_add(1)
                .ok_or(BatchError(Failure::Rejected(Violation::ValueOutOfRange(
                    "revision",
                ))))?;
        self.insert(
            CONFIGS,
            id.get(),
            encode_allocation_executor(&executor).as_slice(),
        )?;
        Ok(executor)
    }

    pub(crate) fn update_native_executor(
        &mut self,
        id: ExecutorId,
        expected_revision: u32,
        update: crate::db::entities::ExecutorUpdate,
    ) -> Result<crate::db::entities::AllocationExecutor, BatchError> {
        use crate::db::entities::ExecutorUpdate;
        self.mutate_native_executor(
            id,
            expected_revision,
            Violation::NotFound(RecordRef::Executor(id)),
            |batch, executor| match update {
                ExecutorUpdate::Enabled(enabled) => {
                    executor.enabled = enabled;
                    Ok(())
                }
                ExecutorUpdate::Name(name) => {
                    if name.trim().is_empty()
                        || name.chars().any(char::is_control)
                        || name.len() > 255
                    {
                        return batch.reject(Violation::InvalidValue(
                            "executor",
                            "invalid Executor configuration",
                        ));
                    }
                    executor.name = name;
                    Ok(())
                }
                ExecutorUpdate::Capabilities(capabilities) => {
                    if capabilities.len() > 8
                        || capabilities.iter().any(|(operation, protocol)| {
                            !matches!(
                                (operation, protocol),
                                (ExecutionOperation::LlmRequest, Some(_))
                                    | (ExecutionOperation::Decision, None)
                            )
                        })
                    {
                        return batch.reject(Violation::InvalidValue(
                            "executor",
                            "invalid Executor configuration",
                        ));
                    }
                    executor.capabilities = capabilities;
                    Ok(())
                }
            },
        )
    }

    fn native_executor_row(
        &mut self,
        id: ExecutorId,
    ) -> Result<Option<crate::db::entities::AllocationExecutor>, BatchError> {
        let existing = self.get(CONFIGS, id.get(), |bytes| match bytes.first() {
            Some(&TAG_ALLOCATION_EXECUTOR) => decode_allocation_executor(id.get(), bytes).map(Some),
            Some(&TAG_ALLOCATION_POOL) => {
                decode_allocation_pool(id.get(), bytes)?;
                Ok(None)
            }
            _ => {
                ConfigRow::decode_at(id.get(), bytes)?;
                Ok(None)
            }
        })?;
        Ok(existing.flatten())
    }

    pub(crate) fn archive_native_executor(
        &mut self,
        id: ExecutorId,
        expected_revision: u32,
    ) -> Result<crate::db::entities::AllocationExecutor, BatchError> {
        self.mutate_native_executor(
            id,
            expected_revision,
            Violation::InvalidValue("executor", "already archived"),
            |batch, executor| {
                let pool = executor.pool.get();
                let held = batch.step(|tx| {
                    let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
                    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
                    let entries = active_allocations(&active, pool, Some(id.get()))?;
                    for &(executor, job) in &entries {
                        match super::reader::indexed_allocation(&allocations, pool, executor, job) {
                            Ok(_) => {}
                            Err(error) if error.kind == StorageErrorKind::Corrupted => {
                                return Ok(None);
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Ok(match entries.len() {
                        0 | 1 => Some(!entries.is_empty()),
                        _ => None,
                    })
                })?;
                match held {
                    Some(false) => {}
                    Some(true) => {
                        return batch.reject(Violation::InvalidValue(
                            "executor",
                            "holds an active Allocation",
                        ));
                    }
                    None => {
                        return batch.reject(Violation::InvalidValue(
                            "executor",
                            "active Allocation index is inconsistent",
                        ));
                    }
                }
                executor.archived = true;
                executor.enabled = false;
                Ok(())
            },
        )
    }

    pub(crate) fn archive_native_pool(
        &mut self,
        id: ExecutionPoolId,
        expected_revision: u32,
    ) -> Result<crate::db::entities::AllocationPool, BatchError> {
        self.mutate_native_pool(
            id,
            expected_revision,
            Violation::InvalidValue("pool", "already archived"),
            |batch, pool| {
                let pool_key = id.get();
                let has_allocation = batch.step(|tx| {
                    let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
                    Ok(active
                        .range((pool_key, 0, 0)..=(pool_key, u128::MAX, u128::MAX))?
                        .next()
                        .is_some())
                })?;
                if has_allocation {
                    return batch.reject(Violation::InvalidValue(
                        "pool",
                        "holds an active Allocation",
                    ));
                }
                let has_unarchived_executor = batch.step(|tx| {
                    let configs = tx.open_table(CONFIGS)?;
                    for (examined, entry) in configs.iter()?.enumerate() {
                        if examined == POOL_ARCHIVE_SCAN_LIMIT {
                            return Ok(Some("child Executor verification exceeds the scan bound"));
                        }
                        let (key, value) = entry?;
                        if matches!(
                            decode_native_config_row(key.value(), value.value())?,
                            NativeConfigRow::Executor(e) if e.pool.get() == pool_key && !e.archived
                        ) {
                            return Ok(Some("has an unarchived Executor"));
                        }
                    }
                    Ok(None)
                })?;
                if let Some(reason) = has_unarchived_executor {
                    return batch.reject(Violation::InvalidValue("pool", reason));
                }
                pool.archived = true;
                pool.enabled = false;
                Ok(())
            },
        )
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
        if matches!(selection, ReadySelection::Scheduled { .. }) {
            let room = self.step(|tx| {
                let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
                let held = active_allocations(&active, pool_id.get(), None)?;
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
            if let ReadySelection::Only(only) = selection
                && only != id
            {
                continue;
            }
            let Some(mut job) =
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
            if job.context_selection == crate::db::entities::MutableContextSelection::FullHistory
                && job.previous.is_some()
                && let Some((producer, source)) = self.latest_verified_summary(job.parent_id, id)?
            {
                job.context_selection =
                    crate::db::entities::MutableContextSelection::AdoptedSummary {
                        producer,
                        source,
                    };
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
                        if !matches!(selection, ReadySelection::Scheduled { .. }) {
                            self.remove(MUTABLE_READY, *key)?;
                        }
                        continue;
                    }
                    Err(BatchError(Failure::Rejected(_))) => continue,
                    Err(error) => return Err(error),
                }
            }
            if !self.mutable_predecessor_ready(&job)? || self.mutable_job_blocked(&job)? {
                continue;
            }
            if job.summary_source.is_none()
                && matches!(&job.input.content, crate::db::entities::MutableJobContent::Generative { configuration, .. } if configuration.is_none_or(|config| config.agent_profile.is_none()))
            {
                continue;
            }
            if self.step(|tx| {
                Ok(tx
                    .open_table(UNSETTLED)?
                    .get(id.get())?
                    .is_some_and(|counts| counts.value().0 > 0))
            })? {
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
            if let ReadySelection::Scheduled {
                generative_policy,
                decision_policy,
            } = selection
            {
                if self.unsupported_start_context(&job)?.is_some() {
                    continue;
                }
                let ext_target = match &frozen {
                    FrozenConfiguration::Generative(frozen) => {
                        let refusal = if job.summary_source.is_some() {
                            frozen.provider_refusal()
                        } else {
                            frozen.native_execution_refusal()
                        };
                        if refusal.is_some() || !generative_policy(frozen) {
                            continue;
                        }
                        Some(frozen)
                    }
                    FrozenConfiguration::Decision(FrozenDecisionConfig::Native(native)) => {
                        if !decision_policy(native)
                            || !native
                                .endpoint
                                .as_deref()
                                .is_some_and(crate::system::http::direct_diagnostic_loopback)
                        {
                            continue;
                        }
                        None
                    }
                    FrozenConfiguration::Decision(FrozenDecisionConfig::Generative(
                        generative,
                        _,
                    )) => {
                        if generative.llm.provider.transport
                            != crate::db::entities::LlmTransport::OpenAiChatCompletions
                            || !generative_policy(generative)
                        {
                            continue;
                        }
                        Some(generative)
                    }
                };
                if let Some(target) = ext_target {
                    let loopback = target
                        .llm
                        .provider
                        .endpoint
                        .as_deref()
                        .is_some_and(crate::system::http::direct_diagnostic_loopback);
                    match self.scheduled_external_policy_for(target)? {
                        ScheduledPolicyMatch::One(record) => {
                            scheduled_external = Some((record.id, record.revision));
                        }
                        ScheduledPolicyMatch::Ambiguous => continue,
                        ScheduledPolicyMatch::None if !loopback => continue,
                        ScheduledPolicyMatch::None => {}
                    }
                }
            }
            let (operation, protocol) = match &frozen {
                FrozenConfiguration::Generative(frozen) => (
                    ExecutionOperation::LlmRequest,
                    Some(frozen.llm.provider.transport),
                ),
                FrozenConfiguration::Decision(FrozenDecisionConfig::Native(_)) => {
                    (ExecutionOperation::Decision, None)
                }
                FrozenConfiguration::Decision(FrozenDecisionConfig::Generative(frozen, _)) => (
                    ExecutionOperation::LlmRequest,
                    Some(frozen.llm.provider.transport),
                ),
            };
            if key.3 != requirement(Some(operation), protocol)
                || !executor.capabilities.contains(&(operation, protocol))
            {
                continue;
            }
            return self
                .reserve_pool_allocation(
                    &pool,
                    &executor,
                    &job,
                    *key,
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

    #[allow(clippy::too_many_arguments)]
    fn reserve_pool_allocation(
        &mut self,
        pool: &crate::db::entities::AllocationPool,
        executor: &crate::db::entities::AllocationExecutor,
        row: &crate::db::entities::MutableJob,
        ready_key: (u8, i64, u128, u16),
        request: crate::db::entities::DevelopmentReservationRequest,
        operation: ExecutionOperation,
        protocol: Option<LlmTransport>,
        scheduled_external: Option<(crate::db::entities::ScheduledExternalPolicyId, u32)>,
    ) -> Result<crate::db::entities::PoolAllocation, BatchError> {
        use crate::db::entities::{AllocationPhase, AllocationToken, PoolAllocation};
        let available = self.step(|tx| {
            let held =
                active_allocations(&tx.open_table(ACTIVE_ALLOCATIONS)?, pool.id.get(), None)?;
            let occupied = held.len() as u64;
            let executor_free = !held.iter().any(|(exec, _)| *exec == executor.id.get());
            Ok((executor_free, occupied))
        })?;
        if !available.0 || available.1 >= u64::from(pool.capacity.get()) {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "Job, Executor or Pool capacity is unavailable",
            ));
        }
        let allocation = PoolAllocation {
            request_id: request.id,
            request_after: request.after,
            token: AllocationToken {
                job: row.id,
                pool: pool.id,
                executor: executor.id,
                generation: std::num::NonZeroU32::MIN,
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
            tx.open_table(PENDING)?.remove(row.id.get())?;
            let mut bindings = tx.open_table(SCHEDULED_EXTERNAL_BINDING)?;
            match scheduled_external {
                Some((policy, policy_revision)) => {
                    let binding = crate::db::entities::ScheduledExternalBinding {
                        job: row.id,
                        policy,
                        policy_revision,
                        allocation_epoch: allocation.token.epoch,
                        allocation_generation: allocation.token.generation,
                    };
                    bindings.insert(
                        row.id.get(),
                        encode_scheduled_external_binding(&binding).as_slice(),
                    )?;
                }
                None => {
                    bindings.remove(row.id.get())?;
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
        self.get(POOL_ALLOCATIONS, job.get(), |bytes| {
            decode_pool_allocation(job.get(), bytes)
        })
    }

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
        if current_pool.as_ref().is_none_or(|current| {
            !current.enabled
                || current.archived
                || current.id != token.pool
                || current.id != pool.id
                || current.revision < allocation.pool_revision
                || current.revision < pool.revision
        }) || current_executor.as_ref().is_none_or(|current| {
            !current.enabled
                || current.archived
                || current.id != token.executor
                || current.id != executor.id
                || current.pool != token.pool
                || current.revision < allocation.executor_revision
                || current.revision < executor.revision
                || !current
                    .capabilities
                    .contains(&(allocation.operation, allocation.protocol))
        }) || !pool.enabled
            || pool.archived
            || !executor.enabled
            || executor.archived
            || executor.pool != pool.id
            || pool.id != token.pool
            || executor.id != token.executor
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
            Ok(executor_exclusive && own)
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
                FrozenConfiguration::Decision(configuration),
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
                    let crate::db::entities::MutableContextSelection::AdoptedSummary {
                        producer,
                        source,
                    } = job.context_selection
                    else {
                        return Err(stale());
                    };
                    if source != summary.summary.source {
                        return Err(stale());
                    }
                    let current = self.step(|tx| {
                        match reader::verify_native_summary_tables(
                            &tx.open_table(JOBS)?,
                            &tx.open_table(JOB_STATES)?,
                            &tx.open_table(JOB_OUTPUTS)?,
                            &tx.open_table(MESSAGES)?,
                            job.parent_id,
                            job.id,
                            producer,
                            source,
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
                    if current.manifest != summary.manifest || current.summary != summary.summary {
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

    pub(crate) fn start_mutable_job(
        &mut self,
        prepared: &crate::db::handle::PreparedDevelopmentHandoff,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase};
        self.require_allocation_durability()?;
        if !prepared.has_bounded_inputs() {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "prepared inputs are incomplete",
            ));
        }
        let candidate = prepared.candidate();
        self.revalidate_development_snapshot(candidate)?;
        let job_id = match candidate {
            reader::DevelopmentCandidate::Generative(context) => context.job.id,
            reader::DevelopmentCandidate::Decision(context) => context.job.id,
        };
        self.scheduled_external_authorization(job_id)?;
        let (snapshot, allocation) = match candidate {
            reader::DevelopmentCandidate::Generative(context) => {
                if context.configuration.native_execution_refusal().is_some() {
                    return self.reject(Violation::InvalidValue(
                        "handoff",
                        "execution policy or provider requires unsupported preparation",
                    ));
                }
                (&context.job, &context.allocation)
            }
            reader::DevelopmentCandidate::Decision(context) => (&context.job, &context.allocation),
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
        if let Some(reason) = self.unsupported_start_context(snapshot)? {
            return self.reject(Violation::InvalidValue("handoff", reason));
        }
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
            || state.outcome.is_some()
            || state.stop_confirmed_at.is_some()
            || state.stop_confirmed_by.is_some()
            || self.job_control(job.parent_id)?.closing.is_some()
        {
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

    pub(crate) fn finalize_development_job(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        expected_seq: u32,
        mut finalization: crate::db::entities::JobFinalization,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        self.require_development_write(proof)?;
        let job_id = proof.token().job;
        if let Some(job) = self.mutable_job(job_id)?
            && let Some(source) = job.summary_source
            && let crate::db::entities::JobFinalization::Completed(ref mut output) = finalization
        {
            let raw = output
                .summary
                .as_deref()
                .ok_or(BatchError(Failure::Rejected(Violation::InvalidValue(
                    "summary",
                    "summary result text is missing",
                ))))?;
            let summary: crate::db::entities::SummarizationResult = serde_json::from_str(raw)
                .map_err(|_| {
                    BatchError(Failure::Rejected(Violation::InvalidValue(
                        "summary",
                        "summary result must be valid JSON",
                    )))
                })?;
            if summary.validate().is_err() || summary.source != source {
                return self.reject(Violation::InvalidValue(
                    "summary",
                    "summary result failed schema or source validation",
                ));
            }
            let summary_value = serde_json::to_value(&summary).map_err(|_| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "summary serialization failed",
                )))
            })?;
            let body = crate::db::entities::MessageBody::new(&summary_value).map_err(|_| {
                BatchError(Failure::Rejected(Violation::InvalidValue(
                    "summary",
                    "summary payload exceeds maximum message size",
                )))
            })?;
            self.put_message(&body)?;
            let producer_job = ContentDigest::of(&encode_mutable_job(&job));
            let link = crate::db::entities::NativeSummaryResultLink {
                source,
                producer_job,
                payload: body.digest(),
            };
            output.value = Some(serde_json::to_value(link).map_err(|_| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "summary link serialization failed",
                )))
            })?);
            output.summary = Some(summary.summary);
        }
        self.finish_development_job(proof, expected_seq, finalization)
    }

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
        let job_id = proof.token().job;
        let Some(job) = self.mutable_job(job_id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job_id)));
        };
        let (operation, decision_frozen, request) = match (job.frozen, job.input.content) {
            (
                Some(MutableFrozenConfiguration::Decision(frozen)),
                MutableJobContent::Decision { request, .. },
            ) => (
                ExecutionOperation::Decision,
                FrozenDecisionConfig::Native(frozen),
                request,
            ),
            (
                Some(MutableFrozenConfiguration::Generative(frozen)),
                MutableJobContent::Decision { request, .. },
            ) => (
                ExecutionOperation::LlmRequest,
                FrozenDecisionConfig::Generative(frozen, None),
                request,
            ),
            _ => {
                return self.reject(Violation::InvalidValue(
                    "decision",
                    "frozen Decision request required",
                ));
            }
        };
        self.require_development_write_for(proof, operation)?;
        if result
            .validate(
                &request,
                &decision_frozen,
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
        let is_predictive_compaction =
            request.state.get("kind").and_then(|v| v.as_str()) == Some("predictive_compaction");
        let predictive_accept = if is_predictive_compaction {
            match &result.outcome {
                crate::db::entities::DecisionOutcome::Answer {
                    answer:
                        crate::db::entities::DecisionAnswer::BinaryProbability { true_probability },
                } => Some(*true_probability),
                _ => None,
            }
        } else {
            None
        };
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
        let outcome = self.finish_development_job(
            proof,
            expected_seq,
            crate::db::entities::JobFinalization::Completed(crate::db::entities::FinalResult {
                summary: None,
                code: None,
                goal_reached: None,
                value: Some(value),
            }),
        );
        if outcome.is_ok()
            && let Some(true_probability) = predictive_accept
            && let Ok((policy, _, _)) =
                self.step(|tx| reader::effective_context_policy(&tx.open_table(META)?))
            && let Some(pred) = &policy.predictive
            && pred.mode == crate::db::entities::PredictionMode::Prepare
        {
            let min_prob = pred
                .acceptance
                .as_ref()
                .map_or(500_000, |a| a.minimum_probability_millionths);
            if true_probability * 1_000_000.0 >= f64::from(min_prob) {
                let summarizer = policy.summarization_model.or(match &decision_frozen {
                    FrozenDecisionConfig::Generative(g, _) => Some(g.llm.model.id),
                    FrozenDecisionConfig::Native(_) => None,
                });
                if let Some(summarizer) = summarizer {
                    let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
                    ctx.update(b"native-predictive-compaction");
                    ctx.update(&job.parent_id.get().to_le_bytes());
                    ctx.update(&job_id.get().to_le_bytes());
                    let digest: [u8; 32] = ctx.finish().as_ref().try_into().unwrap_or([0; 32]);
                    let submission = crate::db::entities::Submission {
                        key: crate::db::entities::SubmissionKey::from_digest(digest),
                        fingerprint: crate::db::entities::RequestFingerprint::from_digest(digest),
                    };
                    let _ =
                        self.request_compaction_for(job.parent_id, summarizer, submission, None);
                }
            }
        }
        outcome
    }

    fn finish_development_job(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        expected_seq: u32,
        finalization: crate::db::entities::JobFinalization,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
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
            if tx
                .open_table(UNSETTLED)?
                .get(token.job.get())?
                .map(|v| v.value())
                != Some((0, 0))
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
        if kind == OutcomeKind::Completed
            && job.summary_source.is_none()
            && matches!(
                job.input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            )
            && let Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) =
                &job.frozen
        {
            let _ = self.maybe_trigger_native_compaction(job.parent_id, frozen);
        }
        Ok(crate::db::entities::AppendedOutput {
            output: id,
            seq: i64::from(seq),
        })
    }

    fn latest_verified_summary(
        &mut self,
        conversation: JobId,
        consumer: JobId,
    ) -> Result<Option<(JobId, ContentDigest)>, BatchError> {
        let candidates = self.step(|tx| {
            let children = tx.open_table(CHILDREN)?;
            let jobs = tx.open_table(JOBS)?;
            let mut found = Vec::new();
            for entry in children
                .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                .rev()
            {
                let child_id = JobId::new(entry?.0.value().1);
                let Some(bytes) = jobs.get(child_id.get())? else {
                    continue;
                };
                if bytes.value().first() != Some(&TAG_JOB) {
                    continue;
                }
                let child = decode_mutable_job(child_id.get(), bytes.value())?;
                if let Some(source) = child.summary_source {
                    found.push((child_id, source));
                    if found.len() >= 8 {
                        break;
                    }
                }
            }
            Ok(found)
        })?;
        for (producer, source) in candidates {
            let ok = self.step(|tx| {
                Ok(reader::verify_native_summary_tables(
                    &tx.open_table(JOBS)?,
                    &tx.open_table(JOB_STATES)?,
                    &tx.open_table(JOB_OUTPUTS)?,
                    &tx.open_table(MESSAGES)?,
                    conversation,
                    consumer,
                    producer,
                    source,
                )
                .is_ok())
            })?;
            if ok {
                return Ok(Some((producer, source)));
            }
        }
        Ok(None)
    }

    fn maybe_trigger_native_compaction(
        &mut self,
        conversation: JobId,
        frozen: &FrozenExecutionConfig,
    ) -> Result<(), BatchError> {
        let Some(window) = frozen.llm.config.context_window else {
            return Ok(());
        };
        let reserve = frozen
            .llm
            .config
            .parameters
            .max_completion_tokens
            .or(frozen.llm.config.parameters.max_tokens)
            .unwrap_or(crate::db::entities::SUMMARIZER_OUTPUT_RESERVE);
        let (policy, _, _) =
            self.step(|tx| reader::effective_context_policy(&tx.open_table(META)?))?;
        let Ok(recipe) = policy.recipe(Some(window), reserve, 1024, 0) else {
            return Ok(());
        };
        let Some(trigger) = recipe.trigger else {
            return Ok(());
        };
        let summarizer = policy.summarization_model.unwrap_or(frozen.llm.model.id);
        let total_tokens = self.step(|tx| {
            let turns = tx.open_table(TURNS)?;
            let Some(tip) = reader::turn_head(&turns, conversation)? else {
                return Ok(0usize);
            };
            let jobs = tx.open_table(JOBS)?;
            let outputs = tx.open_table(JOB_OUTPUTS)?;
            let mut cursor = Some(tip);
            let mut total_tokens = 0usize;
            let mut steps = 0usize;
            while let Some(turn_id) = cursor {
                steps += 1;
                if steps > 4096 {
                    break;
                }
                let Some(row) = jobs.get(turn_id.get())? else {
                    break;
                };
                let turn_job = decode_mutable_job(turn_id.get(), row.value())?;
                let mut bytes = 16usize
                    .saturating_add(turn_job.input.text().len())
                    .saturating_add(turn_job.input.attachments().len().saturating_mul(4800));
                for entry in outputs.range((turn_id.get(), 0)..=(turn_id.get(), u32::MAX))? {
                    let (key, value) = entry?;
                    let out = decode_output(key.value(), value.value())?;
                    if out.kind == OutputKind::Result {
                        bytes = bytes
                            .saturating_add(out.display_text().as_ref().map_or(0, String::len));
                    }
                }
                total_tokens = total_tokens.saturating_add(bytes.div_ceil(3));
                cursor = turn_job.previous;
            }
            Ok(total_tokens)
        })?;
        let cooldown_micros = i64::from(recipe.cooldown_seconds).saturating_mul(1_000_000);
        let pred_interval_micros = policy.predictive.as_ref().map_or(0, |p| {
            i64::from(p.minimum_interval_seconds).saturating_mul(1_000_000)
        });
        let now_micros = self.now.as_unix_micros();
        let (summary_blocked, pred_blocked, pred_count) = self.step(|tx| {
            let children = tx.open_table(CHILDREN)?;
            let jobs = tx.open_table(JOBS)?;
            let states = tx.open_table(JOB_STATES)?;
            let mut summary_blocked = false;
            let mut pred_blocked = false;
            let mut pred_count = 0u32;
            for entry in
                children.range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
            {
                let (entry_key, _) = entry?;
                let (_, child_id) = entry_key.value();
                let Some(row) = jobs.get(child_id)? else {
                    continue;
                };
                let Ok(child_job) = decode_mutable_job(child_id, row.value()) else {
                    continue;
                };
                let outcome = states
                    .get(child_id)?
                    .map(|s| JobState::decode(s.value()))
                    .transpose()?
                    .and_then(|s| s.outcome);
                if child_job.summary_source.is_some() {
                    if outcome.is_none()
                        || (cooldown_micros > 0
                            && now_micros.saturating_sub(child_job.created_at.as_unix_micros())
                                < cooldown_micros)
                    {
                        summary_blocked = true;
                    }
                } else if let crate::db::entities::MutableJobContent::Decision { request, .. } =
                    &child_job.input.content
                    && request.state.get("kind").and_then(|v| v.as_str())
                        == Some("predictive_compaction")
                {
                    pred_count = pred_count.saturating_add(1);
                    if outcome.is_none()
                        || (pred_interval_micros > 0
                            && now_micros.saturating_sub(child_job.created_at.as_unix_micros())
                                < pred_interval_micros)
                    {
                        pred_blocked = true;
                    }
                }
            }
            Ok((summary_blocked, pred_blocked, pred_count))
        })?;
        if summary_blocked {
            return Ok(());
        }
        if total_tokens >= trigger as usize {
            let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
            ctx.update(b"native-auto-compaction");
            ctx.update(&conversation.get().to_le_bytes());
            ctx.update(&self.now.as_unix_micros().to_le_bytes());
            let digest: [u8; 32] = ctx.finish().as_ref().try_into().unwrap_or([0; 32]);
            let submission = crate::db::entities::Submission {
                key: crate::db::entities::SubmissionKey::from_digest(digest),
                fingerprint: crate::db::entities::RequestFingerprint::from_digest(digest),
            };
            let _ = self.request_compaction_for(conversation, summarizer, submission, Some(recipe));
        } else if let Some(pred) = &policy.predictive
            && pred.mode == crate::db::entities::PredictionMode::Prepare
            && !pred_blocked
            && pred_count < pred.max_preparations
            && total_tokens >= (trigger as usize).saturating_mul(3) / 4
        {
            let decision_request = crate::db::entities::DecisionRequest {
                schema_version: 1,
                question: crate::db::entities::DecisionQuestion::BinaryProbability {
                    prompt: "Predict whether context compaction should be prepared before the next turn."
                        .to_owned(),
                },
                state: serde_json::json!({
                    "kind": "predictive_compaction",
                    "conversation": conversation.get().to_string(),
                    "tokens": total_tokens,
                    "trigger": trigger,
                }),
            };
            let cfg = ExecutionConfiguration {
                reasoning_effort: None,
                agent_profile: None,
                llm_profile: ProfileId::new(pred.decision_agent.get()),
            };
            if let Ok(frozen_cfg) = self.freeze_configuration(cfg) {
                let decision_frozen = match frozen_cfg {
                    FrozenConfiguration::Decision(d) => d,
                    FrozenConfiguration::Generative(g) => FrozenDecisionConfig::Generative(g, None),
                };
                let wrapped = FrozenConfiguration::Decision(decision_frozen.clone());
                if let Ok(storable) = self.storable_frozen(&wrapped) {
                    let input = crate::db::entities::MutableJobInput {
                        priority: crate::db::entities::JobQueuePriority::Speculative,
                        content: crate::db::entities::MutableJobContent::Decision {
                            profile: decision_frozen.profile_ref().id,
                            references: decision_frozen.references().collect(),
                            request: decision_request,
                        },
                    };
                    let pred_id = JobId::new(self.next_id()?);
                    let pred_job = crate::db::entities::MutableJob {
                        id: pred_id,
                        parent_id: conversation,
                        previous: None,
                        composer_context: crate::db::entities::ComposerContextId::new(
                            self.next_id()?,
                        ),
                        created_at: self.now,
                        submitted_at: Some(self.now),
                        revision: 1,
                        phase: crate::db::entities::MutableJobPhase::Queued,
                        input,
                        context_selection:
                            crate::db::entities::MutableContextSelection::FullHistory,
                        summary_source: None,
                        frozen: Some(storable),
                    };
                    let req_tag = mutable_configuration_requirement(&wrapped);
                    self.enqueue_mutable_job(&pred_job, req_tag, false, true)?;
                }
            }
        }
        Ok(())
    }

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
        self.advance_recurrence_on_settlement(job, kind)?;
        Ok((seq, id))
    }

    fn planning_policy_for(
        &mut self,
        parent: &JobSpec,
        job: &crate::db::entities::MutableJob,
        generative: &FrozenExecutionConfig,
    ) -> Result<
        Option<(
            ProjectId,
            crate::db::entities::ProjectRecord,
            crate::db::entities::ProjectDecisionConfig,
        )>,
        BatchError,
    > {
        if job.summary_source.is_some()
            || !generative.agent.as_ref().is_some_and(|agent| {
                !agent.capabilities.is_empty() || agent.workspace_grant().is_some()
            })
        {
            return Ok(None);
        }
        let JobKind::Group {
            project: Some(project_id),
            ref origin,
            ..
        } = parent.kind
        else {
            return Ok(None);
        };
        if let Origin::Agent { call: opened } = origin
            && self
                .tool_evidence(*opened)?
                .is_some_and(|opening| opening.name == "job_plan")
        {
            return Ok(None);
        }
        let project = self.require_project_record(project_id)?;
        let policy = project.decision_policy();
        Ok(policy.enabled.then_some((project_id, project, policy)))
    }

    fn maybe_attach_planning_decision(
        &mut self,
        parent: &JobSpec,
        job: &crate::db::entities::MutableJob,
        frozen: &FrozenConfiguration,
    ) -> Result<bool, BatchError> {
        use crate::db::entities::{
            ChoiceOption, ComposerContextId, DecisionQuestion, DecisionRequest,
            FrozenDecisionConfig, JobQueuePriority, MutableContextSelection, MutableJob,
            MutableJobContent, MutableJobInput, MutableJobPhase,
        };
        let FrozenConfiguration::Generative(generative) = frozen else {
            return Ok(false);
        };
        let Some((project_id, project, policy)) =
            self.planning_policy_for(parent, job, generative)?
        else {
            return Ok(false);
        };
        let llm_profile = policy
            .decision_profile
            .map_or(generative.llm.model.id.into(), |id| {
                ProfileId::new(id.get())
            });
        let cfg = ExecutionConfiguration {
            reasoning_effort: None,
            agent_profile: None,
            llm_profile,
        };
        let frozen_decision = match self.freeze_configuration(cfg) {
            Ok(FrozenConfiguration::Decision(d)) => d,
            Ok(FrozenConfiguration::Generative(g)) => FrozenDecisionConfig::Generative(g, None),
            Err(_) => FrozenDecisionConfig::Generative(generative.clone(), None),
        };
        let wrapped = FrozenConfiguration::Decision(frozen_decision.clone());
        let storable = self.storable_frozen(&wrapped)?;
        let request = DecisionRequest {
            schema_version: 1,
            question: DecisionQuestion::Choice {
                prompt: format!(
                    "Decide whether the task below can run directly or requires structured job_plan planning before side-effecting tools.\nTask: {}",
                    job.input.text()
                ),
                options: vec![
                    ChoiceOption {
                        id: "direct".to_owned(),
                        description: "Simple single-step task; no job_plan required.".to_owned(),
                    },
                    ChoiceOption {
                        id: "phased".to_owned(),
                        description:
                            "Multi-step task; require job_plan before side-effecting tools."
                                .to_owned(),
                    },
                    ChoiceOption {
                        id: "delegated".to_owned(),
                        description:
                            "Complex task; require job_plan delegation before side-effecting tools."
                                .to_owned(),
                    },
                ],
            },
            state: serde_json::json!({
                "kind": "project_planning_decision",
                "target_job": job.id.get().to_string(),
                "project": project_id.get().to_string(),
                "decision_revision": project.decision_revision,
            }),
        };
        let dec_id = JobId::new(self.next_id()?);
        let dec_job = MutableJob {
            id: dec_id,
            parent_id: job.parent_id,
            previous: None,
            composer_context: ComposerContextId::new(self.next_id()?),
            created_at: self.now,
            submitted_at: Some(self.now),
            revision: 1,
            phase: MutableJobPhase::Queued,
            input: MutableJobInput {
                priority: JobQueuePriority::Foreground,
                content: MutableJobContent::Decision {
                    profile: frozen_decision.profile_ref().id,
                    references: frozen_decision.references().collect(),
                    request,
                },
            },
            context_selection: MutableContextSelection::FullHistory,
            summary_source: None,
            frozen: Some(storable),
        };
        let req_tag = mutable_configuration_requirement(&wrapped);
        self.enqueue_mutable_job(&dec_job, req_tag, false, true)?;
        self.step(|tx| {
            tx.open_table(DEPENDENCY_WAITERS)?.insert(
                (dec_id.get(), job.id.get()),
                DependencyCondition::Settled.code(),
            )?;
            Ok(())
        })?;
        Ok(true)
    }

    fn require_planning_decision_satisfied(&mut self, call: ToolCallRef) -> Result<(), BatchError> {
        use crate::db::entities::{
            DecisionAnswer, DecisionOutcome, DecisionResult, MutableFrozenConfiguration,
            MutableJobContent,
        };
        let Some(job) = self.mutable_job(call.job)? else {
            return Ok(());
        };
        let Some(MutableFrozenConfiguration::Generative(generative)) = &job.frozen else {
            return Ok(());
        };
        let parent = self.require_mutable_conversation(job.parent_id)?;
        let Some((_, _, policy)) = self.planning_policy_for(&parent, &job, generative)? else {
            return Ok(());
        };
        let target_str = call.job.get().to_string();
        let dec_job_id = self.step(|tx| {
            let children = tx.open_table(CHILDREN)?;
            let jobs = tx.open_table(JOBS)?;
            for entry in
                children.range((job.parent_id.get(), 0)..=(job.parent_id.get(), u128::MAX))?
            {
                let (key, _) = entry?;
                let (_, child_id) = key.value();
                let Some(row) = jobs.get(child_id)? else {
                    continue;
                };
                let Ok(child) = decode_mutable_job(child_id, row.value()) else {
                    continue;
                };
                if let MutableJobContent::Decision { request, .. } = &child.input.content
                    && request.state.get("kind").and_then(|v| v.as_str())
                        == Some("project_planning_decision")
                    && request.state.get("target_job").and_then(|v| v.as_str())
                        == Some(target_str.as_str())
                {
                    return Ok(Some(JobId::new(child_id)));
                }
            }
            Ok(None)
        })?;
        let Some(dec_job_id) = dec_job_id else {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "planning decision missing; side-effecting tool refused",
            ));
        };
        let dec_state = self.require_job_state(dec_job_id)?;
        if dec_state.outcome.as_ref().map(|o| o.kind) != Some(OutcomeKind::Completed) {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "planning decision failed or uncertain; side-effecting tool refused",
            ));
        }
        let decision_result: Option<DecisionResult> = self.step(|tx| {
            let outputs = tx.open_table(JOB_OUTPUTS)?;
            for entry in outputs.range((dec_job_id.get(), 0)..=(dec_job_id.get(), u32::MAX))? {
                let (key, value) = entry?;
                let out = decode_output(key.value(), value.value())?;
                if out.kind == OutputKind::Result
                    && let Some(val) = out.value.as_ref().and_then(|v| v.get("decision"))
                {
                    return Ok(serde_json::from_value::<DecisionResult>(val.clone()).ok());
                }
            }
            Ok(None)
        })?;
        let Some(DecisionResult {
            outcome:
                DecisionOutcome::Answer {
                    answer:
                        DecisionAnswer::Choice {
                            selected,
                            confidence,
                            ..
                        },
                },
            ..
        }) = decision_result
        else {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "planning decision outcome unusable; side-effecting tool refused",
            ));
        };
        if let Some(conf) = confidence
            && (conf.value * 1000.0) < f64::from(policy.minimum_confidence_millis)
        {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "planning decision confidence below policy minimum; call job_plan first",
            ));
        }
        if selected == "direct" {
            return Ok(());
        }
        let has_plan = self.previous_tool_evidence(call)?.iter().any(|prev| {
            prev.name == "job_plan"
                && matches!(prev.decision.fact, ToolDecision::Admitted(_))
                && !prev.outcome.as_ref().is_some_and(|o| {
                    matches!(
                        o.fact,
                        ToolOutcome::NotApplied { .. } | ToolOutcome::NotInvoked { .. }
                    )
                })
        });
        if !has_plan {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "project decision policy requires job_plan before side-effecting tools",
            ));
        }
        Ok(())
    }

    fn advance_recurrence_on_settlement(
        &mut self,
        job: &crate::db::entities::MutableJob,
        kind: OutcomeKind,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{Recurrence, RecurrenceHalt, RecurrencePhase};
        let Some(mut rec) = self.get(RECURRENCES, job.parent_id.get(), Recurrence::decode)? else {
            return Ok(());
        };
        if rec.phase != RecurrencePhase::Active || rec.current_turn != job.id {
            return Ok(());
        }
        if kind == OutcomeKind::Completed {
            let control = self.job_control(job.parent_id)?;
            let parent_settled = self.require_job_state(job.parent_id)?.outcome.is_some();
            if control.closing.is_some() || control.hidden_at.is_some() || parent_settled {
                rec.phase = RecurrencePhase::Halted {
                    reason: RecurrenceHalt::Closed,
                };
            } else {
                let (limits, _) = self.group_limits(job.parent_id)?;
                if rec.n >= u64::from(limits.max_iterations) {
                    rec.phase = RecurrencePhase::Halted {
                        reason: RecurrenceHalt::LimitReached,
                    };
                } else {
                    let due = self
                        .now
                        .as_unix_micros()
                        .saturating_add(i64::from(rec.interval_s).saturating_mul(1_000_000));
                    rec.phase = RecurrencePhase::WaitingDue { due };
                    self.step(|tx| {
                        tx.open_table(RECURRENCE_DUE)?
                            .insert((due, job.parent_id.get()), ())?;
                        Ok(())
                    })?;
                }
            }
        } else {
            let reason = if kind == OutcomeKind::Cancelled {
                RecurrenceHalt::Closed
            } else {
                RecurrenceHalt::Refused
            };
            rec.phase = RecurrencePhase::Halted { reason };
        }
        self.insert(RECURRENCES, job.parent_id.get(), rec.encode().as_slice())
    }

    pub(crate) fn claim_due_recurrences(
        &mut self,
        limit: usize,
    ) -> Result<Option<std::time::Duration>, BatchError> {
        use crate::db::entities::{Recurrence, RecurrenceHalt, RecurrencePhase};
        let now_micros = self.now.as_unix_micros();
        let due_entries = self.step(|tx| {
            let table = tx.open_table(RECURRENCE_DUE)?;
            let mut due = Vec::new();
            for entry in table.range((i64::MIN, 0)..=(now_micros, u128::MAX))? {
                if due.len() >= limit {
                    break;
                }
                let (key, _) = entry?;
                due.push(key.value());
            }
            Ok(due)
        })?;
        for (due_micros, conv_u128) in due_entries {
            self.step(|tx| {
                tx.open_table(RECURRENCE_DUE)?
                    .remove((due_micros, conv_u128))?;
                Ok(())
            })?;
            let Some(mut rec) = self.get(RECURRENCES, conv_u128, Recurrence::decode)? else {
                continue;
            };
            if rec.phase != (RecurrencePhase::WaitingDue { due: due_micros }) {
                continue;
            }
            let conv = JobId::new(conv_u128);
            let control = self.job_control(conv)?;
            let parent_settled = self.require_job_state(conv)?.outcome.is_some();
            if control.closing.is_some() || control.hidden_at.is_some() || parent_settled {
                rec.phase = RecurrencePhase::Halted {
                    reason: RecurrenceHalt::Closed,
                };
                self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                continue;
            }
            let (limits, _) = self.group_limits(conv)?;
            if rec.n >= u64::from(limits.max_iterations) {
                rec.phase = RecurrencePhase::Halted {
                    reason: RecurrenceHalt::LimitReached,
                };
                self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                continue;
            }
            let Some(prev_turn) = self.mutable_job(rec.current_turn)? else {
                rec.phase = RecurrencePhase::Halted {
                    reason: RecurrenceHalt::ConfigUnavailable,
                };
                self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                continue;
            };
            let objective = prev_turn.input.text().to_owned();
            let priority = prev_turn.input.priority;
            let cfg = match prev_turn.input.configuration() {
                Some(cfg) => cfg,
                None => {
                    rec.phase = RecurrencePhase::Halted {
                        reason: RecurrenceHalt::ConfigUnavailable,
                    };
                    self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                    continue;
                }
            };
            let next_n = rec.n.saturating_add(1);
            let sub = |label: &[u8]| {
                let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
                ctx.update(label);
                ctx.update(&conv_u128.to_le_bytes());
                ctx.update(&next_n.to_le_bytes());
                let digest: [u8; 32] = ctx.finish().as_ref().try_into().unwrap_or([0; 32]);
                crate::db::entities::Submission {
                    key: crate::db::entities::SubmissionKey::from_digest(digest),
                    fingerprint: crate::db::entities::RequestFingerprint::from_digest(digest),
                }
            };
            match self.host_turn(
                conv,
                objective,
                cfg,
                priority,
                sub(b"recurrence-turn-create"),
                sub(b"recurrence-turn-submit"),
            ) {
                Ok(next_turn) => {
                    rec.phase = RecurrencePhase::Active;
                    rec.n = next_n;
                    rec.previous_turn = Some(rec.current_turn);
                    rec.current_turn = next_turn;
                    self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                }
                Err(_) => {
                    rec.phase = RecurrencePhase::Halted {
                        reason: RecurrenceHalt::ConfigUnavailable,
                    };
                    self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                }
            }
        }
        let next = self.step(|tx| {
            let table = tx.open_table(RECURRENCE_DUE)?;
            Ok(table.first()?.map(|(k, _)| k.value().0))
        })?;
        Ok(next.map(|due| {
            let delta = due.saturating_sub(self.now.as_unix_micros()).max(0);
            std::time::Duration::from_micros(u64::try_from(delta).unwrap_or(0))
        }))
    }

    pub(crate) fn resume_recurrence(&mut self, conversation: JobId) -> Result<(), BatchError> {
        use crate::db::entities::{Recurrence, RecurrencePhase};
        let Some(mut rec) = self.get(RECURRENCES, conversation.get(), Recurrence::decode)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(conversation)));
        };
        if !matches!(rec.phase, RecurrencePhase::Halted { .. }) {
            return Ok(());
        }
        let control = self.job_control(conversation)?;
        let parent_settled = self.require_job_state(conversation)?.outcome.is_some();
        if control.closing.is_some() || control.hidden_at.is_some() || parent_settled {
            return self.reject(Violation::InvalidValue(
                "recurrence",
                "cannot resume a closed recurrence",
            ));
        }
        let (limits, _) = self.group_limits(conversation)?;
        if rec.n >= u64::from(limits.max_iterations) {
            return self.reject(Violation::InvalidValue(
                "recurrence",
                "recurrence iteration limit reached",
            ));
        }
        let due = self.now.as_unix_micros();
        rec.phase = RecurrencePhase::WaitingDue { due };
        self.step(|tx| {
            tx.open_table(RECURRENCE_DUE)?
                .insert((due, conversation.get()), ())?;
            Ok(())
        })?;
        self.insert(RECURRENCES, conversation.get(), rec.encode().as_slice())?;
        self.wake_scheduler();
        Ok(())
    }

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

    fn end_untouched_reservation(
        &mut self,
        token: crate::db::entities::AllocationToken,
        kind: OutcomeKind,
        result: crate::db::entities::FinalResult,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase};
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
        {
            return Err(refused(
                "the Job is not the queued revision that was reserved",
            ));
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

    pub(crate) fn reconcile_stopped_allocation(
        &mut self,
        action: &super::recovery::RecoveryAction,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase, OutcomeRecord};

        self.require_allocation_durability()?;
        if action.token.epoch != self.epoch {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "stale offline recovery epoch",
            ));
        }
        let unchanged = self.step(|tx| {
            let allocation = tx
                .open_table(POOL_ALLOCATIONS)?
                .get(action.token.job.get())?
                .is_some_and(|row| row.value() == action.expected_allocation);
            let state = tx
                .open_table(JOB_STATES)?
                .get(action.token.job.get())?
                .is_some_and(|row| row.value() == action.expected_state);
            Ok(allocation && state)
        })?;
        if !unchanged {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "offline recovery source rows changed",
            ));
        }
        let mut allocation = self.allocation_for_token(action.token)?;
        if allocation.phase != action.phase
            || allocation.stop_confirmed_at.is_some()
            || allocation.stop_proof.is_some()
        {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "offline recovery phase changed",
            ));
        }
        let Some(job) = self.mutable_job(action.token.job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(action.token.job)));
        };
        if job.phase != MutableJobPhase::Executing || job.revision != allocation.job_revision {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "offline recovery Job revision changed",
            ));
        }
        let mut state = self.require_job_state(action.token.job)?;
        match (&state.outcome, action.preserve_unknown) {
            (Some(outcome), true) if outcome.kind == OutcomeKind::Unknown => {}
            (None, false) => {
                state.outcome = Some(OutcomeRecord {
                    kind: OutcomeKind::Unknown,
                    at: self.now,
                    cause: Some(crate::db::entities::OutcomeCause::Uncertain(action.reason)),
                });
                self.put_mutable_job_state(action.token.job, &state)?;
            }
            _ => {
                return self.reject(Violation::InvalidValue(
                    "outcome",
                    "offline recovery would change an existing outcome",
                ));
            }
        }
        let previous = allocation.clone();
        allocation.phase = AllocationPhase::Released;
        allocation.stop_confirmed_at = Some(self.now);
        allocation.stop_proof = Some(action.proof);
        self.put_pool_allocation(&allocation, Some(&previous))?;
        self.freed_capacity = true;
        Ok(())
    }

    pub(crate) fn request_native_stop(
        &mut self,
        job: JobId,
    ) -> Result<crate::db::entities::NativeStopRequest, BatchError> {
        use crate::db::entities::{AllocationPhase, NativeStopRequest};
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
                        summary: Some(STOPPED_BEFORE_START_SUMMARY.to_owned()),
                        code: Some(STOPPED_BEFORE_START_CODE.to_owned()),
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

pub(super) fn requirement(
    operation: Option<ExecutionOperation>,
    protocol: Option<LlmTransport>,
) -> u16 {
    let operation = operation.map_or(0, |operation| u16::from(operation.code()));
    let protocol = protocol.map_or(0, |protocol| u16::from(protocol.code()));
    (operation << 8) | protocol
}

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

pub(super) fn decision_work_requirement(frozen: &FrozenDecisionConfig) -> u16 {
    match frozen {
        FrozenDecisionConfig::Native(_) => decision_requirement(),
        FrozenDecisionConfig::Generative(frozen, _) => execution_requirement(frozen),
    }
}

pub(super) fn decision_requirement() -> u16 {
    requirement(Some(ExecutionOperation::Decision), None)
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
            let crate::db::AdmissionCandidate::Decision(executor, _) = candidate;
            if !self
                .config_record(executor.into())?
                .is_some_and(|row| matches!(row.body, ConfigBody::Pool(_)))
            {
                return self.reject(Violation::NotFound(RecordRef::Executor(executor)));
            }
            if !self.step(|tx| reader::candidate_compatible(tx, candidate))? {
                return self.reject(Violation::InvalidValue(
                    "executor_incompatible",
                    "the Executor cannot run this request",
                ));
            }
        }
        Ok(value)
    }

    pub(crate) fn global_headers(
        &mut self,
    ) -> Result<crate::db::entities::GlobalHeaders, BatchError> {
        self.step(|tx| reader::global_outbound_headers(&tx.open_table(META)?))
    }

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

    pub(super) fn adjust_open_children(
        &mut self,
        job: JobId,
        delta: i64,
    ) -> Result<(), BatchError> {
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

    pub(crate) fn tool_decide(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        evidence: crate::db::entities::ToolEvidence,
    ) -> Result<(), BatchError> {
        let crate::db::entities::ToolEvidence {
            call,
            approval,
            tool_call_id,
            name,
            decision,
            spawns,
        } = evidence;
        let (tool_call_id, name) = (tool_call_id.as_str(), name.as_str());
        self.require_development_call(proof, call)?;
        if matches!(
            &decision,
            ToolDecision::Admitted(admitted) if admitted.effect != ToolEffect::ReadOnly
        ) && name != "job_plan"
        {
            self.require_planning_decision_satisfied(call)?;
        }
        let conversation = self.turn_conversation(call.job)?;
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
            }
        }
        self.probe = Some(Probe::ToolCall(call));
        Ok(())
    }

    pub(crate) fn tool_outcome(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), BatchError> {
        self.require_development_call(proof, call)?;
        let Some(mut record) = self.tool_evidence(call)? else {
            return self.reject(Violation::ToolCallNotFound(call));
        };
        if record.admission().is_none() {
            return self.reject(Violation::ToolCallNotAdmitted(call));
        }
        if record.outcome.is_some() {
            return self.reject(Violation::ToolOutcomeAlreadyRecorded(call));
        }
        if matches!(outcome, ToolOutcome::CompletedElided { .. })
            && record
                .admission()
                .is_none_or(|admission| admission.effect != ToolEffect::ReadOnly)
        {
            return self.reject(Violation::InvalidValue(
                "outcome",
                "only a read-only call may keep just the size and digest of its result",
            ));
        }
        let current_stats = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_STATS)?
                .get(record.name.as_str())?
                .map(|found| found.value())
                .unwrap_or((0, 0, 0, 0, 0, 0)))
        })?;
        let mut updated_stats = current_stats;
        let duration_us = self
            .now
            .as_unix_micros()
            .saturating_sub(record.decision.at.as_unix_micros())
            .max(0) as u64;
        updated_stats.4 = updated_stats.4.saturating_add(duration_us);
        updated_stats.5 = updated_stats.5.max(duration_us);
        match &outcome {
            ToolOutcome::Completed { .. }
            | ToolOutcome::CompletedWithheld { .. }
            | ToolOutcome::CompletedElided { .. } => {
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

    pub(crate) fn require_development_call(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        call: ToolCallRef,
    ) -> Result<(), BatchError> {
        if proof.token().job != call.job {
            return self.reject(Violation::InvalidValue(
                "tool_call",
                "the call belongs to another Job",
            ));
        }
        self.require_development_write(proof)?;
        Ok(())
    }
}

impl Batch {}

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

    fn mutate_scheduled_external_policy(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        reject_revoked: Option<&'static str>,
        apply: impl FnOnce(
            &mut Self,
            crate::db::entities::ScheduledExternalPolicyRecord,
        ) -> Result<
            Option<crate::db::entities::ScheduledExternalPolicyRecord>,
            BatchError,
        >,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        let Some(record) = self.scheduled_external_policy_by_id(id)? else {
            return self.reject(Violation::NotFound(
                crate::db::entities::RecordRef::ScheduledExternalPolicy(id),
            ));
        };
        if record.revoked
            && let Some(msg) = reject_revoked
        {
            return self.reject(Violation::InvalidValue("scheduled_external_policy", msg));
        }
        if record.revision != expected_revision {
            return self.reject(Violation::InvalidValue(
                "scheduled_external_policy",
                "the policy revision changed; reload before saving",
            ));
        }
        let Some(mut updated) = apply(self, record.clone())? else {
            return Ok(record);
        };
        updated.revision = record.revision.checked_add(1).ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "scheduled external policy revision exhausted",
            )))
        })?;
        updated.updated_at = self.now;
        let bytes = encode_scheduled_external_policy(&updated);
        self.step(|tx| {
            tx.open_table(SCHEDULED_EXTERNAL_POLICY)?
                .insert(id.get(), bytes.as_slice())?;
            Ok(())
        })?;
        Ok(updated)
    }

    pub(crate) fn update_scheduled_external_policy(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        draft: &crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        self.mutate_scheduled_external_policy(
            id,
            expected_revision,
            Some("a revoked policy is immutable; create a new one"),
            |batch, record| {
                Ok(Some(crate::db::entities::ScheduledExternalPolicyRecord {
                    endpoint: draft.endpoint().to_owned(),
                    credential_ref: draft.credential_ref().to_owned(),
                    model: draft.model().to_owned(),
                    proxy: draft.proxy().to_owned(),
                    max_request_bytes: u32::try_from(draft.limits().max_request_bytes)
                        .map_err(|_| batch.invalid("scheduled_external_policy"))?,
                    max_output_tokens: draft.limits().max_output_tokens,
                    timeout_secs: u32::try_from(draft.limits().timeout.as_secs())
                        .map_err(|_| batch.invalid("scheduled_external_policy"))?,
                    ..record
                }))
            },
        )
    }

    pub(crate) fn set_scheduled_external_policy_enabled(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        enabled: bool,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        self.mutate_scheduled_external_policy(
            id,
            expected_revision,
            Some("a revoked policy cannot be enabled"),
            |_, record| {
                Ok((record.enabled != enabled).then_some(
                    crate::db::entities::ScheduledExternalPolicyRecord { enabled, ..record },
                ))
            },
        )
    }

    pub(crate) fn revoke_scheduled_external_policy(
        &mut self,
        id: crate::db::entities::ScheduledExternalPolicyId,
        expected_revision: u32,
    ) -> Result<crate::db::entities::ScheduledExternalPolicyRecord, BatchError> {
        self.mutate_scheduled_external_policy(id, expected_revision, None, |_, record| {
            Ok(
                (!record.revoked).then_some(crate::db::entities::ScheduledExternalPolicyRecord {
                    enabled: false,
                    revoked: true,
                    ..record
                }),
            )
        })
    }

    fn scheduled_external_policy_for(
        &mut self,
        frozen: &FrozenExecutionConfig,
    ) -> Result<ScheduledPolicyMatch, BatchError> {
        if !ExecutionControl::ScheduledExternalTransport.is_enabled() {
            return Ok(ScheduledPolicyMatch::None);
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
        let mut matching = matching.into_iter();
        Ok(match (matching.next(), matching.next()) {
            (None, _) => ScheduledPolicyMatch::None,
            (Some(record), None) => ScheduledPolicyMatch::One(record),
            (Some(_), Some(_)) => ScheduledPolicyMatch::Ambiguous,
        })
    }

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
