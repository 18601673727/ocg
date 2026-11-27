use super::{StorageError, TxRead, execution};
use crate::db::entities::{
    Actor, Appended, AttachmentId, ConfigId, ExecutionConfiguration, ExecutionEventKind,
    ExecutionOperation, FrozenConfiguration, FrozenDecisionConfig, JobId, JobLifecycle, JobSpec,
    JobState, LlmTransport, Origin, OutcomeCause, OutcomeKind, OutcomeRecord, ProfileId, ProjectId,
    RecordRef, Recurrence, RecurrenceHalt, RecurrencePhase, SubmissionRecord, Violation,
};
use crate::db::redb::batch::{Batch, BatchError, Commit, Failure, Probe};
use crate::db::redb::codec::{Code, Dec, DecodeResult, Enc, TAG_JOB};
use crate::db::redb::reservations::mutable_configuration_requirement;
use crate::db::redb::rows::encode_submission;
use crate::db::redb::schema::{
    CHILD_GROUPS, CHILDREN, CONVERSATIONS, DRAFT_CONTEXTS, JOB_INPUTS, JOBS, LIVE_CHILDREN,
    MUTABLE_READY, PENDING, RECURRENCE_DUE, RECURRENCES, RETRY_JOBS, SUBMISSIONS, SUCCESSORS,
    TOOL_CALLS, TURNS,
};
use crate::db::redb::state::JobRuntime;
use crate::db::redb::{indexes, reads, state};
use ::redb::ReadableTable;
use std::collections::BTreeSet;

pub(super) const MAX_INPUT_BYTES: usize = 32 * 1024;

pub(super) fn invalid_input_character(c: char) -> bool {
    c.is_control() && !matches!(c, '\t' | '\n' | '\r')
}

pub(super) fn mutable_priority(priority: crate::db::entities::JobQueuePriority) -> u8 {
    use crate::db::entities::JobQueuePriority;
    match priority {
        JobQueuePriority::Foreground => 0,
        JobQueuePriority::Automatic => 1,
        JobQueuePriority::Speculative => 2,
    }
}

pub(super) fn encode_mutable_job(job: &crate::db::entities::MutableJob) -> Vec<u8> {
    use crate::db::entities::{MutableContextSelection, MutableJobPhase};
    let mut e = Enc::new(TAG_JOB);
    e.u128(job.id.get());
    e.u128(job.parent_id.get());
    e.ou128(job.previous.map(JobId::get));
    e.opt(job.retry.as_ref(), |e, retry| {
        e.u128(retry.original.get());
    });
    e.actor(&job.submitted_by);
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
    e.u32(job.child_results.len() as u32);
    for result in &job.child_results {
        e.u128(result.child.get());
        e.u128(result.head.get());
        e.u8(result.outcome.code());
        e.u32(result.answer_start);
        e.u32(result.answer_end);
    }
    e.opt(job.child_notice.as_ref(), |e, call| e.call_ref(call));
    e.ostr(job.prerequisite_context.as_deref());
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
            if text.len() > MAX_INPUT_BYTES
                || text.chars().any(invalid_input_character)
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
            let request = DecisionRequest { state, question };
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

fn mutable_job_header(key: u128, bytes: &[u8]) -> DecodeResult<(JobId, JobId, Dec<'_>)> {
    let mut d = Dec::new(bytes, TAG_JOB)?;
    let id = JobId::new(d.u128()?);
    let parent_id = JobId::new(d.u128()?);
    if id.get() != key || parent_id.get() == 0 || parent_id >= id {
        return Err(StorageError::corrupted("invalid mutable Job"));
    }
    Ok((id, parent_id, d))
}

pub(super) fn mutable_job_parent(key: u128, bytes: &[u8]) -> DecodeResult<JobId> {
    mutable_job_header(key, bytes).map(|(_, parent_id, _)| parent_id)
}

pub(super) fn decode_mutable_job(
    key: u128,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::MutableJob> {
    use crate::db::entities::{MutableContextSelection, MutableJob, MutableJobPhase};
    let (id, parent_id, mut d) = mutable_job_header(key, bytes)?;
    let previous = d.ou128()?.map(JobId::new);
    let retry = d.opt(|d| {
        Ok(crate::db::entities::JobRetry {
            original: JobId::new(d.u128()?),
        })
    })?;
    let submitted_by = d.actor()?;
    if submitted_by == Actor::Loop {
        return Err(StorageError::corrupted("invalid Job author"));
    }
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
    let result_count = d.u32()? as usize;
    if result_count > crate::db::redb::job_tree::MAX_LIVE_CHILDREN {
        return Err(StorageError::corrupted("too many child results on Job"));
    }
    let mut child_results = Vec::with_capacity(result_count);
    for _ in 0..result_count {
        child_results.push(crate::db::entities::ChildTurnResult {
            child: JobId::new(d.u128()?),
            head: JobId::new(d.u128()?),
            outcome: d.code(OutcomeKind::from_code)?,
            answer_start: d.u32()?,
            answer_end: d.u32()?,
        });
    }
    let child_notice = d.opt(Dec::call_ref)?;
    let prerequisite_context = d.ostr()?;
    d.end()?;
    if let Some(context) = &prerequisite_context
        && (previous.is_some()
            || retry.is_some()
            || phase == MutableJobPhase::Unsubmitted
            || !matches!(submitted_by, Actor::Agent { .. })
            || summary_source.is_some()
            || !matches!(
                input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            )
            || !context.starts_with(super::job_tree::PREREQUISITE_HEADER)
            || context.len() > MAX_INPUT_BYTES
            || context.chars().any(invalid_input_character))
    {
        return Err(StorageError::corrupted("invalid Job prerequisite context"));
    }
    if !child_results.is_empty() {
        let crate::db::entities::MutableJobContent::Generative { text, .. } = &input.content else {
            return Err(StorageError::corrupted(
                "non-generative Job has child results",
            ));
        };
        if submitted_by != Actor::Auto || retry.is_some() || summary_source.is_some() {
            return Err(StorageError::corrupted(
                "invalid Job provenance for child results",
            ));
        }
        let mut prev_child = None;
        let mut prev_end = 0usize;
        for result in &child_results {
            let start = result.answer_start as usize;
            let end = result.answer_end as usize;
            if result.child.get() == 0
                || result.head.get() == 0
                || prev_child.is_some_and(|prev| result.child <= prev)
                || start < prev_end
                || start > end
                || end > text.len()
                || !text.is_char_boundary(start)
                || !text.is_char_boundary(end)
            {
                return Err(StorageError::corrupted("invalid child result range on Job"));
            }
            prev_child = Some(result.child);
            prev_end = end;
        }
    }
    if let Some(call) = child_notice
        && (!child_results.is_empty()
            || !matches!(submitted_by, Actor::Agent { .. })
            || retry.is_some()
            || summary_source.is_some()
            || call.job.get() == 0
            || !matches!(
                input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            ))
    {
        return Err(StorageError::corrupted(
            "invalid Job provenance for child notice",
        ));
    }
    if previous.is_some_and(|previous| previous >= id)
        || retry
            .as_ref()
            .is_some_and(|retry| retry.original >= id || retry.original.get() == 0)
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
        retry,
        submitted_by,
        composer_context,
        created_at,
        submitted_at,
        revision,
        phase,
        input,
        context_selection,
        summary_source,
        frozen,
        child_results,
        child_notice,
        prerequisite_context,
    })
}

pub(super) fn is_predictive_helper(
    tx: &impl super::TxRead,
    job: &crate::db::entities::MutableJob,
) -> Result<bool, StorageError> {
    use crate::db::entities::{JobQueuePriority, MutableContextSelection, MutableJobPhase};

    if job.phase == MutableJobPhase::Unsubmitted
        || job.input.priority != JobQueuePriority::Speculative
        || job.previous.is_some()
        || job.retry.is_some()
        || job.summary_source.is_some()
        || job.frozen.is_none()
        || job.submitted_at != Some(job.created_at)
        || job.context_selection != MutableContextSelection::FullHistory
        || !matches!(
        &job.input.content,
        crate::db::entities::MutableJobContent::Decision { request, .. }
            if request.state.get("kind").and_then(|value| value.as_str())
                == Some("predictive_compaction")
        )
    {
        return Ok(false);
    }
    // System Decisions can be non-Turn children. Public Decisions always become Turns.
    let key = (job.parent_id.get(), job.id.get());
    if tx.table(CHILDREN)?.get(key)?.is_none() {
        return Err(StorageError::corrupted(format!(
            "predictive Decision {} is missing its membership in {}",
            job.id, job.parent_id
        )));
    }
    if tx.table(TURNS)?.get(key)?.is_some() {
        return Ok(false);
    }
    let conversations = tx.table(CONVERSATIONS)?;
    let Some(parent) = conversations.get(job.parent_id.get())? else {
        return Err(StorageError::corrupted(format!(
            "predictive Helper {} has missing Group {}",
            job.id, job.parent_id
        )));
    };
    let parent = JobSpec::decode_at(job.parent_id.get(), parent.value())?;
    Ok(parent.parent_id.is_none()
        && !parent.is_chunk_group()
        && !parent.is_standalone_decision_group())
}

pub(super) fn validate_retry_source(
    job: &crate::db::entities::MutableJob,
    original: &crate::db::entities::MutableJob,
) -> Result<(), StorageError> {
    use crate::db::entities::{MutableFrozenConfiguration, MutableJobContent};

    if job.retry.as_ref().map(|retry| retry.original) != Some(original.id)
        || original.id >= job.id
        || original.parent_id != job.parent_id
        || original.retry.is_some()
        || original.submitted_at.is_none()
        || original.summary_source.is_some()
        || job.summary_source.is_some()
        || !matches!(job.input.content, MutableJobContent::Generative { .. })
        || !matches!(original.input.content, MutableJobContent::Generative { .. })
        || !matches!(
            original.frozen,
            Some(MutableFrozenConfiguration::Generative(_))
        )
    {
        return Err(StorageError::corrupted("invalid retry source"));
    }
    Ok(())
}

pub(super) fn execution_input_text<'a>(
    table: &impl ::redb::ReadableTable<u128, &'static [u8]>,
    job: &'a crate::db::entities::MutableJob,
) -> Result<std::borrow::Cow<'a, str>, StorageError> {
    let source = job
        .retry
        .as_ref()
        .map(|retry| {
            let row = table
                .get(retry.original.get())?
                .ok_or_else(|| StorageError::corrupted("retry source missing"))?;
            let source = decode_mutable_job(retry.original.get(), row.value())?;
            validate_retry_source(job, &source)?;
            Ok::<_, StorageError>(source)
        })
        .transpose()?;
    let context = source
        .as_ref()
        .unwrap_or(job)
        .prerequisite_context
        .as_deref();
    let text = job.input.text();
    // The assignment and report each have a 32 KiB cap. The assembled text
    // counts in full against the execution context and model limits.
    Ok(match context {
        Some(context) => std::borrow::Cow::Owned(format!("{text}\n\n{context}")),
        None => std::borrow::Cow::Borrowed(text),
    })
}

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

pub(super) fn decode_mutable_input_revision(
    id: JobId,
    revision: u32,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::MutableJobInput> {
    let mut d = Dec::new(bytes, 52)?;
    let stored_id = d.u128()?;
    let stored_revision = d.u32()?;
    let input = decode_mutable_input(&mut d)?;
    d.end()?;
    if stored_id != id.get() || stored_revision != revision {
        return Err(StorageError::corrupted(
            "mutable job input key does not match record payload",
        ));
    }
    Ok(input)
}

/// Result of [`Batch::capture_prerequisite_context`].
pub(super) enum PrerequisiteCapture {
    /// The conversation has no uncaptured prerequisite context.
    NotRequired,
    /// The prerequisites are not yet satisfied; nothing was written.
    Pending,
    /// The context was written onto the original assignment.
    Captured { assignment: JobId, context: String },
}

pub(super) enum TurnMembership {
    Turn,
    NonTurn,
}

impl Batch {
    pub(super) fn require_generative_predecessors(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<(), BatchError> {
        self.inspect(|tx| require_generative_history(tx, job.parent_id, job.previous, Some(job.id)))
    }

    pub(crate) fn unsupported_start_context(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<Option<&'static str>, BatchError> {
        const CHAIN_LIMIT: usize = 4096;
        const UNSUPPORTED: &str = "child work or tool evidence require unsupported preparation";
        self.step(|tx| {
            let jobs = tx.open_table(JOBS)?;
            let mut previous = job.previous;
            for _ in 0..CHAIN_LIMIT {
                let Some(id) = previous else { break };
                let row = jobs
                    .get(id.get())?
                    .ok_or_else(|| StorageError::corrupted("conversation predecessor missing"))?;
                previous = decode_mutable_job(id.get(), row.value())?.previous;
            }
            if previous.is_some() {
                return Ok(Some(UNSUPPORTED));
            }
            let range = (job.id.get(), 0, 0)..=(job.id.get(), u32::MAX, u32::MAX);
            let tools = tx.open_table(TOOL_CALLS)?;
            Ok(tools
                .range(range)?
                .next()
                .transpose()?
                .is_some()
                .then_some(UNSUPPORTED))
        })
    }

    pub(super) fn mutable_predecessor_ready(
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
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            return Ok(self.pool_allocation(id)?.is_some_and(|allocation| {
                allocation.phase == crate::db::entities::AllocationPhase::Released
            }));
        }
        if !state.confirmed_terminal() {
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
        // A stored selection naming a missing producer is a broken reference,
        // so it must stay fail-closed. A candidate borrowed for a prospective
        // selection only has to be usable.
        let verified = self.verify_summary_source(|tx| {
            if selection != job.context_selection
                && tx.open_table(JOBS)?.get(producer.get())?.is_none()
            {
                return Ok(None);
            }
            reads::execution_context::verify_native_summary_in(
                tx,
                job.parent_id,
                job.id,
                producer,
                source,
            )
        })?;
        Ok(verified.is_some())
    }

    pub(super) fn store_mutable_job(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<(), BatchError> {
        let previous = self.mutable_job(job.id)?;
        if job.submitted_by == Actor::Loop
            || previous
                .as_ref()
                .is_some_and(|old| old.submitted_by != job.submitted_by)
        {
            return self.corrupted("invalid change of Job author".into());
        }
        if previous.as_ref().is_some_and(|old| {
            old.child_results != job.child_results || old.child_notice != job.child_notice
        }) {
            return self.corrupted("invalid change of Job child metadata".into());
        }
        if previous
            .as_ref()
            .is_some_and(|old| old.prerequisite_context != job.prerequisite_context)
        {
            return self.corrupted("invalid change of Job prerequisite context".into());
        }
        self.record_job_phase(job.id, previous.as_ref().map(|row| row.phase), job.phase)?;
        self.record_mutable_input_revision(
            job.id,
            previous.as_ref().map(|row| row.revision),
            job.revision,
        )?;
        if previous.as_ref().and_then(|row| row.retry.as_ref()) != job.retry.as_ref() {
            if previous.as_ref().is_some_and(|row| {
                row.retry.is_some()
                    || row.phase != crate::db::entities::MutableJobPhase::Unsubmitted
            }) || job.phase != crate::db::entities::MutableJobPhase::Unsubmitted
            {
                return self
                    .corrupted("retry identity cannot be rewritten after submission".into());
            }
            if let Some(retry) = &job.retry {
                let original = self.mutable_job(retry.original)?.ok_or_else(|| {
                    BatchError(Failure::Storage(StorageError::corrupted(
                        "retry source missing",
                    )))
                })?;
                validate_retry_source(job, &original)
                    .map_err(|error| BatchError(Failure::Storage(error)))?;
                self.insert(RETRY_JOBS, (retry.original.get(), job.id.get()), ())?;
            }
        }
        self.insert(JOBS, job.id.get(), encode_mutable_job(job).as_slice())
    }

    /// Captures the prerequisite results onto a conversation's original
    /// assignment, the only row that owns them. This is the single runtime
    /// write of the field: it reads the authoritative assignment, refuses once
    /// any Generative turn has started without the context, never replaces an
    /// existing context, and changes no other field. Callers must commit it in
    /// the same batch as the first eligible reservation.
    pub(super) fn capture_prerequisite_context(
        &mut self,
        conversation: JobId,
    ) -> Result<PrerequisiteCapture, BatchError> {
        let spec = self.require_mutable_conversation(conversation)?;
        let Some(mut assignment) =
            self.step(|tx| super::job_tree::prerequisite_assignment(tx, &spec))?
        else {
            return Ok(PrerequisiteCapture::NotRequired);
        };
        if assignment.prerequisite_context.is_some() {
            return Ok(PrerequisiteCapture::NotRequired);
        }
        if self
            .step(|tx| super::job_tree::prerequisite_admissions(tx, &assignment))?
            .first_started
            .is_some()
        {
            return self.reject(Violation::InvalidValue(
                "prerequisite_context",
                super::job_tree::PREREQUISITE_CONTEXT_LOST,
            ));
        }
        let lifecycle = self.job_lifecycle(conversation)?;
        let Some(context) =
            self.step(|tx| super::job_tree::prerequisite_results(tx, &spec, &lifecycle.after))?
        else {
            return Ok(PrerequisiteCapture::Pending);
        };
        assignment.prerequisite_context = Some(context.clone());
        self.insert(
            JOBS,
            assignment.id.get(),
            encode_mutable_job(&assignment).as_slice(),
        )?;
        Ok(PrerequisiteCapture::Captured {
            assignment: assignment.id,
            context,
        })
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

    pub(super) fn frozen_configuration(
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

    pub(super) fn storable_frozen(
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

    pub(super) fn mutable_execution(
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
            if matches!(
                &frozen,
                FrozenDecisionConfig::Generative(generative, _)
                    if generative.llm.provider.transport.execution_refusal().is_some()
            ) {
                return self.reject(Violation::InvalidValue(
                    "decision",
                    "the configured transport does not support generative Decision execution",
                ));
            }
            if frozen.references().collect::<Vec<_>>() != *references {
                return self.reject(Violation::InvalidValue(
                    "decision",
                    "Decision configuration changed",
                ));
            }
            return Ok(FrozenConfiguration::Decision(frozen));
        }
        self.inspect(|tx| generative_execution(tx, input))
            .map(FrozenConfiguration::Generative)
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
        self.step(|tx| mutable_job_in(tx, job))
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
        Ok(Some(crate::db::entities::MutableJobRecord {
            job,
            outcome,
            project: parent.project,
            event_seq: self.step(|tx| execution::latest_seq(tx, id))?,
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

    #[cfg(test)]
    pub(crate) fn create_unsubmitted_job(
        &mut self,
        context: crate::db::entities::ComposerContextId,
        input: crate::db::entities::MutableJobInput,
        submission: crate::db::entities::Submission,
        conversation: Option<JobId>,
        project: Option<ProjectId>,
    ) -> Result<JobId, BatchError> {
        self.create_job_as(
            context,
            input,
            submission,
            conversation,
            project,
            Actor::User,
        )
    }

    pub(crate) fn create_job_as(
        &mut self,
        context: crate::db::entities::ComposerContextId,
        input: crate::db::entities::MutableJobInput,
        submission: crate::db::entities::Submission,
        conversation: Option<JobId>,
        project: Option<ProjectId>,
        by: Actor,
    ) -> Result<JobId, BatchError> {
        self.create_job_with_child_data(
            context,
            input,
            submission,
            conversation,
            project,
            by,
            Vec::new(),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_job_with_child_data(
        &mut self,
        context: crate::db::entities::ComposerContextId,
        mut input: crate::db::entities::MutableJobInput,
        submission: crate::db::entities::Submission,
        conversation: Option<JobId>,
        project: Option<ProjectId>,
        by: Actor,
        child_results: Vec<crate::db::entities::ChildTurnResult>,
        child_notice: Option<crate::db::entities::ToolCallRef>,
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
                if parent.project != Some(project) {
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
        self.inspect(|tx| validate_mutable_input(tx, &input))?;
        let parent_id = match conversation {
            Some(parent) => {
                let row = self.require_mutable_conversation(parent)?;
                if let Some(project) = project {
                    self.require_project(project)?;
                    if row.project != Some(project) {
                        return self.reject(Violation::InvalidValue(
                            "project",
                            "conversation belongs to another project",
                        ));
                    }
                }
                let runtime = self.step(|tx| state::require(tx, row.id))?;
                require_open_turn_parent(&row, &runtime)?;
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
            submitted_by: by,
            retry: None,
            composer_context: context,
            created_at: self.now,
            submitted_at: None,
            revision: 1,
            phase: MutableJobPhase::Unsubmitted,
            input,
            context_selection: crate::db::entities::MutableContextSelection::FullHistory,
            summary_source: None,
            frozen: None,
            child_results,
            child_notice,
            prerequisite_context: None,
        };
        self.store_mutable_job(&job)?;
        self.put_job_state(id, &JobState::new(id))?;
        if job.submitted_by == Actor::User {
            let mut lifecycle = self.job_lifecycle(parent_id)?;
            if lifecycle.loop_count != 0 {
                lifecycle.loop_count = 0;
                self.put_job_lifecycle(parent_id, &lifecycle)?;
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
        if job.retry.is_some()
            && !matches!(
                input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            )
        {
            return self.reject(Violation::InvalidValue(
                "input",
                "a retry must remain a Generative Job",
            ));
        }
        self.inspect(|tx| validate_mutable_input(tx, &input))?;
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
            let agent = parent.agent;
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
                if keys.len() > 1 {
                    return Err(StorageError::corrupted(
                        "queued Job has duplicate ready entries",
                    ));
                }
                if let Some(key) = keys.first() {
                    ready.remove(*key)?;
                    ready.insert(new_key, ())?;
                } else if state::pending(tx, id)?.is_some_and(|counts| counts.0 > 0) {
                    // Waiting on dependencies; it will enter MUTABLE_READY when dependencies settle.
                } else {
                    return Err(StorageError::corrupted(
                        "queued Job has invalid ready entries",
                    ));
                }
                Ok(())
            })?;
        }
        job.input = input;
        job.context_selection = next_context;
        job.revision = next;
        self.store_mutable_job(&job)?;
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
        self.put_job_state(id, &state)?;
        self.release_draft_context(&job)?;
        self.settle_native_dependents(id, OutcomeKind::Cancelled)?;
        self.flush_child_notices(job.parent_id)?;
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

    pub(crate) fn cancel_queued_job(
        &mut self,
        id: JobId,
        expected_revision: u64,
        unsupported_only: bool,
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
        {
            return self.reject(Violation::InvalidValue(
                "job",
                "Job cannot be cancelled by this operation",
            ));
        }
        if !unsupported_only {
            return self.end_queued_native_job(
                &job,
                OutcomeCause::Text("withdrawn by user before execution".to_owned()),
            );
        }
        if !matches!(job.input.content, MutableJobContent::Generative { .. })
            || !self.inspect(|tx| {
                has_decision_predecessors(tx, job.parent_id, job.previous, Some(job.id))
            })?
        {
            return self.reject(Violation::InvalidValue(
                "job",
                "Job does not have unsupported Decision history",
            ));
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
        if state.outcome.is_some() {
            return Ok(());
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
            if !matches!(state::pending(tx, id)?, Some((_, 0))) {
                return Err(StorageError::corrupted(
                    "queued Job has unsettled dependents",
                ));
            }
            state::set_pending(tx, id, None)?;
            Ok(())
        })?;
        self.adjust_open_children(job.parent_id, -1)?;
        state.outcome = Some(OutcomeRecord {
            kind: OutcomeKind::Cancelled,
            at: self.now,
            cause: Some(cause),
        });
        self.put_job_state(id, &state)?;
        self.probe = Some(Probe::Outcome(id));
        self.settle_native_dependents(id, OutcomeKind::Cancelled)?;
        self.settle_summary_membership(job)?;
        self.settle_standalone_decision(job)?;
        Ok(())
    }

    pub(super) fn enqueue_mutable_job(
        &mut self,
        job: &crate::db::entities::MutableJob,
        req_tag: u16,
        membership: TurnMembership,
        planning_wait: bool,
        create_state: bool,
    ) -> Result<(), BatchError> {
        let submitted_micros = job.submitted_at.unwrap_or(self.now).as_unix_micros();
        self.store_mutable_job(job)?;
        if create_state {
            self.put_job_state(job.id, &JobState::new(job.id))?;
        }
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
            if matches!(membership, TurnMembership::Turn) {
                tx.open_table(TURNS)?
                    .insert((job.parent_id.get(), job.id.get()), ())?;
            }
            tx.open_table(CHILDREN)?
                .insert((job.parent_id.get(), job.id.get()), ())?;
            state::set_pending(tx, job.id, Some((u32::from(planning_wait), 0)))?;
            if let Some(previous) = job.previous {
                tx.open_table(SUCCESSORS)?.insert(previous.get(), ())?;
            }
            Ok(())
        })?;
        self.adjust_open_children(job.parent_id, 1)?;
        Ok(())
    }
}

impl Batch {
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
        let lifecycle = self.job_lifecycle(job.parent_id)?;
        if parent.is_chunk_group()
            || self.require_job_state(job.parent_id)?.outcome.is_some()
            || lifecycle.closing.is_some()
            || lifecycle.hidden_at.is_some()
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
        if job.retry.is_none()
            && let Some(previous) = job.previous
            && self
                .step(|tx| super::job_tree::routed_step_recovery(tx, &parent, previous))?
                .is_some()
        {
            return self.reject(Violation::InvalidValue(
                "step_recovery_pending",
                "resolve this step's failure before sending another turn",
            ));
        }
        self.inspect(|tx| validate_mutable_input(tx, &job.input))?;
        if matches!(
            &job.input.content,
            crate::db::entities::MutableJobContent::Generative { .. }
        ) {
            self.require_generative_predecessors(&job)?;
        }
        let frozen = self.mutable_execution(&job.input)?;
        job.frozen = Some(self.storable_frozen(&frozen)?);
        match &frozen {
            FrozenConfiguration::Generative(frozen) => {
                let selected = frozen.agent_ref.map(|reference| reference.id);
                if job.previous.is_none() && parent.agent.is_none() {
                    parent.agent = selected;
                    self.insert(CONVERSATIONS, parent.id.get(), parent.encode().as_slice())?;
                } else {
                    require_conversation_agent(&parent, selected)?;
                }
            }
            FrozenConfiguration::Decision(frozen) => {
                if parent.agent.is_some() {
                    require_conversation_agent(
                        &parent,
                        frozen.agent_ref().map(|reference| reference.id),
                    )?;
                }
            }
        }
        let submitted = self.now;
        job.phase = MutableJobPhase::Queued;
        job.submitted_at = Some(submitted);
        let planning_wait = self.maybe_attach_planning_decision(&parent, &job, &frozen)?;
        let req_tag = mutable_configuration_requirement(&frozen);
        self.enqueue_mutable_job(&job, req_tag, TurnMembership::Turn, planning_wait, false)?;
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
        name: Option<String>,
        configuration: ExecutionConfiguration,
        request: crate::db::entities::DecisionRequest,
        submission: Option<crate::db::entities::Submission>,
        has_capability: impl FnOnce(ExecutionOperation, Option<LlmTransport>) -> bool,
    ) -> Result<(JobId, Appended), BatchError> {
        use crate::db::entities::{
            AgentCapability, ComposerContextId, JobQueuePriority, MutableContextSelection,
            MutableJob, MutableJobContent, MutableJobInput, MutableJobPhase,
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
        let frozen =
            match self.freeze_configuration(configuration)? {
                FrozenConfiguration::Decision(frozen) => frozen,
                FrozenConfiguration::Generative(frozen) => {
                    if frozen.agent.as_ref().is_some_and(|agent| {
                        !agent.capabilities.contains(&AgentCapability::Decision)
                    }) {
                        return self.reject(Violation::InvalidValue(
                            "capabilities",
                            "the Agent must support decision work with this model",
                        ));
                    }
                    FrozenDecisionConfig::Generative(frozen, None)
                }
            };
        if let Err(reason) = frozen.validate() {
            return self.reject(Violation::InvalidValue("decision", reason));
        }
        if matches!(
            &frozen,
            FrozenDecisionConfig::Generative(generative, _)
                if generative.llm.provider.transport.execution_refusal().is_some()
        ) {
            return self.reject(Violation::InvalidValue(
                "decision",
                "the configured transport does not support generative Decision execution",
            ));
        }
        if let Err(reason) = request.validate(&frozen.capabilities()) {
            return self.reject(Violation::InvalidValue("decision", reason));
        }
        let (operation, protocol) = match &frozen {
            FrozenDecisionConfig::Native(_) => (ExecutionOperation::Decision, None),
            FrozenDecisionConfig::Generative(generative, _) => (
                ExecutionOperation::LlmRequest,
                Some(generative.llm.provider.transport),
            ),
        };
        if !has_capability(operation, protocol)
            && !self.has_native_capability(operation, protocol)?
        {
            return self.reject(Violation::InvalidValue(
                "capability_unsatisfiable",
                "no Pool Executor supports this Decision capability",
            ));
        }
        let storable = self.storable_frozen(&FrozenConfiguration::Decision(frozen.clone()))?;
        let conversation_id = JobId::new(self.next_id()?);
        let conversation_row = JobSpec {
            id: conversation_id,
            created_at: self.now,
            parent_id: None,
            agent: configuration.agent_profile.map(Into::into),
            project: None,
            origin: Origin::System,
            keep_open: false,
        };
        self.insert(
            CONVERSATIONS,
            conversation_id.get(),
            conversation_row.encode().as_slice(),
        )?;
        self.put_job_state(conversation_id, &JobState::new(conversation_id))?;
        self.put_job_lifecycle(
            conversation_id,
            &JobLifecycle {
                title: name,
                ..JobLifecycle::default()
            },
        )?;
        self.step(|tx| state::set_pending(tx, conversation_id, Some((0, 0))))?;
        self.insert(CHILDREN, (0, conversation_id.get()), ())?;

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
            submitted_by: Actor::User,
            previous: None,
            retry: None,
            composer_context: ComposerContextId::new(self.next_id()?),
            created_at: submitted,
            submitted_at: Some(submitted),
            revision: 1,
            phase: MutableJobPhase::Queued,
            input: input.clone(),
            context_selection: MutableContextSelection::FullHistory,
            summary_source: None,
            frozen: Some(storable),
            child_results: Vec::new(),
            child_notice: None,
            prerequisite_context: None,
        };
        let req_tag = mutable_configuration_requirement(&FrozenConfiguration::Decision(frozen));
        self.enqueue_mutable_job(&job, req_tag, TurnMembership::Turn, false, true)?;
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
}

impl Batch {
    pub(crate) fn job(&mut self, id: JobId) -> Result<Option<JobSpec>, BatchError> {
        self.get(CONVERSATIONS, id.get(), |bytes| {
            JobSpec::decode_at(id.get(), bytes)
        })
    }

    pub(crate) fn turn_conversation(&mut self, id: JobId) -> Result<JobId, BatchError> {
        match self.mutable_job(id)? {
            Some(job) => Ok(job.parent_id),
            None => self.reject(Violation::InvalidValue("job", "the Job does not exist")),
        }
    }

    pub(crate) fn require_job(&mut self, id: JobId) -> Result<JobSpec, BatchError> {
        match self.job(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    pub(crate) fn job_state(&mut self, id: JobId) -> Result<Option<JobState>, BatchError> {
        self.step(|tx| Ok(state::read(tx, id)?.map(|row| row.state)))
    }

    pub(crate) fn require_job_state(&mut self, id: JobId) -> Result<JobState, BatchError> {
        match self.job_state(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    pub(crate) fn put_job_state(&mut self, job: JobId, state: &JobState) -> Result<(), BatchError> {
        self.catalog_changed = true;
        if state.job_id != job {
            return self.corrupted("Job state has a different identity".into());
        }
        let previous = self.step(|tx| state::read(tx, job))?;
        if state.outcome.is_some()
            && previous
                .as_ref()
                .is_some_and(|row| row.state.outcome.is_none())
        {
            self.expire_child_progress(job)?;
        }
        self.record_job_state(previous.as_ref().map(|row| &row.state), state)?;
        let mut row = previous.unwrap_or_else(|| JobRuntime::new(job));
        row.state = state.clone();
        self.step(|tx| state::write(tx, &row))
    }
}

impl Batch {
    pub(crate) fn require_mutable_conversation(
        &mut self,
        id: JobId,
    ) -> Result<JobSpec, BatchError> {
        self.require_job(id)
    }

    pub(crate) fn create_mutable_conversation(
        &mut self,
        project: Option<ProjectId>,
    ) -> Result<JobId, BatchError> {
        if let Some(project) = project {
            self.require_project(project)?;
        }
        let id = JobId::new(self.next_id()?);
        let row = JobSpec {
            id,
            created_at: self.now,
            parent_id: None,
            agent: None,
            project,
            origin: Origin::User,
            keep_open: true,
        };
        self.insert(CONVERSATIONS, id.get(), row.encode().as_slice())?;
        self.put_job_state(id, &JobState::new(id))?;
        self.put_job_lifecycle(id, &JobLifecycle::default())?;
        self.step(|tx| state::set_pending(tx, id, Some((0, 0))))?;
        self.insert(CHILDREN, (0, id.get()), ())?;
        Ok(id)
    }

    pub(crate) fn job_lifecycle(&mut self, root: JobId) -> Result<JobLifecycle, BatchError> {
        self.step(|tx| Ok(state::require(tx, root)?.lifecycle))
    }

    pub(crate) fn put_job_lifecycle(
        &mut self,
        root: JobId,
        row: &JobLifecycle,
    ) -> Result<(), BatchError> {
        self.catalog_changed = true;
        let mut runtime = self.step(|tx| state::require(tx, root))?;
        let previous = &runtime.lifecycle;
        if previous.closing != row.closing {
            match (&previous.closing, &row.closing) {
                (None, Some(closing)) => {
                    self.record_execution_event(
                        root,
                        ExecutionEventKind::Closing {
                            at: closing.at,
                            by: closing.by.clone(),
                            running: closing.running,
                        },
                    )?;
                }
                (Some(old), Some(closing)) if old.at == closing.at && old.by == closing.by => {
                    self.record_execution_event(
                        root,
                        ExecutionEventKind::ClosingRunningChanged {
                            running: closing.running,
                        },
                    )?;
                }
                (Some(_), None | Some(_)) | (None, None) => {
                    return self
                        .corrupted("Job closing intent cannot be removed or rewritten".into());
                }
            }
        }
        if previous.on_hold != row.on_hold {
            self.record_execution_event(
                root,
                ExecutionEventKind::HoldChanged {
                    reason: row.on_hold,
                },
            )?;
        }
        if previous.hidden_at != row.hidden_at {
            self.record_execution_event(
                root,
                ExecutionEventKind::VisibilityChanged { at: row.hidden_at },
            )?;
        }
        if previous.after != row.after {
            self.record_execution_event(
                root,
                ExecutionEventKind::PrerequisitesChanged {
                    after: row.after.clone(),
                },
            )?;
        }
        let previous = std::mem::replace(&mut runtime.lifecycle, row.clone());
        self.step(|tx| {
            if previous.compaction_policy != row.compaction_policy {
                super::indexes::update_compaction_refs(
                    tx,
                    root,
                    previous.compaction_policy.as_ref(),
                    row.compaction_policy.as_ref(),
                )?;
            }
            state::write(tx, &runtime)
        })?;
        if (previous.closing.is_none() && row.closing.is_some())
            || (previous.hidden_at.is_none() && row.hidden_at.is_some())
        {
            self.drop_notices_to_closed_parent(root)?;
        }
        if (row.closing.is_some() || row.hidden_at.is_some())
            && let Some(mut rec) = self.get(RECURRENCES, root.get(), Recurrence::decode)?
            && !matches!(rec.phase, RecurrencePhase::Halted { .. })
        {
            if let RecurrencePhase::WaitingDue { due } = rec.phase {
                self.step(|tx| {
                    tx.open_table(RECURRENCE_DUE)?.remove((due, root.get()))?;
                    Ok(())
                })?;
            }
            rec.phase = RecurrencePhase::Halted {
                reason: RecurrenceHalt::Closed,
            };
            self.insert(RECURRENCES, root.get(), rec.encode().as_slice())?;
        }
        Ok(())
    }
}

impl Batch {
    pub(crate) fn link_job_group(&mut self, parent: JobId, child: JobId) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(CHILD_GROUPS)?
                .insert((parent.get(), child.get()), ())?;
            tx.open_table(LIVE_CHILDREN)?
                .insert((parent.get(), child.get()), ())?;
            Ok(())
        })
    }
}

impl Batch {
    pub(super) fn adjust_open_children(
        &mut self,
        job: JobId,
        delta: i64,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            let counts = state::pending(tx, job)?;
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
            state::set_pending(tx, job, Some((dependencies, children)))?;
            Ok(())
        })
    }
}

pub(super) fn decision_predecessors(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    parent: JobId,
    mut previous: Option<JobId>,
    current: Option<JobId>,
) -> Result<Result<bool, &'static str>, StorageError> {
    use crate::db::entities::MutableJobContent;

    const MAX_PREDECESSORS: usize = 4096;
    let mut seen = BTreeSet::new();
    while let Some(id) = previous {
        if seen.len() == MAX_PREDECESSORS {
            return Ok(Err(
                "conversation history exceeds the supported admission limit",
            ));
        }
        if Some(id) == current || !seen.insert(id) {
            return Err(StorageError::corrupted("conversation predecessor cycle"));
        }
        let row = jobs
            .get(id.get())?
            .ok_or_else(|| StorageError::corrupted("conversation predecessor missing"))?;
        let prior = decode_mutable_job(id.get(), row.value())?;
        if prior.parent_id != parent || prior.submitted_at.is_none() {
            return Err(StorageError::corrupted("invalid conversation predecessor"));
        }
        if matches!(prior.input.content, MutableJobContent::Decision { .. }) {
            return Ok(Ok(true));
        }
        previous = prior.previous;
    }
    Ok(Ok(false))
}

pub(super) fn validate_mutable_input(
    tx: &impl TxRead,
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
            return Err(Violation::InvalidValue("decision", reason).into());
        }
        return Ok(());
    }
    if input.text().len() > MAX_INPUT_BYTES
        || input.text().chars().any(invalid_input_character)
        || input.attachments().len() > 8
    {
        return Err(Violation::InvalidValue("input", "invalid Job input").into());
    }
    let mut seen = BTreeSet::new();
    for attachment in input.attachments() {
        if !seen.insert(*attachment) {
            return Err(Violation::InvalidValue("attachments", "duplicate attachment").into());
        }
        let record = reads::catalog::attachment(tx, *attachment)?
            .ok_or(Violation::NotFound(RecordRef::Attachment(*attachment)))?;
        if !record.is_available() {
            return Err(Violation::AttachmentCleanedUp(*attachment).into());
        }
    }
    Ok(())
}

pub(super) fn generative_execution(
    tx: &impl TxRead,
    input: &crate::db::entities::MutableJobInput,
) -> Result<crate::db::entities::FrozenExecutionConfig, BatchError> {
    if input.text().trim().is_empty() && input.attachments().is_empty() {
        return Err(Violation::InvalidValue("input", "Job input is empty").into());
    }
    let Some(configuration) = input.configuration() else {
        return Err(
            Violation::InvalidValue("configuration", "Job configuration is required").into(),
        );
    };
    match super::configs::freeze_configuration(tx, configuration)? {
        FrozenConfiguration::Generative(frozen) => {
            if let Some(reason) = frozen.native_execution_refusal() {
                return Err(Violation::InvalidValue("configuration", reason).into());
            }
            Ok(frozen)
        }
        FrozenConfiguration::Decision(_) => Err(Violation::InvalidValue(
            "configuration",
            "typed decision work is not supported here",
        )
        .into()),
    }
}

pub(super) fn require_conversation_agent(
    parent: &JobSpec,
    selected: Option<ConfigId>,
) -> Result<(), Violation> {
    if parent.agent != selected {
        return Err(Violation::InvalidValue(
            "conversation",
            "Agent differs from the conversation",
        ));
    }
    Ok(())
}

pub(super) fn require_open_turn_parent(
    row: &JobSpec,
    runtime: &JobRuntime,
) -> Result<(), Violation> {
    if row.is_chunk_group()
        || row.is_standalone_decision_group()
        || runtime.state.outcome.is_some()
        || runtime.lifecycle.closing.is_some()
        || runtime.lifecycle.hidden_at.is_some()
    {
        return Err(Violation::InvalidValue(
            "conversation",
            "conversation is not open",
        ));
    }
    Ok(())
}

pub(super) fn require_generative_history(
    tx: &impl TxRead,
    parent: JobId,
    previous: Option<JobId>,
    current: Option<JobId>,
) -> Result<(), BatchError> {
    if let Some(spec) = reads::jobs::read_conversation_row(tx, parent)?
        && super::job_tree::prerequisite_context_lost(tx, &spec)?
    {
        return Err(Violation::InvalidValue(
            "prerequisite_context",
            super::job_tree::PREREQUISITE_CONTEXT_LOST,
        )
        .into());
    }
    if has_decision_predecessors(tx, parent, previous, current)? {
        return Err(Violation::InvalidValue(
            "execution_context",
            "Generative Jobs cannot follow Decision history",
        )
        .into());
    }
    Ok(())
}

fn has_decision_predecessors(
    tx: &impl TxRead,
    parent: JobId,
    previous: Option<JobId>,
    current: Option<JobId>,
) -> Result<bool, BatchError> {
    let jobs = tx.table(JOBS).map_err(StorageError::from)?;
    decision_predecessors(&jobs, parent, previous, current)?
        .map_err(|reason| Violation::InvalidValue("execution_context", reason).into())
}

pub(super) fn mutable_job_in(
    tx: &impl TxRead,
    id: JobId,
) -> Result<Option<crate::db::entities::MutableJob>, StorageError> {
    tx.table(JOBS)?
        .get(id.get())?
        .map(|row| decode_mutable_job(id.get(), row.value()))
        .transpose()
}
