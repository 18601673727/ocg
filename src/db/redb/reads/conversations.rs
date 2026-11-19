use std::ops::Bound;

use super::catalog::{config_record, effective_context_policy, global_compaction_settings};
use super::jobs::tool_evidence;
use crate::db::entities::JobId;
use crate::db::entities::{
    Actor, ChildConversationEntry, ContextState, ContextWorkDetails, ContextWorkState,
    ConversationEntry, ConversationsPage, JobLifecycle, JobLimits, JobSpec,
    MutableConversationPage, MutableConversationTurn, Origin, OutputKind, Recurrence,
};
use crate::db::entities::{
    ConfigId, ConfigRecord, ExecutionOperation, MutableJobPhase, OutcomeKind,
};
use crate::db::redb::allocations::pools_offering_capability;
use crate::db::redb::batch::{BatchError, Failure};
use crate::db::redb::configs::ConfigurationResolver;
use crate::db::redb::reads::execution_context::{SummarySourceError, verify_native_summary_in};
use crate::db::redb::reads::jobs::{
    active_job_in, first_entry_text, mutable_job_details_in, mutable_job_record_in,
    project_planning_target, read_conversation_row, read_job_lifecycle, read_state, turn_first,
    turn_head, unstarted_settlement_in,
};
use crate::db::redb::reads::read;
use crate::db::redb::rows::{ConfigRow, decode_output, decode_project};
use crate::db::redb::schema::MAX_PAGE;
use crate::db::redb::schema::{
    ATTACHMENTS, CHILD_GROUPS, CHILDREN, CONFIGS, CONVERSATION_SEARCH, CONVERSATIONS, JOB_OUTPUTS,
    JOBS, MESSAGES, META, OPEN_APPROVALS, POOL_ALLOCATIONS, PROJECTS, RECURRENCES, TURNS,
};
use crate::db::redb::{StorageError, TxRead};
use crate::db::redb::{allocations, indexes, rows, state};
use ::redb::{Database, ReadTransaction};
use std::collections::BTreeSet;

pub(crate) fn read_mutable_conversation_turn_page(
    database: &Database,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Option<MutableConversationPage>, StorageError> {
    let rx = read(database)?;
    if read_conversation_row(&rx, conversation)?.is_none() {
        return Ok(None);
    }
    if let Some(before) = before {
        if rx
            .open_table(TURNS)?
            .get((conversation.get(), before.get()))?
            .is_none()
        {
            return Ok(Some(MutableConversationPage {
                turns: Vec::new(),
                has_more: false,
            }));
        }
        let valid = mutable_job_record_in(&rx, before)?.is_some_and(|row| {
            row.job.parent_id == conversation
                && row.job.phase != crate::db::entities::MutableJobPhase::Unsubmitted
        });
        if !valid {
            return Ok(Some(MutableConversationPage {
                turns: Vec::new(),
                has_more: false,
            }));
        }
    }
    let bounds = (
        Bound::Included((conversation.get(), 0)),
        before.map_or(Bound::Included((conversation.get(), u128::MAX)), |id| {
            Bound::Excluded((conversation.get(), id.get()))
        }),
    );
    let turns = rx.open_table(TURNS)?;
    let mut rows = Vec::new();
    for entry in turns
        .range(bounds)?
        .rev()
        .take(limit.saturating_add(1).min(MAX_PAGE + 1))
    {
        let (key, _) = entry?;
        let details = mutable_job_details_in(&rx, JobId::new(key.value().1), None, false)?
            .ok_or_else(|| StorageError::corrupted("turn projection has no source Job"))?;
        if details.record.job.parent_id != conversation
            || details.record.job.phase == crate::db::entities::MutableJobPhase::Unsubmitted
        {
            return Err(StorageError::corrupted(
                "turn projection disagrees with source Job",
            ));
        }
        let id = details.record.job.id;
        let mut outputs = Vec::new();
        for entry in rx
            .open_table(JOB_OUTPUTS)?
            .range((id.get(), 0)..=(id.get(), u32::MAX))?
            .take(TURN_PAGE_OUTPUTS)
        {
            let (key, bytes) = entry?;
            outputs.push(decode_output(key.value(), bytes.value())?);
        }
        let outputs_truncated = outputs.len() == TURN_PAGE_OUTPUTS
            && outputs
                .last()
                .is_some_and(|output| output.kind != OutputKind::Result);
        if outputs_truncated && let Some(result) = &details.result {
            outputs.push(result.clone());
        }
        let evidence = if details
            .record
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            tool_evidence(&rx, id, None, TURN_PAGE_OUTPUTS)?
        } else {
            Vec::new()
        };
        rows.push(MutableConversationTurn {
            planning_target: project_planning_target(&rx, &details.record.job)?,
            details,
            outputs,
            outputs_truncated,
            evidence,
        });
    }
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    Ok(Some(MutableConversationPage {
        turns: rows,
        has_more,
    }))
}

fn conversation_job_state(
    rx: &ReadTransaction,
    runtime: &state::JobRuntime,
    tip: Option<JobId>,
) -> Result<&'static str, StorageError> {
    if let Some(outcome) = &runtime.state.outcome {
        return Ok(outcome_label(outcome.kind));
    }
    let conversation = runtime.state.job_id;
    for entry in rx.open_table(OPEN_APPROVALS)?.range(
        (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
    )? {
        let (_, state) = entry?;
        if state.value() == 0 {
            return Ok("waiting_for_user");
        }
    }
    let active = active_job_in(rx, conversation)?;
    let turn = active
        .as_ref()
        .map(|job| job.id)
        .or(tip)
        .map(|id| state::require(rx, id))
        .transpose()?;
    let runtime = turn.as_ref().unwrap_or(runtime);
    Ok(match &runtime.state.outcome {
        Some(outcome) => outcome_label(outcome.kind),
        None if active.is_some_and(|job| job.phase == MutableJobPhase::Executing) => "running",
        None if runtime
            .pending
            .is_some_and(|(dependencies, _)| dependencies > 0) =>
        {
            "waiting"
        }
        None => "queued",
    })
}

fn outcome_label(kind: OutcomeKind) -> &'static str {
    match kind {
        OutcomeKind::Completed => "completed",
        OutcomeKind::Failed => "failed",
        OutcomeKind::Cancelled => "cancelled",
        OutcomeKind::Unknown => "unknown",
    }
}

fn has_visible_child(rx: &ReadTransaction, conversation: JobId) -> Result<bool, StorageError> {
    for entry in rx
        .open_table(CHILD_GROUPS)?
        .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
    {
        let child = JobId::new(entry?.0.value().1);
        if let Some(row) = read_conversation_row(rx, child)?
            && matches!(row.origin, Origin::User | Origin::Agent { .. })
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn conversation_title(
    rx: &ReadTransaction,
    conversation: JobId,
    metadata: &JobLifecycle,
) -> Result<String, StorageError> {
    Ok(match &metadata.title {
        Some(title) => title.clone(),
        None => match turn_first(&rx.open_table(TURNS)?, conversation)? {
            Some(first) => first_entry_text(rx, first)?,
            None => String::new(),
        },
    })
}

pub(crate) fn conversation_summary(
    rx: &ReadTransaction,
    row: &JobSpec,
    tip: Option<JobId>,
) -> Result<ConversationEntry, StorageError> {
    let conversation = row.id;
    let runtime = state::require(rx, conversation)?;
    let metadata = &runtime.lifecycle;
    let title = conversation_title(rx, conversation, metadata)?;
    let on_hold = metadata.on_hold.map(|code| {
        if code == super::job_tree::HELD_BY_AGENT {
            Actor::Agent {
                conversation: row.parent_id.unwrap_or(conversation),
            }
        } else {
            Actor::User
        }
    });
    let outcome = runtime.state.outcome.as_ref().map(|outcome| outcome.kind);
    let (tip_outcome, tip_created_at) = match tip {
        Some(tip) => {
            let record = mutable_job_record_in(rx, tip)?
                .ok_or_else(|| StorageError::corrupted("missing tip"))?;
            (
                record.outcome.map(|outcome| outcome.kind),
                record.job.created_at,
            )
        }
        None => (outcome, row.created_at),
    };
    Ok(ConversationEntry {
        root_job: conversation,
        tip_job: tip.unwrap_or(conversation),
        agent: row.agent,
        project: row.project,
        title,
        state: if outcome.is_some() {
            "settled"
        } else if metadata.closing.is_some() {
            "closing"
        } else {
            "open"
        }
        .to_owned(),
        on_hold,
        tip_settlement_state: tip_outcome.map_or("unsettled", outcome_label).to_owned(),
        tip_created_at,
        job_state: conversation_job_state(rx, &runtime, tip)?.to_owned(),
        has_children: has_visible_child(rx, conversation)?,
    })
}

pub(crate) fn conversation_tip(
    rx: &ReadTransaction,
    row: &JobSpec,
) -> Result<Option<JobId>, StorageError> {
    if row.is_chunk_group() {
        return Ok(None);
    }
    turn_head(rx, row.id)?
        .map(Some)
        .ok_or_else(|| StorageError::corrupted(format!("{} has no tip", row.id)))
}

pub(crate) fn read_conversation_children(
    database: &Database,
    parent: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Option<Vec<ChildConversationEntry>>, StorageError> {
    let rx = read(database)?;
    if read_conversation_row(&rx, parent)?.is_none() {
        return Ok(None);
    }
    state::require(&rx, parent)?;
    let mut children = Vec::new();
    for entry in rx
        .open_table(CHILD_GROUPS)?
        .range((parent.get(), 0)..(parent.get(), before.map_or(u128::MAX, JobId::get)))?
        .rev()
        .take(limit.min(MAX_PAGE))
    {
        let child = JobId::new(entry?.0.value().1);
        let row = read_conversation_row(&rx, child)?
            .ok_or_else(|| StorageError::corrupted("child conversation missing"))?;
        let conversation = conversation_summary(&rx, &row, conversation_tip(&rx, &row)?)?;
        children.push(ChildConversationEntry {
            conversation,
            origin: row.origin,
            keep_open: row.keep_open,
            recurrence: rx
                .open_table(RECURRENCES)?
                .get(child.get())?
                .map(|bytes| Recurrence::decode(bytes.value()))
                .transpose()?,
        });
    }
    Ok(Some(children))
}

pub(crate) fn read_conversations(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
) -> Result<ConversationsPage, StorageError> {
    conversation_page(database, before, limit, &[])
}

pub(crate) fn read_conversation_search(
    database: &Database,
    terms: &[String],
    before: Option<JobId>,
    limit: usize,
) -> Result<ConversationsPage, StorageError> {
    conversation_page(database, before, limit, terms)
}

fn conversation_page(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
    terms: &[String],
) -> Result<ConversationsPage, StorageError> {
    let rx = read(database)?;
    let groups = rx.open_table(CONVERSATIONS)?;
    let limit = limit.clamp(1, 100);
    let mut conversations = Vec::new();
    let mut last = None;
    let mut last_examined = None;
    let mut has_more = false;
    for (examined, item) in groups
        .range(..before.map_or(u128::MAX, JobId::get))?
        .rev()
        .enumerate()
    {
        if !terms.is_empty() && examined == 5_000 {
            has_more = true;
            last = last_examined;
            break;
        }
        let (key, bytes) = item?;
        let conversation = JobId::new(key.value());
        last_examined = Some(conversation);
        let row = JobSpec::decode_at(key.value(), bytes.value())?;
        if !indexes::searchable_conversation(&row) {
            continue;
        }
        let metadata = read_job_lifecycle(&rx, conversation)?;
        if metadata.hidden_at.is_some() {
            continue;
        }
        let Some(tip) = turn_head(&rx, conversation)? else {
            continue;
        };
        let matches = if terms.is_empty() {
            true
        } else {
            let title = conversation_title(&rx, conversation, &metadata)?;
            conversation_matches(&rx, conversation, &title, terms)?
        };
        if matches {
            if conversations.len() == limit {
                has_more = true;
                break;
            }
            last = Some(conversation);
            conversations.push(conversation_summary(&rx, &row, Some(tip))?);
            if !terms.is_empty() && conversations.len() == limit {
                has_more = true;
                break;
            }
        }
    }
    Ok(ConversationsPage {
        conversations,
        next_before: last.filter(|_| has_more),
        has_more,
    })
}

fn conversation_matches(
    rx: &ReadTransaction,
    conversation: JobId,
    title: &str,
    terms: &[String],
) -> Result<bool, StorageError> {
    let title = title.to_lowercase();
    let index = rx.open_table(CONVERSATION_SEARCH)?;
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    for term in terms {
        if title.contains(term) {
            continue;
        }
        let chars = term.chars().take(3).collect::<Vec<_>>();
        let gram = indexes::search_gram(&chars);
        let mut matched = false;
        for candidate in
            index.range((conversation.get(), gram, 0)..=(conversation.get(), gram, u128::MAX))?
        {
            let (_, _, turn) = candidate?.0.value();
            let turn = JobId::new(turn);
            {
                let job = mutable_job_record_in(rx, turn)?
                    .ok_or_else(|| StorageError::corrupted("search turn missing"))?;
                if job.job.parent_id != conversation
                    || job.job.phase == crate::db::entities::MutableJobPhase::Unsubmitted
                {
                    return Err(StorageError::corrupted(
                        "search turn has invalid conversation",
                    ));
                }
                if indexes::search_input_text(&rx.open_table(ATTACHMENTS)?, &job.job.input)?
                    .to_lowercase()
                    .contains(term)
                {
                    matched = true;
                    break;
                }
                let completed = job
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
                for output in outputs.range((turn.get(), 0)..=(turn.get(), u32::MAX))? {
                    let (key, bytes) = output?;
                    let output = rows::decode_output(key.value(), bytes.value())?;
                    if indexes::search_output_text(&output, completed)
                        .is_some_and(|text| text.to_lowercase().contains(term))
                    {
                        matched = true;
                        break;
                    }
                }
                if matched {
                    break;
                }
            }
        }
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

const TURN_PAGE_OUTPUTS: usize = 1000;

pub(crate) fn read_conversation_limits(
    database: &Database,
    conversation: JobId,
) -> Result<Option<(JobLimits, u8)>, StorageError> {
    let rx = read(database)?;
    let mut current = conversation;
    let mut limits = JobLimits::CEILING;
    for depth in 0..=crate::db::entities::MAX_JOB_DEPTH {
        let Some(row) = read_conversation_row(&rx, current)? else {
            return Ok(None);
        };
        let own = read_job_lifecycle(&rx, current)?.limits;
        if let Some(own) = own {
            limits = limits.intersect(own);
        }
        match row.parent_id {
            Some(parent) => current = parent,
            None => return Ok(Some((limits.intersect(own.unwrap_or_default()), depth))),
        }
    }
    Err(StorageError::corrupted(
        "conversation ancestry exceeds the hard depth limit",
    ))
}

fn mutable_context_usage(
    rx: &ReadTransaction,
    conversation: JobId,
    start: JobId,
) -> Result<(usize, u32, u32), StorageError> {
    use crate::db::entities::MutableJobPhase;
    let mut cursor = Some(start);
    let mut seen = BTreeSet::new();
    let mut total = 0usize;
    let mut growth = 0usize;
    let mut settled = 0u32;
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let allocations = rx.open_table(POOL_ALLOCATIONS)?;
    while let Some(turn) = cursor {
        if !seen.insert(turn) {
            return Err(StorageError::corrupted("context ancestry cycle"));
        }
        if seen.len() > 4096 {
            return Ok((usize::MAX, u32::try_from(growth).unwrap_or(u32::MAX), 4096));
        }
        let record = mutable_job_record_in(rx, turn)?
            .ok_or_else(|| StorageError::corrupted("context turn missing"))?;
        if record.job.parent_id != conversation || record.job.phase == MutableJobPhase::Unsubmitted
        {
            return Err(StorageError::corrupted("invalid context turn relationship"));
        }
        let mut bytes = 16usize
            .saturating_add(record.job.input.text().len())
            .saturating_add(record.job.input.attachments().len().saturating_mul(4800));
        let ended_unstarted = record.job.phase == MutableJobPhase::Queued
            && unstarted_settlement_in(
                rx,
                &record,
                allocations
                    .get(turn.get())?
                    .map(|row| allocations::decode_pool_allocation(turn.get(), row.value()))
                    .transpose()?
                    .as_ref(),
            )?;
        for entry in outputs.range((turn.get(), 0)..=(turn.get(), u32::MAX))? {
            if record.job.phase != MutableJobPhase::Executing && !ended_unstarted {
                return Err(StorageError::corrupted("Job has output before execution"));
            }
            let (key, value) = entry?;
            let output = decode_output(key.value(), value.value())?;
            if output.kind == OutputKind::Result {
                bytes = bytes.saturating_add(output.display_text().as_ref().map_or(0, String::len));
            }
        }
        let tokens = bytes.div_ceil(3);
        total = total.saturating_add(tokens);
        let stopped = match record.outcome.as_ref().map(|outcome| outcome.kind) {
            Some(OutcomeKind::Completed | OutcomeKind::Failed | OutcomeKind::Cancelled) => true,
            Some(OutcomeKind::Unknown) => {
                let allocation = allocations.get(turn.get())?.ok_or_else(|| {
                    StorageError::corrupted("unknown Job has no allocation evidence")
                })?;
                let allocation =
                    allocations::decode_pool_allocation(turn.get(), allocation.value())?;
                if allocation.token.job != turn || allocation.job_revision != record.job.revision {
                    return Err(StorageError::corrupted("invalid context allocation"));
                }
                allocation.stop_confirmed_at.is_some()
            }
            None => false,
        };
        if stopped {
            settled = settled.saturating_add(1);
            if settled <= 8 {
                growth = growth.max(tokens);
            }
        }
        cursor = record.job.previous;
    }
    Ok((total, u32::try_from(growth).unwrap_or(u32::MAX), settled))
}

pub(crate) fn read_context_state(
    database: &Database,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Option<ContextState>, StorageError> {
    let rx = read(database)?;
    let Some(owner) = read_conversation_row(&rx, conversation)? else {
        return Ok(None);
    };
    let (mut policy, revision) = effective_context_policy(&rx.open_table(META)?)?;
    let tip = turn_head(&rx, conversation)?;
    let tip_details = tip
        .map(|tip| mutable_job_details_in(&rx, tip, None, false))
        .transpose()?
        .flatten();
    let active = tip_details
        .as_ref()
        .and_then(|details| match &details.record.job.frozen {
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(configuration)) => {
                Some(configuration.clone())
            }
            Some(crate::db::entities::MutableFrozenConfiguration::Decision(_)) | None => None,
        });
    let model = active.as_ref().map(|frozen| frozen.llm.clone());
    let (raw, growth, _) = tip
        .map(|tip| mutable_context_usage(&rx, conversation, tip))
        .transpose()?
        .unwrap_or_default();
    let settings = global_compaction_settings(&rx.open_table(META)?)?;
    let project = owner.project;
    let decision = project
        .map(|id| {
            rx.open_table(PROJECTS)?
                .get(id.get())?
                .map(|bytes| decode_project(id.get(), bytes.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("context project missing"))
        })
        .transpose()?
        .map(|project| project.decision_policy())
        .unwrap_or_default();
    let mut predictor = None;
    let mut predictor_model = None;
    let previous_binding = policy.predictive.take();
    let prediction_unavailable = if !settings.prediction_enabled {
        Some("prediction is disabled in global settings".to_owned())
    } else if matches!(policy.mode, crate::db::entities::CompactionMode::Off) {
        Some("automatic compaction is disabled".to_owned())
    } else if !decision.enabled {
        Some("prediction is disabled by the project decision configuration".to_owned())
    } else if let Some(active) = &active {
        let resolved = ConfigurationResolver::new(|id| {
            read_config_record(&rx, id).map_err(|error| BatchError(Failure::Storage(error)))
        })
        .project_decision(&decision, active);
        match resolved {
            Ok(Some(frozen)) => {
                predictor = Some(frozen.profile_ref().id);
                predictor_model = Some(frozen.model_ref().id);
                policy.predictive = previous_binding;
                policy.bind_prediction(&frozen);
                if !decision_executor_available(&rx, &frozen)? {
                    Some("no enabled Executor offers the project decision model".to_owned())
                } else if active.llm.config.context_window.is_none() {
                    Some("foreground model capacity is unknown".to_owned())
                } else {
                    None
                }
            }
            Ok(None) => {
                Some("prediction is disabled by the project decision configuration".to_owned())
            }
            Err(BatchError(Failure::Rejected(violation))) => Some(violation.to_string()),
            Err(BatchError(Failure::Storage(error))) => return Err(error),
        }
    } else {
        Some("foreground model is unavailable".to_owned())
    };
    let limit = limit.clamp(1, 100);
    let mut work = Vec::new();
    let adopted_producer = tip_details.and_then(|d| match d.record.job.context_selection {
        crate::db::entities::MutableContextSelection::AdoptedSummary { producer, .. } => {
            Some(producer)
        }
        crate::db::entities::MutableContextSelection::FullHistory => None,
    });
    for entry in rx
        .open_table(CHILDREN)?
        .range((conversation.get(), 0)..(conversation.get(), before.map_or(u128::MAX, JobId::get)))?
        .rev()
        .take(limit)
    {
        let child_id = JobId::new(entry?.0.value().1);
        let Some(bytes) = rx.open_table(JOBS)?.get(child_id.get())? else {
            continue;
        };
        let job = crate::db::redb::jobs::decode_mutable_job(child_id.get(), bytes.value())?;
        let Some(source) = job.summary_source else {
            continue;
        };
        let state = read_state(&rx, child_id)?;
        let manifest_bytes = rx
            .open_table(MESSAGES)?
            .get(source.as_bytes().as_slice())?
            .ok_or_else(|| {
                StorageError::corrupted(format!("summary manifest for {child_id} is missing"))
            })?;
        let manifest = serde_json::from_slice::<crate::db::entities::NativeSummaryManifest>(
            manifest_bytes.value(),
        )
        .map_err(|_| {
            StorageError::corrupted(format!("summary manifest for {child_id} is malformed"))
        })?;
        manifest.validate().map_err(StorageError::corrupted)?;
        let (Some(first_b), Some(last_b)) = (manifest.sources.first(), manifest.sources.last())
        else {
            continue;
        };
        let label = match state.outcome.as_ref().map(|o| o.kind) {
            None => match job.phase {
                crate::db::entities::MutableJobPhase::Unsubmitted => "queued",
                crate::db::entities::MutableJobPhase::Queued => "queued",
                crate::db::entities::MutableJobPhase::Executing => "preparing",
            },
            Some(OutcomeKind::Failed) => "failed",
            Some(OutcomeKind::Cancelled) => "cancelled",
            Some(OutcomeKind::Unknown) => "unknown",
            Some(OutcomeKind::Completed) => {
                if adopted_producer == Some(child_id) {
                    "adopted"
                } else if let Some(t) = tip
                    && verify_native_summary_in(&rx, conversation, t, child_id, source)
                        .map_err(SummarySourceError::into_storage)?
                        .is_some()
                {
                    "completed"
                } else {
                    "stale"
                }
            }
        };
        let failure = if matches!(label, "failed" | "cancelled" | "unknown") {
            rx.open_table(JOB_OUTPUTS)?
                .range((child_id.get(), 0)..=(child_id.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map(|(k, b)| decode_output(k.value(), b.value()))
                .transpose()?
                .and_then(|o| o.summary)
        } else {
            None
        };
        let summarizer = match &job.frozen {
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(f)) => f.llm.model.id,
            _ => ConfigId::new(0),
        };
        let boundary =
            |b: &crate::db::entities::NativeSummaryBoundary| crate::db::entities::SourceBoundary {
                turn: b.job,
                input_through: u32::try_from(b.revision).unwrap_or(u32::MAX),
                output_through: b.output_through,
                content: b.content,
            };
        work.push(ContextWorkState {
            job: child_id,
            conversation,
            state: label,
            details: ContextWorkDetails::Summary {
                summarizer,
                failure,
                automatic: job.input.priority != crate::db::entities::JobQueuePriority::Foreground,
                stage: "direct",
                chain: None,
                prediction: None,
                reserved_tokens: 0,
                reserved_requests: 0,
                source,
                first: boundary(first_b),
                through: boundary(last_b),
                turn_count: manifest.sources.len(),
            },
        });
    }
    Ok(Some(ContextState {
        steer_blocked_by: None,
        policy,
        stored_override: None,
        policy_source: "global",
        revision,
        model,
        estimated_tokens: (raw != usize::MAX).then_some(u64::try_from(raw).unwrap_or(u64::MAX)),
        growth,
        predictor,
        predictor_model,
        prediction_unavailable,
        last_use: None,
        work,
        next_before: None,
    }))
}

fn read_config_record(
    rx: &ReadTransaction,
    id: ConfigId,
) -> Result<Option<ConfigRecord>, StorageError> {
    rx.open_table(CONFIGS)?
        .get(id.get())?
        .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()).map(config_record))
        .transpose()
}

pub(super) fn decision_executor_available(
    rx: &impl TxRead,
    frozen: &crate::db::entities::FrozenDecisionConfig,
) -> Result<bool, StorageError> {
    let (operation, protocol) = match frozen {
        crate::db::entities::FrozenDecisionConfig::Native(_) => {
            (ExecutionOperation::Decision, None)
        }
        crate::db::entities::FrozenDecisionConfig::Generative(active, _) => (
            ExecutionOperation::LlmRequest,
            Some(active.llm.provider.transport),
        ),
    };
    Ok(!pools_offering_capability(rx, operation, protocol)?.is_empty())
}
