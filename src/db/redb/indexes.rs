//! The derived tables, computed from the authoritative ones alone.
//!
//! Each derived table is written in the same transaction as the fact it
//! derives from. After an unclean shutdown the open procedure compares every
//! one with its definition (I10) and rebuilds those that differ, logging the
//! difference. An index is never an authority.

use std::collections::BTreeSet;

use ::redb::TableHandle;

use super::*;
use crate::db::entities::{OutcomeKind, RequestEventRecord, RequestPayload};

/// Encodes up to three Unicode scalars, including the length.
pub(super) fn search_gram(chars: &[char]) -> u128 {
    chars.iter().fold(chars.len() as u128, |key, ch| {
        (key << 21) | u128::from(u32::from(*ch))
    })
}

pub(super) fn search_grams(text: &str) -> BTreeSet<u128> {
    let mut grams = BTreeSet::new();
    let mut window = [char::default(); 3];
    let mut count = 0;
    for ch in text.to_lowercase().chars() {
        window.rotate_left(1);
        window[2] = ch;
        count = (count + 1).min(3);
        for length in 1..=count {
            grams.insert(search_gram(&window[3 - length..]));
        }
    }
    grams
}

pub(super) fn index_search_text(
    tx: &WriteTransaction,
    conversation: JobId,
    turn: JobId,
    source: u8,
    seq: u32,
    text: &str,
) -> Result<(), StorageError> {
    let jobs = tx.open_table(JOBS)?;
    let row = jobs
        .get(conversation.get())?
        .ok_or_else(|| StorageError::corrupted("search conversation missing"))?;
    let turn_row = jobs
        .get(turn.get())?
        .ok_or_else(|| StorageError::corrupted("search turn missing"))?;
    if !searchable_conversation(&JobSpec::decode_at(conversation.get(), row.value())?)
        || !searchable_turn(&JobSpec::decode_at(turn.get(), turn_row.value())?)
    {
        return Ok(());
    }
    let mut table = tx.open_table(CONVERSATION_SEARCH)?;
    for gram in search_grams(text) {
        table.insert((conversation.get(), gram, turn.get(), source, seq), ())?;
    }
    Ok(())
}

pub(super) fn searchable_conversation(row: &JobSpec) -> bool {
    row.parent_id.is_none()
        && matches!(&row.kind, JobKind::Group { origin, .. } if *origin != Origin::System)
}

pub(super) fn searchable_turn(row: &JobSpec) -> bool {
    matches!(
        &row.kind,
        JobKind::Execution {
            execution: JobExecution::Frozen(_) | JobExecution::Ref(_),
            ..
        }
    )
}

pub(super) fn search_input_text(
    attachments: &impl ReadableTable<u128, &'static [u8]>,
    entry: &Entry,
) -> Result<Option<String>, StorageError> {
    match entry {
        Entry::UserMessage {
            text,
            attachments: ids,
        }
        | Entry::UserMessageEdit {
            text,
            attachments: ids,
            ..
        } => {
            let mut text = text.clone();
            for id in ids {
                let bytes = attachments
                    .get(id.get())?
                    .ok_or_else(|| StorageError::corrupted("search attachment missing"))?;
                if let Some(name) = rows::decode_attachment(id.get(), bytes.value())?.display_name {
                    text.push('\n');
                    text.push_str(&name);
                }
            }
            Ok(Some(text))
        }
        Entry::AgentMessage { text, .. } => Ok(Some(text.clone())),
        Entry::Notice { .. }
        | Entry::Decision { .. }
        | Entry::RetryOf { .. }
        | Entry::Iteration { .. }
        | Entry::UserMessageWithdraw { .. }
        | Entry::ContextPreparation { .. }
        | Entry::ContextUse { .. }
        | Entry::ResponseFormat { .. } => Ok(None),
    }
}

pub(super) fn search_output_text(output: &JobOutputRecord, completed: bool) -> Option<&str> {
    if output.kind != OutputKind::Result {
        return None;
    }
    output
        .value
        .as_ref()
        .and_then(|value| value.get("text"))
        .and_then(serde_json::Value::as_str)
        .or(output.summary.as_deref().filter(|_| completed))
}

fn visit_search_keys(
    tx: &WriteTransaction,
    mut visit: impl FnMut(schema::ConversationSearchKey) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let jobs = tx.open_table(JOBS)?;
    let conversation_for = |turn: u128| -> Result<Option<JobId>, StorageError> {
        let row = jobs
            .get(turn)?
            .ok_or_else(|| StorageError::corrupted("search turn missing"))?;
        let row = JobSpec::decode_at(turn, row.value())?;
        if !searchable_turn(&row) {
            return Ok(None);
        }
        let Some(parent) = row.parent_id else {
            return Ok(None);
        };
        let row = jobs
            .get(parent.get())?
            .ok_or_else(|| StorageError::corrupted("search parent missing"))?;
        Ok(
            searchable_conversation(&JobSpec::decode_at(parent.get(), row.value())?)
                .then_some(parent),
        )
    };
    let inputs = tx.open_table(JOB_INPUTS)?;
    for entry in jobs.iter()? {
        let turn = entry?.0.value();
        if let Some(conversation) = conversation_for(turn)? {
            for (seq, entry) in
                reader::effective_entries(reader::job_entries_of(&inputs, JobId::new(turn))?)?
            {
                if let Some(text) = search_input_text(&tx.open_table(ATTACHMENTS)?, &entry.entry)? {
                    for gram in search_grams(&text) {
                        visit((conversation.get(), gram, turn, 0, seq))?;
                    }
                }
            }
        }
    }
    for entry in tx.open_table(JOB_OUTPUTS)?.iter()? {
        let (key, bytes) = entry?;
        let (turn, seq) = key.value();
        let completed = tx
            .open_table(JOB_STATES)?
            .get(turn)?
            .map(|bytes| JobState::decode_at(turn, bytes.value()))
            .transpose()?
            .and_then(|state| state.outcome)
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
        if let Some(conversation) = conversation_for(turn)? {
            let output = decode_output(key.value(), bytes.value())?;
            if let Some(text) = search_output_text(&output, completed) {
                for gram in search_grams(text) {
                    visit((conversation.get(), gram, turn, 1, seq))?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn request_scope_key(
    identity: Option<&reader::UsageIdentity>,
    event: &RequestEventRecord,
) -> Option<schema::RequestScopeKey> {
    identity
        .filter(|_| {
            !matches!(
                event.payload,
                RequestPayload::Sent { .. }
                    | RequestPayload::Received { .. }
                    | RequestPayload::ApprovedCalls
            )
        })
        .map(|identity| {
            (
                identity.provider,
                *ContentDigest::of(identity.model.as_bytes()).as_bytes(),
                event.observed_at.as_unix_micros(),
                event.job_id.get(),
                event.request.get(),
                event.seq,
            )
        })
}

fn job(tx: &WriteTransaction, id: u128) -> Result<JobSpec, StorageError> {
    tx.open_table(JOBS)?
        .get(id)?
        .map(|bytes| JobSpec::decode_at(id, bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("missing job {id}")))
}

fn state(tx: &WriteTransaction, id: u128) -> Result<JobState, StorageError> {
    tx.open_table(JOB_STATES)?
        .get(id)?
        .map(|bytes| JobState::decode_at(id, bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("job {id} has no state")))
}

fn control(tx: &WriteTransaction, id: u128) -> Result<JobControl, StorageError> {
    tx.open_table(JOB_CONTROLS)?
        .get(id)?
        .map(|bytes| JobControl::decode(bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("group {id} has no control")))
}

fn visit_jobs(
    tx: &WriteTransaction,
    reverse: bool,
    mut visit: impl FnMut(JobSpec) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let mut cursor = if reverse { u128::MAX } else { 0 };
    let mut first = true;
    loop {
        let ids = {
            let jobs = tx.open_table(JOBS)?;
            if reverse {
                jobs.range(..cursor)?
                    .rev()
                    .take(256)
                    .map(|entry| {
                        entry
                            .map(|(key, _)| key.value())
                            .map_err(StorageError::from)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else if first {
                jobs.iter()?
                    .take(256)
                    .map(|entry| {
                        entry
                            .map(|(key, _)| key.value())
                            .map_err(StorageError::from)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                jobs.range((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))?
                .take(256)
                .map(|entry| {
                    entry
                        .map(|(key, _)| key.value())
                        .map_err(StorageError::from)
                })
                .collect::<Result<Vec<_>, _>>()?
            }
        };
        if ids.is_empty() {
            return Ok(());
        }
        for id in ids {
            cursor = id;
            visit(job(tx, id)?)?;
        }
        first = false;
    }
}

fn held(tx: &WriteTransaction, id: JobId) -> Result<bool, StorageError> {
    let mut current = Some(id);
    let mut direct = true;
    while let Some(id) = current {
        let row = job(tx, id.get())?;
        if !row.kind.is_group() {
            return Ok(false);
        }
        if let Some(seq) = control(tx, id.get())?.on_hold {
            let notice = tx
                .open_table(NOTICES)?
                .get((id.get(), seq))?
                .map(|bytes| Notice::decode(bytes.value()))
                .transpose()?;
            if direct
                || !matches!(
                    notice.map(|notice| notice.body),
                    Some(NoticeBody::Record {
                        by: Actor::Loop,
                        ..
                    })
                )
            {
                return Ok(true);
            }
        }
        current = row.parent_id;
        direct = false;
    }
    Ok(false)
}

fn rebuild_jobs(tx: &WriteTransaction, clock: i64) -> Result<(), StorageError> {
    visit_jobs(tx, false, |row| {
        let id = row.id.get();
        if let Some(parent) = row.parent_id {
            if parent >= row.id {
                return Err(StorageError::corrupted("job parent does not precede child"));
            }
            job(tx, parent.get())?;
        }
        let state = state(tx, id)?;
        let parent = row.parent_id.map_or(0, JobId::get);
        tx.open_table(CHILDREN)?.insert((parent, id), ())?;
        if let Some(allocation) = &row.allocation {
            tx.open_table(JOB_ALLOCATIONS)?
                .insert((allocation.owner.get(), id), ())?;
        }
        for attachment in row.kind.attachments() {
            tx.open_table(ATTACHMENT_REFS)?
                .insert((attachment.get(), id), ())?;
        }
        if let Some(previous) = row.kind.previous() {
            tx.open_table(SUCCESSORS)?.insert(previous.get(), id)?;
        }
        if row.kind.is_group() {
            control(tx, id)?;
            tx.open_table(CONVERSATIONS)?.insert(id, ())?;
            if row.parent_id.is_some() {
                tx.open_table(CHILD_GROUPS)?.insert((parent, id), ())?;
            }
        } else if row.kind.is_persistent() && row.parent_id.is_some() {
            tx.open_table(TURNS)?.insert((parent, id), ())?;
            tx.open_table(GROUP_HEADS)?.insert(parent, id)?;
            if let Some(claim) = &state.claim {
                let mut notices = tx.open_table(NOTICE_READ)?;
                let previous = notices.get(parent)?.map_or(0, |value| value.value());
                notices.insert(parent, previous.max(claim.notice_seq))?;
            }
            if let Some(outcome) = &state.outcome {
                let has_result = {
                    let mut found = false;
                    for entry in tx
                        .open_table(JOB_OUTPUTS)?
                        .range((id, 0)..=(id, u32::MAX))?
                    {
                        let (key, bytes) = entry?;
                        if decode_output(key.value(), bytes.value())?.kind == OutputKind::Result {
                            found = true;
                            break;
                        }
                    }
                    found
                };
                let mut results = tx.open_table(GROUP_RESULTS)?;
                let mut tips = results.get(parent)?.map_or((0, 0), |value| value.value());
                if outcome.kind != OutcomeKind::Cancelled {
                    tips.0 = id;
                }
                if has_result {
                    tips.1 = id;
                }
                if tips != (0, 0) {
                    results.insert(parent, tips)?;
                }
            }
        }
        Ok(())
    })?;
    visit_jobs(tx, true, |row| {
        let id = row.id.get();
        let state = state(tx, id)?;
        let children = tx
            .open_table(UNSETTLED)?
            .get(id)?
            .map_or(0, |value| value.value().1);
        if state.locally_stopped() && children == 0 {
            tx.open_table(UNSETTLED)?.remove(id)?;
        } else {
            let mut unsettled = tx.open_table(UNSETTLED)?;
            unsettled.insert(id, (0, children))?;
            if let Some(parent) = row.parent_id {
                let count = unsettled
                    .get(parent.get())?
                    .map_or(0, |value| value.value().1);
                unsettled.insert(parent.get(), (0, count.saturating_add(1)))?;
            }
        }
        if row.kind.is_persistent()
            && state.claim.is_some()
            && !state.locally_stopped()
            && let Some(parent) = row.parent_id
        {
            tx.open_table(ACTIVE_JOBS)?.insert(parent.get(), id)?;
        }
        Ok(())
    })?;
    visit_jobs(tx, false, |row| {
        let id = row.id.get();
        let state = state(tx, id)?;
        let unsettled = tx.open_table(UNSETTLED)?.get(id)?.is_some();
        if row.kind.is_group()
            && let Some(parent) = row.parent_id
            && unsettled
        {
            tx.open_table(LIVE_CHILDREN)?
                .insert((parent.get(), id), ())?;
        }
        let mut dependencies = 0u32;
        if !row.kind.is_group() && state.claim.is_none() && state.outcome.is_none() {
            tx.open_table(PENDING)?.insert(id, ())?;
            for (dependency, condition) in row.kind.waits_on() {
                if tx.open_table(UNSETTLED)?.get(dependency.get())?.is_some() {
                    dependencies = dependencies.saturating_add(1);
                    tx.open_table(DEPENDENCY_WAITERS)?
                        .insert((dependency.get(), id), condition.code())?;
                }
            }
            let blocked = match row.parent_id {
                Some(parent) => {
                    held(tx, parent)?
                        || tx
                            .open_table(ACTIVE_JOBS)?
                            .get(parent.get())?
                            .is_some_and(|active| active.value() != id)
                }
                None => false,
            };
            if dependencies == 0 && !blocked {
                match row.kind.eligible_at().map(Timestamp::as_unix_micros) {
                    Some(due) if due > clock => {
                        tx.open_table(DELAYED)?.insert((due, id), ())?;
                    }
                    Some(_) | None => {
                        let requirement = match &row.kind {
                            JobKind::Execution {
                                execution: JobExecution::Decision(_),
                                ..
                            } => transactions::decision_requirement(),
                            JobKind::Execution { .. } => {
                                let execution =
                                    reader::turn_config(tx, &row)?.ok_or_else(|| {
                                        StorageError::corrupted("ready job has no configuration")
                                    })?;
                                transactions::execution_requirement(&execution)
                            }
                            JobKind::Group { .. } => {
                                return Err(StorageError::corrupted("ready job is a group"));
                            }
                        };
                        tx.open_table(READY)?.insert((requirement, id), ())?;
                    }
                }
            }
        }
        if unsettled {
            let mut table = tx.open_table(UNSETTLED)?;
            let children = table.get(id)?.map_or(0, |value| value.value().1);
            table.insert(id, (dependencies, children))?;
        }
        if let Some(claim) = &state.claim {
            let holds = match &state.outcome {
                None => true,
                Some(outcome) => {
                    outcome.kind == OutcomeKind::Unknown && state.stop_confirmed_at.is_none()
                }
            };
            if holds {
                tx.open_table(HELD_SLOTS)?.insert(
                    (claim.pool_id.get(), id),
                    (claim.epoch, state.outcome.is_some()),
                )?;
            }
        }
        Ok(())
    })
}

fn rebuild_inputs(tx: &WriteTransaction) -> Result<(), StorageError> {
    for entry in tx.open_table(JOB_INPUTS)?.iter()? {
        let (key, bytes) = entry?;
        let (turn, _) = key.value();
        let input = JobInputEntry::decode(bytes.value())?;
        let user_message = matches!(input.entry, Entry::UserMessage { .. });
        match input.entry {
            Entry::UserMessage { attachments, .. } | Entry::UserMessageEdit { attachments, .. } => {
                for attachment in attachments {
                    tx.open_table(ATTACHMENT_REFS)?
                        .insert((attachment.get(), turn), ())?;
                }
                if user_message && let Some(parent) = job(tx, turn)?.parent_id {
                    let mut times = tx.open_table(USER_INPUT_AT)?;
                    if times.get(parent.get())?.is_none() {
                        times.insert(parent.get(), input.at.as_unix_micros())?;
                    }
                }
            }
            Entry::RetryOf { turn: original, by } => {
                tx.open_table(RETRIES)?
                    .insert((original.get(), turn), matches!(by, Actor::Agent { .. }))?;
            }
            Entry::AgentMessage { .. }
            | Entry::Notice { .. }
            | Entry::Decision { .. }
            | Entry::Iteration { .. }
            | Entry::UserMessageWithdraw { .. }
            | Entry::ContextPreparation { .. }
            | Entry::ContextUse { .. }
            | Entry::ResponseFormat { .. } => {}
        }
    }
    Ok(())
}

fn rebuild_tools(tx: &WriteTransaction) -> Result<(), StorageError> {
    for entry in tx.open_table(TOOL_CALLS)?.iter()? {
        let (key, bytes) = entry?;
        let call = decode_tool_evidence(tool_call_ref(key.value())?, bytes.value())?;
        if let Some(original) = call.approval
            && tx
                .open_table(APPROVAL_CALLS)?
                .insert(tool_call_key(original), key.value())?
                .is_some()
        {
            return Err(StorageError::corrupted("approval executed twice"));
        }
        if matches!(
            call.decision.fact,
            crate::db::entities::ToolDecision::AwaitingApproval(_)
                | crate::db::entities::ToolDecision::AwaitingAnswer(_)
        ) {
            let conversation = job(tx, call.call.job.get())?
                .parent_id
                .ok_or_else(|| StorageError::corrupted("awaiting call has no conversation"))?;
            let decision = tx
                .open_table(APPROVALS)?
                .get(key.value())?
                .map(|value| ApprovalRecord::decode(value.value()))
                .transpose()?;
            let open = match decision.map(|record| record.decision) {
                None => Some(0),
                Some(ApprovalDecision::Approved) => Some(1),
                Some(ApprovalDecision::Answered { .. }) if call.name == "job_dispatch" => Some(1),
                Some(
                    ApprovalDecision::Rejected
                    | ApprovalDecision::Answered { .. }
                    | ApprovalDecision::Voided,
                ) => None,
            };
            if let Some(open) = open {
                tx.open_table(OPEN_APPROVALS)?
                    .insert(approvals::open_key(conversation, call.call), open)?;
            }
        }
        tx.open_table(TOOL_CALL_IDS)?.insert(
            (call.call.job.get(), call.tool_call_id.as_str()),
            (call.call.request.get(), call.call.index),
        )?;
        let (admitted, refused) = match &call.decision.fact {
            crate::db::entities::ToolDecision::Admitted(_) => (1, 0),
            crate::db::entities::ToolDecision::Refused(
                crate::db::redb::ToolCallRefusal::InvalidArguments,
            ) => (0, 1),
            crate::db::entities::ToolDecision::AwaitingApproval(_)
            | crate::db::entities::ToolDecision::AwaitingAnswer(_)
            | crate::db::entities::ToolDecision::Refused(
                crate::db::redb::ToolCallRefusal::Malformed
                | crate::db::redb::ToolCallRefusal::ConflictingIdentity
                | crate::db::redb::ToolCallRefusal::AmbiguousIdentity
                | crate::db::redb::ToolCallRefusal::LimitExceeded
                | crate::db::redb::ToolCallRefusal::Incomplete
                | crate::db::redb::ToolCallRefusal::Unauthorized
                | crate::db::redb::ToolCallRefusal::MissingSchema
                | crate::db::redb::ToolCallRefusal::UnsupportedSchema
                | crate::db::redb::ToolCallRefusal::NoDecider,
            ) => (0, 0),
        };
        if admitted != 0 || refused != 0 || call.outcome.is_some() {
            let mut stats = tx.open_table(TOOL_STATS)?;
            let mut value = stats
                .get(call.name.as_str())?
                .map_or((0, 0, 0, 0, 0, 0), |value| value.value());
            value.0 = value.0.saturating_add(admitted);
            value.1 = value.1.saturating_add(refused);
            if let Some(outcome) = &call.outcome {
                let duration = outcome
                    .at
                    .as_unix_micros()
                    .saturating_sub(call.decision.at.as_unix_micros())
                    .max(0) as u64;
                value.4 = value.4.saturating_add(duration);
                value.5 = value.5.max(duration);
                match &outcome.fact {
                    crate::db::entities::ToolOutcome::Completed { .. }
                    | crate::db::entities::ToolOutcome::CompletedWithheld { .. } => {
                        value.2 = value.2.saturating_add(1)
                    }
                    crate::db::entities::ToolOutcome::NotApplied { .. }
                    | crate::db::entities::ToolOutcome::NotInvoked { .. }
                    | crate::db::entities::ToolOutcome::Uncertain { .. } => {
                        value.3 = value.3.saturating_add(1)
                    }
                }
            }
            stats.insert(call.name.as_str(), value)?;
        }
    }
    for entry in tx.open_table(APPROVAL_CALLS)?.iter()? {
        let (original, _) = entry?;
        let original = tool_call_ref(original.value())?;
        let conversation = job(tx, original.job.get())?
            .parent_id
            .ok_or_else(|| StorageError::corrupted("approval has no conversation"))?;
        tx.open_table(OPEN_APPROVALS)?
            .remove(approvals::open_key(conversation, original))?;
    }
    Ok(())
}

fn add_sample(
    tx: &WriteTransaction,
    sample: Option<crate::db::entities::TokenRequestSample>,
) -> Result<(), StorageError> {
    let Some(sample) = sample else {
        return Ok(());
    };
    let row = job(tx, sample.job.get())?;
    let Some(identity) = reader::turn_usage_identity_of(&tx.open_table(JOBS)?, &row)? else {
        return Ok(());
    };
    let parent = row
        .parent_id
        .map(|parent| job(tx, parent.get()))
        .transpose()?;
    let key = (
        sample
            .observed_at
            .as_unix_micros()
            .div_euclid(crate::db::entities::USAGE_SUMMARY_HOUR_MICROS),
        usage_agent(&row, parent.as_ref()),
        identity.provider,
        *ContentDigest::of(identity.model.as_bytes()).as_bytes(),
    );
    let mut totals = tx.open_table(USAGE_TOTALS)?;
    let mut value = totals
        .get(key)?
        .map_or((0, 0, 0, 0, 0, 0, 0), |value| value.value());
    value.0 = value.0.saturating_add(1);
    value.1 = value.1.saturating_add(u64::from(
        sample.usage == crate::db::entities::TokenUsageState::Complete,
    ));
    value.2 = value
        .2
        .saturating_add(u64::from(sample.input.value().unwrap_or(0)));
    value.3 = value
        .3
        .saturating_add(u64::from(sample.output.value().unwrap_or(0)));
    value.4 = value
        .4
        .saturating_add(u64::from(sample.cache_read.value().unwrap_or(0)));
    value.5 = value
        .5
        .saturating_add(u64::from(sample.cache_write.value().unwrap_or(0)));
    value.6 = value
        .6
        .saturating_add(u64::from(sample.reasoning.value().unwrap_or(0)));
    totals.insert(key, value)?;
    Ok(())
}

fn rebuild_events(tx: &WriteTransaction) -> Result<(), StorageError> {
    let table = tx.open_table(REQUEST_EVENTS)?;
    let mut current = None;
    let mut identity = None;
    let mut evidence = crate::db::entities::TokenEvidence::default();
    let mut invalid = false;
    for entry in table.iter()? {
        let (key, bytes) = entry?;
        let (id, request, seq) = key.value();
        if current != Some((id, request)) {
            if current.is_some() && !invalid {
                add_sample(tx, evidence.sample())?;
            }
            evidence = Default::default();
            invalid = false;
            current = Some((id, request));
            let row = job(tx, id)?;
            identity = reader::turn_usage_identity_of(&tx.open_table(JOBS)?, &row)?;
        }
        let request = RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("stored request number is zero"))?;
        let event = rows::decode_request_event(JobId::new(id), request, seq, bytes.value())?;
        if let Some(scope) = request_scope_key(identity.as_ref(), &event) {
            tx.open_table(REQUEST_SCOPES)?.insert(scope, ())?;
            tx.open_table(REQUEST_TIMELINE)?
                .insert((scope.2, scope.3, scope.4, scope.5), ())?;
        }
        if let Some(kind) = observations::observation_kind(&table, &event)? {
            tx.open_table(JOB_OBSERVATIONS)?
                .insert((id, kind, event.id.get()), (request.get(), seq))?;
        }
        if !invalid
            && let Some(record) = observations::metric_record(&table, &None, &event)?
            && let Some(observation) = crate::db::entities::TokenObservation::from_record(&record)
            && let Err(error) = evidence.add(observation)
        {
            log::warn!(
                "dropping Token usage of {} request {}: {error}",
                event.job_id,
                request.get()
            );
            invalid = true;
        }
    }
    if current.is_some() && !invalid {
        add_sample(tx, evidence.sample())?;
    }
    Ok(())
}

pub(super) fn content_reference(
    tx: &WriteTransaction,
    record: &AttachmentRecord,
    previous: Option<&AttachmentRecord>,
) -> Result<(), StorageError> {
    let mut references = tx.open_table(CONTENT_REFERENCES)?;
    let digest = record.sha256.as_bytes().as_slice();
    let (length, mut count) = references
        .get(digest)?
        .map_or((record.byte_length, 0), |value| value.value());
    if length != record.byte_length {
        return Err(StorageError::corrupted(
            "content digest has conflicting lengths",
        ));
    }
    let was_available = previous.is_some_and(AttachmentRecord::is_available);
    if was_available != record.is_available() {
        if record.is_available() {
            count = count
                .checked_add(1)
                .ok_or_else(|| StorageError::corrupted("content reference overflow"))?;
        } else {
            count = count
                .checked_sub(1)
                .ok_or_else(|| StorageError::corrupted("content reference underflow"))?;
        }
    }
    references.insert(digest, (length, count))?;
    let mut reclaim = tx.open_table(CONTENT_RECLAIM)?;
    if count == 0 {
        reclaim.insert(digest, length)?;
    } else {
        reclaim.remove(digest)?;
    }
    Ok(())
}

fn rebuild_contents(tx: &WriteTransaction) -> Result<(), StorageError> {
    for entry in tx.open_table(ATTACHMENTS)?.iter()? {
        let (key, bytes) = entry?;
        let record = decode_attachment(key.value(), bytes.value())?;
        content_reference(tx, &record, None)?;
    }
    Ok(())
}

fn fingerprint<K: ::redb::Key + 'static, V: ::redb::Value + 'static>(
    tx: &WriteTransaction,
    definition: TableDefinition<K, V>,
) -> Result<[u8; 32], StorageError> {
    let mut hash = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    for entry in tx.open_table(definition)?.iter()? {
        let (key, value) = entry?;
        let key = key.value();
        let value = value.value();
        let key_bytes = K::as_bytes(&key);
        let value_bytes = V::as_bytes(&value);
        for bytes in [key_bytes.as_ref(), value_bytes.as_ref()] {
            hash.update(&(bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
    }
    hash.finish()
        .as_ref()
        .try_into()
        .map_err(|_| StorageError::corrupted("invalid index digest"))
}

pub(super) fn rebuild(tx: &WriteTransaction, clock: i64) -> Result<Vec<String>, StorageError> {
    let mut before = Vec::new();
    macro_rules! reset { ($($definition:ident),+ $(,)?) => {$({
        before.push((stringify!($definition), fingerprint(tx, $definition)?));
        tx.delete_table($definition)?;
        drop(tx.open_table($definition)?);
    })+}; }
    reset!(
        JOB_ALLOCATIONS,
        CONVERSATIONS,
        TURNS,
        PENDING,
        READY,
        DELAYED,
        DEPENDENCY_WAITERS,
        UNSETTLED,
        HELD_SLOTS,
        CHILDREN,
        SUCCESSORS,
        GROUP_HEADS,
        CONVERSATION_SEARCH,
        CHILD_GROUPS,
        LIVE_CHILDREN,
        USER_INPUT_AT,
        ACTIVE_JOBS,
        GROUP_RESULTS,
        NOTICE_READ,
        TOOL_CALL_IDS,
        TOOL_STATS,
        USAGE_TOTALS,
        RETRIES,
        ATTACHMENT_REFS,
        REQUEST_SCOPES,
        REQUEST_TIMELINE,
        OPEN_APPROVALS,
        APPROVAL_CALLS,
        RECURRENCE_DUE,
        JOB_OBSERVATIONS,
        CONTENT_REFERENCES,
        CONTENT_RECLAIM
    );
    rebuild_jobs(tx, clock)?;
    rebuild_inputs(tx)?;
    rebuild_tools(tx)?;
    rebuild_events(tx)?;
    rebuild_contents(tx)?;
    for entry in tx.open_table(RECURRENCES)?.iter()? {
        let (id, bytes) = entry?;
        if let RecurrencePhase::WaitingDue { due } = Recurrence::decode(bytes.value())?.phase {
            tx.open_table(RECURRENCE_DUE)?
                .insert((due, id.value()), ())?;
        }
    }
    visit_search_keys(tx, |key| {
        tx.open_table(CONVERSATION_SEARCH)?.insert(key, ())?;
        Ok(())
    })?;
    let mut changed = Vec::new();
    macro_rules! compare { ($($definition:ident),+ $(,)?) => {$({
        if before.iter().find(|(name, _)| *name == stringify!($definition)).map(|(_, digest)| *digest) != Some(fingerprint(tx, $definition)?) {
            let name = $definition.name().to_owned();
            if !matches!(stringify!($definition), "PENDING" | "READY" | "DELAYED" | "DEPENDENCY_WAITERS" | "UNSETTLED") { log::warn!("derived table {name} differed from its definition and was rebuilt"); }
            changed.push(name);
        }
    })+}; }
    compare!(
        JOB_ALLOCATIONS,
        CONVERSATIONS,
        TURNS,
        PENDING,
        READY,
        DELAYED,
        DEPENDENCY_WAITERS,
        UNSETTLED,
        HELD_SLOTS,
        CHILDREN,
        SUCCESSORS,
        GROUP_HEADS,
        CONVERSATION_SEARCH,
        CHILD_GROUPS,
        LIVE_CHILDREN,
        USER_INPUT_AT,
        ACTIVE_JOBS,
        GROUP_RESULTS,
        NOTICE_READ,
        TOOL_CALL_IDS,
        TOOL_STATS,
        USAGE_TOTALS,
        RETRIES,
        ATTACHMENT_REFS,
        REQUEST_SCOPES,
        REQUEST_TIMELINE,
        OPEN_APPROVALS,
        APPROVAL_CALLS,
        RECURRENCE_DUE,
        JOB_OBSERVATIONS,
        CONTENT_REFERENCES,
        CONTENT_RECLAIM
    );
    Ok(changed)
}
