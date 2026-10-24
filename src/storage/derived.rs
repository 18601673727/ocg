//! The derived tables, computed from the authoritative ones alone.
//!
//! Each derived table is written in the same transaction as the fact it
//! derives from. After an unclean shutdown the open procedure compares every
//! one with its definition (I10) and rebuilds those that differ, logging the
//! difference. An index is never an authority.

use std::collections::{BTreeMap, HashMap, HashSet};

use redb::ReadableTableMetadata;

use super::*;
use crate::persistence::{OutcomeKind, RequestEventRecord, RequestPayload};

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
    if !searchable_conversation(&JobRow::decode_at(conversation.get(), row.value())?) {
        return Ok(());
    }
    let mut table = tx.open_table(CONVERSATION_SEARCH)?;
    for gram in search_grams(text) {
        table.insert((conversation.get(), gram, turn.get(), source, seq), ())?;
    }
    Ok(())
}

pub(super) fn searchable_conversation(row: &JobRow) -> bool {
    row.parent_id.is_none()
        && matches!(&row.kind, JobKind::Conversation { origin, .. } if *origin != Origin::System)
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
    auth: &Authoritative,
    mut visit: impl FnMut(schema::ConversationSearchKey) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    let conversation_for = |turn: u128| {
        auth.jobs
            .get(&turn)
            .and_then(|row| row.parent_id)
            .filter(|id| {
                auth.jobs
                    .get(&id.get())
                    .is_some_and(searchable_conversation)
            })
    };
    for entry in tx.open_table(TURN_INPUTS)?.iter()? {
        let (key, bytes) = entry?;
        let (turn, seq) = key.value();
        if let Some(conversation) = conversation_for(turn)
            && let Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. } =
                TurnEntry::decode(bytes.value())?.entry
        {
            for gram in search_grams(&text) {
                visit((conversation.get(), gram, turn, 0, seq))?;
            }
        }
    }
    for entry in tx.open_table(JOB_OUTPUTS)?.iter()? {
        let (key, bytes) = entry?;
        let (turn, seq) = key.value();
        let completed = auth
            .states
            .get(&turn)
            .and_then(|state| state.outcome.as_ref())
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
        if let Some(conversation) = conversation_for(turn) {
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

fn sync_search(tx: &WriteTransaction, auth: &Authoritative) -> Result<bool, StorageError> {
    let table = tx.open_table(CONVERSATION_SEARCH)?;
    let mut count = 0u64;
    let mut missing = false;
    visit_search_keys(tx, auth, |key| {
        count = count.saturating_add(1);
        missing |= table.get(key)?.is_none();
        Ok(())
    })?;
    let differs = missing || table.len()? != count;
    drop(table);
    if differs {
        tx.delete_table(CONVERSATION_SEARCH)?;
        let mut table = tx.open_table(CONVERSATION_SEARCH)?;
        visit_search_keys(tx, auth, |key| {
            table.insert(key, ())?;
            Ok(())
        })?;
    }
    Ok(differs)
}

/// What every derived table must hold, given the authoritative rows.
#[derive(Default)]
pub(super) struct Derived {
    pending: BTreeMap<u128, ()>,
    ready: BTreeMap<(u16, u128), ()>,
    delayed: BTreeMap<(i64, u128), ()>,
    waiters: BTreeMap<(u128, u128), u8>,
    unsettled: BTreeMap<u128, (u32, u32)>,
    held: BTreeMap<(u128, u128), (u64, bool)>,
    children: BTreeMap<(u128, u128), ()>,
    successors: BTreeMap<u128, u128>,
    tips: BTreeMap<u128, u128>,
    recent: BTreeMap<(u128, u128), ()>,
    child_conversations: BTreeMap<(u128, u128), ()>,
    live_children: BTreeMap<(u128, u128), ()>,
    user_entry_at: BTreeMap<u128, i64>,
    notice_read: BTreeMap<u128, u32>,
    answer_tips: BTreeMap<u128, (u128, u128)>,
    active_turns: BTreeMap<u128, u128>,
    tool_call_ids: BTreeMap<(u128, String), (u32, u32)>,
    tool_totals: BTreeMap<u128, (u64, u64)>,
    tool_stats: BTreeMap<String, schema::ToolStatValue>,
    usage_totals: BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue>,
    retries: BTreeMap<(u128, u128), bool>,
    attachment_refs: BTreeMap<(u128, u128), ()>,
}

struct Authoritative {
    jobs: BTreeMap<u128, JobRow>,
    states: BTreeMap<u128, JobStateRow>,
    conversations: BTreeMap<u128, ConversationRow>,
    result_turns: BTreeSet<u128>,
    executions: BTreeMap<u128, FrozenExecutionConfig>,
    held_conversations: BTreeSet<u128>,
    tool_call_ids: Vec<(u128, String, u32, u32)>,
    tool_totals: BTreeMap<u128, (u64, u64)>,
    tool_stats: BTreeMap<String, schema::ToolStatValue>,
    usage_totals: BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue>,
    attachment_refs: BTreeMap<(u128, u128), ()>,
    user_entry_at: BTreeMap<u128, i64>,
    retries: BTreeMap<(u128, u128), bool>,
    request_scopes_differ: bool,
}

fn read_authoritative(tx: &WriteTransaction) -> Result<Authoritative, StorageError> {
    let mut jobs = BTreeMap::new();
    for entry in tx.open_table(JOBS)?.iter()? {
        let (key, bytes) = entry?;
        jobs.insert(key.value(), JobRow::decode_at(key.value(), bytes.value())?);
    }
    let mut states = BTreeMap::new();
    for entry in tx.open_table(JOB_STATES)?.iter()? {
        let (key, bytes) = entry?;
        states.insert(
            key.value(),
            JobStateRow::decode_at(key.value(), bytes.value())?,
        );
    }
    let mut conversations = BTreeMap::new();
    for entry in tx.open_table(CONVERSATIONS)?.iter()? {
        let (key, bytes) = entry?;
        conversations.insert(key.value(), ConversationRow::decode(bytes.value())?);
    }
    let mut result_turns = BTreeSet::new();
    for entry in tx.open_table(JOB_OUTPUTS)?.iter()? {
        let (key, bytes) = entry?;
        let output = decode_output(key.value(), bytes.value())?;
        if output.kind == OutputKind::Result {
            result_turns.insert(key.value().0);
        }
    }
    let mut attachment_refs = BTreeMap::new();
    let mut user_entry_at = BTreeMap::new();
    let mut retries = BTreeMap::new();
    for entry in tx.open_table(TURN_INPUTS)?.iter()? {
        let (key, bytes) = entry?;
        let (turn, _) = key.value();
        let input = TurnEntry::decode(bytes.value())?;
        match &input.entry {
            Entry::UserMessage { attachments, .. } => {
                for attachment in attachments {
                    attachment_refs.insert((attachment.get(), turn), ());
                }
                let row = jobs.get(&turn).ok_or_else(|| {
                    StorageError::corrupted(format!(
                        "turn input names job {turn}, which has no job row"
                    ))
                })?;
                if let Some(conversation) = row.parent_id {
                    user_entry_at
                        .entry(conversation.get())
                        .or_insert(input.at.as_unix_micros());
                }
            }
            Entry::RetryOf { turn: original, by } => {
                retries.insert((original.get(), turn), matches!(by, Actor::Agent { .. }));
            }
            Entry::AgentMessage { .. }
            | Entry::Notice { .. }
            | Entry::Decision { .. }
            | Entry::Iteration { .. }
            | Entry::ContextPreparation { .. }
            | Entry::ContextUse { .. }
            | Entry::ResponseFormat { .. } => {}
        }
    }
    let mut executions = BTreeMap::new();
    let mut held_conversations = BTreeSet::new();
    let mut subtree_holds = BTreeSet::new();
    for (id, row) in &jobs {
        if !row.kind.is_conversation() {
            executions.insert(*id, reads::turn_execution(&tx.open_table(JOBS)?, row)?.0);
            continue;
        }
        if row
            .parent_id
            .is_some_and(|parent| subtree_holds.contains(&parent.get()))
        {
            held_conversations.insert(*id);
            subtree_holds.insert(*id);
        }
        if let Some(metadata) = conversations.get(id)
            && let Some(seq) = metadata.on_hold
        {
            held_conversations.insert(*id);
            let hold = tx
                .open_table(NOTICES)?
                .get((*id, seq))?
                .map(|bytes| Notice::decode(bytes.value()))
                .transpose()?;
            if !matches!(
                hold.map(|notice| notice.body),
                Some(NoticeBody::Record {
                    by: Actor::Loop,
                    ..
                })
            ) {
                subtree_holds.insert(*id);
            }
        }
    }
    let mut tool_call_ids = Vec::new();
    let mut tool_totals: BTreeMap<u128, (u64, u64)> = BTreeMap::new();
    let mut tool_stats: BTreeMap<String, schema::ToolStatValue> = BTreeMap::new();
    for entry in tx.open_table(TOOL_CALLS)?.iter()? {
        let (key, bytes) = entry?;
        let (job, request, index) = key.value();
        let record = decode_tool_evidence(tool_call_ref((job, request, index))?, bytes.value())?;
        if let Some(admission) = record.decision.fact.stored_admission() {
            let totals = tool_totals.entry(job).or_insert((0, 0));
            totals.0 = totals.0.saturating_add(admission.arguments.len() as u64);
            totals.1 = totals.1.saturating_add(admission.parameters.len() as u64);
        }
        let mut count_changed = false;
        let mut admitted_inc = 0;
        let mut refused_inc = 0;
        match &record.decision.fact {
            crate::persistence::ToolDecision::Admitted(_) => {
                admitted_inc = 1;
                count_changed = true;
            }
            crate::persistence::ToolDecision::AwaitingApproval(_)
            | crate::persistence::ToolDecision::AwaitingAnswer(_) => {}
            crate::persistence::ToolDecision::Refused(
                crate::storage::ToolCallRefusal::InvalidArguments,
            ) => {
                refused_inc = 1;
                count_changed = true;
            }
            crate::persistence::ToolDecision::Refused(
                crate::storage::ToolCallRefusal::Malformed
                | crate::storage::ToolCallRefusal::ConflictingIdentity
                | crate::storage::ToolCallRefusal::AmbiguousIdentity
                | crate::storage::ToolCallRefusal::LimitExceeded
                | crate::storage::ToolCallRefusal::Incomplete
                | crate::storage::ToolCallRefusal::Unauthorized
                | crate::storage::ToolCallRefusal::MissingSchema
                | crate::storage::ToolCallRefusal::UnsupportedSchema
                | crate::storage::ToolCallRefusal::NoDecider,
            ) => {}
        }
        if count_changed || record.outcome.is_some() {
            let stats = tool_stats
                .entry(record.name.clone())
                .or_insert((0, 0, 0, 0, 0, 0));
            stats.0 = stats.0.saturating_add(admitted_inc);
            stats.1 = stats.1.saturating_add(refused_inc);
            if let Some(outcome) = &record.outcome {
                let duration_us = outcome
                    .at
                    .as_unix_micros()
                    .saturating_sub(record.decision.at.as_unix_micros())
                    .max(0) as u64;
                stats.4 = stats.4.saturating_add(duration_us);
                stats.5 = stats.5.max(duration_us);
                match &outcome.fact {
                    crate::persistence::ToolOutcome::Completed { .. }
                    | crate::persistence::ToolOutcome::CompletedWithheld { .. } => {
                        stats.2 = stats.2.saturating_add(1);
                    }
                    crate::persistence::ToolOutcome::NotApplied { .. }
                    | crate::persistence::ToolOutcome::NotInvoked { .. }
                    | crate::persistence::ToolOutcome::Uncertain { .. } => {
                        stats.3 = stats.3.saturating_add(1);
                    }
                }
            }
        }
        tool_call_ids.push((job, record.tool_call_id, request, index));
    }
    let (usage_totals, request_scopes_differ) = usage_totals_from_events(
        &tx.open_table(REQUEST_EVENTS)?,
        &tx.open_table(REQUEST_SCOPES)?,
        &jobs,
        &executions,
    )?;
    Ok(Authoritative {
        jobs,
        states,
        conversations,
        result_turns,
        executions,
        held_conversations,
        tool_call_ids,
        tool_totals,
        tool_stats,
        usage_totals,
        attachment_refs,
        user_entry_at,
        retries,
        request_scopes_differ,
    })
}

/// The REQUEST_SCOPES key an event writes, if it writes one: only an event
/// of a Job with a frozen execution, which no conversation has, and never a
/// Sent, Received or ApprovedCalls event. The definition every writer and
/// the rebuild share.
pub(super) fn request_scope_key(
    execution: Option<&FrozenExecutionConfig>,
    event: &RequestEventRecord,
) -> Option<schema::RequestScopeKey> {
    execution
        .filter(|_| {
            !matches!(
                event.payload,
                RequestPayload::Sent { .. }
                    | RequestPayload::Received { .. }
                    | RequestPayload::ApprovedCalls
            )
        })
        .map(|execution| {
            (
                execution.llm.provider.id.get(),
                *ContentDigest::of(execution.llm.config.model.as_bytes()).as_bytes(),
                event.observed_at.as_unix_micros(),
                event.job_id.get(),
                event.request.get(),
                event.seq,
            )
        })
}

/// One ordered pass over the request events, grouped by (Job, request): the
/// events of one request are contiguous in table order, so each group is
/// reduced on its own as the pass meets its first event of the next request.
/// Nothing else is kept, so the pass holds one request's events, never the
/// whole table. Each event's expected scope key is looked up as the event is
/// decoded and counted, never collected, and the scopes differ when one is
/// missing or the count does not match the table.
fn usage_totals_from_events(
    table: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    scopes: &impl ReadableTable<schema::RequestScopeKey, ()>,
    jobs: &BTreeMap<u128, JobRow>,
    executions: &BTreeMap<u128, FrozenExecutionConfig>,
) -> Result<
    (
        BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue>,
        bool,
    ),
    StorageError,
> {
    let mut totals: BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue> = BTreeMap::new();
    let mut scopes_differ = false;
    let mut scopes_seen = 0u64;
    let mut current: Option<(u128, u32)> = None;
    let mut events: Vec<crate::persistence::RequestEventRecord> = Vec::new();
    for entry in table.iter()? {
        let (key, bytes) = entry?;
        let (job, request, seq) = key.value();
        if current != Some((job, request)) {
            if let Some((job, request)) = current {
                add_usage(&mut totals, jobs, executions, job, request, &events)?;
            }
            events.clear();
            current = Some((job, request));
        }
        if let Some(request) = RequestNumber::new(request) {
            let event = rows::decode_request_event(JobId::new(job), request, seq, bytes.value())?;
            if let Some(scope) = request_scope_key(executions.get(&job), &event) {
                scopes_seen += 1;
                if scopes.get(scope)?.is_none() {
                    scopes_differ = true;
                }
            }
            events.push(event);
        }
    }
    if let Some((job, request)) = current {
        add_usage(&mut totals, jobs, executions, job, request, &events)?;
    }
    if ReadableTableMetadata::len(scopes)? != scopes_seen {
        scopes_differ = true;
    }
    Ok((totals, scopes_differ))
}

/// Adds one request's sample, per the shared Token rule, to the usage
/// totals: an evidence error drops only this request, logged by the rule.
fn add_usage(
    totals: &mut BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue>,
    jobs: &BTreeMap<u128, JobRow>,
    executions: &BTreeMap<u128, FrozenExecutionConfig>,
    job: u128,
    request: u32,
    events: &[crate::persistence::RequestEventRecord],
) -> Result<(), StorageError> {
    let Some(job_row) = jobs.get(&job) else {
        return Ok(());
    };
    let Some(execution) = executions.get(&job) else {
        return Ok(());
    };
    let Some(request) = RequestNumber::new(request) else {
        return Ok(());
    };
    let target = Some(LlmTarget {
        llm_profile: execution.llm.model.id.into(),
        frozen: execution.llm.clone(),
    });
    let Some(sample) = observations::usage_sample_of(JobId::new(job), request, &target, events)
    else {
        return Ok(());
    };
    let hour = sample
        .observed_at
        .as_unix_micros()
        .div_euclid(crate::persistence::USAGE_SUMMARY_HOUR_MICROS);
    let key = (
        hour,
        usage_agent(
            job_row,
            job_row.parent_id.and_then(|parent| jobs.get(&parent.get())),
        ),
        execution.llm.provider.id.get(),
        *ContentDigest::of(execution.llm.config.model.as_bytes()).as_bytes(),
    );
    let entry = totals.entry(key).or_insert((0, 0, 0, 0, 0, 0, 0));
    let complete = u64::from(sample.usage == crate::token_usage::TokenUsageState::Complete);
    entry.0 = entry.0.saturating_add(1);
    entry.1 = entry.1.saturating_add(complete);
    entry.2 = entry
        .2
        .saturating_add(u64::from(sample.input.value().unwrap_or(0)));
    entry.3 = entry
        .3
        .saturating_add(u64::from(sample.output.value().unwrap_or(0)));
    entry.4 = entry
        .4
        .saturating_add(u64::from(sample.cache_read.value().unwrap_or(0)));
    entry.5 = entry
        .5
        .saturating_add(u64::from(sample.cache_write.value().unwrap_or(0)));
    entry.6 = entry
        .6
        .saturating_add(u64::from(sample.reasoning.value().unwrap_or(0)));
    Ok(())
}

/// Computes every derived table. `clock` is the promotion clock, which splits
/// the Jobs without an open dependency between `ready` and `delayed`.
fn derive(auth: &Authoritative, clock: i64) -> Result<Derived, StorageError> {
    let mut derived = Derived::default();
    // Children have larger ids than their parents, so a descending pass knows
    // whether every child has settled before it reaches the parent.
    let mut open_children: HashMap<u128, u32> = HashMap::new();
    let mut settled: HashSet<u128> = HashSet::new();
    for (id, row) in auth.jobs.iter().rev() {
        let state = auth
            .states
            .get(id)
            .ok_or_else(|| StorageError::corrupted(format!("job:{id} has no job_state")))?;
        if row.kind.is_stored()
            && state.claim.is_some()
            && !state.locally_stopped()
            && let Some(conversation) = row.parent_id
        {
            derived.active_turns.insert(conversation.get(), *id);
        }
        if state.locally_stopped() && open_children.get(id).copied().unwrap_or(0) == 0 {
            settled.insert(*id);
        } else if let Some(parent) = row.parent_id {
            *open_children.entry(parent.get()).or_default() += 1;
        }
    }
    for (id, row) in &auth.jobs {
        let state = &auth.states[id];
        let parent = row.parent_id.map_or(0, JobId::get);
        derived.children.insert((parent, *id), ());
        for attachment in row.kind.attachments() {
            derived.attachment_refs.insert((attachment.get(), *id), ());
        }
        if let Some(previous) = row.kind.previous() {
            derived.successors.insert(previous.get(), *id);
        }
        if row.kind.is_conversation() {
            if let Some(parent) = row.parent_id {
                derived.child_conversations.insert((parent.get(), *id), ());
                if !settled.contains(id) {
                    derived.live_children.insert((parent.get(), *id), ());
                }
            }
        } else if row.kind.is_stored()
            && let Some(conversation) = row.parent_id
        {
            derived.tips.insert(conversation.get(), *id);
            if let Some(claim) = &state.claim {
                derived
                    .notice_read
                    .entry(conversation.get())
                    .and_modify(|seq| *seq = (*seq).max(claim.notice_seq))
                    .or_insert(claim.notice_seq);
            }
        }
        if row.kind.is_stored()
            && let Some(conversation) = row.parent_id
            && let Some(outcome) = &state.outcome
        {
            let tips = derived
                .answer_tips
                .entry(conversation.get())
                .or_insert((0, 0));
            if outcome.kind != OutcomeKind::Cancelled {
                tips.0 = *id;
            }
            if auth.result_turns.contains(id) {
                tips.1 = *id;
            }
            if *tips == (0, 0) {
                derived.answer_tips.remove(&conversation.get());
            }
        }
        let pending =
            !row.kind.is_conversation() && state.claim.is_none() && state.outcome.is_none();
        let mut open_dependencies = 0u32;
        if pending {
            derived.pending.insert(*id, ());
            for (dependency, condition) in row.kind.waits_on() {
                if !settled.contains(&dependency.get()) {
                    open_dependencies += 1;
                    derived
                        .waiters
                        .insert((dependency.get(), *id), condition.code());
                }
            }
            if open_dependencies == 0
                && !row.parent_id.is_some_and(|parent| {
                    auth.held_conversations.contains(&parent.get())
                        || derived
                            .active_turns
                            .get(&parent.get())
                            .is_some_and(|active| *active != *id)
                })
            {
                match row.kind.eligible_at().map(Timestamp::as_unix_micros) {
                    Some(eligible_at) if eligible_at > clock => {
                        derived.delayed.insert((eligible_at, *id), ());
                    }
                    _ => {
                        derived.ready.insert(
                            (
                                transactions::execution_requirement(&auth.executions[id]),
                                *id,
                            ),
                            (),
                        );
                    }
                }
            }
        }
        if !settled.contains(id) {
            derived.unsettled.insert(
                *id,
                (
                    open_dependencies,
                    open_children.get(id).copied().unwrap_or(0),
                ),
            );
        }
        if let Some(claim) = &state.claim {
            let holds = match &state.outcome {
                None => true,
                Some(outcome) => {
                    outcome.kind == OutcomeKind::Unknown && state.stop_confirmed_at.is_none()
                }
            };
            if holds {
                derived.held.insert(
                    (claim.pool_id.get(), *id),
                    (claim.epoch, state.outcome.is_some()),
                );
            }
        }
    }
    for (conversation, tip) in &derived.tips {
        if auth.jobs[conversation].parent_id.is_none()
            && auth
                .conversations
                .get(conversation)
                .is_some_and(|row| row.hidden_at.is_none())
        {
            derived.recent.insert((*tip, *conversation), ());
        }
    }
    for key in auth.attachment_refs.keys() {
        derived.attachment_refs.insert(*key, ());
    }
    for (conversation, at) in &auth.user_entry_at {
        derived.user_entry_at.entry(*conversation).or_insert(*at);
    }
    for (key, by_agent) in &auth.retries {
        derived.retries.insert(*key, *by_agent);
    }
    for (job, id, request, index) in &auth.tool_call_ids {
        derived
            .tool_call_ids
            .insert((*job, id.clone()), (*request, *index));
    }
    derived.tool_totals = auth.tool_totals.clone();
    derived.tool_stats = auth.tool_stats.clone();
    derived.usage_totals = auth.usage_totals.clone();
    Ok(derived)
}

/// Replaces the contents of a table with `expected` when they differ, and
/// says whether they did.
macro_rules! sync {
    ($tx:expr, $definition:expr, $expected:expr) => {{
        let mut current = BTreeMap::new();
        for entry in $tx.open_table($definition)?.iter()? {
            let (key, value) = entry?;
            current.insert(key.value(), value.value());
        }
        let differs = current != $expected;
        if differs {
            $tx.delete_table($definition)?;
            let mut table = $tx.open_table($definition)?;
            for (key, value) in &$expected {
                table.insert(*key, *value)?;
            }
        }
        differs
    }};
}

/// Rebuilds the derived tables from the authoritative ones, as an open after
/// an unclean shutdown does. The readiness tables are rebuilt without comment;
/// a difference in any other derived table is logged, since only a fault can
/// cause it. Returns the names of the tables that were changed.
pub(super) fn rebuild(
    tx: &WriteTransaction,
    clock: i64,
) -> Result<Vec<&'static str>, StorageError> {
    let auth = read_authoritative(tx)?;
    let derived = derive(&auth, clock)?;
    let mut changed = Vec::new();
    let mut note = |name: &'static str, differs: bool, verified: bool| {
        if differs {
            if verified {
                log::warn!("derived table {name} differed from its definition and was rebuilt");
            }
            changed.push(name);
        }
    };
    let mut open_approvals = BTreeMap::new();
    let mut approval_calls = BTreeMap::new();
    for entry in tx.open_table(TOOL_CALLS)?.iter()? {
        let (key, bytes) = entry?;
        let call = decode_tool_evidence(tool_call_ref(key.value())?, bytes.value())?;
        if let Some(original) = call.approval
            && approval_calls
                .insert(tool_call_key(original), key.value())
                .is_some()
        {
            return Err(StorageError::corrupted("approval executed twice"));
        }
        if matches!(
            call.decision.fact,
            crate::persistence::ToolDecision::AwaitingApproval(_)
                | crate::persistence::ToolDecision::AwaitingAnswer(_)
        ) {
            let conversation = auth
                .jobs
                .get(&call.call.job.get())
                .and_then(|row| row.parent_id)
                .ok_or_else(|| StorageError::corrupted("awaiting call has no conversation"))?;
            let decision = tx
                .open_table(APPROVALS)?
                .get(key.value())?
                .map(|v| ApprovalRecord::decode(v.value()))
                .transpose()?;
            match decision.map(|r| r.decision) {
                None => {
                    open_approvals.insert(approvals::open_key(conversation, call.call), 0);
                }
                Some(ApprovalDecision::Approved) => {
                    open_approvals.insert(approvals::open_key(conversation, call.call), 1);
                }
                Some(
                    ApprovalDecision::Rejected
                    | ApprovalDecision::Answered { .. }
                    | ApprovalDecision::Voided,
                ) => {}
            }
        }
    }
    for approval in approval_calls.keys() {
        let conversation = auth
            .jobs
            .get(&approval.0)
            .and_then(|row| row.parent_id)
            .ok_or_else(|| StorageError::corrupted("approval has no conversation"))?;
        open_approvals.remove(&(conversation.get(), approval.0, approval.1, approval.2));
    }
    note(
        "open_approval",
        sync!(tx, OPEN_APPROVALS, open_approvals),
        true,
    );
    note(
        "approval_call",
        sync!(tx, APPROVAL_CALLS, approval_calls),
        true,
    );
    note("pending", sync!(tx, PENDING, derived.pending), false);
    note("ready", sync!(tx, READY, derived.ready), false);
    note("delayed", sync!(tx, DELAYED, derived.delayed), false);
    note(
        "dependency_waiter",
        sync!(tx, DEPENDENCY_WAITERS, derived.waiters),
        false,
    );
    note("unsettled", sync!(tx, UNSETTLED, derived.unsettled), false);
    note("held_slot", sync!(tx, HELD_SLOTS, derived.held), true);
    note("children", sync!(tx, CHILDREN, derived.children), true);
    note("successor", sync!(tx, SUCCESSORS, derived.successors), true);
    note("tip", sync!(tx, TIPS, derived.tips), true);
    note("recent", sync!(tx, RECENT, derived.recent), true);
    note("conversation_search", sync_search(tx, &auth)?, true);
    note(
        "child_conversation",
        sync!(tx, CHILD_CONVERSATIONS, derived.child_conversations),
        true,
    );
    note(
        "live_child",
        sync!(tx, LIVE_CHILDREN, derived.live_children),
        true,
    );
    note(
        "user_entry_at",
        sync!(tx, USER_ENTRY_AT, derived.user_entry_at),
        true,
    );
    note(
        "notice_read",
        sync!(tx, NOTICE_READ, derived.notice_read),
        true,
    );
    note(
        "answer_tip",
        sync!(tx, ANSWER_TIPS, derived.answer_tips),
        true,
    );
    note(
        "active_turn",
        sync!(tx, ACTIVE_TURNS, derived.active_turns),
        true,
    );
    note("retry", sync!(tx, RETRIES, derived.retries), true);
    let mut recurrence_due = BTreeMap::new();
    for entry in tx.open_table(RECURRENCES)?.iter()? {
        let (id, bytes) = entry?;
        if let RecurrencePhase::WaitingDue { due } = Recurrence::decode(bytes.value())?.phase {
            recurrence_due.insert((due, id.value()), ());
        }
    }
    note(
        "recurrence_due",
        sync!(tx, RECURRENCE_DUE, recurrence_due),
        true,
    );
    note(
        "attachment_ref",
        sync!(tx, ATTACHMENT_REFS, derived.attachment_refs),
        true,
    );
    note(
        "tool_total",
        sync!(tx, TOOL_TOTALS, derived.tool_totals),
        true,
    );
    // The tool call ids carry text, so they are compared as owned keys.
    let mut current = BTreeMap::new();
    for entry in tx.open_table(TOOL_CALL_IDS)?.iter()? {
        let (key, value) = entry?;
        let (job, id) = key.value();
        current.insert((job, id.to_owned()), value.value());
    }
    let differs = current != derived.tool_call_ids;
    if differs {
        tx.delete_table(TOOL_CALL_IDS)?;
        let mut table = tx.open_table(TOOL_CALL_IDS)?;
        for ((job, id), value) in &derived.tool_call_ids {
            table.insert((*job, id.as_str()), *value)?;
        }
    }
    note("tool_call_id", differs, true);
    let mut current_tool_stats = BTreeMap::new();
    for entry in tx.open_table(TOOL_STATS)?.iter()? {
        let (key, value) = entry?;
        current_tool_stats.insert(key.value().to_owned(), value.value());
    }
    let tool_stats_differs = current_tool_stats != derived.tool_stats;
    if tool_stats_differs {
        tx.delete_table(TOOL_STATS)?;
        let mut table = tx.open_table(TOOL_STATS)?;
        for (name, value) in &derived.tool_stats {
            table.insert(name.as_str(), *value)?;
        }
    }
    note("tool_stat", tool_stats_differs, true);
    let mut current_usage_totals = BTreeMap::new();
    for entry in tx.open_table(USAGE_TOTALS)?.iter()? {
        let (key, value) = entry?;
        current_usage_totals.insert(key.value(), value.value());
    }
    let usage_totals_differs = current_usage_totals != derived.usage_totals;
    if usage_totals_differs {
        tx.delete_table(USAGE_TOTALS)?;
        let mut table = tx.open_table(USAGE_TOTALS)?;
        for (key, value) in &derived.usage_totals {
            table.insert(*key, *value)?;
        }
    }
    note("usage_total", usage_totals_differs, true);
    if auth.request_scopes_differ {
        tx.delete_table(REQUEST_SCOPES)?;
        let mut scopes = tx.open_table(REQUEST_SCOPES)?;
        for entry in tx.open_table(REQUEST_EVENTS)?.iter()? {
            let (key, bytes) = entry?;
            let (job, request, seq) = key.value();
            if let Some(request) = RequestNumber::new(request) {
                let event =
                    rows::decode_request_event(JobId::new(job), request, seq, bytes.value())?;
                if let Some(scope) = request_scope_key(auth.executions.get(&job), &event) {
                    scopes.insert(scope, ())?;
                }
            }
        }
        note("request_scope", true, true);
    }
    Ok(changed)
}

/// The tables whose rows differ from their definitions in a database, for
/// tests that corrupt one and for the verification they stand for.
#[cfg(test)]
pub(crate) fn inconsistencies(database: &Database) -> Result<Vec<&'static str>, StorageError> {
    let tx = database.begin_write()?;
    let changed = rebuild(&tx, open::promotion_clock())?;
    tx.abort()?;
    Ok(changed)
}

/// One line per derived table that differs from its definition.
#[cfg(test)]
pub(crate) fn relationship_inconsistencies(
    database: &Database,
) -> Result<Vec<String>, StorageError> {
    Ok(inconsistencies(database)?
        .into_iter()
        .map(|table| format!("{table} differs from its definition"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::data_directory;

    #[test]
    fn tool_stats_rebuild_detects_difference() {
        let (_data, directory) = data_directory("derived", "tool-stats-rebuild");
        let opened =
            crate::storage::open(directory.database_path()).unwrap_or_else(|_| panic!("open"));

        // Corrupt TOOL_STATS by adding an extra entry
        let tx = opened.database.begin_write().expect("tx");
        {
            let mut table = tx.open_table(TOOL_STATS).expect("open");
            table
                .insert("phantom_tool", (1, 0, 0, 0, 0, 0))
                .expect("insert");
        }
        tx.commit().expect("commit");

        let problems = relationship_inconsistencies(&opened.database).expect("inconsistencies");
        assert!(
            problems.iter().any(|p| p.contains("tool_stat differs")),
            "expected tool_stat difference, got: {problems:?}"
        );

        // Now run rebuild
        let tx = opened.database.begin_write().expect("tx");
        let changed = rebuild(&tx, open::promotion_clock()).expect("rebuild");
        assert!(changed.contains(&"tool_stat"));
        tx.commit().expect("commit");

        // After rebuild, clean
        let problems_after = relationship_inconsistencies(&opened.database).expect("clean");
        assert!(
            problems_after.is_empty(),
            "expected clean, got: {problems_after:?}"
        );
    }

    #[test]
    fn usage_totals_rebuild_detects_difference() {
        let (_data, directory) = data_directory("derived", "usage-totals-rebuild");
        let opened =
            crate::storage::open(directory.database_path()).unwrap_or_else(|_| panic!("open"));

        // Corrupt USAGE_TOTALS by adding an extra entry
        let tx = opened.database.begin_write().expect("tx");
        {
            let mut table = tx.open_table(USAGE_TOTALS).expect("open");
            table
                .insert((1, 1, 1, [0u8; 32]), (1, 1, 10, 20, 0, 0, 0))
                .expect("insert");
        }
        tx.commit().expect("commit");

        let problems = relationship_inconsistencies(&opened.database).expect("inconsistencies");
        assert!(
            problems.iter().any(|p| p.contains("usage_total differs")),
            "expected usage_total difference, got: {problems:?}"
        );

        // Now run rebuild
        let tx = opened.database.begin_write().expect("tx");
        let changed = rebuild(&tx, open::promotion_clock()).expect("rebuild");
        assert!(changed.contains(&"usage_total"));
        tx.commit().expect("commit");

        // After rebuild, clean
        let problems_after = relationship_inconsistencies(&opened.database).expect("clean");
        assert!(
            problems_after.is_empty(),
            "expected clean, got: {problems_after:?}"
        );
    }

    #[test]
    fn request_scope_rebuild_detects_difference() {
        let (_data, directory) = data_directory("derived", "request-scope-rebuild");
        let opened =
            crate::storage::open(directory.database_path()).unwrap_or_else(|_| panic!("open"));

        // Corrupt REQUEST_SCOPES by adding an extra entry
        let tx = opened.database.begin_write().expect("tx");
        {
            let mut table = tx.open_table(REQUEST_SCOPES).expect("open");
            table
                .insert((1, [0u8; 32], 1, 1, 1, 1), ())
                .expect("insert");
        }
        tx.commit().expect("commit");

        let problems = relationship_inconsistencies(&opened.database).expect("inconsistencies");
        assert!(
            problems.iter().any(|p| p.contains("request_scope differs")),
            "expected request_scope difference, got: {problems:?}"
        );

        // Now run rebuild
        let tx = opened.database.begin_write().expect("tx");
        let changed = rebuild(&tx, open::promotion_clock()).expect("rebuild");
        assert!(changed.contains(&"request_scope"));
        tx.commit().expect("commit");

        // After rebuild, clean
        let problems_after = relationship_inconsistencies(&opened.database).expect("clean");
        assert!(
            problems_after.is_empty(),
            "expected clean, got: {problems_after:?}"
        );
    }

    fn provision(
        database: &crate::database::DatabaseHandle,
    ) -> (
        crate::persistence::ExecutionPoolId,
        crate::persistence::ProfileId,
    ) {
        use crate::persistence::*;
        use crate::test_support::wait;
        use std::collections::BTreeSet;
        use std::num::NonZeroU32;
        let connection = wait(database.create_config(NewConfig {
            name: "conn".into(),
            body: ConfigBody::Connection(ConnectionConfig {
                protocol: LlmTransport::OpenAiChatCompletions,
                endpoint: None,
                credential_ref: None,
            }),
        }))
        .unwrap();
        let llm = wait(database.create_config(NewConfig {
            name: "model".into(),
            body: ConfigBody::Model(ModelConfig {
                connection_id: connection,
                model: "m-1".into(),
                context_window: None,
                parameters: Default::default(),
                declared_capabilities: Default::default(),
            }),
        }))
        .unwrap();
        let pool = wait(database.create_execution_pool(NewExecutionPool {
            name: "pool".into(),
            config: ExecutionPoolConfig {
                concurrency_limit: NonZeroU32::new(16).unwrap(),
                capabilities: CapabilitySet {
                    operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
                    llm_protocols: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
                },
            },
            enabled: true,
        }))
        .unwrap();
        (pool, llm.into())
    }

    #[test]
    fn tool_stats_counter_lifecycle_and_rebuild_clean() {
        use crate::database::DatabaseWorker;
        use crate::persistence::*;
        use crate::test_support::wait;
        use std::collections::BTreeSet;

        let (_data, directory) = data_directory("derived", "tool-counter-lifecycle");
        let worker = DatabaseWorker::start(&directory).expect("worker");
        let database = worker.handle();
        let (pool, llm) = provision(&database);
        let job = wait(database.create_job(NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: database.genesis(),
            project: None,
            input: JobInputDraft::default(),
            configuration: Some(ExecutionConfiguration {
                agent_profile: None,
                llm_profile: llm,
            }),
            previous: None,
        }))
        .unwrap();
        wait(database.claim_execution(ClaimRequest {
            job,
            allowed_pools: BTreeSet::from([pool]),
        }))
        .unwrap()
        .unwrap();

        let request = RequestNumber::FIRST;
        wait(database.record_metrics(NewMetrics {
            job,
            request,
            observed_at: Timestamp::now(),
            measurements: Measurements {
                ttfb_ms: Some(1),
                ..Measurements::default()
            },
            token: None,
        }))
        .unwrap();

        let admission = ToolAdmission {
            handler_version: "workspace_run_command-6".into(),
            effect: ToolEffect::ReadOnly,
            parameters: "{}".into(),
            arguments: ToolPayload::new("{}"),
            authorization: AuthorizationBasis::RegistryGrant,
        };

        // Call 1: Admitted -> Completed
        let call1 = ToolCallRef {
            job,
            request,
            index: 0,
        };
        wait(database.record_tool_decision(NewToolEvidence {
            approval: None,
            call: call1,
            tool_call_id: "c-1".into(),
            name: "tool_a".into(),
            spawns: false,
            decision: ToolDecision::Admitted(admission.clone()),
        }))
        .unwrap();
        wait(database.record_tool_outcome(
            call1,
            ToolOutcome::Completed {
                result: ToolPayload::new("{}"),
            },
        ))
        .unwrap();

        // Call 2: Admitted -> NotApplied
        let call2 = ToolCallRef {
            job,
            request,
            index: 1,
        };
        wait(database.record_tool_decision(NewToolEvidence {
            approval: None,
            call: call2,
            tool_call_id: "c-2".into(),
            name: "tool_a".into(),
            spawns: false,
            decision: ToolDecision::Admitted(admission),
        }))
        .unwrap();
        wait(database.record_tool_outcome(
            call2,
            ToolOutcome::NotApplied {
                message: "skipped".into(),
            },
        ))
        .unwrap();

        // Call 3: Refused (InvalidArguments)
        let call3 = ToolCallRef {
            job,
            request,
            index: 2,
        };
        wait(database.record_tool_decision(NewToolEvidence {
            approval: None,
            call: call3,
            tool_call_id: "c-3".into(),
            name: "tool_a".into(),
            spawns: false,
            decision: ToolDecision::Refused(crate::storage::ToolCallRefusal::InvalidArguments),
        }))
        .unwrap();

        let stats = wait(database.tool_stats()).unwrap();
        let stat = stats.iter().find(|s| s.name == "tool_a").expect("tool_a");
        assert_eq!(stat.admitted, 2);
        assert_eq!(stat.refused_arguments, 1);
        assert_eq!(stat.completed, 1);
        assert_eq!(stat.failed, 1);
        let open = stat.admitted - stat.completed - stat.failed;
        assert_eq!(open, 0);

        worker.shutdown().unwrap();
        let opened =
            crate::storage::open(directory.database_path()).unwrap_or_else(|_| panic!("open"));
        let inconsistencies = relationship_inconsistencies(&opened.database).unwrap();
        assert!(
            inconsistencies.is_empty(),
            "expected clean, got: {inconsistencies:?}"
        );
    }

    #[test]
    fn usage_summary_buckets_by_day_and_week() {
        use crate::database::DatabaseWorker;
        use crate::openai_compatible::NormalizedUsage;
        use crate::persistence::*;
        use crate::test_support::wait;
        use std::collections::BTreeSet;

        const H: i64 = USAGE_SUMMARY_HOUR_MICROS;
        const D: i64 = 24 * H;
        let (_data, directory) = data_directory("derived", "usage-buckets");
        let worker = DatabaseWorker::start(&directory).expect("worker");
        let database = worker.handle();
        let (pool, llm) = provision(&database);
        let job = wait(database.create_job(NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: database.genesis(),
            project: None,
            input: JobInputDraft::default(),
            configuration: Some(ExecutionConfiguration {
                agent_profile: None,
                llm_profile: llm,
            }),
            previous: None,
        }))
        .unwrap();
        wait(database.claim_execution(ClaimRequest {
            job,
            allowed_pools: BTreeSet::from([pool]),
        }))
        .unwrap()
        .unwrap();
        let second = RequestNumber::FIRST.next().unwrap();
        let third = second.next().unwrap();
        for (request, at, input) in [
            (RequestNumber::FIRST, 4 * D + 3 * H, 10),
            (second, 4 * D + 15 * H, 20),
            (third, 5 * D + 5 * H, 30),
        ] {
            wait(database.record_metrics(NewMetrics {
                job,
                request,
                observed_at: Timestamp::from_unix_micros(at),
                measurements: Measurements {
                    usage: NormalizedUsage {
                        input_tokens: Some(input),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                token: Some(crate::token_usage::TokenAnnotation::eligible(
                    crate::token_usage::UsageFinality::Final,
                )),
            }))
            .unwrap();
        }
        for (width, offset_minutes, expected) in [
            (UsageBucketWidth::Day, 0, vec![(4 * D, 30), (5 * D, 30)]),
            (
                UsageBucketWidth::Day,
                600,
                vec![(4 * D - 10 * H, 10), (5 * D - 10 * H, 50)],
            ),
            (UsageBucketWidth::Week, 0, vec![(4 * D, 60)]),
        ] {
            let bucketing = UsageBucketing {
                width,
                offset_minutes,
            };
            let summary = wait(database.usage_buckets(
                UsageSummaryFilter {
                    from_us: Some(0),
                    until_us: Some(7 * D),
                    agent: None,
                },
                bucketing,
                None,
            ))
            .unwrap();
            assert!(summary.complete);
            assert_eq!(summary.resume_after, None);
            assert_eq!(summary.from_us, bucketing.start_of(0).unwrap());
            assert_eq!(summary.until_us, bucketing.end_of(7 * D - 1).unwrap());
            assert_eq!(
                summary
                    .rows
                    .iter()
                    .map(|bucket| (bucket.bucket_us, bucket.row.input_tokens))
                    .collect::<Vec<_>>(),
                expected
            );
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn usage_totals_cumulative_then_final_and_agent_hour_filtering() {
        use crate::database::DatabaseWorker;
        use crate::openai_compatible::NormalizedUsage;
        use crate::persistence::*;
        use crate::test_support::wait;
        use std::collections::BTreeSet;

        let (_data, directory) = data_directory("derived", "usage-filtering");
        let worker = DatabaseWorker::start(&directory).expect("worker");
        let database = worker.handle();
        let (pool, llm) = provision(&database);

        let agent_1 = wait(database.create_config(NewConfig {
            name: "agent-1".into(),
            body: ConfigBody::Agent(AgentConfig {
                dispatch_to: Vec::new(),
                instructions: Some("agent 1".into()),
                behavior: AgentBehavior::Chat,
                models: vec![llm.into()],
                ..Default::default()
            }),
        }))
        .unwrap();

        let agent_2 = wait(database.create_config(NewConfig {
            name: "agent-2".into(),
            body: ConfigBody::Agent(AgentConfig {
                dispatch_to: Vec::new(),
                instructions: Some("agent 2".into()),
                behavior: AgentBehavior::Chat,
                models: vec![llm.into()],
                ..Default::default()
            }),
        }))
        .unwrap();

        let job_1 = wait(database.create_job(NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: database.genesis(),
            project: None,
            input: JobInputDraft::default(),
            configuration: Some(ExecutionConfiguration {
                agent_profile: Some(agent_1.into()),
                llm_profile: llm,
            }),
            previous: None,
        }))
        .unwrap();
        wait(database.claim_execution(ClaimRequest {
            job: job_1,
            allowed_pools: BTreeSet::from([pool]),
        }))
        .unwrap()
        .unwrap();

        let t0 = Timestamp::from_unix_micros(3_600_000_000); // Hour 1

        // 1. Cumulative metrics
        wait(database.record_metrics(NewMetrics {
            job: job_1,
            request: RequestNumber::FIRST,
            observed_at: t0,
            measurements: Measurements {
                usage: NormalizedUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(5),
                    ..Default::default()
                },
                ..Default::default()
            },
            token: Some(crate::token_usage::TokenAnnotation::eligible(
                crate::token_usage::UsageFinality::Cumulative,
            )),
        }))
        .unwrap();

        // Check summary so far: the scan finished, though the usage is not final.
        let summary =
            wait(database.usage_summary(crate::persistence::UsageSummaryFilter::default(), None))
                .unwrap();
        assert_eq!(summary.rows.len(), 1);
        assert_eq!(summary.rows[0].requests, 1);
        assert_eq!(summary.rows[0].complete_requests, 0);
        assert_eq!(summary.rows[0].input_tokens, 10);
        assert_eq!(summary.rows[0].output_tokens, 5);
        assert!(summary.complete, "complete says the scan finished");
        assert_eq!(summary.resume_after, None);

        // 2. Final metrics for the same request
        wait(database.record_metrics(NewMetrics {
            job: job_1,
            request: RequestNumber::FIRST,
            observed_at: t0,
            measurements: Measurements {
                usage: NormalizedUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(8),
                    ..Default::default()
                },
                ..Default::default()
            },
            token: Some(crate::token_usage::TokenAnnotation::eligible(
                crate::token_usage::UsageFinality::Final,
            )),
        }))
        .unwrap();

        let summary =
            wait(database.usage_summary(crate::persistence::UsageSummaryFilter::default(), None))
                .unwrap();
        assert_eq!(summary.rows.len(), 1);
        assert_eq!(summary.rows[0].requests, 1);
        assert_eq!(summary.rows[0].complete_requests, 1);
        assert_eq!(summary.rows[0].input_tokens, 10);
        assert_eq!(summary.rows[0].output_tokens, 8);
        assert!(summary.complete);

        // 3. Second request 1 hour later under agent_2
        let job_2 = wait(database.create_job(NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: database.genesis(),
            project: None,
            input: JobInputDraft::default(),
            configuration: Some(ExecutionConfiguration {
                agent_profile: Some(agent_2.into()),
                llm_profile: llm,
            }),
            previous: None,
        }))
        .unwrap();
        wait(database.claim_execution(ClaimRequest {
            job: job_2,
            allowed_pools: BTreeSet::from([pool]),
        }))
        .unwrap()
        .unwrap();

        let t1 = Timestamp::from_unix_micros(7_200_000_000); // Hour 2
        wait(database.record_metrics(NewMetrics {
            job: job_2,
            request: RequestNumber::FIRST,
            observed_at: t1,
            measurements: Measurements {
                usage: NormalizedUsage {
                    input_tokens: Some(20),
                    output_tokens: Some(15),
                    ..Default::default()
                },
                ..Default::default()
            },
            token: Some(crate::token_usage::TokenAnnotation::eligible(
                crate::token_usage::UsageFinality::Final,
            )),
        }))
        .unwrap();

        // Filter by agent_1
        let summary_agent1 = wait(database.usage_summary(
            crate::persistence::UsageSummaryFilter {
                from_us: None,
                until_us: None,
                agent: Some(agent_1.get()),
            },
            None,
        ))
        .unwrap();
        assert_eq!(summary_agent1.rows.len(), 1);
        assert_eq!(summary_agent1.rows[0].agent, agent_1.get());
        assert_eq!(summary_agent1.rows[0].input_tokens, 10);

        // Filter by agent_2
        let summary_agent2 = wait(database.usage_summary(
            crate::persistence::UsageSummaryFilter {
                from_us: None,
                until_us: None,
                agent: Some(agent_2.get()),
            },
            None,
        ))
        .unwrap();
        assert_eq!(summary_agent2.rows.len(), 1);
        assert_eq!(summary_agent2.rows[0].agent, agent_2.get());
        assert_eq!(summary_agent2.rows[0].input_tokens, 20);

        // An empty range: `until_us` is exclusive, so equal bounds cover no
        // instant and no bucket, even when the bound starts an hour.
        let summary_empty = wait(database.usage_summary(
            crate::persistence::UsageSummaryFilter {
                from_us: Some(3_600_000_000),
                until_us: Some(3_600_000_000),
                agent: None,
            },
            None,
        ))
        .unwrap();
        assert!(summary_empty.rows.is_empty());
        assert!(summary_empty.complete);
        assert_eq!(summary_empty.resume_after, None);

        // Hour 1 only: [3_600_000_000, 7_200_000_000) rounds up to a bucket
        // end exclusive of hour 2.
        let summary_hour1 = wait(database.usage_summary(
            crate::persistence::UsageSummaryFilter {
                from_us: Some(3_600_000_000),
                until_us: Some(7_200_000_000),
                agent: None,
            },
            None,
        ))
        .unwrap();
        assert_eq!(summary_hour1.rows.len(), 1);
        assert_eq!(summary_hour1.rows[0].agent, agent_1.get());

        // Hour 2 only: [7_200_000_000, 10_800_000_000).
        let summary_hour2 = wait(database.usage_summary(
            crate::persistence::UsageSummaryFilter {
                from_us: Some(7_200_000_000),
                until_us: Some(10_800_000_000),
                agent: None,
            },
            None,
        ))
        .unwrap();
        assert_eq!(summary_hour2.rows.len(), 1);
        assert_eq!(summary_hour2.rows[0].agent, agent_2.get());

        worker.shutdown().unwrap();

        let opened =
            crate::storage::open(directory.database_path()).unwrap_or_else(|_| panic!("open"));
        let problems = relationship_inconsistencies(&opened.database).unwrap();
        assert!(problems.is_empty(), "expected clean, got: {problems:?}");
    }
}
