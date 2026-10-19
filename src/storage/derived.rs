//! The derived tables, computed from the authoritative ones alone.
//!
//! Each derived table is written in the same transaction as the fact it
//! derives from. After an unclean shutdown the open procedure compares every
//! one with its definition (I10) and rebuilds those that differ, logging the
//! difference. An index is never an authority.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::*;
use crate::persistence::{AgentBehavior, OutcomeKind};

/// What every derived table must hold, given the authoritative rows.
#[derive(Default)]
pub(super) struct Derived {
    pending: BTreeMap<u64, ()>,
    ready: BTreeMap<(u16, u64), ()>,
    delayed: BTreeMap<(i64, u64), ()>,
    waiters: BTreeMap<(u64, u64), u8>,
    unsettled: BTreeMap<u64, (u32, u32)>,
    held: BTreeMap<(u64, u64), (u64, bool)>,
    children: BTreeMap<(u64, u64), ()>,
    successors: BTreeMap<u64, u64>,
    chat_threads: BTreeMap<(u64, u64), ()>,
    tool_call_ids: BTreeMap<(u64, String), (u32, u32)>,
    tool_totals: BTreeMap<u64, (u64, u64)>,
    tool_stats: BTreeMap<String, schema::ToolStatValue>,
    usage_totals: BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue>,
    retries: BTreeMap<(u64, u64), ()>,
    attachment_refs: BTreeMap<(u64, u64), ()>,
}

struct Authoritative {
    jobs: BTreeMap<u64, JobRow>,
    states: BTreeMap<u64, JobStateRow>,
    tool_call_ids: Vec<(u64, String, u32, u32)>,
    tool_totals: BTreeMap<u64, (u64, u64)>,
    tool_stats: BTreeMap<String, schema::ToolStatValue>,
    usage_totals: BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue>,
}

fn read_authoritative(tx: &WriteTransaction) -> Result<Authoritative, StorageError> {
    let mut jobs = BTreeMap::new();
    for entry in tx.open_table(JOBS)?.iter()? {
        let (key, bytes) = entry?;
        jobs.insert(key.value(), JobRow::decode(bytes.value())?);
    }
    let mut states = BTreeMap::new();
    for entry in tx.open_table(JOB_STATES)?.iter()? {
        let (key, bytes) = entry?;
        states.insert(key.value(), JobStateRow::decode(bytes.value())?);
    }
    let mut tool_call_ids = Vec::new();
    let mut tool_totals: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    let mut tool_stats: BTreeMap<String, schema::ToolStatValue> = BTreeMap::new();
    for entry in tx.open_table(TOOL_CALLS)?.iter()? {
        let (key, bytes) = entry?;
        let (job, request, index) = key.value();
        let record = decode_tool_evidence(tool_call_ref((job, request, index))?, bytes.value())?;
        if let Some(admission) = record.admission() {
            let totals = tool_totals.entry(job).or_insert((0, 0));
            totals.0 = totals.0.saturating_add(admission.arguments.len() as u64);
            totals.1 = totals.1.saturating_add(admission.parameters.len() as u64);
        }
        let stats = tool_stats.entry(record.name.clone()).or_insert((0, 0, 0, 0, 0, 0));
        match &record.decision.fact {
            crate::persistence::ToolDecision::Admitted(_) => stats.0 = stats.0.saturating_add(1),
            crate::persistence::ToolDecision::Refused(crate::storage::ToolCallRefusal::InvalidArguments) => {
                stats.1 = stats.1.saturating_add(1);
            }
            crate::persistence::ToolDecision::Refused(
                crate::storage::ToolCallRefusal::Malformed
                | crate::storage::ToolCallRefusal::ConflictingIdentity
                | crate::storage::ToolCallRefusal::AmbiguousIdentity
                | crate::storage::ToolCallRefusal::LimitExceeded
                | crate::storage::ToolCallRefusal::Incomplete
                | crate::storage::ToolCallRefusal::Unauthorized
                | crate::storage::ToolCallRefusal::MissingSchema
                | crate::storage::ToolCallRefusal::UnsupportedSchema,
            ) => {}
        }
        if let Some(outcome) = &record.outcome {
            let duration_us = outcome
                .at
                .as_unix_micros()
                .saturating_sub(record.decision.at.as_unix_micros())
                .max(0) as u64;
            stats.4 = stats.4.saturating_add(duration_us);
            stats.5 = stats.5.max(duration_us);
            match &outcome.fact {
                crate::persistence::ToolOutcome::Completed { .. } => {
                    stats.2 = stats.2.saturating_add(1);
                }
                crate::persistence::ToolOutcome::CompletedWithheld { .. }
                | crate::persistence::ToolOutcome::NotApplied { .. }
                | crate::persistence::ToolOutcome::NotInvoked { .. }
                | crate::persistence::ToolOutcome::Uncertain { .. } => {
                    stats.3 = stats.3.saturating_add(1);
                }
            }
        }
        tool_call_ids.push((job, record.tool_call_id, request, index));
    }
    let mut usage_totals: BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue> = BTreeMap::new();
    let mut job_events_map: BTreeMap<u64, Vec<crate::persistence::RequestEventRecord>> = BTreeMap::new();
    for entry in tx.open_table(REQUEST_EVENTS)?.iter()? {
        let (key, bytes) = entry?;
        let (job, request, seq) = key.value();
        if let Some(req) = crate::persistence::RequestNumber::new(request) {
            let event = rows::decode_request_event(JobId::new(job), req, seq, bytes.value())?;
            job_events_map.entry(job).or_default().push(event);
        }
    }
    for (job_id, events) in job_events_map {
        let Some(job_row) = jobs.get(&job_id) else { continue };
        let Some(execution) = &job_row.execution else { continue };
        let agent = usage_agent(job_row);
        let provider = execution.llm.provider.id.get();
        let model_hash = *ContentDigest::of(execution.llm.config.model.as_bytes()).as_bytes();
        let target = Some(LlmTarget {
            llm_profile: execution.llm.model.id.into(),
            frozen: execution.llm.clone(),
        });
        let records = observations::metrics_of(JobId::new(job_id), &target, events);
        if let Ok(samples) = crate::token_usage::TokenRequestSample::per_request(&records) {
            for sample in samples {
                let hour = (sample.observed_at.as_unix_micros().max(0) as u64) / 3_600_000_000;
                let key = (hour, agent, provider, model_hash);
                let entry = usage_totals.entry(key).or_insert((0, 0, 0, 0, 0, 0, 0));
                let input = sample.input.value().unwrap_or(0) as u64;
                let output = sample.output.value().unwrap_or(0) as u64;
                let cache_read = sample.cache_read.value().unwrap_or(0) as u64;
                let cache_write = sample.cache_write.value().unwrap_or(0) as u64;
                let reasoning = sample.reasoning.value().unwrap_or(0) as u64;
                let total = sample.total.unwrap_or(0);
                entry.0 = entry.0.saturating_add(1);
                entry.1 = entry.1.saturating_add(input);
                entry.2 = entry.2.saturating_add(output);
                entry.3 = entry.3.saturating_add(cache_read);
                entry.4 = entry.4.saturating_add(cache_write);
                entry.5 = entry.5.saturating_add(reasoning);
                entry.6 = entry.6.saturating_add(total);
            }
        }
    }
    Ok(Authoritative {
        jobs,
        states,
        tool_call_ids,
        tool_totals,
        tool_stats,
        usage_totals,
    })
}

/// Computes every derived table. `clock` is the promotion clock, which splits
/// the Jobs without an open dependency between `ready` and `delayed`.
fn derive(auth: &Authoritative, clock: i64) -> Result<Derived, StorageError> {
    let mut derived = Derived::default();
    // Children have larger ids than their parents, so a descending pass knows
    // whether every child has settled before it reaches the parent.
    let mut open_children: HashMap<u64, u32> = HashMap::new();
    let mut settled: HashSet<u64> = HashSet::new();
    for (id, row) in auth.jobs.iter().rev() {
        let state = auth
            .states
            .get(id)
            .ok_or_else(|| StorageError::corrupted(format!("job:{id} has no job_state")))?;
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
        for attachment in &row.attachments {
            derived.attachment_refs.insert((attachment.get(), *id), ());
        }
        if let Some(original) = row.retry_of {
            derived.retries.insert((original.get(), *id), ());
        }
        if let Some(previous) = row.continues {
            derived.successors.insert(previous.get(), *id);
            if let Some(first) = auth.jobs.get(&previous.get())
                && first.continues.is_none()
                && let Some(agent) = first.agent_id
            {
                derived
                    .chat_threads
                    .insert((agent.get(), previous.get()), ());
            }
        }
        if row.continues.is_none()
            && let Some(agent) = row.agent_id
            && row
                .execution
                .as_ref()
                .and_then(|execution| execution.agent.as_ref())
                .is_some_and(|agent| agent.behavior == AgentBehavior::Chat)
        {
            derived
                .chat_threads
                .insert((agent.get(), *id), ());
        }
        let pending = state.claim.is_none() && state.outcome.is_none();
        let mut open_dependencies = 0u32;
        if pending {
            derived.pending.insert(*id, ());
            for (dependency, condition) in &row.dependencies {
                if !settled.contains(&dependency.get()) {
                    open_dependencies += 1;
                    derived
                        .waiters
                        .insert((dependency.get(), *id), condition.code());
                }
            }
            if open_dependencies == 0 {
                match row.eligible_at.map(Timestamp::as_unix_micros) {
                    Some(eligible_at) if eligible_at > clock => {
                        derived.delayed.insert((eligible_at, *id), ());
                    }
                    _ => {
                        derived
                            .ready
                            .insert((transactions::job_requirement(row), *id), ());
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
            let holds = state.suspension.is_none()
                && match &state.outcome {
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
    note(
        "chat_thread",
        sync!(tx, CHAT_THREADS, derived.chat_threads),
        true,
    );
    note("retry", sync!(tx, RETRIES, derived.retries), true);
    note(
        "attachment_ref",
        sync!(tx, ATTACHMENT_REFS, derived.attachment_refs),
        true,
    );
    note("tool_total", sync!(tx, TOOL_TOTALS, derived.tool_totals), true);
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
        let opened = crate::storage::open(directory.database_path())
            .unwrap_or_else(|_| panic!("open"));

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
        let opened = crate::storage::open(directory.database_path())
            .unwrap_or_else(|_| panic!("open"));

        // Corrupt USAGE_TOTALS by adding an extra entry
        let tx = opened.database.begin_write().expect("tx");
        {
            let mut table = tx.open_table(USAGE_TOTALS).expect("open");
            table
                .insert((1, 1, 1, [0u8; 32]), (1, 10, 20, 0, 0, 0, 30))
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
}
