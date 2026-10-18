//! The derived tables, computed from the authoritative ones alone.
//!
//! Each derived table is written in the same transaction as the fact it
//! derives from. After an unclean shutdown the open procedure compares every
//! one with its definition (I10) and rebuilds those that differ, logging the
//! difference. An index is never an authority.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::*;
use crate::persistence::OutcomeKind;

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
    retries: BTreeMap<(u64, u64), ()>,
    attachment_refs: BTreeMap<(u64, u64), ()>,
    job_projects: BTreeMap<(u64, u64), ()>,
}

struct Authoritative {
    jobs: BTreeMap<u64, JobRow>,
    states: BTreeMap<u64, JobStateRow>,
    tool_call_ids: Vec<(u64, String, u32, u32)>,
    members: Vec<(u64, u64)>,
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
    for entry in tx.open_table(TOOL_CALLS)?.iter()? {
        let (key, bytes) = entry?;
        let (job, request, index) = key.value();
        let record = decode_tool_evidence(tool_call_ref((job, request, index))?, bytes.value())?;
        tool_call_ids.push((job, record.tool_call_id, request, index));
    }
    let mut members = Vec::new();
    for entry in tx.open_table(PROJECT_MEMBERS)?.iter()? {
        members.push(entry?.0.value());
    }
    Ok(Authoritative {
        jobs,
        states,
        tool_call_ids,
        members,
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
    for (project, job) in &auth.members {
        derived.job_projects.insert((*job, *project), ());
    }
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
    note(
        "job_project",
        sync!(tx, JOB_PROJECTS, derived.job_projects),
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

/// Test fault: removes a Job's spec while its derived rows still name it.
#[cfg(test)]
pub(crate) fn remove_job_row(database: &Database, job: JobId) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.open_table(JOBS)?.remove(job.get())?;
    tx.commit()?;
    Ok(())
}
