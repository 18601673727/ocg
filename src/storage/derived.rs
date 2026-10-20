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
        let mut count_changed = false;
        let mut admitted_inc = 0;
        let mut refused_inc = 0;
        match &record.decision.fact {
            crate::persistence::ToolDecision::Admitted(_) => {
                admitted_inc = 1;
                count_changed = true;
            }
            crate::persistence::ToolDecision::Refused(crate::storage::ToolCallRefusal::InvalidArguments) => {
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
                | crate::storage::ToolCallRefusal::UnsupportedSchema,
            ) => {}
        }
        if count_changed || record.outcome.is_some() {
            let stats = tool_stats.entry(record.name.clone()).or_insert((0, 0, 0, 0, 0, 0));
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
    let usage_totals = usage_totals_from_events(&tx.open_table(REQUEST_EVENTS)?, &jobs)?;
    Ok(Authoritative {
        jobs,
        states,
        tool_call_ids,
        tool_totals,
        tool_stats,
        usage_totals,
    })
}

/// One ordered pass over the request events, grouped by (Job, request): the
/// events of one request are contiguous in table order, so each group is
/// reduced on its own as the pass meets its first event of the next request.
/// Nothing else is kept, so the pass holds one request's events, never the
/// whole table.
fn usage_totals_from_events(
    table: &impl ReadableTable<(u64, u32, u32), &'static [u8]>,
    jobs: &BTreeMap<u64, JobRow>,
) -> Result<BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue>, StorageError> {
    let mut totals: BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue> = BTreeMap::new();
    let mut current: Option<(u64, u32)> = None;
    let mut events: Vec<crate::persistence::RequestEventRecord> = Vec::new();
    for entry in table.iter()? {
        let (key, bytes) = entry?;
        let (job, request, seq) = key.value();
        if current != Some((job, request)) {
            if let Some((job, request)) = current {
                add_usage(&mut totals, jobs, job, request, &events)?;
            }
            events.clear();
            current = Some((job, request));
        }
        if let Some(request) = RequestNumber::new(request) {
            events.push(rows::decode_request_event(
                JobId::new(job),
                request,
                seq,
                bytes.value(),
            )?);
        }
    }
    if let Some((job, request)) = current {
        add_usage(&mut totals, jobs, job, request, &events)?;
    }
    Ok(totals)
}

/// Adds one request's sample, per the shared Token rule, to the usage
/// totals: an evidence error drops only this request, logged by the rule.
fn add_usage(
    totals: &mut BTreeMap<schema::UsageTotalKey, schema::UsageTotalValue>,
    jobs: &BTreeMap<u64, JobRow>,
    job: u64,
    request: u32,
    events: &[crate::persistence::RequestEventRecord],
) -> Result<(), StorageError> {
    let Some(job_row) = jobs.get(&job) else {
        return Ok(());
    };
    let Some(execution) = &job_row.execution else {
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
    let hour = sample.observed_at.as_unix_micros().div_euclid(crate::persistence::USAGE_SUMMARY_HOUR_MICROS);
    let key = (
        hour,
        usage_agent(job_row),
        execution.llm.provider.id.get(),
        *ContentDigest::of(execution.llm.config.model.as_bytes()).as_bytes(),
    );
    let entry = totals.entry(key).or_insert((0, 0, 0, 0, 0, 0, 0));
    let complete = u64::from(sample.usage == crate::token_usage::TokenUsageState::Complete);
    entry.0 = entry.0.saturating_add(1);
    entry.1 = entry.1.saturating_add(complete);
    entry.2 = entry.2.saturating_add(u64::from(sample.input.value().unwrap_or(0)));
    entry.3 = entry.3.saturating_add(u64::from(sample.output.value().unwrap_or(0)));
    entry.4 = entry.4.saturating_add(u64::from(sample.cache_read.value().unwrap_or(0)));
    entry.5 = entry.5.saturating_add(u64::from(sample.cache_write.value().unwrap_or(0)));
    entry.6 = entry.6.saturating_add(u64::from(sample.reasoning.value().unwrap_or(0)));
    Ok(())
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

    fn provision(database: &crate::database::DatabaseHandle) -> (crate::persistence::ExecutionPoolId, crate::persistence::ProfileId) {
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
            name: None,
            eligible_at: None,
            parent: database.genesis(),
            project: None,
            dependencies: Vec::new(),
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
        let call1 = ToolCallRef { job, request, index: 0 };
        wait(database.record_tool_decision(NewToolEvidence {
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
        let call2 = ToolCallRef { job, request, index: 1 };
        wait(database.record_tool_decision(NewToolEvidence {
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
        let call3 = ToolCallRef { job, request, index: 2 };
        wait(database.record_tool_decision(NewToolEvidence {
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
        let opened = crate::storage::open(directory.database_path())
            .unwrap_or_else(|_| panic!("open"));
        let inconsistencies = relationship_inconsistencies(&opened.database).unwrap();
        assert!(inconsistencies.is_empty(), "expected clean, got: {inconsistencies:?}");
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
                instructions: Some("agent 2".into()),
                behavior: AgentBehavior::Chat,
                models: vec![llm.into()],
                ..Default::default()
            }),
        }))
        .unwrap();

        let job_1 = wait(database.create_job(NewJob {
            name: None,
            eligible_at: None,
            parent: database.genesis(),
            project: None,
            dependencies: Vec::new(),
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
            token: Some(crate::token_usage::TokenAnnotation::eligible(crate::token_usage::UsageFinality::Cumulative)),
        }))
        .unwrap();

        // Check summary so far: the scan finished, though the usage is not final.
        let summary = wait(database.usage_summary(crate::persistence::UsageSummaryFilter::default())).unwrap();
        assert_eq!(summary.rows.len(), 1);
        assert_eq!(summary.rows[0].requests, 1);
        assert_eq!(summary.rows[0].complete_requests, 0);
        assert_eq!(summary.rows[0].input_tokens, 10);
        assert_eq!(summary.rows[0].output_tokens, 5);
        assert!(summary.complete, "complete says the scan finished");
        assert_eq!(summary.resume_from_us, None);

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
            token: Some(crate::token_usage::TokenAnnotation::eligible(crate::token_usage::UsageFinality::Final)),
        }))
        .unwrap();

        let summary = wait(database.usage_summary(crate::persistence::UsageSummaryFilter::default())).unwrap();
        assert_eq!(summary.rows.len(), 1);
        assert_eq!(summary.rows[0].requests, 1);
        assert_eq!(summary.rows[0].complete_requests, 1);
        assert_eq!(summary.rows[0].input_tokens, 10);
        assert_eq!(summary.rows[0].output_tokens, 8);
        assert!(summary.complete);

        // 3. Second request 1 hour later under agent_2
        let job_2 = wait(database.create_job(NewJob {
            name: None,
            eligible_at: None,
            parent: database.genesis(),
            project: None,
            dependencies: Vec::new(),
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
            token: Some(crate::token_usage::TokenAnnotation::eligible(crate::token_usage::UsageFinality::Final)),
        }))
        .unwrap();

        // Filter by agent_1
        let summary_agent1 = wait(database.usage_summary(crate::persistence::UsageSummaryFilter {
            from_us: None,
            until_us: None,
            agent: Some(agent_1.get()),
        }))
        .unwrap();
        assert_eq!(summary_agent1.rows.len(), 1);
        assert_eq!(summary_agent1.rows[0].agent, agent_1.get());
        assert_eq!(summary_agent1.rows[0].input_tokens, 10);

        // Filter by agent_2
        let summary_agent2 = wait(database.usage_summary(crate::persistence::UsageSummaryFilter {
            from_us: None,
            until_us: None,
            agent: Some(agent_2.get()),
        }))
        .unwrap();
        assert_eq!(summary_agent2.rows.len(), 1);
        assert_eq!(summary_agent2.rows[0].agent, agent_2.get());
        assert_eq!(summary_agent2.rows[0].input_tokens, 20);

        // An empty range: `until_us` is exclusive, so equal bounds cover no
        // instant and no bucket, even when the bound starts an hour.
        let summary_empty = wait(database.usage_summary(crate::persistence::UsageSummaryFilter {
            from_us: Some(3_600_000_000),
            until_us: Some(3_600_000_000),
            agent: None,
        }))
        .unwrap();
        assert!(summary_empty.rows.is_empty());
        assert!(summary_empty.complete);
        assert_eq!(summary_empty.resume_from_us, None);

        // Hour 1 only: [3_600_000_000, 7_200_000_000) rounds up to a bucket
        // end exclusive of hour 2.
        let summary_hour1 = wait(database.usage_summary(crate::persistence::UsageSummaryFilter {
            from_us: Some(3_600_000_000),
            until_us: Some(7_200_000_000),
            agent: None,
        }))
        .unwrap();
        assert_eq!(summary_hour1.rows.len(), 1);
        assert_eq!(summary_hour1.rows[0].agent, agent_1.get());

        // Hour 2 only: [7_200_000_000, 10_800_000_000).
        let summary_hour2 = wait(database.usage_summary(crate::persistence::UsageSummaryFilter {
            from_us: Some(7_200_000_000),
            until_us: Some(10_800_000_000),
            agent: None,
        }))
        .unwrap();
        assert_eq!(summary_hour2.rows.len(), 1);
        assert_eq!(summary_hour2.rows[0].agent, agent_2.get());

        worker.shutdown().unwrap();

        let opened = crate::storage::open(directory.database_path())
            .unwrap_or_else(|_| panic!("open"));
        let problems = relationship_inconsistencies(&opened.database).unwrap();
        assert!(problems.is_empty(), "expected clean, got: {problems:?}");
    }
}
