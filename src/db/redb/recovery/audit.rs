use crate::db::entities::{JobId, OutcomeKind, OutcomeRecord};
use crate::db::redb::StorageError;
use crate::db::redb::execution;
use crate::db::redb::recovery::reserved::ReservedRefusal;
use crate::db::redb::schema::{
    ACTIVE_ALLOCATIONS, ALLOCATION_REQUESTS, APPROVALS, ATTACHMENTS, CHILD_GROUPS, CHILDREN,
    CONFIGS, CONTENT_RECLAIM, CONTENT_REFERENCES, CONVERSATION_CONFIG_REFS, CONVERSATION_SEARCH,
    CONVERSATIONS, CREDENTIALS, DEPENDENCY_WAITERS, DRAFT_CONTEXTS, EXECUTION_COMMITS, EXECUTORS,
    JOB_EVENTS, JOB_INPUTS, JOB_OBSERVATIONS, JOB_OUTPUTS, JOB_RUNTIME, JOBS, LIVE_CHILDREN,
    MESSAGES, META, MUTABLE_READY, OPEN_APPROVALS, PENDING, POOL_ALLOCATIONS, POOL_EXECUTORS,
    POOLS, PROJECTS, RECURRENCE_DUE, RECURRENCES, REQUEST_EVENTS, REQUEST_TIMELINE, RETRY_JOBS,
    SCHEDULED_EXTERNAL_BINDING, SCHEDULED_EXTERNAL_POLICY, SEQUENCES, SUBMISSIONS, SUCCESSORS,
    TOOL_CALL_IDS, TOOL_CALLS, TOOL_STATS, TURNS, USAGE_MODEL, USAGE_TOTALS,
};
use crate::db::redb::{rows, state};
use ::redb::{Key, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, Value};

pub(crate) fn audit_delta(detail: impl Into<String>) -> ReservedRefusal {
    ReservedRefusal::new("audit_failed", detail)
}

pub(in crate::db::redb) fn verify_identical_table<K, V>(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    definition: TableDefinition<'_, K, V>,
) -> Result<(), ReservedRefusal>
where
    K: Key + 'static,
    V: Value + 'static,
{
    use ::redb::TableHandle;

    let name = definition.name().to_owned();
    let source = source_tx
        .open_table(definition)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(definition)
        .map_err(StorageError::from)?;
    let mut source_rows = source.iter().map_err(StorageError::from)?;
    let mut published_rows = published.iter().map_err(StorageError::from)?;
    loop {
        let source_row = source_rows.next().transpose().map_err(StorageError::from)?;
        let published_row = published_rows
            .next()
            .transpose()
            .map_err(StorageError::from)?;
        match (source_row, published_row) {
            (None, None) => return Ok(()),
            (Some((source_key, source_value)), Some((published_key, published_value))) => {
                if K::as_bytes(&source_key.value()).as_ref()
                    != K::as_bytes(&published_key.value()).as_ref()
                    || V::as_bytes(&source_value.value()).as_ref()
                        != V::as_bytes(&published_value.value()).as_ref()
                {
                    return Err(audit_delta(format!(
                        "publication changed preserved table {name}"
                    )));
                }
            }
            _ => {
                return Err(audit_delta(format!(
                    "publication changed the row count of preserved table {name}"
                )));
            }
        }
    }
}

pub(crate) fn verify_unrelated_recovery_rows(
    source: &impl ReadableDatabase,
    published: &impl ReadableDatabase,
    targets: &std::collections::BTreeSet<u128>,
    settlement: Option<(JobId, bool)>,
) -> Result<(), ReservedRefusal> {
    use ::redb::{MultimapTableHandle, TableHandle};

    let source_tx = source.begin_read().map_err(StorageError::from)?;
    let published_tx = published.begin_read().map_err(StorageError::from)?;
    let table_names = |tx: &ReadTransaction| -> Result<Vec<String>, ReservedRefusal> {
        let mut names = tx
            .list_tables()
            .map_err(StorageError::from)?
            .map(|table| table.name().to_owned())
            .collect::<Vec<_>>();
        names.extend(
            tx.list_multimap_tables()
                .map_err(StorageError::from)?
                .map(|table| format!("multimap:{}", table.name())),
        );
        Ok(names)
    };
    if table_names(&source_tx)? != table_names(&published_tx)? {
        return Err(audit_delta("publication changed the database table set"));
    }
    if source_tx
        .list_multimap_tables()
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        return Err(audit_delta(
            "the publication contains a legacy or multimap table outside this format",
        ));
    }

    macro_rules! unchanged {
        ($($table:ident),+ $(,)?) => {
            $(verify_identical_table(&source_tx, &published_tx, $table)?;)+
        };
    }
    unchanged!(
        META,
        CONFIGS,
        POOLS,
        EXECUTORS,
        POOL_EXECUTORS,
        CREDENTIALS,
        JOBS,
        SUBMISSIONS,
        ATTACHMENTS,
        TOOL_CALLS,
        MESSAGES,
        REQUEST_EVENTS,
        PROJECTS,
        JOB_INPUTS,
        APPROVALS,
        RECURRENCES,
        SCHEDULED_EXTERNAL_POLICY,
        SCHEDULED_EXTERNAL_BINDING,
        JOB_OBSERVATIONS,
        CONTENT_REFERENCES,
        CONTENT_RECLAIM,
        ALLOCATION_REQUESTS,
        DRAFT_CONTEXTS,
        CONVERSATIONS,
        CONVERSATION_CONFIG_REFS,
        TURNS,
        PENDING,
        MUTABLE_READY,
        DEPENDENCY_WAITERS,
        CHILDREN,
        SUCCESSORS,
        CONVERSATION_SEARCH,
        CHILD_GROUPS,
        LIVE_CHILDREN,
        TOOL_CALL_IDS,
        TOOL_STATS,
        USAGE_TOTALS,
        RETRY_JOBS,
        USAGE_MODEL,
        REQUEST_TIMELINE,
        OPEN_APPROVALS,
        RECURRENCE_DUE,
    );
    let mut history_targets = targets.clone();
    if let Some((parent, true)) = settlement {
        history_targets.insert(parent.get());
    }
    verify_execution_publication_delta(&source_tx, &published_tx, &history_targets)?;
    for definition in [POOL_ALLOCATIONS, JOB_RUNTIME] {
        let source_table = source_tx
            .open_table(definition)
            .map_err(StorageError::from)?;
        let published_table = published_tx
            .open_table(definition)
            .map_err(StorageError::from)?;
        let mut source_count = 0u64;
        for entry in source_table.iter().map_err(StorageError::from)? {
            let (key, value) = entry.map_err(StorageError::from)?;
            source_count += 1;
            if targets.contains(&key.value())
                || (definition.name() == JOB_RUNTIME.name()
                    && settlement.map(|(parent, _)| parent.get()) == Some(key.value()))
            {
                continue;
            }
            let current = published_table
                .get(key.value())
                .map_err(StorageError::from)?;
            if current.as_ref().map(|row| row.value()) != Some(value.value()) {
                return Err(audit_delta(
                    "an unrelated Allocation or Job state differs from the rollback source",
                ));
            }
        }
        let mut published_count = 0u64;
        for entry in published_table.iter().map_err(StorageError::from)? {
            let (key, _) = entry.map_err(StorageError::from)?;
            published_count += 1;
            if !targets.contains(&key.value())
                && source_table
                    .get(key.value())
                    .map_err(StorageError::from)?
                    .is_none()
            {
                return Err(audit_delta(
                    "publication introduced an unrelated Allocation or Job state",
                ));
            }
        }
        if source_count != published_count {
            return Err(audit_delta(
                "publication changed the number of Allocation or Job state rows",
            ));
        }
    }
    Ok(())
}

fn verify_execution_publication_delta(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    targets: &std::collections::BTreeSet<u128>,
) -> Result<(), ReservedRefusal> {
    use crate::db::entities::{AllocationPhase, ExecutionEventKind};

    let previous = source_tx
        .open_table(SEQUENCES)
        .map_err(StorageError::from)?
        .get(execution::EVENT_SEQUENCE_KEY)
        .map_err(StorageError::from)?
        .map_or(0, |row| row.value());
    let source = source_tx
        .open_table(JOB_EVENTS)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(JOB_EVENTS)
        .map_err(StorageError::from)?;
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if published
            .get(key.value())
            .map_err(StorageError::from)?
            .as_ref()
            .map(|row| row.value())
            != Some(value.value())
        {
            return Err(audit_delta(
                "publication rewrote or removed execution history",
            ));
        }
    }
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if source
            .get(key.value())
            .map_err(StorageError::from)?
            .is_some()
        {
            continue;
        }
        let event = execution::decode_event(key.value(), value.value())?;
        if u128::from(event.seq) <= previous || !targets.contains(&event.job.get()) {
            return Err(audit_delta(
                "publication introduced an unrelated execution event",
            ));
        }
        let allowed = match event.kind {
            ExecutionEventKind::AllocationPhaseChanged { phase } => {
                phase == AllocationPhase::Released
            }
            ExecutionEventKind::JobOutcome { outcome } => {
                matches!(outcome.kind, OutcomeKind::Failed | OutcomeKind::Unknown)
            }
            ExecutionEventKind::AllocationStopConfirmed { .. }
            | ExecutionEventKind::Closing { .. }
            | ExecutionEventKind::OutputRecorded { .. } => true,
            ExecutionEventKind::Created { .. }
            | ExecutionEventKind::PhaseChanged { .. }
            | ExecutionEventKind::InputRevisionRecorded { .. }
            | ExecutionEventKind::AllocationReserved { .. }
            | ExecutionEventKind::AllocationStarted
            | ExecutionEventKind::ClosingRunningChanged { .. }
            | ExecutionEventKind::HoldChanged { .. }
            | ExecutionEventKind::VisibilityChanged { .. }
            | ExecutionEventKind::PrerequisitesChanged { .. }
            | ExecutionEventKind::DependencyAdded { .. }
            | ExecutionEventKind::DependencyResolved { .. }
            | ExecutionEventKind::RequestRecorded { .. }
            | ExecutionEventKind::ToolRecorded { .. } => false,
        };
        if !allowed {
            return Err(audit_delta(
                "publication introduced an unauthorized execution transition",
            ));
        }
    }
    let source = source_tx
        .open_table(EXECUTION_COMMITS)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(EXECUTION_COMMITS)
        .map_err(StorageError::from)?;
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if published
            .get(key.value())
            .map_err(StorageError::from)?
            .map(|row| row.value())
            != Some(value.value())
        {
            return Err(audit_delta(
                "publication rewrote a historical execution commit",
            ));
        }
    }
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if source
            .get(key.value())
            .map_err(StorageError::from)?
            .is_none()
            && u128::from(key.value()) <= previous
        {
            return Err(audit_delta(
                "publication inserted an execution commit into old history",
            ));
        }
    }
    Ok(())
}

fn verify_recovery_sequences(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    entity: Option<u128>,
) -> Result<(), ReservedRefusal> {
    let source = source_tx
        .open_table(SEQUENCES)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(SEQUENCES)
        .map_err(StorageError::from)?;
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        let current = published
            .get(key.value())
            .map_err(StorageError::from)?
            .map(|row| row.value());
        if key.value() == execution::EVENT_SEQUENCE_KEY {
            if current.is_none_or(|current| current < value.value()) {
                return Err(audit_delta(
                    "publication regressed the execution event sequence",
                ));
            }
            continue;
        }
        let expected = if key.value() == "entity" {
            entity.unwrap_or(value.value())
        } else {
            value.value()
        };
        if current != Some(expected) {
            return Err(audit_delta("publication changed an unrelated sequence"));
        }
    }
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if source
            .get(key.value())
            .map_err(StorageError::from)?
            .is_none()
            && key.value() != execution::EVENT_SEQUENCE_KEY
        {
            return Err(audit_delta("publication introduced an unrelated sequence"));
        }
    }
    Ok(())
}

pub(crate) fn verify_active_allocation_delta(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    released: &std::collections::BTreeSet<(u128, u128, u128)>,
) -> Result<(), ReservedRefusal> {
    let source = source_tx
        .open_table(ACTIVE_ALLOCATIONS)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(ACTIVE_ALLOCATIONS)
        .map_err(StorageError::from)?;
    let mut removed = 0usize;
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        let current = published.get(key.value()).map_err(StorageError::from)?;
        if released.contains(&key.value()) {
            removed += 1;
            if current.is_some() {
                return Err(audit_delta(
                    "a released Allocation remains in the active index",
                ));
            }
        } else if current.is_none() {
            return Err(audit_delta(
                "publication removed an unrelated active Allocation",
            ));
        }
    }
    if removed != released.len() {
        return Err(audit_delta(
            "the rollback source lacks an authorized active Allocation",
        ));
    }
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if source
            .get(key.value())
            .map_err(StorageError::from)?
            .is_none()
        {
            return Err(audit_delta("publication introduced an active Allocation"));
        }
    }
    Ok(())
}

pub(crate) fn verify_reserved_output_delta(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    job: JobId,
) -> Result<crate::db::entities::JobOutputRecord, ReservedRefusal> {
    use crate::db::entities::{OutputKind, UnstartedCause};

    let source = source_tx
        .open_table(JOB_OUTPUTS)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(JOB_OUTPUTS)
        .map_err(StorageError::from)?;
    let next = source
        .range((job.get(), 0)..=(job.get(), u32::MAX))
        .map_err(StorageError::from)?
        .next_back()
        .transpose()
        .map_err(StorageError::from)?
        .map_or(0, |(key, _)| key.value().1)
        .checked_add(1)
        .ok_or_else(|| audit_delta("the Result output sequence is exhausted"))?;
    let output_key = (job.get(), next);
    let output_row = published
        .get(output_key)
        .map_err(StorageError::from)?
        .ok_or_else(|| audit_delta("the OfflineReconciled Result is missing"))?;
    let output = rows::decode_output(output_key, output_row.value())?;
    drop(output_row);
    if output.job != job
        || output.seq != i64::from(next)
        || output.kind != OutputKind::Result
        || output.summary.as_deref() != Some(UnstartedCause::OfflineReconciled.summary())
        || output.code.as_deref() != Some(UnstartedCause::OfflineReconciled.code())
        || output.goal_reached != Some(false)
        || output.value.is_some()
    {
        return Err(audit_delta(
            "the published output is not the exact OfflineReconciled Result",
        ));
    }
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if published
            .get(key.value())
            .map_err(StorageError::from)?
            .as_ref()
            .map(|row| row.value())
            != Some(value.value())
        {
            return Err(audit_delta("publication changed a pre-existing output"));
        }
    }
    let mut additions = 0usize;
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if source
            .get(key.value())
            .map_err(StorageError::from)?
            .is_none()
        {
            if key.value() != output_key {
                return Err(audit_delta("publication introduced an unrelated output"));
            }
            additions += 1;
        }
    }
    if additions != 1 {
        return Err(audit_delta(
            "publication did not add exactly one OfflineReconciled Result",
        ));
    }

    let source_sequences = source_tx
        .open_table(SEQUENCES)
        .map_err(StorageError::from)?;
    let published_sequences = published_tx
        .open_table(SEQUENCES)
        .map_err(StorageError::from)?;
    let previous = source_sequences
        .get("entity")
        .map_err(StorageError::from)?
        .map(|row| row.value())
        .ok_or_else(|| audit_delta("the rollback source lacks the entity sequence"))?;
    if output.id.get() <= previous
        || published_sequences
            .get("entity")
            .map_err(StorageError::from)?
            .map(|row| row.value())
            != Some(output.id.get())
    {
        return Err(audit_delta(
            "the Result identity and entity sequence are not the authorized allocation",
        ));
    }
    verify_recovery_sequences(source_tx, published_tx, Some(output.id.get()))?;
    Ok(output)
}

pub(crate) fn verify_reserved_settlement_delta(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    job: JobId,
    parent: JobId,
    standalone: bool,
    outcome: &OutcomeRecord,
) -> Result<(), ReservedRefusal> {
    if state::pending(source_tx, job)? != Some((0, 0))
        || state::pending(published_tx, job)?.is_some()
    {
        return Err(audit_delta(
            "the recovered Job's pending work was not cleared exactly",
        ));
    }
    let mut expected = state::require(source_tx, parent)?;
    let (dependencies, children) = expected
        .pending
        .ok_or_else(|| audit_delta("the source parent has no pending work"))?;
    let expected_children = children
        .checked_sub(1)
        .ok_or_else(|| audit_delta("the source parent child count underflows"))?;
    expected.pending = Some((dependencies, expected_children));
    expected.record_result(job);
    if standalone {
        if dependencies != 0 || expected_children != 0 {
            return Err(audit_delta(
                "the standalone Decision root is not fully settled",
            ));
        }
        if expected.state.outcome.is_some() {
            return Err(audit_delta("standalone Decision root was already settled"));
        }
        expected.pending = None;
        expected.state.outcome = Some(outcome.clone());
        if expected.lifecycle.closing.is_none() {
            expected.lifecycle.closing = Some(crate::db::entities::JobClosing {
                at: outcome.at,
                by: crate::db::entities::Actor::Auto,
                running: false,
            });
        }
    }
    if state::require(published_tx, parent)? != expected {
        return Err(audit_delta(
            "the parent runtime is not the exact authorized settlement",
        ));
    }
    Ok(())
}
