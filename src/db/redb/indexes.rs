use std::collections::{BTreeMap, BTreeSet};

use super::{StorageError, TxRead, jobs};
use crate::db::entities::{
    AttachmentRecord, CompactionPolicy, ConfigId, ContentDigest, JobId, JobOutputRecord, JobSpec,
    MutableJobInput, Origin, OutcomeKind, OutputKind,
};
use crate::db::entities::{RequestEventRecord, RequestPayload};
use crate::db::redb::schema::{
    ATTACHMENTS, CHILD_GROUPS, CHILDREN, CONTENT_RECLAIM, CONTENT_REFERENCES,
    CONVERSATION_CONFIG_REFS, CONVERSATION_SEARCH, CONVERSATIONS, DRAFT_CONTEXTS, JOB_OUTPUTS,
    JOB_RUNTIME, JOBS, LIVE_CHILDREN, MUTABLE_READY, PENDING, POOL_ALLOCATIONS, SUCCESSORS,
    USAGE_MODEL,
};
use crate::db::redb::{allocations, reads, reservations, rows, schema, state};
use ::redb::{ReadTransaction, ReadableTable, ReadableTableMetadata, WriteTransaction};

#[derive(Clone, Copy)]
pub(super) enum CompactionReference {
    Summary = 1,
    Predictor = 2,
    Acceptance = 3,
}

impl CompactionReference {
    const ALL: [Self; 3] = [Self::Summary, Self::Predictor, Self::Acceptance];

    fn config(self, policy: Option<&CompactionPolicy>) -> Option<ConfigId> {
        let policy = policy?;
        match self {
            Self::Summary => policy.summarization_model,
            Self::Predictor => policy.predictive.as_ref().map(|p| p.decision_agent),
            Self::Acceptance => policy
                .predictive
                .as_ref()?
                .acceptance
                .as_ref()
                .map(|p| p.model.id),
        }
    }

    pub(super) fn matches(self, policy: &CompactionPolicy, config: ConfigId) -> bool {
        self.config(Some(policy)) == Some(config)
    }

    fn key(self, config: ConfigId, owner: JobId) -> (u8, u128, u128) {
        (self as u8, config.get(), owner.get())
    }

    pub(super) fn range(self, config: ConfigId) -> std::ops::RangeInclusive<(u8, u128, u128)> {
        (self as u8, config.get(), 0)..=(self as u8, config.get(), u128::MAX)
    }

    pub(super) fn configs(
        self,
        index: &impl ReadableTable<(u8, u128, u128), ()>,
    ) -> Result<BTreeSet<ConfigId>, StorageError> {
        let role = self as u8;
        let mut configs = BTreeSet::new();
        let mut first = 0;
        // Seek to the next ConfigId without visiting every owner of this one.
        while let Some((key, _)) = index
            .range((role, first, 0)..=(role, u128::MAX, u128::MAX))?
            .next()
            .transpose()?
        {
            let (_, config, _) = key.value();
            configs.insert(ConfigId::new(config));
            let Some(next) = config.checked_add(1) else {
                break;
            };
            first = next;
        }
        Ok(configs)
    }
}

pub(super) fn update_compaction_refs(
    tx: &WriteTransaction,
    owner: JobId,
    previous: Option<&CompactionPolicy>,
    next: Option<&CompactionPolicy>,
) -> Result<(), StorageError> {
    for role in CompactionReference::ALL {
        let old = role.config(previous);
        let new = role.config(next);
        if old == new {
            continue;
        }
        let mut index = tx.open_table(CONVERSATION_CONFIG_REFS)?;
        if let Some(config) = old
            && index.remove(role.key(config, owner))?.is_none()
        {
            return Err(StorageError::corrupted(
                "missing conversation configuration reference",
            ));
        }
        if let Some(config) = new
            && index.insert(role.key(config, owner), ())?.is_some()
        {
            return Err(StorageError::corrupted(
                "duplicate conversation configuration reference",
            ));
        }
    }
    Ok(())
}

fn visit_compaction_refs(
    tx: &impl TxRead,
    mut visit: impl FnMut(CompactionReference, ConfigId, JobId) -> Result<(), StorageError>,
) -> Result<(u64, bool), StorageError> {
    let conversations = tx.table(CONVERSATIONS)?;
    let mut count = 0;
    let mut has_unknown = false;
    for entry in tx.table(JOB_RUNTIME)?.iter()? {
        let (key, bytes) = entry?;
        let job = JobId::new(key.value());
        let runtime = state::JobRuntime::decode_at(job.get(), bytes.value())?;
        has_unknown |= runtime
            .state
            .outcome
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown);
        let policy = runtime.lifecycle.compaction_policy.as_ref();
        if policy.is_some() && conversations.get(job.get())?.is_none() {
            return Err(StorageError::corrupted(format!(
                "{job} has a policy but is not a conversation"
            )));
        }
        for role in CompactionReference::ALL {
            if let Some(config) = role.config(policy) {
                visit(role, config, job)?;
                count += 1;
            }
        }
    }
    Ok((count, has_unknown))
}

pub(super) fn verify_compaction_refs(tx: &ReadTransaction) -> Result<bool, StorageError> {
    let index = tx.open_table(CONVERSATION_CONFIG_REFS)?;
    let (expected, has_unknown) = visit_compaction_refs(tx, |role, config, job| {
        if index.get(role.key(config, job))?.is_none() {
            return Err(StorageError::corrupted(
                "missing conversation configuration reference",
            ));
        }
        Ok(())
    })?;
    if index.len()? != expected {
        return Err(StorageError::corrupted(
            "extra conversation configuration references",
        ));
    }
    Ok(has_unknown)
}

#[cfg(unix)]
pub(super) struct CompactionRefCounts {
    pub(super) expected: u64,
    pub(super) missing: u64,
    pub(super) extra: u64,
}

#[cfg(unix)]
pub(super) fn compaction_ref_rebuild_counts(
    tx: &ReadTransaction,
) -> Result<CompactionRefCounts, StorageError> {
    use crate::db::entities::ConfigKind;

    let index = tx.open_table(CONVERSATION_CONFIG_REFS)?;
    let configs = tx.open_table(schema::CONFIGS)?;
    let mut matched = 0;
    let (expected, _) = visit_compaction_refs(tx, |role, config, job| {
        let row = configs.get(config.get())?.ok_or_else(|| {
            StorageError::corrupted(format!("{job} references missing config {config}"))
        })?;
        let row = rows::ConfigRow::decode_at(config.get(), row.value())?;
        let kind_matches = match role {
            CompactionReference::Summary => row.kind == ConfigKind::Model,
            CompactionReference::Predictor => row.kind == ConfigKind::DecisionAgent,
            CompactionReference::Acceptance => {
                matches!(row.kind, ConfigKind::Model | ConfigKind::DecisionModel)
            }
        };
        if !kind_matches {
            return Err(StorageError::corrupted(format!(
                "{job} has an invalid compaction reference to config {config}"
            )));
        }
        matched += u64::from(index.get(role.key(config, job))?.is_some());
        Ok(())
    })?;
    Ok(CompactionRefCounts {
        expected,
        missing: expected - matched,
        extra: index.len()?.checked_sub(matched).ok_or_else(|| {
            StorageError::corrupted("conversation configuration reference count is inconsistent")
        })?,
    })
}

#[cfg(unix)]
pub(super) fn rebuild_compaction_refs(tx: &WriteTransaction) -> Result<(), StorageError> {
    tx.delete_table(CONVERSATION_CONFIG_REFS)?;
    let mut index = tx.open_table(CONVERSATION_CONFIG_REFS)?;
    visit_compaction_refs(tx, |role, config, job| {
        index.insert(role.key(config, job), ())?;
        Ok(())
    })?;
    Ok(())
}

pub(super) fn search_gram(chars: &[char]) -> u64 {
    chars
        .iter()
        .fold(0, |key, ch| (key << 21) | u64::from(u32::from(*ch) + 1))
}

pub(super) fn search_grams(text: &str) -> BTreeSet<u64> {
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

pub(super) fn update_mutable_turn_search(
    tx: &WriteTransaction,
    conversation: JobId,
    turn: JobId,
    old: Option<&crate::db::entities::MutableJobInput>,
    new: &crate::db::entities::MutableJobInput,
) -> Result<(), StorageError> {
    let attachments = tx.open_table(ATTACHMENTS)?;
    let grams = |input: &MutableJobInput| {
        search_input_text(&attachments, input).map(|text| search_grams(&text))
    };
    let previous = old.map(grams).transpose()?.unwrap_or_default();
    let next = grams(new)?;
    let mut index = tx.open_table(CONVERSATION_SEARCH)?;
    for gram in previous.difference(&next) {
        index.remove((conversation.get(), *gram, turn.get()))?;
    }
    for gram in next.difference(&previous) {
        index.insert((conversation.get(), *gram, turn.get()), ())?;
    }
    Ok(())
}

fn searchable_turn_context(
    tx: &WriteTransaction,
    conversation: JobId,
    turn: JobId,
) -> Result<bool, StorageError> {
    let conversations = tx.open_table(CONVERSATIONS)?;
    let jobs = tx.open_table(JOBS)?;
    let row = conversations
        .get(conversation.get())?
        .ok_or_else(|| StorageError::corrupted("search conversation missing"))?;
    let turn_row = jobs
        .get(turn.get())?
        .ok_or_else(|| StorageError::corrupted("search turn missing"))?;
    let job = jobs::decode_mutable_job(turn.get(), turn_row.value())?;
    Ok(
        searchable_conversation(&JobSpec::decode_at(conversation.get(), row.value())?)
            && job.parent_id == conversation
            && matches!(
                job.input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            ),
    )
}

fn insert_turn_search_grams(
    tx: &WriteTransaction,
    conversation: JobId,
    turn: JobId,
    grams: &BTreeSet<u64>,
) -> Result<(), StorageError> {
    let mut table = tx.open_table(CONVERSATION_SEARCH)?;
    for gram in grams {
        table.insert((conversation.get(), *gram, turn.get()), ())?;
    }
    Ok(())
}

pub(super) fn append_output_search_index(
    tx: &WriteTransaction,
    conversation: JobId,
    turn: JobId,
    output: &JobOutputRecord,
    completed: bool,
) -> Result<(), StorageError> {
    if output.kind != OutputKind::Result || !searchable_turn_context(tx, conversation, turn)? {
        return Ok(());
    }
    let answer = super::observations::answer_text(tx, turn, output)?;
    if let Some(text) = answer
        .as_deref()
        .or_else(|| search_output_text(output, completed))
    {
        insert_turn_search_grams(tx, conversation, turn, &search_grams(text))?;
    }
    Ok(())
}

pub(super) fn searchable_conversation(row: &JobSpec) -> bool {
    row.parent_id.is_none() && row.origin != Origin::System
}

pub(super) fn search_input_text(
    attachments: &impl ReadableTable<u128, &'static [u8]>,
    input: &MutableJobInput,
) -> Result<String, StorageError> {
    let mut text = input.text().to_owned();
    for id in input.attachments() {
        let bytes = attachments
            .get(id.get())?
            .ok_or_else(|| StorageError::corrupted("search attachment missing"))?;
        if let Some(name) = rows::decode_attachment(id.get(), bytes.value())?.display_name {
            text.push('\n');
            text.push_str(&name);
        }
    }
    Ok(text)
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

fn indexed_identity<'a>(
    identity: Option<&'a reads::jobs::UsageIdentity>,
    event: &RequestEventRecord,
) -> Option<&'a reads::jobs::UsageIdentity> {
    identity.filter(|_| {
        !matches!(
            event.payload,
            RequestPayload::Sent { .. } | RequestPayload::Received { .. }
        )
    })
}

pub(super) fn request_timeline_key(
    identity: Option<&reads::jobs::UsageIdentity>,
    event: &RequestEventRecord,
) -> Option<schema::RequestTimelineKey> {
    indexed_identity(identity, event).map(|_| {
        (
            event.observed_at.as_unix_micros(),
            event.job_id.get(),
            event.request.get(),
            event.seq,
        )
    })
}

pub(super) fn usage_model_key(
    identity: Option<&reads::jobs::UsageIdentity>,
    event: &RequestEventRecord,
) -> Option<(u128, [u8; 32])> {
    indexed_identity(identity, event).map(|identity| {
        (
            identity.provider,
            *ContentDigest::of(identity.model.as_bytes()).as_bytes(),
        )
    })
}

pub(super) fn record_usage_model(
    tx: &WriteTransaction,
    key: (u128, [u8; 32]),
    job: JobId,
) -> Result<(), StorageError> {
    let mut table = tx.open_table(USAGE_MODEL)?;
    let current = table.get(key)?.map(|value| value.value());
    if current.is_none_or(|current| job.get() < current) {
        table.insert(key, job.get())?;
    }
    Ok(())
}

pub(super) fn validate_native_sample(
    sample: Option<&crate::db::entities::TokenRequestSample>,
) -> Result<(), StorageError> {
    use crate::db::entities::TokenCount;
    if sample.is_some_and(|sample| {
        [
            sample.input,
            sample.output,
            sample.cache_read,
            sample.cache_write,
            sample.reasoning,
        ]
        .contains(&TokenCount::Conflicted)
    }) {
        return Err(StorageError::corrupted(
            "conflicting native final Token usage",
        ));
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

#[derive(Debug, PartialEq, Eq)]
pub(super) struct AllocationIndexes {
    pub(super) active: BTreeSet<(u128, u128, u128)>,
    pub(super) requests: std::collections::BTreeMap<u128, u128>,
}

pub(super) fn compute_allocation_indexes(
    allocations: &impl ReadableTable<u128, &'static [u8]>,
    pool_rows: &impl ReadableTable<u128, &'static [u8]>,
) -> Result<AllocationIndexes, StorageError> {
    let mut indexes = AllocationIndexes {
        active: BTreeSet::new(),
        requests: std::collections::BTreeMap::new(),
    };
    let mut executors = BTreeSet::new();
    let mut pools = BTreeSet::<u128>::new();
    for entry in allocations.iter()? {
        let (key, value) = entry?;
        let allocation = allocations::decode_pool_allocation(key.value(), value.value())?;
        if allocation.token.job.get() != key.value() {
            return Err(StorageError::corrupted(
                "Allocation is stored under another Job",
            ));
        }
        if indexes
            .requests
            .insert(allocation.request_id.get(), key.value())
            .is_some()
        {
            return Err(StorageError::corrupted(
                "duplicate reservation request identity",
            ));
        }
        if allocation.holds_capacity() {
            let (pool, executor) = (allocation.token.pool.get(), allocation.token.executor.get());
            if !executors.insert(executor) {
                return Err(StorageError::corrupted(
                    "Executor has multiple active allocations",
                ));
            }
            indexes.active.insert((pool, executor, key.value()));
            pools.insert(pool);
        }
    }
    for pool in pools {
        let Some(bytes) = pool_rows.get(pool)? else {
            return Err(StorageError::corrupted(
                "active Allocation references a missing Pool",
            ));
        };
        allocations::decode_allocation_pool(pool, bytes.value())?;
    }
    Ok(indexes)
}

pub(super) fn stored_allocation_indexes(
    active: &impl ReadableTable<(u128, u128, u128), ()>,
    requests: &impl ReadableTable<u128, u128>,
) -> Result<AllocationIndexes, StorageError> {
    let mut stored = AllocationIndexes {
        active: BTreeSet::new(),
        requests: std::collections::BTreeMap::new(),
    };
    for entry in active.iter()? {
        stored.active.insert(entry?.0.value());
    }
    for entry in requests.iter()? {
        let (request, job) = entry?;
        stored.requests.insert(request.value(), job.value());
    }
    Ok(stored)
}

pub(super) fn allocation_indexes_agree(
    allocations: &impl ReadableTable<u128, &'static [u8]>,
    pools: &impl ReadableTable<u128, &'static [u8]>,
    active: &impl ReadableTable<(u128, u128, u128), ()>,
    requests: &impl ReadableTable<u128, u128>,
) -> Result<bool, StorageError> {
    Ok(compute_allocation_indexes(allocations, pools)?
        == stored_allocation_indexes(active, requests)?)
}

pub(super) fn verify_pool_executor_index(
    executors: &impl ReadableTable<u128, &'static [u8]>,
    members: &impl ReadableTable<(u128, u128), ()>,
) -> Result<(), StorageError> {
    for entry in executors.iter()? {
        let (key, bytes) = entry?;
        let executor = allocations::decode_allocation_executor(key.value(), bytes.value())?;
        if members
            .get((executor.pool.get(), executor.id.get()))?
            .is_none()
        {
            return Err(StorageError::corrupted(
                "Executor is missing its Pool membership",
            ));
        }
    }
    for entry in members.iter()? {
        let (pool, id) = entry?.0.value();
        let bytes = executors.get(id)?.ok_or_else(|| {
            StorageError::corrupted("Pool membership references a missing Executor")
        })?;
        if allocations::decode_allocation_executor(id, bytes.value())?
            .pool
            .get()
            != pool
        {
            return Err(StorageError::corrupted(
                "Pool membership names another Pool",
            ));
        }
    }
    Ok(())
}

pub(super) fn verify_job_indexes(tx: &ReadTransaction) -> Result<(), StorageError> {
    use crate::db::entities::{MutableFrozenConfiguration, MutableJobPhase};

    #[derive(Clone, Copy, Default)]
    struct ConversationFacts {
        open_children: u32,
        last_result: Option<JobId>,
    }

    let conversations = tx.open_table(CONVERSATIONS)?;
    let jobs = tx.open_table(JOBS)?;
    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
    let mut drafts = BTreeMap::new();
    let mut pending = BTreeSet::new();
    let mut ready = BTreeSet::new();
    let mut children = BTreeSet::new();
    let mut child_groups = BTreeSet::new();
    let mut live_children = BTreeSet::new();
    let mut successors = BTreeSet::new();
    let mut conversation_facts = BTreeMap::<u128, ConversationFacts>::new();
    let mut result_jobs = BTreeSet::new();

    for entry in tx.open_table(JOB_OUTPUTS)?.iter()? {
        let (key, bytes) = entry?;
        let key = key.value();
        if rows::decode_output(key, bytes.value())?.kind == OutputKind::Result {
            result_jobs.insert(key.0);
        }
    }

    for entry in conversations.iter()? {
        let (key, bytes) = entry?;
        let id = key.value();
        let conversation = JobSpec::decode_at(id, bytes.value())?;
        let runtime = state::require(tx, conversation.id)?;
        children.insert((conversation.parent_id.map_or(0, JobId::get), id));
        conversation_facts.entry(id).or_default();
        if let Some(parent) = conversation.parent_id {
            if parent >= conversation.id || conversations.get(parent.get())?.is_none() {
                return Err(StorageError::corrupted("invalid conversation parent"));
            }
            child_groups.insert((parent.get(), id));
            if runtime.state.outcome.is_none() {
                live_children.insert((parent.get(), id));
                let parent = conversation_facts.entry(parent.get()).or_default();
                parent.open_children = parent
                    .open_children
                    .checked_add(1)
                    .ok_or_else(|| StorageError::corrupted("conversation child count overflow"))?;
            }
        }
    }

    for entry in jobs.iter()? {
        let (key, bytes) = entry?;
        let id = key.value();
        let job = jobs::decode_mutable_job(id, bytes.value())?;
        let runtime = state::require(tx, job.id)?;
        if runtime.last_result.is_some() {
            return Err(StorageError::corrupted(
                "executable Job has a child result pointer",
            ));
        }
        if let Some(previous) = job.previous {
            let prior = jobs
                .get(previous.get())?
                .ok_or_else(|| StorageError::corrupted("Job predecessor is missing"))?;
            let prior = jobs::decode_mutable_job(previous.get(), prior.value())?;
            if prior.parent_id != job.parent_id || prior.phase == MutableJobPhase::Unsubmitted {
                return Err(StorageError::corrupted(
                    "Job predecessor is outside its submitted history",
                ));
            }
        }
        let parent = conversation_facts
            .get_mut(&job.parent_id.get())
            .ok_or_else(|| StorageError::corrupted("Job parent conversation is missing"))?;
        let submitted = job.phase != MutableJobPhase::Unsubmitted;
        let allocation = allocations
            .get(id)?
            .map(|row| allocations::decode_pool_allocation(id, row.value()))
            .transpose()?;
        let unresolved =
            state::has_open_membership(runtime.state.outcome.as_ref(), allocation.as_ref());
        if !submitted {
            if runtime.pending.is_some() || allocation.is_some() {
                return Err(StorageError::corrupted(
                    "unsubmitted Job has execution membership",
                ));
            }
            if runtime.state.outcome.is_none()
                && drafts.insert(job.composer_context.get(), id).is_some()
            {
                return Err(StorageError::corrupted(
                    "Composer has multiple unsubmitted Jobs",
                ));
            }
        } else {
            children.insert((job.parent_id.get(), id));
            if let Some(previous) = job.previous {
                successors.insert(previous.get());
            }
            if unresolved {
                if !matches!(runtime.pending, Some((_, 0))) {
                    return Err(StorageError::corrupted(
                        "unsettled Job has invalid pending counts",
                    ));
                }
                parent.open_children = parent
                    .open_children
                    .checked_add(1)
                    .ok_or_else(|| StorageError::corrupted("conversation child count overflow"))?;
            } else if runtime.pending.is_some() {
                return Err(StorageError::corrupted(
                    "settled Job retains pending counts",
                ));
            }
            if job.phase == MutableJobPhase::Queued
                && runtime.state.outcome.is_none()
                && allocation.is_none()
            {
                pending.insert(id);
                // This queue indexes dependency readiness. Dispatch checks other eligibility.
                if runtime.pending == Some((0, 0)) {
                    let submitted_at = job.submitted_at.ok_or_else(|| {
                        StorageError::corrupted("queued Job has no submission time")
                    })?;
                    let requirement = match job.frozen.as_ref() {
                        Some(MutableFrozenConfiguration::Generative(frozen)) => {
                            reservations::execution_requirement(frozen)
                        }
                        Some(MutableFrozenConfiguration::Decision(_)) => {
                            reservations::decision_requirement()
                        }
                        None => return Err(StorageError::corrupted("queued Job is not frozen")),
                    };
                    ready.insert((
                        jobs::mutable_priority(job.input.priority),
                        submitted_at.as_unix_micros(),
                        id,
                        requirement,
                    ));
                }
            }
        }
        if result_jobs.remove(&id) {
            if !submitted || !runtime.state.confirmed_terminal() {
                return Err(StorageError::corrupted(
                    "Job result has no settled execution",
                ));
            }
            parent.last_result = Some(parent.last_result.map_or(job.id, |old| old.max(job.id)));
        }
    }
    if !result_jobs.is_empty() {
        return Err(StorageError::corrupted(
            "Result output references a missing Job",
        ));
    }

    for entry in conversations.iter()? {
        let id = entry?.0.value();
        let runtime = state::require(tx, JobId::new(id))?;
        let facts = conversation_facts.get(&id).copied().unwrap_or_default();
        let expected_pending = runtime
            .state
            .outcome
            .is_none()
            .then_some((0, facts.open_children));
        let reason = if runtime.state.outcome.is_some() && facts.open_children != 0 {
            "settled conversation still has open children"
        } else if runtime.pending != expected_pending || runtime.last_result != facts.last_result {
            "conversation child counts or latest Result disagree with its children"
        } else {
            continue;
        };
        let result_id =
            |id: Option<JobId>| id.map_or_else(|| "None".to_owned(), |id| id.to_string());
        return Err(StorageError::corrupted(format!(
            "{reason}: conversation {}, outcome {:?}, open children counted {}, \
             pending (dependencies, children) stored {:?}, expected {expected_pending:?}, \
             last_result stored {}, expected {}",
            JobId::new(id),
            runtime.state.outcome,
            facts.open_children,
            runtime.pending,
            result_id(runtime.last_result),
            result_id(facts.last_result),
        )));
    }

    for entry in tx.open_table(DRAFT_CONTEXTS)?.iter()? {
        let (context, job) = entry?;
        if drafts.remove(&context.value()) != Some(job.value()) {
            return Err(StorageError::corrupted(
                "draft_context index disagrees with its Job",
            ));
        }
    }
    if !drafts.is_empty() {
        return Err(StorageError::corrupted(
            "unsubmitted Job has no draft_context entry",
        ));
    }

    macro_rules! verify_members {
        ($table:ident, $expected:ident) => {
            for entry in tx.open_table($table)?.iter()? {
                if !$expected.remove(&entry?.0.value()) {
                    return Err(StorageError::corrupted(concat!(
                        stringify!($table),
                        " contains a key without a matching source fact"
                    )));
                }
            }
            if !$expected.is_empty() {
                return Err(StorageError::corrupted(concat!(
                    stringify!($table),
                    " is missing a source fact"
                )));
            }
        };
    }
    verify_members!(PENDING, pending);
    verify_members!(MUTABLE_READY, ready);
    verify_members!(CHILDREN, children);
    verify_members!(CHILD_GROUPS, child_groups);
    verify_members!(LIVE_CHILDREN, live_children);
    verify_members!(SUCCESSORS, successors);
    Ok(())
}
