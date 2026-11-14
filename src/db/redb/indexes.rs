use std::collections::BTreeSet;

use super::*;
use crate::db::entities::{RequestEventRecord, RequestPayload};

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
    let grams =
        |input: &crate::db::entities::MutableJobInput| -> Result<BTreeSet<u64>, StorageError> {
            let entry = Entry::UserMessage {
                text: input.text().to_owned(),
                attachments: input.attachments().to_vec(),
            };
            Ok(search_input_text(&attachments, &entry)?
                .map_or_else(BTreeSet::new, |text| search_grams(&text)))
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
    let jobs = tx.open_table(JOBS)?;
    let row = jobs
        .get(conversation.get())?
        .ok_or_else(|| StorageError::corrupted("search conversation missing"))?;
    let turn_row = jobs
        .get(turn.get())?
        .ok_or_else(|| StorageError::corrupted("search turn missing"))?;
    if matches!(turn_row.value(), [TAG_JOB, version, ..] if u64::from(*version) == FORMAT_VERSION) {
        let mutable = transactions::decode_mutable_job(turn.get(), turn_row.value())?;
        return Ok(
            searchable_conversation(&JobSpec::decode_at(conversation.get(), row.value())?)
                && mutable.parent_id == conversation
                && matches!(
                    mutable.input.content,
                    crate::db::entities::MutableJobContent::Generative { .. }
                ),
        );
    }
    Ok(
        searchable_conversation(&JobSpec::decode_at(conversation.get(), row.value())?)
            && searchable_turn(&JobSpec::decode_at(turn.get(), turn_row.value())?),
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
    if let Some(text) = search_output_text(output, completed) {
        insert_turn_search_grams(tx, conversation, turn, &search_grams(text))?;
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

fn indexed_identity<'a>(
    identity: Option<&'a reader::UsageIdentity>,
    event: &RequestEventRecord,
) -> Option<&'a reader::UsageIdentity> {
    identity.filter(|_| {
        !matches!(
            event.payload,
            RequestPayload::Sent { .. }
                | RequestPayload::Received { .. }
                | RequestPayload::ApprovedCalls
        )
    })
}

pub(super) fn request_timeline_key(
    identity: Option<&reader::UsageIdentity>,
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
    identity: Option<&reader::UsageIdentity>,
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
    configs: &impl ReadableTable<u128, &'static [u8]>,
) -> Result<AllocationIndexes, StorageError> {
    let mut indexes = AllocationIndexes {
        active: BTreeSet::new(),
        requests: std::collections::BTreeMap::new(),
    };
    let mut executors = BTreeSet::new();
    let mut pools = BTreeSet::<u128>::new();
    for entry in allocations.iter()? {
        let (key, value) = entry?;
        let allocation = transactions::decode_pool_allocation(key.value(), value.value())?;
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
        let Some(bytes) = configs.get(pool)? else {
            return Err(StorageError::corrupted(
                "active Allocation references a missing Pool",
            ));
        };
        transactions::decode_allocation_pool(pool, bytes.value())?;
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

pub(super) fn rebuild_allocation_indexes(tx: &WriteTransaction) -> Result<(), StorageError> {
    let computed =
        compute_allocation_indexes(&tx.open_table(POOL_ALLOCATIONS)?, &tx.open_table(CONFIGS)?)?;
    tx.delete_table(ACTIVE_ALLOCATIONS)?;
    tx.delete_table(ALLOCATION_REQUESTS)?;
    let mut active = tx.open_table(ACTIVE_ALLOCATIONS)?;
    for key in computed.active {
        active.insert(key, ())?;
    }
    let mut requests = tx.open_table(ALLOCATION_REQUESTS)?;
    for (request, job) in computed.requests {
        requests.insert(request, job)?;
    }
    Ok(())
}

pub(super) fn allocation_indexes_agree(
    allocations: &impl ReadableTable<u128, &'static [u8]>,
    configs: &impl ReadableTable<u128, &'static [u8]>,
    active: &impl ReadableTable<(u128, u128, u128), ()>,
    requests: &impl ReadableTable<u128, u128>,
) -> Result<bool, StorageError> {
    Ok(compute_allocation_indexes(allocations, configs)?
        == stored_allocation_indexes(active, requests)?)
}
