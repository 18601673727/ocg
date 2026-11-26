use super::offline::{finalization_copy, verify_payloads, with_verified_rebuild_copy};
use super::reserved::ReservedRefusal;
use crate::db::entities::{
    AWAITING_TOOL_DECISION, Actor, ChildNoticeDrop, ChildNoticeKind, ChildTurnResult,
    ContentDigest, JobId, JobOutputId, JobOutputRecord, JobSpec, MutableJob, MutableJobContent,
    MutableJobPhase, NativeSummaryResultLink, Origin, OutcomeKind, OutputKind, Recurrence,
    RequestNumber, SubmissionKey, SubmissionRecord, ToolCallRef, ToolDecision, ToolEvidenceRecord,
    ToolOutcome,
};
use crate::db::redb::codec::{
    Code, Dec, Enc, TAG_GLOBAL_COMPACTION, TAG_GLOBAL_HEADERS, TAG_JOB, TAG_JOB_RUNTIME,
    TAG_OUTPUT, TAG_TOOL_CALL,
};
use crate::db::redb::execution::TAG_EXECUTION_EVENT;
use crate::db::redb::schema::{
    APPROVALS, CONVERSATIONS, DATABASE_FORMAT_KEY, FORMAT_VERSION, JOB_OUTPUTS, JOB_RUNTIME, JOBS,
    MESSAGES, META, OPEN_APPROVALS, PENDING_CHILD_NOTICES, POOL_ALLOCATIONS, RECURRENCES,
    REQUEST_EVENTS, SCHEMA_FINGERPRINT, SCHEMA_FINGERPRINT_KEY, SUBMISSIONS, TOOL_CALLS, TURNS,
    check_migration_source_tables, check_required_tables, record_version_byte,
    rewrite_record_headers, verify_identical_database, verify_record_headers,
};
use crate::db::redb::{
    StorageError, TxRead, allocations, approvals, job_tree, jobs, observations, reads, rows,
    settlement, state,
};
use ::redb::{
    Database, Durability, Key, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition,
    TableHandle, WriteTransaction,
};
use serde_json::{Value as Json, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};

#[derive(Clone, Copy)]
pub(in crate::db::redb) struct Versions {
    from: u8,
    to: u8,
}

impl Versions {
    fn crosses(self, version: u8) -> bool {
        self.from < version && version <= self.to
    }

    // A step that owns a body conversion also promotes and verifies its header.
    fn changes_body(self, tags: &[u8]) -> bool {
        match tags {
            [TAG_OUTPUT] => self.crosses(2),
            [TAG_JOB] => self.crosses(3) || self.crosses(7),
            [TAG_JOB_RUNTIME] => self.crosses(3) || self.crosses(5),
            _ => false,
        }
    }
}

fn source_layout(source: &ReadTransaction) -> Result<Versions, ReservedRefusal> {
    let meta = source.open_table(META).map_err(StorageError::from)?;
    let format = meta.get(DATABASE_FORMAT_KEY).map_err(StorageError::from)?;
    let fingerprint = meta
        .get(SCHEMA_FINGERPRINT_KEY)
        .map_err(StorageError::from)?;
    let fingerprint = fingerprint.as_ref().map(|marker| marker.value());
    let source_format = format
        .as_ref()
        .and_then(|v| v.value().try_into().ok())
        .map(u64::from_le_bytes);
    let from = match (source_format, fingerprint) {
        (Some(1), Some(b"ocg-native-format-1")) => 1,
        (Some(2), Some(b"ocg-native-format-2")) => 2,
        (Some(3), Some(b"ocg-native-format-3")) => 3,
        (Some(4), Some(b"ocg-native-format-4")) => 4,
        (Some(5), Some(b"ocg-native-format-5")) => 5,
        (Some(6), Some(b"ocg-native-format-6")) => 6,
        (Some(FORMAT_VERSION), Some(SCHEMA_FINGERPRINT)) => {
            return Err(ReservedRefusal::new(
                "already_migrated",
                format!("the database is already in Format {FORMAT_VERSION}"),
            ));
        }
        _ => {
            return Err(ReservedRefusal::new(
                "unsupported_source",
                "migration requires Format 1 through 6 with its matching schema fingerprint",
            ));
        }
    };
    if from < 6 {
        check_migration_source_tables(source)?;
    } else {
        check_required_tables(source)?;
    }
    Ok(Versions {
        from,
        to: record_version_byte(),
    })
}

fn promote_header(versions: Versions, bytes: &mut [u8], tags: &[u8]) -> Result<(), StorageError> {
    match bytes {
        [tag, version, ..] if tags.contains(tag) && *version == versions.from => {
            *version = versions.to;
            Ok(())
        }
        _ => Err(StorageError::corrupted(format!(
            "invalid Format {} record header for {tags:?}",
            versions.from
        ))),
    }
}

fn promote_record(versions: Versions, bytes: &[u8], tags: &[u8]) -> Result<Vec<u8>, StorageError> {
    let mut row = bytes.to_vec();
    promote_header(versions, &mut row, tags)?;
    if versions.crosses(4) && row[0] == TAG_TOOL_CALL {
        row.push(0); // Historical commands have no recorded process identity.
    }
    if versions.crosses(6) && row[0] == TAG_TOOL_CALL {
        row.push(0);
    }
    if row[0] == TAG_EXECUTION_EVENT {
        let mut d = Dec::payload(&row[2..]);
        d.u128()?;
        d.u64()?;
        d.u64()?;
        d.u64()?;
        d.ts()?;
        if d.u8()? == 2 {
            let len = d.u32()? as usize;
            d.take(len)?;
            d.end()?;
            let start = row.len() - len;
            promote_header(versions, &mut row[start..], &[42])?;
        }
    }
    Ok(row)
}

pub(in crate::db::redb) fn rewrite_tagged<K: Key + 'static>(
    versions: Versions,
    source: &ReadTransaction,
    tx: &WriteTransaction,
    definition: TableDefinition<'_, K, &'static [u8]>,
    tags: &[u8],
) -> Result<(), StorageError> {
    if versions.changes_body(tags) {
        return Ok(());
    }
    let mut result = tx.open_table(definition)?;
    for entry in source.open_table(definition)?.iter()? {
        let (key, bytes) = entry?;
        result.insert(
            key.value(),
            promote_record(versions, bytes.value(), tags)?.as_slice(),
        )?;
    }
    Ok(())
}

fn promote_metadata(versions: Versions, key: &str, value: &[u8]) -> Result<Vec<u8>, StorageError> {
    match key {
        SCHEMA_FINGERPRINT_KEY => Ok(SCHEMA_FINGERPRINT.to_vec()),
        DATABASE_FORMAT_KEY => Ok(FORMAT_VERSION.to_le_bytes().to_vec()),
        "compaction_settings" => promote_record(versions, value, &[TAG_GLOBAL_COMPACTION]),
        "outbound_headers" => promote_record(versions, value, &[TAG_GLOBAL_HEADERS]),
        "open_epoch" | "clean_shutdown" => Ok(value.to_vec()),
        _ => Err(StorageError::corrupted(format!(
            "unrecognized source metadata row {key:?}"
        ))),
    }
}

fn verify_transformed<K: Key + 'static>(
    source: &ReadTransaction,
    result: &ReadTransaction,
    definition: TableDefinition<'_, K, &'static [u8]>,
    mut promote: impl FnMut(K::SelfType<'_>, &[u8]) -> Result<Vec<u8>, StorageError>,
) -> Result<(), StorageError> {
    let source = source.open_table(definition)?;
    let result = result.open_table(definition)?;
    let mut after = result.iter()?;
    for entry in source.iter()? {
        let (key, value) = entry?;
        let Some((new_key, new_value)) = after.next().transpose()? else {
            return Err(StorageError::corrupted(format!(
                "migration lost rows in {}",
                definition.name()
            )));
        };
        if K::as_bytes(&key.value()).as_ref() != K::as_bytes(&new_key.value()).as_ref()
            || promote(key.value(), value.value())? != new_value.value()
        {
            return Err(StorageError::corrupted(format!(
                "migration changed unexpected data in {}",
                definition.name()
            )));
        }
    }
    if after.next().transpose()?.is_some() {
        return Err(StorageError::corrupted(format!(
            "migration added rows in {}",
            definition.name()
        )));
    }
    Ok(())
}

pub(in crate::db::redb) fn verify_tagged<K: Key + 'static>(
    versions: Versions,
    source: &ReadTransaction,
    result: &ReadTransaction,
    definition: TableDefinition<'_, K, &'static [u8]>,
    tags: &[u8],
) -> Result<(), StorageError> {
    if versions.changes_body(tags) {
        return Ok(());
    }
    verify_transformed(source, result, definition, |_, bytes| {
        promote_record(versions, bytes, tags)
    })
}

pub(in crate::db::redb) fn verify_metadata(
    versions: Versions,
    source: &ReadTransaction,
    result: &ReadTransaction,
) -> Result<(), StorageError> {
    verify_transformed(source, result, META, |k, v| {
        promote_metadata(versions, k, v)
    })
}

#[derive(Default)]
pub(in crate::db::redb) struct LegacyNotices {
    pending: BTreeSet<(u128, u128, u32, u32)>,
    dropped: BTreeMap<ToolCallRef, ChildNoticeDrop>,
}

impl LegacyNotices {
    fn read(tx: &WriteTransaction) -> Result<Self, StorageError> {
        let mut notices = Self::default();
        let calls = tx.open_table(TOOL_CALLS)?;
        let approvals = tx.open_table(OPEN_APPROVALS)?;
        let outputs = tx.open_table(JOB_OUTPUTS)?;
        let submissions = tx.open_table(SUBMISSIONS)?;
        for entry in calls.iter()? {
            let (key, value) = entry?;
            let call = rows::tool_call_ref(key.value())?;
            let tool = rows::decode_tool_evidence(call, value.value())?;
            let progress = match tool.name.as_str() {
                "job_report_progress" if legacy_progress_deferred(&tool) => true,
                "job_ask_parent"
                    if matches!(tool.decision.fact, ToolDecision::AwaitingAnswer(_)) =>
                {
                    false
                }
                _ => continue,
            };
            let posted = job_tree::tool_submission(call, b"child notice", 1).key;
            if submissions.get(posted.as_bytes().as_slice())?.is_some() {
                continue;
            }
            let turn = jobs::mutable_job_in(tx, call.job)?
                .ok_or_else(|| StorageError::corrupted("child notice has no Job"))?;
            if !progress
                && (approvals
                    .get(approvals::open_key(turn.parent_id, call))?
                    .is_none()
                    || !legacy_child_is_awaiting(&outputs, call.job)?)
            {
                continue;
            }
            let child = legacy_notice_conversation(tx, turn.parent_id)?;
            let parent = child
                .parent_id
                .ok_or_else(|| StorageError::corrupted("child notice has no parent"))?;
            if progress && state::require(tx, call.job)?.state.outcome.is_some() {
                notices
                    .dropped
                    .insert(call, ChildNoticeDrop::ChildTurnSettled);
            } else if jobs::require_open_turn_parent(
                &legacy_notice_conversation(tx, parent)?,
                &state::require(tx, parent)?,
            )
            .is_err()
            {
                notices.dropped.insert(call, ChildNoticeDrop::ParentClosed);
            } else {
                notices.pending.insert(approvals::open_key(parent, call));
            }
        }
        Ok(notices)
    }

    fn apply(&self, tx: &WriteTransaction) -> Result<(), StorageError> {
        let mut pending = tx.open_table(PENDING_CHILD_NOTICES)?;
        for key in &self.pending {
            pending.insert(*key, ())?;
        }
        let mut calls = tx.open_table(TOOL_CALLS)?;
        for (&call, &reason) in &self.dropped {
            let key = rows::tool_call_key(call);
            let mut record = {
                let row = calls.get(key)?.ok_or_else(|| {
                    StorageError::corrupted("dropped child notice has no tool call")
                })?;
                rows::decode_tool_evidence(call, row.value())?
            };
            record.child_notice_drop = Some(reason);
            calls.insert(key, rows::encode_tool_evidence(&record).as_slice())?;
        }
        Ok(())
    }

    pub(in crate::db::redb) fn verify_queue(
        &self,
        result: &ReadTransaction,
    ) -> Result<(), StorageError> {
        let table = result.open_table(PENDING_CHILD_NOTICES)?;
        let mut found = table.iter()?;
        for expected in &self.pending {
            if found.next().transpose()?.map(|(key, _)| key.value()) != Some(*expected) {
                return Err(StorageError::corrupted(
                    "migration changed a pending child notice",
                ));
            }
        }
        if found.next().transpose()?.is_some() {
            return Err(StorageError::corrupted(
                "migration added unexpected pending child notices",
            ));
        }
        Ok(())
    }

    pub(in crate::db::redb) fn verify_calls(
        &self,
        versions: Versions,
        source: &ReadTransaction,
        result: &ReadTransaction,
    ) -> Result<(), StorageError> {
        verify_transformed(source, result, TOOL_CALLS, |key, bytes| {
            let promoted = promote_record(versions, bytes, &[TAG_TOOL_CALL])?;
            let call = rows::tool_call_ref(key)?;
            let Some(&reason) = self.dropped.get(&call) else {
                return Ok(promoted);
            };
            let mut record = rows::decode_tool_evidence(call, &promoted)?;
            record.child_notice_drop = Some(reason);
            Ok(rows::encode_tool_evidence(&record))
        })
    }
}

fn legacy_progress_deferred(tool: &ToolEvidenceRecord) -> bool {
    let Some(ToolOutcome::Completed { result }) =
        tool.outcome.as_ref().map(|outcome| &outcome.fact)
    else {
        return false;
    };
    serde_json::from_str::<Json>(result.as_str())
        .ok()
        .and_then(|result| result.get("deferred")?.as_bool())
        == Some(true)
}

fn legacy_child_is_awaiting(
    outputs: &impl ReadableTable<(u128, u32), &'static [u8]>,
    turn: JobId,
) -> Result<bool, StorageError> {
    let id = turn.get();
    let Some((key, row)) = outputs
        .range((id, 0)..=(id, u32::MAX))?
        .next_back()
        .transpose()?
    else {
        return Ok(false);
    };
    let output = rows::decode_output(key.value(), row.value())?;
    Ok(output.kind == OutputKind::Result && output.code.as_deref() == Some(AWAITING_TOOL_DECISION))
}

fn legacy_notice_conversation(tx: &impl TxRead, id: JobId) -> Result<JobSpec, StorageError> {
    reads::jobs::read_conversation_row(tx, id)?
        .ok_or_else(|| StorageError::corrupted("child notice conversation is missing"))
}

fn old_output(
    versions: Versions,
    key: (u128, u32),
    bytes: &[u8],
) -> Result<JobOutputRecord, StorageError> {
    let row = promote_record(versions, bytes, &[TAG_OUTPUT])?;
    let mut d = Dec::new(&row, TAG_OUTPUT)?;
    let output = JobOutputRecord {
        job: JobId::new(key.0),
        seq: i64::from(key.1),
        id: JobOutputId::new(d.u128()?),
        kind: d.code(OutputKind::from_code)?,
        summary: d.ostr()?,
        code: d.ostr()?,
        goal_reached: d.opt(Dec::bool)?,
        value: d.ojson()?,
        created_at: d.ts()?,
        part: None,
        span: None,
        answer_request: None,
    };
    d.end()?;
    Ok(output)
}

#[derive(Default)]
struct ConversionCounts {
    kept_text_jobs: Vec<JobId>,
    kept_text_details: Vec<String>,
    converted_summary_links: usize,
    retained_summary_links: usize,
}

fn record_kept_text(
    tx: &impl TxRead,
    output: &JobOutputRecord,
    counts: &mut ConversionCounts,
) -> Result<(), StorageError> {
    let mut probe = output.clone();
    probe.answer_request = observations::latest_request(&tx.table(REQUEST_EVENTS)?, output.job)?;
    let answer = observations::answer_text(tx, output.job, &probe)?;
    let stored = output.model_text().or(output.summary.as_deref());
    let comparison = match (stored, answer.as_deref()) {
        (Some(text), Some(answer)) if text == answer => "matches recorded answer",
        (Some(_), Some(_)) => "differs from recorded answer",
        (None, _) | (_, None) => "recorded answer unavailable",
    };
    counts.kept_text_jobs.push(output.job);
    let summary = if output
        .summary
        .as_deref()
        .is_some_and(|text| Some(text) != output.model_text())
    {
        "; distinct summary retained"
    } else {
        ""
    };
    counts
        .kept_text_details
        .push(format!("{}: {comparison}{summary}", output.job));
    Ok(())
}

fn convert_summary_link(
    tx: &impl TxRead,
    output: &mut JobOutputRecord,
    source: ContentDigest,
) -> Result<bool, StorageError> {
    let value = output
        .value
        .as_ref()
        .ok_or_else(|| StorageError::corrupted("summary result link missing"))?;
    let link: NativeSummaryResultLink = serde_json::from_value(value.clone())
        .map_err(|_| StorageError::corrupted("invalid Format 1 summary link"))?;
    let NativeSummaryResultLink::Payload {
        payload,
        producer_job,
        ..
    } = link
    else {
        return Err(StorageError::corrupted(
            "expected a Format 1 summary payload",
        ));
    };
    if link.source() != source {
        return Err(StorageError::corrupted(
            "summary result names another source",
        ));
    }
    let old = observations::linked_summary(tx, output.job, link)?;
    if output.summary.as_ref() == Some(&old.summary) {
        output.summary = None;
    }
    let Some(request) = observations::latest_request(&tx.table(REQUEST_EVENTS)?, output.job)?
        .and_then(RequestNumber::new)
    else {
        return Ok(false);
    };
    let Some(summary) = observations::received_summary(tx, output.job, request, source)? else {
        return Ok(false);
    };
    // Format 1 serialized the typed summary through a JSON Value before hashing it.
    let canonical = serde_json::to_value(summary)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|_| StorageError::corrupted("cannot encode received summary"))?;
    let messages = tx.table(MESSAGES)?;
    let old_bytes = messages
        .get(payload.as_bytes().as_slice())?
        .ok_or_else(|| StorageError::corrupted("summary payload missing"))?;
    if old_bytes.value() != canonical {
        return Ok(false);
    }
    let new_link = NativeSummaryResultLink::Received {
        source,
        producer_job,
        answer_request: request,
    };
    output.value = Some(
        serde_json::to_value(new_link)
            .map_err(|_| StorageError::corrupted("cannot encode summary result link"))?,
    );
    Ok(true)
}

fn convert_output(
    tx: &impl TxRead,
    output: &mut JobOutputRecord,
    counts: &mut ConversionCounts,
) -> Result<(), StorageError> {
    if output.kind != OutputKind::Result || output.code.is_some() {
        return Ok(());
    }
    let job = {
        let jobs = tx.table(JOBS)?;
        let row = jobs
            .get(output.job.get())?
            .ok_or_else(|| StorageError::corrupted("result Job missing"))?;
        jobs::decode_mutable_job(output.job.get(), row.value())?
    };
    if !matches!(job.input.content, MutableJobContent::Generative { .. })
        || !state::read(tx, job.id)?.is_some_and(|state| {
            state
                .state
                .outcome
                .is_some_and(|o| o.kind == OutcomeKind::Completed)
        })
    {
        return Ok(());
    }
    if let Some(source) = job.summary_source {
        if convert_summary_link(tx, output, source)? {
            counts.converted_summary_links += 1;
        } else {
            counts.retained_summary_links += 1;
        }
        return Ok(());
    }
    if output
        .value
        .as_ref()
        .and_then(|v| v["finish_reason"].as_str())
        == Some("tool_calls")
    {
        return Ok(());
    }
    if output
        .summary
        .as_deref()
        .is_some_and(|summary| Some(summary) != output.model_text())
    {
        record_kept_text(tx, output, counts)?;
        return Ok(());
    }
    let Some(visible) = output
        .model_text()
        .or(output.summary.as_deref())
        .map(str::to_owned)
    else {
        return Ok(());
    };
    output.answer_request = observations::latest_request(&tx.table(REQUEST_EVENTS)?, job.id)?;
    if observations::answer_text(tx, job.id, output)?.as_deref() == Some(&visible) {
        if let Some(object) = output.value.as_mut().and_then(Json::as_object_mut) {
            object.remove("text");
        }
        output.summary = None;
        return Ok(());
    }
    output.answer_request = None;
    let object = output
        .value
        .get_or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| StorageError::corrupted("answer Result value is not an object"))?;
    object.insert("text".to_owned(), Json::String(visible));
    output.summary = None;
    record_kept_text(tx, output, counts)?;
    Ok(())
}

fn verify_outputs(
    versions: Versions,
    source: &ReadTransaction,
    result: &ReadTransaction,
) -> Result<ConversionCounts, StorageError> {
    use ::redb::ReadableTableMetadata;
    let updated = result.open_table(JOB_OUTPUTS)?;
    let old = source.open_table(JOB_OUTPUTS)?;
    if old.len()? != updated.len()? {
        return Err(StorageError::corrupted("migration changed output count"));
    }
    let mut counts = ConversionCounts::default();
    for entry in old.iter()? {
        let (k, v) = entry?;
        let key = k.value();
        let prior = old_output(versions, key, v.value())?;
        let mut expected = prior.clone();
        convert_output(result, &mut expected, &mut counts)?;
        let row = updated
            .get(key)?
            .ok_or_else(|| StorageError::corrupted("migration lost output"))?;
        let mut actual = rows::decode_output(key, row.value())?;
        if actual != expected {
            return Err(StorageError::corrupted("migration changed output facts"));
        }
        observations::project_answer(result, prior.job, &mut actual)?;
        if let Some(summary) = prior.summary.as_deref()
            && actual.summary.as_deref() != Some(summary)
            && prior.model_text() != Some(summary)
        {
            return Err(StorageError::corrupted(
                "migration lost distinct output summary",
            ));
        }
        if prior.model_text().or(prior.summary.as_deref())
            != actual.model_text().or(actual.summary.as_deref())
        {
            return Err(StorageError::corrupted(
                "migration changed visible output text",
            ));
        }
    }
    Ok(counts)
}

// TOOL_CALLS and APPROVALS are indexed by call, while a creation receipt names
// only its digest. Invert the host's key formulas once for the whole store.
enum HostCreation {
    Tool(ToolCallRef, &'static str),
    Approval,
    Recurrence(JobId),
}

struct AuthorEvidence {
    submissions: BTreeMap<JobId, SubmissionRecord>,
    host: BTreeMap<SubmissionKey, HostCreation>,
}

impl AuthorEvidence {
    fn read(tx: &impl TxRead) -> Result<Self, StorageError> {
        let mut submissions = BTreeMap::new();
        for entry in tx.table(SUBMISSIONS)?.iter()? {
            let (key, value) = entry?;
            let record = rows::decode_submission(key.value(), value.value())?;
            if record.seq == 0 && submissions.insert(record.job, record).is_some() {
                return Err(StorageError::corrupted(format!(
                    "multiple creation submissions for {}",
                    record.job
                )));
            }
        }
        let mut host = BTreeMap::new();
        for entry in tx.table(TOOL_CALLS)?.iter()? {
            let call = rows::tool_call_ref(entry?.0.value())?;
            for purpose in ["assign", "message"] {
                let key = job_tree::tool_submission(call, purpose.as_bytes(), 0).key;
                host.insert(key, HostCreation::Tool(call, purpose));
            }
        }
        for entry in tx.table(APPROVALS)?.iter()? {
            let call = rows::tool_call_ref(entry?.0.value())?;
            host.insert(
                approvals::continuation_submission(call, b"create").key,
                HostCreation::Approval,
            );
        }
        for entry in tx.table(RECURRENCES)?.iter()? {
            let (conv, row) = entry?;
            let recurrence = Recurrence::decode(row.value())?;
            if recurrence.n > u64::from(crate::db::entities::JobLimits::CEILING.max_iterations) {
                return Err(StorageError::corrupted("invalid recurrence count"));
            }
            for n in 2..=recurrence.n {
                let key =
                    settlement::recurrence_submission(conv.value(), n, b"recurrence-turn-create")
                        .key;
                host.insert(key, HostCreation::Recurrence(JobId::new(conv.value())));
            }
        }
        Ok(Self { submissions, host })
    }

    fn classify(
        &self,
        tx: &impl TxRead,
        job: &MutableJob,
        caller: impl Fn(ToolCallRef) -> Result<Actor, StorageError>,
    ) -> Result<(Actor, &'static str), StorageError> {
        if job.retry.is_some() {
            return Ok((job.submitted_by.clone(), "retry"));
        }
        let parent = reads::jobs::read_conversation_row(tx, job.parent_id)?
            .ok_or_else(|| StorageError::corrupted(format!("missing parent of Job {}", job.id)))?;
        let record = self.submissions.get(&job.id);
        match record {
            None => {
                if jobs::is_predictive_helper(tx, job)? {
                    return Ok((Actor::Auto, "predictive"));
                }
                if reads::jobs::project_planning_target(tx, job)?.is_some() {
                    return Ok((Actor::Auto, "planning_decision"));
                }
                if job.summary_source.is_some() {
                    return Ok((Actor::Auto, "summary"));
                }
                if parent.is_chunk_group() {
                    return Ok((Actor::Auto, "chunk"));
                }
                if parent.is_standalone_decision_group() {
                    return Ok((Actor::User, "decision"));
                }
            }
            Some(r) if r.key.as_bytes() != r.fingerprint.as_bytes() => {
                return Ok((Actor::User, "composer"));
            }
            Some(r) => {
                if let Origin::Agent { call } = parent.origin
                    && job.previous.is_none()
                    && reads::jobs::turn_first(&tx.table(TURNS)?, parent.id)? == Some(job.id)
                {
                    for (purpose, class) in [
                        (b"plan create".as_slice(), "plan"),
                        (b"dispatch create".as_slice(), "dispatch"),
                    ] {
                        if (0..job_tree::MAX_LIVE_CHILDREN as u32)
                            .any(|n| job_tree::tool_submission(call, purpose, n).key == r.key)
                        {
                            return Ok((caller(call)?, class));
                        }
                    }
                }
                match self.host.get(&r.key) {
                    Some(HostCreation::Tool(call, purpose)) => {
                        return Ok((caller(*call)?, purpose));
                    }
                    Some(HostCreation::Approval) => return Ok((Actor::Auto, "approval")),
                    Some(HostCreation::Recurrence(id)) if *id == parent.id => {
                        return Ok((Actor::Auto, "recurrence"));
                    }
                    _ => {}
                }
                if job.input.text().starts_with(job_tree::REPORT_HEADER) {
                    return Ok((Actor::Auto, "results"));
                }
            }
        }
        let key = record.map_or_else(|| "no seq-0 submission".to_owned(), |r| r.key.to_string());
        Err(StorageError::corrupted(format!(
            "cannot classify Job {} in {} ({key})",
            job.id, parent.id
        )))
    }
}

fn source_caller(
    source: &ReadTransaction,
    versions: Versions,
    call: ToolCallRef,
) -> Result<Actor, StorageError> {
    let jobs = source.open_table(JOBS)?;
    let row = jobs
        .get(call.job.get())?
        .ok_or_else(|| StorageError::corrupted(format!("tool caller {} missing", call.job)))?;
    let row = promote_record(versions, row.value(), &[TAG_JOB])?;
    let mut d = Dec::new(&row, TAG_JOB)?;
    if d.u128()? != call.job.get() {
        return Err(StorageError::corrupted("invalid tool caller identity"));
    }
    Ok(Actor::Agent {
        conversation: JobId::new(d.u128()?),
    })
}

struct MigratedJobBase {
    row: Vec<u8>,
    author: Option<(Actor, &'static str)>,
}

fn migrate_job_base(
    versions: Versions,
    key: u128,
    bytes: &[u8],
    tx: &impl TxRead,
    evidence: Option<&AuthorEvidence>,
    source: &ReadTransaction,
) -> Result<MigratedJobBase, StorageError> {
    let mut row = promote_record(versions, bytes, &[TAG_JOB])?;
    if versions.crosses(7) {
        // Append empty child_results (u32 0) and child_notice (None).
        row.extend_from_slice(&[0, 0, 0, 0, 0]);
    }
    let Some(evidence) = evidence else {
        return Ok(MigratedJobBase { row, author: None });
    };
    let mut d = Dec::new(&row, TAG_JOB)?;
    d.u128()?;
    d.u128()?;
    d.ou128()?;
    let retry = d.ou128()?;
    let offset = d.position();
    // A retry already has its actor at this offset. Supply a temporary slot
    // for ordinary Jobs so the current decoder can read the input and links.
    if retry.is_none() {
        row.insert(offset, 0);
    }
    let job = jobs::decode_mutable_job(key, &row)?;
    let (author, class) =
        evidence.classify(tx, &job, |call| source_caller(source, versions, call))?;
    if retry.is_none() {
        let mut encoded = Enc(Vec::new());
        encoded.actor(&author);
        row.splice(offset..=offset, encoded.0);
    }
    Ok(MigratedJobBase {
        row,
        author: Some((author, class)),
    })
}

fn migrate_runtime(versions: Versions, key: u128, bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
    let mut row = promote_record(versions, bytes, &[TAG_JOB_RUNTIME])?;
    if !versions.crosses(3) {
        return Ok(row);
    }
    let mut d = Dec::new(&row, TAG_JOB_RUNTIME)?;
    state::JobRuntime::decode_state_and_lifecycle(key, &mut d)?;
    let offset = d.position();
    d.bool()?;
    row.remove(offset);
    Ok(row)
}

fn migrate_turn_bases(
    versions: Versions,
    source: &ReadTransaction,
    tx: &WriteTransaction,
    evidence: Option<&AuthorEvidence>,
) -> Result<(), StorageError> {
    let mut jobs = tx.open_table(JOBS)?;
    for entry in source.open_table(JOBS)?.iter()? {
        let (k, v) = entry?;
        let migrated = migrate_job_base(versions, k.value(), v.value(), tx, evidence, source)?;
        jobs.insert(k.value(), migrated.row.as_slice())?;
    }
    Ok(())
}

struct ParsedReportHeader {
    conversation: JobId,
    call: ToolCallRef,
    state: OutcomeKind,
    title_matches_stored: bool,
    state_matches_stored: bool,
}

struct ParsedChildResult {
    conversation: JobId,
    call: ToolCallRef,
    state: OutcomeKind,
    answer_start: u32,
    answer_end: u32,
}

fn parse_report_header(
    tx: &impl TxRead,
    job: &MutableJob,
    heading: &str,
    origin: Option<ToolCallRef>,
) -> Result<Option<ParsedReportHeader>, StorageError> {
    let Some((raw_title, source)) = heading.rsplit_once("] conversation ") else {
        return Ok(None);
    };
    let Some((id, outcome)) = source.split_once(": ") else {
        return Ok(None);
    };
    let state = match outcome {
        "completed" => OutcomeKind::Completed,
        "failed" => OutcomeKind::Failed,
        "cancelled" => OutcomeKind::Cancelled,
        "unknown" => OutcomeKind::Unknown,
        _ => return Ok(None),
    };
    let Some(conversation) = crate::db::entities::parse_entity_id(id).map(JobId::new) else {
        return Ok(None);
    };
    let Some(child) = reads::jobs::read_conversation_row(tx, conversation)? else {
        return Ok(None);
    };
    let Origin::Agent { call } = child.origin else {
        return Ok(None);
    };
    if child.parent_id != Some(job.parent_id)
        || origin.is_some_and(|opening| opening != call)
        || child.created_at > job.created_at
    {
        return Ok(None);
    }
    let runtime = state::require(tx, conversation)?;
    let title_matches_stored = match &runtime.lifecycle.title {
        Some(stored) => {
            let truncated = &stored[..stored.floor_char_boundary(128)];
            let normalized: String = truncated
                .chars()
                .map(|c| {
                    if matches!(c, '\r' | '\n' | '\t') || jobs::invalid_input_character(c) {
                        ' '
                    } else {
                        c
                    }
                })
                .collect();
            raw_title == normalized.trim() || raw_title == truncated
        }
        None => raw_title == id,
    };
    let state_matches_stored = runtime
        .state
        .outcome
        .as_ref()
        .is_some_and(|stored| stored.kind == state);
    Ok(Some(ParsedReportHeader {
        conversation,
        call,
        state,
        title_matches_stored,
        state_matches_stored,
    }))
}

fn is_report_fence(line: &str) -> bool {
    if let Some(rest) = line.strip_prefix("~~~") {
        if rest.is_empty() {
            return true;
        }
        if rest.len() <= 13 && rest.bytes().all(|b| b == b'~') {
            return true;
        }
        if let Some(digits) = rest.strip_prefix('-') {
            return !digits.is_empty()
                && digits.len() <= 10
                && digits.bytes().all(|b| b.is_ascii_digit());
        }
    }
    false
}

fn report_offset(byte_pos: usize) -> Result<u32, StorageError> {
    u32::try_from(byte_pos)
        .map_err(|_| StorageError::corrupted("child report byte offset overflow"))
}

fn parse_fenced_child_results(
    tx: &impl TxRead,
    job: &MutableJob,
    report: &str,
) -> Result<Option<Vec<ParsedChildResult>>, StorageError> {
    if !report.starts_with("\n\n[") {
        return Ok(None);
    }
    let Some((_, after_first_heading)) = report[3..].split_once('\n') else {
        return Ok(None);
    };
    let Some((fence, _)) = after_first_heading.split_once('\n') else {
        return Ok(None);
    };
    if !is_report_fence(fence) {
        return Ok(None);
    }
    let open_marker = format!("{fence}\n");
    let close_marker = format!("\n{fence}");
    let mut pos = 0usize;
    let mut origin = None;
    let mut results: Vec<ParsedChildResult> = Vec::new();
    while pos < report.len() {
        if results.len() == job_tree::MAX_LIVE_CHILDREN || !report[pos..].starts_with("\n\n[") {
            return Ok(None);
        }
        let heading_start = pos + 3;
        let Some((heading, rest)) = report[heading_start..].split_once('\n') else {
            return Ok(None);
        };
        let Some(parsed) = parse_report_header(tx, job, heading, origin)? else {
            return Ok(None);
        };
        if results
            .last()
            .is_some_and(|previous| parsed.conversation <= previous.conversation)
        {
            return Ok(None);
        }
        if !rest.starts_with(&open_marker) {
            return Ok(None);
        }
        let answer_start = heading_start + heading.len() + 1 + open_marker.len();
        let Some(rel_end) = report[answer_start..].find(&close_marker) else {
            return Ok(None);
        };
        let answer_end = answer_start + rel_end;
        let next_pos = answer_end + close_marker.len();
        if next_pos < report.len() && !report[next_pos..].starts_with("\n\n[") {
            return Ok(None);
        }
        origin = Some(parsed.call);
        results.push(ParsedChildResult {
            conversation: parsed.conversation,
            call: parsed.call,
            state: parsed.state,
            answer_start: report_offset(job_tree::REPORT_HEADER.len() + answer_start)?,
            answer_end: report_offset(job_tree::REPORT_HEADER.len() + answer_end)?,
        });
        pos = next_pos;
    }
    Ok((!results.is_empty()).then_some(results))
}

fn parse_unfenced_child_results(
    tx: &impl TxRead,
    job: &MutableJob,
    text: &str,
    report: &str,
) -> Result<Vec<ParsedChildResult>, StorageError> {
    struct Candidate {
        start: usize,
        answer_start: usize,
        parsed: ParsedReportHeader,
    }
    let mut candidates: Vec<Candidate> = Vec::new();
    let mut origin = None;
    for (start, _) in report.match_indices("\n\n[") {
        let Some((heading, _)) = report[start + 3..].split_once('\n') else {
            continue;
        };
        let Some(parsed) = parse_report_header(tx, job, heading, origin)? else {
            continue;
        };
        if candidates.is_empty() {
            if start != 0 {
                return Ok(Vec::new());
            }
            origin = Some(parsed.call);
        }
        let answer_start = start + 3 + heading.len() + 1;
        candidates.push(Candidate {
            start,
            answer_start,
            parsed,
        });
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let mut best_for_conv: BTreeMap<JobId, usize> = BTreeMap::new();
    for (idx, candidate) in candidates.iter().enumerate() {
        let conv = candidate.parsed.conversation;
        match best_for_conv.get(&conv).copied() {
            None => {
                best_for_conv.insert(conv, idx);
            }
            Some(existing_idx) => {
                if existing_idx == 0 {
                    continue;
                }
                let existing = &candidates[existing_idx].parsed;
                let existing_score = u8::from(existing.title_matches_stored)
                    + u8::from(existing.state_matches_stored);
                let new_score = u8::from(candidate.parsed.title_matches_stored)
                    + u8::from(candidate.parsed.state_matches_stored);
                if new_score >= existing_score {
                    best_for_conv.insert(conv, idx);
                }
            }
        }
    }
    let end = report_offset(text.len())?;
    let mut results: Vec<ParsedChildResult> = Vec::new();
    let mut min_next_start = 0usize;
    for (idx, candidate) in candidates.into_iter().enumerate() {
        if best_for_conv.get(&candidate.parsed.conversation).copied() != Some(idx) {
            continue;
        }
        if candidate.start < min_next_start {
            continue;
        }
        if results
            .last()
            .is_some_and(|previous| candidate.parsed.conversation <= previous.conversation)
        {
            continue;
        }
        if let Some(previous) = results.last_mut() {
            previous.answer_end = report_offset(job_tree::REPORT_HEADER.len() + candidate.start)?;
        }
        results.push(ParsedChildResult {
            conversation: candidate.parsed.conversation,
            call: candidate.parsed.call,
            state: candidate.parsed.state,
            answer_start: report_offset(job_tree::REPORT_HEADER.len() + candidate.answer_start)?,
            answer_end: end,
        });
        min_next_start = candidate.answer_start;
        if results.len() == job_tree::MAX_LIVE_CHILDREN {
            break;
        }
    }
    Ok(results)
}

fn child_segment_heads_before(
    tx: &impl TxRead,
    parent: JobId,
    child: JobId,
    before_turn: JobId,
    seq0_submissions: &BTreeMap<JobId, SubmissionKey>,
) -> Result<Vec<JobId>, StorageError> {
    let recurrence = tx
        .table(RECURRENCES)?
        .get(child.get())?
        .map(|row| Recurrence::decode(row.value()))
        .transpose()?;
    let max_n = recurrence.as_ref().map_or(0, |r| r.n);
    let turn_ids = {
        let turns = tx.table(TURNS)?;
        turns
            .range((child.get(), 0)..(child.get(), before_turn.get()))?
            .map(|entry| Ok(JobId::new(entry?.0.value().1)))
            .collect::<Result<Vec<_>, StorageError>>()?
    };
    let child_turns = {
        let jobs = tx.table(JOBS)?;
        turn_ids
            .into_iter()
            .map(|id| {
                let row = jobs.get(id.get())?.ok_or_else(|| {
                    StorageError::corrupted("child turn missing during migration")
                })?;
                jobs::decode_mutable_job(id.get(), row.value())
            })
            .collect::<Result<Vec<_>, StorageError>>()?
    };
    let mut heads = Vec::new();
    let mut current_head = None;
    let mut in_segment = false;
    let mut settled_non_planning = Vec::new();
    for turn in child_turns {
        let id = turn.id;
        if reads::jobs::project_planning_target(tx, &turn)?.is_some() {
            continue;
        }
        let settled = state::require(tx, id)?.state.outcome.is_some();
        if settled {
            settled_non_planning.push(id);
        }
        let is_recurrence_starter = turn.retry.is_none()
            && turn.submitted_by == Actor::Auto
            && recurrence.as_ref().is_some_and(|r| {
                r.current_turn == id
                    || r.previous_turn == Some(id)
                    || (2..=max_n.min(u64::from(
                        crate::db::entities::JobLimits::CEILING.max_iterations,
                    )))
                        .any(|n| {
                            seq0_submissions.get(&id)
                                == Some(
                                    &settlement::recurrence_submission(
                                        child.get(),
                                        n,
                                        b"recurrence-turn-create",
                                    )
                                    .key,
                                )
                        })
            });
        let is_agent_starter = turn.retry.is_none()
            && turn.submitted_by
                == (Actor::Agent {
                    conversation: parent,
                });
        if is_agent_starter || is_recurrence_starter {
            if let Some(prev) = current_head.take() {
                heads.push(prev);
            }
            in_segment = true;
            current_head = settled.then_some(id);
        } else if turn.retry.is_none() && turn.submitted_by == Actor::User {
            if let Some(prev) = current_head.take() {
                heads.push(prev);
            }
            in_segment = false;
        } else if in_segment {
            current_head = settled.then_some(id);
        }
    }
    if let Some(head) = current_head {
        heads.push(head);
    }
    if heads.is_empty() {
        heads = settled_non_planning;
    }
    Ok(heads)
}

fn resolve_report_heads(
    tx: &impl TxRead,
    job: &MutableJob,
    parsed: &[ParsedChildResult],
    seq0_submissions: &BTreeMap<JobId, SubmissionKey>,
    claimed_heads: &mut BTreeMap<JobId, BTreeSet<JobId>>,
) -> Result<Vec<ChildTurnResult>, StorageError> {
    let mut results = Vec::with_capacity(parsed.len());
    for item in parsed {
        let all_heads = child_segment_heads_before(
            tx,
            job.parent_id,
            item.conversation,
            job.id,
            seq0_submissions,
        )?;
        let unclaimed: Vec<JobId> = all_heads
            .iter()
            .copied()
            .filter(|head| {
                !claimed_heads
                    .get(&item.conversation)
                    .is_some_and(|used| used.contains(head))
            })
            .collect();
        let candidates = if unclaimed.is_empty() {
            &all_heads
        } else {
            &unclaimed
        };
        let head = if candidates.len() == 1 {
            candidates[0]
        } else if parsed.len() == 1
            && let Some(&key) = seq0_submissions.get(&job.id)
            && let Some(&matched) = candidates.iter().find(|&&head| {
                job_tree::tool_submission(item.call, b"results create", head.get() as u32).key
                    == key
            })
        {
            matched
        } else {
            *candidates.first().ok_or_else(|| {
                StorageError::corrupted("child report turn has no child head turn")
            })?
        };
        claimed_heads
            .entry(item.conversation)
            .or_default()
            .insert(head);
        results.push(ChildTurnResult {
            child: item.conversation,
            head,
            outcome: item.state,
            answer_start: item.answer_start,
            answer_end: item.answer_end,
        });
    }
    Ok(results)
}

#[derive(Default, PartialEq, Eq)]
struct LegacyChildTurns {
    reports: BTreeMap<u128, Vec<ChildTurnResult>>,
    notices: BTreeMap<u128, ToolCallRef>,
}

impl LegacyChildTurns {
    fn read(tx: &impl TxRead) -> Result<Self, StorageError> {
        let mut seq0_submissions = BTreeMap::new();
        for entry in tx.table(SUBMISSIONS)?.iter()? {
            let (key, value) = entry?;
            let record = rows::decode_submission(key.value(), value.value())?;
            if record.seq == 0 {
                seq0_submissions.insert(record.job, record.key);
            }
        }
        let mut notice_calls = BTreeMap::new();
        {
            let submissions = tx.table(SUBMISSIONS)?;
            for entry in tx.table(TOOL_CALLS)?.iter()? {
                let (key, value) = entry?;
                let call = rows::tool_call_ref(key.value())?;
                let tool = rows::decode_tool_evidence(call, value.value())?;
                let kind = match tool.name.as_str() {
                    "job_report_progress" => ChildNoticeKind::Progress,
                    "job_ask_parent" => ChildNoticeKind::Help,
                    _ => continue,
                };
                let key = job_tree::tool_submission(call, b"child notice", 0).key;
                if let Some(row) = submissions.get(key.as_bytes().as_slice())? {
                    let record = rows::decode_submission(key.as_bytes(), row.value())?;
                    notice_calls.insert(record.job.get(), (call, kind));
                }
            }
        }
        let candidates = {
            let jobs = tx.table(JOBS)?;
            let mut candidates = Vec::new();
            for entry in jobs.iter()? {
                let (key, bytes) = entry?;
                let job = jobs::decode_mutable_job(key.value(), bytes.value())?;
                if job.retry.is_some() || job.summary_source.is_some() {
                    continue;
                }
                let is_candidate = match job.submitted_by {
                    Actor::Agent { .. } => {
                        matches!(
                            job.input.text().split_once('\n'),
                            Some((
                                job_tree::CHILD_PROGRESS_HEADER | job_tree::CHILD_HELP_HEADER,
                                _
                            ))
                        )
                    }
                    Actor::Auto => job.input.text().starts_with(job_tree::REPORT_HEADER),
                    Actor::User | Actor::Loop => false,
                };
                if is_candidate {
                    candidates.push(job);
                }
            }
            candidates
        };
        let mut turns = Self::default();
        let mut claimed_heads = BTreeMap::new();
        for job in candidates {
            let id = job.id.get();
            if let Actor::Agent { conversation } = job.submitted_by {
                let text = job.input.text();
                if let Some((header, _)) = text.split_once('\n') {
                    let header_kind = match header {
                        job_tree::CHILD_PROGRESS_HEADER => Some(ChildNoticeKind::Progress),
                        job_tree::CHILD_HELP_HEADER => Some(ChildNoticeKind::Help),
                        _ => None,
                    };
                    if let Some(kind) = header_kind
                        && text.split_once("\n\n").is_some()
                        && let Some(source) = reads::jobs::read_conversation_row(tx, conversation)?
                        && source.parent_id == Some(job.parent_id)
                    {
                        let Some(&(call, call_kind)) = notice_calls.get(&id) else {
                            return Err(StorageError::corrupted(
                                "child notice turn has no matching tool call submission",
                            ));
                        };
                        if call_kind != kind {
                            return Err(StorageError::corrupted(
                                "child notice turn kind disagrees with tool call",
                            ));
                        }
                        turns.notices.insert(id, call);
                    }
                }
                continue;
            }
            if job.submitted_by != Actor::Auto {
                continue;
            }
            let text = job.input.text();
            let Some(report) = text.strip_prefix(job_tree::REPORT_HEADER) else {
                continue;
            };
            let parsed = if let Some(fenced) = parse_fenced_child_results(tx, &job, report)? {
                fenced
            } else {
                parse_unfenced_child_results(tx, &job, text, report)?
            };
            if parsed.is_empty() {
                continue;
            }
            let results =
                resolve_report_heads(tx, &job, &parsed, &seq0_submissions, &mut claimed_heads)?;
            turns.reports.insert(id, results);
        }
        Ok(turns)
    }

    fn apply(&self, tx: &WriteTransaction) -> Result<(), StorageError> {
        let mut jobs = tx.open_table(JOBS)?;
        for (&id, results) in &self.reports {
            let mut job = {
                let row = jobs.get(id)?.ok_or_else(|| {
                    StorageError::corrupted("child report turn missing during migration")
                })?;
                jobs::decode_mutable_job(id, row.value())?
            };
            job.child_results = results.clone();
            jobs.insert(id, jobs::encode_mutable_job(&job).as_slice())?;
        }
        for (&id, &call) in &self.notices {
            let mut job = {
                let row = jobs.get(id)?.ok_or_else(|| {
                    StorageError::corrupted("child notice turn missing during migration")
                })?;
                jobs::decode_mutable_job(id, row.value())?
            };
            job.child_notice = Some(call);
            jobs.insert(id, jobs::encode_mutable_job(&job).as_slice())?;
        }
        Ok(())
    }
}

fn promote_runtimes(
    versions: Versions,
    source: &ReadTransaction,
    tx: &WriteTransaction,
) -> Result<(), StorageError> {
    let mut runtime = tx.open_table(JOB_RUNTIME)?;
    for entry in source.open_table(JOB_RUNTIME)?.iter()? {
        let (k, v) = entry?;
        runtime.insert(
            k.value(),
            migrate_runtime(versions, k.value(), v.value())?.as_slice(),
        )?;
    }
    Ok(())
}

enum Membership {
    Job { open: bool },
    Conversation { children: u32 },
}

fn add_child(members: &mut BTreeMap<JobId, Membership>, id: JobId) -> Result<(), StorageError> {
    let Some(Membership::Conversation { children }) = members.get_mut(&id) else {
        return Err(StorageError::corrupted(format!(
            "missing parent conversation {id}"
        )));
    };
    *children = children.checked_add(1).ok_or_else(|| {
        StorageError::corrupted(format!("conversation {id} child count overflow"))
    })?;
    Ok(())
}

fn execution_membership(tx: &impl TxRead) -> Result<BTreeMap<JobId, Membership>, StorageError> {
    let mut members = BTreeMap::new();
    // Conversation keys precede their children.
    for entry in tx.table(CONVERSATIONS)?.iter()? {
        let (key, bytes) = entry?;
        let row = JobSpec::decode_at(key.value(), bytes.value())?;
        if let Some(parent) = row.parent_id
            && state::require(tx, row.id)?.state.outcome.is_none()
        {
            add_child(&mut members, parent)?;
        }
        members.insert(row.id, Membership::Conversation { children: 0 });
    }
    let allocations = tx.table(POOL_ALLOCATIONS)?;
    for entry in tx.table(JOBS)?.iter()? {
        let (key, bytes) = entry?;
        let job = jobs::decode_mutable_job(key.value(), bytes.value())?;
        let allocation = allocations
            .get(key.value())?
            .map(|row| allocations::decode_pool_allocation(key.value(), row.value()))
            .transpose()?;
        let open = job.phase != MutableJobPhase::Unsubmitted
            && state::has_open_membership(
                state::require(tx, job.id)?.state.outcome.as_ref(),
                allocation.as_ref(),
            );
        if open {
            add_child(&mut members, job.parent_id)?;
        }
        members.insert(job.id, Membership::Job { open });
    }
    Ok(members)
}

#[derive(Default)]
struct MembershipChanges {
    jobs: usize,
    conversations: usize,
    closing: Vec<String>,
}

impl Membership {
    fn pending(&self, row: &state::JobRuntime) -> Result<Option<(u32, u32)>, StorageError> {
        let id = row.state.job_id;
        match self {
            Self::Job { open: false } => Ok(None),
            Self::Job { open: true } => {
                let (dependencies, _) = row.pending.ok_or_else(|| {
                    StorageError::corrupted(format!("open Job {id} has no pending counts"))
                })?;
                Ok(Some((dependencies, 0)))
            }
            Self::Conversation { children } => {
                if row
                    .pending
                    .is_some_and(|(dependencies, _)| dependencies != 0)
                {
                    return Err(StorageError::corrupted(format!(
                        "conversation {id} has nonzero stored dependencies"
                    )));
                }
                Ok(row.state.outcome.is_none().then_some((0, *children)))
            }
        }
    }
}

fn membership_for(
    members: &BTreeMap<JobId, Membership>,
    id: JobId,
) -> Result<&Membership, StorageError> {
    members
        .get(&id)
        .ok_or_else(|| StorageError::corrupted(format!("runtime {id} has no Job or conversation")))
}

fn migrate_membership(
    source: &ReadTransaction,
    tx: &WriteTransaction,
) -> Result<MembershipChanges, StorageError> {
    let members = execution_membership(tx)?;
    let mut changes = MembershipChanges::default();
    for entry in source.open_table(JOB_RUNTIME)?.iter()? {
        let id = JobId::new(entry?.0.value());
        let mut row = state::require(tx, id)?;
        let member = membership_for(&members, id)?;
        let pending = member.pending(&row)?;
        match member {
            Membership::Job { .. } => changes.jobs += usize::from(pending != row.pending),
            Membership::Conversation { .. } => {
                changes.conversations += usize::from(pending != row.pending);
                if pending == Some((0, 0)) && row.lifecycle.closing.is_some() {
                    changes.closing.push(id.to_string());
                }
            }
        }
        if pending != row.pending {
            row.pending = pending;
            state::write(tx, &row)?;
        }
    }
    Ok(changes)
}

fn migrate_outputs(
    versions: Versions,
    source: &ReadTransaction,
    tx: &WriteTransaction,
) -> Result<(), StorageError> {
    let mut outputs = tx.open_table(JOB_OUTPUTS)?;
    for entry in source.open_table(JOB_OUTPUTS)?.iter()? {
        let (k, v) = entry?;
        let mut row = old_output(versions, k.value(), v.value())?;
        convert_output(tx, &mut row, &mut ConversionCounts::default())?;
        outputs.insert(k.value(), rows::encode_output(&row).as_slice())?;
    }
    Ok(())
}

fn migrate_chain(
    database: &Database,
    directory: &Path,
    sha: &str,
) -> Result<Json, ReservedRefusal> {
    let source = database.begin_read().map_err(StorageError::from)?;
    let versions = source_layout(&source)?;
    let mut tx = database.begin_write().map_err(StorageError::from)?;
    tx.set_durability(Durability::Immediate)
        .map_err(StorageError::from)?;
    if tx
        .list_persistent_savepoints()
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        return Err(ReservedRefusal::new(
            "unsupported_source",
            "the source contains persistent redb savepoints",
        ));
    }
    tx.open_table(PENDING_CHILD_NOTICES)
        .map_err(StorageError::from)?;
    rewrite_record_headers(versions, &source, &tx)?;
    {
        let mut meta = tx.open_table(META).map_err(StorageError::from)?;
        for entry in source
            .open_table(META)
            .map_err(StorageError::from)?
            .iter()
            .map_err(StorageError::from)?
        {
            let (k, v) = entry.map_err(StorageError::from)?;
            meta.insert(
                k.value(),
                promote_metadata(versions, k.value(), v.value())?.as_slice(),
            )
            .map_err(StorageError::from)?;
        }
    }
    let evidence = if versions.crosses(3) {
        Some(AuthorEvidence::read(&tx)?)
    } else {
        None
    };
    migrate_turn_bases(versions, &source, &tx, evidence.as_ref())?;
    promote_runtimes(versions, &source, &tx)?;
    let changes = migrate_membership(&source, &tx)?;
    // Format 1 output conversion reads the final Job and runtime layouts.
    if versions.crosses(2) {
        migrate_outputs(versions, &source, &tx)?;
    }
    let notices = if versions.crosses(6) {
        let notices = LegacyNotices::read(&tx)?;
        notices.apply(&tx)?;
        Some(notices)
    } else {
        None
    };
    let child_turns = LegacyChildTurns::read(&tx)?;
    child_turns.apply(&tx)?;
    tx.commit().map_err(StorageError::from)?;

    super::verify_reserved_residual(database)?;
    let result = database.begin_read().map_err(StorageError::from)?;
    verify_record_headers(versions, &source, &result, notices.as_ref())?;
    verify_payloads(&result, directory)?;
    if LegacyChildTurns::read(&result)? != child_turns {
        return Err(ReservedRefusal::new(
            "audit_failed",
            "migration child turn metadata changed on re-evaluation",
        ));
    }
    let mut authors = BTreeMap::from([("user", 0u64), ("auto", 0), ("agent", 0)]);
    let mut classes = BTreeMap::new();
    verify_transformed(&source, &result, JOBS, |key, bytes| {
        let migrated = migrate_job_base(versions, key, bytes, &result, evidence.as_ref(), &source)?;
        let mut row = migrated.row;
        if let Some((author, class)) = migrated.author {
            let name = match author {
                Actor::User => "user",
                Actor::Agent { .. } => "agent",
                Actor::Auto => "auto",
                Actor::Loop => return Err(StorageError::corrupted("invalid Job author")),
            };
            *authors.entry(name).or_default() += 1;
            *classes.entry(class).or_insert(0u64) += 1;
        }
        if let Some(results) = child_turns.reports.get(&key) {
            let mut job = jobs::decode_mutable_job(key, &row)?;
            job.child_results = results.clone();
            row = jobs::encode_mutable_job(&job);
        } else if let Some(&call) = child_turns.notices.get(&key) {
            let mut job = jobs::decode_mutable_job(key, &row)?;
            job.child_notice = Some(call);
            row = jobs::encode_mutable_job(&job);
        }
        Ok(row)
    })?;
    let members = execution_membership(&result)?;
    verify_transformed(&source, &result, JOB_RUNTIME, |key, bytes| {
        let prior = migrate_runtime(versions, key, bytes)?;
        let mut row = state::JobRuntime::decode_at(key, &prior)?;
        row.pending = membership_for(&members, row.state.job_id)?.pending(&row)?;
        Ok(row.encode())
    })?;
    let counts = if versions.crosses(2) {
        verify_outputs(versions, &source, &result)?
    } else {
        ConversionCounts::default()
    };
    use ::redb::ReadableTableMetadata;
    let historical_calls = if versions.crosses(4) {
        source
            .open_table(TOOL_CALLS)
            .map_err(StorageError::from)?
            .len()
            .map_err(StorageError::from)?
    } else {
        0
    };
    let mut plan = json!({"command_groups_not_recorded": historical_calls, "kind": "storage-migration", "source_sha256": sha, "source_format": versions.from,
        "target_format": versions.to, "submitted_by": authors, "creation_classes": classes,
        "converted_summary_links": counts.converted_summary_links, "retained_summary_links": counts.retained_summary_links,
        "kept_text_job_count": counts.kept_text_jobs.len(), "kept_text_jobs": counts.kept_text_jobs.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "jobs_changed": changes.jobs, "conversations_changed": changes.conversations,
        "closing_without_children": changes.closing, "kept_text_details": counts.kept_text_details});
    if let Some(notices) = &notices
        && (!notices.pending.is_empty() || !notices.dropped.is_empty())
    {
        plan["child_notices"] = json!({
            "queued": notices.pending.len(),
            "dropped": notices.dropped.len(),
        });
    }
    if !child_turns.reports.is_empty() || !child_turns.notices.is_empty() {
        plan["child_turns"] = json!({
            "reports": child_turns.reports.len(),
            "notices": child_turns.notices.len(),
        });
    }
    let encoded = serde_json::to_vec(&plan)
        .map_err(|e| ReservedRefusal::new("invalid_plan", e.to_string()))?;
    Ok(json!({"plan": plan, "plan_digest": ContentDigest::of(&encoded).to_string()}))
}

pub(crate) fn migrate_storage(
    file: File,
    directory: &Path,
    sha: &str,
    expected: Option<&str>,
) -> Result<Json, ReservedRefusal> {
    with_verified_rebuild_copy(file, |database| {
        let evidence = migrate_chain(database, directory, sha)?;
        if expected.is_some_and(|expected| evidence["plan_digest"].as_str() != Some(expected)) {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the source or migration plan changed; repeat the dry-run",
            ));
        }
        Ok(evidence)
    })
}

pub(crate) fn verify_storage_migration(
    directory: &Path,
    source: &File,
    published: &File,
    sha: &str,
    expected_plan: &Json,
    expected_digest: &str,
) -> Result<Json, ReservedRefusal> {
    with_verified_rebuild_copy(finalization_copy(source)?, |source| {
        let evidence = migrate_chain(source, directory, sha)?;
        if evidence["plan"] != *expected_plan
            || evidence["plan_digest"].as_str() != Some(expected_digest)
        {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the migration differs from the authorized plan",
            ));
        }
        with_verified_rebuild_copy(finalization_copy(published)?, |published| {
            let expected = source.begin_read().map_err(StorageError::from)?;
            let actual = published.begin_read().map_err(StorageError::from)?;
            check_required_tables(&actual)?;
            verify_identical_database(&expected, &actual)?;
            Ok(evidence)
        })
    })
}
