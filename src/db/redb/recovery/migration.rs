use super::offline::{finalization_copy, verify_payloads, with_verified_rebuild_copy};
use super::reserved::ReservedRefusal;
use crate::db::entities::{
    ContentDigest, JobId, JobOutputId, JobOutputRecord, MutableJobContent, NativeSummaryResultLink,
    OutcomeKind, OutputKind, RequestNumber,
};
use crate::db::redb::codec::{Code, Dec, TAG_GLOBAL_COMPACTION, TAG_GLOBAL_HEADERS, TAG_OUTPUT};
use crate::db::redb::execution::TAG_EXECUTION_EVENT;
use crate::db::redb::schema::{
    DATABASE_FORMAT_KEY, FORMAT_VERSION, JOB_OUTPUTS, JOBS, MESSAGES, META, REQUEST_EVENTS,
    SCHEMA_FINGERPRINT, SCHEMA_FINGERPRINT_KEY, check_required_tables, record_version_byte,
    rewrite_format1_records, verify_format1_records,
};
use crate::db::redb::{StorageError, TxRead, indexes, jobs, observations, rows, state};
use ::redb::{
    Database, Durability, Key, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition,
    TableHandle, WriteTransaction,
};
use serde_json::{Value as Json, json};
use std::{fs::File, path::Path};
const SOURCE_FORMAT: u64 = 1;

fn source_layout(source: &ReadTransaction) -> Result<(), ReservedRefusal> {
    let meta = source.open_table(META).map_err(StorageError::from)?;
    let format = meta.get(DATABASE_FORMAT_KEY).map_err(StorageError::from)?;
    let fingerprint = meta
        .get(SCHEMA_FINGERPRINT_KEY)
        .map_err(StorageError::from)?;
    let fingerprint = fingerprint.as_ref().map(|marker| marker.value());
    match format.as_ref().map(|marker| marker.value()) {
        Some(bytes) if bytes == SOURCE_FORMAT.to_le_bytes() => {}
        Some(bytes)
            if bytes == FORMAT_VERSION.to_le_bytes() && fingerprint == Some(SCHEMA_FINGERPRINT) =>
        {
            return Err(ReservedRefusal::new(
                "already_migrated",
                "the database is already in Format 2",
            ));
        }
        _ => {
            return Err(ReservedRefusal::new(
                "unsupported_source",
                "migration requires Format 1",
            ));
        }
    }
    if fingerprint != Some(b"ocg-native-format-1".as_slice()) {
        return Err(ReservedRefusal::new(
            "unsupported_source",
            "migration requires the ocg-native-format-1 fingerprint",
        ));
    }
    check_required_tables(source)?;
    Ok(())
}

fn promote_header(bytes: &mut [u8], tags: &[u8]) -> Result<(), StorageError> {
    match bytes {
        [tag, version, ..] if tags.contains(tag) && u64::from(*version) == SOURCE_FORMAT => {
            *version = record_version_byte();
            Ok(())
        }
        _ => Err(StorageError::corrupted(format!(
            "invalid Format 1 record header for {tags:?}"
        ))),
    }
}

fn promote_record(bytes: &[u8], tags: &[u8]) -> Result<Vec<u8>, StorageError> {
    let mut row = bytes.to_vec();
    promote_header(&mut row, tags)?;
    if row[0] == TAG_EXECUTION_EVENT {
        let mut d = Dec::new(&row, TAG_EXECUTION_EVENT)?;
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
            promote_header(&mut row[start..], &[42])?;
        }
    }
    Ok(row)
}

pub(in crate::db::redb) fn rewrite_tagged<K: Key + 'static>(
    source: &ReadTransaction,
    tx: &WriteTransaction,
    definition: TableDefinition<'_, K, &'static [u8]>,
    tags: &[u8],
) -> Result<(), StorageError> {
    let mut result = tx.open_table(definition)?;
    for entry in source.open_table(definition)?.iter()? {
        let (key, bytes) = entry?;
        result.insert(key.value(), promote_record(bytes.value(), tags)?.as_slice())?;
    }
    Ok(())
}

fn promote_metadata(key: &str, value: &[u8]) -> Result<Vec<u8>, StorageError> {
    match key {
        SCHEMA_FINGERPRINT_KEY => Ok(SCHEMA_FINGERPRINT.to_vec()),
        DATABASE_FORMAT_KEY => Ok(FORMAT_VERSION.to_le_bytes().to_vec()),
        "compaction_settings" => promote_record(value, &[TAG_GLOBAL_COMPACTION]),
        "outbound_headers" => promote_record(value, &[TAG_GLOBAL_HEADERS]),
        "open_epoch" | "clean_shutdown" => Ok(value.to_vec()),
        _ => Err(StorageError::corrupted(format!(
            "unrecognized Format 1 metadata row {key:?}"
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
    source: &ReadTransaction,
    result: &ReadTransaction,
    definition: TableDefinition<'_, K, &'static [u8]>,
    tags: &[u8],
) -> Result<(), StorageError> {
    verify_transformed(source, result, definition, |_, bytes| {
        promote_record(bytes, tags)
    })
}

pub(in crate::db::redb) fn verify_metadata(
    source: &ReadTransaction,
    result: &ReadTransaction,
) -> Result<(), StorageError> {
    verify_transformed(source, result, META, promote_metadata)
}

fn old_output(key: (u128, u32), bytes: &[u8]) -> Result<JobOutputRecord, StorageError> {
    let row = promote_record(bytes, &[TAG_OUTPUT])?;
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
        let prior = old_output(key, v.value())?;
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

fn verify_delta(
    source: &ReadTransaction,
    database: &Database,
    directory: &Path,
    sha: &str,
) -> Result<Json, ReservedRefusal> {
    source_layout(source)?;
    super::verify_reserved_residual(database)?;
    let result = database.begin_read().map_err(StorageError::from)?;
    verify_format1_records(source, &result)?;
    verify_payloads(&result, directory)?;
    let counts = verify_outputs(source, &result)?;
    let plan = json!({"kind": "migrate-answers-f2", "source_sha256": sha, "source_format": SOURCE_FORMAT,
        "target_format": FORMAT_VERSION, "converted_summary_links": counts.converted_summary_links, "retained_summary_links": counts.retained_summary_links,
        "kept_text_job_count": counts.kept_text_jobs.len(), "kept_text_jobs": counts.kept_text_jobs.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "kept_text_details": counts.kept_text_details});
    let encoded = serde_json::to_vec(&plan)
        .map_err(|e| ReservedRefusal::new("invalid_plan", e.to_string()))?;
    Ok(json!({"plan": plan, "plan_digest": ContentDigest::of(&encoded).to_string()}))
}

pub(crate) fn migrate_answers(
    file: File,
    directory: &Path,
    sha: &str,
    expected: Option<&str>,
) -> Result<Json, ReservedRefusal> {
    with_verified_rebuild_copy(file, |database| {
        let source = database.begin_read().map_err(StorageError::from)?;
        source_layout(&source)?;
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
        rewrite_format1_records(&source, &tx)?;
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
                    promote_metadata(k.value(), v.value())?.as_slice(),
                )
                .map_err(StorageError::from)?;
            }
            let mut outputs = tx.open_table(JOB_OUTPUTS).map_err(StorageError::from)?;
            for entry in source
                .open_table(JOB_OUTPUTS)
                .map_err(StorageError::from)?
                .iter()
                .map_err(StorageError::from)?
            {
                let (k, v) = entry.map_err(StorageError::from)?;
                let mut row = old_output(k.value(), v.value())?;
                convert_output(&tx, &mut row, &mut ConversionCounts::default())?;
                outputs
                    .insert(k.value(), rows::encode_output(&row).as_slice())
                    .map_err(StorageError::from)?;
            }
        }
        indexes::rebuild_compaction_refs(&tx)?;
        tx.commit().map_err(StorageError::from)?;
        let evidence = verify_delta(&source, database, directory, sha)?;
        if expected.is_some_and(|expected| evidence["plan_digest"].as_str() != Some(expected)) {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the source or migration plan changed; repeat the dry-run",
            ));
        }
        Ok(evidence)
    })
}

pub(crate) fn verify_answer_migration(
    directory: &Path,
    source: &File,
    published: &File,
    sha: &str,
    expected_plan: &Json,
    expected_digest: &str,
) -> Result<Json, ReservedRefusal> {
    with_verified_rebuild_copy(finalization_copy(source)?, |source| {
        let tx = source.begin_read().map_err(StorageError::from)?;
        with_verified_rebuild_copy(finalization_copy(published)?, |published| {
            let evidence = verify_delta(&tx, published, directory, sha)?;
            let mut verified_plan = evidence["plan"].clone();
            if expected_plan.get("kept_text_details").is_none()
                && let Some(plan) = verified_plan.as_object_mut()
            {
                plan.remove("kept_text_details");
            }
            let encoded = serde_json::to_vec(&verified_plan)
                .map_err(|error| ReservedRefusal::new("invalid_plan", error.to_string()))?;
            if verified_plan != *expected_plan
                || ContentDigest::of(&encoded).to_string() != expected_digest
            {
                return Err(ReservedRefusal::new(
                    "plan_digest_mismatch",
                    "the migration differs from the authorized plan",
                ));
            }
            Ok(evidence)
        })
    })
}
