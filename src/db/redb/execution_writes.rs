use super::rows::{decode_request_event, encode_request_event};
use super::{StorageError, jobs, observations};
use crate::db::entities::{
    AppendedOutput, AvailabilityStatId, ConfigId, ContentDigest, ExecutionEventKind,
    ExecutionOperation, JobId, JobOutputId, JobOutputRecord, Measurements, Metrics, MetricsId,
    MetricsRecord, MutableJob, OutcomeKind, OutputKind, RecordRef, RequestEventRecord,
    RequestNumber, RequestPayload, RequestRef, Timestamp, TokenAnnotation, TokenEvidence,
    ToolCallRef, Violation,
};
use crate::db::handoff::DevelopmentUsageCheckpoint;
use crate::db::redb::batch::{Batch, BatchError, Failure, Probe};
use crate::db::redb::jobs::encode_mutable_job;
use crate::db::redb::reads::execution_context::summary_child_job;
use crate::db::redb::rows::encode_output;
use crate::db::redb::schema::{
    CHILDREN, CONVERSATIONS, JOB_OBSERVATIONS, JOB_OUTPUTS, JOB_RUNTIME, JOBS, META,
    REQUEST_EVENTS, REQUEST_TIMELINE, TOOL_CALLS, USAGE_TOTALS,
};
use crate::db::redb::{indexes, reads, rows, state};
use ::redb::ReadableTable;

struct PreparedUsage {
    measurement_id: u128,
    token_id: u128,
    observed_at: Timestamp,
    measurements: Measurements,
    token: TokenAnnotation,
}

fn hand_over_pieces(
    tx: &::redb::WriteTransaction,
    job: JobId,
    request: RequestNumber,
    body: &[u8],
) -> Result<(), StorageError> {
    let received = serde_json::from_slice(body)
        .map_err(|e| StorageError::corrupted(format!("invalid recorded answer: {e}")))?;
    let mut groups = std::collections::BTreeMap::new();
    let mut outputs = tx.open_table(JOB_OUTPUTS)?;
    let mut first = None;
    for entry in outputs.range((job.get(), 0)..=(job.get(), u32::MAX))?.rev() {
        let (key, bytes) = entry?;
        let row = super::rows::decode_output(key.value(), bytes.value())?;
        if let Some((number, _, _)) = row.part {
            if number < request.get() {
                break;
            }
            if number == request.get() {
                first = Some(key.value().1);
            }
        }
    }
    let Some(first) = first else {
        return Ok(());
    };
    for entry in outputs.range((job.get(), first)..=(job.get(), u32::MAX))? {
        let (key, bytes) = entry?;
        let row = super::rows::decode_output(key.value(), bytes.value())?;
        let Some((n, output, part)) = row.part else {
            continue;
        };
        if n != request.get() {
            continue;
        }
        let Some(text) = super::observations::received_part(&received, row.kind, output, part)
        else {
            continue;
        };
        let (expected, offset, matches, spans) = groups
            .entry((row.kind, output, part))
            .or_insert_with(|| (text, 0usize, true, Vec::new()));
        let piece = row
            .summary
            .as_deref()
            .ok_or_else(|| StorageError::corrupted("streaming piece has no text"))?;
        let end = offset
            .checked_add(piece.len())
            .ok_or_else(|| StorageError::corrupted("streamed part exceeds limit"))?;
        *matches &= expected.get(*offset..end) == Some(piece);
        let span = u32::try_from(*offset)
            .ok()
            .zip(u32::try_from(piece.len()).ok())
            .ok_or_else(|| StorageError::corrupted("streamed part exceeds limit"))?;
        spans.push((key.value(), span));
        *offset = end;
    }
    for ((kind, output, part), (text, end, matches, spans)) in groups {
        if !matches || end != text.len() {
            log::warn!(
                "Job {job} request {} {kind:?} part ({output}, {part}) differs from the recorded answer",
                request.get()
            );
            continue;
        }
        for (key, span) in spans {
            let mut row = {
                let bytes = outputs
                    .get(key)?
                    .ok_or_else(|| StorageError::corrupted("streaming piece missing"))?;
                super::rows::decode_output(key, bytes.value())?
            };
            row.summary = None;
            row.span = Some(span);
            outputs.insert(key, encode_output(&row).as_slice())?;
        }
    }
    Ok(())
}

impl Batch {
    pub(crate) fn append_development_output(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        expected_seq: u32,
        output: crate::db::entities::JobOutput,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        use crate::db::entities::IntermediateOutputKind;
        self.require_development_write_for(proof, Some(ExecutionOperation::LlmRequest))?;
        if !matches!(
            output.kind,
            IntermediateOutputKind::Progress
                | IntermediateOutputKind::Evaluation
                | IntermediateOutputKind::Reasoning
        ) {
            return self.reject(Violation::InvalidValue("output", "unsupported output kind"));
        }
        let job = proof.token().job;
        let last = self.step(|tx| {
            Ok(tx
                .open_table(JOB_OUTPUTS)?
                .range((job.get(), 0)..=(job.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(0, |(key, _)| key.value().1))
        })?;
        if last.checked_add(1) != Some(expected_seq) {
            return self.reject(Violation::InvalidValue(
                "output",
                "sequence is not the next output",
            ));
        }
        let (seq, id) = self.write_output(
            job,
            output.kind.into(),
            output.part,
            output.summary,
            output.code,
            output.goal_reached,
            output.value,
        )?;
        Ok(crate::db::entities::AppendedOutput {
            output: id,
            seq: i64::from(seq),
        })
    }

    pub(crate) fn record_development_sent_request(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        sent: crate::db::entities::SentRequest,
        attachments: &[crate::db::entities::AttachmentRecord],
    ) -> Result<(), BatchError> {
        if self
            .pool_allocation(proof.token().job)?
            .is_some_and(|allocation| {
                allocation.token == proof.token()
                    && allocation.phase == crate::db::entities::AllocationPhase::StopRequested
            })
        {
            return self.reject(Violation::InvalidValue(
                "execution",
                crate::db::entities::STOPPED_BEFORE_SEND,
            ));
        }
        self.scheduled_external_authorization(proof.token().job)?;
        self.require_development_write_for(proof, None)?;
        if sent.job != proof.token().job {
            return self.reject(Violation::InvalidValue(
                "request",
                "wrong Job or request identity",
            ));
        }
        for expected in attachments {
            if !self.require_attachment(expected.id)?.is_available() {
                return self.reject(Violation::InvalidValue(
                    "attachments",
                    "prepared Attachment changed or was cleaned up",
                ));
            }
        }
        if !self.development_request_slot_is_next(sent.job, sent.request)? {
            return self.reject(Violation::InvalidValue(
                "request",
                "the request is not the next one, or its evidence already exists",
            ));
        }
        for body in &sent.bodies {
            self.put_message(body)?;
        }
        self.require_messages(
            sent.bodies
                .iter()
                .map(crate::db::entities::MessageBody::digest)
                .chain(sent.messages.iter().copied())
                .chain(sent.required_messages.iter().copied()),
        )?;
        let event = RequestEventRecord {
            id: crate::db::entities::RequestEventId::new(self.next_id()?),
            job_id: sent.job,
            request: sent.request,
            seq: 1,
            observed_at: sent.observed_at,
            recorded_at: self.now,
            payload: RequestPayload::Sent {
                messages: sent.messages,
                tools: sent.tools,
                parameters: sent.parameters,
                dropped_turns: sent.dropped_turns,
            },
        };
        self.insert(
            REQUEST_EVENTS,
            (event.job_id.get(), event.request.get(), event.seq),
            encode_request_event(&event).as_slice(),
        )?;
        self.record_request_reference(event.job_id, event.request, event.seq)?;
        Ok(())
    }

    pub(super) fn development_usage_identity(
        &mut self,
        job: JobId,
    ) -> Result<reads::jobs::UsageIdentity, BatchError> {
        let Some(row) = self.mutable_job(job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job)));
        };
        let identity = match row.frozen {
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) => {
                reads::jobs::UsageIdentity {
                    provider: frozen.llm.provider.id.get(),
                    model: frozen.llm.config.model,
                }
            }
            Some(crate::db::entities::MutableFrozenConfiguration::Decision(frozen)) => {
                reads::jobs::UsageIdentity {
                    provider: frozen.connection_ref.id.get(),
                    model: frozen.model.model,
                }
            }
            None => {
                return self.reject(Violation::InvalidValue(
                    "execution",
                    "frozen configuration required",
                ));
            }
        };
        Ok(identity)
    }

    pub(in crate::db) fn record_development_provider_answer(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        answer: crate::db::entities::ProviderAnswer,
        usages: Vec<(u32, crate::db::entities::Metrics)>,
        checkpoint: crate::db::handoff::DevelopmentUsageCheckpoint,
    ) -> Result<
        (
            MetricsId,
            AvailabilityStatId,
            Vec<MetricsId>,
            crate::db::handoff::DevelopmentUsageCheckpoint,
        ),
        BatchError,
    > {
        self.require_development_write_for(proof, None)?;
        let job = proof.token().job;
        let stat = answer.stat;
        if stat.job != job {
            return self.reject(Violation::InvalidValue(
                "request",
                "wrong Job or request identity",
            ));
        }
        if answer.measurements.is_empty() || stat.observation.is_empty() {
            return self.reject(Violation::InvalidValue(
                "answer",
                "missing measurements or availability",
            ));
        }
        let usage = &answer.measurements.usage;
        if let (Some(input), Some(output)) = (usage.input_tokens, usage.output_tokens)
            && input.checked_add(output).is_none()
        {
            return self.reject(Violation::ValueOutOfRange("usage"));
        }
        let prefix = self.development_usage_prefix(job, stat.request)?;
        if prefix != Some(checkpoint.last_sequence()) {
            return self.reject(Violation::InvalidValue(
                "response",
                "request missing or answer already recorded",
            ));
        }
        let mut evidence = checkpoint.evidence();
        let ids = [self.next_id()?, self.next_id()?, self.next_id()?];
        let mut prepared_usages = Vec::with_capacity(usages.len());
        let Some(first_answer_seq) = checkpoint.last_sequence().checked_add(1) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        let Some(mut next_seq) = first_answer_seq.checked_add(3) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        for (expected_seq, metrics) in usages {
            if metrics.job != job || metrics.request != stat.request || expected_seq != next_seq {
                return self.reject(Violation::InvalidValue(
                    "usage",
                    "wrong Job, request or event sequence",
                ));
            }
            let Some(token) = metrics.token else {
                return self.reject(Violation::InvalidValue(
                    "usage",
                    "Token annotation required",
                ));
            };
            let Some(token_seq) = expected_seq.checked_add(1) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            let Some(next_expected) = expected_seq.checked_add(2) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            next_seq = next_expected;
            let usage = self.prepare_development_usage(metrics, token, &mut evidence)?;
            prepared_usages.push((expected_seq, token_seq, usage));
        }
        let new = evidence.sample();
        self.step(|_| indexes::validate_native_sample(new.as_ref()))?;
        let identity = self.development_usage_identity(job)?;
        self.put_message(&answer.response)?;
        self.step(|tx| hand_over_pieces(tx, job, stat.request, &answer.response.bytes))?;
        let observed_at = stat.observed_at;
        let payloads = [
            RequestPayload::Measurement {
                measurements: answer.measurements,
            },
            RequestPayload::Received {
                digest: answer.response.digest(),
            },
            RequestPayload::Availability {
                status_code: stat.observation.status_code,
                finish_reason: stat.observation.finish_reason,
                raw_finish_reason: stat.observation.raw_finish_reason,
                failure: stat.observation.failure,
            },
        ];
        for ((offset, payload), id) in [0u32, 1, 2].into_iter().zip(payloads).zip(ids) {
            let Some(seq) = first_answer_seq.checked_add(offset) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            self.insert_development_request_event_with_id(
                &identity,
                job,
                stat.request,
                id,
                seq,
                observed_at,
                payload,
            )?;
        }
        let (usage_ids, next_checkpoint) = self.write_development_usages(
            &identity,
            RequestRef {
                job,
                request: stat.request,
            },
            prepared_usages,
            checkpoint,
            evidence,
        )?;
        Ok((
            MetricsId::new(ids[0]),
            AvailabilityStatId::new(ids[2]),
            usage_ids,
            next_checkpoint,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_development_request_event_with_id(
        &mut self,
        identity: &crate::db::redb::reads::jobs::UsageIdentity,
        job: JobId,
        request: RequestNumber,
        id: u128,
        seq: u32,
        observed_at: Timestamp,
        payload: RequestPayload,
    ) -> Result<(), BatchError> {
        let event = RequestEventRecord {
            id: crate::db::entities::RequestEventId::new(id),
            job_id: job,
            request,
            seq,
            observed_at,
            recorded_at: self.now,
            payload,
        };
        let timeline = indexes::request_timeline_key(Some(identity), &event);
        let model = indexes::usage_model_key(Some(identity), &event);
        self.step(|tx| {
            let mut events = tx.open_table(REQUEST_EVENTS)?;
            if events
                .insert(
                    (job.get(), request.get(), seq),
                    encode_request_event(&event).as_slice(),
                )?
                .is_some()
            {
                return Err(StorageError::corrupted(
                    "request evidence would overwrite history",
                ));
            }
            if let Some(kind) = observations::observation_kind(&events, &event)? {
                tx.open_table(JOB_OBSERVATIONS)?
                    .insert((job.get(), kind, id), (request.get(), seq))?;
            }
            if let Some(key) = timeline {
                tx.open_table(REQUEST_TIMELINE)?.insert(key, ())?;
            }
            if let Some(key) = model {
                indexes::record_usage_model(tx, key, job)?;
            }
            Ok(())
        })?;
        self.record_request_reference(job, request, seq)
    }

    pub(super) fn record_request_reference(
        &mut self,
        job: JobId,
        request: RequestNumber,
        seq: u32,
    ) -> Result<(), BatchError> {
        if self.pool_allocation(job)?.is_none() {
            return self.corrupted("request evidence has no Job allocation".into());
        }
        self.record_execution_event(job, ExecutionEventKind::RequestRecorded { request, seq })?;
        Ok(())
    }

    pub(in crate::db) fn record_development_partial_usage(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        request: RequestNumber,
        usages: Vec<crate::db::entities::Metrics>,
        checkpoint: crate::db::handoff::DevelopmentUsageCheckpoint,
    ) -> Result<
        (
            Vec<MetricsId>,
            crate::db::handoff::DevelopmentUsageCheckpoint,
        ),
        BatchError,
    > {
        self.require_development_write_for(proof, None)?;
        let job = proof.token().job;
        if self.development_usage_prefix(job, request)? != Some(checkpoint.last_sequence()) {
            return self.reject(Violation::InvalidValue(
                "usage",
                "request missing or response evidence already recorded",
            ));
        }
        let identity = self.development_usage_identity(job)?;
        let mut evidence = checkpoint.evidence();
        let mut prepared = Vec::with_capacity(usages.len());
        let Some(mut seq) = checkpoint.last_sequence().checked_add(1) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        for metrics in usages {
            if metrics.job != job || metrics.request != request {
                return self.reject(Violation::InvalidValue(
                    "usage",
                    "wrong Job or request identity",
                ));
            }
            let Some(token) = metrics.token else {
                return self.reject(Violation::InvalidValue(
                    "usage",
                    "Token annotation required",
                ));
            };
            let usage = self.prepare_development_usage(metrics, token, &mut evidence)?;
            let Some(token_seq) = seq.checked_add(1) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            prepared.push((seq, token_seq, usage));
            let Some(next) = token_seq.checked_add(1) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            seq = next;
        }
        let new = evidence.sample();
        self.step(|_| indexes::validate_native_sample(new.as_ref()))?;
        self.write_development_usages(
            &identity,
            RequestRef { job, request },
            prepared,
            checkpoint,
            evidence,
        )
    }

    fn prepare_development_usage(
        &mut self,
        metrics: Metrics,
        token: TokenAnnotation,
        evidence: &mut TokenEvidence,
    ) -> Result<PreparedUsage, BatchError> {
        let measurement_id = self.next_id()?;
        let token_id = self.next_id()?;
        let record = MetricsRecord {
            id: MetricsId::new(measurement_id),
            job: metrics.job,
            request: metrics.request,
            target: None,
            decision_target: None,
            observed_at: metrics.observed_at,
            measurements: metrics.measurements,
            token: Some(token),
            recorded_at: self.now,
        };
        let Some(observation) = crate::db::entities::TokenObservation::from_record(&record) else {
            return self.reject(Violation::InvalidValue(
                "usage",
                "Token annotation required",
            ));
        };
        if evidence.add(observation).is_err() {
            return self.reject(Violation::InvalidValue(
                "usage",
                "inconsistent Token evidence",
            ));
        }
        Ok(PreparedUsage {
            measurement_id,
            token_id,
            observed_at: record.observed_at,
            measurements: record.measurements,
            token,
        })
    }

    fn write_development_usages(
        &mut self,
        identity: &reads::jobs::UsageIdentity,
        request: RequestRef,
        prepared: Vec<(u32, u32, PreparedUsage)>,
        checkpoint: DevelopmentUsageCheckpoint,
        evidence: TokenEvidence,
    ) -> Result<(Vec<MetricsId>, DevelopmentUsageCheckpoint), BatchError> {
        let mut usage_ids = Vec::with_capacity(prepared.len());
        let mut last_token_seq = None;
        for (measurement_seq, token_seq, usage) in prepared {
            self.insert_development_request_event_with_id(
                identity,
                request.job,
                request.request,
                usage.measurement_id,
                measurement_seq,
                usage.observed_at,
                RequestPayload::Measurement {
                    measurements: usage.measurements,
                },
            )?;
            self.insert_development_request_event_with_id(
                identity,
                request.job,
                request.request,
                usage.token_id,
                token_seq,
                usage.observed_at,
                RequestPayload::Token {
                    eligibility: usage.token.eligibility,
                    finality: usage.token.finality,
                },
            )?;
            usage_ids.push(MetricsId::new(usage.measurement_id));
            last_token_seq = Some(token_seq);
        }
        let next_checkpoint = match last_token_seq {
            Some(token_seq) => {
                self.replace_development_usage_total(
                    request.job,
                    checkpoint.evidence().sample(),
                    evidence.sample(),
                )?;
                DevelopmentUsageCheckpoint::advanced(token_seq, evidence)
            }
            None => checkpoint,
        };
        Ok((usage_ids, next_checkpoint))
    }

    fn development_usage_prefix(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<Option<u32>, BatchError> {
        self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            let mut last = 0u32;
            for entry in events
                .range((job.get(), request.get(), 0)..=(job.get(), request.get(), u32::MAX))?
            {
                let (key, bytes) = entry?;
                let seq = key.value().2;
                if seq != last.saturating_add(1) {
                    return Ok(None);
                }
                let expected = match decode_request_event(job, request, seq, bytes.value())?.payload
                {
                    RequestPayload::Sent { .. } => seq == 1,
                    RequestPayload::Measurement { .. } => seq.is_multiple_of(2),
                    RequestPayload::Token { .. } => seq > 1 && !seq.is_multiple_of(2),
                    RequestPayload::Received { .. } | RequestPayload::Availability { .. } => false,
                };
                if !expected {
                    return Ok(None);
                }
                last = seq;
            }
            Ok((last > 0 && !last.is_multiple_of(2)).then_some(last))
        })
    }

    fn development_request_slot_is_next(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            if events
                .range((job.get(), request.get(), 0)..=(job.get(), u32::MAX, u32::MAX))?
                .next()
                .transpose()?
                .is_some()
            {
                return Ok(false);
            }
            if request == RequestNumber::FIRST {
                return Ok(true);
            }
            let Some(previous) = request.get().checked_sub(1).and_then(RequestNumber::new) else {
                return Ok(false);
            };
            let (mut sent, mut answered) = (false, false);
            for entry in events
                .range((job.get(), previous.get(), 0)..=(job.get(), previous.get(), u32::MAX))?
            {
                let (key, bytes) = entry?;
                let (_, _, seq) = key.value();
                match decode_request_event(job, previous, seq, bytes.value())?.payload {
                    RequestPayload::Sent { .. } => sent = true,
                    RequestPayload::Availability { .. } => answered = true,
                    RequestPayload::Measurement { .. }
                    | RequestPayload::Received { .. }
                    | RequestPayload::Token { .. } => {}
                }
            }
            Ok(sent && answered)
        })
    }

    fn record_development_provider_failure(
        &mut self,
        job: JobId,
        stat: crate::db::entities::AvailabilityStat,
    ) -> Result<(), BatchError> {
        if stat.job != job {
            return self.reject(Violation::InvalidValue(
                "request",
                "wrong Job or request identity",
            ));
        }
        let observation = stat.observation;
        if observation.status_code.is_none() || observation.failure.is_none() {
            return self.reject(Violation::InvalidValue(
                "failure",
                "a received Provider failure needs its status and failure facts",
            ));
        }
        let Some(prefix) = self.development_usage_prefix(job, stat.request)? else {
            return self.reject(Violation::InvalidValue(
                "failure",
                "request missing or response evidence already recorded",
            ));
        };
        let Some(seq) = prefix.checked_add(1) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        let identity = self.development_usage_identity(job)?;
        let id = self.next_id()?;
        self.insert_development_request_event_with_id(
            &identity,
            job,
            stat.request,
            id,
            seq,
            stat.observed_at,
            RequestPayload::Availability {
                status_code: observation.status_code,
                finish_reason: observation.finish_reason,
                raw_finish_reason: observation.raw_finish_reason,
                failure: observation.failure,
            },
        )
    }

    fn replace_development_usage_total(
        &mut self,
        job: JobId,
        old: Option<crate::db::entities::TokenRequestSample>,
        new: Option<crate::db::entities::TokenRequestSample>,
    ) -> Result<(), BatchError> {
        if old == new {
            return Ok(());
        }
        let Some(row) = self.mutable_job(job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job)));
        };
        let identity = self.development_usage_identity(job)?;
        let parent = self.require_mutable_conversation(row.parent_id)?;
        let agent = parent.agent.map_or(0, ConfigId::get);
        let provider = identity.provider;
        let hash = *ContentDigest::of(identity.model.as_bytes()).as_bytes();
        let key = |sample: &crate::db::entities::TokenRequestSample| {
            (
                sample
                    .observed_at
                    .as_unix_micros()
                    .div_euclid(crate::db::entities::USAGE_SUMMARY_HOUR_MICROS),
                agent,
                provider,
                hash,
            )
        };
        let numbers = |sample: &crate::db::entities::TokenRequestSample| {
            [
                1u64,
                u64::from(sample.usage == crate::db::entities::TokenUsageState::Complete),
                u64::from(sample.input.value().unwrap_or(0)),
                u64::from(sample.output.value().unwrap_or(0)),
                u64::from(sample.cache_read.value().unwrap_or(0)),
                u64::from(sample.cache_write.value().unwrap_or(0)),
                u64::from(sample.reasoning.value().unwrap_or(0)),
            ]
        };
        let ok = self.step(|tx| {
            let mut totals = tx.open_table(USAGE_TOTALS)?;
            for (remove, sample) in [(true, old.as_ref()), (false, new.as_ref())] {
                let Some(sample) = sample else {
                    continue;
                };
                let k = key(sample);
                let mut values = match totals.get(k)?.map(|v| v.value()) {
                    Some(v) => [v.0, v.1, v.2, v.3, v.4, v.5, v.6],
                    None if remove => {
                        return Err(StorageError::corrupted("native usage aggregate missing"));
                    }
                    None => [0; 7],
                };
                for (value, delta) in values.iter_mut().zip(numbers(sample)) {
                    let Some(next) = (if remove {
                        value.checked_sub(delta)
                    } else {
                        value.checked_add(delta)
                    }) else {
                        return Ok(false);
                    };
                    *value = next;
                }
                if values[0] == 0 {
                    if values.iter().any(|v| *v != 0) {
                        return Ok(false);
                    }
                    totals.remove(k)?;
                } else {
                    totals.insert(
                        k,
                        (
                            values[0], values[1], values[2], values[3], values[4], values[5],
                            values[6],
                        ),
                    )?;
                }
            }
            Ok(true)
        })?;
        if !ok {
            return self.reject(Violation::ValueOutOfRange("usage aggregate"));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_output(
        &mut self,
        job: JobId,
        kind: OutputKind,
        part: Option<(u32, u32, u32)>,
        summary: Option<String>,
        code: Option<String>,
        goal_reached: Option<bool>,
        value: Option<serde_json::Value>,
    ) -> Result<(u32, JobOutputId), BatchError> {
        let summary = if part.is_some() {
            summary
        } else {
            summary.map(|text| crate::db::entities::output_filter::text(&text).into_owned())
        };
        let code = code.map(
            |text| match crate::db::entities::output_filter::text(&text) {
                std::borrow::Cow::Borrowed(_) => text,
                std::borrow::Cow::Owned(filtered) => filtered,
            },
        );
        let value = value.map(
            |value| match crate::db::entities::output_filter::value(&value) {
                std::borrow::Cow::Borrowed(_) => value,
                std::borrow::Cow::Owned(filtered) => filtered,
            },
        );
        let seq = self.step(|tx| {
            let outputs = tx.open_table(JOB_OUTPUTS)?;
            let last = outputs
                .range((job.get(), 0)..=(job.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(0, |(key, _)| key.value().1);
            Ok(last)
        })?;
        let Some(seq) = seq.checked_add(1) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        let id = JobOutputId::new(self.next_id()?);
        let mut record = JobOutputRecord {
            part,
            span: None,
            answer_request: None,
            id,
            job,
            seq: i64::from(seq),
            kind,
            summary,
            code,
            goal_reached,
            value,
            created_at: self.now,
        };
        self.step(|tx| {
            if kind == OutputKind::Result && record.model_text().is_some() && record.code.is_none()
            {
                record.answer_request =
                    super::observations::latest_request(&tx.open_table(REQUEST_EVENTS)?, job)?;
                let answer = super::observations::answer_text(tx, job, &record)?;
                if answer.as_deref() == record.model_text()
                    && record.summary.as_deref().is_none_or(|summary| Some(summary) == record.model_text())
                {
                    if let Some(object) = record
                        .value
                        .as_mut()
                        .and_then(serde_json::Value::as_object_mut)
                    {
                        object.remove("text");
                    }
                    record.summary = None;
                } else {
                    record.answer_request = None;
                    log::warn!("Job {job} recorded answer or summary does not match Result text; retaining Result text");
                }
            }
            tx.open_table(JOB_OUTPUTS)?
                .insert((job.get(), seq), encode_output(&record).as_slice())?;
            if record.kind == OutputKind::Result {
                let conversation = tx
                    .open_table(JOBS)?
                    .get(job.get())?
                    .map(|bytes| {
                        jobs::decode_mutable_job(job.get(), bytes.value()).map(|job| job.parent_id)
                    })
                    .transpose()?;
                if let Some(conversation) = conversation {
                    let completed = tx
                        .open_table(JOB_RUNTIME)?
                        .get(job.get())?
                        .map(|bytes| state::decode_state(job.get(), bytes.value()))
                        .transpose()?
                        .and_then(|state| state.outcome)
                        .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
                    indexes::append_output_search_index(tx, conversation, job, &record, completed)?;
                }
            }
            Ok(())
        })?;
        self.record_execution_event(job, ExecutionEventKind::OutputRecorded { seq })?;
        Ok((seq, id))
    }
}

impl Batch {
    pub(super) fn normalize_summary_output(
        &mut self,
        job: &crate::db::entities::MutableJob,
        output: &mut crate::db::entities::FinalResult,
    ) -> Result<(), BatchError> {
        let Some(source) = job.summary_source else {
            return Ok(());
        };
        let request = self.step(|tx| {
            let Some(request) =
                super::observations::latest_request(&tx.open_table(REQUEST_EVENTS)?, job.id)?
                    .and_then(RequestNumber::new)
            else {
                return Ok(None);
            };
            Ok(
                super::observations::received_summary(tx, job.id, request, source)?
                    .map(|_| request),
            )
        })?;
        let Some(answer_request) = request else {
            return self.reject(Violation::InvalidValue(
                "summary",
                "recorded summary answer is missing or invalid",
            ));
        };
        let link = crate::db::entities::NativeSummaryResultLink::Received {
            source,
            producer_job: ContentDigest::of(&encode_mutable_job(job)),
            answer_request,
        };
        output.value = Some(serde_json::to_value(link).map_err(|_| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "summary link serialization failed",
            )))
        })?);
        output.summary = None;
        Ok(())
    }

    pub(crate) fn record_development_uncertain(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        cause: crate::db::entities::UncertaintyReason,
    ) -> Result<(), BatchError> {
        self.require_development_write_for(proof, None)?;
        let token = proof.token();
        self.transition_pool_allocation(
            token,
            crate::db::entities::AllocationPhase::Uncertain,
            Some(cause),
        )?;
        if self
            .step(|tx| Ok(super::recovery::held::unfinished_commands(tx, token.job)?.is_empty()))?
        {
            self.release_uncertain(
                token,
                proof.revision(),
                crate::db::entities::StopProof::Proof,
                None,
            )?;
        }
        self.probe = Some(Probe::Outcome(token.job));
        Ok(())
    }

    pub(crate) fn finalize_development_failure(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        expected_seq: u32,
        finalization: crate::db::entities::JobFinalization,
        rejection: Option<crate::db::entities::AvailabilityStat>,
    ) -> Result<AppendedOutput, BatchError> {
        use crate::db::entities::JobFinalization;
        let job = self.require_development_write_for(proof, None)?;
        match (&finalization, rejection) {
            (JobFinalization::Failed(_), Some(stat)) => {
                self.record_development_provider_failure(job.id, stat)?;
            }
            (JobFinalization::Failed(_) | JobFinalization::Cancelled(_), None) => {}
            (JobFinalization::Completed(_), None)
            | (JobFinalization::Completed(_) | JobFinalization::Cancelled(_), Some(_)) => {
                return self.reject(Violation::InvalidValue(
                    "outcome",
                    "only an unanswered failure can be settled here",
                ));
            }
        }
        self.finish_development_job(proof.token(), &job, expected_seq, finalization)
    }

    pub(crate) fn finalize_development_decision(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        expected_seq: u32,
        result: crate::db::entities::DecisionResult,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        use crate::db::entities::{
            FrozenDecisionConfig, MutableFrozenConfiguration, MutableJobContent, RequestRef,
        };
        let job_id = proof.token().job;
        let Some(job) = self.mutable_job(job_id)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job_id)));
        };
        let (operation, decision_frozen, request) = match (job.frozen, job.input.content) {
            (
                Some(MutableFrozenConfiguration::Decision(frozen)),
                MutableJobContent::Decision { request, .. },
            ) => (
                ExecutionOperation::Decision,
                FrozenDecisionConfig::Native(frozen),
                request,
            ),
            (
                Some(MutableFrozenConfiguration::Generative(frozen)),
                MutableJobContent::Decision { request, .. },
            ) => (
                ExecutionOperation::LlmRequest,
                FrozenDecisionConfig::Generative(frozen, None),
                request,
            ),
            _ => {
                return self.reject(Violation::InvalidValue(
                    "decision",
                    "frozen Decision request required",
                ));
            }
        };
        let job = self.require_development_write_for(proof, Some(operation))?;
        let first_request = RequestNumber::FIRST;
        if result
            .validate(
                &request,
                &decision_frozen,
                RequestRef {
                    job: job_id,
                    request: first_request,
                },
            )
            .is_err()
        {
            return self.reject(Violation::InvalidValue(
                "decision",
                "result does not match the frozen Decision request",
            ));
        }
        let predictive_accept = if self.step(|tx| jobs::is_predictive_helper(tx, &job))? {
            match &result.outcome {
                crate::db::entities::DecisionOutcome::Answer {
                    answer:
                        crate::db::entities::DecisionAnswer::BinaryProbability { true_probability },
                } => Some(*true_probability),
                _ => None,
            }
        } else {
            None
        };
        let value = serde_json::to_value(result).map_err(|_| {
            BatchError(Failure::Rejected(Violation::InvalidValue(
                "decision",
                "result cannot be serialized",
            )))
        })?;
        let value = serde_json::json!({"decision": value});
        if crate::db::entities::output_filter::value(&value).as_ref() != &value {
            return self.reject(Violation::InvalidValue(
                "decision",
                "result cannot be preserved by the output filter",
            ));
        }
        let outcome = self.finish_development_job(
            proof.token(),
            &job,
            expected_seq,
            crate::db::entities::JobFinalization::Completed(crate::db::entities::FinalResult {
                summary: None,
                code: None,
                goal_reached: None,
                value: Some(value),
            }),
        )?;
        if let Some(true_probability) = predictive_accept {
            self.automatic_compaction(|batch| {
                let (policy, _) = batch
                    .step(|tx| reads::catalog::effective_context_policy(&tx.open_table(META)?))?;
                let Some(pred) = &policy.predictive else {
                    return Ok(());
                };
                if pred.mode != crate::db::entities::PredictionMode::Prepare {
                    return Ok(());
                }
                let min_prob = pred
                    .acceptance
                    .as_ref()
                    .map_or(500_000, |a| a.minimum_probability_millionths);
                if true_probability * 1_000_000.0 >= f64::from(min_prob) {
                    let summarizer = policy.summarization_model.or(match &decision_frozen {
                        FrozenDecisionConfig::Generative(g, _) => Some(g.llm.model.id),
                        FrozenDecisionConfig::Native(_) => None,
                    });
                    if let Some(summarizer) = summarizer {
                        let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
                        ctx.update(b"native-predictive-compaction");
                        ctx.update(&job.parent_id.get().to_le_bytes());
                        ctx.update(&job_id.get().to_le_bytes());
                        let digest: [u8; 32] = ctx.finish().as_ref().try_into().unwrap_or([0; 32]);
                        let submission = crate::db::entities::Submission {
                            key: crate::db::entities::SubmissionKey::from_digest(digest),
                            fingerprint: crate::db::entities::RequestFingerprint::from_digest(
                                digest,
                            ),
                        };
                        batch.request_compaction_for(
                            job.parent_id,
                            summarizer,
                            submission,
                            None,
                        )?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(outcome)
    }

    pub(crate) fn finalize_development_job(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        expected_seq: u32,
        mut finalization: crate::db::entities::JobFinalization,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        let job =
            self.require_development_write_for(proof, Some(ExecutionOperation::LlmRequest))?;
        if let crate::db::entities::JobFinalization::Completed(ref mut output) = finalization {
            self.normalize_summary_output(&job, output)?;
        }
        self.finish_development_job(proof.token(), &job, expected_seq, finalization)
    }

    fn finish_development_job(
        &mut self,
        token: crate::db::entities::AllocationToken,
        job: &MutableJob,
        expected_seq: u32,
        finalization: crate::db::entities::JobFinalization,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        let (kind, result) = match finalization {
            crate::db::entities::JobFinalization::Completed(result) => {
                (OutcomeKind::Completed, result)
            }
            crate::db::entities::JobFinalization::Failed(result) => (OutcomeKind::Failed, result),
            crate::db::entities::JobFinalization::Cancelled(result) => {
                (OutcomeKind::Cancelled, result)
            }
        };
        let valid = self.step(|tx| {
            if state::pending(tx, token.job)? != Some((0, 0)) {
                return Ok(false);
            }
            let mut requests = std::collections::BTreeMap::<u32, (bool, bool)>::new();
            for entry in tx.open_table(REQUEST_EVENTS)?.range(
                (token.job.get(), RequestNumber::FIRST.get(), 0)
                    ..=(token.job.get(), u32::MAX, u32::MAX),
            )? {
                let (key, row) = entry?;
                let (_, request, seq) = key.value();
                let number = RequestNumber::new(request)
                    .ok_or_else(|| StorageError::corrupted("zero request identity"))?;
                let state = requests.entry(request).or_default();
                match rows::decode_request_event(token.job, number, seq, row.value())?.payload {
                    crate::db::entities::RequestPayload::Sent { .. } => state.0 = true,
                    crate::db::entities::RequestPayload::Received { .. } => state.1 = true,
                    crate::db::entities::RequestPayload::Availability {
                        status_code,
                        finish_reason,
                        raw_finish_reason,
                        failure,
                    } => {
                        let observation = crate::db::entities::AvailabilityObservation {
                            status_code,
                            finish_reason,
                            raw_finish_reason,
                            failure,
                        };
                        state.1 |= matches!(
                            observation.classification(),
                            crate::db::entities::AvailabilityClassification::Rejected(_)
                        );
                    }
                    crate::db::entities::RequestPayload::Measurement { .. }
                    | crate::db::entities::RequestPayload::Token { .. } => {}
                }
            }
            if requests.values().any(|&(sent, settled)| !sent || !settled) {
                return Ok(false);
            }
            for entry in tx.open_table(TOOL_CALLS)?.range(
                (token.job.get(), RequestNumber::FIRST.get(), 0)
                    ..=(token.job.get(), u32::MAX, u32::MAX),
            )? {
                let (key, row) = entry?;
                let call =
                    rows::decode_tool_evidence(rows::tool_call_ref(key.value())?, row.value())?;
                if call.admission().is_some()
                    && call.reconciliation.is_none()
                    && call
                        .outcome
                        .as_ref()
                        .is_none_or(|outcome| !outcome.fact.is_definite())
                {
                    return Ok(false);
                }
            }
            let mut last = 0u32;
            for entry in tx
                .open_table(JOB_OUTPUTS)?
                .range((token.job.get(), 0)..=(token.job.get(), u32::MAX))?
            {
                let (key, value) = entry?;
                if rows::output_kind(value.value())? == OutputKind::Result {
                    return Ok(false);
                }
                last = key.value().1;
            }
            Ok(last.checked_add(1) == Some(expected_seq))
        })?;
        if !valid {
            return self.reject(Violation::InvalidValue(
                "outcome",
                "required evidence or output sequence is invalid",
            ));
        }
        let (seq, id) = self.settle_mutable_outcome(token, job, kind, result)?;
        self.transition_pool_allocation(
            token,
            crate::db::entities::AllocationPhase::Released,
            None,
        )?;
        self.settle_summary_membership(job)?;
        self.settle_standalone_decision(job)?;
        self.freed_capacity = true;
        self.probe = Some(Probe::Outcome(token.job));
        if kind == OutcomeKind::Completed
            && job.summary_source.is_none()
            && matches!(
                job.input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            )
            && let Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) =
                &job.frozen
        {
            self.automatic_compaction(|batch| {
                batch.maybe_trigger_native_compaction(job.parent_id, frozen)
            })?;
        }
        Ok(crate::db::entities::AppendedOutput {
            output: id,
            seq: i64::from(seq),
        })
    }

    pub(super) fn latest_verified_summary(
        &mut self,
        conversation: JobId,
        consumer: JobId,
    ) -> Result<Option<(JobId, ContentDigest)>, BatchError> {
        let candidates = self.read_summary_source(|tx| {
            let children = tx.open_table(CHILDREN)?;
            let jobs = tx.open_table(JOBS)?;
            let groups = tx.open_table(CONVERSATIONS)?;
            let mut found = Vec::new();
            for entry in children
                .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                .rev()
            {
                let child_id = JobId::new(entry?.0.value().1);
                let Some(child) = summary_child_job(&jobs, &groups, conversation, child_id)? else {
                    continue;
                };
                if let Some(source) = child.summary_source {
                    found.push((child_id, source));
                    if found.len() >= 8 {
                        break;
                    }
                }
            }
            Ok(found)
        })?;
        for (producer, source) in candidates {
            let verified = self.verify_summary_source(|tx| {
                reads::execution_context::verify_native_summary_in(
                    tx,
                    conversation,
                    consumer,
                    producer,
                    source,
                )
            })?;
            if verified.is_some() {
                return Ok(Some((producer, source)));
            }
        }
        Ok(None)
    }
}

impl Batch {
    pub(crate) fn require_development_call(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        call: ToolCallRef,
    ) -> Result<(), BatchError> {
        if proof.token().job != call.job {
            return self.reject(Violation::InvalidValue(
                "tool_call",
                "the call belongs to another Job",
            ));
        }
        self.require_development_write_for(proof, Some(ExecutionOperation::LlmRequest))?;
        Ok(())
    }
}
