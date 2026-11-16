use super::rows::{decode_request_event, encode_request_event};
use super::{StorageError, jobs, observations};
use crate::db::entities::{
    AppendedOutput, AvailabilityStatId, ConfigId, ContentDigest, ExecutionEventKind,
    ExecutionOperation, JobId, JobOutputId, JobOutputRecord, MetricsId, MutableJob, OutcomeKind,
    OutputKind, RecordRef, RequestEventRecord, RequestNumber, RequestPayload, Timestamp,
    ToolCallRef, Violation,
};
use crate::db::redb::batch::{Batch, BatchError, Failure, Probe};
use crate::db::redb::jobs::{decode_mutable_job, encode_mutable_job};
use crate::db::redb::rows::encode_output;
use crate::db::redb::schema::{
    CHILDREN, JOB_OBSERVATIONS, JOB_OUTPUTS, JOB_RUNTIME, JOBS, MESSAGES, META, REQUEST_EVENTS,
    REQUEST_TIMELINE, TOOL_CALLS, USAGE_TOTALS,
};
use crate::db::redb::{indexes, reads, rows, state};
use ::redb::ReadableTable;

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
        let old = evidence.sample();
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
            let first_id = self.next_id()?;
            let second_id = self.next_id()?;
            let measurement = crate::db::entities::MetricsRecord {
                id: MetricsId::new(first_id),
                job,
                request: stat.request,
                target: None,
                decision_target: None,
                observed_at: metrics.observed_at,
                measurements: metrics.measurements.clone(),
                token: Some(token),
                recorded_at: self.now,
            };
            let Some(observation) =
                crate::db::entities::TokenObservation::from_record(&measurement)
            else {
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
            prepared_usages.push((
                first_id,
                second_id,
                expected_seq,
                token_seq,
                metrics.observed_at,
                metrics.measurements,
                token,
            ));
        }
        let new = evidence.sample();
        self.step(|_| indexes::validate_native_sample(new.as_ref()))?;
        let identity = self.development_usage_identity(job)?;
        self.put_message(&answer.response)?;
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
        let mut usage_ids = Vec::with_capacity(prepared_usages.len());
        let mut last_token_seq = None;
        for (first_id, second_id, expected_seq, token_seq, observed_at, measurements, token) in
            prepared_usages
        {
            self.insert_development_request_event_with_id(
                &identity,
                job,
                stat.request,
                first_id,
                expected_seq,
                observed_at,
                RequestPayload::Measurement { measurements },
            )?;
            self.insert_development_request_event_with_id(
                &identity,
                job,
                stat.request,
                second_id,
                token_seq,
                observed_at,
                RequestPayload::Token {
                    eligibility: token.eligibility,
                    finality: token.finality,
                },
            )?;
            usage_ids.push(MetricsId::new(first_id));
            last_token_seq = Some(token_seq);
        }
        let next_checkpoint = match last_token_seq {
            Some(token_seq) => {
                self.replace_development_usage_total(job, old, new)?;
                crate::db::handoff::DevelopmentUsageCheckpoint::advanced(token_seq, evidence)
            }
            None => checkpoint,
        };
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
            if tx
                .open_table(REQUEST_EVENTS)?
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
            if let Some(kind) =
                observations::observation_kind(&tx.open_table(REQUEST_EVENTS)?, &event)?
            {
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
        let old = evidence.sample();
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
            let first_id = self.next_id()?;
            let second_id = self.next_id()?;
            let record = crate::db::entities::MetricsRecord {
                id: MetricsId::new(first_id),
                job,
                request,
                target: None,
                decision_target: None,
                observed_at: metrics.observed_at,
                measurements: metrics.measurements.clone(),
                token: Some(token),
                recorded_at: self.now,
            };
            let Some(observation) = crate::db::entities::TokenObservation::from_record(&record)
            else {
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
            let Some(token_seq) = seq.checked_add(1) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            prepared.push((
                first_id,
                second_id,
                seq,
                token_seq,
                metrics.observed_at,
                metrics.measurements,
                token,
            ));
            let Some(next) = token_seq.checked_add(1) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            seq = next;
        }
        let new = evidence.sample();
        self.step(|_| indexes::validate_native_sample(new.as_ref()))?;
        let mut usage_ids = Vec::with_capacity(prepared.len());
        let mut last_token_seq = None;
        for (first_id, second_id, measurement_seq, token_seq, observed_at, measurements, token) in
            prepared
        {
            self.insert_development_request_event_with_id(
                &identity,
                job,
                request,
                first_id,
                measurement_seq,
                observed_at,
                RequestPayload::Measurement { measurements },
            )?;
            self.insert_development_request_event_with_id(
                &identity,
                job,
                request,
                second_id,
                token_seq,
                observed_at,
                RequestPayload::Token {
                    eligibility: token.eligibility,
                    finality: token.finality,
                },
            )?;
            usage_ids.push(MetricsId::new(first_id));
            last_token_seq = Some(token_seq);
        }
        let next_checkpoint = match last_token_seq {
            Some(token_seq) => {
                self.replace_development_usage_total(job, old, new)?;
                crate::db::handoff::DevelopmentUsageCheckpoint::advanced(token_seq, evidence)
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
                    RequestPayload::Received { .. }
                    | RequestPayload::Availability { .. }
                    | RequestPayload::ApprovedCalls => false,
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
                    RequestPayload::ApprovedCalls => {
                        sent = true;
                        answered = true;
                    }
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

    pub(super) fn write_output(
        &mut self,
        job: JobId,
        kind: OutputKind,
        summary: Option<String>,
        code: Option<String>,
        goal_reached: Option<bool>,
        value: Option<serde_json::Value>,
    ) -> Result<(u32, JobOutputId), BatchError> {
        let summary = summary.map(
            |text| match crate::db::entities::output_filter::text(&text) {
                std::borrow::Cow::Borrowed(_) => text,
                std::borrow::Cow::Owned(filtered) => filtered,
            },
        );
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
        let record = JobOutputRecord {
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
        let raw = output
            .summary
            .as_deref()
            .ok_or(BatchError(Failure::Rejected(Violation::InvalidValue(
                "summary",
                "summary result text is missing",
            ))))?;
        let summary: crate::db::entities::SummarizationResult =
            serde_json::from_str(raw).map_err(|_| {
                BatchError(Failure::Rejected(Violation::InvalidValue(
                    "summary",
                    "summary result must be valid JSON",
                )))
            })?;
        if summary.validate().is_err() || summary.source != source {
            return self.reject(Violation::InvalidValue(
                "summary",
                "summary result failed schema or source validation",
            ));
        }
        let summary_value = serde_json::to_value(&summary).map_err(|_| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "summary serialization failed",
            )))
        })?;
        let body = crate::db::entities::MessageBody::new(&summary_value).map_err(|_| {
            BatchError(Failure::Rejected(Violation::InvalidValue(
                "summary",
                "summary payload exceeds maximum message size",
            )))
        })?;
        self.put_message(&body)?;
        let producer_job = ContentDigest::of(&encode_mutable_job(job));
        let link = crate::db::entities::NativeSummaryResultLink {
            source,
            producer_job,
            payload: body.digest(),
        };
        output.value = Some(serde_json::to_value(link).map_err(|_| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "summary link serialization failed",
            )))
        })?);
        output.summary = Some(summary.summary);
        Ok(())
    }

    pub(crate) fn record_development_uncertain(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        reason: crate::db::entities::UncertaintyReason,
    ) -> Result<(), BatchError> {
        self.require_development_write_for(proof, None)?;
        self.transition_pool_allocation(
            proof.token(),
            crate::db::entities::AllocationPhase::Uncertain,
            Some(reason),
        )?;
        self.probe = Some(Probe::Outcome(proof.token().job));
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
        let is_predictive_compaction =
            request.state.get("kind").and_then(|v| v.as_str()) == Some("predictive_compaction");
        let predictive_accept = if is_predictive_compaction {
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
        );
        if outcome.is_ok()
            && let Some(true_probability) = predictive_accept
            && let Ok((policy, _)) =
                self.step(|tx| reads::catalog::effective_context_policy(&tx.open_table(META)?))
            && let Some(pred) = &policy.predictive
            && pred.mode == crate::db::entities::PredictionMode::Prepare
        {
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
                        fingerprint: crate::db::entities::RequestFingerprint::from_digest(digest),
                    };
                    let _ =
                        self.request_compaction_for(job.parent_id, summarizer, submission, None);
                }
            }
        }
        outcome
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
                    crate::db::entities::RequestPayload::ApprovedCalls => *state = (true, true),
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
            let _ = self.maybe_trigger_native_compaction(job.parent_id, frozen);
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
        let candidates = self.step(|tx| {
            let children = tx.open_table(CHILDREN)?;
            let jobs = tx.open_table(JOBS)?;
            let mut found = Vec::new();
            for entry in children
                .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                .rev()
            {
                let child_id = JobId::new(entry?.0.value().1);
                let Some(bytes) = jobs.get(child_id.get())? else {
                    continue;
                };
                let child = decode_mutable_job(child_id.get(), bytes.value())?;
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
            let ok = self.step(|tx| {
                Ok(reads::execution_context::verify_native_summary_tables(
                    &tx.open_table(JOBS)?,
                    &tx.open_table(JOB_RUNTIME)?,
                    &tx.open_table(JOB_OUTPUTS)?,
                    &tx.open_table(MESSAGES)?,
                    conversation,
                    consumer,
                    producer,
                    source,
                )
                .is_ok())
            })?;
            if ok {
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
