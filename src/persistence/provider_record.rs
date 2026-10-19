use super::*;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageBody {
    pub(crate) digest: ContentDigest,
    pub(crate) bytes: Vec<u8>,
}

impl MessageBody {
    pub fn new(value: &Value) -> Result<Self, serde_json::Error> {
        let bytes = serde_json::to_vec(value)?;
        Ok(Self {
            digest: ContentDigest::of(&bytes),
            bytes,
        })
    }

    pub fn digest(&self) -> ContentDigest {
        self.digest
    }

    pub fn byte_length(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Clone, Debug)]
pub struct NewSentRequest {
    pub job: JobId,
    pub request: RequestNumber,
    pub observed_at: Timestamp,
    pub messages: Vec<ContentDigest>,
    pub tools: Option<ContentDigest>,
    pub parameters: ContentDigest,
    pub dropped_turns: Option<u32>,
    pub bodies: Vec<MessageBody>,
}

#[derive(Clone, Debug)]
pub struct NewProviderAnswer {
    pub stat: NewAvailabilityStat,
    pub measurements: Measurements,
    pub response: MessageBody,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ProviderRequestRecord {
    pub messages: Vec<Value>,
    pub tools: Option<Value>,
    pub parameters: Value,
    pub dropped_turns: u32,
    pub response: Option<Value>,
}

pub(crate) fn record_sent_request(
    batch: &mut Batch,
    sent: NewSentRequest,
) -> mutations::OpResult<()> {
    if batch.require_job(sent.job)?.execution.is_none() {
        return batch.reject(Violation::NotAnLlmExecution(sent.job));
    }
    for body in &sent.bodies {
        batch.put_message(body)?;
    }
    batch.require_messages(
        sent.messages
            .iter()
            .copied()
            .chain(sent.tools)
            .chain([sent.parameters]),
    )?;
    batch.insert_request_event(
        sent.job,
        sent.request,
        sent.observed_at,
        RequestPayload::Sent {
            messages: sent.messages,
            tools: sent.tools,
            parameters: sent.parameters,
            dropped_turns: sent.dropped_turns,
        },
    )?;
    Ok(())
}

pub(crate) fn record_provider_answer(
    batch: &mut Batch,
    answer: NewProviderAnswer,
) -> mutations::OpResult<()> {
    let stat = answer.stat;
    mutations::record_metrics(
        batch,
        NewMetrics {
            job: stat.job,
            request: stat.request,
            observed_at: stat.observed_at,
            measurements: answer.measurements,
            token: None,
        },
    )?;
    batch.put_message(&answer.response)?;
    batch.insert_request_event(
        stat.job,
        stat.request,
        stat.observed_at,
        RequestPayload::Received {
            digest: answer.response.digest,
        },
    )?;
    mutations::record_availability_stat(batch, stat)?;
    Ok(())
}
