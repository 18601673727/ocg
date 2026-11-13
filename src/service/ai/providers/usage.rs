use super::*;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReportedUsage {
    pub(crate) usage: NormalizedUsage,
    pub(crate) finality: UsageFinality,
}

fn counter(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

pub(crate) fn reported_usage(event: &Value) -> Option<ReportedUsage> {
    let value = event.get("usage").filter(|value| value.is_object())?;
    let usage_only = event
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty);
    Some(ReportedUsage {
        usage: NormalizedUsage {
            input_tokens: counter(value.get("prompt_tokens")),
            output_tokens: counter(value.get("completion_tokens")),
            cache_read_tokens: counter(
                value
                    .get("prompt_tokens_details")
                    .and_then(|details| details.get("cached_tokens")),
            ),
            cache_write_tokens: None,
            reasoning_tokens: counter(
                value
                    .get("completion_tokens_details")
                    .and_then(|details| details.get("reasoning_tokens")),
            ),
            raw: Some(value.clone()),
        },
        finality: if usage_only {
            UsageFinality::Final
        } else {
            UsageFinality::Cumulative
        },
    })
}

pub(crate) fn reported_responses_usage(event: &Value) -> Option<ReportedUsage> {
    let value = event
        .get("response")?
        .get("usage")
        .filter(|value| value.is_object())?;
    Some(ReportedUsage {
        usage: NormalizedUsage {
            input_tokens: counter(value.get("input_tokens")),
            output_tokens: counter(value.get("output_tokens")),
            cache_read_tokens: counter(
                value
                    .get("input_tokens_details")
                    .and_then(|v| v.get("cached_tokens")),
            ),
            cache_write_tokens: None,
            reasoning_tokens: counter(
                value
                    .get("output_tokens_details")
                    .and_then(|v| v.get("reasoning_tokens")),
            ),
            raw: Some(value.clone()),
        },
        finality: if matches!(
            event["type"].as_str(),
            Some("response.completed" | "response.failed" | "response.incomplete")
        ) {
            UsageFinality::Final
        } else {
            UsageFinality::Cumulative
        },
    })
}

pub(crate) struct UsageCapture {
    last_cumulative: Option<NormalizedUsage>,
    clock: fn() -> Timestamp,
}

impl Default for UsageCapture {
    fn default() -> Self {
        Self::with_clock(Timestamp::now)
    }
}

impl UsageCapture {
    pub(crate) fn with_clock(clock: fn() -> Timestamp) -> Self {
        Self {
            last_cumulative: None,
            clock,
        }
    }

    pub(crate) async fn record(
        &mut self,
        sink: Option<&mut (dyn ProviderOutputSink + '_)>,
        reported: ReportedUsage,
    ) {
        let Some(sink) = sink else { return };
        let observed_at = (self.clock)();
        if reported.finality == UsageFinality::Cumulative {
            if self.last_cumulative.as_ref() == Some(&reported.usage) {
                return;
            }
            self.last_cumulative = Some(reported.usage.clone());
        }
        announce_at(
            Some(sink),
            observed_at,
            ProviderFact::Usage {
                usage: reported.usage,
                finality: reported.finality,
            },
        )
        .await;
    }
}
