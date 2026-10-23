use super::*;

/// A valid, non-null usage object of one event, and what its position says
/// about its finality.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct ReportedUsage {
    pub(super) usage: NormalizedUsage,
    finality: UsageFinality,
}

fn counter(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

/// The usage object of an event. `usage: null` and any other non-object is
/// absence: it reports nothing and erases nothing.
///
/// Final is the documented `stream_options.include_usage` chunk: usage with
/// an explicit empty `choices` array carries the whole request's usage. Usage
/// beside choices (or with no `choices` at all) is a running total and stays
/// cumulative. A finish reason, `[DONE]`, the end of the stream or the Job's
/// completion never promotes anything to final.
pub(super) fn reported_usage(event: &Value) -> Option<ReportedUsage> {
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

pub(super) fn reported_responses_usage(event: &Value) -> Option<ReportedUsage> {
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

/// Hands each usage object a stream reports to the sink as it is parsed. It
/// keeps no history, no count and no merged total: the only state is the last
/// cumulative object, to skip an identical repeat, and the clock. Memory is
/// one bounded SSE object plus that one, whatever the stream reports, and each
/// object is awaited before the next is read, so a slow sink slows the read.
pub(crate) struct UsageCapture {
    last_cumulative: Option<NormalizedUsage>,
    /// When the producer observes an object. It is read when the object is
    /// parsed, before the sink is awaited.
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

    /// Hands one reported object over as it is, with its own normalization,
    /// its finality and the time it was parsed. Distinct objects are never
    /// merged or dropped, however many there are; per-field maxima belong to
    /// the reducer. Only an identical consecutive cumulative repeat, which
    /// carries nothing new, is skipped.
    pub(super) async fn record(
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
