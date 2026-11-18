use std::collections::{BTreeMap, BTreeSet};

use super::{
    NormalizedUsage, ProviderFact, ProviderOutputSink, Timestamp, UsageFinality, Value, announce_at,
};

fn counter(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn reported_usage(event: &Value) -> Option<NormalizedUsage> {
    let value = event.get("usage").filter(|value| value.is_object())?;
    Some(NormalizedUsage {
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
    })
}

#[derive(Clone)]
struct UsageReport {
    at: Timestamp,
    usage: NormalizedUsage,
    usage_only: bool,
}

#[derive(Default)]
pub(crate) struct UsageCapture {
    choices: BTreeMap<u64, bool>,
    open_choices: usize,
    ambiguous_choices: bool,
    last_cumulative: Option<NormalizedUsage>,
    pending_final: Option<UsageReport>,
}

impl UsageCapture {
    fn observe_choices(&mut self, choices: &[Value]) -> (bool, bool) {
        let mut saw_finish = false;
        let mut new_choice = false;
        let mut seen = BTreeSet::new();
        for choice in choices {
            let index = match choice.get("index") {
                Some(index) => index.as_u64(),
                None if choices.len() == 1 => Some(0),
                None => None,
            };
            let Some(index) = index.filter(|_| choice.is_object()) else {
                self.ambiguous_choices = true;
                continue;
            };
            if choices.len() > 1 && !seen.insert(index) {
                self.ambiguous_choices = true;
            }
            let finished = choice
                .get("finish_reason")
                .and_then(Value::as_str)
                .and_then(super::openai_compatible::stream::parse_finish_reason)
                .is_some();
            let before = self.choices.insert(index, finished);
            self.open_choices -= usize::from(before == Some(false));
            self.open_choices += usize::from(!finished);
            saw_finish |= finished;
            new_choice |= before.is_none();
        }
        (saw_finish, new_choice)
    }

    fn all_choices_closed(&self) -> bool {
        self.open_choices == 0 && !self.ambiguous_choices
    }

    pub(crate) async fn observe(
        &mut self,
        event: &Value,
        latest: &mut NormalizedUsage,
        sink: &mut dyn ProviderOutputSink,
    ) {
        let choices = event.get("choices").and_then(Value::as_array);
        let usage_only = choices.is_some_and(Vec::is_empty);
        let (saw_finish, new_choice) =
            choices.map_or((false, false), |choices| self.observe_choices(choices));
        let choices_finished = self.all_choices_closed();
        if (!choices_finished || new_choice)
            && self
                .pending_final
                .as_ref()
                .is_some_and(|report| !report.usage_only)
        {
            self.finish(sink, false).await;
        }
        let Some(usage) = reported_usage(event) else {
            return;
        };
        let report = UsageReport {
            at: Timestamp::now(),
            usage,
            usage_only,
        };
        // Empty usage objects are placeholders.
        if !report.usage.has_counts() {
            if !latest.has_counts() {
                *latest = report.usage.clone();
            }
            self.emit(sink, report, UsageFinality::Cumulative).await;
            return;
        }

        // An explicit usage-only tail takes precedence over choice finish reports.
        if let Some(tail) = self
            .pending_final
            .as_ref()
            .filter(|tail| tail.usage_only && !usage_only)
        {
            if tail.usage != report.usage {
                self.emit(sink, report, UsageFinality::Cumulative).await;
            }
            return;
        }

        // More choices or a usage-only tail can supersede a candidate before EOF.
        if let Some(previous) = self.pending_final.take()
            && previous.usage != report.usage
        {
            self.emit(sink, previous, UsageFinality::Cumulative).await;
        }

        *latest = report.usage.clone();
        if usage_only || (saw_finish && choices_finished) {
            self.pending_final = Some(report);
        } else {
            self.emit(sink, report, UsageFinality::Cumulative).await;
        }
    }

    pub(crate) async fn checkpoint(&mut self, sink: &mut dyn ProviderOutputSink) {
        // Persist provisional counters before waiting for the next chunk.
        if let Some(report) = self
            .pending_final
            .as_ref()
            .filter(|report| self.last_cumulative.as_ref() != Some(&report.usage))
            .cloned()
        {
            self.emit(sink, report, UsageFinality::Cumulative).await;
        }
    }

    pub(crate) async fn finish(&mut self, sink: &mut dyn ProviderOutputSink, complete: bool) {
        if let Some(report) = self.pending_final.take() {
            let finality = if report.usage_only || complete {
                UsageFinality::Final
            } else {
                UsageFinality::Cumulative
            };
            self.emit(sink, report, finality).await;
        }
    }

    async fn emit(
        &mut self,
        sink: &mut dyn ProviderOutputSink,
        report: UsageReport,
        finality: UsageFinality,
    ) {
        if finality == UsageFinality::Cumulative {
            if self.last_cumulative.as_ref() == Some(&report.usage) {
                return;
            }
            self.last_cumulative = Some(report.usage.clone());
        }
        announce_at(
            Some(sink),
            report.at,
            ProviderFact::Usage {
                usage: report.usage,
                finality,
            },
        )
        .await;
    }
}
