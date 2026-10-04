use super::{invalid, now, sql, validate_id, DomainRepository};
use crate::contracts::*;
use crate::error::Result;
use rusqlite::{params, OptionalExtension};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const MAX_CALLS: usize = 50_000;
const MAX_CONVERSATIONS: usize = 100;
const MAX_BREAKDOWNS: usize = 64;
const API_VERSION: &str = super::super::canonical_control::CANONICAL_CONTROL_API_VERSION;

#[derive(Clone, Copy, Default)]
struct Counter {
    value: u64,
    known: u64,
    missing: u64,
}

impl Counter {
    fn add(&mut self, value: Option<u64>) -> Result<()> {
        if let Some(value) = value {
            self.value = self
                .value
                .checked_add(value)
                .ok_or_else(|| invalid("usage counter overflow"))?;
            self.known += 1;
        } else {
            self.missing += 1;
        }
        Ok(())
    }

    fn finish(self, limited: bool) -> UsageQuantity {
        let missing = self.missing > 0 || limited;
        UsageQuantity {
            value: if self.known == 0 && missing {
                None
            } else {
                Some(self.value)
            },
            completeness: if self.known == 0 && missing {
                UsageCompleteness::Unavailable
            } else if missing {
                UsageCompleteness::Partial
            } else {
                UsageCompleteness::Complete
            },
        }
    }
}

#[derive(Default)]
struct Aggregate {
    jobs: BTreeSet<String>,
    attempts: BTreeSet<String>,
    turns: BTreeSet<String>,
    provider_calls: u64,
    native_calls: u64,
    counters: [Counter; 16],
    currencies: BTreeMap<String, i64>,
    settled: u64,
    unresolved: u64,
    unavailable: u64,
    limited: bool,
    first: Option<i64>,
    last: Option<i64>,
}

struct CallFacts {
    job: String,
    attempt: Option<String>,
    conversation: Option<String>,
    call: Option<String>,
    provider: bool,
    native: bool,
    created: i64,
    updated: i64,
    metadata: bool,
    limited: bool,
    route_provider: Option<String>,
    route_model: Option<String>,
    money: Value,
    unresolved: bool,
    legacy_usage: Option<Value>,
}

impl Aggregate {
    fn call(&mut self, facts: &CallFacts) -> Result<()> {
        self.limited |= facts.provider && facts.limited;
        self.jobs.insert(facts.job.clone());
        if let Some(attempt) = &facts.attempt {
            self.attempts.insert(attempt.clone());
        }
        if facts.conversation.is_some() {
            self.turns.insert(facts.job.clone());
        }
        self.first = Some(
            self.first
                .map_or(facts.created, |time| time.min(facts.created)),
        );
        self.last = Some(
            self.last
                .map_or(facts.updated, |time| time.max(facts.updated)),
        );
        self.native_calls += u64::from(facts.native);
        if !facts.provider {
            return Ok(());
        }
        self.provider_calls += 1;
        if !facts.metadata {
            for (index, counter) in self.counters.iter_mut().enumerate() {
                let key = match index {
                    2 => Some("input"),
                    3 => Some("output"),
                    4 => Some("reasoning"),
                    5 => Some("cache_read"),
                    6 => Some("cache_write"),
                    _ => None,
                };
                let value = key.and_then(|key| {
                    facts
                        .legacy_usage
                        .as_ref()
                        .and_then(|usage| usage[key].as_u64())
                });
                counter.add(value)?;
            }
        }
        let mut settled = false;
        for money in facts
            .money
            .as_array()
            .ok_or_else(|| invalid("invalid usage Money projection"))?
        {
            let Some(amount) = money["actual"].as_i64() else {
                continue;
            };
            let currency = money["currency"]
                .as_str()
                .ok_or_else(|| invalid("settled usage has no currency"))?;
            if amount < 0 || currency.len() > 8 {
                return Err(invalid("invalid settled usage Money"));
            }
            if !self.currencies.contains_key(currency) && self.currencies.len() >= MAX_BREAKDOWNS {
                self.limited = true;
                continue;
            }
            let current = self.currencies.entry(currency.to_owned()).or_default();
            *current = current
                .checked_add(amount)
                .ok_or_else(|| invalid("usage Money overflow"))?;
            settled = true;
        }
        if settled {
            self.settled += 1;
        } else if facts.unresolved {
            self.unresolved += 1;
        } else {
            self.unavailable += 1;
        }
        Ok(())
    }

    fn request(&mut self, request: &Value) -> Result<()> {
        let number = |key: &str| request[key].as_u64();
        let projected = number("schema");
        let baseline = number("baseline");
        let saved = number("saved").filter(|saved| {
            baseline
                .zip(projected)
                .is_some_and(|(baseline, projected)| {
                    baseline.checked_sub(projected) == Some(*saved)
                })
        });
        let capsule = number("capsule");
        let values = [
            Some(1),
            match request["purpose"].as_str() {
                Some("execution") => Some(1),
                Some("context_summary") => Some(0),
                _ => None,
            },
            number("input"),
            number("output"),
            number("reasoning"),
            number("cache_read"),
            number("cache_write"),
            number("total"),
            number("messages"),
            projected,
            baseline,
            saved,
            number("wire"),
            capsule,
            capsule.map(|value| u64::from(value > 0)),
            number("tool_results"),
        ];
        for (counter, value) in self.counters.iter_mut().zip(values) {
            counter.add(value)?;
        }
        Ok(())
    }

    fn finish(&self, limited: bool) -> UsageTotals {
        let limited = limited || self.limited;
        let quantity = |index: usize| self.counters[index].finish(limited);
        let currencies = self
            .currencies
            .iter()
            .map(|(currency, actual_micros)| UsageCurrencyCost {
                currency: currency.clone(),
                actual_micros: *actual_micros,
            })
            .collect::<Vec<_>>();
        let single = (currencies.len() == 1).then(|| &currencies[0]);
        UsageTotals {
            turns: self.turns.len() as u64,
            jobs: self.jobs.len() as u64,
            attempts: self.attempts.len() as u64,
            provider_calls: self.provider_calls,
            native_calls: self.native_calls,
            provider_requests: quantity(0),
            provider_rounds: quantity(1),
            tokens: UsageTokenTotals {
                input: quantity(2),
                output: quantity(3),
                reasoning: quantity(4),
                cache_read: quantity(5),
                cache_write: quantity(6),
                total: quantity(7),
            },
            cost: UsageCost {
                source: UsageCostSource::CanonicalSettlement,
                actual_micros: single.map(|value| value.actual_micros),
                currency: single.map(|value| value.currency.clone()),
                currencies,
                completeness: if self.settled == 0 {
                    UsageCompleteness::Unavailable
                } else if self.unresolved > 0 || self.unavailable > 0 || limited {
                    UsageCompleteness::Partial
                } else {
                    UsageCompleteness::Complete
                },
                settled_calls: self.settled,
                unresolved_calls: self.unresolved,
                unavailable_calls: self.unavailable,
            },
            context_costs: ContextCostTotals {
                canonical_message_bytes: quantity(8),
                tool_schema_bytes: quantity(9),
                full_schema_baseline_bytes: quantity(10),
                schema_bytes_saved: quantity(11),
                wire_bytes: quantity(12),
                capsule_bytes: quantity(13),
                capsule_injected_requests: quantity(14),
                tool_result_bytes: quantity(15),
            },
            first_activity_at: self.first,
            last_activity_at: self.last,
        }
    }
}

struct ConversationRow {
    id: String,
    session: String,
    title: Option<String>,
    created: String,
    updated: String,
    totals: Aggregate,
    providers: BTreeSet<String>,
    models: BTreeSet<String>,
}

struct Projection {
    totals: Aggregate,
    providers: BTreeMap<Option<String>, Aggregate>,
    models: BTreeMap<(Option<String>, Option<String>), Aggregate>,
    rows: Vec<ConversationRow>,
    conversations: u64,
    truncated: bool,
    incomplete_facts: bool,
}

// Only numeric accounting fields cross this boundary. SQLite parses the stored
// JSON; prompts, diagnostics, credentials and transcripts never enter Rust.
const FACTS_SQL: &str = r#"
WITH selected AS (
 SELECT j.id AS job,j.created_at AS job_created,j.updated_at AS job_updated,
        t.conversation_id,a.id AS attempt,c.id AS call,c.created_at,c.finished_at,
        CASE WHEN json_valid(c.request) THEN json_extract(c.request,'$.executor_transport')='provider' ELSE 0 END AS is_provider,
        CASE WHEN json_valid(c.request) THEN json_extract(c.request,'$.kind')='native_tool' ELSE 0 END AS is_native,
        CASE WHEN json_valid(c.response) THEN CASE WHEN json_extract(c.response,'$.context_costs.version')=1
          AND json_type(c.response,'$.context_costs.requests')='array' THEN json_extract(c.response,'$.context_costs.requests') END END AS requests,
        CASE WHEN json_valid(c.response) THEN (COALESCE(json_extract(c.response,'$.context_costs.truncated'),0)
          OR COALESCE(json_array_length(c.response,'$.context_costs.requests'),0)>64) ELSE 0 END AS limited,
        i.provider_key,COALESCE(i.upstream_model_id,i.model) AS model,
        i.reservation_id,i.effect_state
 FROM domain_jobs j
 LEFT JOIN domain_chat_turns t ON t.job_id=j.id
 LEFT JOIN domain_attempts a ON a.job_id=j.id
 LEFT JOIN domain_calls c ON c.attempt_id=a.id
 LEFT JOIN domain_dispatch_intents i ON i.call_id=c.id
 WHERE j.project_id=?1 AND (?2 IS NULL OR t.conversation_id=?2) AND (?3 IS NULL OR j.id=?3)
   AND (?4 IS NULL OR COALESCE(c.created_at,j.created_at)>=?4)
 ORDER BY COALESCE(c.created_at,j.created_at) DESC,c.id DESC,a.id DESC,j.id DESC
 LIMIT 50001
), money AS (
 SELECT s.call_id,json_extract(s.settlement,'$.effect.actual.currency') AS currency,
        SUM(CASE WHEN s.disposition='settled' AND s.usage_authoritative=1
                 THEN json_extract(s.settlement,'$.effect.actual.micros') END) AS actual,
        MAX(s.disposition IN ('settled','released')) AS closed,
        MAX(s.disposition='unresolved') AS unresolved
 FROM domain_settlements s JOIN selected c ON c.call=s.call_id
 WHERE s.project_id=?1 GROUP BY s.call_id,currency
), costs AS (
 SELECT call_id,json_group_array(json_object('currency',currency,'actual',actual)) AS amounts,
        MAX(closed) AS closed,MAX(unresolved) AS unresolved
 FROM money GROUP BY call_id
), facts AS (
 SELECT c.*,
   COALESCE(costs.amounts,'[]') AS amounts,
   (NOT COALESCE(costs.closed,0) AND (COALESCE(costs.unresolved,0)
     OR (c.reservation_id IS NOT NULL AND c.effect_state='unknown'))) AS unresolved
 FROM selected c LEFT JOIN costs ON costs.call_id=c.call
)
SELECT job,attempt,conversation_id,call,COALESCE(is_provider,0),COALESCE(is_native,0),
       COALESCE(created_at,job_created),COALESCE(finished_at,created_at,job_updated),
       provider_key,model,requests IS NOT NULL,COALESCE(limited,0),amounts,unresolved,r.key,
       CASE WHEN r.key IS NOT NULL THEN json_object(
         'purpose',json_extract(r.value,'$.purpose'),
         'input',json_extract(r.value,'$.reported_usage.input_tokens'),
         'output',json_extract(r.value,'$.reported_usage.output_tokens'),
         'reasoning',json_extract(r.value,'$.reported_usage.reasoning_tokens'),
         'cache_read',json_extract(r.value,'$.reported_usage.cache_read_tokens'),
         'cache_write',json_extract(r.value,'$.reported_usage.cache_write_tokens'),
         'total',json_extract(r.value,'$.reported_usage.total_tokens'),
         'messages',json_extract(r.value,'$.canonical.total_message_bytes'),
         'schema',json_extract(r.value,'$.canonical.tool_schema_bytes'),
         'baseline',json_extract(r.value,'$.tool_projection.full_schema_baseline_bytes'),
         'saved',json_extract(r.value,'$.tool_projection.schema_bytes_saved'),
         'wire',json_extract(r.value,'$.wire.request_bytes'),
         'capsule',json_extract(r.value,'$.canonical.handoff_capsule_content_bytes'),
         'tool_results',json_extract(r.value,'$.canonical.tool_result_bytes')) END,
       CASE WHEN requests IS NULL THEN (SELECT json_object('input',json_extract(s.settlement,'$.usage.input_tokens'),
         'output',json_extract(s.settlement,'$.usage.output_tokens'),
         'reasoning',json_extract(s.settlement,'$.usage.reasoning_tokens'),
         'cache_read',json_extract(s.settlement,'$.usage.cache_read_tokens'),
         'cache_write',json_extract(s.settlement,'$.usage.cache_write_tokens'))
        FROM domain_settlements s WHERE s.call_id=facts.call AND s.project_id=?1
         AND json_type(s.settlement,'$.usage')='object'
        ORDER BY s.usage_authoritative DESC,s.created_at DESC,s.settlement_id DESC LIMIT 1) END
FROM facts LEFT JOIN json_each(COALESCE(requests,'[]')) r ON CAST(r.key AS INTEGER)<64
ORDER BY COALESCE(created_at,job_created) DESC,call DESC,attempt DESC,job DESC,r.key
"#;

impl DomainRepository {
    fn usage_projection(
        &self,
        project: &str,
        conversation: Option<&str>,
        job: Option<&str>,
        since: Option<i64>,
    ) -> Result<Projection> {
        validate_id(project)?;
        let exists: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_projects WHERE id=?1)",
                [project],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !exists {
            return Err(invalid("unknown Project"));
        }
        let mut projection = Projection {
            totals: Aggregate::default(),
            providers: BTreeMap::new(),
            models: BTreeMap::new(),
            rows: Vec::new(),
            conversations: 0,
            truncated: false,
            incomplete_facts: false,
        };
        let mut query = self.connection.prepare(
            "SELECT v.id,v.session_id,substr(json_extract(v.record,'$.title'),1,160),json_extract(v.record,'$.created_at'),json_extract(v.record,'$.updated_at'),json_extract(v.record,'$.archived_at') IS NOT NULL
             FROM domain_conversations v WHERE v.project_id=?1 AND (?2 IS NULL OR v.id=?2) AND (?4 IS NULL OR EXISTS(SELECT 1 FROM domain_chat_turns t WHERE t.conversation_id=v.id AND t.job_id=?4))
             AND (?3 IS NULL OR EXISTS(SELECT 1 FROM domain_chat_turns t JOIN domain_jobs j ON j.id=t.job_id
               WHERE t.conversation_id=v.id AND (j.created_at>=?3 OR EXISTS(SELECT 1 FROM domain_calls c JOIN domain_attempts a ON a.id=c.attempt_id WHERE a.job_id=j.id AND c.created_at>=?3))))
             ORDER BY CAST(json_extract(v.record,'$.updated_at') AS INTEGER) DESC,v.id DESC") .map_err(sql)?;
        let mut rows = query
            .query(params![project, conversation, since, job])
            .map_err(sql)?;
        while let Some(row) = rows.next().map_err(sql)? {
            projection.conversations += 1;
            if conversation.is_none() && row.get::<_, bool>(5).map_err(sql)? {
                continue;
            }
            if projection.rows.len() < MAX_CONVERSATIONS {
                projection.rows.push(ConversationRow {
                    id: row.get(0).map_err(sql)?,
                    session: row.get(1).map_err(sql)?,
                    title: row
                        .get::<_, Option<String>>(2)
                        .map_err(sql)?
                        .map(|value| bounded(&value)),
                    created: row.get(3).map_err(sql)?,
                    updated: row.get(4).map_err(sql)?,
                    totals: Aggregate::default(),
                    providers: BTreeSet::new(),
                    models: BTreeSet::new(),
                });
            } else {
                projection.truncated = true;
            }
        }
        let row_indexes = projection
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| (row.id.clone(), index))
            .collect::<BTreeMap<_, _>>();
        let mut query = self.connection.prepare(FACTS_SQL).map_err(sql)?;
        let mut rows = query
            .query(params![project, conversation, job, since])
            .map_err(sql)?;
        let mut previous: Option<(String, Option<String>, Option<String>)> = None;
        let mut selected_count = 0;
        while let Some(row) = rows.next().map_err(sql)? {
            let facts = CallFacts {
                job: row.get(0).map_err(sql)?,
                attempt: row.get(1).map_err(sql)?,
                conversation: row.get(2).map_err(sql)?,
                call: row.get(3).map_err(sql)?,
                provider: row.get::<_, i64>(4).map_err(sql)? != 0,
                native: row.get::<_, i64>(5).map_err(sql)? != 0,
                created: row.get(6).map_err(sql)?,
                updated: row.get(7).map_err(sql)?,
                route_provider: row
                    .get::<_, Option<String>>(8)
                    .map_err(sql)?
                    .filter(|value| value.len() <= 512),
                route_model: row
                    .get::<_, Option<String>>(9)
                    .map_err(sql)?
                    .filter(|value| value.len() <= 512),
                metadata: row.get::<_, i64>(10).map_err(sql)? != 0,
                limited: row.get::<_, i64>(11).map_err(sql)? != 0,
                money: serde_json::from_str(&row.get::<_, String>(12).map_err(sql)?)
                    .map_err(|error| invalid(&error.to_string()))?,
                unresolved: row.get::<_, i64>(13).map_err(sql)? != 0,
                legacy_usage: row
                    .get::<_, Option<String>>(16)
                    .map_err(sql)?
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| invalid(&error.to_string()))?,
            };
            let identity = (facts.job.clone(), facts.attempt.clone(), facts.call.clone());
            let first = previous.as_ref() != Some(&identity);
            if first {
                selected_count += 1;
            }
            if selected_count > MAX_CALLS {
                projection.truncated = true;
                projection.incomplete_facts = true;
                break;
            }
            previous = Some(identity);
            let row_index = facts
                .conversation
                .as_ref()
                .and_then(|id| row_indexes.get(id))
                .copied();
            if first {
                projection.totals.call(&facts)?;
                projection.truncated |= facts.limited;
                if let Some(index) = row_index {
                    let conversation = &mut projection.rows[index];
                    conversation.totals.call(&facts)?;
                    if facts.provider {
                        if let Some(provider) = &facts.route_provider {
                            if conversation.providers.len() < MAX_BREAKDOWNS {
                                conversation.providers.insert(provider.clone());
                            } else {
                                projection.truncated = true;
                            }
                        }
                        if let Some(model) = &facts.route_model {
                            if conversation.models.len() < MAX_BREAKDOWNS {
                                conversation.models.insert(model.clone());
                            } else {
                                projection.truncated = true;
                            }
                        }
                    }
                }
            }
            let request: Option<String> = row.get(15).map_err(sql)?;
            let request = request
                .filter(|_| facts.provider)
                .map(|value| serde_json::from_str::<Value>(&value))
                .transpose()
                .map_err(|error| invalid(&error.to_string()))?;
            if let Some(request) = &request {
                projection.totals.request(request)?;
                if let Some(index) = row_index {
                    projection.rows[index].totals.request(request)?;
                }
            }
            if facts.provider {
                let provider = facts.route_provider.clone();
                let model = (provider.clone(), facts.route_model.clone());
                if !projection.providers.contains_key(&provider)
                    && projection.providers.len() >= MAX_BREAKDOWNS
                {
                    projection.truncated = true;
                } else {
                    let total = projection.providers.entry(provider).or_default();
                    if first {
                        total.call(&facts)?;
                    }
                    if let Some(request) = &request {
                        total.request(request)?;
                    }
                }
                if !projection.models.contains_key(&model)
                    && projection.models.len() >= MAX_BREAKDOWNS
                {
                    projection.truncated = true;
                } else {
                    let total = projection.models.entry(model).or_default();
                    if first {
                        total.call(&facts)?;
                    }
                    if let Some(request) = &request {
                        total.request(request)?;
                    }
                }
            }
        }
        projection.truncated |= projection.totals.limited;
        Ok(projection)
    }

    pub fn project_usage(
        &self,
        project_id: &str,
        window: UsageWindow,
    ) -> Result<ProjectUsageResponse> {
        let generated_at = now();
        let since = match window {
            UsageWindow::All => None,
            UsageWindow::Today => Some(generated_at - generated_at.rem_euclid(86400)),
            UsageWindow::SevenDays => Some(generated_at.saturating_sub(7 * 86400)),
            UsageWindow::ThirtyDays => Some(generated_at.saturating_sub(30 * 86400)),
        };
        let transaction = self.connection.unchecked_transaction().map_err(sql)?;
        let projection = self.usage_projection(project_id, None, None, since)?;
        let result = ProjectUsageResponse {
            api_version: API_VERSION.into(),
            project_id: project_id.into(),
            window,
            window_started_at: since,
            generated_at,
            conversations: projection.conversations,
            totals: projection.totals.finish(projection.incomplete_facts),
            providers: provider_rows(&projection),
            models: model_rows(&projection),
            conversation_rows: projection
                .rows
                .iter()
                .map(|row| UsageConversationRow {
                    conversation_id: row.id.clone(),
                    session_id: row.session.clone(),
                    title: row.title.clone(),
                    updated_at: row.updated.clone(),
                    totals: row.totals.finish(projection.incomplete_facts),
                    providers: row.providers.iter().cloned().collect(),
                    models: row.models.iter().cloned().collect(),
                })
                .collect(),
            truncated: projection.truncated,
        };
        transaction.commit().map_err(sql)?;
        Ok(result)
    }

    pub fn conversation_usage(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<ConversationUsageResponse> {
        validate_id(session_id)?;
        let transaction = self.connection.unchecked_transaction().map_err(sql)?;
        let conversation: String = self
            .connection
            .query_row(
                "SELECT id FROM domain_conversations WHERE project_id=?1 AND session_id=?2",
                params![project_id, session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?
            .ok_or_else(|| invalid("unknown Conversation"))?;
        let projection = self.usage_projection(project_id, Some(&conversation), None, None)?;
        let row = projection
            .rows
            .first()
            .ok_or_else(|| invalid("unknown Conversation"))?;
        let latest_job_state = self.connection.query_row("SELECT j.state FROM domain_chat_turns t JOIN domain_jobs j ON j.id=t.job_id WHERE t.conversation_id=?1 ORDER BY t.turn_order DESC LIMIT 1",[&conversation],|row|row.get(0)).optional().map_err(sql)?;
        let result = ConversationUsageResponse {
            api_version: API_VERSION.into(),
            project_id: project_id.into(),
            conversation_id: conversation,
            session_id: session_id.into(),
            title: row.title.clone(),
            created_at: row.created.clone(),
            updated_at: row.updated.clone(),
            latest_job_state,
            generated_at: now(),
            totals: projection.totals.finish(projection.incomplete_facts),
            providers: provider_rows(&projection),
            models: model_rows(&projection),
            truncated: projection.truncated,
        };
        transaction.commit().map_err(sql)?;
        Ok(result)
    }

    pub fn job_usage(&self, project_id: &str, job_id: &str) -> Result<JobUsageResponse> {
        validate_id(job_id)?;
        let transaction = self.connection.unchecked_transaction().map_err(sql)?;
        let owns: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_jobs WHERE project_id=?1 AND id=?2)",
                params![project_id, job_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !owns {
            return Err(invalid("unknown Project Job"));
        }
        let projection = self.usage_projection(project_id, None, Some(job_id), None)?;
        let result = JobUsageResponse {
            api_version: API_VERSION.into(),
            project_id: project_id.into(),
            job_id: job_id.into(),
            generated_at: now(),
            totals: projection.totals.finish(projection.incomplete_facts),
            providers: provider_rows(&projection),
            models: model_rows(&projection),
            truncated: projection.truncated,
        };
        transaction.commit().map_err(sql)?;
        Ok(result)
    }
}

fn bounded(value: &str) -> String {
    value.chars().take(160).collect()
}

fn provider_rows(projection: &Projection) -> Vec<UsageBreakdown> {
    projection
        .providers
        .iter()
        .map(|(provider, total)| UsageBreakdown {
            provider: provider.clone(),
            model: None,
            totals: total.finish(projection.incomplete_facts),
        })
        .collect()
}

fn model_rows(projection: &Projection) -> Vec<UsageBreakdown> {
    projection
        .models
        .iter()
        .map(|((provider, model), total)| UsageBreakdown {
            provider: provider.clone(),
            model: model.clone(),
            totals: total.finish(projection.incomplete_facts),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_currency_cost_preserves_buckets_without_fx() {
        let totals = Aggregate {
            currencies: BTreeMap::from([("EUR".into(), 200), ("USD".into(), 100)]),
            settled: 2,
            ..Default::default()
        }
        .finish(false);
        assert_eq!(totals.cost.actual_micros, None);
        assert_eq!(totals.cost.currency, None);
        assert_eq!(
            totals.cost.currencies,
            vec![
                UsageCurrencyCost {
                    currency: "EUR".into(),
                    actual_micros: 200
                },
                UsageCurrencyCost {
                    currency: "USD".into(),
                    actual_micros: 100
                },
            ]
        );
        assert_eq!(totals.cost.completeness, UsageCompleteness::Complete);
    }
}
