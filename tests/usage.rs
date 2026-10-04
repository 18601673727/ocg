use ocg::contracts::{JobLaunchRequest, UsageCompleteness, UsageWindow};
use ocg::orchestration::budget::{
    BillableUsage, BudgetConfig, PricingBasis, QuotaFacts, TokenPrice, UsageRecord,
};
use ocg::orchestration::domain::{
    AccountingAuthority, Attempt, DispatchAccounting, DomainRepository, Executor,
    ProviderSettlement,
};
use serde_json::{json, Value};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[test]
fn generated_usage_contract_matches_backend() {
    assert_eq!(
        ocg::contracts::render().expect("render canonical contracts"),
        include_str!("../frontend/components/ocg/contracts/generated.ts")
    );
}

fn turn(
    d: &mut DomainRepository,
    project: &str,
    session: &str,
    command: &str,
) -> Result<(String, Attempt, Executor)> {
    let job = d.create_job(project, "private prompt must not appear in usage")?;
    let (attempt, executor) = d.dispatch_job(&job.id, "provider")?;
    d.prepare_chat_turn(
        &JobLaunchRequest {
            command_id: command.into(),
            draft_id: command.into(),
            project_id: project.into(),
            session_id: session.into(),
            objective: "private prompt must not appear in usage".into(),
            success_criteria: None,
            constraints: None,
            hard_budget_micros: 0,
            resource_commitment: None,
        },
        command,
        &attempt,
        "private prompt must not appear in usage",
    )?;
    Ok((job.id, attempt, executor))
}

fn request(input: Option<u64>, output: Option<u64>) -> Value {
    json!({"purpose":"execution","reported_usage":{"input_tokens":input,"output_tokens":output,"total_tokens":input.zip(output).map(|(a,b)|a+b),"cache_read_tokens":0},
        "canonical":{"total_message_bytes":200,"tool_schema_bytes":60,"handoff_capsule_content_bytes":0,"tool_result_bytes":20},
        "wire":{"request_bytes":300},"tool_projection":{"full_schema_baseline_bytes":100,"schema_bytes_saved":40}})
}

fn provider_call(
    d: &mut DomainRepository,
    a: &Attempt,
    e: &Executor,
    requests: Vec<Value>,
) -> Result<String> {
    let c = d.create_call(
        &a.id,
        Some(&e.id),
        a.generation,
        true,
        "{\"executor_transport\":\"provider\"}",
    )?;
    d.set_provider_config(
        &c.id,
        "fixture",
        "friendly",
        "actual-model",
        "http://127.0.0.1:1",
        None,
    )?;
    d.start_call(&c.id, &a.id, a.generation)?;
    d.finish_call(
        &c.id,
        &a.id,
        a.generation,
        &json!({"context_costs":{"version":1,"requests":requests,"truncated":false}}).to_string(),
    )?;
    Ok(c.id)
}

#[test]
fn conversation_aggregates_all_turns_and_replaced_attempts_without_double_counting() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut d = DomainRepository::open(root.path())?;
    let p = d.ensure_project(root.path())?;
    let (job, a, e) = turn(&mut d, &p.id, "conversation", "first")?;
    provider_call(
        &mut d,
        &a,
        &e,
        vec![request(Some(100), Some(10)), request(Some(120), None)],
    )?;
    let replacement = d.replace_attempt(&job, "provider")?;
    provider_call(
        &mut d,
        &replacement.attempt,
        &replacement.executor,
        vec![request(Some(50), Some(5))],
    )?;
    let (_, a, e) = turn(&mut d, &p.id, "conversation", "second")?;
    provider_call(&mut d, &a, &e, vec![request(Some(40), Some(4))])?;
    let native = d.create_call(
        &a.id,
        Some(&e.id),
        a.generation,
        false,
        "{\"kind\":\"native_tool\"}",
    )?;
    d.start_call(&native.id, &a.id, a.generation)?;
    d.finish_call(
        &native.id,
        &a.id,
        a.generation,
        &serde_json::to_string(&ocg::native_tools::ToolResult::success(json!({})))?,
    )?;
    let c = d.conversation_usage(&p.id, "conversation")?;
    assert_eq!(
        (c.totals.turns, c.totals.jobs, c.totals.attempts),
        (2, 2, 3)
    );
    assert_eq!((c.totals.provider_calls, c.totals.native_calls), (3, 1));
    assert_eq!(c.totals.provider_requests.value, Some(4));
    assert_eq!(c.totals.tokens.input.value, Some(310));
    assert_eq!(
        c.totals.tokens.input.completeness,
        UsageCompleteness::Complete
    );
    assert_eq!(c.totals.tokens.output.value, Some(19));
    assert_eq!(
        c.totals.tokens.output.completeness,
        UsageCompleteness::Partial
    );
    assert_eq!(c.totals.tokens.cache_read.value, Some(0));
    assert_eq!(c.totals.tokens.cache_write.value, None);
    assert_eq!(c.totals.tokens.reasoning.value, None);
    assert_eq!(c.totals.context_costs.schema_bytes_saved.value, Some(160));
    assert_eq!(c.providers[0].totals.tokens.input.value, Some(310));
    assert_eq!(c.models[0].model.as_deref(), Some("actual-model"));
    assert_eq!(d.project_usage(&p.id, UsageWindow::All)?.totals, c.totals);
    assert_eq!(
        d.job_usage(&p.id, &job)?.totals.provider_requests.value,
        Some(3)
    );
    assert!(!serde_json::to_string(&c)?.contains("private prompt must not appear in usage"));
    assert_eq!(
        d.conversation_usage(&p.id, "conversation")?.totals,
        c.totals
    );
    Ok(())
}

#[test]
fn settled_money_is_idempotent_and_unresolved_reservations_are_not_actual() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut d = DomainRepository::open(root.path())?;
    let p = d.ensure_project(root.path())?;
    let (_, a, e) = turn(&mut d, &p.id, "money", "money-turn")?;
    let config = BudgetConfig {
        currency: Some("USD".into()),
        hard_limit_micros: Some(10_000_000),
        estimated_operation_cost_micros: Some(1000),
        pricing: vec![TokenPrice {
            provider: "fixture".into(),
            model: "m".into(),
            currency: "USD".into(),
            input_micros_per_million: 1_000_000,
            output_micros_per_million: 1_000_000,
            cache_read_micros_per_million: 0,
            cache_write_micros_per_million: 0,
        }],
        ..Default::default()
    };
    let claim = AccountingAuthority {
        attempt_id: a.id.clone(),
        generation: a.generation,
    };
    for failed in [false, true] {
        let c = d.create_call(
            &a.id,
            Some(&e.id),
            a.generation,
            true,
            "{\"executor_transport\":\"provider\"}",
        )?;
        d.freeze_dispatch_pricing(&c.id, &PricingBasis::resolve(&config, "fixture", "m"))?;
        d.admit_dispatch(&p.id, a.generation, &c.id, &config, QuotaFacts::unknown())?;
        d.start_call(&c.id, &a.id, a.generation)?;
        let accounting = if failed {
            DispatchAccounting::Failed
        } else {
            DispatchAccounting::Completed(ProviderSettlement {
                usage: Some(UsageRecord {
                    input_tokens: Some(100),
                    output_tokens: Some(20),
                    ..Default::default()
                }),
                billable: Some(BillableUsage {
                    input_tokens: Some(100),
                    output_tokens: Some(20),
                    ..Default::default()
                }),
            })
        };
        assert!(
            d.settle_dispatch_accounting(&c.id, &claim, &accounting)?
                .recorded
        );
        assert!(
            !d.settle_dispatch_accounting(&c.id, &claim, &accounting)?
                .recorded
        );
    }
    let usage = d.conversation_usage(&p.id, "money")?;
    assert_eq!(usage.totals.cost.actual_micros, Some(120));
    assert_eq!(usage.totals.cost.currency.as_deref(), Some("USD"));
    assert_eq!(usage.totals.cost.completeness, UsageCompleteness::Partial);
    assert_eq!(
        (
            usage.totals.cost.settled_calls,
            usage.totals.cost.unresolved_calls
        ),
        (1, 1)
    );
    assert_eq!(usage.totals.tokens.input.value, Some(100));
    assert_eq!(
        usage.totals.tokens.input.completeness,
        UsageCompleteness::Partial
    );
    assert_eq!(usage.totals.provider_requests.value, None);
    Ok(())
}

#[test]
fn windows_use_request_time_and_reads_preserve_project_isolation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut d = DomainRepository::open(root.path())?;
    let p = d.ensure_project(root.path())?;
    let (job, a, e) = turn(&mut d, &p.id, "old", "old-turn")?;
    let call = provider_call(&mut d, &a, &e, vec![request(Some(10), Some(2))])?;
    let connection = rusqlite::Connection::open(d.path())?;
    connection.execute(
        "UPDATE domain_calls SET created_at=created_at-172800 WHERE id=?1",
        [&call],
    )?;
    connection.execute(
        "UPDATE domain_jobs SET created_at=created_at-172800 WHERE id=?1",
        [&job],
    )?;
    let before = d.call(&call)?;
    assert_eq!(
        d.project_usage(&p.id, UsageWindow::Today)?
            .totals
            .provider_requests
            .value,
        Some(0)
    );
    assert_eq!(
        d.project_usage(&p.id, UsageWindow::SevenDays)?
            .totals
            .provider_requests
            .value,
        Some(1)
    );
    assert_eq!(
        d.project_usage(&p.id, UsageWindow::ThirtyDays)?
            .totals
            .tokens
            .input
            .value,
        Some(10)
    );
    assert_eq!(d.call(&call)?, before);
    let another = tempfile::tempdir()?;
    let other = d.ensure_project(another.path())?;
    assert!(d.conversation_usage(&other.id, "old").is_err());
    assert!(d.job_usage(&other.id, &job).is_err());
    assert_eq!(d.project_usage(&other.id, UsageWindow::All)?.totals.jobs, 0);
    Ok(())
}

#[test]
fn listing_bound_does_not_make_complete_aggregate_partial() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut d = DomainRepository::open(root.path())?;
    let p = d.ensure_project(root.path())?;
    for n in 0..101 {
        turn(&mut d, &p.id, &format!("session-{n}"), &format!("turn-{n}"))?;
    }
    let usage = d.project_usage(&p.id, UsageWindow::All)?;
    assert!(usage.truncated);
    assert_eq!(usage.conversations, 101);
    assert_eq!(usage.conversation_rows.len(), 100);
    assert_eq!(usage.totals.jobs, 101);
    assert_eq!(usage.totals.provider_requests.value, Some(0));
    assert_eq!(
        usage.totals.provider_requests.completeness,
        UsageCompleteness::Complete
    );
    Ok(())
}

#[test]
fn mismatched_schema_baseline_never_manufactures_savings() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut d = DomainRepository::open(root.path())?;
    let p = d.ensure_project(root.path())?;
    let (_, a, e) = turn(&mut d, &p.id, "savings", "savings-turn")?;
    let mut recorded = request(Some(1), Some(1));
    recorded["tool_projection"]["schema_bytes_saved"] = json!(99);
    provider_call(&mut d, &a, &e, vec![recorded])?;
    let c = d.conversation_usage(&p.id, "savings")?;
    assert_eq!(c.totals.context_costs.schema_bytes_saved.value, None);
    assert_eq!(
        c.totals.context_costs.schema_bytes_saved.completeness,
        UsageCompleteness::Unavailable
    );
    Ok(())
}
