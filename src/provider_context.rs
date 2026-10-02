//! The OCG-native Active Context Projection.
//!
//! Everything a provider request carries is decided in this one place. It is a
//! projection, not an authority: it reads the canonical structured state that
//! already exists — the Context Engine's plan, the newest compiler diagnostic
//! delta, the recorded Call outcomes, the compaction module — and renders it.
//! It stores nothing, so it cannot become a second source of truth for any of
//! them.
//!
//! ```text
//! repository context   ─┐
//! compiler delta       ─┤
//! call failures        ─┼─► active context projection ─► provider request
//! conversation history ─┘        (compacted)
//! ```
//!
//! ## Why one message
//!
//! The projection is emitted as a single `system` message rather than appended
//! to the conversation. That placement is what keeps the two layers separate:
//! compaction only ever sees conversation history, so structured facts are never
//! summarized, re-summarized or duplicated into a second copy by a lossy layer.
//! Re-projected each turn, they stay current instead of drifting.
//!
//! ## What is not here
//!
//! Raw compiler output and raw Call input/output stay where they were written.
//! This module renders the bounded projection of each; it never re-injects the
//! original bytes, so an 80 KB tool payload cannot reach the model by being
//! summarized back to itself.

use crate::compaction::{
    CanonicalBlock, CompactionConfig, CompactionOutcome, CompactionPoint, Context, History,
    ModelLimits, Transcript,
};
use crate::context::{ContextConfig, ContextEngine};
use crate::error::Result;
use serde_json::{json, Value};
use std::path::Path;

/// Hard ceiling on any one rendered section.
///
/// A plan is already bounded by its own ranking budget; this is the outer
/// guard so a misconfigured budget cannot reach the provider unbounded.
const MAX_SECTION_BYTES: usize = 32_000;
/// Ceiling on the whole rendered projection.
const MAX_PROJECTION_BYTES: usize = 48_000;
/// How many outstanding Call failures the projection names.
const MAX_CALL_FAILURES: usize = 8;
/// Ceiling on the rendered Call-failure section.
const MAX_CALL_SECTION_BYTES: usize = 8_000;

/// Issues the summarizing model call for a compaction.
///
/// Supplied by the caller rather than built here, because only the provider
/// loop holds the provider. The call runs under the authority of the provider
/// Call being executed: it is a step in assembling that Call's context, not a
/// separate execution, so it needs no admission of its own.
pub type Summarize<'f> =
    Box<dyn Fn(String, u64) -> crate::http::BoxFuture<'f, Result<String>> + Send + Sync + 'f>;

/// What the projection is built from.
pub struct ContextInputs<'a, 'f> {
    /// The task the plan is ranked against.
    pub task: &'a str,
    /// The Attempt whose Calls count as outstanding work, and whose compaction
    /// points apply.
    pub attempt_id: &'a str,
    /// The tool definitions, so compaction measures the real request.
    pub tools: Option<&'a Value>,
    /// The conversation as assembled so far.
    pub conversation: Vec<Value>,
    /// How to reach a model, when one is available.
    pub summarize: Option<Summarize<'f>>,
}

/// One assembled provider request context.
pub struct ActiveContext {
    /// The single system message, or `None` when nothing had to be said.
    pub system: Option<Value>,
    /// The conversation to send, after compaction.
    pub messages: Vec<Value>,
    /// Which canonical sources contributed, for logs and dedup accounting.
    pub sources: Vec<&'static str>,
    /// Size of the rendered projection.
    pub bytes: usize,
}

/// Build the active context projection and compact the conversation.
///
/// `project_root` locates every canonical store; nothing here contacts a
/// network or mutates durable state. A source that cannot be read is reported as
/// a bounded note in the projection rather than raised: losing repository
/// context must not abort a Call the Runtime can otherwise complete.
pub async fn assemble<'a, 'f>(
    project_root: &Path,
    inputs: ContextInputs<'a, 'f>,
) -> Result<ActiveContext> {
    let config = config_data(project_root);
    // One fail-soft read of the orchestration state, shared by every canonical
    // source that lives in it: the compiler baseline and the compaction points.
    let state = crate::orchestration::state::load(project_root);
    let mut sources = Vec::new();
    let mut sections: Vec<(String, String)> = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    if state.corrupt {
        notes.push(
            "orchestration state could not be read; compiler feedback and stored compaction \
             points are unavailable"
                .to_string(),
        );
    }

    if let Ok(data) = &config {
        match repository_section(project_root, inputs.task, data) {
            Ok(Some(section)) => {
                sources.push("repository");
                sections.push(("repository context".to_string(), section));
            }
            Ok(None) => {}
            Err(error) => notes.push(format!("repository context unavailable: {error}")),
        }
    } else if let Err(error) = &config {
        notes.push(format!("configuration unavailable: {error}"));
    }

    match compiler_section(&state.state) {
        Ok(Some(section)) => {
            sources.push("compiler");
            sections.push(("compiler feedback".to_string(), section));
        }
        Ok(None) => {}
        Err(error) => notes.push(format!("compiler feedback unavailable: {error}")),
    }

    match call_section(project_root, inputs.attempt_id, &inputs.conversation) {
        Ok(Some(section)) => {
            sources.push("calls");
            sections.push(("outstanding Call failures".to_string(), section));
        }
        Ok(None) => {}
        Err(error) => notes.push(format!("Call history unavailable: {error}")),
    }

    let (system, bytes) = render_projection(&sections, &notes);
    let (messages, compacted) = compact_conversation(
        project_root,
        &inputs.conversation,
        inputs.tools,
        &config,
        &state.state,
        inputs.attempt_id,
        inputs.summarize,
    )
    .await;
    if let Some(point) = compacted {
        persist_compaction_point(project_root, inputs.attempt_id, &point);
    }

    Ok(ActiveContext {
        system,
        messages,
        sources,
        bytes,
    })
}

/// Persist an accepted compaction point next to the rest of the Attempt's
/// canonical state.
///
/// Best effort by design: a summary that could not be written is still usable
/// for this request, and losing it only means the next request re-sends the
/// history it covered. Failing the Call would be worse than the duplication it
/// avoids.
fn persist_compaction_point(
    project_root: &Path,
    attempt_id: &str,
    point: &crate::compaction::CompactionPoint,
) {
    let mut state = crate::orchestration::state::load(project_root).state;
    state.record_compaction_point(attempt_id, point.clone());
    if let Err(error) = crate::orchestration::state::save(project_root, &state) {
        tracing::warn!(%error, point = %point.id, "compaction point was not persisted");
    }
}

/// Render the sections into one system message.
///
/// Bounded twice: per section and in total. Truncation is stated, never silent,
/// because a model cannot act on context it cannot see is missing.
fn render_projection(sections: &[(String, String)], notes: &[String]) -> (Option<Value>, usize) {
    let mut body = String::new();
    for (title, content) in sections {
        let content = truncate(content, MAX_SECTION_BYTES).0;
        body.push_str(&format!("### {title}\n{content}\n\n"));
    }
    if !notes.is_empty() {
        body.push_str("### context notes\n");
        for note in notes {
            body.push_str(&format!("- {note}\n"));
        }
        body.push('\n');
    }
    let body = truncate(&body, MAX_PROJECTION_BYTES).0;
    if body.trim().is_empty() {
        return (None, 0);
    }
    let content = format!("OCG active context (structured projection)\n\n{body}");
    let bytes = content.len();
    (
        Some(json!({"role": "system", "content": content})),
        bytes,
    )
}

/// The bounded repository-context section, from the existing Context Engine.
///
/// The plan is whatever ranking and the budget already selected — no second
/// selection is applied here, and no unbounded dump of the repository is ever
/// substituted for a plan.
fn repository_section(
    project_root: &Path,
    task: &str,
    config: &Value,
) -> Result<Option<String>> {
    let context_config = ContextConfig::from_config(config)?;
    if !context_config.enabled {
        return Ok(None);
    }
    let git = crate::process::SystemGitHost;
    let clock = crate::clock::SystemClock;
    let engine = ContextEngine::new(project_root, context_config, &git, &clock);
    let outcome = engine.plan(task, None)?;
    let mut text = crate::context::plan_text(&outcome.plan);
    if outcome.from_cache {
        text.push_str("\n(reused from a validated context cache entry)\n");
    }
    for warning in &outcome.warnings {
        text.push_str(&format!("\nnote: {warning}\n"));
    }
    Ok(Some(text))
}

/// The newest compiler diagnostic delta, as the bounded observation.
///
/// The delta is read from the same record the next compile compares against, so
/// the model sees the delta the verification run actually reported rather than a
/// recount. Raw compiler output is not consulted: it stays in `.ocg/logs/`.
fn compiler_section(
    state: &crate::orchestration::state::OrchestrationState,
) -> Result<Option<String>> {
    let Some((command, delta)) = state.latest_compiler_baseline() else {
        return Ok(None);
    };
    Ok(Some(format!(
        "from `{}`:\n{}",
        command,
        delta.observation()
    )))
}

/// Outstanding Call failures for the Attempt, as compact observations.
///
/// Each line is the observation `call_recovery` already classified and bounded
/// when the Call failed; the raw input and output stay in the Call record. An
/// observation already present in this conversation is skipped, so a fact the
/// model has just been told is not told again.
fn call_section(
    project_root: &Path,
    attempt_id: &str,
    conversation: &[Value],
) -> Result<Option<String>> {
    if attempt_id.is_empty() {
        return Ok(None);
    }
    let seen: String = conversation
        .iter()
        .filter_map(|message| message.get("content"))
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    let repository = crate::orchestration::domain::DomainRepository::open(project_root)?;
    let mut entries: Vec<String> = Vec::new();
    for call in repository.calls_for_attempt(attempt_id)? {
        if !matches!(call.state.as_str(), "failed" | "fenced") {
            continue;
        }
        let Some(observation) = call.response.as_deref() else {
            continue;
        };
        let observation = observation.trim();
        if observation.is_empty() || seen.contains(observation) {
            continue;
        }
        entries.push(format!("- `{}` [{}] {observation}", call.id, call.state));
    }
    if entries.is_empty() {
        return Ok(None);
    }
    if entries.len() > MAX_CALL_FAILURES {
        let dropped = entries.len() - MAX_CALL_FAILURES;
        entries.drain(..dropped);
        entries.push(format!("- ({dropped} older failure(s) omitted)"));
    }
    let body = entries.join("\n");
    if body.len() > MAX_CALL_SECTION_BYTES {
        let mut kept: Vec<&str> = Vec::new();
        let mut used = 0usize;
        for line in body.lines() {
            if used + line.len() > MAX_CALL_SECTION_BYTES {
                break;
            }
            used += line.len();
            kept.push(line);
        }
        return Ok(Some(format!(
            "{}\n- (truncated; the full Call records remain in the journal)",
            kept.join("\n")
        )));
    }
    Ok(Some(body))
}

/// Compact the conversation, leaving the projection untouched.
///
/// This is the boundary the two layers are separated by: only messages reach
/// compaction. Repository context, compiler feedback and Call observations are
/// re-rendered from canonical state on every turn, so summarizing them would
/// only create a lossy second copy of a fact that is already exact somewhere
/// else.
///
/// Returns the compaction point when one was accepted, so the caller can persist
/// it. A point is only reported when the projection it produces actually fits:
/// recording a summary that did not resolve the overflow would leave a durable
/// claim that the history is covered when it is still being sent.
async fn compact_conversation<'f>(
    project_root: &Path,
    conversation: &[Value],
    tools: Option<&Value>,
    config: &Result<Value>,
    state: &crate::orchestration::state::OrchestrationState,
    attempt_id: &str,
    summarize: Option<Summarize<'f>>,
) -> (Vec<Value>, Option<CompactionPoint>) {
    let history = History::new(conversation.to_vec());
    let canonical = canonical_block(project_root);
    let compaction_config = match config {
        Ok(data) => match CompactionConfig::from_config(data) {
            Ok(config) => config,
            Err(error) => {
                tracing::warn!(%error, "compaction configuration is invalid; sending the conversation as assembled");
                return (conversation.to_vec(), None);
            }
        },
        Err(_) => CompactionConfig::default(),
    };
    // No provider has declared a window to this path, so compaction is not
    // trusted to judge the budget and declines to guess. Deterministic reduction
    // still runs inside `evaluate`, which is what bounds the payloads.
    let limits = ModelLimits::default();
    // Durable points for this Attempt, restored in order. `active_point` picks
    // the newest that still applies, so a point recorded against a longer
    // transcript than the one in hand is skipped rather than trusted.
    let points = state.compaction_points(attempt_id).to_vec();
    let context = Context::new(
        &history,
        &points,
        &canonical,
        &compaction_config,
        &limits,
    );
    match context.evaluate(None, tools, None) {
        Ok(CompactionOutcome::Ready { messages, footprint, .. }) => {
            tracing::debug!(
                prompt_tokens = footprint.effective_prompt_tokens(),
                tool_result_share_bp = footprint.tool_result_share_bp(),
                restored_points = points.len(),
                active_point = context.active_point().map(|point| point.id.as_str()),
                "provider conversation projected"
            );
            (messages, None)
        }
        Ok(CompactionOutcome::Compact {
            reason,
            transcript,
            request,
            prompt,
            max_summary_tokens,
            ..
        }) => match summarize {
            Some(summarize) => {
                match accept_summary(&context, summarize, &request, &prompt, max_summary_tokens)
                    .await
                {
                    Ok(accepted) => {
                        // The point only stands in for history if it actually
                        // elides some. A point that covers nothing would be a
                        // durable claim that a conversation was summarized when
                        // it was not.
                        if accepted.tail_start == 0
                            || history.after(accepted.tail_start).len() >= history.len()
                        {
                            tracing::warn!(
                                point = %accepted.id,
                                "compaction point covers no history; keeping the conversation whole"
                            );
                            return keep_whole(transcript);
                        }
                        // Re-project against the accepted point so this request
                        // carries the summary instead of the history it replaces.
                        let compacted = points
                            .iter()
                            .cloned()
                            .chain(std::iter::once(accepted.clone()))
                            .collect::<Vec<_>>();
                        let next = Context::new(
                            &history,
                            &compacted,
                            &canonical,
                            &compaction_config,
                            &limits,
                        );
                        match next.evaluate(None, tools, None) {
                            Ok(CompactionOutcome::Ready { messages, .. }) => {
                                tracing::info!(
                                    point = %accepted.id,
                                    through = accepted.through,
                                    tail_start = accepted.tail_start,
                                    messages_before = history.len(),
                                    messages_after = messages.len(),
                                    %reason,
                                    summary_bytes = accepted.summary.len(),
                                    "conversation compacted"
                                );
                                (messages, Some(accepted))
                            }
                            other => {
                                // The summary is genuine but the projection still
                                // does not fit, so this request cannot use it
                                // yet. Recording it now would claim coverage the
                                // next request is not yet getting.
                                tracing::warn!(
                                    point = %accepted.id,
                                    %reason,
                                    outcome = ?outcome_kind(&other),
                                    "compaction did not produce a usable projection; \
                                     keeping the conversation whole"
                                );
                                keep_whole(transcript)
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, %reason, "compaction summary was rejected; keeping the conversation whole");
                        keep_whole(transcript)
                    }
                }
            }
            None => {
                // No model is reachable to write the summary, so nothing may
                // claim the history is covered. The history is kept whole rather
                // than silently dropping everything before the tail.
                tracing::warn!(
                    %reason,
                    "semantic compaction is required but no summarizing transport is available; \
                     sending the uncompacted conversation"
                );
                keep_whole(transcript)
            }
        },
        Err(error) => {
            tracing::warn!(%error, "compaction projection failed; sending the conversation as assembled");
            (conversation.to_vec(), None)
        }
    }
}

/// What a re-projection reported, for the trace when it could not be used.
fn outcome_kind(outcome: &Result<CompactionOutcome>) -> &'static str {
    match outcome {
        Ok(CompactionOutcome::Ready { .. }) => "ready",
        Ok(CompactionOutcome::Compact { .. }) => "still_compacting",
        Err(_) => "projection_failed",
    }
}

/// Run the summarizing call and validate what came back.
///
/// The summary is only accepted once it carries every canonical reference the
/// compaction module requires, so a lossy summary cannot be allowed to stand in
/// for state the model must trust. The returned point is the pending request
/// with its summary filled in.
async fn accept_summary<'f>(
    context: &Context<'_>,
    summarize: Summarize<'f>,
    request: &CompactionPoint,
    prompt: &str,
    max_summary_tokens: u64,
) -> Result<CompactionPoint> {
    let candidate = summarize(prompt.to_owned(), max_summary_tokens).await?;
    context.accept(request, &candidate)
}

/// Keep the whole conversation when no summary stands in for it.
fn keep_whole(transcript: Transcript) -> (Vec<Value>, Option<CompactionPoint>) {
    (
        transcript
            .head
            .into_iter()
            .chain(transcript.tail)
            .collect(),
        None,
    )
}

/// The canonical state block compaction re-renders.
///
/// A snapshot that cannot be read degrades to an empty block: compaction then has
/// no facts to restate, which loses nothing it was relying on.
fn canonical_block(project_root: &Path) -> CanonicalBlock {
    let root = project_root.to_string_lossy().into_owned();
    match crate::orchestration::domain::DomainRepository::open(project_root)
        .and_then(|repository| repository.execution_snapshot())
    {
        Ok(snapshot) => CanonicalBlock::from_snapshot(&snapshot, Some(&root)),
        Err(error) => {
            tracing::debug!(%error, "canonical state block is unavailable for compaction");
            CanonicalBlock::default()
        }
    }
}

/// The merged configuration, read the same way the CLI reads it.
fn config_data(project_root: &Path) -> Result<Value> {
    let env = crate::cli::Env::from_process();
    let (source, home) = match env.home.clone() {
        Some(path) => (
            crate::defaults::OcgSource::Dir(path.clone()),
            Some(path),
        ),
        None => (crate::defaults::OcgSource::Embedded, None),
    };
    let user_path = crate::config::user_config_path(
        None,
        env.user_config.as_deref(),
        env.xdg_config_home.as_deref(),
        env.home_dir.as_deref(),
    );
    let effective = crate::config::build_effective(
        crate::defaults::load_defaults(&source)?,
        home,
        project_root,
        &user_path,
        &user_path,
        env.home_dir,
    )?;
    Ok(effective.data)
}

/// Truncate at a byte ceiling on a character boundary, stating what was cut.
fn truncate(text: &str, cap: usize) -> (String, bool) {
    if text.len() <= cap {
        return (text.to_string(), false);
    }
    let mut end = cap;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    (
        format!("{}\n(truncated at {cap} bytes)", &text[..end]),
        true,
    )
}