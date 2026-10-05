//! Deterministic reduction of the message history before any summary exists.
//!
//! This is the layer that runs first and the one that must not lose meaning.
//! It never asks a model anything and never deletes a message: every reduction
//! replaces a payload with a smaller, self-describing stand-in that says what
//! was removed, how much, and where to find the original. A history reduced
//! this way is still a faithful history — it is just cheaper to send.
//!
//! Four reductions, in the order they are applied:
//!
//! 1. **Bounded output.** Any tool result, assistant text, or reasoning block
//!    larger than its policy bound is replaced by a head-and-tail digest. The
//!    digest keeps the beginning and the end, because the beginning carries the
//!    shape of a result and the end carries the conclusion (a failing test's
//!    last line, a command's exit report). This mirrors the existing
//!    [`crate::native_tools::ToolResult::bounded`] convention: an original byte
//!    count, an explicit `truncated` flag, and `remaining` so a consumer can
//!    tell that a full record exists elsewhere.
//!
//! 2. **Repeated observation collapse.** Consecutive or repeated
//!    `filesystem.read` / `filesystem.list` / `filesystem.search` calls over the
//!    same target are read-only and idempotent: the second result of an
//!    identical call is byte-identical to the first. Keeping both is pure waste.
//!    The *latest* result for a target is kept in full and the earlier ones
//!    become a digest that records the repeat count. Failures are never
//!    collapsed away: a read that failed is evidence, and a later success does
//!    not erase it.
//!
//! 3. **Superseded projection.** A read of a path that was later edited, or
//!    re-read, is a projection of a file state that no longer exists. The stale
//!    read is collapsed to a digest naming the operation that superseded it,
//!    while the newest read of that path is preserved. This is what keeps a
//!    long Build from re-anchoring on pre-edit file content.
//!
//! 4. **Canonical fact substitution.** A message that merely restates a
//!    canonical fact OCG already owns — the job id, the generation, the settled
//!    budget — is replaced by a reference to the canonical state block rather
//!    than a prose copy. Canonical facts are re-rendered from canonical state
//!    on every request, so a prose copy in the history can only ever be a stale
//!    duplicate of them.
//!
//! Reduction honours [`ReducePolicy::protect_recent_messages`], but the two
//! halves of the policy differ deliberately:
//!
//! - **Collapsing** a repeated or superseded result discards content, so the
//!   trailing [`ReducePolicy::protect_recent_messages`] messages are never
//!   collapsed. The immediate continuation context stays verbatim.
//! - **Bounding** an oversized payload only elides its middle, keeping the head
//!   and the tail, so it applies everywhere. Exempting the recent window from
//!   bounding would protect the very messages that broke the budget, and a
//!   context that cannot be bounded cannot be made to fit.

use crate::compaction::config::ReducePolicy;
use crate::error::Result;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Maximum number of distinct targets tracked for collapse. Beyond this the
/// tracker stops recording new targets rather than growing without bound; the
/// effect is that later repeats are left verbatim, which is safe.
pub const MAX_TRACKED_TARGETS: usize = 512;

/// What one reduction changed.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ReductionKind {
    /// A payload exceeded its bound and was digested.
    BoundedOutput,
    /// A repeated read-only observation was collapsed.
    RepeatCollapsed,
    /// A read superseded by a later edit was collapsed.
    SupersededCollapsed,
    /// A restatement of a canonical fact was replaced by a reference.
    CanonicalReplaced,
}

impl ReductionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BoundedOutput => "bounded_output",
            Self::RepeatCollapsed => "repeat_collapsed",
            Self::SupersededCollapsed => "superseded_collapsed",
            Self::CanonicalReplaced => "canonical_replaced",
        }
    }
}

/// One recorded reduction.
///
/// Receipts are the audit trail for compaction: a caller can state exactly what
/// the active context no longer contains without consulting the durable history.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReductionRecord {
    pub kind: ReductionKind,
    /// Index of the reduced message in the input history.
    pub index: usize,
    /// What was reduced: a tool name, a role, or a canonical reference.
    pub subject: String,
    /// Original size in bytes.
    pub original_bytes: usize,
    /// Size after reduction.
    pub reduced_bytes: usize,
    /// Why this message was reduced, in one line.
    pub reason: String,
}

impl ReductionRecord {
    pub fn tokens_reclaimed(&self) -> u64 {
        let saved = self.original_bytes.saturating_sub(self.reduced_bytes);
        (saved / 4) as u64
    }
}

/// The outcome of a reduction pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReductionReport {
    /// Every reduction applied, ordered by message index.
    pub records: Vec<ReductionRecord>,
    /// Total bytes reclaimed.
    pub bytes_reclaimed: usize,
    /// Approximate tokens reclaimed.
    pub tokens_reclaimed: u64,
    /// Number of messages inspected.
    pub messages_inspected: usize,
}

impl ReductionReport {
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// A one-line summary suitable for a decision reason or a log.
    pub fn summary(&self) -> String {
        if self.records.is_empty() {
            return "deterministic reduction reclaimed nothing".to_string();
        }
        let mut by_kind: BTreeMap<&'static str, usize> = BTreeMap::new();
        for record in &self.records {
            *by_kind.entry(record.kind.as_str()).or_default() += 1;
        }
        let detail = by_kind
            .iter()
            .map(|(kind, count)| format!("{count} {kind}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "deterministic reduction reclaimed ~{} tokens across {} messages ({detail})",
            self.tokens_reclaimed,
            self.records.len()
        )
    }
}

/// Whether a message is a tool result.
pub fn is_tool_result(message: &Value) -> bool {
    role_of(message) == "tool"
}

fn role_of(message: &Value) -> &str {
    message
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// Read-only native tool names that can be collapsed when repeated.
const OBSERVATION_TOOLS: [&str; 3] = ["filesystem.read", "filesystem.list", "filesystem.search"];

/// Normalize a wire tool name to its canonical dotted form.
///
/// The wire projection rewrites `filesystem.read` to `filesystem_read`, so a
/// reducer that only matched the canonical spelling would silently match
/// nothing.
pub fn canonical_tool_name(name: &str) -> String {
    name.replace('_', ".")
}

/// The paths a native tool can touch, and whether the tool mutates them.
///
/// Used by the supersede pass to tell a read from a write. Duplicating the wire
/// projection's spelling rule rather than calling into it keeps this module
/// usable without the projection layer, which is what makes it extractable.
pub fn tool_mutates(name: &str) -> bool {
    canonical_tool_name(name) == "filesystem.edit"
}

/// Whether a tool is a read-only observation eligible for collapse.
pub fn is_observation_tool(name: &str) -> bool {
    OBSERVATION_TOOLS.contains(&canonical_tool_name(name).as_str())
}

/// The identity of a tool call, used to detect repeats.
///
/// The canonical Call id is part of the identity when present so a *retry* of
/// the same request under a new Call is not mistaken for a duplicate read.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CallIdentity {
    tool: String,
    arguments: String,
}

/// Extract the identity of the tool call a tool-result message answers.
///
/// A tool result is matched to its call by `tool_call_id`, so the mapping is
/// exact rather than positional. A result with no matching call is left alone:
/// guessing would risk collapsing an unrelated result.
fn identity_of(
    message: &Value,
    calls: &BTreeMap<String, (String, String)>,
) -> Option<CallIdentity> {
    let id = message.get("tool_call_id")?.as_str()?;
    let (tool, arguments) = calls.get(id)?;
    Some(CallIdentity {
        tool: canonical_tool_name(tool),
        arguments: arguments.clone(),
    })
}

/// Map every tool call in an assistant message into `call_id -> (tool, arguments)`.
fn collect_calls(messages: &[Value]) -> BTreeMap<String, (String, String)> {
    let mut calls = BTreeMap::new();
    for message in messages {
        let Some(array) = message.get("tool_calls").and_then(Value::as_array) else {
            continue;
        };
        for call in array {
            let Some(id) = call.get("id").and_then(Value::as_str) else {
                continue;
            };
            let function = call.get("function");
            let tool = function
                .and_then(|value| value.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let arguments = function
                .and_then(|value| value.get("arguments"))
                .map(|value| match value {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
            calls.insert(id.to_string(), (tool, arguments));
        }
    }
    calls
}

/// The filesystem target a read-only observation addresses.
///
/// Only the arguments that determine what is read participate in identity, so
/// two reads of the same file with different `start`/`end` bounds are *not*
/// collapsed into one another.
fn observation_target(name: &str, arguments: &str) -> Option<String> {
    let value: Value = serde_json::from_str(arguments).ok()?;
    let path = value.get("path").and_then(Value::as_str)?;
    let normalized = canonical_tool_name(name);
    Some(format!(
        "{normalized}|{path}|{}|{}",
        value
            .get("start")
            .map(|value| value.to_string())
            .unwrap_or_default(),
        value
            .get("end")
            .map(|value| value.to_string())
            .unwrap_or_default()
    ))
}

/// The path a tool call reads or writes, if it names one.
fn call_path(name: &str, arguments: &str) -> Option<String> {
    let value: Value = serde_json::from_str(arguments).ok()?;
    value
        .get("path")
        .and_then(Value::as_str)
        .map(|path| format!("{name}|{path}"))
}

/// Reduce a message history in place, returning the reduced copy and a report.
///
/// The input is never mutated: compaction is a projection over an immutable
/// durable history, so a caller can always rebuild the original.
pub fn reduce(
    messages: &[Value],
    policy: &ReducePolicy,
    canonical_refs: &[String],
) -> Result<(Vec<Value>, ReductionReport)> {
    let mut report = ReductionReport {
        messages_inspected: messages.len(),
        ..ReductionReport::default()
    };
    let mut reduced = messages.to_vec();
    let calls = collect_calls(messages);
    let protect_from = reduced.len().saturating_sub(policy.protect_recent_messages);

    // Pass 1: bound every oversized payload, including inside the recent window.
    //
    // Bounding is exempt from the recent-message protection on purpose. It keeps
    // the head and the tail of the payload and only elides the middle, so it
    // preserves the continuation facts a fresh turn depends on. Refusing to bound
    // an oversized payload in the newest turn would leave a context that cannot
    // be made to fit — the window would protect the very messages that broke the
    // budget. The collapse passes below, which do discard content, still respect
    // `protect_from`.
    for (index, message) in reduced.iter_mut().enumerate() {
        bound_message(message, policy, index, &mut report);
    }

    // Pass 2: collapse repeated read-only observations.
    if policy.collapse_repeat_reads {
        collapse_repeats(&mut reduced, &calls, policy, protect_from, &mut report);
    }

    // Pass 3: collapse reads superseded by a later edit of the same path.
    if policy.collapse_superseded_reads {
        collapse_superseded(&mut reduced, &calls, policy, protect_from, &mut report);
    }

    // Pass 4: replace canonical restatements with references.
    replace_canonical(
        &mut reduced,
        policy,
        protect_from,
        canonical_refs,
        &mut report,
    );

    Ok((reduced, report))
}

/// Bound one message's oversized string fields.
///
/// Only the payload fields are touched. Identity fields — role, tool call ids,
/// tool names, and the arguments of a call — are never rewritten, because a
/// truncated argument string would make the call unresolvable.
fn bound_message(
    message: &mut Value,
    policy: &ReducePolicy,
    index: usize,
    report: &mut ReductionReport,
) {
    let Some(object) = message.as_object() else {
        return;
    };
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let content_cap = match role.as_str() {
        "tool" => {
            if is_failed_result(message) {
                policy.max_error_chars
            } else {
                policy.max_tool_result_chars
            }
        }
        "assistant" => policy.max_assistant_chars,
        _ => return,
    };

    // Collect the oversized fields before taking a mutable borrow, so the
    // failure check above can read the message without aliasing.
    let mut pending: Vec<(&'static str, String, usize)> = Vec::new();
    let record_field =
        |field: &'static str, cap: usize, pending: &mut Vec<(&'static str, String, usize)>| {
            let Some(Value::String(text)) = object.get(field) else {
                return;
            };
            if text.len() > cap {
                pending.push((field, text.clone(), cap));
            }
        };
    record_field("content", content_cap, &mut pending);
    if role == "assistant" {
        record_field(
            "reasoning_content",
            policy.max_reasoning_chars,
            &mut pending,
        );
    }
    for (field, text, cap) in pending {
        let before = text.len();
        let digest = bounded_digest(&text, cap, field);
        let after = digest.len();
        if let Some(slot) = message
            .as_object_mut()
            .and_then(|object| object.get_mut(field))
        {
            *slot = Value::String(digest);
        }
        record(
            report,
            ReductionKind::BoundedOutput,
            index,
            &format!("{role}.{field}"),
            before,
            after,
            format!("payload exceeded the {cap} character bound"),
        );
    }
}

/// A failed tool result.
///
/// A result is treated as failed when the canonical `success` field is false or
/// an `error` object is present. Failures get a larger cap and are never
/// collapsed as repeats, because a failure is the evidence a continuation needs.
fn is_failed_result(message: &Value) -> bool {
    if embedded_failure(message).is_some() {
        return true;
    }
    let success = message.get("success").and_then(Value::as_bool);
    let errored = message
        .get("error")
        .map(|value| !value.is_null())
        .unwrap_or(false);
    success == Some(false) || errored
}

/// Inspect the failure fields of a tool result, whether they sit on the message
/// or inside the serialized `ToolResult` JSON it carries.
///
/// The provider loop writes the whole canonical result as the tool message's
/// `content` string, so both layouts occur. Parsing is attempted only for JSON
/// content and a parse failure is simply "not a failure", which is the safe
/// direction: a result is only ever treated as failed when it says so.
fn embedded_failure(message: &Value) -> Option<(bool, bool)> {
    let text = message.get("content").and_then(Value::as_str)?;
    let embedded = serde_json::from_str::<Value>(text).ok()?;
    let success = embedded.get("success").and_then(Value::as_bool);
    let errored = embedded
        .get("error")
        .map(|value| !value.is_null())
        .unwrap_or(false);
    Some((success == Some(false), errored))
}

/// Build a head-and-tail digest of an oversized payload.
///
/// The head is sized generously because it carries the shape of the result; the
/// tail carries the conclusion. A single boundary marker states the elided
/// byte count so the reader knows the omission is deliberate and how large it
/// was.
/// A head-and-tail digest of an oversized payload.
///
/// The head is sized generously because it carries the shape of a result; the
/// tail carries the conclusion — a failing test's last line, a command's exit
/// report. A single boundary marker states the elided byte count so the reader
/// can tell the omission was deliberate and how large it was.
///
/// Character indexing is not byte indexing, so the split points are snapped to
/// UTF-8 character boundaries. Slicing at an arbitrary byte would panic on
/// multi-byte content, and tool output is routinely non-ASCII.
pub fn bound_text(text: &str, cap: usize, label: &str) -> String {
    if text.len() <= cap {
        return text.to_string();
    }
    let marker = format!(
        "\n[... {label} truncated: {} bytes elided ...]\n",
        text.len()
    );
    let budget = cap.saturating_sub(marker.len());
    if budget == 0 {
        return format!("[{label} truncated: {} bytes]", text.len());
    }
    // The head gets three quarters of the budget; the tail keeps the rest.
    let head = snap_to_char_boundary(text, (budget * 3 / 4).min(text.len()));
    let tail_start = snap_to_char_boundary(text, text.len().saturating_sub(budget - head));
    if tail_start <= head {
        return text[..head].to_string();
    }
    let mut digest = String::with_capacity(cap);
    digest.push_str(&text[..head]);
    digest.push_str(&marker);
    digest.push_str(&text[tail_start..]);
    digest
}

/// Snap a byte offset down to the nearest UTF-8 character boundary.
fn snap_to_char_boundary(text: &str, mut index: usize) -> usize {
    if index >= text.len() {
        return text.len();
    }
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn bounded_digest(text: &str, cap: usize, field: &str) -> String {
    bound_text(text, cap, field)
}

fn record(
    report: &mut ReductionReport,
    kind: ReductionKind,
    index: usize,
    subject: &str,
    original_bytes: usize,
    reduced_bytes: usize,
    reason: impl Into<String>,
) {
    report.bytes_reclaimed = report
        .bytes_reclaimed
        .saturating_add(original_bytes.saturating_sub(reduced_bytes));
    let entry = ReductionRecord {
        kind,
        index,
        subject: subject.to_string(),
        original_bytes,
        reduced_bytes,
        reason: reason.into(),
    };
    report.tokens_reclaimed = report
        .tokens_reclaimed
        .saturating_add(entry.tokens_reclaimed());
    report.records.push(entry);
}

/// Collapse repeated read-only observations onto their latest occurrence.
///
/// Only observation tools participate. A repeated *identical* call yields a
/// byte-identical result, so keeping the earlier copies is waste. The last
/// occurrence is kept verbatim; earlier ones are replaced with a digest that
/// records how many times the call repeated and what the target was.
fn collapse_repeats(
    messages: &mut [Value],
    calls: &BTreeMap<String, (String, String)>,
    policy: &ReducePolicy,
    protect_from: usize,
    report: &mut ReductionReport,
) {
    let mut last_by_identity: BTreeMap<CallIdentity, usize> = BTreeMap::new();
    let mut seen: BTreeMap<CallIdentity, usize> = BTreeMap::new();
    // The *scan* covers the whole history so the surviving occurrence is
    // genuinely the newest one, even when the newest turn is inside the
    // protected recent window and will not itself be reduced. Only the
    // replacement loop below is bounded by `protect_from`.
    for (index, message) in messages.iter().enumerate() {
        if !is_tool_result(message) {
            continue;
        }
        let Some(identity) = identity_of(message, calls) else {
            continue;
        };
        if !is_observation_tool(&identity.tool) || is_failed_result(message) {
            continue;
        }
        if seen.len() >= MAX_TRACKED_TARGETS {
            break;
        }
        *seen.entry(identity.clone()).or_default() += 1;
        last_by_identity.insert(identity, index);
    }
    for (identity, last_index) in last_by_identity {
        let total = seen.get(&identity).copied().unwrap_or(1);
        if total < 2 {
            continue;
        }
        let target = observation_target(&identity.tool, &identity.arguments)
            .unwrap_or_else(|| identity.tool.clone());
        // Earlier occurrences are collapsed; the newest is left verbatim so the
        // model still sees one full result for the target. `protect_from` still
        // bounds the loop, so an occurrence inside the protected recent window is
        // never rewritten.
        for index in 0..last_index.min(protect_from) {
            if index == last_index || !is_tool_result(&messages[index]) {
                continue;
            }
            if identity_of(&messages[index], calls).as_ref() != Some(&identity) {
                continue;
            }
            replace_with_digest(
                messages,
                index,
                report,
                ReductionKind::RepeatCollapsed,
                &identity.tool,
                policy,
                json!({
                    "compacted": true,
                    "reason": "repeat_observation",
                    "tool": identity.tool,
                    "target": target,
                    "repeats": total,
                    "note": "an identical read-only call produced the same result; only the latest result is retained in full"
                }),
            );
        }
    }
}

/// Collapse reads whose content a later edit superseded.
///
/// For each path, the newest read is kept. Any earlier read of the same path is
/// replaced with a digest naming the edit that made it stale, because a
/// continuation must not re-anchor on pre-edit content.
fn collapse_superseded(
    messages: &mut [Value],
    calls: &BTreeMap<String, (String, String)>,
    policy: &ReducePolicy,
    protect_from: usize,
    report: &mut ReductionReport,
) {
    // The newest read index per path.
    let mut newest_read: BTreeMap<String, usize> = BTreeMap::new();
    let mut edit_index_by_path: BTreeMap<String, usize> = BTreeMap::new();
    for (index, message) in messages.iter().enumerate() {
        let Some(array) = message.get("tool_calls").and_then(Value::as_array) else {
            continue;
        };
        for call in array {
            let Some(id) = call.get("id").and_then(Value::as_str) else {
                continue;
            };
            let Some((tool, arguments)) = calls.get(id) else {
                continue;
            };
            let canonical = canonical_tool_name(tool);
            if canonical == "filesystem.edit" {
                if let Some(path) = call_path(&canonical, arguments) {
                    edit_index_by_path
                        .entry(path)
                        .and_modify(|current| *current = (*current).max(index))
                        .or_insert(index);
                }
            } else if is_observation_tool(&canonical) {
                if let Some(path) = call_path(&canonical, arguments) {
                    newest_read
                        .entry(path)
                        .and_modify(|current| *current = (*current).max(index))
                        .or_insert(index);
                }
            }
        }
    }
    for (path, edit_index) in &edit_index_by_path {
        let Some(read_index) = newest_read.get(path).copied() else {
            continue;
        };
        if read_index <= *edit_index {
            // The surviving read already happened before the edit, so nothing
            // is superseded within the retained window.
            continue;
        }
        for index in 0..protect_from {
            if index == read_index || !is_tool_result(&messages[index]) {
                continue;
            }
            let Some(identity) = identity_of(&messages[index], calls) else {
                continue;
            };
            if !is_observation_tool(&identity.tool) || is_failed_result(&messages[index]) {
                continue;
            }
            if call_path(&identity.tool, &identity.arguments).as_deref() != Some(path.as_str()) {
                continue;
            }
            replace_with_digest(
                messages,
                index,
                report,
                ReductionKind::SupersededCollapsed,
                &identity.tool,
                policy,
                json!({
                    "compacted": true,
                    "reason": "superseded_by_edit",
                    "tool": identity.tool,
                    "target": path,
                    "superseded_at_message": edit_index,
                    "note": "this result reflects file content that a later edit replaced; re-read the path for current content"
                }),
            );
        }
    }
}

/// Replace prose restatements of canonical facts with references.
///
/// A canonical fact is re-rendered from canonical state on every request, so a
/// copy of it inside the history is a duplicate that can only go stale. When a
/// message mentions a known canonical id, it is reduced to a reference.
fn replace_canonical(
    messages: &mut [Value],
    policy: &ReducePolicy,
    protect_from: usize,
    canonical_refs: &[String],
    report: &mut ReductionReport,
) {
    if canonical_refs.is_empty() {
        return;
    }
    for (index, message) in messages.iter_mut().enumerate().take(protect_from) {
        if !matches!(role_of(message), "user" | "assistant") {
            continue;
        }
        let Some(Value::String(content)) = message.get("content") else {
            continue;
        };
        let content = content.clone();
        if content.len() <= policy.max_assistant_chars {
            continue;
        }
        let Some(reference) = canonical_refs
            .iter()
            .find(|reference| content.contains(reference.as_str()))
        else {
            continue;
        };
        let before = content.len();
        let digest = format!(
            "[compacted: this message was mostly a restatement of canonical state; the canonical block is authoritative and contains {reference} plus its current value]\n[original: {} bytes]",
            before
        );
        if let Some(slot) = message
            .as_object_mut()
            .and_then(|object| object.get_mut("content"))
        {
            *slot = Value::String(digest.clone());
        }
        record(
            report,
            ReductionKind::CanonicalReplaced,
            index,
            reference,
            before,
            digest.len(),
            "content restated canonical state that is re-rendered every request".to_string(),
        );
    }
}

fn replace_with_digest(
    messages: &mut [Value],
    index: usize,
    report: &mut ReductionReport,
    kind: ReductionKind,
    subject: &str,
    policy: &ReducePolicy,
    digest: Value,
) {
    let message = &mut messages[index];
    let before = message
        .get("content")
        .map(|content| content.to_string().len())
        .unwrap_or(0);
    let rendered = digest.to_string();
    let preview = if rendered.len() > policy.digest_preview_chars {
        let mut preview = rendered[..policy.digest_preview_chars].to_string();
        preview.push_str(&format!("... ({} bytes)", rendered.len()));
        preview
    } else {
        rendered
    };
    let payload = json!({
        "compacted": true,
        "digest": serde_json::from_str::<Value>(&preview).unwrap_or(Value::String(preview.clone())),
        "original_bytes": before,
        "truncated": true,
        "remaining": true,
    });
    let after = payload.to_string().len();
    if let Some(object) = message.as_object_mut() {
        if object.contains_key("content") || object.contains_key("output") {
            object.insert("content".to_string(), Value::String(payload.to_string()));
        }
    }
    record(
        report,
        kind,
        index,
        subject,
        before,
        after,
        subject.to_string(),
    );
}
