//! Rust compiler feedback as a typed, incremental observation.
//!
//! The waste this module removes: a coding agent that runs `cargo check` gets
//! the full rendered compiler output every time, so a fix that resolves 34 of 37
//! diagnostics still pays for all 37 on the next turn. Nothing improves because
//! the agent re-reads text it already read.
//!
//! `cargo --message-format=json` emits one JSON object per diagnostic with the
//! facts a caller actually needs: level, code, file, span, message. Those facts
//! are extracted here, deduplicated by identity, and compared against the
//! previous compile so only the delta crosses into context.
//!
//! Two properties are enforced by construction:
//!
//! - A diagnostic is identified by `(level, code, file, span)`, never by its
//!   rendered text. Reformatting a diagnostic does not make it "new".
//! - The observation is a count plus code lists. Rendered compiler prose is
//!   never re-sent, and full output stays in the raw log the caller already
//!   stores.
//!
//! Integration point: the verification runner already captures each command's
//! stdout/stderr and writes it to `.ocg/logs/`. Feeding those bytes through
//! [`from_json_lines`] and then [`delta`] yields
//! the observation, while the raw log reference remains the place a human goes
//! for the full text. No new execution authority is introduced.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Largest rendered message retained per diagnostic.
///
/// Rust messages are one sentence in practice. Anything past this is a template
/// expansion the model does not act on.
pub const MESSAGE_CAP: usize = 400;

/// Largest number of codes one observation lists per category.
const MAX_LISTED_CODES: usize = 6;

/// The severity rustc assigns to a diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Error,
    Warning,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warning => "warning",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "error" => Some(Self::Error),
            "warning" => Some(Self::Warning),
            _ => None,
        }
    }
}

/// A half-open source range rustc attributed a diagnostic to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Span {
    pub start_line: u32,
    pub start_column: u32,
    pub end_line: u32,
    pub end_column: u32,
}

/// One compiler diagnostic, reduced to the facts worth carrying forward.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub level: Level,
    /// The stable rustc code, e.g. `E0382`. Absent for lints without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Project-relative path when rustc attributed a file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<Span>,
    pub message: String,
}

impl Diagnostic {
    /// The identity a delta is computed on: level, code, location, span.
    ///
    /// Text is deliberately excluded. Two compiles can render the same
    /// diagnostic differently and it is still the same outstanding problem.
    pub fn identity(&self) -> String {
        let span = self.span.map_or_else(
            || "-".to_string(),
            |span| {
                format!(
                    "{}:{}-{}:{}",
                    span.start_line, span.start_column, span.end_line, span.end_column
                )
            },
        );
        format!(
            "{}|{}|{}|{}|{}",
            self.level.as_str(),
            self.code.as_deref().unwrap_or("-"),
            self.file.as_deref().unwrap_or("-"),
            span,
            self.message
        )
    }

    /// How a diagnostic is shown in a count-plus-codes observation.
    pub fn label(&self) -> String {
        match (&self.code, &self.file) {
            (Some(code), Some(file)) => match self.span {
                Some(span) => format!("{code} at {}:{}", file, span.start_line),
                None => format!("{code} in {file}"),
            },
            (Some(code), None) => code.clone(),
            (None, Some(file)) => match self.span {
                Some(span) => format!("{}:{} ({})", file, span.start_line, self.level.as_str()),
                None => format!("{file} ({})", self.level.as_str()),
            },
            (None, None) => format!("{} ({})", self.level.as_str(), first_line(&self.message)),
        }
    }
}

/// What changed between two consecutive compiles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticDelta {
    /// Diagnostics this compile reported.
    pub current: Vec<Diagnostic>,
    /// Outstanding diagnostics the previous compile did not report.
    pub new: Vec<Diagnostic>,
    /// Outstanding diagnostics that persist across both compiles.
    pub remaining: Vec<Diagnostic>,
    /// Diagnostics the previous compile reported and this one does not.
    pub resolved: Vec<Diagnostic>,
    /// True when the previous compile produced no machine-readable output, so
    /// "new" cannot be trusted as genuinely new.
    ///
    /// This is reported rather than assumed: a missing baseline means every
    /// current diagnostic is unknown-origin, and claiming otherwise would let the
    /// agent believe it had fixed something it had never seen before.
    pub baseline_known: bool,
}

impl DiagnosticDelta {
    /// True when this compile resolved at least one diagnostic and introduced
    /// none. The state worth reporting: progress that is not a regression.
    pub fn is_net_progress(&self) -> bool {
        !self.resolved.is_empty() && self.new.is_empty()
    }

    /// The bounded observation handed to the agent.
    pub fn observation(&self) -> String {
        let mut lines = vec![format!(
            "compiler: {} error(s), {} warning(s) outstanding",
            count_level(&self.current, Level::Error),
            count_level(&self.current, Level::Warning),
        )];
        if self.baseline_known {
            lines.push(format!(
                "delta: {} resolved, {} new, {} carried over",
                self.resolved.len(),
                self.new.len(),
                self.remaining.len()
            ));
            for (label, items) in [
                ("resolved", &self.resolved),
                ("new", &self.new),
                ("remaining", &self.remaining),
            ] {
                if items.is_empty() {
                    continue;
                }
                lines.push(format!(
                    "{label}: {}{}",
                    codes(items),
                    if items.len() > MAX_LISTED_CODES {
                        format!(" (+{} more)", items.len() - MAX_LISTED_CODES)
                    } else {
                        String::new()
                    }
                ));
            }
        } else {
            lines.push(format!(
                "no previous machine-readable compile to compare against; {} diagnostic(s) are \
                 first observed",
                self.current.len()
            ));
            lines.push(format!("outstanding: {}", codes(&self.current)));
        }
        lines.join("\n").trim().to_string()
    }

    /// Distinct outstanding codes, error first, for a one-glance read.
    pub fn outstanding_codes(&self) -> Vec<String> {
        distinct_codes(&self.current)
    }
}

/// One compile's machine-readable diagnostics.
///
/// A compile that produced no machine-readable output is a legitimate state
/// represented by an empty set, not an error.
pub type DiagnosticSet = Vec<Diagnostic>;

/// Parse `cargo --message-format=json` output into a diagnostic set.
///
/// Whether this output came from cargo's machine-readable message stream.
///
/// Detected by evidence rather than by program name: cargo always terminates
/// the stream with a `build-finished` message, so its presence proves the format
/// regardless of what was compiled or whether anything failed.
///
/// This matters separately from [`from_json_lines`] returning an empty set. A
/// clean compile emits artifacts and a `build-finished` message but no
/// diagnostics; treating that as "no structured output" would leave the previous
/// baseline in place, and the next compile would report reintroduced
/// diagnostics as carried over rather than new.
pub fn is_machine_readable_output(output: &str) -> bool {
    output.lines().any(|line| {
        let line = line.trim();
        line.starts_with('{')
            && (line.contains(r#""reason":"build-finished""#)
                || line.contains(r#""reason":"compiler-message""#))
    })
}

/// Non-diagnostic lines (`compiler-artifact`, `build-finished`) are skipped, and
/// a line that is not JSON is skipped rather than treated as an error: a build
/// that printed a warning before cargo started is still a build.
pub fn from_json_lines(output: &str) -> DiagnosticSet {
    let mut diagnostics = Vec::new();
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() || !line.starts_with('{') {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("reason").and_then(Value::as_str) != Some("compiler-message") {
            continue;
        }
        let Some(message) = value.get("message") else {
            continue;
        };
        if let Some(diagnostic) = diagnostic_from(message) {
            // Cargo emits one message per level per site, but a diagnostic can
            // also arrive twice when a crate is compiled for several targets.
            let identity = diagnostic.identity();
            if !diagnostics
                .iter()
                .any(|existing: &Diagnostic| existing.identity() == identity)
            {
                diagnostics.push(diagnostic);
            }
        }
    }
    diagnostics
}

fn diagnostic_from(message: &Value) -> Option<Diagnostic> {
    let level = Level::parse(message.get("level")?.as_str()?)?;
    let message_text = message.get("message")?.as_str()?.to_string();
    let code = message
        .get("code")
        .and_then(|code| code.get("code"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let primary = message
        .get("spans")
        .and_then(Value::as_array)
        .and_then(|spans| spans.iter().find(|span| is_primary(span)))
        .or_else(|| {
            message
                .get("spans")
                .and_then(Value::as_array)
                .and_then(|spans| spans.first())
        })?;
    Some(Diagnostic {
        level,
        code,
        file: primary
            .get("file_name")
            .and_then(Value::as_str)
            .map(str::to_string),
        span: span_of(primary),
        message: truncate(&message_text, MESSAGE_CAP),
    })
}

fn is_primary(span: &Value) -> bool {
    span.get("is_primary")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn span_of(span: &Value) -> Option<Span> {
    Some(Span {
        start_line: span.get("line_start")?.as_u64()? as u32,
        start_column: span.get("column_start")?.as_u64()? as u32,
        end_line: span.get("line_end")?.as_u64()? as u32,
        end_column: span.get("column_end")?.as_u64()? as u32,
    })
}

/// Compare this compile against the previous one.
///
/// `previous` is `None` when no machine-readable baseline exists. The returned
/// delta says so explicitly rather than presenting every diagnostic as new.
pub fn delta(previous: Option<&DiagnosticSet>, current: DiagnosticSet) -> DiagnosticDelta {
    let Some(previous) = previous else {
        return DiagnosticDelta {
            current,
            new: Vec::new(),
            remaining: Vec::new(),
            resolved: Vec::new(),
            baseline_known: false,
        };
    };
    let previous_by_id: BTreeMap<String, &Diagnostic> = previous
        .iter()
        .map(|diagnostic| (diagnostic.identity(), diagnostic))
        .collect();
    let current_by_id: BTreeMap<String, &Diagnostic> = current
        .iter()
        .map(|diagnostic| (diagnostic.identity(), diagnostic))
        .collect();

    let new = current
        .iter()
        .filter(|diagnostic| !previous_by_id.contains_key(&diagnostic.identity()))
        .cloned()
        .collect();
    let remaining = current
        .iter()
        .filter(|diagnostic| previous_by_id.contains_key(&diagnostic.identity()))
        .cloned()
        .collect();
    let resolved = previous
        .iter()
        .filter(|diagnostic| !current_by_id.contains_key(&diagnostic.identity()))
        .cloned()
        .collect();

    DiagnosticDelta {
        current,
        new,
        remaining,
        resolved,
        baseline_known: true,
    }
}

/// Distinct codes, sorted with errors ahead of warnings.
fn distinct_codes(diagnostics: &[Diagnostic]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    let mut ordered: Vec<&Diagnostic> = diagnostics.iter().collect();
    // Errors first, so the codes an agent must act on are never pushed out of a
    // bounded list by warnings.
    ordered.sort_by_key(|diagnostic| diagnostic.level);
    for diagnostic in ordered {
        let code = diagnostic
            .code
            .clone()
            .unwrap_or_else(|| format!("<{}>", diagnostic.level.as_str()));
        if !seen.contains(&code) {
            seen.push(code);
        }
    }
    seen
}

fn codes(diagnostics: &[Diagnostic]) -> String {
    let distinct = distinct_codes(diagnostics);
    if distinct.is_empty() {
        return "none".to_string();
    }
    distinct
        .iter()
        .take(MAX_LISTED_CODES)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ")
}

fn count_level(diagnostics: &[Diagnostic], level: Level) -> usize {
    diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.level == level)
        .count()
}

fn first_line(text: &str) -> String {
    let line = text.lines().next().unwrap_or("").trim();
    if line.len() <= 60 {
        return line.to_string();
    }
    let mut end = 60;
    while end > 0 && !line.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &line[..end])
}

fn truncate(text: &str, cap: usize) -> String {
    if text.len() <= cap {
        return text.to_string();
    }
    let mut end = cap;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// Parse rendered `cargo check` output as a fallback.
///
/// `--message-format=json` is the primary path. This exists so a compile run
/// without JSON still yields codes instead of nothing, and it never invents a
/// code it did not read.
pub fn parse_rendered(output: &str) -> DiagnosticSet {
    let mut diagnostics = Vec::new();
    let mut lines = output.lines().peekable();
    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        let Some(rest) = trimmed
            .strip_prefix("error")
            .or_else(|| trimmed.strip_prefix("warning"))
        else {
            continue;
        };
        let level = if trimmed.starts_with("error") {
            Level::Error
        } else {
            Level::Warning
        };
        // `error[E0382]: message`, `warning: message`, or `error: message`.
        let (code, message) = match rest.strip_prefix('[') {
            Some(coded) => match coded.split_once(']') {
                Some((code, tail)) => (
                    (!code.is_empty()).then(|| code.to_string()),
                    tail.trim_start_matches(':').trim(),
                ),
                None => (None, rest.trim_start_matches(':').trim()),
            },
            None => (None, rest.trim_start_matches(':').trim()),
        };
        // rustc puts the location on the next line, indented.
        // rustc puts the primary location on the next line, indented.
        let location = lines
            .peek()
            .and_then(|next| next.trim().strip_prefix("--> ").map(str::to_string));
        if location.is_some() {
            lines.next();
        }
        let (file, span) = location
            .as_deref()
            .and_then(split_location)
            .unwrap_or_default();
        diagnostics.push(Diagnostic {
            level,
            code,
            file,
            span,
            message: truncate(message, MESSAGE_CAP),
        });
    }
    diagnostics
}

/// Split `path:line:column` into a path and a span.
///
/// Only the first line of a span is recovered, which is where the diagnostic is
/// anchored and the only line an agent needs to navigate to it.
fn split_location(location: &str) -> Option<(Option<String>, Option<Span>)> {
    let mut parts = location.rsplitn(3, ':');
    let column = parts.next()?.parse::<u32>().ok()?;
    let line = parts.next()?.parse::<u32>().ok()?;
    let file = parts.next()?;
    if file.is_empty() {
        return None;
    }
    Some((
        Some(file.to_string()),
        Some(Span {
            start_line: line,
            start_column: column,
            end_line: line,
            end_column: column,
        }),
    ))
}
