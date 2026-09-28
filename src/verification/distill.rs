//! Deterministic log distillation for compiler, test, build, lint and generic
//! output.
//!
//! The distiller never invents a count or a conclusion. It removes progress
//! noise safely, collapses exact duplicate lines and repeated adjacent blocks,
//! groups identical error lines, and extracts errors, warnings, source
//! locations, failed tests and test counts **only** when they are present in
//! the text. `Some(TestCounts)` means a known summary shape parsed cleanly;
//! otherwise the count is `None` and a note explains why.

use serde::{Deserialize, Serialize};

/// Maximum distilled error/warning/summary lines retained.
pub const MAX_DISTILLED_LINES: usize = 200;
/// Maximum source lines captured as stack context for one error.
pub const MAX_CONTEXT_LINES: usize = 3;

/// A `path:line:column` reference extracted verbatim from the output.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceLocation {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
}

impl SourceLocation {
    pub fn display(&self) -> String {
        match (self.line, self.column) {
            (Some(line), Some(column)) => format!("{}:{}:{}", self.path, line, column),
            (Some(line), None) => format!("{}:{}", self.path, line),
            _ => self.path.clone(),
        }
    }
}

/// One distilled error or warning line, with duplicates grouped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistilledLine {
    pub text: String,
    /// How many identical lines were grouped into this one.
    #[serde(default = "one")]
    pub count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<String>,
}

fn one() -> usize {
    1
}

/// Test counts parsed from a known summary shape. All values are read from the
/// output, never inferred from a non-zero exit status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestCounts {
    pub passed: usize,
    pub failed: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignored: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
    /// The summary format the numbers came from (for example `cargo-test`).
    pub source: String,
}

/// The deterministic distillation of one command's output.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DistilledOutput {
    #[serde(default)]
    pub errors: Vec<DistilledLine>,
    #[serde(default)]
    pub warnings: Vec<DistilledLine>,
    #[serde(default)]
    pub failed_tests: Vec<String>,
    #[serde(default)]
    pub source_locations: Vec<SourceLocation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counts: Option<TestCounts>,
    #[serde(default)]
    pub summary: Vec<String>,
    #[serde(default)]
    pub duplicate_lines: usize,
    #[serde(default)]
    pub duplicate_blocks: usize,
    #[serde(default)]
    pub progress_lines_removed: usize,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub notes: Vec<String>,
}

impl DistilledOutput {
    /// A stable multi-line rendering for the CLI.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for line in &self.summary {
            out.push_str(line);
            out.push('\n');
        }
        if !self.failed_tests.is_empty() {
            out.push_str(&format!("failed tests ({}):\n", self.failed_tests.len()));
            for test in &self.failed_tests {
                out.push_str(&format!("  - {test}\n"));
            }
        }
        if let Some(counts) = &self.counts {
            out.push_str(&format!(
                "test counts ({}): {} passed, {} failed",
                counts.source, counts.passed, counts.failed
            ));
            if let Some(ignored) = counts.ignored {
                out.push_str(&format!(", {ignored} ignored"));
            }
            out.push('\n');
        }
        for note in &self.notes {
            out.push_str(&format!("note: {note}\n"));
        }
        out
    }
}

/// Distill stdout and stderr independently, then merge.
pub fn distill(stdout: &str, stderr: &str, success: bool) -> DistilledOutput {
    let mut output = DistilledOutput::default();
    let mut seen_errors: Vec<DistilledLine> = Vec::new();
    let mut seen_warnings: Vec<DistilledLine> = Vec::new();
    let mut failed_tests: Vec<String> = Vec::new();
    let mut locations: Vec<SourceLocation> = Vec::new();
    let mut count_candidates: Vec<TestCounts> = Vec::new();

    for stream in [stdout, stderr] {
        let processed = process(stream);
        output.duplicate_lines += processed.duplicate_lines;
        output.duplicate_blocks += processed.duplicate_blocks;
        output.progress_lines_removed += processed.progress_lines_removed;

        for line in &processed.lines {
            match classify(line.text.trim_start()) {
                Some(LineKind::Error) => push_grouped(&mut seen_errors, line),
                Some(LineKind::Warning) => push_grouped(&mut seen_warnings, line),
                None => {}
            }
            if let Some(location) = parse_location(&line.text) {
                if !locations.contains(&location) {
                    locations.push(location);
                }
            }
            for test in failed_tests_in(&line.text) {
                if !failed_tests.contains(&test) {
                    failed_tests.push(test);
                }
            }
        }
        for counts in parse_counts(&processed.lines) {
            count_candidates.push(counts);
        }
    }

    output.errors = bound(seen_errors, MAX_DISTILLED_LINES, &mut output.truncated);
    output.warnings = bound(seen_warnings, MAX_DISTILLED_LINES, &mut output.truncated);
    output.failed_tests = failed_tests;
    output.source_locations = locations;

    // Only trust a count when every summary found agrees on the source shape.
    let mut distinct: Vec<&TestCounts> = Vec::new();
    for candidate in &count_candidates {
        if !distinct.contains(&candidate) {
            distinct.push(candidate);
        }
    }
    match distinct.as_slice() {
        [] => {}
        [only] => output.counts = Some((*only).clone()),
        _ => {
            output
                .notes
                .push("conflicting test count summaries were not trusted".to_string());
        }
    }

    build_summary(&mut output, success);
    if output.progress_lines_removed > 0 {
        output.notes.push(format!(
            "removed {} progress/overwrite line(s)",
            output.progress_lines_removed
        ));
    }
    if output.duplicate_lines > 0 {
        output.notes.push(format!(
            "collapsed {} exact duplicate line(s)",
            output.duplicate_lines
        ));
    }
    if output.duplicate_blocks > 0 {
        output.notes.push(format!(
            "collapsed {} repeated block(s)",
            output.duplicate_blocks
        ));
    }
    if output.truncated {
        output
            .notes
            .push("distilled output was truncated by the configured limits".to_string());
    }
    output
}

fn bound(mut lines: Vec<DistilledLine>, max: usize, truncated: &mut bool) -> Vec<DistilledLine> {
    if lines.len() > max {
        lines.truncate(max);
        *truncated = true;
    }
    lines
}

/// Group an identical error/warning line with the ones already seen. The first
/// occurrence keeps its context; later occurrences only increase the count.
fn push_grouped(target: &mut Vec<DistilledLine>, line: &CountedLine) {
    if let Some(existing) = target.iter_mut().find(|entry| entry.text == line.text) {
        existing.count += line.count;
        return;
    }
    target.push(DistilledLine {
        text: line.text.clone(),
        count: line.count,
        location: line.location.clone(),
        context: line.context.clone(),
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineKind {
    Error,
    Warning,
}

/// Classify a trimmed line as an error or warning. Conservative: a match must
/// look like a diagnostic prefix, not merely contain the word "error".
fn classify(text: &str) -> Option<LineKind> {
    let lower = text.to_ascii_lowercase();
    let starts = |prefix: &str, next_ok: &[char]| {
        lower.strip_prefix(prefix).map(|rest| {
            rest.chars()
                .next()
                .map(|ch| next_ok.contains(&ch))
                .unwrap_or(true)
        })
    };
    if starts("error", &['[', ':', ' ', '(', '-']).unwrap_or(false)
        || lower.contains(": error:")
        || lower.contains("error ts")
        || lower.contains("panicked at")
        || lower.starts_with("--- fail")
        || lower.starts_with("failed ")
        || lower.starts_with("assertion failed")
    {
        return Some(LineKind::Error);
    }
    if starts("warning", &['[', ':', ' ', '(', '-']).unwrap_or(false)
        || lower.contains(": warning:")
        || lower.contains("warning ts")
        || lower.starts_with("warn ")
    {
        return Some(LineKind::Warning);
    }
    None
}

/// A physical line after progress normalisation, with a duplicate count.
struct CountedLine {
    text: String,
    count: usize,
    location: Option<SourceLocation>,
    context: Vec<String>,
}

struct Processed {
    lines: Vec<CountedLine>,
    duplicate_lines: usize,
    duplicate_blocks: usize,
    progress_lines_removed: usize,
}

fn process(raw: &str) -> Processed {
    let mut lines: Vec<String> = Vec::new();
    let mut progress_lines_removed = 0usize;
    for physical in raw.split('\n') {
        let (line, overwritten) = normalize_physical(physical);
        progress_lines_removed += overwritten;
        if line.trim().is_empty() || is_progress_noise(&line) {
            if !line.trim().is_empty() {
                progress_lines_removed += 1;
            }
            continue;
        }
        lines.push(line);
    }

    // Collapse consecutive identical lines.
    let mut collapsed: Vec<(String, usize)> = Vec::new();
    let mut duplicate_lines = 0usize;
    let mut index = 0usize;
    while index < lines.len() {
        let mut end = index + 1;
        while end < lines.len() && lines[end] == lines[index] {
            end += 1;
        }
        let count = end - index;
        duplicate_lines += count - 1;
        collapsed.push((lines[index].clone(), count));
        index = end;
    }

    // Collapse repeated adjacent blocks (size 2..=8) into one block.
    let block_texts: Vec<String> = collapsed.iter().map(|(text, _)| text.clone()).collect();
    let block_counts: Vec<usize> = collapsed.iter().map(|(_, count)| *count).collect();
    let mut result: Vec<CountedLine> = Vec::new();
    let mut duplicate_blocks = 0usize;
    let mut i = 0usize;
    while i < block_texts.len() {
        let mut matched: Option<usize> = None;
        for size in 2..=8usize {
            if i + 2 * size > block_texts.len() {
                break;
            }
            if block_texts[i..i + size] == block_texts[i + size..i + 2 * size] {
                matched = Some(size);
                break;
            }
        }
        if let Some(size) = matched {
            let mut repeats = 1usize;
            let mut cursor = i + size;
            while cursor + size <= block_texts.len()
                && block_texts[cursor..cursor + size] == block_texts[i..i + size]
            {
                repeats += 1;
                cursor += size;
            }
            duplicate_blocks += repeats - 1;
            for offset in 0..size {
                result.push(CountedLine {
                    text: block_texts[i + offset].clone(),
                    count: block_counts[i + offset],
                    location: None,
                    context: Vec::new(),
                });
            }
            i = cursor;
        } else {
            result.push(CountedLine {
                text: block_texts[i].clone(),
                count: block_counts[i],
                location: None,
                context: Vec::new(),
            });
            i += 1;
        }
    }

    // Attach stack context and a location from the following indented lines
    // (Rust puts `--> path:line:col` on the line after the error).
    for index in 0..result.len() {
        let kind = classify(result[index].text.trim_start());
        if kind != Some(LineKind::Error) && kind != Some(LineKind::Warning) {
            continue;
        }
        let mut context = Vec::new();
        let mut location = parse_location(&result[index].text);
        let mut cursor = index + 1;
        while cursor < result.len() && context.len() < MAX_CONTEXT_LINES {
            let candidate = &result[cursor].text;
            if candidate.starts_with(' ') || candidate.starts_with('\t') {
                if location.is_none() {
                    location = parse_location(candidate);
                }
                context.push(candidate.trim().to_string());
                cursor += 1;
            } else {
                break;
            }
        }
        result[index].location = location;
        result[index].context = context;
    }

    Processed {
        lines: result,
        duplicate_lines,
        duplicate_blocks,
        progress_lines_removed,
    }
}

/// Normalize one physical line: strip ANSI, keep the last carriage-return
/// segment (progress overwrite) and trim trailing whitespace. Returns the
/// resulting line and how many overwritten segments were discarded.
fn normalize_physical(physical: &str) -> (String, usize) {
    let stripped = strip_ansi(physical);
    let segments: Vec<&str> = stripped
        .split('\r')
        .filter(|part| !part.trim().is_empty())
        .collect();
    match segments.last() {
        Some(last) => (last.trim_end().to_string(), segments.len() - 1),
        None => (String::new(), 0),
    }
}

fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            // CSI: ESC [ ... final byte in @..~
            Some('[') => {
                for inner in chars.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&inner) {
                        break;
                    }
                }
            }
            // OSC: ESC ] ... BEL or ST
            Some(']') => {
                while let Some(inner) = chars.next() {
                    if inner == '\u{7}' {
                        break;
                    }
                    if inner == '\u{1b}' {
                        if chars.peek() == Some(&'\\') {
                            chars.next();
                        }
                        break;
                    }
                }
            }
            // Any other escape: drop the introducer and the next character.
            Some(_) => {}
            None => {}
        }
    }
    out
}

/// Lines that carry only progress/overwrite information and no content.
fn is_progress_noise(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return true;
    }
    // Pure percentage: "42%" or "42.5 %".
    if let Some(number) = trimmed.strip_suffix('%') {
        let number = number.trim();
        if !number.is_empty() && number.chars().all(|ch| ch.is_ascii_digit() || ch == '.') {
            return true;
        }
    }
    // Braille spinner-only line (progress indicator without an overwrite).
    if trimmed
        .chars()
        .all(|ch| ('\u{2800}'..='\u{28ff}').contains(&ch))
    {
        return true;
    }
    // Download/extract progress bars that carry no durable content.
    let lower = trimmed.to_ascii_lowercase();
    if lower.contains('%')
        && [
            "downloading",
            "downloaded",
            "receiving",
            "resolving",
            "fetching",
            "unpacking",
            "extracting",
            "installing",
        ]
        .iter()
        .any(|verb| lower.starts_with(verb))
    {
        return true;
    }
    lower.contains("blocking waiting for file lock")
}

/// Extract a `path:line[:column]`, `path(line,col)` or Python `File` location.
pub fn parse_location(text: &str) -> Option<SourceLocation> {
    if let Some(rest) = text.split_once("--> ").map(|(_, rest)| rest) {
        if let Some(location) = parse_colon_path(rest) {
            return Some(location);
        }
    }
    if let Some(rest) = text.split_once("File \"").map(|(_, rest)| rest) {
        if let Some((path, tail)) = rest.split_once('"') {
            let line = tail
                .split_once("line ")
                .and_then(|(_, value)| leading_number(value));
            return Some(SourceLocation {
                path: path.to_string(),
                line,
                column: None,
            });
        }
    }
    if let Some(location) = parse_paren_path(text) {
        return Some(location);
    }
    scan_colon_path(text)
}

fn parse_colon_path(text: &str) -> Option<SourceLocation> {
    let token = text
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(|ch| matches!(ch, '\'' | '"' | '`' | '(' | ')' | ','));
    colon_parts(token)
}

fn parse_paren_path(text: &str) -> Option<SourceLocation> {
    // TypeScript style: path(line,col) or path(line)
    let mut search = text;
    while let Some(open) = search.find('(') {
        let before = &search[..open];
        let token = before
            .split_whitespace()
            .last()
            .unwrap_or("")
            .trim_matches(|ch| matches!(ch, '\'' | '"' | '`'));
        if let Some(close) = search[open + 1..].find(')') {
            let inner = &search[open + 1..open + 1 + close];
            let mut parts = inner.split(',');
            if let (Some(line), column) = (parts.next(), parts.next()) {
                if let Some(line) = leading_number(line.trim()) {
                    if is_source_like(token) {
                        return Some(SourceLocation {
                            path: token.to_string(),
                            line: Some(line),
                            column: column.and_then(|value| leading_number(value.trim())),
                        });
                    }
                }
            }
        }
        search = &search[open + 1..];
    }
    None
}

fn scan_colon_path(text: &str) -> Option<SourceLocation> {
    let bytes = text.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if !is_path_byte(bytes[index]) {
            index += 1;
            continue;
        }
        let start = index;
        while index < bytes.len() && is_path_byte(bytes[index]) {
            index += 1;
        }
        if index >= bytes.len() || bytes[index] != b':' {
            continue;
        }
        let path = &text[start..index];
        if !is_source_like(path) {
            continue;
        }
        let mut parts = text[index..].splitn(4, ':');
        parts.next(); // the empty segment before the first ':'
        let line = parts.next().and_then(leading_number);
        let column = parts.next().and_then(leading_number);
        if let Some(line) = line {
            return Some(SourceLocation {
                path: path.to_string(),
                line: Some(line),
                column,
            });
        }
    }
    None
}

fn is_path_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b'\\' | b'~')
}

fn colon_parts(token: &str) -> Option<SourceLocation> {
    let (path, tail) = token.rsplit_once(':')?;
    // Try `path:line:column` first.
    if let Some((path2, line)) = path.rsplit_once(':') {
        if let (Some(line), Some(column)) = (leading_number(line), leading_number(tail)) {
            if is_source_like(path2) {
                return Some(SourceLocation {
                    path: path2.to_string(),
                    line: Some(line),
                    column: Some(column),
                });
            }
        }
    }
    let line = leading_number(tail)?;
    if !is_source_like(path) {
        return None;
    }
    Some(SourceLocation {
        path: path.to_string(),
        line: Some(line),
        column: None,
    })
}

fn leading_number(text: &str) -> Option<u32> {
    let trimmed = text.trim();
    let digits: String = trimmed.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

/// Heuristic: a path with a recognized source extension, or a slashed path
/// with any extension. Keeps URLs and prose from being treated as locations.
fn is_source_like(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    if lower.contains("://") {
        return false;
    }
    let Some((_, extension)) = lower.rsplit_once('.') else {
        return false;
    };
    SOURCE_EXTENSIONS.contains(&extension)
        || (path.contains('/')
            && extension.len() <= 8
            && extension.chars().all(|ch| ch.is_ascii_alphanumeric()))
}

const SOURCE_EXTENSIONS: [&str; 34] = [
    "rs", "ts", "tsx", "js", "jsx", "mjs", "cjs", "py", "pyi", "go", "java", "kt", "kts", "c", "h",
    "cc", "cpp", "cxx", "hpp", "rb", "php", "cs", "swift", "scala", "sh", "bash", "sql", "html",
    "css", "scss", "json", "toml", "yaml", "yml",
];

/// Failed test names in the common Rust / Go / pytest / jest shapes.
pub fn failed_tests_in(text: &str) -> Vec<String> {
    let trimmed = text.trim_start();
    let mut out = Vec::new();
    if let Some(rest) = trimmed.strip_prefix("test ") {
        if let Some((name, _)) = rest.split_once(" ... ") {
            if trimmed.contains("FAILED") {
                out.push(name.trim().to_string());
            }
        }
    }
    if let Some(rest) = trimmed.strip_prefix("--- FAIL: ") {
        let name = rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches('(');
        if !name.is_empty() {
            out.push(name.to_string());
        }
    }
    if let Some(rest) = trimmed.strip_prefix("FAILED ") {
        let name = rest.split_whitespace().next().unwrap_or("");
        if !name.is_empty() {
            out.push(name.to_string());
        }
    }
    for marker in ["✕ ", "× ", "● "] {
        if let Some(rest) = trimmed.strip_prefix(marker) {
            let name = rest.trim();
            if !name.is_empty() {
                out.push(name.to_string());
            }
        }
    }
    out
}

/// Parse known test-count summary shapes. Returns one entry per detected
/// format (cargo lines are aggregated). An empty vector means no reliable
/// shape was present; callers must not infer counts from the exit status.
fn parse_counts(lines: &[CountedLine]) -> Vec<TestCounts> {
    let mut cargo: Option<TestCounts> = None;
    let mut pytest: Option<TestCounts> = None;
    let mut jest: Option<TestCounts> = None;
    for line in lines {
        if let Some(counts) = parse_cargo_counts(&line.text) {
            cargo = Some(merge_counts(cargo, counts));
        } else if let Some(counts) = parse_pytest_counts(&line.text) {
            pytest = Some(counts);
        } else if let Some(counts) = parse_jest_counts(&line.text) {
            jest = Some(counts);
        }
    }
    let mut out = Vec::new();
    out.extend(cargo);
    out.extend(pytest);
    out.extend(jest);
    out
}

fn merge_counts(existing: Option<TestCounts>, next: TestCounts) -> TestCounts {
    match existing {
        None => next,
        Some(mut current) => {
            current.passed += next.passed;
            current.failed += next.failed;
            current.ignored = match (current.ignored, next.ignored) {
                (Some(a), Some(b)) => Some(a + b),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            current.total = match (current.passed, current.failed, current.ignored) {
                (passed, failed, Some(ignored)) => Some(passed + failed + ignored),
                (passed, failed, None) => Some(passed + failed),
            };
            current
        }
    }
}

fn parse_cargo_counts(text: &str) -> Option<TestCounts> {
    let rest = text.trim_start().strip_prefix("test result:")?.trim_start();
    let passed = number_before(rest, " passed")?;
    let failed = number_before(rest, " failed")?;
    let ignored = number_before(rest, " ignored");
    let total = Some(passed + failed + ignored.unwrap_or(0));
    Some(TestCounts {
        passed,
        failed,
        ignored,
        total,
        source: "cargo-test".to_string(),
    })
}

fn parse_pytest_counts(text: &str) -> Option<TestCounts> {
    let trimmed = text.trim().trim_matches('=').trim();
    if !trimmed.contains(" in ") {
        return None;
    }
    let counts_text = trimmed.split(" in ").next().unwrap_or(trimmed);
    let mut passed = None;
    let mut failed = None;
    let mut ignored = None;
    for part in counts_text.split(',') {
        let part = part.trim();
        if let Some(value) = number_before_suffix(part, " passed") {
            passed = Some(value);
        } else if let Some(value) = number_before_suffix(part, " failed") {
            failed = Some(value);
        } else if let Some(value) = number_before_suffix(part, " skipped") {
            ignored = Some(value);
        } else if let Some(value) = number_before_suffix(part, " error") {
            failed = Some(failed.unwrap_or(0) + value);
        }
    }
    if passed.is_none() && failed.is_none() {
        return None;
    }
    let passed = passed.unwrap_or(0);
    let failed = failed.unwrap_or(0);
    Some(TestCounts {
        passed,
        failed,
        ignored,
        total: Some(passed + failed + ignored.unwrap_or(0)),
        source: "pytest".to_string(),
    })
}

fn parse_jest_counts(text: &str) -> Option<TestCounts> {
    let rest = text.trim_start().strip_prefix("Tests:")?;
    let passed = number_before(rest, " passed")?;
    let failed = number_before(rest, " failed").unwrap_or(0);
    let total = number_before(rest, " total");
    Some(TestCounts {
        passed,
        failed,
        ignored: None,
        total,
        source: "jest".to_string(),
    })
}

fn number_before(text: &str, suffix: &str) -> Option<usize> {
    let index = text.find(suffix)?;
    let before = text[..index].trim_end();
    let digits: String = before
        .chars()
        .rev()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

fn number_before_suffix(text: &str, suffix: &str) -> Option<usize> {
    if !text.ends_with(suffix) {
        return None;
    }
    let before = text[..text.len() - suffix.len()].trim_end();
    if before.is_empty() || !before.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    before.parse().ok()
}

fn build_summary(output: &mut DistilledOutput, success: bool) {
    for error in &output.errors {
        let mut line = format!("error: {}", error.text);
        if error.count > 1 {
            line.push_str(&format!(" (x{})", error.count));
        }
        if let Some(location) = &error.location {
            line.push_str(&format!(" [{}]", location.display()));
        }
        output.summary.push(line);
        for context in &error.context {
            output.summary.push(format!("  {context}"));
        }
    }
    for warning in &output.warnings {
        let mut line = format!("warning: {}", warning.text);
        if warning.count > 1 {
            line.push_str(&format!(" (x{})", warning.count));
        }
        if let Some(location) = &warning.location {
            line.push_str(&format!(" [{}]", location.display()));
        }
        output.summary.push(line);
    }
    if output.summary.is_empty() {
        output.summary.push(if success {
            "no errors or warnings were detected in the captured output".to_string()
        } else {
            "the command failed but no error line was recognized".to_string()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_ansi_and_progress_overwrites() {
        let raw = "Compiling demo\rDownloading 10%\rDownloading 90%\nerror[E0308]: bad\n";
        let output = distill(raw, "", false);
        assert!(output.progress_lines_removed >= 2);
        assert_eq!(output.errors.len(), 1);
        assert_eq!(output.errors[0].text, "error[E0308]: bad");
    }

    #[test]
    fn extracts_rust_error_locations_and_context() {
        let raw = "\
error[E0308]: mismatched types
  --> src/main.rs:12:5
   |
12 |     let x: u32 = \"s\";
   |                  ^^^
";
        let output = distill(raw, "", false);
        assert_eq!(output.errors.len(), 1);
        let error = &output.errors[0];
        assert_eq!(error.location.as_ref().unwrap().path, "src/main.rs");
        assert_eq!(error.location.as_ref().unwrap().line, Some(12));
        assert_eq!(error.location.as_ref().unwrap().column, Some(5));
        assert!(error.context.len() >= 2);
    }

    #[test]
    fn extracts_warnings_and_gcc_style_errors() {
        let raw = "src/foo.c:10:3: error: use of undeclared identifier\n\
warning: unused variable: `x`\n";
        let output = distill(raw, "", false);
        assert_eq!(output.errors.len(), 1);
        assert_eq!(
            output.errors[0].location.as_ref().unwrap().path,
            "src/foo.c"
        );
        assert_eq!(output.warnings.len(), 1);
    }

    #[test]
    fn collapses_duplicate_lines_and_blocks() {
        let raw = "note: first\nnote: first\nnote: first\nerror: repeated\nerror: repeated\n";
        let output = distill(raw, "", false);
        assert!(output.duplicate_lines >= 3);
        // The repeated error block of two lines appears twice.
        assert!(output.duplicate_blocks >= 1 || output.errors[0].count >= 2);
    }

    #[test]
    fn groups_repeated_identical_failures() {
        let raw = "error: same failure at a.rs:1:1\nerror: same failure at a.rs:1:1\n";
        let output = distill(raw, "", false);
        assert_eq!(output.errors.len(), 1);
        assert_eq!(output.errors[0].count, 2);
    }

    #[test]
    fn extracts_rust_failed_tests_and_counts() {
        let raw = "\
running 3 tests
test tests::a ... ok
test tests::b ... FAILED
test tests::c ... ok

failures:
    tests::b

test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out
";
        let output = distill(raw, "", false);
        assert_eq!(output.failed_tests, vec!["tests::b".to_string()]);
        let counts = output.counts.as_ref().unwrap();
        assert_eq!(counts.source, "cargo-test");
        assert_eq!(counts.passed, 2);
        assert_eq!(counts.failed, 1);
        assert_eq!(counts.ignored, Some(0));
    }

    #[test]
    fn extracts_pytest_failed_tests_and_counts() {
        let raw = "FAILED tests/test_x.py::test_y - AssertionError\n==== 1 failed, 2 passed in 0.12s ====\n";
        let output = distill(raw, "", false);
        assert_eq!(
            output.failed_tests,
            vec!["tests/test_x.py::test_y".to_string()]
        );
        let counts = output.counts.as_ref().unwrap();
        assert_eq!(counts.source, "pytest");
        assert_eq!(counts.passed, 2);
        assert_eq!(counts.failed, 1);
    }

    #[test]
    fn extracts_jest_failed_tests_and_counts() {
        let raw = "FAIL src/app.test.ts\n● renders the widget\nTests:       2 failed, 3 passed, 5 total\n";
        let output = distill(raw, "", false);
        assert!(output
            .failed_tests
            .contains(&"renders the widget".to_string()));
        let counts = output.counts.as_ref().unwrap();
        assert_eq!(counts.source, "jest");
        assert_eq!(counts.passed, 3);
        assert_eq!(counts.failed, 2);
        assert_eq!(counts.total, Some(5));
    }

    #[test]
    fn conflicting_counts_are_not_trusted() {
        let raw = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured\nTests:       3 passed, 0 failed, 3 total\n";
        let output = distill(raw, "", true);
        assert!(output.counts.is_none());
        assert!(output.notes.iter().any(|note| note.contains("conflicting")));
    }

    #[test]
    fn never_invents_counts_when_absent() {
        let output = distill("all good\n", "warn: nothing\n", true);
        assert!(output.counts.is_none());
    }

    #[test]
    fn preserves_invalid_utf8_lossily_at_the_bytes_boundary() {
        let bytes = b"error: bad value f\xffo";
        let text = String::from_utf8_lossy(bytes).into_owned();
        let output = distill(&text, "", false);
        assert_eq!(output.errors.len(), 1);
        assert!(output.errors[0].text.contains('\u{fffd}'));
    }

    #[test]
    fn stack_context_is_retained_for_errors() {
        let raw = "error: boom\n    at module (src/a.ts:3:9)\n    at next (src/b.ts:4:1)\n";
        let output = distill(raw, "", false);
        assert_eq!(output.errors.len(), 1);
        assert_eq!(output.errors[0].context.len(), 2);
    }

    #[test]
    fn extracts_network_and_typescript_output() {
        let raw = "\
error TS2322: Type 'string' is not assignable to type 'number'.\n\
src/app.ts(12,5): error TS2322: still bad\n\
error: failed to download https://example.invalid/pkg: connection timed out\n";
        let output = distill(raw, "", false);
        assert_eq!(output.errors.len(), 3);
        assert!(output.errors[0].text.contains("TS2322"));
        assert!(
            output
                .source_locations
                .iter()
                .any(|location| location.path == "src/app.ts"
                    && location.line == Some(12)
                    && location.column == Some(5)),
            "{output:?}"
        );
    }

    #[test]
    fn errors_are_extracted_from_stderr_too() {
        let output = distill("", "error[E0432]: unresolved import\n", false);
        assert_eq!(output.errors.len(), 1);
        assert_eq!(output.errors[0].text, "error[E0432]: unresolved import");
    }

    #[test]
    fn collapses_repeated_adjacent_blocks() {
        let raw = "error: same\n  --> a.rs:1:1\nerror: same\n  --> a.rs:1:1\n";
        let output = distill(raw, "", false);
        assert_eq!(output.duplicate_blocks, 1);
        assert_eq!(output.errors.len(), 1);
    }

    #[test]
    fn empty_output_is_honest() {
        let output = distill("", "", true);
        assert!(output.errors.is_empty());
        assert!(output.warnings.is_empty());
        assert!(output.counts.is_none());
        assert!(output.summary[0].contains("no errors or warnings"));
    }
}
