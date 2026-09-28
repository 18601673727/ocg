//! Conservative, dependency-free symbol extraction.
//!
//! The engine deliberately does not embed a parser generator. Symbol ranges
//! come from a deterministic line/brace/indent scan that is good enough to
//! rank context, and never fails a whole index: an unparsable file simply
//! contributes no symbols. Unsupported languages keep path metadata only.

use serde::{Deserialize, Serialize};

/// The kinds that survive into the index. Kept deliberately small so ranking
/// stays explainable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolKind {
    Function,
    Method,
    Type,
    Class,
    Module,
    Import,
    Export,
    /// A probable content reference rather than a declaration.
    Reference,
}

impl SymbolKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SymbolKind::Function => "function",
            SymbolKind::Method => "method",
            SymbolKind::Type => "type",
            SymbolKind::Class => "class",
            SymbolKind::Module => "module",
            SymbolKind::Import => "import",
            SymbolKind::Export => "export",
            SymbolKind::Reference => "reference",
        }
    }
}

/// One extracted symbol with a 1-based inclusive line range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Symbol {
    pub name: String,
    pub kind: SymbolKind,
    pub start_line: u32,
    pub end_line: u32,
    pub signature: String,
}

/// A symbol together with the file it came from.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SymbolRef {
    pub path: String,
    pub name: String,
    pub kind: SymbolKind,
    pub start_line: u32,
    pub end_line: u32,
}

impl SymbolRef {
    pub fn new(path: impl Into<String>, symbol: &Symbol) -> Self {
        Self {
            path: path.into(),
            name: symbol.name.clone(),
            kind: symbol.kind,
            start_line: symbol.start_line,
            end_line: symbol.end_line,
        }
    }
}

/// The languages the extractor understands. Anything else is metadata-only.
pub fn is_supported(language: &str) -> bool {
    matches!(language, "rust" | "typescript" | "javascript" | "python")
}

/// Extract symbols from `text` for `language`, capped at `max`.
///
/// The result is always sorted by `(start_line, name, kind)` so the index is
/// byte-for-byte stable across runs.
pub fn extract(language: &str, text: &str, max: usize) -> Vec<Symbol> {
    if max == 0 {
        return Vec::new();
    }
    let mut symbols = match language {
        "rust" => extract_rust(text, max),
        "typescript" | "javascript" => extract_script(text, max),
        "python" => extract_python(text, max),
        _ => Vec::new(),
    };
    symbols.truncate_after_sort(max);
    symbols
}

trait TruncateAfterSort {
    fn truncate_after_sort(&mut self, max: usize);
}

impl TruncateAfterSort for Vec<Symbol> {
    fn truncate_after_sort(&mut self, max: usize) {
        self.sort_by(|a, b| (a.start_line, &a.name, a.kind).cmp(&(b.start_line, &b.name, b.kind)));
        self.dedup_by(|a, b| a.name == b.name && a.kind == b.kind && a.start_line == b.start_line);
        self.truncate(max);
    }
}

/// A crude "code only" view of a line: comments and string literals removed.
/// It is only used for keyword detection, never for parsing semantics.
fn code_only(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    while let Some(ch) = chars.next() {
        if in_single {
            if ch == '\\' {
                chars.next();
            } else if ch == '\'' {
                in_single = false;
            }
            out.push(' ');
            continue;
        }
        if in_double {
            if ch == '\\' {
                chars.next();
            } else if ch == '"' {
                in_double = false;
            }
            out.push(' ');
            continue;
        }
        if in_backtick {
            if ch == '\\' {
                chars.next();
            } else if ch == '`' {
                in_backtick = false;
            }
            out.push(' ');
            continue;
        }
        match ch {
            '/' if chars.peek() == Some(&'/') => break,
            '#' if line.trim_start().starts_with('#') => break,
            '\'' => {
                in_single = true;
                out.push(' ');
            }
            '"' => {
                in_double = true;
                out.push(' ');
            }
            '`' => {
                in_backtick = true;
                out.push(' ');
            }
            _ => out.push(ch),
        }
    }
    out
}

fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn signature(code: &str, cut: char) -> String {
    let trimmed = code.trim();
    let head = trimmed.split(cut).next().unwrap_or(trimmed).trim();
    let collapsed = collapse_whitespace(head);
    collapsed.chars().take(200).collect()
}

/// Brace-match from `start` (0-based) to the closing brace line (0-based).
/// Falls back to the last line when the braces never close.
fn brace_end(lines: &[&str], start: usize) -> usize {
    let mut depth: i32 = 0;
    let mut opened = false;
    for (offset, raw) in lines[start..].iter().enumerate() {
        let code = code_only(raw);
        for ch in code.chars() {
            if ch == '{' {
                depth += 1;
                opened = true;
            } else if ch == '}' {
                depth -= 1;
            }
        }
        if opened && depth <= 0 {
            return start + offset;
        }
    }
    lines.len().saturating_sub(1)
}

/// Find top-level brace spans for lines accepted by `predicate`.
fn brace_spans(lines: &[&str], predicate: impl Fn(&str) -> bool) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        let trimmed = code_only(lines[index]);
        let trimmed = trimmed.trim();
        if predicate(trimmed) && lines[index].contains('{') {
            let end = brace_end(lines, index);
            spans.push((index, end));
            index = end + 1;
        } else {
            index += 1;
        }
    }
    spans
}

fn inside(spans: &[(usize, usize)], line: usize) -> bool {
    spans
        .iter()
        .any(|(start, end)| line > *start && line <= *end)
}

fn identifier_after(rest: &str) -> Option<String> {
    let rest = rest.trim_start();
    let name: String = rest
        .chars()
        .take_while(|ch| ch.is_alphanumeric() || *ch == '_')
        .collect();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn starts_word<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let trimmed = text.trim_start();
    let rest = trimmed.strip_prefix(word)?;
    if rest.is_empty() || !rest.starts_with(|ch: char| ch.is_alphanumeric() || ch == '_') {
        Some(rest)
    } else {
        None
    }
}

/// Find `word` as a standalone token anywhere in the line and return the rest.
fn after_token<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let bytes = text.as_bytes();
    let mut index = 0;
    while let Some(found) = text[index..].find(word) {
        let absolute = index + found;
        let before_ok = absolute == 0
            || !bytes[absolute - 1].is_ascii_alphanumeric() && bytes[absolute - 1] != b'_';
        let after = absolute + word.len();
        let after_ok =
            after >= text.len() || !bytes[after].is_ascii_alphanumeric() && bytes[after] != b'_';
        if before_ok && after_ok {
            return Some(&text[after..]);
        }
        index = absolute + word.len();
    }
    None
}

fn extract_rust(text: &str, max: usize) -> Vec<Symbol> {
    let lines: Vec<&str> = text.lines().collect();
    let method_spans = brace_spans(&lines, |trimmed| {
        trimmed.starts_with("impl") && !trimmed.starts_with("impl_")
    });
    let mut symbols = Vec::new();
    for (index, raw) in lines.iter().enumerate() {
        let code = code_only(raw);
        let trimmed = code.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = starts_word(trimmed, "fn").or_else(|| after_token(trimmed, "fn")) {
            if let Some(name) = identifier_after(rest) {
                let kind = if inside(&method_spans, index) {
                    SymbolKind::Method
                } else {
                    SymbolKind::Function
                };
                let end = if raw.contains('{') {
                    brace_end(&lines, index)
                } else {
                    index
                };
                push(&mut symbols, name, kind, index, end, &code, '{');
                if trimmed.starts_with("pub") {
                    push_export(&mut symbols, &lines, index, end, &code);
                }
            }
            continue;
        }
        for (keyword, kind) in [
            ("struct", SymbolKind::Type),
            ("enum", SymbolKind::Type),
            ("union", SymbolKind::Type),
            ("trait", SymbolKind::Type),
            ("type", SymbolKind::Type),
        ] {
            if let Some(rest) =
                starts_word(trimmed, keyword).or_else(|| after_token(trimmed, keyword))
            {
                if let Some(name) = identifier_after(rest) {
                    let end = if raw.contains('{') {
                        brace_end(&lines, index)
                    } else {
                        index
                    };
                    push(&mut symbols, name, kind, index, end, &code, '{');
                    if trimmed.starts_with("pub") {
                        push_export(&mut symbols, &lines, index, end, &code);
                    }
                }
                break;
            }
        }
        if let Some(rest) = starts_word(trimmed, "mod") {
            if let Some(name) = identifier_after(rest) {
                let end = if raw.contains('{') {
                    brace_end(&lines, index)
                } else {
                    index
                };
                push(
                    &mut symbols,
                    name,
                    SymbolKind::Module,
                    index,
                    end,
                    &code,
                    '{',
                );
                if trimmed.starts_with("pub") {
                    push_export(&mut symbols, &lines, index, end, &code);
                }
            }
        }
        if let Some(rest) = starts_word(trimmed, "use") {
            let path = rest.trim().trim_end_matches(';').trim().to_string();
            if !path.is_empty() {
                push_name(
                    &mut symbols,
                    path.clone(),
                    SymbolKind::Import,
                    index,
                    index,
                    &code,
                );
                if trimmed.starts_with("pub") {
                    push_name(&mut symbols, path, SymbolKind::Export, index, index, &code);
                }
            }
        }
    }
    if symbols.len() > max * 4 {
        // A pathological file: keep the extractor bounded before sorting.
        symbols.truncate(max * 4);
    }
    symbols
}

fn extract_script(text: &str, max: usize) -> Vec<Symbol> {
    let lines: Vec<&str> = text.lines().collect();
    let class_spans = brace_spans(&lines, |trimmed| {
        starts_word(trimmed, "class").is_some() || starts_word(trimmed, "interface").is_some()
    });
    let mut symbols = Vec::new();
    for (index, raw) in lines.iter().enumerate() {
        let code = code_only(raw);
        let trimmed = code.trim();
        if trimmed.is_empty() {
            continue;
        }
        let exported = trimmed.starts_with("export");
        let body = trimmed.strip_prefix("export").unwrap_or(trimmed).trim();
        let body = body.strip_prefix("default").unwrap_or(body).trim();

        if let Some(rest) = starts_word(body, "function").or_else(|| after_token(body, "function"))
        {
            if let Some(name) = identifier_after(rest) {
                let kind = if inside(&class_spans, index) {
                    SymbolKind::Method
                } else {
                    SymbolKind::Function
                };
                let end = brace_end(&lines, index);
                push(&mut symbols, name.clone(), kind, index, end, &code, '{');
                if exported {
                    push_name(&mut symbols, name, SymbolKind::Export, index, end, &code);
                }
            }
            continue;
        }
        for (keyword, kind) in [
            ("class", SymbolKind::Class),
            ("interface", SymbolKind::Type),
            ("enum", SymbolKind::Type),
            ("namespace", SymbolKind::Module),
            ("module", SymbolKind::Module),
        ] {
            if let Some(rest) = starts_word(body, keyword) {
                if let Some(name) = identifier_after(rest) {
                    let end = brace_end(&lines, index);
                    push(&mut symbols, name.clone(), kind, index, end, &code, '{');
                    if exported {
                        push_name(&mut symbols, name, SymbolKind::Export, index, end, &code);
                    }
                }
                break;
            }
        }
        if let Some(rest) = starts_word(body, "type") {
            if let Some(name) = identifier_after(rest) {
                push(
                    &mut symbols,
                    name.clone(),
                    SymbolKind::Type,
                    index,
                    index,
                    &code,
                    '=',
                );
                if exported {
                    push_name(&mut symbols, name, SymbolKind::Export, index, index, &code);
                }
            }
        }
        let raw_trimmed = raw.trim();
        if let Some(rest) = starts_word(raw_trimmed, "import") {
            if let Some(name) = import_specifier(rest) {
                push_name(&mut symbols, name, SymbolKind::Import, index, index, &code);
            }
        }
        // `const name = (...) =>` and `name(...) {` inside a class.
        if let Some(rest) = starts_word(body, "const")
            .or_else(|| starts_word(body, "let"))
            .or_else(|| starts_word(body, "var"))
        {
            let rest = rest.trim_start();
            if let Some(name) = identifier_after(rest) {
                let after_name = rest[name.len()..].trim_start();
                if let Some(value) = after_name.strip_prefix('=') {
                    let value = value.trim_start();
                    if value.contains("=>") || value.starts_with("function") {
                        let end = brace_end(&lines, index);
                        push(
                            &mut symbols,
                            name.clone(),
                            SymbolKind::Function,
                            index,
                            end,
                            &code,
                            '=',
                        );
                        if exported {
                            push_name(&mut symbols, name, SymbolKind::Export, index, end, &code);
                        }
                    }
                }
            }
        }
        if inside(&class_spans, index) {
            if let Some(name) = class_method_name(body) {
                let end = brace_end(&lines, index);
                push(
                    &mut symbols,
                    name,
                    SymbolKind::Method,
                    index,
                    end,
                    &code,
                    '{',
                );
            }
        }
    }
    if symbols.len() > max * 4 {
        symbols.truncate(max * 4);
    }
    symbols
}

fn class_method_name(text: &str) -> Option<String> {
    let text = text.trim();
    if text.starts_with("function")
        || text.starts_with("if")
        || text.starts_with("for")
        || text.starts_with("while")
        || text.starts_with("switch")
        || text.starts_with("return")
        || text.starts_with("class")
    {
        return None;
    }
    let paren = text.find('(')?;
    let before = text[..paren].trim();
    if before.is_empty()
        || !before
            .chars()
            .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '$')
    {
        return None;
    }
    if !text.contains('{') && !text.contains("=>") {
        return None;
    }
    Some(before.to_string())
}

fn import_specifier(rest: &str) -> Option<String> {
    let rest = rest.trim();
    if rest.is_empty() {
        return None;
    }
    let from = rest.rfind("from");
    let tail = match from {
        Some(position) => rest[position + "from".len()..].trim(),
        None => rest,
    };
    let quoted = tail.trim_matches(|ch: char| ch == ';' || ch.is_whitespace());
    let first = quoted.chars().find(|ch| *ch == '\'' || *ch == '"');
    if let Some(quote) = first {
        let inner: String = quoted
            .chars()
            .skip_while(|ch| *ch != quote)
            .skip(1)
            .take_while(|ch| *ch != quote)
            .collect();
        if !inner.is_empty() {
            return Some(inner);
        }
    }
    let bare: String = quoted
        .chars()
        .take_while(|ch| !ch.is_whitespace() && *ch != ';')
        .collect();
    if bare.is_empty() {
        None
    } else {
        Some(bare)
    }
}

fn extract_python(text: &str, max: usize) -> Vec<Symbol> {
    let lines: Vec<&str> = text.lines().collect();
    let mut symbols = Vec::new();
    for (index, raw) in lines.iter().enumerate() {
        let index_indent = raw.len() - raw.trim_start().len();
        let code = code_only(raw);
        let trimmed = code.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = starts_word(trimmed, "def")
            .or_else(|| after_token(trimmed, "def"))
            .or_else(|| starts_word(trimmed, "async").and_then(|_| after_token(trimmed, "def")))
        {
            if let Some(name) = identifier_after(rest) {
                let kind = if index_indent == 0 {
                    SymbolKind::Function
                } else {
                    SymbolKind::Method
                };
                let end = indent_end(&lines, index, index_indent);
                push(&mut symbols, name, kind, index, end, &code, ':');
            }
            continue;
        }
        if let Some(rest) = starts_word(trimmed, "class") {
            if let Some(name) = identifier_after(rest) {
                let end = indent_end(&lines, index, index_indent);
                push(
                    &mut symbols,
                    name,
                    SymbolKind::Class,
                    index,
                    end,
                    &code,
                    ':',
                );
            }
            continue;
        }
        if let Some(rest) = starts_word(trimmed, "import") {
            let module = rest.split_whitespace().next().unwrap_or("").to_string();
            if !module.is_empty() {
                push_name(
                    &mut symbols,
                    module,
                    SymbolKind::Import,
                    index,
                    index,
                    &code,
                );
            }
        }
        if let Some(rest) = starts_word(trimmed, "from") {
            let module = rest.split_whitespace().next().unwrap_or("").to_string();
            if !module.is_empty() {
                push_name(
                    &mut symbols,
                    module,
                    SymbolKind::Import,
                    index,
                    index,
                    &code,
                );
            }
        }
    }
    if symbols.len() > max * 4 {
        symbols.truncate(max * 4);
    }
    symbols
}

fn indent_end(lines: &[&str], start: usize, indent: usize) -> usize {
    let mut end = start;
    for (index, raw) in lines.iter().enumerate().skip(start + 1) {
        if raw.trim().is_empty() {
            continue;
        }
        let current = raw.len() - raw.trim_start().len();
        if current <= indent {
            break;
        }
        end = index;
    }
    end
}

fn push(
    symbols: &mut Vec<Symbol>,
    name: String,
    kind: SymbolKind,
    start: usize,
    end: usize,
    code: &str,
    _cut: char,
) {
    push_name(symbols, name, kind, start, end, code);
}

fn push_name(
    symbols: &mut Vec<Symbol>,
    name: String,
    kind: SymbolKind,
    start: usize,
    end: usize,
    code: &str,
) {
    symbols.push(Symbol {
        name,
        kind,
        start_line: (start + 1) as u32,
        end_line: (end + 1) as u32,
        signature: signature(code, '{'),
    });
}

fn push_export(symbols: &mut Vec<Symbol>, _lines: &[&str], start: usize, end: usize, code: &str) {
    let name = public_name(code).unwrap_or_else(|| "public".to_string());
    push_name(symbols, name, SymbolKind::Export, start, end, code);
}

fn public_name(code: &str) -> Option<String> {
    let trimmed = code.trim();
    let rest = trimmed.strip_prefix("pub").unwrap_or(trimmed).trim_start();
    let rest = rest.strip_prefix("(crate)").unwrap_or(rest).trim_start();
    for keyword in [
        "fn", "struct", "enum", "trait", "union", "type", "mod", "use",
    ] {
        if let Some(after) = after_token(rest, keyword) {
            if keyword == "use" {
                let path = after.trim().trim_end_matches(';').trim().to_string();
                if !path.is_empty() {
                    return Some(path);
                }
            } else if let Some(name) = identifier_after(after) {
                return Some(name);
            }
        }
    }
    None
}

/// A simple word-boundary search used by the reference APIs.
pub fn contains_word(text: &str, word: &str) -> bool {
    let bytes = text.as_bytes();
    let mut index = 0;
    while let Some(found) = text[index..].find(word) {
        let absolute = index + found;
        let before_ok = absolute == 0
            || !bytes[absolute - 1].is_ascii_alphanumeric() && bytes[absolute - 1] != b'_';
        let after = absolute + word.len();
        let after_ok =
            after >= text.len() || !bytes[after].is_ascii_alphanumeric() && bytes[after] != b'_';
        if before_ok && after_ok {
            return true;
        }
        index = absolute + word.len();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(symbols: &[Symbol], kind: SymbolKind) -> Vec<String> {
        symbols
            .iter()
            .filter(|symbol| symbol.kind == kind)
            .map(|symbol| symbol.name.clone())
            .collect()
    }

    #[test]
    fn rust_functions_types_methods_imports() {
        let text = "use std::collections::HashMap;\n\npub struct Engine {\n    field: u32,\n}\n\nimpl Engine {\n    pub fn run(&self) -> u32 {\n        self.field\n    }\n\n    fn helper(&self) {}\n}\n\npub fn free(x: u32) -> u32 {\n    x\n}\n";
        let symbols = extract("rust", text, 100);
        assert_eq!(names(&symbols, SymbolKind::Function), vec!["free"]);
        assert_eq!(names(&symbols, SymbolKind::Method), vec!["run", "helper"]);
        assert!(names(&symbols, SymbolKind::Type).contains(&"Engine".to_string()));
        assert!(
            names(&symbols, SymbolKind::Import).contains(&"std::collections::HashMap".to_string())
        );
        assert!(names(&symbols, SymbolKind::Export).contains(&"Engine".to_string()));
        let free = symbols.iter().find(|s| s.name == "free").unwrap();
        assert_eq!(free.start_line, 15);
        assert_eq!(free.end_line, 17);
    }

    #[test]
    fn typescript_classes_functions_imports_exports() {
        let text = "import { readFile } from 'node:fs';\n\nexport function load(path: string) {\n  return readFile(path);\n}\n\nclass Store {\n  get(key: string) {\n    return key;\n  }\n}\n\nexport interface Config {\n  name: string;\n}\n\nconst arrow = (x: number) => {\n  return x + 1;\n};\n";
        let symbols = extract("typescript", text, 100);
        assert!(names(&symbols, SymbolKind::Function).contains(&"load".to_string()));
        assert!(names(&symbols, SymbolKind::Function).contains(&"arrow".to_string()));
        assert!(names(&symbols, SymbolKind::Class).contains(&"Store".to_string()));
        assert!(names(&symbols, SymbolKind::Method).contains(&"get".to_string()));
        assert!(names(&symbols, SymbolKind::Type).contains(&"Config".to_string()));
        assert!(names(&symbols, SymbolKind::Import).contains(&"node:fs".to_string()));
        assert!(names(&symbols, SymbolKind::Export).contains(&"load".to_string()));
    }

    #[test]
    fn python_functions_methods_classes_imports() {
        let text = "import os\n\nfrom pathlib import Path\n\nclass Store:\n    def get(self, key):\n        return key\n\n    async def put(self, key):\n        return None\n\ndef free(value):\n    return value\n";
        let symbols = extract("python", text, 100);
        assert_eq!(names(&symbols, SymbolKind::Function), vec!["free"]);
        assert_eq!(names(&symbols, SymbolKind::Method), vec!["get", "put"]);
        assert!(names(&symbols, SymbolKind::Class).contains(&"Store".to_string()));
        assert!(names(&symbols, SymbolKind::Import).contains(&"os".to_string()));
        assert!(names(&symbols, SymbolKind::Import).contains(&"pathlib".to_string()));
        let free = symbols.iter().find(|s| s.name == "free").unwrap();
        assert_eq!(free.start_line, 12);
        assert_eq!(free.end_line, 13);
    }

    #[test]
    fn unsupported_language_yields_nothing() {
        assert!(extract("go", "package main\nfunc main() {}\n", 100).is_empty());
        assert!(extract("ruby", "def foo\nend\n", 100).is_empty());
    }

    #[test]
    fn output_is_deterministic_and_bounded() {
        let text = "fn a() {}\nfn b() {}\nfn c() {}\n";
        let first = extract("rust", text, 2);
        let second = extract("rust", text, 2);
        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].name, "a");
    }

    #[test]
    fn word_search_is_boundary_aware() {
        assert!(contains_word("call run()", "run"));
        assert!(!contains_word("running", "run"));
        assert!(contains_word("let runner = run;", "run"));
    }
}
