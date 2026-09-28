//! Conservative targeted-test selection.
//!
//! The proposal API never *runs* anything and never claims that unselected
//! tests cannot fail. It returns candidates with explicit evidence and always
//! sets `complete = false`. When it has no candidates it reports a fallback to
//! the configured verification stage instead of inventing a test to run.
//!
//! The heuristics are deliberately simple and language-aware:
//!
//! - adjacent/naming conventions for Rust, TypeScript/JavaScript and Python;
//! - a test whose indexed symbols have the same name as a changed symbol (an
//!   *index name match*, not proof of a textual reference);
//! - the changed file itself when it is already a test.
//!
//! Unsupported languages are reported, not guessed at.

use serde::{Deserialize, Serialize};

/// Confidence in a candidate. A label, not a probability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    High,
    Medium,
    Low,
}

impl Confidence {
    fn rank(self) -> u8 {
        match self {
            Confidence::High => 3,
            Confidence::Medium => 2,
            Confidence::Low => 1,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Confidence::High => "high",
            Confidence::Medium => "medium",
            Confidence::Low => "low",
        }
    }
}

/// One proposed test file with the evidence that selected it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestCandidate {
    pub path: String,
    pub confidence: Confidence,
    #[serde(default)]
    pub reasons: Vec<String>,
}

impl TestCandidate {
    fn merge(&mut self, confidence: Confidence, reason: String) {
        if confidence.rank() > self.confidence.rank() {
            self.confidence = confidence;
        }
        if !self.reasons.contains(&reason) {
            self.reasons.push(reason);
        }
    }
}

/// A changed source file reduced to what selection needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceChange {
    pub path: String,
    pub language: String,
    #[serde(default)]
    pub symbols: Vec<String>,
}

/// A test file and the changed symbol names that match its indexed symbols.
///
/// The match is an **index name match** (a declaration or import symbol with
/// the same name), not proof of a textual reference; the candidate evidence
/// says so explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestIndexEntry {
    pub path: String,
    pub language: String,
    #[serde(default)]
    pub index_names: Vec<String>,
}

/// The selection proposal. `complete` is always `false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestProposal {
    pub changed_files: Vec<String>,
    #[serde(default)]
    pub candidates: Vec<TestCandidate>,
    #[serde(default)]
    pub unsupported: Vec<String>,
    /// Always false: unselected tests may still fail.
    pub complete: bool,
    /// The configured stage to run when no targeted test is proposed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<String>,
    #[serde(default)]
    pub notes: Vec<String>,
}

impl TestProposal {
    /// Whether the proposal suggests targeted tests (it may still be partial).
    pub fn has_candidates(&self) -> bool {
        !self.candidates.is_empty()
    }

    /// A stable multi-line rendering for the CLI.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "targeted test proposal (complete=false): {} candidate(s)\n",
            self.candidates.len()
        ));
        for candidate in &self.candidates {
            let reasons = if candidate.reasons.is_empty() {
                "-".to_string()
            } else {
                candidate.reasons.join(", ")
            };
            out.push_str(&format!(
                "  [{}] {} ({})\n",
                candidate.confidence.as_str(),
                candidate.path,
                reasons
            ));
        }
        if !self.unsupported.is_empty() {
            out.push_str(&format!(
                "unsupported changed files (not guessed): {}\n",
                self.unsupported.join(", ")
            ));
        }
        if let Some(fallback) = &self.fallback {
            out.push_str(&format!(
                "fallback: run the configured '{fallback}' verification stage\n"
            ));
        }
        for note in &self.notes {
            out.push_str(&format!("note: {note}\n"));
        }
        out
    }
}

/// Languages the selection conventions cover.
pub const SUPPORTED_LANGUAGES: [&str; 4] = ["rust", "typescript", "javascript", "python"];

/// Build a proposal from changed sources and the known test files.
pub fn propose(
    changes: &[SourceChange],
    tests: &[TestIndexEntry],
    fallback_stage: Option<&str>,
) -> TestProposal {
    let test_paths: Vec<&str> = tests.iter().map(|entry| entry.path.as_str()).collect();
    let mut candidates: Vec<TestCandidate> = Vec::new();
    let mut unsupported: Vec<String> = Vec::new();
    let mut changed_files: Vec<String> = Vec::new();

    for change in changes {
        if !changed_files.contains(&change.path) {
            changed_files.push(change.path.clone());
        }
        if !SUPPORTED_LANGUAGES.contains(&change.language.as_str()) {
            if !unsupported.contains(&change.path) {
                unsupported.push(change.path.clone());
            }
            continue;
        }

        // The changed file is itself a test: propose it directly.
        if crate::context::gitdiff::is_test_path(&change.path)
            && test_paths.contains(&change.path.as_str())
        {
            push_candidate(
                &mut candidates,
                &change.path,
                Confidence::High,
                format!("changed test file (language: {})", change.language),
            );
            continue;
        }

        // Adjacent / naming conventions.
        for derived in adjacent_test_paths(&change.path, &change.language) {
            if test_paths.contains(&derived.as_str()) {
                push_candidate(
                    &mut candidates,
                    &derived,
                    Confidence::High,
                    format!("adjacent to {}", change.path),
                );
            }
        }

        // A test whose indexed symbols have the same name as a changed symbol.
        if !change.symbols.is_empty() {
            for test in tests {
                if test.path == change.path {
                    continue;
                }
                let mut hits: Vec<&String> = test
                    .index_names
                    .iter()
                    .filter(|name| change.symbols.contains(name))
                    .collect();
                hits.sort();
                hits.dedup();
                if let Some(hit) = hits.first() {
                    push_candidate(
                        &mut candidates,
                        &test.path,
                        Confidence::Medium,
                        format!("index name match: {}::{hit}", change.path),
                    );
                }
            }
        }
    }

    candidates.sort_by(|a, b| a.path.cmp(&b.path));
    let mut notes =
        vec!["selection is a conservative heuristic; unselected tests may still fail".to_string()];
    let fallback = fallback_stage.map(str::to_string);
    if candidates.is_empty() {
        if let Some(stage) = &fallback {
            notes.push(format!(
                "no targeted test was proposed; run the configured '{stage}' verification stage"
            ));
        } else {
            notes.push(
                "no targeted test was proposed and no fallback stage is configured".to_string(),
            );
        }
    }
    TestProposal {
        changed_files,
        candidates,
        unsupported,
        complete: false,
        fallback,
        notes,
    }
}

fn push_candidate(
    candidates: &mut Vec<TestCandidate>,
    path: &str,
    confidence: Confidence,
    reason: String,
) {
    if let Some(existing) = candidates.iter_mut().find(|entry| entry.path == path) {
        existing.merge(confidence, reason);
        return;
    }
    candidates.push(TestCandidate {
        path: path.to_string(),
        confidence,
        reasons: vec![reason],
    });
}

/// Candidate test paths for a source file, following common conventions.
pub fn adjacent_test_paths(path: &str, language: &str) -> Vec<String> {
    let (directory, file_name) = match path.rsplit_once('/') {
        Some((directory, file_name)) => (format!("{directory}/"), file_name.to_string()),
        None => (String::new(), path.to_string()),
    };
    let (stem, extension) = split_name(&file_name);
    let mut out: Vec<String> = Vec::new();
    match language {
        "rust" => {
            if file_name == "mod.rs" {
                // `src/foo/mod.rs` -> `tests/foo.rs`.
                let parent = directory.trim_end_matches('/');
                let parent = parent
                    .rsplit_once('/')
                    .map(|(_, name)| name)
                    .unwrap_or(parent);
                if !parent.is_empty() {
                    out.push(format!("tests/{parent}.rs"));
                    out.push(format!("tests/{parent}/mod.rs"));
                }
            } else {
                out.push(format!("tests/{stem}.rs"));
                out.push(format!("tests/{stem}_tests.rs"));
                out.push(format!("{directory}{stem}_test.rs"));
                out.push(format!("tests/{stem}/mod.rs"));
            }
        }
        "typescript" | "javascript" => {
            for suffix in ["test", "spec"] {
                out.push(format!("{directory}{stem}.{suffix}.{extension}"));
                out.push(format!("{directory}__tests__/{stem}.{suffix}.{extension}"));
                out.push(format!("tests/{stem}.{suffix}.{extension}"));
                out.push(format!("test/{stem}.{suffix}.{extension}"));
            }
        }
        "python" => {
            out.push(format!("{directory}test_{stem}.py"));
            out.push(format!("tests/test_{stem}.py"));
            out.push(format!("test/test_{stem}.py"));
            out.push(format!("tests/{stem}_test.py"));
        }
        _ => {}
    }
    out.sort();
    out.dedup();
    out
}

fn split_name(file_name: &str) -> (String, String) {
    match file_name.rsplit_once('.') {
        Some((stem, extension)) => (stem.to_string(), extension.to_string()),
        None => (file_name.to_string(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(path: &str, language: &str, symbols: &[&str]) -> SourceChange {
        SourceChange {
            path: path.to_string(),
            language: language.to_string(),
            symbols: symbols.iter().map(|name| name.to_string()).collect(),
        }
    }

    fn test_entry(path: &str, language: &str, index_names: &[&str]) -> TestIndexEntry {
        TestIndexEntry {
            path: path.to_string(),
            language: language.to_string(),
            index_names: index_names.iter().map(|name| name.to_string()).collect(),
        }
    }

    #[test]
    fn proposes_adjacent_rust_test() {
        let proposal = propose(
            &[change("src/parser.rs", "rust", &["parse"])],
            &[test_entry("tests/parser.rs", "rust", &[])],
            Some("normal"),
        );
        assert!(proposal.has_candidates());
        assert_eq!(proposal.candidates[0].path, "tests/parser.rs");
        assert!(proposal.candidates[0]
            .reasons
            .iter()
            .any(|reason| reason.starts_with("adjacent to")));
        assert!(!proposal.complete);
    }

    #[test]
    fn proposes_test_by_index_name_match() {
        let proposal = propose(
            &[change("src/engine.rs", "rust", &["Engine"])],
            &[
                test_entry("tests/engine_test.rs", "rust", &["Engine", "run"]),
                test_entry("tests/unrelated.rs", "rust", &["other"]),
            ],
            Some("normal"),
        );
        let paths: Vec<&str> = proposal
            .candidates
            .iter()
            .map(|candidate| candidate.path.as_str())
            .collect();
        assert!(paths.contains(&"tests/engine_test.rs"));
        assert!(!paths.contains(&"tests/unrelated.rs"));
        let candidate = proposal
            .candidates
            .iter()
            .find(|candidate| candidate.path == "tests/engine_test.rs")
            .unwrap();
        assert!(
            candidate
                .reasons
                .iter()
                .any(|reason| reason.contains("index name match")),
            "{:?}",
            candidate.reasons
        );
    }

    #[test]
    fn proposes_typescript_and_python_conventions() {
        let proposal = propose(
            &[
                change("src/util.ts", "typescript", &[]),
                change("pkg/service.py", "python", &[]),
            ],
            &[
                test_entry("src/util.test.ts", "typescript", &[]),
                test_entry("tests/test_service.py", "python", &[]),
            ],
            Some("normal"),
        );
        let paths: Vec<&str> = proposal
            .candidates
            .iter()
            .map(|candidate| candidate.path.as_str())
            .collect();
        assert!(paths.contains(&"src/util.test.ts"));
        assert!(paths.contains(&"tests/test_service.py"));
    }

    #[test]
    fn reports_no_candidates_with_fallback() {
        let proposal = propose(
            &[change("src/parser.rs", "rust", &[])],
            &[test_entry("tests/other.rs", "rust", &[])],
            Some("normal"),
        );
        assert!(!proposal.has_candidates());
        assert_eq!(proposal.fallback.as_deref(), Some("normal"));
        assert!(!proposal.complete);
        assert!(proposal
            .notes
            .iter()
            .any(|note| note.contains("'normal' verification stage")));
    }

    #[test]
    fn reports_unsupported_language_without_guessing() {
        let proposal = propose(
            &[change("src/App.vue", "other", &[])],
            &[test_entry("tests/x.rs", "rust", &[])],
            Some("full"),
        );
        assert!(!proposal.has_candidates());
        assert_eq!(proposal.unsupported, vec!["src/App.vue".to_string()]);
        assert_eq!(proposal.fallback.as_deref(), Some("full"));
    }

    #[test]
    fn multiple_sources_produce_sorted_deduplicated_candidates() {
        let proposal = propose(
            &[
                change("src/a.rs", "rust", &["A"]),
                change("src/b.rs", "rust", &["B"]),
            ],
            &[
                test_entry("tests/b_test.rs", "rust", &["B"]),
                test_entry("tests/a_test.rs", "rust", &["A"]),
            ],
            Some("normal"),
        );
        let paths: Vec<&str> = proposal
            .candidates
            .iter()
            .map(|candidate| candidate.path.as_str())
            .collect();
        assert_eq!(paths, vec!["tests/a_test.rs", "tests/b_test.rs"]);
    }

    #[test]
    fn changed_test_file_is_proposed_directly() {
        let proposal = propose(
            &[change("tests/parser.rs", "rust", &[])],
            &[test_entry("tests/parser.rs", "rust", &[])],
            Some("normal"),
        );
        assert_eq!(proposal.candidates.len(), 1);
        assert_eq!(proposal.candidates[0].confidence, Confidence::High);
        assert!(proposal.candidates[0]
            .reasons
            .iter()
            .any(|reason| reason.contains("changed test file")));
    }
}
