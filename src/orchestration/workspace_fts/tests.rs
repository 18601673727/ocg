//! Tests of the experimental indexed search: tokenization, query parsing,
//! the acceptance scenarios of the experiment, and calls through the tool
//! registry boundary.

use std::path::{Path, PathBuf};

use super::*;
use crate::openai_compatible::CompletedToolCall;
use crate::test_support::{TestRoot, data_directory, temporary_directory, wait};

const SUITE: &str = "ocg-workspace-fts";

/// A Workspace with its own data directory, holding the files written
/// before the test searched them.
pub(super) struct Setup {
    _root_guard: TestRoot,
    _data_guard: TestRoot,
    pub(super) workspace: Workspace,
    pub(super) root: PathBuf,
}

pub(super) fn setup(name: &str, files: &[(&str, &[u8])]) -> Setup {
    let (root_guard, root) = temporary_directory(SUITE, name);
    let (data_guard, directory) = data_directory(SUITE, &format!("{name}-data"));
    for (path, content) in files {
        write_file(&root, path, content);
    }
    let workspace = Workspace::authorize(&root, &directory).expect("authorize");
    Setup {
        _root_guard: root_guard,
        _data_guard: data_guard,
        workspace,
        root,
    }
}

pub(super) fn write_file(root: &Path, path: &str, content: &[u8]) {
    let path = root.join(path);
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("create");
    std::fs::write(path, content).expect("write");
}

/// An index of `setup`, under the default limits.
fn indexed(setup: &Setup) -> WorkspaceFts {
    let fts = WorkspaceFts::new(setup.workspace.clone(), FtsLimits::default());
    fts.build().expect("build");
    fts
}

/// The `(path, line)` pairs of a result, in the order returned.
fn at(value: &Value) -> Vec<(String, u64)> {
    value["results"]
        .as_array()
        .expect("results")
        .iter()
        .map(|hit| {
            (
                hit["path"].as_str().expect("path").to_owned(),
                hit["line"].as_u64().expect("line"),
            )
        })
        .collect()
}

fn tokens(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    tokenize(text, &mut |lower, _, _| found.push(lower.to_owned()));
    found
}

#[test]
fn tokenization_documents_its_rules() {
    assert_eq!(
        tokens("JobScheduler runs job_admission; HTTPServer u64 Mixed"),
        [
            "job",
            "scheduler",
            "runs",
            "job",
            "admission",
            "http",
            "server",
            "u64",
            "mixed"
        ]
    );
    assert_eq!(
        tokens("schedulers and jobless are their own words"),
        [
            "schedulers",
            "and",
            "jobless",
            "are",
            "their",
            "own",
            "words"
        ]
    );
    // Every reported range is the word part the subtoken came from.
    let text = "call JobScheduler now";
    let mut ranges = Vec::new();
    tokenize(text, &mut |lower, start, end| {
        ranges.push((
            lower.to_owned(),
            text[start as usize..end as usize].to_owned(),
        ));
    });
    assert_eq!(
        ranges,
        [
            ("call".to_owned(), "call".to_owned()),
            ("job".to_owned(), "Job".to_owned()),
            ("scheduler".to_owned(), "Scheduler".to_owned()),
            ("now".to_owned(), "now".to_owned()),
        ]
    );
}

#[test]
fn query_parsing_treats_words_as_terms_and_phrases() {
    let parsed = parse("provider cancellation").expect("parse");
    assert_eq!(
        parsed.clauses,
        [
            Clause {
                subtokens: vec!["cancellation".to_owned()]
            },
            Clause {
                subtokens: vec!["provider".to_owned()]
            }
        ]
    );
    assert_eq!(parsed.subtokens, ["cancellation", "provider"]);
    // One word of several parts is a phrase of those parts.
    let parsed = parse("JobScheduler").expect("parse");
    assert_eq!(
        parsed.clauses,
        [Clause {
            subtokens: vec!["job".to_owned(), "scheduler".to_owned()]
        }]
    );
    // A quoted phrase is one clause in word order; words around it are
    // their own clauses.
    let parsed = parse("\"job admission\" scheduler").expect("parse");
    assert_eq!(
        parsed.clauses,
        [
            Clause {
                subtokens: vec!["job".to_owned(), "admission".to_owned()]
            },
            Clause {
                subtokens: vec!["scheduler".to_owned()]
            }
        ]
    );
    // Duplicate words collapse; case does not matter.
    let parsed = parse("Job job JOB").expect("parse");
    assert_eq!(parsed.clauses.len(), 1);
    assert_eq!(parse("").expect_err("empty"), QueryRefusal::Invalid);
    assert_eq!(
        parse("\"unterminated").expect_err("unterminated"),
        QueryRefusal::Invalid
    );
    assert_eq!(
        parse(&"x".repeat(130)).expect_err("oversized"),
        QueryRefusal::OversizedToken
    );
    assert_eq!(
        parse("one two three four five six seven eight nine").expect_err("clauses"),
        QueryRefusal::Invalid
    );
}

/// A. A known term appears in several files: every expected path and line
/// comes back, and nothing else.
#[test]
fn a_single_term_finds_every_expected_file_and_line() {
    let setup = setup(
        "single-term",
        &[
            (
                "src/alpha.rs",
                b"// alpha\n// the provider adapter\nfn a() {}\n",
            ),
            ("src/beta.rs", b"fn b() {}\n// provider\n"),
            (
                "src/gamma.rs",
                b"// no match here\n// PROVIDER and provider\n",
            ),
            ("src/delta.rs", b"// providers are not provider\n"),
        ],
    );
    let fts = indexed(&setup);
    let found = fts.search("provider", 10).expect("search");
    let expected = vec![
        ("src/alpha.rs".to_owned(), 2),
        ("src/beta.rs".to_owned(), 2),
        ("src/delta.rs".to_owned(), 1),
        ("src/gamma.rs".to_owned(), 2),
    ];
    let mut found_at = at(&found);
    found_at.sort();
    assert_eq!(
        found_at, expected,
        "every expected path and line, once per line"
    );
    // Records carry the excerpt, the matched words, and a score.
    let first = &found["results"][0];
    assert!(
        first["excerpt"]
            .as_str()
            .expect("excerpt")
            .contains("provider")
    );
    assert_eq!(first["terms"], json!(["provider"]));
    assert!(first["score"].as_f64().expect("score") > 0.0);
    // Tokenized search is not substring search: a fragment never matches,
    // and the plural is its own word.
    assert_eq!(at(&fts.search("provid", 10).expect("search")), []);
    assert_eq!(
        at(&fts.search("providers", 10).expect("search")),
        [("src/delta.rs".to_owned(), 1)]
    );
    // The snapshot reports its own identity and coverage.
    let index = &found["index"];
    assert_eq!(index["generation"], 1);
    assert_eq!(index["files_indexed"], 4);
    assert_eq!(index["complete"], true);
    assert!(
        index["snapshot"]
            .as_str()
            .expect("snapshot")
            .contains("generation 1")
    );
    assert_eq!(
        fts.coverage().expect("coverage").0,
        1,
        "one build serves every query"
    );
}

/// B. AND semantics and plausible ranking: files with different
/// concentrations of related terms.
#[test]
fn b_multi_term_requires_every_word_and_ranks_by_concentration() {
    let dense = "scheduler runs; executor runs;\n".repeat(20);
    let setup = setup(
        "multi-term",
        &[
            ("zz_dense.rs", dense.as_bytes()),
            ("aa_once.rs", b"// one scheduler executor line\n"),
            (
                "bb_scheduler_only.rs",
                b"// scheduler scheduler scheduler\n",
            ),
            ("cc_executor_only.rs", b"// executor executor\n"),
        ],
    );
    let fts = indexed(&setup);
    let found = fts.search("scheduler executor", 10).expect("search");
    let paths = at(&found)
        .into_iter()
        .map(|(path, _)| path)
        .collect::<Vec<_>>();
    assert!(
        paths.contains(&"zz_dense.rs".to_owned()) && paths.contains(&"aa_once.rs".to_owned()),
        "{paths:?}"
    );
    assert!(
        !paths.iter().any(|path| path.contains("only")),
        "AND semantics: a file with just one word never matches, {paths:?}"
    );
    assert_eq!(
        paths.first().map(String::as_str),
        Some("zz_dense.rs"),
        "the file concentrating both words ranks first, not the first name walked"
    );
    assert_eq!(found["files_matched"], 2);
    assert!(
        found["results"]
            .as_array()
            .expect("results")
            .windows(2)
            .all(|pair| pair[0]["score"].as_f64() >= pair[1]["score"].as_f64()),
        "records are ordered by score"
    );
}

/// B, phrase half: adjacency is required by quotes and by words of several
/// parts, and may span lines.
#[test]
fn b_phrases_need_the_words_adjacent_in_order() {
    let setup = setup(
        "phrases",
        &[
            ("adjacent.rs", b"call job admission now\n"),
            ("across_lines.rs", b"call job\nadmission now\n"),
            ("scattered.rs", b"admission of the job\n"),
            ("identifier.rs", b"pub struct JobAdmission {}\n"),
        ],
    );
    let fts = indexed(&setup);
    let found = fts.search("\"job admission\"", 10).expect("search");
    let mut paths = at(&found)
        .into_iter()
        .map(|(path, _)| path)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    assert_eq!(
        paths,
        [
            "across_lines.rs".to_owned(),
            "adjacent.rs".to_owned(),
            "identifier.rs".to_owned()
        ],
        "the phrase spans lines and matches an identifier's words in a row, but never reverses or scatters"
    );
    // The same phrase as one word of several parts.
    let found = fts.search("JobAdmission", 10).expect("search");
    let mut paths = at(&found)
        .into_iter()
        .map(|(path, _)| path)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    assert_eq!(
        paths,
        [
            "across_lines.rs".to_owned(),
            "adjacent.rs".to_owned(),
            "identifier.rs".to_owned()
        ],
        "an identifier is the same words in a row"
    );
    // Without quotes the words need only both match.
    let found = fts.search("job admission", 10).expect("search");
    assert_eq!(found["files_matched"], 4);
}

/// C. Hidden, protected, binary and symlinked files cannot be retrieved
/// through the index.
#[test]
fn c_excluded_files_are_never_retrieved() {
    let setup = setup(
        "excluded",
        &[
            ("ok.rs", b"zqxneedle allowedpayload\n"),
            (".hidden/secret.txt", b"zqxneedle hiddenpayload\n"),
            ("config/secrets.yaml", b"zqxneedle yamlpayload\n"),
            ("id_rsa", b"zqxneedle keypayload\n"),
            ("cert.pem", b"zqxneedle pempayload\n"),
            ("ocg_db", b"zqxneedle dbpayload\n"),
            ("target/build.rs", b"zqxneedle buildpayload\n"),
            ("node_modules/pkg/index.js", b"zqxneedle modulepayload\n"),
            ("binary.bin", b"\x00zqxneedle binarypayload\n"),
            ("invalid.rs", b"\xffzqxneedle invalidpayload\n"),
        ],
    );
    let huge = format!("zqxneedle hugepayload {}\n", "padding".repeat(60));
    write_file(&setup.root, "huge.rs", huge.as_bytes());
    let (_outside_guard, outside) = temporary_directory(SUITE, "excluded-outside");
    std::fs::write(outside.join("secret.rs"), "zqxneedle outsidepayload\n").expect("write");
    std::os::unix::fs::symlink(outside.join("secret.rs"), setup.root.join("link.rs"))
        .expect("link");
    std::os::unix::fs::symlink(&outside, setup.root.join("linkdir")).expect("link");
    let limits = FtsLimits {
        file_bytes: 256,
        ..FtsLimits::default()
    };
    let fts = WorkspaceFts::new(setup.workspace.clone(), limits);
    fts.build().expect("build");
    let found = fts.search("zqxneedle", 50).expect("search");
    assert_eq!(
        at(&found),
        [("ok.rs".to_owned(), 1)],
        "only the one allowed file is retrievable"
    );
    let shown = found.to_string();
    assert!(shown.contains("allowedpayload"));
    for payload in [
        "hiddenpayload",
        "yamlpayload",
        "keypayload",
        "pempayload",
        "dbpayload",
        "buildpayload",
        "modulepayload",
        "binarypayload",
        "invalidpayload",
        "hugepayload",
        "outsidepayload",
    ] {
        assert!(!shown.contains(payload), "{payload} was disclosed");
    }
    // The coverage reports what was left out, truthfully.
    let index = &found["index"];
    let skipped = &index["skipped"];
    assert!(skipped["protected"].as_u64().expect("protected") >= 5);
    assert!(skipped["symlink"].as_u64().expect("symlink") >= 2);
    assert!(skipped["binary"].as_u64().expect("binary") >= 2);
    assert_eq!(skipped["too_large"], 1);
    assert!(skipped["other"].as_u64().expect("other") >= 2);
    assert_eq!(index["files_indexed"], 1);
}

/// D. An edit after indexing appears only after an explicit rebuild, and
/// the old snapshot is released.
#[test]
fn d_rebuild_sees_edits_and_replaces_the_old_snapshot() {
    let setup = setup("rebuild", &[("src/log.rs", b"alpha marker\n")]);
    let fts = indexed(&setup);
    assert_eq!(
        at(&fts.search("alpha", 10).expect("search")),
        [("src/log.rs".to_owned(), 1)]
    );
    write_file(&setup.root, "src/log.rs", b"beta marker\n");
    // Until the rebuild, the result is honest about being the old snapshot.
    let stale = fts.search("alpha", 10).expect("search");
    assert_eq!(at(&stale), [("src/log.rs".to_owned(), 1)]);
    assert_eq!(stale["index"]["generation"], 1);
    assert!(
        stale["index"]["snapshot"]
            .as_str()
            .expect("snapshot")
            .contains("not necessarily the files as they are now")
    );
    assert_eq!(
        at(&fts.search("beta", 10).expect("search")),
        [],
        "the edit is not claimed before a rebuild"
    );
    let report = fts.rebuild().expect("rebuild");
    assert_eq!(report.generation, 2);
    assert_eq!(
        at(&fts.search("beta", 10).expect("search")),
        [("src/log.rs".to_owned(), 1)]
    );
    assert_eq!(at(&fts.search("alpha", 10).expect("search")), []);
    assert_eq!(fts.coverage().expect("coverage").0, 2);
    // Disposing releases the snapshot and its resources.
    fts.dispose();
    assert!(fts.coverage().is_none());
    assert_eq!(
        fts.search("beta", 10).expect_err("gone"),
        SearchRefusal::Disposed
    );
}

/// E. A Workspace larger than the indexing budget gives a bounded, truthful
/// partial index.
#[test]
fn e_a_workspace_beyond_the_budget_indexes_part_of_it_truthfully() {
    let files = (1..=20)
        .map(|number| {
            (
                format!("src/file{number:02}.rs"),
                format!("// budgetneedle in file {number:02}\nfn f{number}() {{}}\n"),
            )
        })
        .collect::<Vec<_>>();
    let borrowed = files
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_bytes()))
        .collect::<Vec<_>>();
    let files_setup = setup("budget-files", &borrowed);
    let fts = WorkspaceFts::new(
        files_setup.workspace.clone(),
        FtsLimits {
            files: 5,
            ..FtsLimits::default()
        },
    );
    let report = fts.build().expect("build");
    assert_eq!(report.coverage.files_indexed, 5);
    assert!(!report.coverage.complete);
    assert_eq!(report.coverage.limit, Some(BuildLimit::Files));
    let found = fts.search("budgetneedle", 50).expect("search");
    assert_eq!(at(&found).len(), 5);
    assert_eq!(found["index"]["files_indexed"], 5);
    assert_eq!(found["index"]["complete"], false);
    assert_eq!(found["index"]["limit"], "files");

    // The same truthfulness for the content budget.
    let bytes_setup = setup("budget-bytes", &borrowed);
    let fts = WorkspaceFts::new(
        bytes_setup.workspace.clone(),
        FtsLimits {
            content_bytes: 500,
            ..FtsLimits::default()
        },
    );
    let report = fts.build().expect("build");
    assert!(report.coverage.content_bytes_indexed <= 500);
    assert!(!report.coverage.complete);
    assert_eq!(report.coverage.limit, Some(BuildLimit::ContentBytes));
}

#[test]
fn a_directory_entry_cap_indexes_admitted_names_and_reports_the_omission() {
    let setup = setup(
        "directory-entry-cap",
        &[
            ("a.rs", b"capneedle\n"),
            ("b.rs", b"capneedle\n"),
            ("c.rs", b"capneedle\n"),
            ("d.rs", b"capneedle\n"),
            ("e.rs", b"capneedle\n"),
            ("f.rs", b"capneedle\n"),
        ],
    );
    let root = setup.workspace.open_root().expect("root");
    let (admitted, _, _) = names(&root, 5);
    assert_eq!(admitted.len(), 5);
    let fts = WorkspaceFts::new(
        setup.workspace.clone(),
        FtsLimits {
            directory_entries: 5,
            ..FtsLimits::default()
        },
    );
    let report = fts.build().expect("partial build");
    assert_eq!(report.coverage.entries_examined, 5);
    assert_eq!(report.coverage.files_indexed, 5);
    assert!(!report.coverage.complete);
    assert_eq!(report.coverage.limit, Some(BuildLimit::Entries));
    let found = fts.search("capneedle", 10).expect("search");
    let mut indexed_paths = at(&found)
        .into_iter()
        .map(|(path, _)| path)
        .collect::<Vec<_>>();
    indexed_paths.sort();
    assert_eq!(indexed_paths, admitted, "only admitted entries are indexed");
    assert_eq!(found["index"]["complete"], false);
    assert_eq!(found["index"]["limit"], "entries");
    let file = std::fs::File::open(setup.root.join("a.rs")).expect("regular file");
    let (admitted, refused, omission) = names(&file, 5);
    assert!(admitted.is_empty());
    assert_eq!(refused, 0, "read failures are not protected entries");
    assert_eq!(omission, Some(BuildLimit::Unreadable));
}

/// One snapshot serves many queries, and a call never rebuilds unless asked.
#[test]
fn one_snapshot_serves_many_queries() {
    let setup = setup(
        "reuse",
        &[
            ("src/one.rs", b"// tool evidence accrues\n"),
            (
                "src/two.rs",
                b"// tool evidence again, twice tool evidence\n",
            ),
        ],
    );
    let fts = WorkspaceFts::new(setup.workspace.clone(), FtsLimits::default());
    assert_eq!(
        fts.search("tool", 10).expect_err("nothing indexed"),
        SearchRefusal::NoIndex
    );
    let first = fts.build().expect("build");
    for query in ["tool", "evidence", "\"tool evidence\"", "TOOL EVIDENCE"] {
        fts.search(query, 10).expect("search");
    }
    let (generation, coverage, _) = fts.coverage().expect("coverage");
    assert_eq!(generation, first.generation);
    assert_eq!(coverage.files_indexed, 2);
}

/// The tool runs through the registry boundary: registered and unauthorized
/// by default, granted only explicitly, and its schema refuses what it does
/// not define.
#[test]
fn the_registered_tool_runs_calls_through_the_registry_boundary() {
    let setup = setup("registry", &[("src/lib.rs", b"// the provider adapter\n")]);
    let registry = ToolRegistry::default();
    assert!(
        registry.offer(&[TOOL_NAME]).is_err(),
        "unregistered by default"
    );
    register(&registry, setup.workspace.clone()).expect("register");
    assert!(
        registry.offer(&[TOOL_NAME]).is_err(),
        "registering authorizes nothing"
    );
    registry.authorize(TOOL_NAME).expect("authorize");
    let offer = registry.offer(&[TOOL_NAME]).expect("offer");
    let call = |arguments: Value| CompletedToolCall {
        index: 0,
        id: "call-1".to_owned(),
        name: TOOL_NAME.to_owned(),
        arguments: arguments.to_string(),
    };
    let dispatch = offer
        .resolve(&call(json!({"query": "provider"})))
        .expect("resolve");
    assert_eq!(dispatch.version, "workspace_fts_search-2");
    assert_eq!(dispatch.effect, ToolEffect::ReadOnly);
    let result = wait(dispatch.handler.call(dispatch.arguments.clone())).expect("call");
    assert_eq!(at(&result), [("src/lib.rs".to_owned(), 1)]);
    assert_eq!(result["index"]["generation"], 1);
    for refused in [
        json!({"query": "provider", "unknown": true}),
        json!({"query": ""}),
        json!({"query": "provider", "limit": 0}),
        json!({"query": "provider", "limit": 51}),
        json!({"query": "provider", "refresh": "yes"}),
    ] {
        assert!(
            offer.resolve(&call(refused)).is_err(),
            "the schema or the handler refuses"
        );
    }
    // A refresh call rebuilds before it searches.
    write_file(
        &setup.root,
        "src/lib.rs",
        b"// the provider adapter, edited\n",
    );
    let dispatch = offer
        .resolve(&call(json!({"query": "edited", "refresh": true})))
        .expect("resolve");
    let refreshed = wait(dispatch.handler.call(dispatch.arguments.clone())).expect("call");
    assert_eq!(at(&refreshed), [("src/lib.rs".to_owned(), 1)]);
    assert_eq!(refreshed["index"]["generation"], 2);
}

pub(super) type Probe = Arc<dyn Fn(&str, u64) + Send + Sync>;

#[test]
fn every_required_clause_must_match_before_any_line_is_returned() {
    let setup = setup(
        "all-clauses",
        &[
            ("bad.rs", b"alpha separate beta\nother token\n"),
            ("good.rs", b"alpha beta\nother token\n"),
        ],
    );
    let fts = indexed(&setup);
    for query in [
        "\"alpha beta\" other",
        "\"alpha beta\" \"other token\"",
        "alphaBeta other",
    ] {
        let result = fts.search(query, 50).expect("search");
        assert!(at(&result).iter().all(|(path, _)| path == "good.rs"));
        assert_eq!(result["files_matched"], 1);
    }
    assert!(
        at(&fts
            .search("\"alpha beta\" \"token other\"", 50)
            .expect("search"))
        .is_empty()
    );
}

#[test]
fn every_output_limit_is_truthful_and_serialized_bytes_are_enforced() {
    let text = "needle\n".repeat(6);
    let setup = setup(
        "output-limits",
        &[("a.rs", text.as_bytes()), ("b.rs", b"needle\n")],
    );
    let fts = indexed(&setup);
    let result = fts.search("needle", 50).expect("search");
    assert_eq!(result["complete"], false);
    assert_eq!(result["limit"], "matches_per_file");
    let result = fts.search("needle", 1).expect("search");
    assert_eq!(result["complete"], false);
    assert_eq!(result["limit"], "results");
    let limited = WorkspaceFts::new(
        setup.workspace.clone(),
        FtsLimits {
            query_candidates: 1,
            ..FtsLimits::default()
        },
    );
    limited.build().expect("build");
    let result = limited.search("needle", 50).expect("search");
    assert_eq!(result["complete"], false);
    assert_eq!(result["limit"], "candidates");
    let sized = WorkspaceFts::new(
        setup.workspace.clone(),
        FtsLimits {
            result_bytes: 1500,
            ..FtsLimits::default()
        },
    );
    sized.build().expect("build");
    let result = sized.search("needle", 50).expect("search");
    assert!(result.to_string().len() <= 1500);
    assert_eq!(result["complete"], false);
    let tiny = WorkspaceFts::new(
        setup.workspace.clone(),
        FtsLimits {
            result_bytes: 1,
            ..FtsLimits::default()
        },
    );
    tiny.build().expect("build");
    assert_eq!(
        tiny.search("needle", 50).expect_err("envelope cannot fit"),
        SearchRefusal::ResultBytes
    );
}

#[test]
fn late_unicode_match_offsets_locate_the_actual_excerpt() {
    let text = format!("{} needle {}\n", "界".repeat(400), "後".repeat(100));
    let setup = setup("late-excerpt", &[("a.rs", text.as_bytes())]);
    let fts = indexed(&setup);
    let result = fts.search("needle", 10).expect("search");
    let hit = &result["results"][0];
    let excerpt = hit["excerpt"].as_str().expect("excerpt");
    let column = hit["offset"].as_u64().expect("offset") as usize;
    let window = hit["excerpt_offset"].as_u64().expect("window") as usize;
    assert!(excerpt.len() <= 200);
    assert_eq!(&text[window..window + excerpt.len()], excerpt);
    assert!(excerpt[column - window..].starts_with("needle"));
    assert_eq!(hit["excerpt_truncated"], true);
    assert_eq!(hit["line"], 1);
}

fn paused_refresh(
    fts: &mut WorkspaceFts,
    point: &'static str,
    generation: u64,
) -> (flume::Receiver<()>, flume::Sender<()>) {
    let (reached, reached_rx) = flume::bounded(1);
    let (release, release_rx) = flume::bounded(1);
    fts.probe = Some(Arc::new(move |at, ticket| {
        if at == point && ticket == generation {
            reached.send(()).expect("report checkpoint");
            release_rx
                .recv_timeout(crate::test_support::WAIT)
                .expect("release checkpoint");
        }
    }));
    (reached_rx, release)
}

#[test]
fn concurrent_refresh_keeps_old_readers_and_releases_replaced_snapshots() {
    let setup = setup("concurrent-refresh", &[("a.rs", b"oldneedle\n")]);
    let mut fts = indexed(&setup);
    let old = fts.lock().index.clone().expect("old snapshot");
    let released = Arc::downgrade(&old);
    let (reached, release) = paused_refresh(&mut fts, "before_publish", 2);
    let fts = Arc::new(fts);
    write_file(&setup.root, "a.rs", b"newneedle\n");
    let worker = {
        let fts = fts.clone();
        thread::spawn(move || fts.rebuild())
    };
    reached
        .recv_timeout(crate::test_support::WAIT)
        .expect("refresh paused");
    let value = fts
        .search("oldneedle", 10)
        .expect("old snapshot still searchable");
    assert_eq!(value["index"]["generation"], 1);
    assert_eq!(value["index"]["lifecycle"], "refreshing");
    assert_eq!(at(&value).len(), 1);
    release.send(()).expect("release");
    worker.join().expect("join").expect("refresh");
    assert_eq!(
        at(&fts.search("newneedle", 10).expect("new snapshot")).len(),
        1
    );
    assert!(
        released.upgrade().is_some(),
        "old reader retains immutable snapshot"
    );
    drop(old);
    assert!(released.upgrade().is_none());
    let latest = Arc::downgrade(fts.lock().index.as_ref().expect("latest"));
    fts.dispose();
    assert!(latest.upgrade().is_none());
}

#[test]
fn obsolete_and_disposed_builds_cannot_publish() {
    for dispose in [false, true] {
        let setup = setup(
            if dispose {
                "dispose-inflight"
            } else {
                "superseded-build"
            },
            &[("a.rs", b"firstneedle\n")],
        );
        let mut fts = indexed(&setup);
        let (reached, release) = paused_refresh(&mut fts, "before_publish", 2);
        let fts = Arc::new(fts);
        let worker = {
            let fts = fts.clone();
            thread::spawn(move || fts.rebuild())
        };
        reached
            .recv_timeout(crate::test_support::WAIT)
            .expect("paused");
        if dispose {
            fts.dispose();
        } else {
            write_file(&setup.root, "a.rs", b"latestneedle\n");
            assert_eq!(fts.rebuild().expect("newer build").generation, 3);
        }
        release.send(()).expect("release");
        assert!(worker.join().expect("join").is_err());
        if dispose {
            assert!(fts.coverage().is_none());
            assert_eq!(
                fts.build().expect_err("permanently disposed"),
                SearchRefusal::Disposed
            );
        } else {
            assert_eq!(fts.coverage().expect("snapshot").0, 3);
            assert_eq!(
                at(&fts.search("latestneedle", 10).expect("latest")).len(),
                1
            );
        }
    }
}

#[test]
fn cancellation_during_initial_build_and_refresh_never_publishes() {
    for refreshing in [false, true] {
        let setup = setup(
            if refreshing {
                "cancel-refresh"
            } else {
                "cancel-build"
            },
            &[("a.rs", b"needle\n")],
        );
        let mut fts = WorkspaceFts::new(setup.workspace.clone(), FtsLimits::default());
        if refreshing {
            fts.build().expect("initial");
        }
        let expected = if refreshing { 2 } else { 1 };
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        fts.probe = Some(Arc::new(move |point, _| {
            if point == "before_publish" {
                stopped.store(true, Ordering::Relaxed);
            }
        }));
        assert_eq!(
            fts.build_with(&stop, Instant::now() + Duration::from_secs(10))
                .expect_err("cancelled"),
            SearchRefusal::Stopped
        );
        assert_eq!(
            fts.coverage().map(|coverage| coverage.0),
            refreshing.then_some(expected - 1)
        );
        if refreshing {
            assert_eq!(
                fts.search("needle", 10).expect("previous")["index"]["freshness"],
                "known_dirty"
            );
        }
    }
}

#[test]
fn failed_refresh_keeps_previous_snapshot_and_reports_uncertainty() {
    let setup = setup("failed-refresh", &[("a.rs", b"needle\n")]);
    let fts = indexed(&setup);
    let moved = setup.root.with_extension("moved");
    std::fs::rename(&setup.root, &moved).expect("move root");
    std::fs::create_dir(&setup.root).expect("replacement root");
    assert!(matches!(
        fts.rebuild(),
        Err(SearchRefusal::Access(Refusal::WorkspaceChanged))
    ));
    assert!(
        fts.search("needle", 10).is_err(),
        "cached contents do not bypass root identity"
    );
    std::fs::remove_dir(&setup.root).expect("remove replacement");
    std::fs::rename(moved, &setup.root).expect("restore authorized root");
    let result = fts
        .search("needle", 10)
        .expect("previous snapshot retained");
    assert_eq!(result["index"]["generation"], 1);
    assert_eq!(result["index"]["freshness"], "known_dirty");
}

#[test]
fn writes_during_build_cannot_be_cleared_by_publication() {
    let setup = setup("write-during-build", &[("a.rs", b"before\n")]);
    let mut fts = indexed(&setup);
    let root = setup.root.clone();
    fts.probe = Some(Arc::new(move |point, _| {
        if point == "before_publish" {
            let _mutation = super::super::workspace_read::WorkspaceMutation::begin();
            write_file(&root, "a.rs", b"after\n");
        }
    }));
    fts.rebuild().expect("bounded snapshot published");
    let value = fts.search("before", 10).expect("snapshot");
    assert_eq!(value["index"]["freshness"], "known_dirty");
    assert_eq!(value["index"]["external_modifications"], "unverified");
    fts.probe = None;
    fts.rebuild().expect("refresh after write");
    assert_eq!(at(&fts.search("after", 10).expect("refreshed")).len(), 1);
}

#[test]
fn active_mutation_cannot_establish_freshness_and_cross_root_commands_are_conservative() {
    let setup = setup("active-mutation", &[("a.rs", b"needle\n")]);
    let mutation = super::super::workspace_read::WorkspaceMutation::begin();
    let fts = indexed(&setup);
    assert_eq!(
        fts.search("needle", 10).expect("snapshot")["index"]["freshness"],
        "known_dirty"
    );
    drop(mutation);
    assert_eq!(
        fts.search("needle", 10).expect("snapshot")["index"]["freshness"],
        "known_dirty"
    );
}

#[test]
fn actual_write_tool_invalidates_and_refresh_observes_create_update_delete() {
    use super::super::workspace_write;
    let setup = setup("write-tool-invalidates", &[("old.rs", b"oldneedle\n")]);
    let fts = indexed(&setup);
    let registry = ToolRegistry::default();
    workspace_write::register(&registry, setup.workspace.clone()).expect("register");
    registry
        .authorize(workspace_write::TOOL_NAME)
        .expect("authorize");
    let offer = registry
        .offer(&[workspace_write::TOOL_NAME])
        .expect("offer");
    for arguments in [
        json!({"operation":"create","path":"new.rs","content":"newneedle\n"}),
        json!({"operation":"update","path":"old.rs","expected_content":"oldneedle\n","content":"changedneedle\n"}),
    ] {
        let call = CompletedToolCall {
            index: 0,
            id: "write".into(),
            name: workspace_write::TOOL_NAME.into(),
            arguments: arguments.to_string(),
        };
        let dispatch = offer.resolve(&call).expect("resolve write");
        wait(dispatch.handler.call(dispatch.arguments)).expect("write");
        assert_eq!(
            fts.search("oldneedle", 10).expect("stale snapshot")["index"]["freshness"],
            "known_dirty"
        );
    }
    {
        let _mutation = super::super::workspace_read::WorkspaceMutation::begin();
        std::fs::remove_file(setup.root.join("old.rs")).expect("delete");
    }
    fts.rebuild().expect("refresh");
    assert!(at(&fts.search("oldneedle", 10).expect("deleted")).is_empty());
    assert!(at(&fts.search("changedneedle", 10).expect("deleted")).is_empty());
    assert_eq!(at(&fts.search("newneedle", 10).expect("created")).len(), 1);
}

#[test]
fn dropped_handler_future_stops_and_joins_its_worker() {
    for refreshing in [false, true] {
        let setup = setup("joined-worker", &[("a.rs", b"needle\n")]);
        let mut fts = WorkspaceFts::new(setup.workspace.clone(), FtsLimits::default());
        if refreshing {
            fts.build().expect("initial snapshot");
        }
        let (reached, reached_rx) = flume::bounded(1);
        let (release, release_rx) = flume::bounded(1);
        fts.probe = Some(Arc::new(move |point, _| {
            if point == "build_started" {
                reached.send(()).expect("started");
                release_rx
                    .recv_timeout(crate::test_support::WAIT)
                    .expect("cancellation releases worker");
            } else if point == "worker_stopped" {
                release.send(()).expect("release");
            }
        }));
        let fts = Arc::new(fts);
        let handler = FtsSearch { fts: fts.clone() };
        let mut future = handler.call(json!({"query":"needle", "refresh":refreshing}));
        assert!(crate::test_support::poll_once(future.as_mut()).is_none());
        reached_rx
            .recv_timeout(crate::test_support::WAIT)
            .expect("worker began");
        drop(future);
        assert_eq!(
            fts.coverage().map(|coverage| coverage.0),
            refreshing.then_some(1)
        );
        assert!(fts.lock().building.is_none());
        assert_eq!(
            Arc::strong_count(&fts),
            2,
            "no detached worker retains its owner"
        );
    }
}

#[test]
fn resource_bounds_and_authorized_roots_are_enforced() {
    let setup = setup(
        "resource-and-root",
        &[
            ("a.rs", b"needle needle needle\n"),
            ("nested/b.rs", b"needle\n"),
        ],
    );
    std::fs::hard_link(setup.root.join("a.rs"), setup.root.join("hard.rs")).expect("hard link");
    let fts = indexed(&setup);
    assert_eq!(fts.coverage().expect("coverage").1.skipped.hard_linked, 2);
    let other = self::setup("other-authorized-root", &[("private.rs", b"privateonly\n")]);
    let other_fts = indexed(&other);
    assert!(at(&fts.search("privateonly", 10).expect("isolated")).is_empty());
    assert!(at(&other_fts.search("needle", 10).expect("isolated")).is_empty());
    for (limits, reason) in [
        (
            FtsLimits {
                tokens: 0,
                ..FtsLimits::default()
            },
            BuildLimit::Tokens,
        ),
        (
            FtsLimits {
                depth: 0,
                ..FtsLimits::default()
            },
            BuildLimit::Depth,
        ),
        (
            FtsLimits {
                time: Duration::ZERO,
                ..FtsLimits::default()
            },
            BuildLimit::Time,
        ),
        (
            FtsLimits {
                directory_entries: 0,
                ..FtsLimits::default()
            },
            BuildLimit::Entries,
        ),
    ] {
        let bounded = WorkspaceFts::new(other.workspace.clone(), limits);
        if reason == BuildLimit::Depth {
            write_file(&other.root, "nested/file.rs", b"needle\n");
        }
        let coverage = bounded.build().expect("partial build").coverage;
        assert!(!coverage.complete);
        assert_eq!(coverage.limit, Some(reason));
    }
    let timed = WorkspaceFts::new(
        other.workspace.clone(),
        FtsLimits {
            query_time: Duration::ZERO,
            ..FtsLimits::default()
        },
    );
    timed.build().expect("build");
    assert_eq!(
        timed.search("needle", 10).expect_err("query deadline"),
        SearchRefusal::QueryTime
    );
}

#[test]
fn a_data_directory_moved_under_the_workspace_is_never_indexed() {
    let (root_guard, root) = temporary_directory(SUITE, "moved-data-root");
    let (data_guard, directory) = data_directory(SUITE, "moved-data");
    let workspace = Workspace::authorize(&root, &directory).expect("authorize");
    let original = directory.root().to_path_buf();
    write_file(&original, "payload.rs", b"privatedataneedle");
    std::fs::rename(&original, root.join("ordinary")).expect("move data under root");
    let fts = WorkspaceFts::new(workspace, FtsLimits::default());
    let coverage = fts.build().expect("build without data").coverage;
    assert_eq!(coverage.files_indexed, 0);
    let result = fts.search("privatedataneedle", 10).expect("search");
    assert!(at(&result).is_empty());
    assert!(!result.to_string().contains("payload.rs"));
    std::fs::rename(root.join("ordinary"), original).expect("restore data");
    drop((root_guard, data_guard));
}

#[test]
fn cancellation_during_file_admission_discards_initial_and_replacement_builds() {
    for initial in [true, false] {
        let setup = setup(
            if initial {
                "admission-initial"
            } else {
                "admission-refresh"
            },
            &[("a.rs", b"needle\n")],
        );
        let mut fts = WorkspaceFts::new(setup.workspace.clone(), FtsLimits::default());
        if !initial {
            fts.build().expect("initial");
        }
        let stop = Arc::new(AtomicBool::new(false));
        let cancelled = stop.clone();
        fts.probe = Some(Arc::new(move |point, _| {
            if point == "file_read" {
                cancelled.store(true, Ordering::Relaxed);
            }
        }));
        assert_eq!(
            fts.build_with(&stop, Instant::now() + Duration::from_secs(10))
                .expect_err("cancelled admission"),
            SearchRefusal::Stopped
        );
        assert_eq!(
            fts.coverage().map(|coverage| coverage.0),
            (!initial).then_some(1)
        );
    }
}

#[test]
fn aggregate_content_budget_uses_securely_read_bytes_after_stat_changes() {
    let setup = setup("post-read-budget", &[("a.rs", b"small")]);
    let mut fts = WorkspaceFts::new(
        setup.workspace.clone(),
        FtsLimits {
            content_bytes: 10,
            ..FtsLimits::default()
        },
    );
    let root = setup.root.clone();
    fts.probe = Some(Arc::new(move |point, _| {
        if point == "before_read" {
            write_file(&root, "a.rs", b"larger content after stat");
        }
    }));
    let report = fts.build().expect("truthful partial");
    assert_eq!(report.coverage.limit, Some(BuildLimit::ContentBytes));
    assert_eq!(report.coverage.content_bytes_indexed, 0);
    assert_eq!(report.coverage.files_indexed, 0);
}

#[test]
fn excluded_binary_content_does_not_consume_the_snapshot_budget() {
    let setup = setup(
        "binary-content-budget",
        &[("a.bin", b"\0padding"), ("b.rs", b"needle")],
    );
    let fts = WorkspaceFts::new(
        setup.workspace.clone(),
        FtsLimits {
            content_bytes: 6,
            ..FtsLimits::default()
        },
    );
    let coverage = fts.build().expect("build").coverage;
    assert!(coverage.complete);
    assert_eq!(coverage.content_bytes_indexed, 6);
    assert_eq!(coverage.skipped.binary, 1);
    assert_eq!(at(&fts.search("needle", 10).expect("search")).len(), 1);
}

#[test]
fn an_inflight_query_retains_its_snapshot_across_refresh_and_disposal() {
    for dispose in [false, true] {
        let setup = setup("reader-lifecycle", &[("a.rs", b"oldneedle\n")]);
        let mut fts = indexed(&setup);
        let old = Arc::downgrade(fts.lock().index.as_ref().expect("snapshot"));
        let (reached, release) = paused_refresh(&mut fts, "query_started", 1);
        let fts = Arc::new(fts);
        let reader = {
            let fts = fts.clone();
            thread::spawn(move || fts.search("oldneedle", 10))
        };
        reached
            .recv_timeout(crate::test_support::WAIT)
            .expect("reader paused");
        if dispose {
            fts.dispose();
        } else {
            write_file(&setup.root, "a.rs", b"newneedle\n");
            fts.rebuild().expect("replacement");
        }
        assert!(old.upgrade().is_some(), "inflight reader owns old snapshot");
        release.send(()).expect("release reader");
        let result = reader
            .join()
            .expect("reader joined")
            .expect("reader finishes");
        assert_eq!(result["index"]["generation"], 1);
        assert_eq!(at(&result), [("a.rs".to_owned(), 1)]);
        assert!(old.upgrade().is_none(), "last reader releases snapshot");
        if dispose {
            assert_eq!(fts.search("oldneedle", 10), Err(SearchRefusal::Disposed));
        } else {
            assert_eq!(fts.coverage().expect("replacement").0, 2);
        }
    }
}

#[test]
fn sparse_phrase_verification_agrees_with_exhaustive_token_streams() {
    let files: &[(&str, &[u8])] = &[
        ("a.rs", b"alpha unrelated gamma\nbeta alpha beta\n"),
        ("b.rs", b"alpha gamma\nbeta beta\n"),
        (
            "c.rs",
            "HTTPServer snake_case café École 東京\nalpha\nbeta gamma\n".as_bytes(),
        ),
        ("d.rs", b"beta alpha alpha gamma beta\n"),
        (
            "e.rs",
            "α CAFÉ\nÉCOLE café école\nbeta beta beta\n".as_bytes(),
        ),
    ];
    let setup = setup("sparse-oracle", files);
    let fts = indexed(&setup);
    let index = fts.lock().index.clone().expect("snapshot");
    let pieces = [
        "alpha",
        "beta",
        "gamma",
        "\"alpha gamma\"",
        "\"alpha beta\"",
        "\"beta beta\"",
        "\"beta beta beta\"",
        "\"CAFÉ ÉCOLE\"",
        "missing",
        "HTTPServer",
        "snake_case",
        "CAFÉ",
        "École",
        "東京",
    ];
    for first in pieces {
        for second in pieces {
            let query = format!("{first} {second}");
            let parsed = parse(&query).expect("query");
            let stop = AtomicBool::new(false);
            let deadline = Instant::now() + Duration::from_secs(10);
            for document in &index.documents {
                let mut full = Vec::new();
                tokenize(&document.text, &mut |lower, start, _| {
                    full.push((lower.to_owned(), start));
                });
                let exhaustive = Scanned {
                    tokens: full
                        .iter()
                        .enumerate()
                        .map(|(position, (lower, start))| ScannedToken {
                            lower,
                            start: *start,
                            line: line_of(&document.line_starts, *start),
                            position: position as u32,
                        })
                        .collect(),
                };
                let sparse = scan_document(document, &parsed, &stop, deadline).expect("scan");
                assert_eq!(
                    matching_lines(document, &sparse, &parsed, &stop, deadline)
                        .expect("sparse lines"),
                    matching_lines(document, &exhaustive, &parsed, &stop, deadline)
                        .expect("exhaustive lines"),
                    "exact line counts, terms and byte offsets: {query} in {}",
                    document.path,
                );
            }
            let mut expected = files
                .iter()
                .filter_map(|(path, text)| {
                    let tokens = tokens(std::str::from_utf8(text).expect("UTF8"));
                    parsed
                        .clauses
                        .iter()
                        .all(|clause| {
                            tokens
                                .windows(clause.subtokens.len())
                                .any(|window| window == clause.subtokens)
                        })
                        .then_some((*path).to_owned())
                })
                .collect::<Vec<_>>();
            let result = fts.search(&query, 50).expect("search");
            let mut actual = at(&result)
                .into_iter()
                .map(|(path, _)| path)
                .collect::<Vec<_>>();
            actual.sort();
            actual.dedup();
            expected.sort();
            assert_eq!(actual, expected, "{query}");
        }
    }
}

#[test]
fn invalidation_after_build_reservation_is_not_cleared_even_before_the_walk() {
    let setup = setup("reservation-write", &[("a.rs", b"before\n")]);
    let mut fts = WorkspaceFts::new(setup.workspace.clone(), FtsLimits::default());
    fts.probe = Some(Arc::new(|point, _| {
        if point == "build_started" {
            drop(super::super::workspace_read::WorkspaceMutation::begin());
        }
    }));
    fts.build().expect("snapshot");
    assert_eq!(
        fts.search("before", 10).expect("search")["index"]["freshness"],
        "known_dirty"
    );
}

#[test]
fn excerpt_keeps_a_maximum_length_token_after_multibyte_padding() {
    let term = "x".repeat(MAX_TOKEN_BYTES);
    let text = format!("{} {term} tail\n", "界".repeat(400));
    let setup = setup("max-token-excerpt", &[("a.rs", text.as_bytes())]);
    let result = indexed(&setup).search(&term, 10).expect("search");
    let hit = &result["results"][0];
    let excerpt = hit["excerpt"].as_str().expect("excerpt");
    let start =
        hit["offset"].as_u64().expect("offset") - hit["excerpt_offset"].as_u64().expect("window");
    assert!(excerpt.len() <= MAX_EXCERPT_BYTES);
    assert!(excerpt[start as usize..].starts_with(&term));
}

#[test]
fn phrase_scan_and_matching_respect_cancellation_and_deadlines() {
    let setup = setup(
        "phrase-query-stop",
        &[("a.rs", b"alpha gap beta alpha\nbeta beta\n")],
    );
    let fts = indexed(&setup);
    let index = fts.lock().index.clone().expect("snapshot");
    let document = &index.documents[0];
    let query = parse("\"alpha beta\"").expect("query");
    let stop = AtomicBool::new(false);
    let future = Instant::now() + Duration::from_secs(10);
    let scanned = scan_document(document, &query, &stop, future).expect("scan");
    for (cancelled, deadline, expected) in [
        (true, future, SearchRefusal::Stopped),
        (false, Instant::now(), SearchRefusal::QueryTime),
    ] {
        stop.store(cancelled, Ordering::Relaxed);
        assert!(
            matches!(scan_document(document, &query, &stop, deadline), Err(reason) if reason == expected)
        );
        assert_eq!(
            matching_lines(document, &scanned, &query, &stop, deadline),
            Err(expected)
        );
    }
}
