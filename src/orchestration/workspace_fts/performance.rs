use super::tests::{setup, write_file};
use super::*;
use crate::openai_compatible::CompletedToolCall;
use crate::test_support::wait;

fn measure(runs: usize, mut run: impl FnMut()) -> Value {
    for _ in 0..3 {
        run();
    }
    let mut samples = Vec::new();
    for _ in 0..runs {
        let start = Instant::now();
        run();
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(f64::total_cmp);
    json!({"min_ms": samples[0], "median_ms": samples[runs/2], "p95_ms": samples[(runs*95).div_ceil(100)-1]})
}

// The same deterministic source distribution used for selection; no source
// excerpts or workspace paths are printed by the measurements.
#[test]
#[ignore = "release-mode bounded search performance measurement"]
fn production_search_measurements() {
    if cfg!(debug_assertions) {
        panic!("release measurements only");
    }
    let setup = setup("production-performance", &[]);
    let real_root = std::env::var_os("OCG_FTS_REAL_ROOT");
    for file in 0..if real_root.is_some() { 0 } else { 4000 } {
        let target = [512, 2048, 4096, 9216][file % 4];
        let mut content = String::new();
        if file == 0 {
            content.push_str("// rarebeacon alpha beta HTTPServer\n");
        }
        let mut line = 0;
        while content.len() < target {
            let identifier = format!("itemcode{file:05x}part{line:05x}");
            if file % 2 == 0 {
                content.push_str(&format!("pub fn {identifier}(state: &mut State) -> Result<u64, Error> {{ let packet = state.apply(valuecode{file:05x}{line:05x})?; Ok(packet) }}\n"));
            } else {
                content.push_str(&format!("export async function {identifier}(state: State): Promise<number> {{ const packet = await state.apply(valuecode{file:05x}{line:05x}); return packet; }}\n"));
            }
            line += 1;
        }
        let extension = if file % 2 == 0 { "rs" } else { "ts" };
        write_file(
            &setup.root,
            &format!("src/group{:03}/nested/unit{file:05}.{extension}", file / 40),
            content.as_bytes(),
        );
    }
    let (_real_data_guard, real_data) =
        crate::test_support::data_directory("ocg-workspace-fts", "real-performance-data");
    let workspace = real_root.as_ref().map_or_else(
        || setup.workspace.clone(),
        |root| {
            Workspace::authorize(std::path::Path::new(root), &real_data).expect("real workspace")
        },
    );
    let queries = if real_root.is_some() {
        [
            ("rare", "statvfs"),
            ("missing", "neverpresenttoken"),
            ("selective_phrase", "\"authorized workspace\""),
            ("broad_phrase_zero", "\"workspace tool rustix\""),
            ("broad_phrase_matching", "\"workspace tool\""),
            ("scattered_phrase_zero", "\"workspace rustix\""),
            ("and", "rustix flume"),
            ("and_complete", "statvfs rustix"),
        ]
    } else {
        [
            ("rare", "rarebeacon"),
            ("missing", "neverpresenttoken"),
            ("selective_phrase", "\"alpha beta\""),
            ("broad_phrase_zero", "\"packet state error\""),
            ("broad_phrase_matching", "\"packet state\""),
            ("scattered_phrase_zero", "\"state error\""),
            ("and", "rarebeacon packet"),
            ("and_complete", "rarebeacon alpha"),
        ]
    };
    println!(
        "FTS_MEASUREMENT {}",
        json!({"class":"initial_construction", "timing":measure(7, || {
            let fts = WorkspaceFts::new(workspace.clone(), FtsLimits::default());
            std::hint::black_box(fts.build().expect("initial build"));
        })})
    );
    let registry = ToolRegistry::default();
    register(&registry, workspace.clone()).expect("register");
    registry.authorize(TOOL_NAME).expect("authorize");
    let offer = registry.offer(&[TOOL_NAME]).expect("offer");
    let invoke = |query: &str, refresh: bool| {
        let call = CompletedToolCall {
            index: 0,
            id: "measurement".into(),
            name: TOOL_NAME.into(),
            arguments: json!({"query":query,"limit":50,"refresh":refresh}).to_string(),
        };
        let dispatch = offer.resolve(&call).expect("resolve");
        wait(dispatch.handler.call(dispatch.arguments)).expect("search")
    };
    let started = Instant::now();
    let first = invoke(queries[0].1, false);
    println!(
        "FTS_MEASUREMENT {}",
        json!({"class":"first_tool_query", "ms":started.elapsed().as_secs_f64()*1000.0,"index":first["index"]})
    );
    let mut results = BTreeMap::new();
    for (class, query) in queries {
        let value = invoke(query, false);
        if class.ends_with("zero") {
            assert!(value["results"].as_array().expect("results").is_empty());
        }
        results.insert(class, json!({"results":value["results"], "complete":value["complete"], "limit":value["limit"], "files_matched":value["files_matched"]}));
        println!(
            "FTS_MEASUREMENT {}",
            json!({"class":class,"tool":measure(21,||{std::hint::black_box(invoke(query,false));}),"complete":value["complete"],"limit":value["limit"],"records":value["results"].as_array().expect("results").len()})
        );
    }
    if let Some(path) = std::env::var_os("OCG_FTS_RESULTS") {
        std::fs::write(path, serde_json::to_vec(&results).expect("results JSON"))
            .expect("write results");
    }
    let mixed = if real_root.is_some() {
        [
            "statvfs",
            "workspace",
            "neverfindbeaconxqz",
            "rustix flume",
            "workspace tool",
            "\"authorized workspace\"",
            "\"workspace tool\"",
            "\"workspace rustix\"",
            "WorkspaceFts",
        ]
    } else {
        [
            "rarebeacon",
            "packet",
            "neverfindbeaconxqz",
            "rarebeacon packet",
            "packet state",
            "\"alpha beta\"",
            "\"packet state\"",
            "\"state error\"",
            "HTTPServer",
        ]
    };
    let timing = measure(9, || {
        for _ in 0..3 {
            for query in mixed {
                std::hint::black_box(invoke(query, false));
            }
        }
    });
    println!(
        "FTS_MEASUREMENT {}",
        json!({"class":"mixed_tool_throughput", "queries_per_sample":3*mixed.len(), "queries_per_second":(3*mixed.len()) as f64 * 1000.0/timing["median_ms"].as_f64().expect("median"), "timing":timing})
    );
    println!(
        "FTS_MEASUREMENT {}",
        json!({"class":"full_refresh_tool","timing":measure(5,||{std::hint::black_box(invoke(queries[0].1,true));})})
    );
    let rss = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("measure process RSS");
    assert!(rss.status.success());
    let retained = std::str::from_utf8(&rss.stdout)
        .expect("RSS text")
        .trim()
        .parse::<u64>()
        .expect("RSS KiB")
        * 1024;
    println!(
        "FTS_MEASUREMENT {}",
        json!({"class":"retained_process_rss", "bytes":retained,
        "scope":"process after queries and repeated full refreshes; includes allocator retention"})
    );
}

#[test]
#[ignore = "release-mode phrase scan phase profiling"]
fn phrase_scan_phases() {
    if cfg!(debug_assertions) {
        panic!("release profiling only");
    }
    let setup = setup("phrase-profile", &[]);
    let text = "packet state unrelated error packet state unrelated error\n".repeat(8000);
    write_file(&setup.root, "candidate.rs", text.as_bytes());
    let fts = WorkspaceFts::new(setup.workspace.clone(), FtsLimits::default());
    fts.build().expect("build");
    let slot = fts.lock();
    let index = slot.index.as_ref().expect("snapshot");
    let document = &index.documents[0];
    let query = parse("\"packet state error\"").expect("query");
    let stop = AtomicBool::new(false);
    let deadline = Instant::now() + Duration::from_secs(60);
    let scanned = scan_document(document, &query, &stop, deadline).expect("scan");
    println!(
        "FTS_MEASUREMENT {}",
        json!({"class":"phrase_scan_phases", "occurrences":scanned.tokens.len(), "scan":measure(31, || {
            std::hint::black_box(scan_document(document, &query, &stop, deadline).expect("scan"));
        }), "matching":measure(31, || {
            std::hint::black_box(matching_lines(document, &scanned, &query, &stop, deadline).expect("match"));
        })})
    );
}
