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
    let setup = setup("production-performance", &[]);
    for file in 0..4000 {
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
    let registry = ToolRegistry::default();
    register(&registry, setup.workspace.clone()).expect("register");
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
    let first = invoke("rarebeacon", false);
    println!(
        "FTS_MEASUREMENT {}",
        json!({"class":"first_tool_query", "ms":started.elapsed().as_secs_f64()*1000.0,"index":first["index"]})
    );
    for (class, query) in [
        ("rare", "rarebeacon"),
        ("missing", "neverpresenttoken"),
        ("selective_phrase", "\"alpha beta\""),
        ("broad_phrase_zero", "\"packet state error\""),
        ("scattered_phrase_zero", "\"state error\""),
        ("and", "rarebeacon state"),
        ("and_complete", "rarebeacon alpha"),
    ] {
        let value = invoke(query, false);
        println!(
            "FTS_MEASUREMENT {}",
            json!({"class":class,"tool":measure(21,||{std::hint::black_box(invoke(query,false));}),"complete":value["complete"],"limit":value["limit"],"records":value["results"].as_array().expect("results").len()})
        );
    }
    println!(
        "FTS_MEASUREMENT {}",
        json!({"class":"full_refresh_tool","timing":measure(5,||{std::hint::black_box(invoke("rarebeacon",true));})})
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
