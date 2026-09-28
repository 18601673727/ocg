//! Bounded end-to-end smoke for the latest-Lead-output capture.
//!
//! This drives the *real* generated OpenCode 2 adapter (the exact file
//! `ocg` materializes at launch) under `node`, with the real `ocg` test binary
//! as the bridge executable. The adapter reports a completed root Lead message
//! through `ocg __bridge lead.output --project ...`, and the assertion is the
//! real artifact on disk: `<project>/.ocg/reports/latest-lead-output.md`
//! containing exactly the Lead's raw text.
//!
//! Everything except the OpenCode host process is production code: the
//! adapter source, the bridge spawn (direct argv, JSON on stdin), the reports
//! policy, and the atomic write. The OpenCode host itself is not started (a
//! nested runtime cannot be isolated on this host; that limitation is recorded
//! in the closeout report), so the event objects below mirror the OpenCode
//! 2.0.14 plugin event contract observed directly on the real runtime:
//! `{id, created, type, location?, durable?, data}` envelopes with
//! `session.step.started` / `session.text.delta` / `session.text.ended` /
//! `session.step.ended` payloads (there is no `properties` bag).

mod common;

use common::TestDir;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn node_or_skip(test: &str) -> bool {
    let available = Command::new("node")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false);
    if available {
        return true;
    }
    assert_ne!(
        std::env::var("OCG_REQUIRE_JS_TESTS").as_deref(),
        Ok("1"),
        "node is required for the latest-Lead-output smoke test ({test}) but was not found"
    );
    eprintln!("skipping latest-Lead-output smoke ({test}): node is unavailable");
    false
}

fn write_check(dir: &Path, script: &str) {
    fs::write(dir.join("check.mjs"), script).expect("write check.mjs");
}

const PRELUDE: &str = r#"import fs from "node:fs";
import plugin from "./plugin.mjs";

function makeStream() {
  const queue = [];
  let pending = null;
  let closed = false;
  const iterator = {
    [Symbol.asyncIterator]() { return this; },
    next() {
      if (queue.length) return Promise.resolve({ value: queue.shift(), done: false });
      if (closed) return Promise.resolve({ value: undefined, done: true });
      return new Promise((resolve) => { pending = resolve; });
    },
    return() {
      closed = true;
      if (pending) { const resolve = pending; pending = null; resolve({ value: undefined, done: true }); }
      return Promise.resolve({ value: undefined, done: true });
    },
  };
  return {
    iterator,
    push(event) {
      if (pending) { const resolve = pending; pending = null; resolve({ value: event, done: false }); }
      else queue.push(event);
    },
  };
}

const stream = makeStream();
const ctx = {
  event: { subscribe: () => stream.iterator },
  session: { hook: async () => ({ dispose: async () => {} }) },
  tool: { hook: async () => ({ dispose: async () => {} }) },
};

// The real OpenCode 2.0.14 plugin envelope, as observed on the live runtime.
let eventSeq = 0;
const envelope = (type, data) => ({
  id: `evt_smoke_${++eventSeq}`,
  created: 1790120849467 + eventSeq,
  type,
  durable: { aggregateID: typeof data.sessionID === "string" ? data.sessionID : "ses_smoke", seq: eventSeq, version: 1 },
  location: { directory: process.env.OCG_PROJECT || process.cwd() },
  data,
});
const stepStarted = (sessionID, messageID, agent) =>
  envelope("session.step.started", { sessionID, assistantMessageID: messageID, agent, model: { id: "smoke-model", providerID: "smoke-provider" }, started: 1 });
const textDelta = (sessionID, messageID, ordinal, delta) =>
  envelope("session.text.delta", { sessionID, assistantMessageID: messageID, ordinal, delta });
const textEnded = (sessionID, messageID, ordinal, text) =>
  envelope("session.text.ended", { sessionID, assistantMessageID: messageID, ordinal, text });
const stepEnded = (sessionID, messageID, finish) =>
  envelope("session.step.ended", { sessionID, assistantMessageID: messageID, finish });
const executionInterrupted = (sessionID) =>
  envelope("session.execution.interrupted", { sessionID });
const completedLeadTurn = (sessionID, stepID, text) => {
  stream.push(stepStarted(sessionID, stepID, "lead-low"));
  stream.push(textDelta(sessionID, stepID, 0, text.slice(0, 3)));
  stream.push(textEnded(sessionID, stepID, 0, text));
  stream.push(stepEnded(sessionID, stepID, "stop"));
};
"#;

fn project(dir: &TestDir) -> PathBuf {
    let project = dir.project();
    fs::create_dir_all(project.join("src")).expect("create project");
    fs::write(
        project.join("src/parser.rs"),
        "pub fn parse() -> u32 { 1 }\n",
    )
    .expect("write source");
    fs::write(project.join(".ocg.yaml"), "{}\n").expect("write project config");
    project
}

fn admit_root(dir: &TestDir, project: &Path, session_id: &str) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ocg"))
        // This smoke test exercises durable root-output capture, not V2 prompt
        // admission. Use the legacy local admission path so the test does not
        // need to fabricate a live authenticated V2 runtime client.
        .args(["__bridge", "chat.message", "--project"])
        .arg(project)
        .env("OCG_PROJECT", project)
        .env("OCG_ORCHESTRATION_ENABLED", "1")
        .env("OCG_REPORTS_LATEST_LEAD_OUTPUT", "1")
        .env("OCG_USER_CONFIG", dir.join("no-user.yaml"))
        .env("OCG_PROJECT_CONFIG", project.join(".ocg.yaml"))
        .env("HOME", dir.join("home"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn chat.message bridge");
    child
        .stdin
        .as_mut()
        .expect("chat.message stdin")
        .write_all(
            &serde_json::to_vec(&serde_json::json!({
                "session_id": session_id,
                "text": "capture the current root execution"
            }))
            .expect("serialize chat.message"),
        )
        .expect("write chat.message");
    let output = child.wait_with_output().expect("wait for chat.message");
    assert!(
        output.status.success(),
        "chat.message bridge failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("parse admission");
    assert_eq!(value["ok"], serde_json::json!(true), "{value}");
}

#[test]
fn the_generated_v2_adapter_produces_the_raw_latest_lead_output_file() {
    if !node_or_skip("the_generated_v2_adapter_produces_the_raw_latest_lead_output_file") {
        return;
    }
    let dir = TestDir::new();
    let project = project(&dir);
    admit_root(&dir, &project, "root");

    // Materialize the real adapter through the exact launch path.
    let plugin_path = ocg::orchestration::plugin::materialize_v2_with(
        &project,
        ocg::runtime::compat::v2_adapter().plugin_source(),
    )
    .expect("materialize the V2 adapter");
    assert!(plugin_path.is_file(), "adapter must be materialized");
    let source = fs::read_to_string(&plugin_path).expect("read adapter");
    // Node needs an explicit ESM extension; the bytes are the materialized ones.
    fs::write(dir.join("plugin.mjs"), source).expect("stage adapter for node");

    let raw_text = "# Final answer\n\n- one\n- two\n\n```rust\nfn done() {}\n```\n";
    let body = format!(
        r#"{PRELUDE}
const dispose = await plugin.setup(ctx);
// The intermediate tool-call step narrates but never completes the turn:
// `finish: "tool-calls"` is not a completed response.
stream.push(stepStarted("root", "m1", "lead-high"));
stream.push(textDelta("root", "m1", 0, "narration that must not be reported"));
stream.push(stepEnded("root", "m1", "tool-calls"));
// The final step of the turn: streaming deltas, then the full ordinal text,
// then `finish: "stop"` — the completed root Lead response.
stream.push(stepStarted("root", "m2", "lead-high"));
stream.push(textDelta("root", "m2", 0, "{escaped}"));
stream.push(textEnded("root", "m2", 0, "{escaped}"));
stream.push(stepEnded("root", "m2", "stop"));
const target = process.env.OCG_EXPECTED_FILE;
const deadline = Date.now() + 10000;
while (!fs.existsSync(target) && Date.now() < deadline) {{
  await new Promise((resolve) => setTimeout(resolve, 20));
}}
await dispose();
console.log(JSON.stringify({{ exists: fs.existsSync(target) }}));
"#,
        escaped = raw_text
            .replace('\\', "\\\\")
            .replace('\n', "\\n")
            .replace('"', "\\\"")
    );
    write_check(dir.path(), &body);

    let expected = ocg::reports::latest_lead_output_path(&project);
    let output = Command::new("node")
        .arg("check.mjs")
        .current_dir(dir.path())
        // The bridge is the real binary under test.
        .env("OCG_BRIDGE", env!("CARGO_BIN_EXE_ocg"))
        .env("OCG_PROJECT", &project)
        .env("OCG_ORCHESTRATION_ENABLED", "1")
        .env("OCG_REPORTS_LATEST_LEAD_OUTPUT", "1")
        // Never read the developer's real configuration from the bridge child.
        .env("OCG_USER_CONFIG", dir.join("no-user.yaml"))
        .env("OCG_PROJECT_CONFIG", project.join(".ocg.yaml"))
        .env("HOME", dir.join("home"))
        .env("OCG_EXPECTED_FILE", &expected)
        .output()
        .expect("run the node adapter check");
    assert!(
        output.status.success(),
        "node check failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        expected.is_file(),
        "the report file must exist: {expected:?}"
    );
    assert_eq!(
        fs::read_to_string(&expected).expect("read report"),
        raw_text,
        "the report must be the Lead's raw text, byte-verbatim"
    );
    // The report lives under the ignored project state directory.
    assert_eq!(
        expected,
        project
            .join(".ocg")
            .join("reports")
            .join("latest-lead-output.md")
    );
}

#[test]
fn the_switch_disables_the_capture_end_to_end() {
    if !node_or_skip("the_switch_disables_the_capture_end_to_end") {
        return;
    }
    let dir = TestDir::new();
    let project = project(&dir);
    let plugin_path = ocg::orchestration::plugin::materialize_v2_with(
        &project,
        ocg::runtime::compat::v2_adapter().plugin_source(),
    )
    .expect("materialize the V2 adapter");
    fs::write(
        dir.join("plugin.mjs"),
        fs::read_to_string(&plugin_path).expect("read adapter"),
    )
    .expect("stage adapter");

    let body = r#"import fs from "node:fs";
import plugin from "./plugin.mjs";
const ctx = {
  event: { subscribe: () => ({ [Symbol.asyncIterator]() { return this; }, next: () => new Promise(() => {}) }) },
  session: { hook: async () => ({ dispose: async () => {} }) },
  tool: { hook: async () => ({ dispose: async () => {} }) },
};
await (await plugin.setup(ctx))();
console.log(JSON.stringify({ exists: fs.existsSync(process.env.OCG_EXPECTED_FILE) }));
"#;
    write_check(dir.path(), body);

    let expected = ocg::reports::latest_lead_output_path(&project);
    let output = Command::new("node")
        .arg("check.mjs")
        .current_dir(dir.path())
        .env("OCG_BRIDGE", env!("CARGO_BIN_EXE_ocg"))
        .env("OCG_PROJECT", &project)
        .env("OCG_ORCHESTRATION_ENABLED", "1")
        .env("OCG_REPORTS_LATEST_LEAD_OUTPUT", "0")
        .env("OCG_USER_CONFIG", dir.join("no-user.yaml"))
        .env("OCG_PROJECT_CONFIG", project.join(".ocg.yaml"))
        .env("HOME", dir.join("home"))
        .env("OCG_EXPECTED_FILE", &expected)
        .output()
        .expect("run the node adapter check");
    assert!(
        output.status.success(),
        "node check failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !expected.exists(),
        "a disabled switch must not write anything"
    );
}

/// Regression boundary for the real OpenCode 2.0.14 contract: a completed
/// root Lead response is written; a later interrupted, errored, failed,
/// worker or non-OCG response never replaces it; the next genuinely completed
/// root Lead response does. The events below replay the envelope and payload
/// shapes captured from the live 2.0.14 runtime (session.* types, `data`
/// payloads, `durable`/`location` envelope fields).
#[test]
fn only_a_completed_root_lead_response_replaces_the_latest_output() {
    if !node_or_skip("only_a_completed_root_lead_response_replaces_the_latest_output") {
        return;
    }
    let dir = TestDir::new();
    let project = project(&dir);
    admit_root(&dir, &project, "ses_root");
    let plugin_path = ocg::orchestration::plugin::materialize_v2_with(
        &project,
        ocg::runtime::compat::v2_adapter().plugin_source(),
    )
    .expect("materialize the V2 adapter");
    fs::write(
        dir.join("plugin.mjs"),
        fs::read_to_string(&plugin_path).expect("read adapter"),
    )
    .expect("stage adapter");

    let body = r#"import fs from "node:fs";
import plugin from "./plugin.mjs";

function makeStream() {
  const queue = [];
  let pending = null;
  let closed = false;
  const iterator = {
    [Symbol.asyncIterator]() { return this; },
    next() {
      if (queue.length) return Promise.resolve({ value: queue.shift(), done: false });
      if (closed) return Promise.resolve({ value: undefined, done: true });
      return new Promise((resolve) => { pending = resolve; });
    },
    return() {
      closed = true;
      if (pending) { const resolve = pending; pending = null; resolve({ value: undefined, done: true }); }
      return Promise.resolve({ value: undefined, done: true });
    },
  };
  return {
    iterator,
    push(event) {
      if (pending) { const resolve = pending; pending = null; resolve({ value: event, done: false }); }
      else queue.push(event);
    },
  };
}

const stream = makeStream();
const ctx = {
  event: { subscribe: () => stream.iterator },
  session: { hook: async () => ({ dispose: async () => {} }) },
  tool: { hook: async () => ({ dispose: async () => {} }) },
};

// Real 2.0.14 envelope shapes, mirrored from the captured live event dump.
let eventSeq = 0;
const envelope = (type, data) => ({
  id: `evt_regression_${++eventSeq}`,
  created: 1790120849467 + eventSeq,
  type,
  durable: { aggregateID: typeof data.sessionID === "string" ? data.sessionID : "ses_regression", seq: eventSeq, version: 1 },
  location: { directory: process.env.OCG_PROJECT || process.cwd() },
  data,
});
const stepStarted = (sessionID, messageID, agent) =>
  envelope("session.step.started", { sessionID, assistantMessageID: messageID, agent, model: { id: "claude-fable-5", providerID: "vsllm", variant: "high" }, snapshot: "abc", started: 1 });
const textDelta = (sessionID, messageID, ordinal, delta) =>
  envelope("session.text.delta", { sessionID, assistantMessageID: messageID, ordinal, delta });
const textEnded = (sessionID, messageID, ordinal, text) =>
  envelope("session.text.ended", { sessionID, assistantMessageID: messageID, ordinal, text });
const stepEnded = (sessionID, messageID, finish, extra = {}) =>
  envelope("session.step.ended", { sessionID, assistantMessageID: messageID, finish, rawFinish: finish === "stop" ? "stop" : finish === "tool-calls" ? "tool_calls" : null, cost: 0, tokens: { input: 1, output: 1, reasoning: 0, cache: { read: 0, write: 0 } }, snapshot: "abc", files: [], ...extra });
const executionInterrupted = (sessionID) => envelope("session.execution.interrupted", { sessionID });
const executionFailed = (sessionID) => envelope("session.execution.failed", { sessionID, error: { type: "unknown", message: "boom" } });

const target = process.env.OCG_EXPECTED_FILE;
const readTarget = () => { try { return fs.readFileSync(target, "utf8"); } catch (_) { return null; } };
const waitFor = async (expected) => {
  const deadline = Date.now() + 10000;
  while (readTarget() !== expected && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return readTarget();
};

const dispose = await plugin.setup(ctx);

// Turn 1: a completed root Lead response (deltas then the full ordinal text).
stream.push(stepStarted("ses_root", "msg_good1", "lead-low"));
stream.push(textDelta("ses_root", "msg_good1", 0, "GOOD-"));
stream.push(textDelta("ses_root", "msg_good1", 0, "ONE"));
stream.push(textEnded("ses_root", "msg_good1", 0, "GOOD-ONE"));
stream.push(stepEnded("ses_root", "msg_good1", "stop"));
const first = await waitFor("GOOD-ONE");

// Turn 2: an interrupted response — partial text, an interruption and no
// completing stop. The good output must stay byte-identical.
stream.push(stepStarted("ses_root", "msg_partial", "lead-low"));
stream.push(textDelta("ses_root", "msg_partial", 0, "PARTIAL-"));
stream.push(executionInterrupted("ses_root"));
// Turn 3: an errored step (finish: "error") and a failed execution.
stream.push(stepStarted("ses_root", "msg_errored", "lead-low"));
stream.push(textEnded("ses_root", "msg_errored", 0, "ERRORED"));
stream.push(stepEnded("ses_root", "msg_errored", "error"));
stream.push(stepStarted("ses_root", "msg_failed", "lead-low"));
stream.push(textEnded("ses_root", "msg_failed", 0, "FAILED"));
stream.push(executionFailed("ses_root"));
// Turn 4: a worker and an unrelated agent complete but must never report.
stream.push(stepStarted("ses_worker", "msg_worker", "build"));
stream.push(textEnded("ses_worker", "msg_worker", 0, "WORKER"));
stream.push(stepEnded("ses_worker", "msg_worker", "stop"));
stream.push(stepStarted("ses_other", "msg_other", "general"));
stream.push(textEnded("ses_other", "msg_other", 0, "OTHER"));
stream.push(stepEnded("ses_other", "msg_other", "stop"));
// Give the pump every chance to (wrongly) overwrite the good output.
await new Promise((resolve) => setTimeout(resolve, 500));
const afterBad = readTarget();

// Turn 5: the next genuinely completed root Lead response replaces it.
stream.push(stepStarted("ses_root", "msg_good2", "build"));
stream.push(textEnded("ses_root", "msg_good2", 0, "GOOD-TWO"));
stream.push(stepEnded("ses_root", "msg_good2", "stop"));
const second = await waitFor("GOOD-TWO");

await dispose();
console.log(JSON.stringify({ first, afterBad, second }));
"#;
    write_check(dir.path(), body);

    let expected = ocg::reports::latest_lead_output_path(&project);
    let output = Command::new("node")
        .arg("check.mjs")
        .current_dir(dir.path())
        .env("OCG_BRIDGE", env!("CARGO_BIN_EXE_ocg"))
        .env("OCG_PROJECT", &project)
        .env("OCG_ORCHESTRATION_ENABLED", "1")
        .env("OCG_REPORTS_LATEST_LEAD_OUTPUT", "1")
        .env("OCG_USER_CONFIG", dir.join("no-user.yaml"))
        .env("OCG_PROJECT_CONFIG", project.join(".ocg.yaml"))
        .env("HOME", dir.join("home"))
        .env("OCG_EXPECTED_FILE", &expected)
        .output()
        .expect("run the node adapter check");
    assert!(
        output.status.success(),
        "node check failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse check output");
    assert_eq!(
        value["first"],
        serde_json::json!("GOOD-ONE"),
        "the first completed root Lead response must be captured"
    );
    assert_eq!(
        value["afterBad"],
        serde_json::json!("GOOD-ONE"),
        "an interrupted, errored, failed, worker or non-OCG response must never overwrite a good output"
    );
    assert_eq!(
        value["second"],
        serde_json::json!("GOOD-TWO"),
        "a later completed root Lead response must replace the previous one"
    );
    assert_eq!(
        fs::read_to_string(&expected).expect("read final report"),
        "GOOD-TWO",
        "the final artifact is the latest good response, byte-verbatim"
    );
}
