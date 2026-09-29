//! Generated OpenCode plugin adapter.
//!
//! OpenCode's supported extension mechanism is a local JavaScript plugin. OCG
//! generates a *thin* adapter under the project's ignored state directory and
//! injects its `file://` URL into the generated config's `plugin` array. The
//! adapter does no ranking and no policy: every decision is made by the Rust
//! bridge it spawns (`ocg __bridge ...`) with a direct argv and stdin, never a
//! shell.
//!
//! The adapter is deliberately small and boring:
//!
//! - V1: `chat.message` first enforces the Rust-resolved primary Lead
//!   agent/model/variant contract on the mutable user message, then optionally
//!   appends a delimited dynamic-context suffix (never replacing the prompt);
//! - V2: `session.prompt` reports a genuinely admitted user prompt to the
//!   bridge for task bookkeeping (strictly read-only: the event is never
//!   mutated), and `session.context` pushes the session repository baseline
//!   onto the outgoing root-Lead request's system context at every model
//!   dispatch — an ephemeral injection that is never persisted into the user
//!   message or the session history;
//! - `tool.execute.before` appends the role hand-off to a delegation prompt;
//! - `tool.execute.after` appends verification feedback to a delegation result;
//! - V2 also observes the event stream and reports the raw text of a completed
//!   root Lead assistant message to the bridge, which persists
//!   `.ocg/reports/latest-lead-output.md` byte-verbatim (streaming
//!   partials, errored/interrupted messages, worker sessions and non-OCG
//!   agents are never reported);
//! - a delimiter guard makes the append idempotent if a hook fires twice;
//! - every bridge failure is swallowed so a broken bridge cannot break the
//!   session, while a Lead contract that cannot be enforced fails loudly before
//!   an incorrect provider request can be sent.

use crate::error::{OcgError, Result};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// The plugin directory under `.ocg/orchestration/`.
pub const PLUGIN_DIR: &str = "plugin";
/// The generated adapter file name.
pub const PLUGIN_FILE: &str = "ocg-orchestration.js";
/// The stable start delimiter of the dynamic context suffix.
pub const CONTEXT_START: &str = "<<<OCG:DYNAMIC_CONTEXT v1>>>";
/// The stable end delimiter of the dynamic context suffix.
pub const CONTEXT_END: &str = "<<<OCG:END>>>";

/// The plugin path for a project root.
pub fn plugin_path(root: &Path) -> PathBuf {
    crate::orchestration::state::state_dir(root)
        .join(PLUGIN_DIR)
        .join(PLUGIN_FILE)
}

/// The dedicated OpenCode 2 config root under the orchestration state dir.
///
/// OpenCode 2 treats `OPENCODE_CONFIG_DIR` as its own namespace and discovers
/// local plugins from its `plugins/` child. The V1 adapter and the V1
/// orchestration state also live under `.ocg/orchestration/`, so the
/// V2 config dir must be a dedicated child root: a config dir shared with the
/// V1 state lets the V2 runtime discover stale V1 artifacts (which export the
/// V1 `export const server` contract) and fail to load.
pub const V2_CONFIG_DIR: &str = "v2-config";

/// OpenCode 2 discovers local plugins from the `plugins/` child of its custom
/// config directory. Keep it inside OCG's ignored state rather than writing an
/// untracked `.opencode/plugins` file into the project.
pub fn v2_plugin_path(root: &Path) -> PathBuf {
    v2_config_dir(root).join("plugins").join(PLUGIN_FILE)
}

/// The custom config directory which makes [`v2_plugin_path`] discoverable.
/// It is a dedicated root that never contains V1 plugin artifacts or other V1
/// state, so a V2 runtime can only ever discover the generated V2 adapter.
pub fn v2_config_dir(root: &Path) -> PathBuf {
    crate::orchestration::state::state_dir(root).join(V2_CONFIG_DIR)
}

/// The `file://` URL injected into the OpenCode config.
pub fn plugin_uri(root: &Path) -> Option<String> {
    let path = plugin_path(root);
    let absolute = path
        .canonicalize()
        .unwrap_or_else(|_| absolutize(root).join(relative_from_state()));
    Some(format!("file://{}", absolute.to_string_lossy()))
}

/// The canonical, absolute `file://` URL for the generated adapter.
///
/// OpenCode 2 requires a canonical local plugin URI; a URI that could not be
/// made absolute is refused rather than silently passed through.
pub fn canonical_plugin_uri(root: &Path) -> Option<String> {
    let uri = plugin_uri(root)?;
    let absolute = uri
        .strip_prefix("file://")
        .map(Path::new)
        .map(Path::is_absolute)
        .unwrap_or(false);
    absolute.then_some(uri)
}

fn relative_from_state() -> PathBuf {
    Path::new(crate::context::repomap::OCG_DIR)
        .join(crate::orchestration::state::ORCHESTRATION_DIR)
        .join(PLUGIN_DIR)
        .join(PLUGIN_FILE)
}

fn absolutize(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

/// Whether the generated adapter exists on disk.
pub fn is_installed(root: &Path) -> bool {
    plugin_path(root).is_file()
}

/// Materialize the v1 generated adapter. Writes only under ignored local state.
pub fn materialize(root: &Path) -> Result<PathBuf> {
    materialize_with(root, plugin_source())
}

/// Materialize a generated adapter from an explicit source. Writes only under
/// ignored local state.
pub fn materialize_with(root: &Path, source: &str) -> Result<PathBuf> {
    crate::runtime::install::ensure_gitignore(root)?;
    let path = plugin_path(root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| OcgError::io(format!("cannot create {}", parent.display()), error))?;
    }
    std::fs::write(&path, source).map_err(|error| OcgError::write(&path, error))?;
    Ok(path)
}

/// Materialize the adapter at the local-discovery path used by OpenCode 2.
///
/// Also migrates legacy generated artifacts from earlier layouts that shared
/// the V1 state root (or double-applied the orchestration state path), so a
/// stale V1-era file can never be discovered by a V2 runtime.
pub fn materialize_v2_with(root: &Path, source: &str) -> Result<PathBuf> {
    crate::runtime::install::ensure_gitignore(root)?;
    let path = v2_plugin_path(root);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| OcgError::io(format!("cannot create {}", parent.display()), error))?;
    }
    std::fs::write(&path, source).map_err(|error| OcgError::write(&path, error))?;
    migrate_legacy_v2_artifacts(root);
    Ok(path)
}

/// The header every OCG-generated adapter carries. Legacy artifact migration
/// only ever removes files that OCG itself generated; anything else is treated
/// as user-owned and left untouched.
const GENERATED_HEADER: &str = "// Generated by OCG (ocg)";

/// Legacy V2 artifact locations from earlier builds: one that shared the V1
/// state root (`orchestration/plugins/`, where a V2 runtime also discovered the
/// V1 `orchestration/plugin/` artifact) and one that double-applied the
/// orchestration state path (`orchestration/orchestration/plugins/`). Each
/// entry lists the legacy file and the directories to prune afterwards.
fn legacy_v2_artifacts(root: &Path) -> Vec<(PathBuf, Vec<PathBuf>)> {
    let state = crate::orchestration::state::state_dir(root);
    let nested = state.join(crate::orchestration::state::ORCHESTRATION_DIR);
    vec![
        (
            state.join("plugins").join(PLUGIN_FILE),
            vec![state.join("plugins")],
        ),
        (
            nested.join("plugins").join(PLUGIN_FILE),
            vec![nested.join("plugins"), nested],
        ),
    ]
}

/// Remove stale generated adapters from legacy V2 locations. Only files
/// carrying [`GENERATED_HEADER`] are removed, and directories are pruned only
/// when they become empty, so user-owned files are never deleted. A failed
/// removal is intentionally silent: the dedicated config root already makes
/// legacy artifacts undiscoverable, and migration must never fail a launch.
fn migrate_legacy_v2_artifacts(root: &Path) {
    for (path, prune) in legacy_v2_artifacts(root) {
        let generated = std::fs::read_to_string(&path)
            .map(|text| text.starts_with(GENERATED_HEADER))
            .unwrap_or(false);
        if !generated {
            continue;
        }
        let _ = std::fs::remove_file(&path);
        for dir in prune {
            // Succeeds only when the directory is empty; never forced.
            let _ = std::fs::remove_dir(dir);
        }
    }
}

/// Inject the plugin URL under the given config array key (`plugin` for
/// OpenCode 1.x, `plugins` for OpenCode 2.x), preserving every existing entry
/// and never adding a duplicate.
pub fn inject_plugin_for(config: &mut Value, key: &str, uri: &str) {
    let Some(object) = config.as_object_mut() else {
        return;
    };
    let mut entries: Vec<Value> = match object.get(key) {
        Some(Value::Array(values)) => values
            .iter()
            .filter(|value| !entry_is_uri(value, uri))
            .cloned()
            .collect(),
        _ => Vec::new(),
    };
    entries.push(Value::String(uri.to_string()));
    object.insert(key.to_string(), Value::Array(entries));
}

/// Inject the plugin URL under the historical singular `plugin` key.
pub fn inject_plugin(config: &mut Value, uri: &str) {
    inject_plugin_for(config, "plugin", uri);
}

/// Whether one configured plugin entry already names `uri`.
fn entry_is_uri(value: &Value, uri: &str) -> bool {
    match value {
        Value::String(text) => text == uri,
        Value::Array(items) => items.first().and_then(Value::as_str) == Some(uri),
        _ => false,
    }
}

/// Whether a config already contains an OCG plugin entry under `key` (used by
/// diagnostics and by tests that assert disabled orchestration emits nothing).
pub fn has_ocg_plugin_for(config: &Value, key: &str) -> bool {
    config
        .get(key)
        .and_then(Value::as_array)
        .map(|entries| entries.iter().any(entry_is_ocg))
        .unwrap_or(false)
}

/// Whether a config already contains an OCG plugin entry under the historical
/// singular `plugin` key.
pub fn has_ocg_plugin(config: &Value) -> bool {
    has_ocg_plugin_for(config, "plugin")
}

/// Remove the generated OCG plugin entry from `key` while preserving every user
/// plugin. Used for non-coding sessions (`ocg models`) that must not depend on
/// the adapter being materialized.
pub fn remove_ocg_plugin_for(config: &mut Value, key: &str) {
    let Some(object) = config.as_object_mut() else {
        return;
    };
    let Some(Value::Array(entries)) = object.get_mut(key) else {
        return;
    };
    entries.retain(|entry| !entry_is_ocg(entry));
    if entries.is_empty() {
        object.remove(key);
    }
}

/// Remove the generated OCG plugin entry from the historical singular `plugin`
/// key.
pub fn remove_ocg_plugin(config: &mut Value) {
    remove_ocg_plugin_for(config, "plugin");
}

fn entry_is_ocg(value: &Value) -> bool {
    let text = match value {
        Value::String(text) => text.as_str(),
        Value::Array(items) => items.first().and_then(Value::as_str).unwrap_or(""),
        _ => "",
    };
    text.contains(PLUGIN_FILE)
}

/// The generated adapter source. Kept as one `const` so the bytes are stable
/// and reviewable.
pub fn plugin_source() -> &'static str {
    PLUGIN_SOURCE
}

const PLUGIN_SOURCE: &str = r#"// Generated by OCG (ocg). Do not edit by hand.
// Thin adapter: all ranking, projection and policy live in the `ocg` Rust
// bridge. No shell is used; Bun.spawn receives a direct argv vector.
//
// Hooks:
//   chat.message          -> enforce Lead contract; append dynamic context
//   tool.execute.before   -> append the role hand-off to a `task` prompt
//   tool.execute.after    -> append verification feedback to a `task` result
//
// Refresh policy: the bridge injects the full repository snapshot on the first
// Lead prompt of a session. If the effective snapshot is unchanged, a later
// prompt in the same session gets an empty `context`, which this adapter treats
// as a no-op, so the snapshot is not duplicated across turns. A materially
// changed snapshot is injected again on the next prompt.
//
// Every bridge failure is swallowed: a broken bridge must never break a
// session. `ocg __bridge` is expected on OCG_BRIDGE (or PATH).

const START = "<<<OCG:DYNAMIC_CONTEXT v1>>>";
const END = "<<<OCG:END>>>";

function executable() {
  return process.env.OCG_BRIDGE || "ocg";
}

function project() {
  return process.env.OCG_PROJECT || process.cwd();
}

function orchestrationEnabled() {
  return process.env.OCG_ORCHESTRATION_ENABLED === "1";
}

function leadContract() {
  const raw = process.env.OCG_LEAD_CONTRACT;
  if (!raw) throw new Error("OCG Lead contract is missing");
  let value;
  try {
    value = JSON.parse(raw);
  } catch (_) {
    throw new Error("OCG Lead contract is invalid");
  }
  for (const key of ["agent", "provider_id", "model_id"]) {
    if (typeof value[key] !== "string" || value[key].length === 0) {
      throw new Error("OCG Lead contract is incomplete");
    }
  }
  // A reasoning variant is optional and provider-specific. When the resolved
  // Lead declares none, it is absent (or null) and the request stays at the
  // provider default; it is never fabricated. A present variant must be a
  // non-empty string.
  if (value.variant !== undefined && value.variant !== null) {
    if (typeof value.variant !== "string" || value.variant.length === 0) {
      throw new Error("OCG Lead contract is incomplete");
    }
  } else {
    value.variant = undefined;
  }
  return value;
}

function agentOf(value) {
  return value && typeof value.agent === "string" ? value.agent : "";
}

// Only a positively identified OCG Lead session is rewritten. OpenCode always
// resolves an agent before `chat.message` (the selected agent for the primary
// session, the subagent name for a `task` child), so a worker request can
// never be mistaken for the Lead. A request whose agent cannot be established
// is left untouched rather than silently re-routed onto a Lead model.
function isPrimaryLead(input, output) {
  const selected = agentOf(input) || agentOf(output && output.message);
  return selected.startsWith("lead-");
}

function enforceLeadContract(input, output) {
  if (!isPrimaryLead(input, output)) return null;
  const contract = leadContract();
  if (!output || !output.message) {
    throw new Error("OpenCode did not expose a mutable Lead request");
  }
  output.message.agent = contract.agent;
  // Assign a fresh model object so a stale sticky variant cannot survive a
  // provider-default contract: absent variant means no `variant` key at all.
  const model = {
    providerID: contract.provider_id,
    modelID: contract.model_id,
  };
  if (contract.variant !== undefined) {
    model.variant = contract.variant;
  }
  output.message.model = model;
  const actual = output.message.model;
  if (
    output.message.agent !== contract.agent ||
    !actual ||
    actual.providerID !== contract.provider_id ||
    actual.modelID !== contract.model_id ||
    (contract.variant !== undefined && actual.variant !== contract.variant)
  ) {
    throw new Error("OpenCode rejected the OCG Lead request contract");
  }
  return contract;
}

async function bridge(event, payload) {
  try {
    const proc = Bun.spawn([executable(), "__bridge", event, "--project", project()], {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
      env: process.env,
    });
    try {
      proc.stdin.write(JSON.stringify(payload));
      proc.stdin.end();
    } catch (_) {
      // stdin may already be closed; the bridge still answers or fails soft.
    }
    const text = await new Response(proc.stdout).text();
    await proc.exited;
    if (!text) return null;
    return JSON.parse(text);
  } catch (_) {
    return null;
  }
}

function hasContext(text) {
  return typeof text === "string" && text.includes(START) && text.includes(END);
}

function suffix(context) {
  return "\n\n" + START + "\n" + context + "\n" + END + "\n";
}

// A compact, clearly-estimated presentation header. The bridge supplies the
// counts; this adapter never computes or claims exact provider billing tokens
// and never emits ANSI/display-control syntax. When the bridge returns no
// metadata (or an empty context), the context is passed through unchanged.
function contextWithMetadata(result) {
  const text = result && typeof result.context === "string" ? result.context : "";
  if (!text) return "";
  const tokens = result.estimated_tokens;
  const files = result.file_count;
  const symbols = result.symbol_count;
  if (typeof tokens !== "number" || typeof files !== "number" || typeof symbols !== "number") {
    return text;
  }
  const pretty = tokens >= 1000 ? (tokens / 1000).toFixed(1) + "k" : String(tokens);
  return "OCG Context · ≈" + pretty + " tokens · " + files + " files · " + symbols + " symbols\n" + text;
}

function textParts(parts) {
  return (parts || []).filter(
    (part) => part && part.type === "text" && typeof part.text === "string",
  );
}

function appendToParts(parts, context) {
  const texts = textParts(parts);
  if (texts.length === 0) return;
  const target = texts[texts.length - 1];
  if (hasContext(target.text)) return;
  target.text = target.text + suffix(context);
}

function appendToPrompt(args, context) {
  if (!args || typeof args.prompt !== "string" || hasContext(args.prompt)) return;
  args.prompt = args.prompt + suffix(context);
}

function appendToOutput(output, context) {
  if (!output || typeof output.output !== "string" || hasContext(output.output)) return;
  output.output = output.output + suffix(context);
}

export const server = async (_input) => ({
  "chat.message": async (input, output) => {
    // Observed OpenCode 1.18.x request lifecycle (supported plugin surface):
    //   SessionPrompt.createUserMessage builds the user message `j`
    //   (agent + model{providerID,modelID,variant}, where an explicit/selected
    //   variant wins over the agent variant), then triggers `chat.message`
    //   with {message: j, parts}, then persists `j` via updateMessage(j).
    //   SessionPrompt.run later reads that persisted message and derives the
    //   provider request from `j.agent` and `j.model.variant`.
    // Writing the contract onto the mutable `output.message` therefore decides
    // the actual request and overrides sticky per-model/session UI state,
    // without mutating OpenCode's global saved variant.
    const contract = enforceLeadContract(input, output);
    // Only the Lead owns the top-level dynamic context. A worker subagent
    // session must not receive a second/duplicate Lead context block. The
    // task before/after hooks below stay active in every session.
    const agent = contract ? contract.agent : (typeof input.agent === "string" ? input.agent : "");
    if (agent && !agent.startsWith("lead-")) return;
    if (!orchestrationEnabled()) return;
    const parts = output && output.parts ? output.parts : [];
    const text = textParts(parts)
      .map((part) => part.text)
      .join("\n");
    if (!text) return;
    const result = await bridge("chat.message", {
      session_id: input.sessionID,
      agent: input.agent,
      text,
    });
    const context = contextWithMetadata(result);
    if (context) appendToParts(parts, context);
  },
  "tool.execute.before": async (input, output) => {
    if (input.tool !== "task") return;
    const args = output && output.args ? output.args : {};
    const result = await bridge("tool.execute.before", {
      session_id: input.sessionID,
      tool: input.tool,
      args,
    });
    if (!result || !result.ok || !result.witness) throw new Error("canonical delegation admission failed");
    args.ocg_witness = result.witness;
    if (result && result.context) appendToPrompt(args, result.context);
  },
  "tool.execute.after": async (input, output) => {
    // A missing delivery (a cancelled subagent, or an absent hook output) is
    // ignored: the bridge is optional and must never break the session.
    if (!input || input.tool !== "task") return;
    if (!output) return;
    const result = await bridge("tool.execute.after", {
      session_id: input.sessionID,
      tool: input.tool,
      args: input.args,
      result: output,
    });
    if (result && result.context) appendToOutput(output, result.context);
  },
});
"#;

/// The generated OpenCode 2 adapter source. OpenCode 2 selects the Lead at the
/// session level in Rust, so this adapter is even thinner than v1: it never
/// rewrites the request model or agent.
pub fn v2_plugin_source() -> &'static str {
    PLUGIN_SOURCE_V2
}

const PLUGIN_SOURCE_V2: &str = r#"// Generated by OCG (ocg) for OpenCode 2.x. Do not edit by hand.
// Thin adapter: all ranking, projection and policy live in the `ocg` Rust
// bridge. Lead agent/model/variant selection is session-level and performed in
// Rust; this adapter never rewrites the request model or agent.
//
// Hooks:
//   session.prompt      -> report a genuinely admitted user task (read-only)
//   session.context     -> inject the repository baseline at model dispatch
//   tool.execute.before   -> append the role hand-off to a `subagent` prompt
//   tool.execute.after    -> append verification feedback to a `subagent` result
//
// Event stream (OpenCode 2):
//   session.step.started / session.text.delta / session.text.ended /
//   session.step.ended / session.step.failed /
//   session.execution.interrupted / session.execution.failed
//                       -> report the raw text of a *completed* assistant
//                          response step to the bridge, which checks the
//                          Mission's current root execution before persisting
//                          `.ocg/reports/latest-lead-output.md`
//                          (no headers, no summary, byte-verbatim text), then
//                          sends token/boundary telemetry to the Rust governor
//
// Task admission vs. model dispatch: OpenCode runs the `prompt` hook only when
// a real user prompt is admitted (SessionPrompt.prepare). Runtime-generated
// synthetic user-role messages — interruption/resume continuations and
// similar — bypass prompt admission entirely, so `session.prompt` is the only
// authoritative "current user task" signal. The `context` hook fires on every
// model dispatch, including tool-driven continuations whose trailing user-role
// message may be synthetic; it therefore never derives task state from the
// conversation and only injects the session repository baseline.
//
// Repository baseline: every outgoing root-Lead model request — the first
// turn, later turns and tool-driven continuations alike — receives exactly one
// current session repository baseline, pushed onto the request's system
// context by the `session.context` hook. OpenCode applies context-hook changes
// to the outgoing model call only, so the baseline is never persisted into the
// user message or the session history and cannot accumulate across turns. The
// bridge keys the baseline on the task-independent repository generation: an
// unchanged generation reuses the retained rendering (it is still supplied on
// every dispatch), and only a material repository change re-renders it.
//
// Runtime: OpenCode 2 ships both a Bun build and a Node build, so the bridge is
// spawned with `node:child_process` (supported by Node and Bun) using
// `shell: false` and a direct argv vector, never a shell.
//
// Fail soft: every bridge failure is swallowed so a broken bridge cannot break
// a session. A missing `tool.execute.after` delivery is ignored as well.

import { spawn } from "node:child_process";

const START = "<<<OCG:DYNAMIC_CONTEXT v1>>>";
const END = "<<<OCG:END>>>";

// The generated OCG worker agents. Only a known worker `subagent` launched
// by a Lead session is bridged; any other tool, subagent or caller is ignored.
// `ocg-explore-deep` is a distinct generated agent, not a variant of
// `ocg-explore`.
const WORKER_AGENTS = new Set([
  "ocg-explore",
  "ocg-explore-deep",
  "ocg-build",
  "ocg-verify",
  "ocg-debug",
  "ocg-docs",
]);

function executable() {
  return process.env.OCG_BRIDGE || "ocg";
}

// The durable Run identity of each canonical worker session, learned from the
// OCG-owned envelope on its first admitted prompt. It is a bounded cache of a
// fact the backend already holds; the bridge re-validates every witness, so a
// lost or stale entry can never grant authority.
const ownWitness = new Map();

function project() {
  return process.env.OCG_PROJECT || process.cwd();
}

function orchestrationEnabled() {
  return process.env.OCG_ORCHESTRATION_ENABLED === "1";
}

function isLead(agent) {
  return typeof agent === "string" && agent.startsWith("lead-");
}

function isWorker(agent) {
  return typeof agent === "string" && WORKER_AGENTS.has(agent);
}

function reportsLatestEnabled() {
  // Absent means enabled: the switch is exported by `ocg` at launch and the
  // default policy is on.
  return process.env.OCG_REPORTS_LATEST_LEAD_OUTPUT !== "0";
}

function contextGovernorEnabled() {
  // Absent means enabled for compatibility with an older generated adapter;
  // current launches export the resolved Rust policy explicitly.
  return process.env.OCG_CONTEXT_GOVERNOR_ENABLED !== "0";
}

// OpenCode 2 plugin event envelope, observed directly on the real 2.0.14
// runtime: every event is `{id, created, type, location?, durable?, data}`
// (`metadata` appears on a few housekeeping types). The event-specific payload
// is `data`; there is no `properties` bag. The capture-relevant types and
// their observed `data` payloads are:
//
//   session.agent.selected  {sessionID, agent, previous}
//   session.step.started    {sessionID, assistantMessageID, agent, model, ...}
//   session.text.delta      {sessionID, assistantMessageID, ordinal, delta}
//   session.text.ended      {sessionID, assistantMessageID, ordinal, text}
//                           (`text` is the *full* text of that ordinal, not a
//                           delta; `session.text.started` carries no text)
//   session.step.ended      {sessionID, assistantMessageID, finish, rawFinish,
//                           cost, tokens, ...} — `finish` is "stop" for a
//                           completed response, "tool-calls" for an
//                           intermediate step and "error" for an
//                           interrupted/aborted/failed one
//   session.step.failed     {sessionID, assistantMessageID, ...}
//   session.execution.interrupted / session.execution.failed  {sessionID, ...}
//
// A root Lead response is therefore captured exactly when one of its steps
// ends with `finish === "stop"`: that is the only shape a successfully
// completed answer has. Intermediate tool-call steps, errored steps and
// interrupted executions are never reported, so a previously captured good
// output is never overwritten by a partial response.
function eventData(event) {
  return event && event.data && typeof event.data === "object" ? event.data : null;
}

// Per-subscription capture state. `messages` tracks one in-flight assistant
// step per `assistantMessageID`; `sessionAgents` is the session-level agent
// fallback for a step whose `session.step.started` was missed (for example a
// subscription that started mid-session).
function createCaptureState() {
  return {
    sessionAgents: new Map(), // sessionID -> agent
    messages: new Map(), // assistantMessageID -> {sessionID, agent, ordinals}
  };
}

// Bound a Map by evicting the oldest inserted entries; a long session must not
// grow the capture state without limit.
function bound(map, limit) {
  while (map.size > limit) {
    const oldest = map.keys().next();
    if (oldest.done) break;
    map.delete(oldest.value);
  }
}

function messageEntry(state, sessionID, messageID) {
  let entry = state.messages.get(messageID);
  if (!entry) {
    entry = { sessionID, agent: null, ordinals: new Map() };
    state.messages.set(messageID, entry);
    bound(state.messages, 64);
  }
  return entry;
}

// `session.step.started` is the authoritative per-step agent record: it names
// the agent that owns one `assistantMessageID`.
function trackStepStarted(state, event) {
  const data = eventData(event);
  if (!data || typeof data.assistantMessageID !== "string" || !data.assistantMessageID) return;
  const entry = messageEntry(state, data.sessionID, data.assistantMessageID);
  if (typeof data.sessionID === "string" && data.sessionID) entry.sessionID = data.sessionID;
  if (typeof data.agent === "string" && data.agent) entry.agent = data.agent;
}

// `session.agent.selected` records the session-level agent as a fallback.
function trackAgentSelected(state, event) {
  const data = eventData(event);
  if (!data) return;
  if (typeof data.sessionID !== "string" || !data.sessionID) return;
  if (typeof data.agent !== "string" || !data.agent) return;
  state.sessionAgents.set(data.sessionID, data.agent);
  bound(state.sessionAgents, 256);
}

// Streaming text of one step. `session.text.delta` appends an increment;
// `session.text.ended` fixes the full text of the ordinal. The ended text is
// authoritative; the accumulated deltas are the fallback for a step whose
// ended event was missed.
function recordText(state, event) {
  const data = eventData(event);
  if (!data || typeof data.assistantMessageID !== "string" || !data.assistantMessageID) return;
  const ordinal = typeof data.ordinal === "number" ? data.ordinal : 0;
  const entry = messageEntry(state, data.sessionID, data.assistantMessageID);
  let slot = entry.ordinals.get(ordinal);
  if (!slot) {
    slot = { chunks: [], final: null };
    entry.ordinals.set(ordinal, slot);
  }
  if (event.type === "session.text.delta") {
    if (typeof data.delta === "string") slot.chunks.push(data.delta);
  } else if (typeof data.text === "string") {
    slot.final = data.text;
  }
}

// The raw user-visible text of one step: every text ordinal in order, ended
// text preferred, empty parts dropped. The adapter never rewrites, reorders
// beyond ordinal order or summarizes.
function assembledText(entry) {
  const ordinals = Array.from(entry.ordinals.keys()).sort((left, right) => left - right);
  const parts = [];
  for (const ordinal of ordinals) {
    const slot = entry.ordinals.get(ordinal);
    const text = typeof slot.final === "string" ? slot.final : slot.chunks.join("");
    if (typeof text === "string" && text.trim().length > 0) parts.push(text);
  }
  return parts.join("\n\n");
}

// `session.step.ended` is the only completion boundary. Only `finish ===
// "stop"` is a completed response; "tool-calls" steps continue and "error"
// steps (interrupted, aborted, provider failures) are incomplete — neither may
// replace a previously captured good output. The step's agent is diagnostic
// metadata; the Rust bridge accepts the event only when its execution ID is the
// Mission's current durable root execution.
function completedLeadStep(state, event) {
  const data = eventData(event);
  if (!data || typeof data.assistantMessageID !== "string" || !data.assistantMessageID) return null;
  const entry = state.messages.get(data.assistantMessageID);
  const agent =
    (entry && typeof entry.agent === "string" && entry.agent) ||
    (entry && state.sessionAgents.get(entry.sessionID)) ||
    (typeof data.sessionID === "string" ? state.sessionAgents.get(data.sessionID) : null);
  const sessionID =
    (entry && typeof entry.sessionID === "string" && entry.sessionID) ||
    (typeof data.sessionID === "string" ? data.sessionID : null);
  if (data.finish !== "stop") return null;
  if (!entry) return null;
  const text = assembledText(entry);
  if (!text) return null;
  return { sessionID, messageID: data.assistantMessageID, agent, text };
}

// Drop every in-flight step of a session: an interrupted or failed execution
// must leave no partial text behind that a later event could report.
function dropSession(state, event) {
  const data = eventData(event);
  if (!data || typeof data.sessionID !== "string" || !data.sessionID) return;
  for (const [messageID, entry] of state.messages) {
    if (entry.sessionID === data.sessionID) state.messages.delete(messageID);
  }
}

// Handle exactly one event: remember diagnostic agent metadata and streaming
// text, and report one completed assistant step. The adapter never decides
// root-ness from an agent name and never writes; the Rust bridge checks the
// current Mission execution binding and writes atomically.
async function captureLeadOutput(event, state) {
  if (!event || typeof event.type !== "string") return;
  switch (event.type) {
    case "session.step.started":
      trackStepStarted(state, event);
      return;
    case "session.agent.selected":
      trackAgentSelected(state, event);
      return;
    case "session.text.delta":
    case "session.text.ended":
      recordText(state, event);
      return;
    case "session.step.ended": {
      const completed = completedLeadStep(state, event);
      // The step is over either way: its state is never useful again.
      const data = eventData(event);
      if (data && typeof data.assistantMessageID === "string") {
        state.messages.delete(data.assistantMessageID);
      }
      if (!completed) return;
      const persisted = await bridge("lead.output", {
        session_id: completed.sessionID,
        message_id: completed.messageID,
        agent: completed.agent,
        text: completed.text,
      });
      // Context governance is deliberately sequenced after output
      // persistence. A completed `stop` step is the only root-Lead boundary
      // considered safe here; intermediate tool-call steps and failed steps
      // never claim that the Mission is ready for replacement. The payload
      // contains token counters and ids, never transcript text or credentials.
      if (contextGovernorEnabled() && persisted && (persisted.ok === true || persisted.disabled === true)) {
        const data = eventData(event) || {};
        await bridge("context.observe", {
          session_id: completed.sessionID,
          event_id: typeof event.id === "string" ? event.id : undefined,
          assistant_message_id: completed.messageID,
          agent: completed.agent,
          finish: data.finish,
          tokens: data.tokens,
          safe_boundary: data.finish === "stop",
          output_persisted: true,
        });
      }
      return;
    }
    case "session.step.failed": {
      const data = eventData(event);
      if (data && typeof data.assistantMessageID === "string") {
        state.messages.delete(data.assistantMessageID);
      }
      return;
    }
    case "session.execution.interrupted":
    case "session.execution.failed":
      dropSession(state, event);
      return;
    default:
      return;
  }
}

// Spawn `ocg __bridge <event> --project <project>` with an exact argv vector and
// a JSON payload on stdin. The child inherits the exact process environment.
// Absence, a spawn error, a non-zero exit and invalid JSON all resolve to null:
// the caller then leaves the session untouched.
async function bridge(event, payload) {
  try {
    const child = spawn(executable(), ["__bridge", event, "--project", project()], {
      shell: false,
      stdio: ["pipe", "pipe", "ignore"],
      env: process.env,
    });
    const finished = new Promise((resolve) => {
      let stdout = "";
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk) => {
        stdout += chunk;
      });
      child.on("error", () => resolve(null));
      child.on("close", (code) => resolve(code === 0 ? stdout : null));
    });
    try {
      // A spawn failure can close stdin before the write; that must stay soft.
      child.stdin.on("error", () => {});
      child.stdin.end(JSON.stringify(payload));
    } catch (_) {
      // stdin may already be closed; the bridge still answers or fails soft.
    }
    const text = await finished;
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (_) {
      return null;
    }
  } catch (_) {
    return null;
  }
}

// STRICT bridge for Lead authority gate (session.prompt ONLY).
// Throws on ANY failure: spawn, non-zero exit, timeout, empty stdout, invalid JSON, ok:false.
const STRICT_BRIDGE_TIMEOUT_MS = 30000;
const STRICT_BRIDGE_KILL_GRACE_MS = 100;

async function strictBridge(event, payload, timeoutMs = STRICT_BRIDGE_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn(executable(), ["__bridge", event, "--project", project()], {
        shell: false,
        stdio: ["pipe", "pipe", "ignore"],
        env: process.env,
      });
    } catch (e) {
      return reject(new Error(`OCG strict bridge spawn failed: ${e.message}`));
    }

    let stdout = "";
    let settled = false;
    let timer;
    let killTimer;
    const settle = (fn, value) => {
      if (settled) return false;
      settled = true;
      clearTimeout(timer);
      fn(value);
      return true;
    };
    const killTimedOutChild = () => {
      try {
        const terminated = child.kill("SIGTERM");
        // Node returns false when the child is already gone. If SIGTERM did
        // not reach a still-running child, escalate without waiting.
        if (terminated === false) child.kill("SIGKILL");
      } catch (_) {
        try { child.kill("SIGKILL"); } catch (_) {}
      }
      killTimer = setTimeout(() => {
        try { child.kill("SIGKILL"); } catch (_) {}
      }, STRICT_BRIDGE_KILL_GRACE_MS);
    };
    timer = setTimeout(() => {
      if (settled) return;
      killTimedOutChild();
      settle(reject, new Error("OCG strict bridge timeout"));
    }, timeoutMs);

    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.on("error", (e) => {
      settle(reject, new Error(`OCG strict bridge spawn error: ${e.message}`));
    });
    child.on("close", (code) => {
      if (settled) {
        clearTimeout(killTimer);
        return;
      }
      if (code !== 0) return settle(reject, new Error(`OCG strict bridge exited ${code}`));
      if (!stdout.trim()) return settle(reject, new Error("OCG strict bridge empty response"));
      try {
        const parsed = JSON.parse(stdout);
        if (parsed.ok === false) return settle(reject, new Error(parsed.error || "OCG strict bridge returned ok:false"));
        settle(resolve, parsed);
      } catch (e) {
        settle(reject, new Error(`OCG strict bridge invalid JSON: ${e.message}`));
      }
    });
    try {
      child.stdin.on("error", () => {});
      child.stdin.end(JSON.stringify(payload));
    } catch (_) {
      // stdin may already be closed; let close handler deal with it
    }
  });
}

function hasContext(text) {
  return typeof text === "string" && text.includes(START) && text.includes(END);
}

function suffix(context) {
  return "\n\n" + START + "\n" + context + "\n" + END + "\n";
}

function textParts(parts) {
  return (parts || []).filter(
    (part) => part && part.type === "text" && typeof part.text === "string",
  );
}

function appendToParts(parts, context) {
  const texts = textParts(parts);
  if (texts.length === 0) return;
  const target = texts[texts.length - 1];
  if (hasContext(target.text)) return;
  target.text = target.text + suffix(context);
}

function appendToPrompt(input, context) {
  if (!input || typeof input.prompt !== "string" || hasContext(input.prompt)) return;
  input.prompt = input.prompt + suffix(context);
}

// A complete durable dispatch witness, or nothing. A partial or malformed
// value is treated as absent so the bridge fails closed instead of guessing.
function isWitness(value) {
  if (!value || typeof value !== "object") return false;
  const required = ["job_id", "attempt_id", "executor_id", "call_id"];
  for (const key of required) {
    if (typeof value[key] !== "string" || value[key].length === 0) return false;
  }
  return Number.isSafeInteger(value.generation) && value.generation > 0;
}

// A completed foreground delivery carries both a structured nested output
// (`result.output.output`) and a visible content body (a string or content
// parts). Feedback is appended to both. Metadata and non-text content parts are
// never replaced or dropped.
function appendFeedback(delivery, context) {
  if (!delivery || typeof delivery !== "object") return;
  const nested = delivery.output;
  if (nested && typeof nested === "object" && typeof nested.output === "string") {
    if (!hasContext(nested.output)) nested.output = nested.output + suffix(context);
  }
  if (typeof delivery.content === "string") {
    if (!hasContext(delivery.content)) delivery.content = delivery.content + suffix(context);
  } else if (Array.isArray(delivery.content)) {
    appendToParts(delivery.content, context);
  }
}

function deliveryHasContext(delivery) {
  if (!delivery || typeof delivery !== "object") return false;
  const nested = delivery.output;
  if (nested && typeof nested === "object" && hasContext(nested.output)) return true;
  if (hasContext(delivery.content)) return true;
  if (Array.isArray(delivery.content)) {
    return textParts(delivery.content).some((part) => hasContext(part.text));
  }
  return false;
}

// A dependency-free structural default export. OpenCode 2 only requires a
// default `{ id, setup }` (or `{ id, effect }`) object, so the adapter imports
// no SDK package and keeps working across the renamed `@opencode/plugin`
// package layouts (the V2 tag has no `@opencode-ai/plugin/v2/promise` alias).
export default {
  id: "ocg-orchestration",
  setup: async (ctx) => {
    const registrations = [];

    // The `prompt` hook fires exactly once per genuinely admitted user prompt.
    // It is strictly non-mutating: OpenCode persists whatever `event.prompt`
    // contains after the hook, so the admitted text, files, metadata and
    // delivery are only ever read, never touched — the persisted user message
    // stays byte-verbatim what the user submitted.
    //
    // Lead authority gate (fail-closed): uses strictBridge which throws on
    // ANY failure (spawn, non-zero exit, timeout, empty stdout, invalid JSON,
    // explicit ok:false). A throwing prompt hook fails prompt admission,
    // preventing the model dispatch entirely. This enforces:
    //   resolved Lead contract -> bind/verify session agent+model -> only then permit inference
    registrations.push(await ctx.session.hook("prompt", async (event) => {
      if (!orchestrationEnabled()) return;
      if (!event || !event.sessionID) return;
      const prompt = event.prompt;
      const text = prompt && typeof prompt.text === "string" ? prompt.text : "";
      if (!text.trim()) return;
      // A worker session's own task prompt carries the OCG-owned dispatch
      // envelope from the Run that created it. Remembering it is what lets this
      // session delegate recursively for its own subtree; the bridge
      // re-validates the Run the witness names, so the cache is never
      // authority.
      const WITNESS_START = "<<<OCG:ATTEMPT_WITNESS v1>>>";
      const WITNESS_END = "<<<OCG:ATTEMPT_WITNESS:END>>>";
      const begin = text.indexOf(WITNESS_START);
      if (begin !== -1) {
        const from = begin + WITNESS_START.length;
        const stop = text.indexOf(WITNESS_END, from);
        if (stop !== -1) {
          try {
            const witness = JSON.parse(text.slice(from, stop).trim());
            if (isWitness(witness)) ownWitness.set(event.sessionID, witness);
          } catch (_) {
            // A malformed envelope is simply absent: the bridge fails closed.
          }
        }
      }
      // strictBridge throws on any failure -> prompt admission fails -> zero model dispatch
      await strictBridge("session.prompt", {
        session_id: event.sessionID,
        text,
      });
    }));

    // Provider Gateway correlation only. Ownership and economic admission
    // are decided by Rust after authoritative lineage lookup, never here.
    registrations.push(await ctx.session.hook("model.request", (event) => {
      const invocation = process.env.OCG_PROVIDER_INVOCATION;
      const provider = process.env.OCG_PROVIDER_ID;
      if (!invocation || !provider || event?.model?.providerID !== provider) return;
      if (!event || typeof event.sessionID !== "string" || !event.headers) return;
      if (!/^[A-Za-z0-9_-]{1,120}$/.test(event.sessionID)) return;
      event.headers["x-ocg-session"] = event.sessionID;
      event.headers["x-ocg-request-kind"] = String(event.kind || "unknown").slice(0, 32);
      event.headers["x-ocg-invocation"] = invocation;
      event.headers["x-ocg-logical-operation"] = invocation;
    }));

    // The `context` hook fires for the agent loop of every session, including
    // tool-driven continuations, and its changes apply only to the outgoing
    // model call — never to persisted history. The event carries the session's
    // current agent directly, so the root Lead is identified exactly the way
    // the delegation hooks identify a Lead caller: a worker/subagent session
    // (`ocg-*`) or any other agent is skipped, and no session lookup or
    // parentID tracking is needed. The persisted user message is never
    // touched: the baseline is pushed onto the request's system context. The
    // delimiter guard keeps a repeated invocation on the same request from
    // adding a second copy. Dispatch-time conversation content (including
    // synthetic user-role continuation messages) is never reported: task
    // identity is owned by the `prompt` admission hook above.
    registrations.push(await ctx.session.hook("context", async (event) => {
      if (!orchestrationEnabled()) return;
      if (!event || !event.sessionID) return;
      if (!isLead(event.agent)) return;
      if (!Array.isArray(event.system)) return;
      const already = event.system.some(
        (part) => part && part.type === "text" && hasContext(part.text),
      );
      if (already) return;
      const result = await bridge("session.context", {
        session_id: event.sessionID,
        agent: event.agent,
      });
      const context = result && typeof result.context === "string" ? result.context : "";
      if (context) {
        event.system.push({ type: "text", text: START + "\n" + context + "\n" + END });
      }
    }));

    // The generic Tool boundary consumes a direct mutation of
    // `event.input.prompt`. The delimiter is checked before the bridge so a
    // repeated hook can never advance the controller twice.
    registrations.push(await ctx.tool.hook("execute.before", async (event) => {
      if (!orchestrationEnabled()) return;
      if (!event || event.tool !== "subagent") return;
      const input = event.input && typeof event.input === "object" ? event.input : {};
      // An invocation that already carries a canonical witness was dispatched
      // by a previous call for this exact input. Re-dispatching would create a
      // second child WorkNode, so this hook is a no-op for it.
      if (isWitness(input.ocg_witness)) return;
      // A Lead always owns a delegation, and so does an OCG Worker that is
      // itself an authoritative canonical Run (recursive child creation).
      // Any other caller is still ignored.
      const cached = isWitness(ownWitness.get(event.sessionID)) ? ownWitness.get(event.sessionID) : null;
      if (!isWitness(input.ocg_caller_witness) && cached && !isWitness(input.ocg_witness)) {
        input.ocg_caller_witness = cached;
      }
      const canonicalCaller = isWitness(input.ocg_caller_witness);
      if (!canonicalCaller && !isLead(event.agent)) return;
      if (!isWorker(input.agent)) return;
      if (typeof input.prompt !== "string" || input.prompt.length === 0) return;
      if (hasContext(input.prompt)) return;
      const result = await strictBridge("tool.execute.before", {
        session_id: event.sessionID,
        tool: event.tool,
        args: input,
      });
      if (!isWitness(result.witness)) throw new Error("canonical execution witness missing");
      // The bridge may have created a canonical child WorkNode + Run and
      // returned its durable dispatch witness. The witness is attached to the
      // *exact* subagent input object the host hands back to `execute.after`,
      // so the completion is correlated by Run identity and never by prompt,
      // role, session ordering or ready order. It is a structured field, not
      // model-visible prose.
      if (result && result.witness && isWitness(result.witness)) {
        input.ocg_witness = result.witness;
      }
      if (result && result.context) appendToPrompt(input, result.context);
    }));

    // Only a completed foreground subagent has `result.output` with a
    // `completed` status. A background subagent is still `running` when the
    // tool returns, so asynchronous completion feedback is not supported: the
    // hook has no later delivery to consume and OCG must not guess that a
    // still-running task finished. Errors and missing deliveries are ignored.
    // As above, the delimiter is checked before the bridge.
    registrations.push(await ctx.tool.hook("execute.after", async (event) => {
      if (!orchestrationEnabled()) return;
      if (!event || event.tool !== "subagent") return;
      const canonicalInput = event.input && typeof event.input === "object" ? event.input : {};
      // A witnessed completion is delivered from a Lead or from an OCG Worker
      // that is itself an authoritative canonical Run.
      const witnessed = isWitness(canonicalInput.ocg_witness);
      if (!witnessed && !isLead(event.agent)) return;
      if (!isWorker(canonicalInput.agent)) return;
      if (event.status !== "completed") return;
      const delivery = event.result;
      if (!delivery || typeof delivery !== "object") return;
      const nested = delivery.output;
      if (!nested || typeof nested !== "object") return;
      if (nested.status !== "completed") return;
      if (typeof nested.output !== "string") return;
      if (deliveryHasContext(delivery)) return;
      // The witness attached to this exact invocation travels back unchanged.
      // When it is present the canonical Run decides the result; the legacy
      // session/phase correlation is not consulted.
      const witness = event.input && event.input.ocg_witness;
      const result = await bridge("tool.execute.after", {
        session_id: event.sessionID,
        tool: event.tool,
        args: event.input,
        result: nested.output,
        host_session_id: typeof nested.sessionID === "string" ? nested.sessionID : undefined,
        witness: isWitness(witness) ? witness : undefined,
      });
      if (result && result.context) appendFeedback(delivery, result.context);
    }));

    // Raw latest-Lead-output capture. The event stream is a best-effort bus:
    // the pump swallows every failure, and the subscription is disposed with
    // the other registrations. When both output reporting and context
    // governance are disabled, or a runtime context lacks the event surface,
    // no stream is registered.
    if ((reportsLatestEnabled() || contextGovernorEnabled()) && ctx.event && typeof ctx.event.subscribe === "function") {
      let stream = null;
      const abort = new AbortController();
      try {
        // V2's subscription is global (not typed) and accepts an options
        // object. The signal is the supported lifecycle boundary; `return()`
        // below is retained for the small async-iterator fixture and older
        // compatible hosts.
        stream = ctx.event.subscribe({ signal: abort.signal });
      } catch (_) {
        try {
          // A host that rejects the options object still gets the capture:
          // subscribing without it is strictly better than reporting nothing.
          stream = ctx.event.subscribe();
        } catch (_) {
          stream = null;
        }
      }
      if (stream) {
        const capture = createCaptureState();
        let stopped = false;
        const pump = (async () => {
          try {
            for await (const event of stream) {
              if (stopped) break;
              try {
                await captureLeadOutput(event, capture);
              } catch (_) {
                // fail soft: a report must never break a session
              }
            }
          } catch (_) {
            // fail soft: the event stream is best-effort
          }
        })();
        registrations.push({
          dispose: async () => {
            stopped = true;
            abort.abort();
            try {
              if (typeof stream.return === "function") await stream.return();
            } catch (_) {}
            if (reportsLatestEnabled()) await pump;
          },
        });
      }
    }

    return async () => {
      for (const registration of registrations) await registration.dispose();
    };
  },
};
"#;
