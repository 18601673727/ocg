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
// The timeout is an internal parameter so tests can use a short bound while
// production keeps the 30 second contract.
const STRICT_BRIDGE_TIMEOUT_MS = 30000;
const STRICT_BRIDGE_KILL_GRACE_MS = 100;

function strictBridgeTimeoutMs() {
  // The production path has a fixed 30 second bound. The private global is
  // only an injection seam for the generated-plugin contract tests; callers
  // cannot configure the production timeout through the public plugin API.
  const injected = globalThis.__OCG_STRICT_BRIDGE_TIMEOUT_MS;
  return Number.isFinite(injected) && injected >= 0 ? injected : STRICT_BRIDGE_TIMEOUT_MS;
}

async function strictBridge(event, payload, timeoutMs = strictBridgeTimeoutMs()) {
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
  const required = ["mission_id", "work_node_id", "run_id", "run_generation", "runtime_execution_id", "dispatch_id"];
  for (const key of required) {
    if (value[key] === undefined || value[key] === null || value[key] === "") return false;
  }
  return Number.isInteger(value.work_node_id) && Number.isInteger(value.run_id) && Number.isInteger(value.run_generation);
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
      const WITNESS_START = "<<<OCG:RUN_WITNESS v1>>>";
      const WITNESS_END = "<<<OCG:RUN_WITNESS:END>>>";
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
      const result = await bridge("tool.execute.before", {
        session_id: event.sessionID,
        tool: event.tool,
        args: input,
      });
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The generated plugin runs under an embedded JavaScript runtime; these
    /// tests execute it under `node`. Node is the only optional tool here, so a
    /// missing node skips the execution tests — but never silently in a
    /// controlled environment that sets `OCG_REQUIRE_JS_TESTS=1`.
    fn node_or_skip(test: &str) -> bool {
        let available = std::process::Command::new("node")
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
            "node is required for the JavaScript plugin contract tests ({test}) but was not found"
        );
        eprintln!("skipping JavaScript contract execution ({test}): node is unavailable");
        false
    }

    fn write_plugin(dir: &tempfile::TempDir, script: &str) {
        std::fs::write(dir.path().join("plugin.mjs"), plugin_source()).unwrap();
        std::fs::write(dir.path().join("check.mjs"), script).unwrap();
    }

    fn run_plugin_script(dir: &tempfile::TempDir) -> Value {
        let output = std::process::Command::new("node")
            .arg("check.mjs")
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    #[test]
    fn source_is_shell_free_and_bridge_backed() {
        let source = plugin_source();
        assert!(source.contains("Bun.spawn"));
        assert!(source.contains("__bridge"));
        // The generated adapter must not route through a shell.
        assert!(!source.contains("child_process"));
        assert!(!source.contains("execSync"));
        assert!(!source.contains("sh -c"));
        assert!(source.contains(CONTEXT_START));
        assert!(source.contains(CONTEXT_END));
    }

    #[test]
    fn chat_message_only_targets_the_lead_but_task_hooks_stay_active() {
        let source = plugin_source();
        // The Lead guard is present for chat.message.
        assert!(
            source.contains("startsWith(\"lead-\")"),
            "chat.message must ignore worker subagent sessions"
        );
        assert!(source.contains("if (agent && !agent.startsWith(\"lead-\")) return;"));
        // The task before/after hooks are still registered unconditionally.
        assert!(source.contains("\"tool.execute.before\": async"));
        assert!(source.contains("\"tool.execute.after\": async"));
        assert!(source.contains("output.message.agent = contract.agent"));
        assert!(source.contains("providerID: contract.provider_id"));
        assert!(source.contains("modelID: contract.model_id"));
        // The variant is optional: it is only written when the contract has one.
        assert!(source.contains("if (contract.variant !== undefined)"));
        assert!(source.contains("model.variant = contract.variant"));
    }

    #[test]
    fn chat_message_enforces_sticky_lead_state_and_preserves_workers_when_node_is_available() {
        if !node_or_skip("chat_message_enforces_sticky_lead_state") {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        write_plugin(
            &dir,
            r#"import { server } from "./plugin.mjs";
const hooks = await server({});
// The exact contracts OCG exports for each throttle level.
const CONTRACTS = {
  low: {agent: "lead-low", provider_id: "openai", model_id: "gpt-5.6-sol", variant: "low"},
  mid: {agent: "lead-mid", provider_id: "openai", model_id: "gpt-5.6-sol", variant: "medium"},
  high: {agent: "lead-high", provider_id: "openai", model_id: "gpt-6-astra", variant: "low"},
};
// Mirrors upstream createUserMessage: an explicitly selected input variant wins
// over the agent variant, and a reused session keeps its sticky agent/model.
async function check(level, inputAgent, outputAgent, providerID, modelID, variant) {
  process.env.OCG_LEAD_CONTRACT = JSON.stringify(CONTRACTS[level]);
  process.env.OCG_ORCHESTRATION_ENABLED = "0";
  const output = {message: {agent: outputAgent, model: {providerID, modelID, variant}}, parts: []};
  await hooks["chat.message"]({sessionID: "session", agent: inputAgent}, output);
  return output.message;
}
const lead = {
  // Previous mid use left lead-mid sticky; low must still win.
  low: await check("low", "lead-mid", "lead-mid", "openai", "gpt-5.6-sol", "high"),
  // A session stuck on the old Sol/medium variant must not satisfy mid.
  mid: await check("mid", "lead-low", "lead-low", "openai", "gpt-5.6-sol", "low"),
  // A stale UI variant selection must not satisfy high, which needs Astra low.
  high: await check("high", "lead-low", "lead-low", "openai", "gpt-5.6-sol", "high"),
};
// A reused session whose sticky state is a different Execution Tier must be corrected.
const reused = await check("high", "lead-low", "lead-low", "openai", "gpt-5.6-sol", "medium");
// Workers keep their own configured route at every throttle level.
const worker = {
  low: await check("low", "ocg-build", "ocg-build", "opencode-go", "deepseek-v4.1-flash", "high"),
  mid: await check("mid", "ocg-explore", "ocg-explore", "volcengine-coding-plan", "kimi-k2.7-code", "default"),
  high: await check("high", "ocg-verify", "ocg-verify", "opencode-go", "glm-5.3-flash", "high"),
};
// A request whose agent cannot be established must be left alone rather than
// silently re-routed onto a Lead model.
const unknown = await check("high", "", "", "opencode-go", "deepseek-v4.1-flash", "high");
console.log(JSON.stringify({lead, reused, worker, unknown}));
"#,
        );
        let value = run_plugin_script(&dir);
        // `ocg low` -> lead-low / Sol / low, even with sticky mid state.
        assert_eq!(value["lead"]["low"]["agent"], json!("lead-low"));
        assert_eq!(
            value["lead"]["low"]["model"]["modelID"],
            json!("gpt-5.6-sol")
        );
        assert_eq!(value["lead"]["low"]["model"]["variant"], json!("low"));
        // `ocg mid` -> lead-mid / Sol / medium, even with a sticky Sol low.
        assert_eq!(value["lead"]["mid"]["agent"], json!("lead-mid"));
        assert_eq!(
            value["lead"]["mid"]["model"]["modelID"],
            json!("gpt-5.6-sol")
        );
        assert_eq!(value["lead"]["mid"]["model"]["variant"], json!("medium"));
        // `ocg high` -> lead-high / Astra / low, never a Sol fallback.
        assert_eq!(value["lead"]["high"]["agent"], json!("lead-high"));
        assert_eq!(
            value["lead"]["high"]["model"]["modelID"],
            json!("gpt-6-astra")
        );
        assert_eq!(value["lead"]["high"]["model"]["variant"], json!("low"));
        // A reused session cannot silently override the explicit throttle.
        assert_eq!(value["reused"]["agent"], json!("lead-high"));
        assert_eq!(value["reused"]["model"]["modelID"], json!("gpt-6-astra"));
        assert_eq!(value["reused"]["model"]["variant"], json!("low"));
        // Worker routing is unchanged for every throttle level.
        for (level, agent, model) in [
            ("low", "ocg-build", "deepseek-v4.1-flash"),
            ("mid", "ocg-explore", "kimi-k2.7-code"),
            ("high", "ocg-verify", "glm-5.3-flash"),
        ] {
            assert_eq!(value["worker"][level]["agent"], json!(agent));
            assert_eq!(value["worker"][level]["model"]["modelID"], json!(model));
        }
        // An unidentifiable agent is not a Lead and must not be rewritten.
        assert_eq!(value["unknown"]["agent"], json!(""));
        assert_eq!(
            value["unknown"]["model"]["modelID"],
            json!("deepseek-v4.1-flash")
        );
        assert_eq!(value["unknown"]["model"]["variant"], json!("high"));
    }

    #[test]
    fn chat_message_omits_a_variant_when_the_contract_has_none() {
        if !node_or_skip("chat_message_omits_a_variant_when_the_contract_has_none") {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        write_plugin(
            &dir,
            r#"import { server } from "./plugin.mjs";
const hooks = await server({});
process.env.OCG_ORCHESTRATION_ENABLED = "0";
async function check(contract, providerID, modelID, variant) {
  process.env.OCG_LEAD_CONTRACT = JSON.stringify(contract);
  const output = {message: {agent: "lead-custom", model: {providerID, modelID, variant}}, parts: []};
  await hooks["chat.message"]({sessionID: "session", agent: "lead-custom"}, output);
  return output.message;
}
// A provider-default contract (no variant) must not inherit a sticky variant
// and must not fabricate one.
const providerDefault = await check(
  {agent: "lead-custom", provider_id: "acme", model_id: "widget"},
  "acme", "widget", "high",
);
// An explicit variant is still enforced over any sticky selection.
const explicit = await check(
  {agent: "lead-custom", provider_id: "acme", model_id: "widget", variant: "max"},
  "acme", "widget", "low",
);
console.log(JSON.stringify({providerDefault, explicit}));
"#,
        );
        let value = run_plugin_script(&dir);
        assert_eq!(
            value["providerDefault"]["model"]["providerID"],
            json!("acme")
        );
        assert_eq!(
            value["providerDefault"]["model"]["modelID"],
            json!("widget")
        );
        assert!(
            value["providerDefault"]["model"].get("variant").is_none(),
            "provider-default contract must not carry a variant: {}",
            value["providerDefault"]["model"]
        );
        assert_eq!(value["explicit"]["model"]["variant"], json!("max"));
    }

    #[test]
    fn lead_contract_survives_the_observed_upstream_save_then_request_path() {
        if !node_or_skip("lead_contract_survives_the_upstream_save_then_request_path") {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        // A faithful reduction of the OpenCode 1.18.31 request lifecycle
        // observed in the installed runtime: `createUserMessage` builds the
        // user message (an explicitly selected input variant wins over the
        // agent variant), `chat.message` runs on that message object, the
        // object is then persisted, and `SessionPrompt.run` derives the actual
        // provider request from the persisted message. The test asserts the
        // request — not just the hook output — satisfies the throttle contract.
        write_plugin(
            &dir,
            r#"import { server } from "./plugin.mjs";
const hooks = await server({});
const CONTRACTS = {
  low: {agent: "lead-low", provider_id: "openai", model_id: "gpt-5.6-sol", variant: "low"},
  mid: {agent: "lead-mid", provider_id: "openai", model_id: "gpt-5.6-sol", variant: "medium"},
  high: {agent: "lead-high", provider_id: "openai", model_id: "gpt-6-astra", variant: "low"},
};
// OpenCode resolves an explicit/selected/sticky variant ahead of the agent
// variant. The historical "stuck on medium" symptom is a session carrying a
// previous level's variant; every case below must still land on the contract.
function createUserMessage(level, selectedVariant) {
  const contract = CONTRACTS[level];
  const agentVariant = contract.variant;
  const variant = selectedVariant ?? agentVariant;
  return {agent: contract.agent, model: {providerID: "openai", modelID: contract.model_id, variant}};
}
async function request(level, selectedVariant) {
  const output = {message: createUserMessage(level, selectedVariant), parts: []};
  process.env.OCG_LEAD_CONTRACT = JSON.stringify(CONTRACTS[level]);
  process.env.OCG_ORCHESTRATION_ENABLED = "0";
  await hooks["chat.message"]({sessionID: "session", agent: output.message.agent}, output);
  const saved = output.message; // SessionPrompt.updateMessage(j)
  // SessionPrompt.run derives the request from the saved message.
  return {agent: saved.agent, model: saved.model.modelID, variant: saved.model.variant};
}
const requests = {
  low_sticky_medium: await request("low", "medium"),
  mid_sticky_high: await request("mid", "high"),
  high_sticky_medium: await request("high", "medium"),
  low_no_selection: await request("low", null),
  mid_no_selection: await request("mid", null),
  high_no_selection: await request("high", null),
};
console.log(JSON.stringify(requests));
"#,
        );
        let value = run_plugin_script(&dir);
        for (name, agent, model, variant) in [
            ("low_sticky_medium", "lead-low", "gpt-5.6-sol", "low"),
            ("mid_sticky_high", "lead-mid", "gpt-5.6-sol", "medium"),
            ("high_sticky_medium", "lead-high", "gpt-6-astra", "low"),
            ("low_no_selection", "lead-low", "gpt-5.6-sol", "low"),
            ("mid_no_selection", "lead-mid", "gpt-5.6-sol", "medium"),
            ("high_no_selection", "lead-high", "gpt-6-astra", "low"),
        ] {
            assert_eq!(value[name]["agent"], json!(agent), "{name}");
            assert_eq!(value[name]["model"], json!(model), "{name}");
            assert_eq!(value[name]["variant"], json!(variant), "{name}");
        }
    }

    #[test]
    fn materialize_writes_under_ignored_state() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!is_installed(dir.path()));
        let path = materialize(dir.path()).unwrap();
        assert!(path.is_file());
        assert!(is_installed(dir.path()));
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".gitignore")).unwrap(),
            ".ocg/\n"
        );
        let uri = plugin_uri(dir.path()).unwrap();
        assert!(uri.starts_with("file://"));
        assert!(uri.ends_with(PLUGIN_FILE));
    }

    #[test]
    fn injection_preserves_user_plugins_and_dedups() {
        let mut config = json!({
            "plugin": ["my-plugin", ["other", {"x": 1}]]
        });
        let uri = "file:///tmp/project/.ocg/orchestration/plugin/ocg-orchestration.js";
        inject_plugin(&mut config, uri);
        let entries = config["plugin"].as_array().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], json!("my-plugin"));
        assert_eq!(entries[1], json!(["other", {"x": 1}]));
        assert_eq!(entries[2], json!(uri));
        // A second injection is idempotent.
        inject_plugin(&mut config, uri);
        assert_eq!(config["plugin"].as_array().unwrap().len(), 3);
        assert!(has_ocg_plugin(&config));
    }

    #[test]
    fn injection_creates_the_array_when_absent() {
        let mut config = json!({"model": "x"});
        inject_plugin(&mut config, "file:///tmp/ocg-orchestration.js");
        assert!(has_ocg_plugin(&config));
        assert_eq!(config["plugin"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn injection_uses_the_plural_key_for_v2_and_preserves_user_plugins() {
        let mut config = json!({"plugins": ["my-plugin"]});
        let uri = "file:///tmp/project/.ocg/orchestration/plugin/ocg-orchestration.js";
        inject_plugin_for(&mut config, "plugins", uri);
        assert!(config.get("plugin").is_none());
        let entries = config["plugins"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], json!("my-plugin"));
        assert_eq!(entries[1], json!(uri));
        assert!(has_ocg_plugin_for(&config, "plugins"));
        assert!(!has_ocg_plugin(&config));
        remove_ocg_plugin_for(&mut config, "plugins");
        assert_eq!(config["plugins"], json!(["my-plugin"]));
        assert!(!has_ocg_plugin_for(&config, "plugins"));
    }

    #[test]
    fn v2_source_uses_a_node_spawn_and_never_rewrites_the_request() {
        let source = v2_plugin_source();
        // The V2 runtime may be Node, so the bridge must not depend on Bun.
        assert!(source.contains("import { spawn } from \"node:child_process\""));
        assert!(source.contains("shell: false"));
        assert!(!source.contains("Bun.spawn"));
        assert!(source.contains("export default {"));
        assert!(source.contains("id: \"ocg-orchestration\""));
        assert!(source.contains("setup: async (ctx)"));
        assert!(source.contains("ctx.tool.hook(\"execute.before\""));
        assert!(source.contains("ctx.tool.hook(\"execute.after\""));
        // The repository baseline is injected at model dispatch through the
        // session `context` hook, which never mutates the admitted prompt.
        assert!(source.contains("ctx.session.hook(\"context\""));
        // Prompt admission is observed through a strictly read-only
        // `session.prompt` hook: it reports the admitted text to the bridge
        // and never touches the event.
        assert!(source.contains("ctx.session.hook(\"prompt\""));
        // The V2 prompt hook uses strictBridge for fail-closed authority enforcement.
        assert!(source.contains("strictBridge(\"session.prompt\""));
        assert!(source.contains("event.system.push({ type: \"text\""));
        assert!(source.contains("bridge(\"session.context\""));
        assert!(source.contains("bridge(\"context.observe\""));
        assert!(source.contains("output_persisted: true"));
        // Dispatch-time conversation content is never a task signal: the
        // context bridge payload carries no message text.
        assert!(!source.contains("lastUserText"));
        // The root Lead is identified from the context event's own agent.
        assert!(source.contains("isLead(event.agent)"));
        assert!(source.contains("event.tool !== \"subagent\""));
        // The exact direct-argv contract: `__bridge <event> --project <project>`.
        assert!(source.contains("[\"__bridge\", event, \"--project\", project()]"));
        // Only known OCG worker agents launched from a Lead are bridged.
        assert!(source.contains("isWorker(input.agent)"));
        assert!(source.contains("isWorker(canonicalInput.agent)"));
        assert!(source.contains("isWitness(input.ocg_caller_witness)"));
        assert!(source.contains("if (isWitness(input.ocg_witness)) return;"));
        // The v2 adapter is thinner: no request-message Lead enforcement, no
        // session lookup in the dispatch path and no prompt-decoration header.
        assert!(!source.contains("enforceLeadContract"));
        assert!(!source.contains("output.message.agent"));
        assert!(!source.contains("providerID:"));
        assert!(!source.contains("export const server"));
        assert!(!source.contains("contextWithMetadata"));
        assert!(!source.contains("ctx.session.get"));
    }

    #[test]
    fn tool_after_registration_ignores_a_missing_delivery() {
        let v1 = plugin_source();
        assert!(v1.contains("\"tool.execute.after\": async"));
        assert!(v1.contains("if (!input || input.tool !=="));
        assert!(v1.contains("if (!output) return;"));
        let v2 = v2_plugin_source();
        assert!(v2.contains("ctx.tool.hook(\"execute.after\""));
        // A non-completed tool call, a background (running) delivery and a
        // missing delivery are all ignored without a bridge call.
        assert!(v2.contains("if (event.status !== \"completed\") return;"));
        assert!(v2.contains("if (nested.status !== \"completed\") return;"));
        assert!(v2.contains("if (!delivery || typeof delivery !== \"object\") return;"));
    }

    // ---- OpenCode 2 controlled hook tests ---------------------------------
    //
    // These execute the generated V2 adapter under `node` against a fixture
    // bridge *executable*. The fixture is a real child process, so the tests
    // prove the exact direct argv, the inherited environment and that no shell
    // is involved (its path contains a space). No OCG Rust bridge runs.

    /// A fixture `ocg __bridge` executable: it records every invocation and
    /// answers with a deterministic context, or fails in a controlled mode.
    #[cfg(unix)]
    const FIXTURE_BRIDGE: &str = r#"#!/usr/bin/env node
const fs = require("node:fs");
const args = process.argv.slice(2);
let stdin = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { stdin += chunk; });
process.stdin.on("end", () => {
  const record = process.env.OCG_RECORD;
  if (record) {
    fs.appendFileSync(
      record,
      JSON.stringify({
        argv: args,
        pid: process.pid,
        project: process.env.OCG_PROJECT || null,
        marker: process.env.OCG_FIXTURE_MARKER || null,
        inherited: process.env.OCG_ORCHESTRATION_ENABLED || null,
        stdin,
      }) + "\n",
    );
  }
  const mode = process.env.OCG_FIXTURE_MODE || "context";
  if (mode === "nonzero") { process.exitCode = 7; return; }
  if (mode === "invalid") { process.stdout.write("this is not json"); return; }
  if (mode === "silent") return;
  if (mode === "hang") {
    // Exercise the parent's SIGKILL fallback: deliberately ignore SIGTERM and
    // keep the event loop alive until the child is forcibly killed.
    process.on("SIGTERM", () => {});
    setInterval(() => {}, 1000);
    return;
  }
  if (mode === "metadata") {
    process.stdout.write(JSON.stringify({
      ok: true,
      context: "fixture snapshot body",
      snapshot_id: "sha256:fixture",
      cached: false,
      estimated_tokens: 6700,
      bytes: 26800,
      file_count: 23,
      symbol_count: 117,
    }));
    return;
  }
  if (mode === "cached") {
    process.stdout.write(JSON.stringify({
      ok: true,
      context: "",
      snapshot_id: "sha256:fixture",
      cached: true,
      estimated_tokens: 6700,
      bytes: 26800,
      file_count: 23,
      symbol_count: 117,
    }));
    return;
  }
  let payload = {};
  try { payload = JSON.parse(stdin); } catch (_) {}
  let context = "fixture context for " + (args[1] || "unknown");
  if (args[1] === "tool.execute.after" && typeof payload.result === "string") {
    context = "fixture verification feedback";
  }
  process.stdout.write(JSON.stringify({ ok: true, context }));
});
"#;

    /// The shared adapter harness: imports the generated V2 default export,
    /// builds a mock hook context and points the bridge at the fixture.
    #[cfg(unix)]
    const JS_PRELUDE: &str = r#"import fs from "node:fs";
import plugin from "./plugin.mjs";

process.env.OCG_BRIDGE = process.env.OCG_BRIDGE_PATH;
process.env.OCG_PROJECT = process.cwd();
process.env.OCG_ORCHESTRATION_ENABLED = "1";
process.env.OCG_FIXTURE_RECORD = process.env.OCG_RECORD;

function makeCtx(session) {
  const hooks = {};
  const ctx = {
    session: {
      hook: async (name, cb) => { hooks[name] = cb; return { dispose: async () => {} }; },
      get: async () => session,
    },
    tool: {
      hook: async (name, cb) => { hooks[name] = cb; return { dispose: async () => {} }; },
    },
  };
  return { ctx, hooks };
}

function lines() {
  try {
    return fs.readFileSync(process.env.OCG_RECORD, "utf8").trim().split("\n").filter(Boolean).length;
  } catch (_) { return 0; }
}
"#;

    #[cfg(unix)]
    fn write_v2_plugin(dir: &tempfile::TempDir) {
        std::fs::write(dir.path().join("plugin.mjs"), v2_plugin_source()).unwrap();
    }

    /// Write the fixture bridge into a directory whose name contains a space.
    /// A shell-based spawn would split that path; a direct argv spawn does not.
    #[cfg(unix)]
    fn write_fixture_bridge(dir: &tempfile::TempDir) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let bridge_dir = dir.path().join("bridge dir");
        std::fs::create_dir_all(&bridge_dir).unwrap();
        let path = bridge_dir.join("fixture-bridge.js");
        std::fs::write(&path, FIXTURE_BRIDGE).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[cfg(unix)]
    fn write_check(dir: &tempfile::TempDir, script: &str) {
        std::fs::write(dir.path().join("check.mjs"), script).unwrap();
    }

    #[cfg(unix)]
    fn run_check(dir: &tempfile::TempDir, bridge: &Path, record: &Path) -> Value {
        let output = std::process::Command::new("node")
            .arg("check.mjs")
            .current_dir(dir.path())
            .env("OCG_BRIDGE_PATH", bridge)
            .env("OCG_RECORD", record)
            .env("OCG_FIXTURE_MARKER", "inherited-env")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    #[cfg(unix)]
    fn records(record: &Path) -> Vec<Value> {
        let text = std::fs::read_to_string(record).unwrap_or_default();
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[cfg(unix)]
    const CONTEXT_BODY: &str = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-mid" });
await plugin.setup(ctx);

// Prompt admission is a registered but strictly read-only surface (exercised
// by the admission test); the context dispatch hook is exercised here.
const promptHookRegistered = "prompt" in hooks;

const user = (text) => ({ role: "user", content: [{ type: "text", text }] });
const assistant = (text) => ({ role: "assistant", content: [{ type: "text", text }] });
const mkEvent = (sessionID, agent, messages) => ({
  sessionID,
  agent,
  system: [{ type: "text", text: "base system" }],
  messages,
  options: {},
  tools: {},
});
const baselines = (event) =>
  event.system.filter(
    (part) => part && part.type === "text" && part.text.includes("<<<OCG:DYNAMIC_CONTEXT v1>>>"),
  ).length;

// First dispatch of a root Lead session: exactly one baseline in `system`.
const first = mkEvent("s1", "lead-mid", [user("hello lead")]);
await hooks["context"](first);
// A repeated invocation on the same request must not add a second copy.
await hooks["context"](first);
const afterDuplicate = lines();

// A tool-driven continuation (same user message, no new prompt): exactly one
// baseline again — never zero because of a cache, never two.
const continuation = mkEvent("s1", "lead-mid", [user("hello lead"), assistant("working on it")]);
await hooks["context"](continuation);
const afterContinuation = lines();

// A synthetic interruption/resume continuation reaches the dispatch as a
// trailing user-role message. It is conversation content, not an admission:
// the dispatch is served exactly like any other and nothing from it is
// reported to the bridge as a task signal.
const synthetic = mkEvent("s1", "lead-mid", [
  user("hello lead"),
  assistant("working on it"),
  user("The previous response was interrupted. Continue from where you left off without repeating completed content."),
]);
await hooks["context"](synthetic);
const afterSynthetic = lines();

// The next user turn: still exactly one baseline.
const nextTurn = mkEvent("s1", "lead-mid", [user("hello lead"), assistant("done"), user("second turn")]);
await hooks["context"](nextTurn);
const afterNextTurn = lines();

// A worker/subagent session and an unrelated agent never receive the root
// Lead baseline.
const worker = mkEvent("s2", "ocg-explore", [user("explore the parser")]);
await hooks["context"](worker);
const other = mkEvent("s3", "general", [user("hi")]);
await hooks["context"](other);

// A separate root Lead session receives its own baseline.
const separate = mkEvent("s4", "lead-high", [user("another session")]);
await hooks["context"](separate);

console.log(JSON.stringify({
  promptHookRegistered,
  firstSystem: first.system,
  firstMessages: first.messages,
  afterDuplicate,
  continuationSystem: continuation.system,
  continuationMessages: continuation.messages,
  afterContinuation,
  syntheticBaselines: baselines(synthetic),
  afterSynthetic,
  nextTurnBaselines: baselines(nextTurn),
  afterNextTurn,
  workerSystem: worker.system,
  otherSystem: other.system,
  separateSystem: separate.system,
  totalLines: lines(),
}));
"#;

    #[test]
    fn v2_context_hook_supplies_one_baseline_per_lead_dispatch() {
        if !node_or_skip("v2_context_hook_supplies_one_baseline_per_lead_dispatch") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_fixture_bridge(&dir);
        let record = dir.path().join("record.jsonl");
        write_check(&dir, &format!("{JS_PRELUDE}{CONTEXT_BODY}"));
        let value = run_check(&dir, &bridge, &record);
        let records = records(&record);

        // Prompt admission is observed through its own read-only hook; the
        // dispatch hook never reports conversation content.
        assert_eq!(value["promptHookRegistered"], json!(true));

        // Dispatch #1: the base system part is preserved and exactly one
        // delimited baseline is pushed; the user message is untouched.
        let first_system = value["firstSystem"].as_array().unwrap();
        assert_eq!(first_system.len(), 2);
        assert_eq!(first_system[0]["text"], json!("base system"));
        assert_eq!(
            first_system[1]["text"],
            json!(
                "<<<OCG:DYNAMIC_CONTEXT v1>>>\nfixture context for session.context\n<<<OCG:END>>>"
            )
        );
        assert_eq!(
            value["firstMessages"][0]["content"][0]["text"],
            json!("hello lead")
        );
        let serialized_messages = serde_json::to_string(&value["firstMessages"]).unwrap();
        assert!(!serialized_messages.contains(CONTEXT_START));
        // A repeated invocation on the same request added no second copy.
        assert_eq!(value["afterDuplicate"], json!(1));

        // Tool-driven continuation: exactly one baseline again.
        let continuation_system = value["continuationSystem"].as_array().unwrap();
        assert_eq!(continuation_system.len(), 2);
        assert!(continuation_system[1]["text"]
            .as_str()
            .unwrap()
            .contains("fixture context for session.context"));
        assert_eq!(value["afterContinuation"], json!(2));
        let serialized_continuation =
            serde_json::to_string(&value["continuationMessages"]).unwrap();
        assert!(!serialized_continuation.contains(CONTEXT_START));

        // A synthetic user-role continuation is an ordinary dispatch: exactly
        // one baseline, and no task signal is derived from it.
        assert_eq!(value["syntheticBaselines"], json!(1));
        assert_eq!(value["afterSynthetic"], json!(3));

        // Next user turn: exactly one baseline again.
        assert_eq!(value["nextTurnBaselines"], json!(1));
        assert_eq!(value["afterNextTurn"], json!(4));

        // Worker and unrelated agents: no bridge, no baseline.
        assert_eq!(value["workerSystem"].as_array().unwrap().len(), 1);
        assert_eq!(value["otherSystem"].as_array().unwrap().len(), 1);

        // A separate root Lead session gets its own baseline.
        let separate_system = value["separateSystem"].as_array().unwrap();
        assert_eq!(separate_system.len(), 2);
        assert_eq!(value["totalLines"], json!(5));

        // Exact direct argv, inherited environment and payloads. Every
        // context dispatch carries only the session id and the event's agent —
        // never conversation text, so a dispatch can never look like a task.
        let root = dir.path().canonicalize().unwrap();
        assert_eq!(records.len(), 5);
        for (index, (session, agent)) in [
            ("s1", "lead-mid"),
            ("s1", "lead-mid"),
            ("s1", "lead-mid"),
            ("s1", "lead-mid"),
            ("s4", "lead-high"),
        ]
        .iter()
        .enumerate()
        {
            let argv: Vec<&str> = records[index]["argv"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item.as_str().unwrap())
                .collect();
            assert_eq!(
                argv,
                vec![
                    "__bridge",
                    "session.context",
                    "--project",
                    root.to_str().unwrap()
                ]
            );
            let stdin: Value =
                serde_json::from_str(records[index]["stdin"].as_str().unwrap()).unwrap();
            assert_eq!(
                stdin,
                json!({"session_id": session, "agent": agent}),
                "context dispatch {index} must not carry conversation text"
            );
        }
        assert_eq!(records[0]["project"], json!(root.to_str().unwrap()));
        assert_eq!(records[0]["marker"], json!("inherited-env"));
        assert_eq!(records[0]["inherited"], json!("1"));
    }

    #[cfg(unix)]
    const PROMPT_BODY: &str = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-mid" });
await plugin.setup(ctx);

// Mirrors SessionPrompt.prepare's trigger shape: the hook sees the admitted
// prompt, metadata and delivery for one session/message.
const mkPrompt = (sessionID, text) => ({
  sessionID,
  messageID: "m1",
  prompt: { text, files: [{ name: "a.rs" }], agents: ["lead-mid"] },
  metadata: { origin: "user" },
  delivery: "steer",
});

// A genuine admission is reported to the bridge, and the event is left
// byte-identical (the runtime persists `event.prompt` after the hook, so the
// user message must stay verbatim).
const admitted = mkPrompt("s1", "update the parser");
const before = JSON.stringify(admitted);
await hooks["prompt"](admitted);
const unchanged = JSON.stringify(admitted) === before;

// A re-admission of the same prompt is still reported; whether anything
// changed is the bridge's decision, not the adapter's.
await hooks["prompt"](mkPrompt("s1", "update the parser"));
const afterReAdmission = lines();

// Whitespace-only or empty prompt text is not an admission; malformed events
// are ignored.
await hooks["prompt"](mkPrompt("s1", "   "));
await hooks["prompt"](mkPrompt("s1", ""));
await hooks["prompt"]({});
await hooks["prompt"](undefined);

console.log(JSON.stringify({
  unchanged,
  afterReAdmission,
  totalLines: lines(),
}));
"#;

    #[test]
    fn v2_prompt_hook_reports_admissions_without_mutating_the_event() {
        if !node_or_skip("v2_prompt_hook_reports_admissions_without_mutating_the_event") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_fixture_bridge(&dir);
        let record = dir.path().join("record.jsonl");
        write_check(&dir, &format!("{JS_PRELUDE}{PROMPT_BODY}"));
        let value = run_check(&dir, &bridge, &record);
        let records = records(&record);

        // Exactly the admission and its re-admission were bridged; the
        // whitespace/empty/malformed events produced no bridge call.
        assert_eq!(value["unchanged"], json!(true));
        assert_eq!(value["afterReAdmission"], json!(2));
        assert_eq!(value["totalLines"], json!(2));
        assert_eq!(records.len(), 2);

        // Exact direct argv and payload: the admitted text, verbatim, with the
        // session id — nothing else.
        let root = dir.path().canonicalize().unwrap();
        for record in &records {
            let argv: Vec<&str> = record["argv"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item.as_str().unwrap())
                .collect();
            assert_eq!(
                argv,
                vec![
                    "__bridge",
                    "session.prompt",
                    "--project",
                    root.to_str().unwrap()
                ]
            );
            let stdin: Value = serde_json::from_str(record["stdin"].as_str().unwrap()).unwrap();
            assert_eq!(
                stdin,
                json!({"session_id": "s1", "text": "update the parser"})
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn v2_strict_prompt_timeout_kills_a_sigterm_ignoring_child() {
        if !node_or_skip("v2_strict_prompt_timeout_kills_a_sigterm_ignoring_child") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_fixture_bridge(&dir);
        let record = dir.path().join("record.jsonl");
        write_check(
            &dir,
            &format!(
                r#"{JS_PRELUDE}
globalThis.__OCG_STRICT_BRIDGE_TIMEOUT_MS = 1000;
process.env.OCG_FIXTURE_MODE = "hang";
const {{ ctx, hooks }} = makeCtx({{ agent: "lead-mid" }});
await plugin.setup(ctx);
let message = "";
try {{
  await hooks["prompt"]({{ sessionID: "timeout-session", prompt: {{ text: "hang" }} }});
}} catch (error) {{
  message = String(error && error.message ? error.message : error);
}}
console.log(JSON.stringify({{ timedOut: message.includes("timeout"), message }}));
"#
            ),
        );
        let value = run_check(&dir, &bridge, &record);
        assert_eq!(value["timedOut"], json!(true), "{value}");

        // The child records its pid synchronously when it reaches the fixture.
        // Under load the fixture process needs a moment to boot past the strict
        // timeout, so wait briefly for the record instead of requiring it
        // synchronously.
        let mut rows = Vec::new();
        for _ in 0..150 {
            rows = records(&record);
            if !rows.is_empty() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(rows.len(), 1, "the hanging child must reach the fixture");
        let pid = rows[0]["pid"].as_u64().unwrap().to_string();
        // The fixture deliberately ignores SIGTERM, so the child only dies when
        // strictBridge's SIGKILL grace fires and the OS reaps it. Poll instead
        // of checking once: a single `kill -0` can transiently report a
        // not-yet-killed or not-yet-reaped child as alive, which was flaky
        // under load.
        let mut alive = true;
        for _ in 0..200 {
            let current = std::process::Command::new("kill")
                .args(["-0", &pid])
                .status()
                .unwrap()
                .success();
            if !current {
                alive = false;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(!alive, "strictBridge left timed-out child {pid} alive");
    }

    /// A witness-aware fixture bridge. It answers `tool.execute.before` with a
    /// durable dispatch witness and answers `tool.execute.after` by echoing the
    /// witness it was given, so a fixture can prove that the *same* witness
    /// travels from dispatch to completion.
    #[cfg(unix)]
    const WITNESS_BRIDGE: &str = r#"#!/usr/bin/env node
const fs = require("node:fs");
// The generated adapter always spawns `__bridge <event> --project <dir>`.
const event = process.argv.slice(2)[1] || "";
let stdin = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { stdin += chunk; });
process.stdin.on("end", () => {
  const record = process.env.OCG_RECORD;
  if (record) fs.appendFileSync(record, JSON.stringify({ event, stdin }) + "\n");
  let payload = {};
  try { payload = JSON.parse(stdin); } catch (_) {}
  if (event === "tool.execute.before") {
    // Two sibling delegations from the same Lead get different Run witnesses,
    // even though the role and prompt are identical. The durable Run identity
    // is never a function of the prompt text.
    const seen = record ? fs.readFileSync(record, "utf8").trim().split("\n").filter(Boolean).length - 1 : 0;
    const witness = {
      mission_id: "wn-fixture",
      work_node_id: seen + 1,
      run_id: seen + 1,
      run_generation: 1,
      runtime_execution_id: "ocg-pending-fixture-" + (seen + 1),
      dispatch_id: "d-fixture-r" + (seen + 1) + "-g1",
    };
    process.stdout.write(JSON.stringify({ ok: true, canonical: true, context: "canonical hand-off", witness }));
    return;
  }
  if (event === "tool.execute.after") {
    const witness = (payload.args && payload.args.ocg_witness) || payload.witness || null;
    process.stdout.write(JSON.stringify({
      ok: true, canonical: true, disposition: "authoritative", applied: true, witness,
    }));
    return;
  }
  process.stdout.write(JSON.stringify({ ok: true, context: "" }));
});
"#;

    #[cfg(unix)]
    const WITNESS_BODY: &str = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-high" });
await plugin.setup(ctx);

// A Lead delegates twice with the identical role and prompt. The two children
// must receive two different durable Run witnesses.
const first = { tool: "subagent", sessionID: "s1", agent: "lead-high", input: { agent: "ocg-build", prompt: "identical objective" } };
const second = { tool: "subagent", sessionID: "s1", agent: "lead-high", input: { agent: "ocg-build", prompt: "identical objective" } };
await hooks["execute.before"](first);
await hooks["execute.before"](second);

// A repeated before hook for an already-dispatched invocation must not create a
// second child Run, so the bridge must not be called again for it.
const linesBeforeRepeat = lines();
await hooks["execute.before"](first);
const linesAfterRepeat = lines();

// Both invocations complete, each with its own witness.
const complete = (event) => ({
  ...event,
  status: "completed",
  result: { output: { sessionID: "host-" + (event.input.ocg_witness ? event.input.ocg_witness.run_id : 0), status: "completed", output: "done" } },
});
const firstAfter = complete(first);
const secondAfter = complete(second);
await hooks["execute.after"](firstAfter);
await hooks["execute.after"](secondAfter);

// A worker that is itself an authoritative canonical Run may delegate further:
// the nested call carries its own caller witness and is bridged.
const nested = {
  tool: "subagent",
  sessionID: "s2",
  agent: "ocg-build",
  input: { agent: "ocg-verify", prompt: "nested", ocg_caller_witness: first.input.ocg_witness },
};
await hooks["execute.before"](nested);

// A worker without a canonical caller witness is still ignored.
const orphan = { tool: "subagent", sessionID: "s3", agent: "ocg-debug", input: { agent: "ocg-build", prompt: "y" } };
await hooks["execute.before"](orphan);

const read = () => fs.readFileSync(process.env.OCG_RECORD, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
const records = read();
const byEvent = (name) => records.filter((record) => record.event === name);
console.log(JSON.stringify({
  firstWitness: first.input.ocg_witness,
  secondWitness: second.input.ocg_witness,
  repeatAddedCalls: linesAfterRepeat - linesBeforeRepeat,
  beforeCount: byEvent("tool.execute.before").length,
  afterCount: byEvent("tool.execute.after").length,
  nestedWitness: nested.input.ocg_witness,
  orphanWitness: orphan.input.ocg_witness ?? null,
  afterPayloads: byEvent("tool.execute.after").map((record) => JSON.parse(record.stdin)),
}));
"#;

    #[test]
    fn v2_hooks_carry_one_durable_witness_from_dispatch_to_completion() {
        if !node_or_skip("v2_hooks_carry_one_durable_witness") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_witness_bridge(&dir);
        let record = dir.path().join("witness-record.jsonl");
        write_check(&dir, &format!("{JS_PRELUDE}{WITNESS_BODY}"));
        let value = run_check(&dir, &bridge, &record);

        // The adapter attached a complete witness to each exact invocation.
        let first = &value["firstWitness"];
        let second = &value["secondWitness"];
        for witness in [first, second] {
            assert_eq!(witness["mission_id"], json!("wn-fixture"));
            assert_eq!(witness["run_generation"], 1);
            assert!(witness["dispatch_id"]
                .as_str()
                .unwrap()
                .starts_with("d-fixture-"));
        }
        // Two identical delegations got two different Runs.
        assert_ne!(first["run_id"], second["run_id"]);
        assert_ne!(first["dispatch_id"], second["dispatch_id"]);
        // A repeated before hook never re-dispatches the same invocation.
        assert_eq!(value["repeatAddedCalls"], json!(0));
        // Both completions carried their own witness back to the bridge.
        assert_eq!(value["beforeCount"], json!(3));
        assert_eq!(value["afterCount"], json!(2));
        let after = value["afterPayloads"].as_array().unwrap();
        assert_eq!(after[0]["witness"]["dispatch_id"], first["dispatch_id"]);
        assert_eq!(after[1]["witness"]["dispatch_id"], second["dispatch_id"]);
        // The host session reported by the runtime is delivered alongside the
        // exact witness it belongs to, so it can be bound to that Run.
        assert_eq!(
            after[0]["host_session_id"],
            json!(format!("host-{}", first["run_id"]))
        );
        assert_eq!(
            after[1]["host_session_id"],
            json!(format!("host-{}", second["run_id"]))
        );
        // A worker with a canonical caller witness may delegate further; one
        // without it is still ignored.
        assert!(value["nestedWitness"]["dispatch_id"].is_string());
        assert!(value["orphanWitness"].is_null());
    }

    #[cfg(unix)]
    fn write_witness_bridge(dir: &tempfile::TempDir) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.path().join("witness-bridge.js");
        std::fs::write(&path, WITNESS_BRIDGE).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[cfg(unix)]
    const BEFORE_BODY: &str = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-mid" });
await plugin.setup(ctx);

const event = {
  tool: "subagent",
  sessionID: "s1",
  agent: "lead-mid",
  messageID: "m1",
  id: "t1",
  input: { agent: "ocg-explore", description: "explore it", prompt: "explore the parser" },
};
await hooks["execute.before"](event);
const mutated = event.input.prompt;
const firstStdin = JSON.parse(fs.readFileSync(process.env.OCG_RECORD, "utf8").trim().split("\n")[0]).stdin;
// Repeating the hook must not advance the controller a second time.
await hooks["execute.before"](event);

// A non-OCG subagent launched from a Lead is never bridged.
const unknown = { tool: "subagent", sessionID: "s2", agent: "lead-mid", input: { agent: "general", prompt: "x" } };
await hooks["execute.before"](unknown);
// A known worker launched by a non-Lead caller is never bridged.
const workerCaller = { tool: "subagent", sessionID: "s3", agent: "ocg-debug", input: { agent: "ocg-build", prompt: "y" } };
await hooks["execute.before"](workerCaller);
// Any other tool is ignored.
const otherTool = { tool: "bash", sessionID: "s4", agent: "lead-mid", input: {} };
await hooks["execute.before"](otherTool);

console.log(JSON.stringify({
  mutated,
  firstStdin,
  countAfterRepeat: lines(),
  unknownPrompt: unknown.input.prompt,
  workerCallerPrompt: workerCaller.input.prompt,
}));
"#;

    #[test]
    fn v2_before_hook_bridges_only_known_workers_from_a_lead() {
        if !node_or_skip("v2_before_hook_bridges_only_known_workers") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_fixture_bridge(&dir);
        let record = dir.path().join("record.jsonl");
        write_check(&dir, &format!("{JS_PRELUDE}{BEFORE_BODY}"));
        let value = run_check(&dir, &bridge, &record);
        let records = records(&record);
        assert_eq!(records.len(), 1, "only the Lead/worker pair may bridge");
        let mutated = value["mutated"].as_str().unwrap();
        assert!(mutated.starts_with("explore the parser"));
        assert!(mutated.contains(CONTEXT_START) && mutated.contains(CONTEXT_END));
        assert!(mutated.contains("fixture context for tool.execute.before"));
        // The bridge saw the pre-mutation prompt and the raw subagent input.
        let stdin: Value = serde_json::from_str(value["firstStdin"].as_str().unwrap()).unwrap();
        assert_eq!(stdin["args"]["agent"], json!("ocg-explore"));
        assert_eq!(stdin["args"]["prompt"], json!("explore the parser"));
        assert_eq!(stdin["session_id"], json!("s1"));
        // Role exclusion leaves every other call untouched.
        assert_eq!(value["unknownPrompt"], json!("x"));
        assert_eq!(value["workerCallerPrompt"], json!("y"));
    }

    #[cfg(unix)]
    const AFTER_BODY: &str = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-mid" });
await plugin.setup(ctx);

const event = {
  tool: "subagent",
  sessionID: "s1",
  agent: "lead-mid",
  messageID: "m1",
  id: "t1",
  status: "completed",
  input: { agent: "ocg-build", description: "build it", prompt: "build" },
  result: {
    output: { sessionID: "sub1", status: "completed", output: "subagent finished" },
    content: "visible body",
    metadata: { keep: true, tokens: 12 },
  },
};
await hooks["execute.after"](event);
const nested = event.result.output.output;
const content = event.result.content;
const metadata = event.result.metadata;
const firstStdin = JSON.parse(fs.readFileSync(process.env.OCG_RECORD, "utf8").trim().split("\n")[0]).stdin;
// Idempotent: the delimiter check runs before the bridge.
await hooks["execute.after"](event);

// A content-part array appends to its last text part, never to the non-text.
const partsEvent = {
  tool: "subagent",
  sessionID: "s2",
  agent: "lead-mid",
  status: "completed",
  input: { agent: "ocg-verify", prompt: "verify" },
  result: {
    output: { status: "completed", output: "done" },
    content: [
      { type: "text", text: "first" },
      { type: "image", url: "x" },
      { type: "text", text: "last" },
    ],
  },
};
await hooks["execute.after"](partsEvent);

console.log(JSON.stringify({
  nested,
  content,
  metadata,
  firstStdin,
  count: lines(),
  parts: partsEvent.result.content,
}));
"#;

    #[test]
    fn v2_after_hook_appends_feedback_to_output_and_visible_content() {
        if !node_or_skip("v2_after_hook_appends_feedback") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_fixture_bridge(&dir);
        let record = dir.path().join("record.jsonl");
        write_check(&dir, &format!("{JS_PRELUDE}{AFTER_BODY}"));
        let value = run_check(&dir, &bridge, &record);
        let records = records(&record);
        // One completion, plus the text-part completion; the repeat adds none.
        assert_eq!(records.len(), 2);
        assert_eq!(value["count"], json!(2));
        let nested = value["nested"].as_str().unwrap();
        assert!(nested.starts_with("subagent finished"));
        assert!(nested.contains("fixture verification feedback"));
        let content = value["content"].as_str().unwrap();
        assert!(content.starts_with("visible body"));
        assert!(content.contains("fixture verification feedback"));
        // Metadata is preserved untouched.
        assert_eq!(value["metadata"], json!({"keep": true, "tokens": 12}));
        // The bridge received the bare nested output string as `result`.
        let stdin: Value = serde_json::from_str(value["firstStdin"].as_str().unwrap()).unwrap();
        assert_eq!(stdin["result"], json!("subagent finished"));
        assert_eq!(stdin["args"]["agent"], json!("ocg-build"));
        // The first text part is untouched, the non-text part survives and the
        // last text part carries the feedback.
        let parts = value["parts"].as_array().unwrap();
        assert_eq!(parts[0]["text"], json!("first"));
        assert_eq!(parts[1]["type"], json!("image"));
        assert!(parts[2]["text"].as_str().unwrap().starts_with("last"));
        assert!(parts[2]["text"]
            .as_str()
            .unwrap()
            .contains("fixture verification feedback"));
    }

    #[cfg(unix)]
    const IGNORE_BODY: &str = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-mid" });
await plugin.setup(ctx);

const running = {
  tool: "subagent", sessionID: "s1", agent: "lead-mid", status: "completed",
  input: { agent: "ocg-build", prompt: "b" },
  result: { output: { status: "running", output: "background" }, content: "bg" },
};
await hooks["execute.after"](running);

const errorStatus = {
  tool: "subagent", sessionID: "s2", agent: "lead-mid", status: "error", error: "boom",
  input: { agent: "ocg-build", prompt: "b" },
  result: { output: { status: "completed", output: "never" }, content: "never" },
};
await hooks["execute.after"](errorStatus);

const missing = { tool: "subagent", sessionID: "s3", agent: "lead-mid", status: "completed", input: { agent: "ocg-build", prompt: "b" } };
await hooks["execute.after"](missing);

const unknownRole = {
  tool: "subagent", sessionID: "s4", agent: "lead-mid", status: "completed",
  input: { agent: "general", prompt: "g" },
  result: { output: { status: "completed", output: "x" }, content: "x" },
};
await hooks["execute.after"](unknownRole);

const workerCaller = {
  tool: "subagent", sessionID: "s5", agent: "ocg-debug", status: "completed",
  input: { agent: "ocg-build", prompt: "b" },
  result: { output: { status: "completed", output: "x" }, content: "x" },
};
await hooks["execute.after"](workerCaller);

await hooks["execute.after"](undefined);
await hooks["execute.after"](null);

console.log(JSON.stringify({
  count: lines(),
  running: running.result,
  errorResult: errorStatus.result,
  missing: missing.result === undefined,
  unknownRole: unknownRole.result,
  workerCaller: workerCaller.result,
}));
"#;

    #[test]
    fn v2_after_hook_ignores_running_errors_and_missing_deliveries() {
        if !node_or_skip("v2_after_hook_ignores_running_errors") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_fixture_bridge(&dir);
        let record = dir.path().join("record.jsonl");
        write_check(&dir, &format!("{JS_PRELUDE}{IGNORE_BODY}"));
        let value = run_check(&dir, &bridge, &record);
        assert_eq!(records(&record).len(), 0);
        assert_eq!(value["count"], json!(0));
        // A running background subagent is not a completion and is untouched.
        assert_eq!(value["running"]["output"]["output"], json!("background"));
        assert_eq!(value["running"]["content"], json!("bg"));
        // An errored tool call is untouched.
        assert_eq!(value["errorResult"]["output"]["output"], json!("never"));
        assert_eq!(value["errorResult"]["content"], json!("never"));
        assert_eq!(value["missing"], json!(true));
        assert_eq!(value["unknownRole"]["output"]["output"], json!("x"));
        assert_eq!(value["workerCaller"]["content"], json!("x"));
    }

    #[test]
    fn v2_hooks_do_nothing_when_orchestration_is_disabled() {
        if !node_or_skip("v2_hooks_do_nothing_when_disabled") {
            return;
        }
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            write_v2_plugin(&dir);
            let bridge = write_fixture_bridge(&dir);
            let record = dir.path().join("record.jsonl");
            let body = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-mid" });
await plugin.setup(ctx);
process.env.OCG_ORCHESTRATION_ENABLED = "0";

const promptEvent = {
  sessionID: "s0",
  messageID: "m0",
  prompt: { text: "update the parser" },
  metadata: {},
  delivery: "steer",
};
await hooks["prompt"](promptEvent);
const contextEvent = {
  sessionID: "s1",
  agent: "lead-mid",
  system: [{ type: "text", text: "base system" }],
  messages: [{ role: "user", content: [{ type: "text", text: "hello" }] }],
  options: {},
  tools: {},
};
await hooks["context"](contextEvent);
const before = { tool: "subagent", sessionID: "s2", agent: "lead-mid", input: { agent: "ocg-build", prompt: "build" } };
await hooks["execute.before"](before);
const after = {
  tool: "subagent", sessionID: "s3", agent: "lead-mid", status: "completed",
  input: { agent: "ocg-build", prompt: "build" },
  result: { output: { status: "completed", output: "done" }, content: "visible" },
};
await hooks["execute.after"](after);

console.log(JSON.stringify({
  count: lines(),
  system: contextEvent.system,
  beforePrompt: before.input.prompt,
  afterOutput: after.result.output.output,
  afterContent: after.result.content,
}));
"#;
            write_check(&dir, &format!("{JS_PRELUDE}{body}"));
            let value = run_check(&dir, &bridge, &record);
            assert_eq!(records(&record).len(), 0);
            assert_eq!(value["count"], json!(0));
            assert_eq!(value["system"].as_array().unwrap().len(), 1);
            assert_eq!(value["system"][0]["text"], json!("base system"));
            assert_eq!(value["beforePrompt"], json!("build"));
            assert_eq!(value["afterOutput"], json!("done"));
            assert_eq!(value["afterContent"], json!("visible"));
        }
    }

    #[test]
    fn v2_bridge_fails_soft_on_absence_errors_and_invalid_json() {
        if !node_or_skip("v2_bridge_fails_soft") {
            return;
        }
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            write_v2_plugin(&dir);
            let bridge = write_fixture_bridge(&dir);
            let record = dir.path().join("record.jsonl");
            let body = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-mid" });
await plugin.setup(ctx);

async function runContext() {
  const event = {
    sessionID: "s",
    agent: "lead-mid",
    system: [{ type: "text", text: "base system" }],
    messages: [{ role: "user", content: [{ type: "text", text: "hello" }] }],
    options: {},
    tools: {},
  };
  await hooks["context"](event);
  return JSON.stringify(event.system);
}

// Missing executable: spawn emits an error.
process.env.OCG_BRIDGE = process.env.OCG_RECORD + ".does-not-exist";
const absent = await runContext();

// Non-zero exit.
process.env.OCG_BRIDGE = process.env.OCG_BRIDGE_PATH;
process.env.OCG_FIXTURE_MODE = "nonzero";
const nonzero = await runContext();

// Invalid JSON on stdout.
process.env.OCG_FIXTURE_MODE = "invalid";
const invalid = await runContext();

// Empty stdout.
process.env.OCG_FIXTURE_MODE = "silent";
const silent = await runContext();

console.log(JSON.stringify({ absent, nonzero, invalid, silent }));
"#;
            write_check(&dir, &format!("{JS_PRELUDE}{body}"));
            let value = run_check(&dir, &bridge, &record);
            // Every failure resolves to null, so the system context is never
            // mutated and the base system part survives untouched.
            let base = json!([{ "type": "text", "text": "base system" }]);
            for key in ["absent", "nonzero", "invalid", "silent"] {
                let parsed: Value = serde_json::from_str(value[key].as_str().unwrap()).unwrap();
                assert_eq!(parsed, base, "{key}");
            }
        }
    }

    #[test]
    fn v2_context_hook_pushes_the_raw_baseline_and_no_ops_on_an_empty_context() {
        if !node_or_skip("v2_context_hook_pushes_the_raw_baseline") {
            return;
        }
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            write_v2_plugin(&dir);
            let bridge = write_fixture_bridge(&dir);
            let record = dir.path().join("record.jsonl");
            let body = r#"
const { ctx, hooks } = makeCtx({ agent: "lead-mid" });
await plugin.setup(ctx);
const mkEvent = () => ({
  sessionID: "s1",
  agent: "lead-mid",
  system: [{ type: "text", text: "base system" }],
  messages: [{ role: "user", content: [{ type: "text", text: "hello lead" }] }],
  options: {},
  tools: {},
});

// The bridge supplies counts, but the adapter presents the baseline exactly
// as delivered — no presentation header is fabricated in the system context.
process.env.OCG_FIXTURE_MODE = "metadata";
const fresh = mkEvent();
await hooks["context"](fresh);

// A bridge answer with an empty context is a defensive no-op.
process.env.OCG_FIXTURE_MODE = "cached";
const empty = mkEvent();
await hooks["context"](empty);

console.log(JSON.stringify({
  freshSystem: fresh.system,
  emptySystem: empty.system,
  count: lines(),
}));
"#;
            write_check(&dir, &format!("{JS_PRELUDE}{body}"));
            let value = run_check(&dir, &bridge, &record);
            let fresh_system = value["freshSystem"].as_array().unwrap();
            assert_eq!(fresh_system.len(), 2);
            assert_eq!(fresh_system[0]["text"], json!("base system"));
            // Exactly the delimited bridge body — no estimated-token header.
            assert_eq!(
                fresh_system[1]["text"],
                json!("<<<OCG:DYNAMIC_CONTEXT v1>>>\nfixture snapshot body\n<<<OCG:END>>>")
            );
            let empty_system = value["emptySystem"].as_array().unwrap();
            assert_eq!(empty_system.len(), 1);
            assert_eq!(empty_system[0]["text"], json!("base system"));
            assert_eq!(value["count"], json!(2));
        }
    }

    #[test]
    fn v1_chat_message_swallows_a_broken_bridge() {
        if !node_or_skip("v1_chat_message_swallows_a_broken_bridge") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        // Under `node` there is no `Bun`, so the V1 spawn path throws. The
        // adapter must swallow it and leave the prompt untouched.
        write_plugin(
            &dir,
            r#"import { server } from "./plugin.mjs";
const hooks = await server({});
process.env.OCG_LEAD_CONTRACT = JSON.stringify({agent: "lead-mid", provider_id: "acme", model_id: "widget"});
process.env.OCG_ORCHESTRATION_ENABLED = "1";
const output = {message: {agent: "lead-mid", model: {providerID: "acme", modelID: "widget"}}, parts: [{type: "text", text: "hello"}]};
await hooks["chat.message"]({sessionID: "s", agent: "lead-mid"}, output);
console.log(JSON.stringify({text: output.parts[0].text}));
"#,
        );
        let value = run_plugin_script(&dir);
        assert_eq!(value["text"], json!("hello"));
    }

    // -----------------------------------------------------------------
    // Latest-Lead-output capture (OpenCode 2 event stream)
    // -----------------------------------------------------------------

    /// The shared event-stream test prelude: a controllable async iterable and
    /// a context whose only extra surface is `event.subscribe`.
    #[cfg(unix)]
    const LEAD_OUTPUT_PRELUDE: &str = r#"import fs from "node:fs";
import plugin from "./plugin.mjs";

process.env.OCG_BRIDGE = process.env.OCG_BRIDGE_PATH;
process.env.OCG_PROJECT = process.cwd();
process.env.OCG_ORCHESTRATION_ENABLED = "1";
process.env.OCG_REPORTS_LATEST_LEAD_OUTPUT = process.env.OCG_REPORTS_FLAG || "1";
process.env.OCG_FIXTURE_RECORD = process.env.OCG_RECORD;

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

function makeCtx(stream) {
  const registrations = [];
  const subscribeCalls = [];
  return {
    subscriptions: registrations,
    subscribeCalls,
    ctx: {
      event: stream ? { subscribe: (options) => { subscribeCalls.push(options); return stream.iterator; } } : undefined,
      session: { hook: async () => ({ dispose: async () => {} }) },
      tool: { hook: async () => ({ dispose: async () => {} }) },
    },
  };
}

// Event fixtures in the real OpenCode 2.0.14 plugin envelope
// (`{id, created, type, location?, durable?, data}`; there is no
// `properties` bag) with the observed `session.*` payload shapes.
let eventSeq = 0;
const envelope = (type, data) => ({
  id: `evt_fixture_${++eventSeq}`,
  created: 1790120849467 + eventSeq,
  type,
  durable: { aggregateID: typeof data.sessionID === "string" ? data.sessionID : "ses_fixture", seq: eventSeq, version: 1 },
  location: { directory: process.cwd() },
  data,
});
const agentSelected = (sessionID, agent, extra = {}) =>
  envelope("session.agent.selected", { sessionID, agent, ...extra });
const stepStarted = (sessionID, messageID, agent, extra = {}) =>
  envelope("session.step.started", {
    sessionID,
    assistantMessageID: messageID,
    agent,
    model: { id: "fixture-model", providerID: "fixture-provider" },
    started: 1,
    ...extra,
  });
const textDelta = (sessionID, messageID, ordinal, delta) =>
  envelope("session.text.delta", { sessionID, assistantMessageID: messageID, ordinal, delta });
const textEnded = (sessionID, messageID, ordinal, text) =>
  envelope("session.text.ended", { sessionID, assistantMessageID: messageID, ordinal, text });
const stepEnded = (sessionID, messageID, finish, extra = {}) =>
  envelope("session.step.ended", { sessionID, assistantMessageID: messageID, finish, ...extra });
const stepFailed = (sessionID, messageID) =>
  envelope("session.step.failed", { sessionID, assistantMessageID: messageID });
const executionInterrupted = (sessionID) =>
  envelope("session.execution.interrupted", { sessionID });
const executionFailed = (sessionID) =>
  envelope("session.execution.failed", {
    sessionID,
    error: { type: "aborted", message: "Step interrupted" },
  });

function records() {
  try {
    return fs.readFileSync(process.env.OCG_RECORD, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
  } catch (_) { return []; }
}
"#;

    #[test]
    fn v2_capture_emits_completed_candidates_for_bridge_identity_filtering() {
        if !node_or_skip("v2_capture_emits_completed_candidates_for_bridge_identity_filtering") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_fixture_bridge(&dir);
        let record = dir.path().join("record.jsonl");
        write_check(
            &dir,
            &format!(
                r#"{LEAD_OUTPUT_PRELUDE}
const stream = makeStream();
const {{ ctx, subscribeCalls }} = makeCtx(stream);
const dispose = await plugin.setup(ctx);

// 1. An intermediate tool-call step is streaming text but is not a completed
//    response: `finish: "tool-calls"` never reports.
stream.push(stepStarted("s1", "m1", "lead-high"));
stream.push(textDelta("s1", "m1", 0, "checking the repository"));
stream.push(stepEnded("s1", "m1", "tool-calls"));

// 2. The final step of the same turn: deltas accumulate, `session.text.ended`
//    fixes the full ordinal text, and only `finish: "stop"` reports. The
//    second ordinal falls back to its accumulated deltas.
stream.push(stepStarted("s1", "m2", "lead-high"));
stream.push(textDelta("s1", "m2", 0, "first part, "));
stream.push(textDelta("s1", "m2", 0, "stale chunk"));
stream.push(textDelta("s1", "m2", 1, "second part"));
stream.push(textEnded("s1", "m2", 0, "first part, final"));
stream.push(stepEnded("s1", "m2", "stop", {{ tokens: {{ input: 1, output: 2 }} }}));

// 3. Worker and non-OCG sessions are still emitted as candidates; the Rust
//    bridge, which knows the Mission's current execution binding, filters them.
stream.push(stepStarted("s2", "m3", "ocg-build"));
stream.push(textEnded("s2", "m3", 0, "worker text"));
stream.push(stepEnded("s2", "m3", "stop"));
stream.push(stepStarted("s3", "m4", "general"));
stream.push(textEnded("s3", "m4", 0, "other text"));
stream.push(stepEnded("s3", "m4", "stop"));

// 4. A step whose `session.step.started` was missed still becomes a candidate;
//    the bridge remains the authority for current root execution identity.
stream.push(agentSelected("s4", "lead-low", {{ previous: "lead-high" }}));
stream.push(textEnded("s4", "m5", 0, "resumed final text"));
stream.push(stepEnded("s4", "m5", "stop"));
stream.push(textEnded("s9", "m9", 0, "anonymous text"));
stream.push(stepEnded("s9", "m9", "stop"));

// 5. An errored step and an interrupted execution never replace the last
//    completed output — even when a (pathological) stop arrives afterwards.
stream.push(stepStarted("s5", "m6", "lead-low"));
stream.push(textEnded("s5", "m6", 0, "errored text"));
stream.push(stepEnded("s5", "m6", "error"));
stream.push(stepStarted("s6", "m7", "lead-low"));
stream.push(textDelta("s6", "m7", 0, "interrupted text"));
stream.push(executionInterrupted("s6"));
stream.push(stepEnded("s6", "m7", "stop"));
stream.push(stepStarted("s7", "m8", "lead-mid"));
stream.push(textDelta("s7", "m8", 0, "failed text"));
stream.push(executionFailed("s7"));
stream.push(stepEnded("s7", "m8", "stop"));
stream.push(stepStarted("s8", "m10", "lead-low"));
stream.push(textEnded("s8", "m10", 0, "dropped step text"));
stream.push(stepFailed("s8", "m10"));

// Wait for the pump to drain and the bridge to answer, then dispose.
const deadline = Date.now() + 5000;
while (records().length < 10 && Date.now() < deadline) {{
  await new Promise((resolve) => setTimeout(resolve, 10));
}}
await dispose();
const all = records();
console.log(JSON.stringify({{ all, subscribed: !!ctx.event, hasSignal: !!subscribeCalls[0]?.signal }}, null, 0));
"#
            ),
        );
        let value = run_check(&dir, &bridge, &record);
        let all = value["all"].as_array().expect("records");
        assert_eq!(
            value["hasSignal"],
            json!(true),
            "V2 subscription must use AbortSignal"
        );
        assert_eq!(
            all.len(),
            10,
            "five completed candidates and five ordered context observations: {value}"
        );
        let outputs: Vec<&Value> = all
            .iter()
            .filter(|entry| entry["argv"][1] == json!("lead.output"))
            .collect();
        let contexts: Vec<&Value> = all
            .iter()
            .filter(|entry| entry["argv"][1] == json!("context.observe"))
            .collect();
        assert_eq!(
            outputs.len(),
            5,
            "the plugin emits every completed candidate for bridge-side identity filtering: {value}"
        );
        assert_eq!(
            contexts.len(),
            5,
            "each completed candidate gets one context boundary: {value}"
        );
        let payloads: Vec<Value> = outputs
            .iter()
            .map(|entry| serde_json::from_str(entry["stdin"].as_str().unwrap()).unwrap())
            .collect();
        assert_eq!(payloads[0]["session_id"], json!("s1"));
        assert_eq!(payloads[0]["agent"], json!("lead-high"));
        assert_eq!(payloads[0]["message_id"], json!("m2"));
        assert_eq!(
            payloads[0]["text"],
            json!("first part, final\n\nsecond part"),
            "ordered text ordinals only, ended text preferred over deltas: {:?}",
            payloads[0]
        );
        assert_eq!(payloads[1]["session_id"], json!("s2"));
        assert_eq!(payloads[1]["agent"], json!("ocg-build"));
        assert_eq!(payloads[2]["session_id"], json!("s3"));
        assert_eq!(payloads[2]["agent"], json!("general"));
        assert_eq!(payloads[3]["session_id"], json!("s4"));
        assert_eq!(payloads[3]["agent"], json!("lead-low"));
        assert_eq!(payloads[4]["session_id"], json!("s9"));
        assert!(payloads
            .iter()
            .all(|payload| payload["message_id"] != json!("m1")));
        for entry in contexts {
            let payload: Value = serde_json::from_str(entry["stdin"].as_str().unwrap()).unwrap();
            assert_eq!(payload["safe_boundary"], json!(true));
            assert_eq!(payload["output_persisted"], json!(true));
            assert!(
                payload.get("text").is_none(),
                "context telemetry must not replicate transcript text"
            );
        }
    }

    #[test]
    fn v2_lead_output_is_inert_when_disabled_or_without_the_event_surface() {
        if !node_or_skip("v2_lead_output_is_inert_when_disabled_or_without_the_event_surface") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        write_v2_plugin(&dir);
        let bridge = write_fixture_bridge(&dir);
        let record = dir.path().join("record.jsonl");
        write_check(
            &dir,
            &format!(
                r#"{LEAD_OUTPUT_PRELUDE}
// The switch is off: a completed Lead response must not reach the bridge.
const stream = makeStream();
const first = makeCtx(stream);
await (await plugin.setup(first.ctx))();
stream.push(stepStarted("s1", "m1", "lead-high"));
stream.push(textEnded("s1", "m1", 0, "must not be reported"));
stream.push(stepEnded("s1", "m1", "stop"));

// A context without the event surface must still register every other hook.
const second = makeCtx(null);
const dispose = await plugin.setup(second.ctx);
await dispose();

const all = records();
console.log(JSON.stringify({{ all }}));
"#
            ),
        );
        let _ = &record;
        // The disabled flag is read at setup time from the process environment.
        let output = std::process::Command::new("node")
            .arg("check.mjs")
            .current_dir(dir.path())
            .env("OCG_BRIDGE_PATH", &bridge)
            .env("OCG_RECORD", &record)
            .env("OCG_REPORTS_FLAG", "0")
            .env("OCG_CONTEXT_GOVERNOR_ENABLED", "0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["all"].as_array().unwrap().len(), 0, "{value}");
    }
}
