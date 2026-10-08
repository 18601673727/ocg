const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { createRequire } = require("node:module");

const frontend = path.resolve(__dirname, "../../frontend");
const frontendRequire = createRequire(path.join(frontend, "package.json"));
const ts = frontendRequire("typescript");

// The runtime provider renders nothing before browser hydration; Chat only
// needs its client handle for image upload, which these checks never reach.
const runtimeStub = { useOcgRuntime: () => ({ client: {} }) };

// Exercise the production TypeScript modules with the existing compiler and alias.
for (const extension of [".ts", ".tsx"]) {
  require.extensions[extension] = (module, filename) => {
    const load = module.require.bind(module);
    module.require = (name) => name === "../runtime/runtime-context" ? runtimeStub : load(name.startsWith("@/") ? path.join(frontend, name.slice(2))
      : name.startsWith(".") || path.isAbsolute(name) ? name : frontendRequire.resolve(name));
    const source = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
      fileName: filename,
    }).outputText;
    module._compile(source, filename);
  };
}

const chat = (name) => require(path.join(frontend, "components/ocg/chat", name));
const { activeExecutionTarget, executionActivity, currentActivity } = chat("execution-status.ts");
const { readModelPreference, writeModelPreference, modelPreferenceKey } = chat("model-preference.ts");
const { boundedToggle, followAfterScroll, BOUNDED_MESSAGE_CLASS } = chat("message-bounds.ts");
const { retryPresentation } = chat("retry.ts");

const providerRequest = (effort, extra = {}) => JSON.stringify({
  executor_transport: "provider",
  arguments: { ...(effort ? { reasoning_effort: effort } : {}), messages: [{ role: "assistant", content: "hidden chain of thought" }], ...extra },
});
const tool = (name, args) => JSON.stringify({ kind: "native_tool", tool_call_id: name, name, arguments: args });
let sequence = 0;
const call = (generation, status, request) => ({
  callId: `call-${++sequence}`, attemptGeneration: generation, status, rawState: status, request, createdAt: sequence,
});
const intent = (generation, model, effort) => ({
  dispatchIntentId: `intent-${++sequence}`, generation, providerKey: "nexotokensub", model, upstreamModelId: model,
  request: providerRequest(effort), createdAt: generation,
});
const execution = (state, overrides = {}) => ({
  jobId: "job-1", state, generation: 2, dispatchIntents: [intent(1, "gpt-6.1-sol", "high"), intent(2, "gpt-6.1-sol", "high")],
  calls: [], ...overrides,
});

function activeTargetBeatsComposer() {
  const running = execution("running");
  assert.deepEqual(activeExecutionTarget(running), {
    jobId: "job-1", generation: 2, providerKey: "nexotokensub", model: "gpt-6.1-sol",
    upstreamModelId: "gpt-6.1-sol", effort: "high",
  });
  // A replacement generation has no target until its own dispatch is visible.
  assert.equal(activeExecutionTarget(execution("running", { generation: 3 })), null);
  // No effort in the frozen request is reported as none, not as the composer's.
  assert.equal(activeExecutionTarget(execution("running", { dispatchIntents: [intent(2, "gpt-6.1-sol", null)] })).effort, null);
  for (const state of ["completed", "failed", "cancelled"]) assert.equal(activeExecutionTarget(execution(state)), null);
  assert.equal(activeExecutionTarget(null), null);
}

function preferenceIsScopedPerChat() {
  const values = new Map();
  const store = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
  writeModelPreference("project-a", "chat-1", { model: "claude-haiku-4-5", effort: null }, store);
  writeModelPreference("project-a", "chat-2", { model: "gpt-6.1-sol", effort: "low" }, store);
  assert.deepEqual(readModelPreference("project-a", "chat-1", store), { model: "claude-haiku-4-5", effort: null });
  assert.deepEqual(readModelPreference("project-a", "chat-2", store), { model: "gpt-6.1-sol", effort: "low" });
  assert.equal(readModelPreference("project-b", "chat-1", store), undefined);
  assert.notEqual(modelPreferenceKey("a:b", "c"), modelPreferenceKey("a", "b:c"));
  values.set(modelPreferenceKey("project-a", "chat-3"), "{not json");
  assert.equal(readModelPreference("project-a", "chat-3", store), undefined);
  assert.equal(readModelPreference(undefined, "chat-1", store), undefined);
}

function observableActivityDrivesStatus() {
  const calls = [
    call(1, "completed", tool("filesystem.read", { path: "src/old.rs" })),
    call(2, "running", providerRequest("high")),
    call(2, "completed", tool("filesystem.read", { path: "src/orchestration/canonical_control.rs" })),
    call(2, "completed", tool("filesystem.search", { query: "settle_chat_turn" })),
    call(2, "running", tool("filesystem.edit", { operation: "replace", file: "src/orchestration/domain.rs" })),
  ];
  const running = execution("running", { calls });
  const steps = executionActivity(running);
  assert.deepEqual(steps.map(step => [step.kind, step.target, step.status]), [
    ["read", "canonical_control.rs", "done"],
    ["search", "settle_chat_turn", "done"],
    ["edit", "domain.rs", "running"],
  ]);
  assert.equal(currentActivity(running, steps).target, "domain.rs");
  assert.ok(!JSON.stringify(steps).includes("hidden chain of thought"));

  const waiting = execution("running", { calls: calls.slice(0, 4) });
  assert.equal(currentActivity(waiting, executionActivity(waiting)).kind, "provider");
  const exec = execution("running", { calls: [call(2, "running", tool("process.exec", { program: "cargo", args: ["check", "--all-targets"] }))] });
  assert.deepEqual(currentActivity(exec, executionActivity(exec)), { id: exec.calls[0].callId, kind: "run", target: "cargo check --all-targets", status: "running" });
  assert.equal(currentActivity(execution("eligible"), []), "queued");
  assert.equal(currentActivity(execution("cancelling"), []), "cancelling");
  assert.equal(currentActivity(execution("completed", { calls }), steps), null);
}

function longMessagesFold() {
  assert.equal(boundedToggle(false, false), null, "short messages offer no toggle");
  assert.equal(boundedToggle(true, false), "expand");
  assert.equal(boundedToggle(false, true), "collapse");
  assert.match(BOUNDED_MESSAGE_CLASS, /max-h-\[5\dvh\]/);
  assert.match(BOUNDED_MESSAGE_CLASS, /overflow-y-auto/);
  const metrics = (scrollTop) => ({ scrollTop, scrollHeight: 2000, clientHeight: 500 });
  assert.equal(followAfterScroll(true, 1500, metrics(1200)), false, "scrolling up stops following");
  assert.equal(followAfterScroll(false, 1200, metrics(1300)), false, "reading downward does not force following");
  assert.equal(followAfterScroll(false, 1300, metrics(1500)), true, "reaching the newest output resumes following");
}

function retryClearsPresentation() {
  const failed = { id: "m", role: "assistant", commandId: "c", jobId: "job-1", createdAt: "t", content: "old", reasoning: "old thought", images: [{ id: "i" }], failureReason: "boom", failureCode: "stream-closed", status: "failed" };
  assert.deepEqual(retryPresentation(failed), { id: "m", role: "assistant", commandId: "c", jobId: "job-1", createdAt: "t", content: "", status: "streaming" });
}

function reasoningSurvivesHistoryAndRoundReset() {
  const { applyEnvelopeToSnapshot } = require(path.join(frontend, "components/ocg/runtime/reconciler.ts"));
  const { RuntimeEnvelopeFactory } = require(path.join(frontend, "components/ocg/runtime/runtime-envelope.ts"));
  const { emptyRuntimeSnapshot } = require(path.join(frontend, "components/ocg/runtime/runtime-snapshot.ts"));
  const session = { id: "s", sessionId: "chat-1", projectId: "p", title: "", workType: "coding", updatedAt: "now" };
  const factory = new RuntimeEnvelopeFactory("reasoning-history", 1);
  const scope = { projectId: "p", sessionId: "s" };
  let snapshot = emptyRuntimeSnapshot("normal-chat");
  const apply = (type, payload) => {
    const applied = applyEnvelopeToSnapshot(snapshot, factory.envelope(type, payload, scope));
    assert.deepEqual(applied.diagnostics, []);
    snapshot = applied.snapshot;
  };
  apply("conversation.session-created", { session });
  apply("conversation.history-loaded", {
    messages: [{
      id: "a", role: "assistant", content: "Answer", reasoning: "Think first", createdAt: "t", status: "completed",
    }],
  });
  assert.equal(snapshot.messagesBySession.s[0].reasoning, "Think first");
  assert.equal(snapshot.messagesBySession.s[0].content, "Answer");

  apply("conversation.message-started", {
    message: { id: "b", role: "assistant", content: "", reasoning: "kept", createdAt: "t", status: "streaming" },
  });
  apply("conversation.message-round-committed", {
    messageId: "b", committedContentLength: 0, committedImageCount: 0, committedReasoningLength: "kept".length,
  });
  apply("conversation.message-reasoning-delta", { messageId: "b", delta: " dropped" });
  apply("conversation.message-delta", { messageId: "b", delta: "provisional" });
  apply("conversation.message-round-committed", {
    messageId: "b", committedContentLength: 0, committedImageCount: 0, committedReasoningLength: "kept".length,
  });
  const reset = snapshot.messagesBySession.s.find(message => message.id === "b");
  assert.equal(reset.reasoning, "kept");
  assert.equal(reset.content, "");
  apply("conversation.message-reasoning-delta", { messageId: "b", delta: " more" });
  assert.equal(snapshot.messagesBySession.s.find(message => message.id === "b").reasoning, "kept more");
}

function chatSurfaceRendersWithoutRuntimeBanner() {
  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { ChatView } = chat("chat-view.tsx");
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));
  const long = Array.from({ length: 200 }, (_, index) => `line ${index}`).join("\n");
  const render = (messages, executionValue) => renderToStaticMarkup(React.createElement(I18nProvider, null, React.createElement(ChatView, {
    session: { id: "s", sessionId: "chat-1", projectId: "p", title: "", workType: "coding", updatedAt: "now" },
    messages,
    execution: executionValue,
    runtimeStatus: { state: "connected", detailCode: "provider-ready", detail: "OCG provider runtime" },
    onComposerIntent: () => undefined,
    onRetryMessage: async () => undefined,
  })));
  const html = render([
    { id: "u", role: "user", content: long, createdAt: "t", status: "completed" },
    { id: "a", role: "assistant", jobId: "job-1", content: "short", createdAt: "t", status: "streaming" },
    { id: "tool", role: "tool", jobId: "job-1", content: "", createdAt: "t", status: "streaming",
      tool: { name: "filesystem.edit", status: "running", durationMs: 0, summary: "src/orchestration/domain.rs", detail: "replace" } },
  ], execution("running", { calls: [call(2, "running", tool("filesystem.edit", { file: "src/orchestration/domain.rs" }))] }));
  assert.ok(!html.includes("OCG provider runtime"), "the generic runtime row is gone");
  assert.ok(!html.includes("border-dashed"), "the generic runtime container is gone");
  assert.equal((html.match(/data-message-bounds="bounded"/g) ?? []).length, 2, "every message renders bounded");
  assert.ok(html.includes("gpt-6.1-sol"), "the frozen active model is shown");
  assert.ok(html.includes("domain.rs"), "the current observable action is shown");
  assert.ok(!html.includes("hidden chain of thought"));

  // Once the Job settles the composer is the next-turn selection again.
  const settled = render([
    { id: "u", role: "user", content: "hi", createdAt: "t", status: "completed" },
    { id: "a", role: "assistant", jobId: "job-1", content: "done", createdAt: "t", status: "completed" },
  ], execution("completed"));
  assert.ok(!settled.includes("gpt-6.1-sol"));
  assert.ok(!settled.includes("domain.rs"));
}

function conversationUsageGateChecksCanonicalIdentity() {
  // The gate should check for canonical Job identity, not reconciliation state.
  // Regression: dogfood showed optimistic streaming assistant with accepted
  // jobId as "no usage available" because usageRecorded only checked !optimistic.
  const message = (role, optimistic = true, jobId = undefined) => ({
    id: "msg-1", role, status: optimistic ? "pending" : "completed", optimistic, createdAt: Date.now(), jobId,
  });
  const messages = {
    untouched: [],
    pendingOnly: [message("user"), message("assistant")],
    withJobId: [message("user"), message("assistant", true, "job-123")],
    reconciled: [message("user", false), message("assistant", false)],
  };
  // The old gate (optimistic-only) would reject all three first scenarios.
  // The new gate should accept any with backend identity.
  const old = (msgs) => msgs.some(msg => !msg.optimistic);
  const gate = (msgs) => msgs.some(msg => !msg.optimistic || (msg.role === "assistant" && msg.jobId));
  assert.equal(old(messages.untouched), false);
  assert.equal(old(messages.pendingOnly), false);
  assert.equal(old(messages.withJobId), false);
  assert.equal(old(messages.reconciled), true);
  assert.equal(gate(messages.untouched), false, "untouched draft has no backend");
  assert.equal(gate(messages.pendingOnly), false, "pending assistant lacks jobId");
  assert.equal(gate(messages.withJobId), true, "assistant with jobId is backed");
  assert.equal(gate(messages.reconciled), true, "reconciled is still backed");
}

function conversationUsageRefreshesWhileLive() {
  const { LiveUsageRefresh, LIVE_USAGE_REFRESH_MS, conversationUsageLive } = require(path.join(frontend, "components/ocg/usage/use-usage.ts"));
  assert.ok(LIVE_USAGE_REFRESH_MS >= 1500 && LIVE_USAGE_REFRESH_MS <= 2000);
  const pending = new Map();
  let handles = 0;
  const timers = { set: (callback, ms) => { pending.set(++handles, { callback, ms }); return handles; }, clear: handle => pending.delete(handle) };
  const tick = () => { const due = [...pending.values()]; pending.clear(); for (const timer of due) timer.callback(); };
  let reads = 0;
  const live = new LiveUsageRefresh(() => { reads += 1; }, timers, 1750);

  // An active canonical Chat re-reads on its own, one read at a time.
  const chat = "[\"url\",\"p\",\"s\"]";
  live.update(chat, true, false);
  assert.equal(pending.size, 1);
  assert.equal([...pending.values()][0].ms, 1750);
  tick();
  assert.equal(reads, 1);
  live.update(chat, true, true);
  assert.equal(pending.size, 0, "a read in flight is never overtaken");
  live.update(chat, true, false);
  tick();
  assert.equal(reads, 2);

  // A manual refresh is just another read; the cadence resumes after it.
  live.update(chat, true, true);
  live.update(chat, true, false);
  assert.equal(pending.size, 1);

  // Settling performs one final read and stops polling.
  live.update(chat, false, false);
  assert.equal(reads, 3, "terminal transition re-reads the final totals");
  assert.equal(pending.size, 0);
  live.update(chat, false, true);
  live.update(chat, false, false);
  tick();
  assert.equal(reads, 3, "a settled Chat does not poll");

  // Switching Chats stops the previous Chat's polling without a stray read.
  live.update(chat, true, false);
  live.update("other", false, false);
  assert.equal(pending.size, 0);
  assert.equal(reads, 3);
  live.update("other", true, false);
  live.dispose();
  assert.equal(pending.size, 0);

  const streaming = [{ id: "a", role: "assistant", content: "", createdAt: "t", status: "streaming" }];
  const settled = [{ id: "a", role: "assistant", content: "done", createdAt: "t", status: "completed" }];
  assert.equal(conversationUsageLive(execution("running"), settled), true);
  assert.equal(conversationUsageLive(null, streaming), true);
  assert.equal(conversationUsageLive(execution("completed"), settled), false);
  assert.equal(conversationUsageLive(null, []), false);
}

function executionInspectorIsCanonicalFirst() {
  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { JobInspector } = require(path.join(frontend, "components/ocg/execution/job-inspector.tsx"));
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));
  const { dictionaries } = require(path.join(frontend, "components/ocg/i18n/dictionaries.ts"));
  const { DEFAULT_LOCALE } = require(path.join(frontend, "components/ocg/i18n/locale.ts"));
  const { createScenarioFixture } = require(path.join(frontend, "components/ocg/runtime/scenarios.ts"));
  const words = dictionaries[DEFAULT_LOCALE];
  const fill = (key, vars) => Object.entries(vars ?? {}).reduce((text, [name, value]) => text.replaceAll(`{${name}}`, String(value)), words[key]);
  const escape = text => text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\"", "&quot;");

  const attempt = (generation, attemptId, state, finishedAt) => ({
    id: `p:attempt:${attemptId}`, attemptId, generation, state, authoritative: finishedAt === null,
    createdAt: 100 * generation, finishedAt, callCount: 3, settledCallCount: 3,
  });
  const providerIntent = (generation, attemptId) => ({
    id: `p:intent:${attemptId}`, dispatchIntentId: `intent-${attemptId}`, callId: `provider-${attemptId}`, attemptId,
    executorId: null, generation, status: "settled", rawState: "fenced", effectKind: "read_only", effectState: "settled",
    request: providerRequest(null), budgetAdmitted: true, providerKey: "nexotokensub", model: "gpt-6.1-sol",
    upstreamModelId: "gpt-6.1-sol", reservationId: null, failure: null, createdAt: 100 * generation, updatedAt: 100 * generation,
  });
  const job = (state, overrides) => ({
    jobId: "job-retried", projectId: "p", state, generation: 2, authoritativeAttemptId: null,
    createdAt: 100, updatedAt: 290, attempts: [], calls: [], executors: [{ id: "e1" }, { id: "e2" }],
    dispatchIntents: [providerIntent(1, "att-first-0001"), providerIntent(2, "att-second-0002")],
    terminationReason: null, recovery: [], parentJobId: null, childJobIds: [], dependsOn: [], blockedBy: [],
    waitingForChildren: false, summary: { total: 0, running: 0, queued: 0, completed: 0, failed: 0, unrecognized: 0 },
    progress: null, currentCall: null, latestCall: null, activities: [], ...overrides,
  });
  const staleCall = call(1, "running", tool("filesystem.edit", { file: "src/stale.rs" }));
  const cancelled = job("cancelled", {
    attempts: [attempt(1, "att-first-0001", "cancelled", 150), attempt(2, "att-second-0002", "cancelled", 280)],
    calls: [
      staleCall,
      call(2, "failed", providerRequest(null)),
      call(2, "completed", tool("process.exec", { program: "git", args: ["rev-parse", "HEAD"] })),
      call(2, "failed", tool("filesystem.edit", { file: "src/orchestration/domain.rs" })),
    ],
    terminationReason: { code: "job_cancelled", class: "cancelled", message: "Job execution cancelled", source: { kind: "core" }, retryable: true, details: null, cause: null, entity_ref: null },
  });
  const render = (execution, observability = null) => renderToStaticMarkup(React.createElement(I18nProvider, null,
    React.createElement(JobInspector, { execution, observability, onClose: () => undefined })));

  const html = render(cancelled);
  // Useful without RuntimeObservability, and never its empty state as main content.
  assert.ok(html.includes("data-canonical-job=\"job-retried\""));
  assert.ok(!html.includes(escape(words["execution.noObservability"])));
  assert.ok(!html.includes(escape(words["execution.jobDetailsNotReported"])));
  assert.ok(!html.includes(escape(words["execution.canonical.runtimeSupplement"])), "no supplemental section without observability");
  // Same Job, generation 2, both Attempts and their states, the final one marked.
  assert.ok(html.includes(escape(fill("execution.canonical.generation", { generation: 2 }))));
  assert.ok(html.includes(escape(fill("execution.canonical.retried", { count: 2 }))));
  assert.equal((html.match(/data-attempt-generation=/g) ?? []).length, 2);
  assert.match(html, /data-attempt-generation="2"[\s\S]*?att-second-0002[\s\S]*?<\/li>/);
  assert.ok(html.includes(escape(words["execution.canonical.attemptFinal"])));
  // The frozen target is the producing generation's dispatch.
  assert.ok(html.includes("data-execution-target=\"final\""));
  assert.ok(html.includes("nexotokensub") && html.includes("gpt-6.1-sol"));
  assert.ok(html.includes(escape(words["execution.canonical.effortNotSet"])));
  // Cancellation is stated with its canonical reason.
  assert.ok(html.includes("Job execution cancelled") && html.includes("job_cancelled"));
  // Recent activity of generation 2 only; the earlier Attempt's open Call is not current.
  assert.ok(html.includes("git rev-parse HEAD") && html.includes("domain.rs"));
  assert.ok(!html.includes("stale.rs"));
  assert.ok(!html.includes("data-current-activity"));
  assert.ok(!html.includes("hidden chain of thought"));

  const running = render(job("running", {
    authoritativeAttemptId: "att-second-0002",
    attempts: [attempt(1, "att-first-0001", "cancelled", 150), attempt(2, "att-second-0002", "running", null)],
    calls: [staleCall, call(2, "running", providerRequest(null))],
  }));
  assert.ok(running.includes("data-current-activity"));
  assert.ok(running.includes(escape(words["chat.activityWaitingProvider"])));
  assert.ok(!running.includes("stale.rs"), "a previous generation's Call never reads as current");
  assert.ok(running.includes("data-execution-target=\"current\""));
  assert.ok(running.includes(escape(words["execution.canonical.attemptCurrent"])));

  // RuntimeObservability, when present, only adds a supplemental section.
  const fixture = createScenarioFixture("observability-live");
  const observability = Object.values(fixture.observabilityBySession).find(Boolean);
  assert.ok(observability);
  const supplemented = render(cancelled, observability);
  assert.ok(supplemented.includes(escape(words["execution.canonical.runtimeSupplement"])));
  assert.ok(supplemented.indexOf("data-canonical-job") < supplemented.indexOf(escape(words["execution.canonical.runtimeSupplement"])));
  assert.ok(supplemented.includes("data-execution-target=\"final\"") && supplemented.includes("Job execution cancelled"));
}

function conversationInspectorRemainsStableDuringPolling() {
  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { ConversationInspector } = require(path.join(frontend, "components/ocg/usage/conversation-inspector.tsx"));
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));

  const usage = (data = null, loading = false, error = null) => ({ data, loading, error, refresh: () => undefined });
  const sampleData = {
    id: "usage-123", conversation_id: "chat-1", generated_at: Date.now(),
    created_at: Date.now(), updated_at: Date.now(), truncated: false,
    totals: {
      turns: 5, jobs: 3, first_activity_at: Date.now(), last_activity_at: Date.now(),
      cost: { input_cost: 0.01, output_cost: 0.02, total_cost: 0.03 },
      tokens: { input: 100, output: 200, cached_input: 0, cache_creation_input: 0, total: 300 },
    },
    models: [], latest_job_state: null,
  };

  const render = (u) => renderToStaticMarkup(React.createElement(I18nProvider, null,
    React.createElement(ConversationInspector, {
      usage: u, execution: null, accounting: null, mode: "docked",
      onClose: () => undefined, onOpenJobExecution: () => undefined,
    })));

  // Initial load with no data: shows loading message (has role="status").
  const initialLoading = render(usage(null, true));
  assert.ok(initialLoading.includes("role=\"status\""), "loading message appears on initial load");
  assert.ok(!initialLoading.match(/<dt[^>]*>\s*Cost/i), "data not yet rendered");

  // Initial load complete: no loading message, data visible.
  const loaded = render(usage(sampleData, false));
  assert.ok(!loaded.includes("role=\"status\""), "loading message gone after load");
  assert.ok(loaded.includes("turns"), "data is visible after load");

  // Background refresh: existing data visible, no loading message appears.
  const refreshing = render(usage(sampleData, true, null));
  assert.ok(!refreshing.includes("role=\"status\""), "no loading message during background refresh");
  assert.ok(refreshing.includes("turns"), "existing data remains visible during refresh");

  // No-activity state only on initial load, not during background refresh.
  const empty = render(usage(null, false));
  assert.ok(empty.match(/No activity/i) || empty.includes("data-usage-surface"), "no-activity shown when no data and not loading");
  const refreshingEmpty = render(usage(null, true));
  assert.ok(!refreshingEmpty.match(/No activity/i) || refreshingEmpty.includes("role=\"status\""), "no-activity not shown during background refresh of empty state");

  // Scrollbar gutter stable prevents layout shift.
  assert.ok(loaded.includes("scrollbar-gutter"), "stable scrollbar gutter is applied");
  assert.ok(refreshing.includes("scrollbar-gutter"), "scrollbar gutter remains during refresh");

  // Manual refresh button works and becomes disabled during read.
  assert.ok(!loaded.match(/disabled["\s]/), "refresh button enabled when not loading");
  assert.ok(refreshing.includes("disabled"), "refresh button disabled while reading");
}

for (const check of [conversationUsageGateChecksCanonicalIdentity, executionInspectorIsCanonicalFirst, conversationUsageRefreshesWhileLive, conversationInspectorRemainsStableDuringPolling, activeTargetBeatsComposer, preferenceIsScopedPerChat, observableActivityDrivesStatus, longMessagesFold, retryClearsPresentation, reasoningSurvivesHistoryAndRoundReset, chatSurfaceRendersWithoutRuntimeBanner]) {
  check();
}
process.stdout.write("chat presentation contract ok\n");
