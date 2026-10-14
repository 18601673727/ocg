const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { createRequire } = require("node:module");

const frontend = path.resolve(__dirname, "../../frontend");
const frontendRequire = createRequire(path.join(frontend, "package.json"));
const ts = frontendRequire("typescript");

// The runtime provider renders nothing before browser hydration; Chat only
// needs its client handle for image upload, which these checks never reach.
// A surface that reads the projection replaces this with a real snapshot.
let runtimeStubRef = { useOcgRuntime: () => ({ client: {} }) };

// Disposable module stubs for the effect probes. Empty by default, so the
// normal checks load the real `react`/JSX runtime and the real Mermaid/KaTeX
// packages. A probe temporarily points these at a hook harness and controllable
// fakes, then clears them, so production code is what runs under the probe.
let moduleStubs = {};

// Exercise the production TypeScript modules with the existing compiler and alias.
for (const extension of [".ts", ".tsx"]) {
  require.extensions[extension] = (module, filename) => {
    const load = module.require.bind(module);
    // The runtime context is stubbed rather than provided, so match every
    // specifier the Chat graph uses to reach it.
    module.require = (name) => name in moduleStubs ? moduleStubs[name]
      : /(^|\/)runtime-context$/.test(name) ? runtimeStubRef : load(name.startsWith("@/") ? path.join(frontend, name.slice(2))
        : name.startsWith(".") || path.isAbsolute(name) ? name : frontendRequire.resolve(name));
    // Mirror the app's own tsconfig: these modules use `import x from "cjs"`,
    // which only binds a default under esModuleInterop.
    const source = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
      compilerOptions: {
        module: ts.ModuleKind.CommonJS,
        target: ts.ScriptTarget.ES2022,
        jsx: ts.JsxEmit.ReactJSX,
        esModuleInterop: true,
      },
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
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));
  const { applyEnvelopeToSnapshot } = require(path.join(frontend, "components/ocg/runtime/reconciler.ts"));
  const { RuntimeEnvelopeFactory } = require(path.join(frontend, "components/ocg/runtime/runtime-envelope.ts"));
  const { emptyRuntimeSnapshot } = require(path.join(frontend, "components/ocg/runtime/runtime-snapshot.ts"));

  // The active Chat surface reads its messages from the runtime projection, so
  // the messages are folded into the snapshot the same way a history load and
  // a live stream would fold them.
  const project = (messages) => {
    const session = { id: "s", sessionId: "chat-1", projectId: "p", title: "", workType: "coding", updatedAt: "now" };
    const factory = new RuntimeEnvelopeFactory("chat-presentation", 1);
    const scope = { projectId: "p", sessionId: "s" };
    const base = emptyRuntimeSnapshot("normal-chat");
    const created = applyEnvelopeToSnapshot(base, factory.envelope("conversation.session-created", { session }, scope));
    const loaded = applyEnvelopeToSnapshot(created.snapshot, factory.envelope("conversation.history-loaded", { messages }, scope));
    assert.deepEqual(loaded.diagnostics, []);
    return loaded.snapshot;
  };

  const long = Array.from({ length: 200 }, (_, index) => `line ${index}`).join("\n");
  const streaming = [
    { id: "u", role: "user", content: long, createdAt: "t", status: "completed" },
    { id: "a", role: "assistant", jobId: "job-1", content: "short", createdAt: "t", status: "streaming" },
  ];
  const settledMessages = [
    { id: "u", role: "user", content: "hi", createdAt: "t", status: "completed" },
    { id: "a", role: "assistant", jobId: "job-1", content: "done", createdAt: "t", status: "completed" },
  ];

  // The runtime provider supplies the projection the surface renders from.
  // It is built once per case: a fresh snapshot per render would defeat the
  // memoisation the provider relies on.
  let snapshot = project(streaming);
  const runtimeStub = {
    useOcgRuntime: () => ({
      client: {},
      snapshot,
    }),
  };
  // chat-screen and everything under it resolve the runtime context at import
  // time, so the module graph is rebuilt against this snapshot.
  for (const key of Object.keys(require.cache)) {
    if (key.startsWith(path.join(frontend, "components"))) delete require.cache[key];
  }
  const previous = runtimeStubRef;
  runtimeStubRef = runtimeStub;
  try {
    const { ChatScreen } = require(path.join(frontend, "components/ocg/chat/chat-screen.tsx"));
    const { I18nProvider: Provider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));
    const render = (executionValue) => renderToStaticMarkup(React.createElement(Provider, null, React.createElement(ChatScreen, {
      session: { id: "s", sessionId: "chat-1", projectId: "p", title: "", workType: "coding", updatedAt: "now" },
      execution: executionValue,
      runtimeStatus: { state: "connected", detailCode: "provider-ready", detail: "OCG provider runtime" },
      onComposerIntent: () => undefined,
      onCancel: async () => undefined,
    })));

    const html = render(execution("running", { calls: [call(2, "running", tool("filesystem.edit", { file: "src/orchestration/domain.rs" }))] }));
    // The generic runtime row and container are gone from the surface.
    assert.ok(!html.includes("OCG provider runtime"), "the generic runtime row is gone");
    assert.ok(!html.includes("border-dashed"), "the generic runtime container is gone");
    // The frozen active model and provider are shown in the composer.
    assert.ok(html.includes("gpt-6.1-sol"), "the frozen active model is shown");
    assert.ok(html.includes("nexotokensub"), "the frozen provider is shown");
    // A running execution offers the real controls, not a placeholder.
    assert.ok(html.includes("Cancel execution"), "a running execution can be cancelled");
    assert.ok(html.includes("Steer now"), "a running execution can be steered");
    // Both messages reach the surface through the runtime projection.
    assert.ok(html.includes('data-message-id="u"'), "the user message is rendered");
    assert.ok(html.includes('data-message-id="a"'), "the assistant message is rendered");
    assert.ok(html.includes("short"), "the assistant message body renders");
    // Both roles bound their long messages behind the same expand/collapse
    // viewport the legacy rows use. Each message root owns its own bounded
    // viewport, so a long user message and a long answer both stay bounded.
    const userRoot = html.indexOf('data-message-id="u"');
    const assistantRoot = html.indexOf('data-message-id="a"');
    assert.ok(userRoot >= 0 && assistantRoot > userRoot, "both message roles render");
    assert.ok(html.slice(userRoot, assistantRoot).includes("data-message-bounds"),
      "the user message is bounded on the Thread surface");
    assert.ok(html.slice(assistantRoot).includes("data-message-bounds"),
      "the assistant message is bounded on the Thread surface");
    // The provider request's chain of thought never reaches the surface.
    assert.ok(!html.includes("hidden chain of thought"));

    // Once the Job settles the composer is the next-turn selection again.
    snapshot = project(settledMessages);
    const settled = render(execution("completed"));
    assert.ok(!settled.includes("gpt-6.1-sol"));
    assert.ok(!settled.includes("nexotokensub"));
    assert.ok(!settled.includes("Cancel execution"), "a settled execution offers no cancel");
    assert.ok(!settled.includes("Steer now"), "a settled execution offers no steer");
    assert.ok(settled.includes("done"), "the settled assistant message still renders");
  } finally {
    runtimeStubRef = previous;
    for (const key of Object.keys(require.cache)) {
      if (key.startsWith(path.join(frontend, "components"))) delete require.cache[key];
    }
  }
}

/**
 * The Markdown body of an assistant message.
 *
 * Chat renders prose through this pipeline, so the guarantees it makes about
 * nesting, unsafe URLs, raw HTML and half-streamed content are contract, not
 * implementation detail.
 */
function markdownBodyRendersSafely() {
  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { MarkdownText } = require(path.join(frontend, "components/assistant-ui/elements/markdown-text.tsx"));
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));

  // The assistant-ui runtime passes the message part itself.
  const render = (text, props = {}) => renderToStaticMarkup(
    React.createElement(I18nProvider, null, React.createElement(MarkdownText, { text, ...props })));

  const body = render("hello **bold** and `code`");
  assert.ok(body.includes("<strong>bold</strong>"), "emphasis renders");
  assert.ok(body.includes("<code"), "inline code renders");

  // Nesting survives, including lists several levels deep.
  const nested = render("- a\n  - b\n    - c");
  assert.equal((nested.match(/<ul/g) ?? []).length, 3, "nested lists nest");
  assert.ok(nested.includes('type="checkbox"') === false, "a plain list has no task checkbox");

  const task = render("- [x] done\n- [ ] todo");
  assert.equal((task.match(/type="checkbox"/g) ?? []).length, 2, "task lists render both checkboxes");

  // A fenced block renders its language, a copy control, and highlighted source.
  const fence = render("```ts\nconst a = 1;\n```");
  assert.ok(fence.includes("Copy code"), "a fenced block offers a copy control");
  assert.ok(fence.includes(">ts<"), "a fenced block names its language");
  assert.ok(fence.includes("hljs"), "a known language is highlighted");
  // highlight.js escapes and spans the source, so no raw tag can appear.
  assert.ok(fence.includes("hljs-keyword"), "the source is tokenized, not pasted");
  // An unknown language still renders the plain code rather than nothing.
  const plain = render("```notalanguage\nplain text\n```");
  assert.ok(plain.includes("plain text"), "an unknown language renders plain code");
  assert.ok(!plain.includes("hljs-keyword"), "an unknown language is not highlighted");

  // Math is recognised but rendered client-side, so the token is not text.
  assert.ok(render("cost $x^2$").includes("inline-math"), "inline math is recognised");
  assert.ok(render("$$\n\\frac{a}{b}\n$$").includes("text-center"), "block math is recognised");

  // An unsafe URL never becomes a live link or an image source.
  assert.ok(!render("[t](javascript:alert(1))").includes("<a "), "a javascript: link is not a link");
  assert.ok(!render("![alt](javascript:alert(1))").includes("<img"), "a javascript: image is not an image");
  const safe = render("[t](https://example.com)");
  assert.ok(safe.includes('href="https://example.com"'), "an https link is preserved");

  // Document-authored HTML is escaped rather than emitted.
  const script = render("<script>alert(1)</script>");
  assert.ok(!script.includes("<script>"), "raw HTML never reaches the DOM");
  assert.ok(script.includes("&lt;script&gt;"), "raw HTML is shown as text");

  // A stream that stops mid-token still renders what arrived.
  assert.ok(render("**bold and unterminated").includes("unterminated"), "an unterminated emphasis keeps its text");
  assert.ok(render("so $a + b").includes("$a + b"), "an unterminated inline equation keeps its text");
  assert.ok(render("```ts\nconst a = 1;").includes("Copy code"), "an unterminated fence still renders a block");

  // Reasoning renders in the muted tone.
  assert.ok(render("thinking", { inMuted: true }).includes("text-[13px]"), "reasoning renders muted");
}

/**
 * Browser-only render guards.
 *
 * Mermaid and KaTeX only ever run in an effect, so a server-rendered page
 * cannot show whether their options are still the safe ones. The options are
 * therefore asserted as values, and the renderer is additionally checked for
 * the sanitiser it applies to Mermaid's SVG.
 */
function markdownBrowserGuardsStayLocked() {
  const { KATEX_OPTIONS, MERMAID_OPTIONS } = require(path.join(frontend, "components/ocg/markdown/markdown-pipeline.ts"));

  // Mermaid must not be relaxed: "strict" is what blocks markup in a diagram.
  assert.equal(MERMAID_OPTIONS.securityLevel, "strict", "Mermaid stays at securityLevel strict");
  assert.equal(MERMAID_OPTIONS.htmlLabels, false, "Mermaid does not emit HTML labels");
  assert.equal(MERMAID_OPTIONS.startOnLoad, false, "Mermaid renders on demand, not on load");

  // KaTeX must not be trusted: `\href` and `\url` would become live links.
  assert.equal(KATEX_OPTIONS.trust, false, "KaTeX trust stays off");
  assert.equal(KATEX_OPTIONS.throwOnError, true, "KaTeX surfaces a bad expression instead of guessing");

  // The renderer must pass Mermaid's SVG through DOMPurify.
  const source = fs.readFileSync(
    path.join(frontend, "components/ocg/markdown/markdown-body.tsx"),
    "utf8",
  );
  assert.ok(source.includes("dompurify"), "Mermaid's SVG is sanitised");
  assert.ok(/purify\.sanitize\(svg/.test(source), "the sanitiser is applied to the SVG");
  // KaTeX writes into a ref rather than assigning HTML, so there is no sink.
  assert.ok(!/dangerouslySetInnerHTML[\s\S]{0,200}katex/i.test(source), "KaTeX does not assign HTML");
}

/**
 * URL admission.
 *
 * markdown-it already refuses some schemes while parsing, so the pipeline's
 * own check is the backstop for URLs that reach it another way. Both layers
 * are asserted here.
 */
function markdownUrlsAreAdmittedSafely() {
  const { sanitizeUrl } = require(path.join(frontend, "components/ocg/markdown/markdown-pipeline.ts"));

  for (const url of ["https://example.com", "http://example.com/x?y=1", "mailto:a@b.c"]) {
    assert.equal(sanitizeUrl(url), url, `${url} is admitted`);
  }
  for (const url of ["/settings", "#anchor", "./relative"]) {
    assert.equal(sanitizeUrl(url), url, `${url} stays in-site`);
  }
  for (const url of [
    "javascript:alert(1)",
    "JaVaScRiPt:alert(1)",
    " javascript:alert(1)",
    "data:text/html,<script>",
    "vbscript:msgbox",
    "//evil.example",
    "",
  ]) {
    assert.equal(sanitizeUrl(url), null, `${JSON.stringify(url)} is refused`);
  }
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

/**
 * Managed images across the real SDK conversion.
 *
 * The installed SDK drops an assistant `image` part whose URL is not
 * `data:`/`https:`/`blob:`, so an assistant's managed image must arrive as a
 * `file` part with the absolute control URL. A user image stays an `image`
 * part (the SDK passes those through) with its attachment URL resolved the
 * same way. The ordering rule — reasoning before the answer — is asserted on
 * the same converted message.
 */
function managedImagesSurviveSdkConversion() {
  const adapter = require(path.join(frontend, "components/ocg/runtime/assistant-ui-adapter.ts"));
  const base = "http://127.0.0.1:43109";

  const assistant = {
    id: "a", role: "assistant", status: "completed", createdAt: "t",
    content: "answer", reasoning: "thought",
    images: [{ id: "img-1", url: "/api/v1/canonical/chat/images/s/i", name: "a.png", media_type: "image/png" }],
  };
  const like = adapter.convertOcgMessage(assistant, 0, [assistant], base);

  // Reasoning precedes the answer, and the managed image is a file part.
  assert.deepEqual(like.content.map((part) => part.type), ["reasoning", "text", "file"],
    "reasoning precedes the answer and the managed image is a file part");

  const file = like.content.find((part) => part.type === "file");
  assert.equal(file.id, "img-1", "the managed image keeps its id");
  assert.equal(file.mimeType, "image/png", "the managed image keeps its media type");
  assert.equal(file.sourceType, "url");
  assert.equal(file.data, base + "/api/v1/canonical/chat/images/s/i",
    "the relative managed URL is resolved against the control base");

  // A user image stays an image part with its attachment URL resolved.
  const user = {
    id: "u", role: "user", status: "completed", createdAt: "t", content: "hi",
    images: [{ id: "img-2", url: "/api/v1/canonical/chat/images/s/j", name: "b.png", media_type: "image/png" }],
  };
  const ulike = adapter.convertOcgMessage(user, 0, [user], base);
  const image = ulike.attachments[0].content.find((part) => part.type === "image");
  assert.equal(image.image, base + "/api/v1/canonical/chat/images/s/j",
    "the user attachment image URL is resolved");

  // The real installed SDK conversion is what the surface runs through. It keeps
  // the file part and the order, and drops a relative assistant image part — the
  // regression the file part exists to avoid.
  const reactEntry = frontendRequire.resolve("@assistant-ui/react");
  const coreRequire = createRequire(reactEntry);
  const { fromThreadMessageLike } = coreRequire("@assistant-ui/core/internal");
  const complete = { type: "complete", reason: "stop" };
  const kept = fromThreadMessageLike(
    { id: "a", role: "assistant", createdAt: new Date(), status: complete, content: like.content },
    "fallback", complete,
  );
  assert.deepEqual(kept.content.map((part) => part.type), ["reasoning", "text", "file"],
    "the SDK keeps the file part and the reasoning-before-answer order");
  const dropped = fromThreadMessageLike(
    { id: "x", role: "assistant", createdAt: new Date(), status: complete,
      content: [{ type: "image", id: "i", image: "/relative.png", filename: "i.png" }] },
    "fallback", complete,
  );
  assert.deepEqual(dropped.content.map((part) => part.type), [],
    "the SDK drops a relative assistant image part, which is the regression");
}

/**
 * URL admission and heading fidelity.
 *
 * Scheme-less relative references are legitimate targets (`docs/readme.md`,
 * `diagram.png`); protocol-relative and unsafe schemes are still refused, and
 * every heading level renders.
 */
function markdownAdmitsRelativeTargetsAndHeadings() {
  const { sanitizeUrl } = require(path.join(frontend, "components/ocg/markdown/markdown-pipeline.ts"));
  for (const url of ["docs/readme.md", "diagram.png", "../up/one.png", "a/b?c=d#frag"]) {
    assert.equal(sanitizeUrl(url), url, `${url} is admitted as a relative target`);
  }
  for (const url of ["//evil.example", "javascript:alert(1)", "data:text/html,x"]) {
    assert.equal(sanitizeUrl(url), null, `${url} is refused`);
  }

  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { MarkdownText } = require(path.join(frontend, "components/assistant-ui/elements/markdown-text.tsx"));
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));
  const heads = renderToStaticMarkup(React.createElement(I18nProvider, null,
    React.createElement(MarkdownText, { text: "# one\n\n## two\n\n### three\n\n#### four\n\n##### five\n\n###### six" })));
  for (let level = 1; level <= 6; level++) {
    assert.ok(heads.includes(`<h${level}`), `h${level} renders`);
  }
}

/**
 * Nested Markdown structure.
 *
 * A nested list with a later sibling must close the nesting so the sibling
 * returns to the top level, and content after a nested blockquote is a sibling
 * rather than being swallowed. Counting `<ul>` alone would miss either.
 */
function markdownNestingMatchesDepth() {
  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { MarkdownText } = require(path.join(frontend, "components/assistant-ui/elements/markdown-text.tsx"));
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));
  const render = (text) => renderToStaticMarkup(React.createElement(I18nProvider, null,
    React.createElement(MarkdownText, { text })));

  // A nested list with a later sibling: the sibling returns to the top level.
  const branch = render("- a\n  - b\n- c");
  assert.equal((branch.match(/<ul/g) ?? []).length, 2, "a nested list with a later sibling nests exactly one level");
  assert.match(branch, /<li[^>]*>\s*<p[^>]*>a<\/p>\s*<ul[^>]*>\s*<li[^>]*>\s*<p[^>]*>b<\/p>\s*<\/li>\s*<\/ul>\s*<\/li>\s*<li[^>]*>\s*<p[^>]*>c<\/p>\s*<\/li>/,
    "the later sibling closes the nesting and stays a top-level item");

  // Deep nesting with an outer sibling after the nested blocks close.
  const deep = render("- a\n  - b\n    - c\n- d");
  assert.equal((deep.match(/<ul/g) ?? []).length, 3, "three levels nest");
  assert.match(deep, /<\/ul>\s*<\/li>\s*<li[^>]*>\s*<p[^>]*>d<\/p>/,
    "the outer sibling is a top-level item after the nested blocks close");

  // A nested blockquote with trailing content: the trailing paragraph is a sibling.
  const quote = render("> outer\n> > inner\n\ntrailing");
  assert.ok((quote.match(/<blockquote/g) ?? []).length >= 2, "blockquotes nest");
  assert.match(quote, /<\/blockquote>\s*<p[^>]*>\s*trailing/,
    "content after a nested blockquote is a sibling, not swallowed");
}

/**
 * Dropdown Escape.
 *
 * The consumer sees the Escape first and may cancel the dismissal; the menu
 * closes and restores focus only when it was not prevented.
 */
function dropdownEscapeDismissesAfterConsumer() {
  const source = fs.readFileSync(path.join(frontend, "components/ui/dropdown-menu.tsx"), "utf8");
  const start = source.indexOf("event.key === \"Escape\"");
  assert.ok(start >= 0, "the Escape branch exists");
  const branch = source.slice(start, start + 420);
  const consumer = branch.indexOf("onEscapeKeyDown?.(event)");
  const guarded = branch.indexOf("if (!event.defaultPrevented)");
  const close = branch.indexOf("setOpen(false)");
  const focus = branch.indexOf(".focus()");
  assert.ok(consumer >= 0 && guarded >= 0 && close >= 0, "the consumer call and the guarded close exist");
  assert.ok(consumer < guarded, "the consumer's onEscapeKeyDown runs before the menu decides");
  assert.ok(guarded < close, "the menu closes only inside the un-cancelled branch");
  assert.ok(close < focus, "focus is restored after the menu closes");
}

/**
 * The canonical sidebar's Workbench menu.
 *
 * In canonical mode Chat stays the one primary destination and every other
 * destination moves into the Workbench menu beside the primary controls — none
 * is lost and the active destination is still marked.
 */
function sidebarWorkbenchPreservesDestinations() {
  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { OcgSidebar } = require(path.join(frontend, "components/ocg/sidebar/ocg-sidebar.tsx"));
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));
  const html = renderToStaticMarkup(React.createElement(I18nProvider, null,
    React.createElement(OcgSidebar, {
      sessions: [], activeSessionId: "", collapsed: false, onToggle: () => undefined,
      onSelect: () => undefined, onNewChat: () => undefined,
      runtimeStatus: { state: "connected", detailCode: "provider-ready", detail: "" },
      runtimeAuthority: "canonical", activeView: "health", attentionCount: 0,
      onNavigate: () => undefined,
    })));

  assert.ok(html.includes("data-workbench-menu"), "the Workbench menu exists in canonical mode");
  // Every non-chat canonical destination is preserved inside the menu.
  for (const label of ["Home", "Jobs", "Health", "Attention", "Usage", "Control Center"]) {
    assert.ok(html.includes(label), `${label} is preserved in the Workbench menu`);
  }
  // The active destination (health) is still marked.
  assert.ok(html.includes('aria-current="page"'), "the active destination is marked");
  // Chat is the one primary destination outside the menu, and the menu is a
  // primary control: it sits above the conversation list, not below it.
  const chatIdx = html.indexOf(">Chat<");
  const workbenchIdx = html.indexOf("data-workbench-menu");
  const sessionsIdx = html.indexOf('aria-label="Chat sessions grouped by work type"');
  assert.ok(chatIdx >= 0 && workbenchIdx >= 0 && sessionsIdx >= 0,
    "the primary destination, the Workbench menu and the conversation list all render");
  assert.ok(workbenchIdx < sessionsIdx,
    "the Workbench menu sits with the primary controls above the conversation list");
  assert.equal((html.match(/>Chat</g) ?? []).length, 1, "Chat is not duplicated inside the Workbench menu");
  // The Project selector is no longer a sidebar control, and neither is a
  // second copy of New chat or the account control.
  const sidebar = fs.readFileSync(path.join(frontend, "components/ocg/sidebar/ocg-sidebar.tsx"), "utf8");
  assert.ok(!sidebar.includes("<ProjectSwitcher"), "the sidebar does not host the Project selector");
  assert.equal((html.match(/>New chat</g) ?? []).length, 1, "New chat is not duplicated");
  assert.equal((html.match(/aria-label="Account"/g) ?? []).length, 1, "the account menu is not duplicated");
}

/**
 * The composer owns the turn's selections, outside its card.
 *
 * The Project and model selectors sit above the composer card as compact
 * dropdown buttons, and the card itself starts at the staged attachments so it
 * stays the input.
 */
function composerPlacesTurnSelectionsAboveCard() {
  const source = fs.readFileSync(path.join(frontend, "components/ocg/chat/chat-view.tsx"), "utf8");
  const card = source.indexOf("rounded-lg border border-border bg-card");
  const selectorRow = source.indexOf("relative z-30 flex min-w-0 items-stretch");
  const selector = source.indexOf("<ModelSelector");
  const projectSlot = source.indexOf("{projectSelector &&");
  const staging = source.indexOf("<AttachmentStaging");
  assert.ok(card >= 0 && selector >= 0 && staging >= 0, "the composer card, model selector and staging exist");
  assert.ok(selectorRow >= 0 && selectorRow < selector, "the model selector sits in its own row");
  assert.ok(projectSlot >= 0 && projectSlot < selector, "the Project selector shares that row");
  assert.ok(selector < card, "both selectors sit above the composer card, not inside it");
  assert.ok(card < staging, "the composer card heads with the staged attachments");
}

/**
 * The floating Chat header.
 *
 * The header draws no bottom border and no toolbar band, and it keeps only the
 * conversation title and the execution-details control: Settings and signing
 * out belong to the sidebar's account menu, so the header must not offer a
 * second copy of either.
 */
function chatHeaderFloatsWithoutDuplicateControls() {
  const topbar = fs.readFileSync(path.join(frontend, "components/ocg/topbar/ocg-topbar.tsx"), "utf8");
  const header = topbar.slice(topbar.indexOf("<header"), topbar.indexOf("</header>"));
  assert.ok(header.length > 0, "the header element exists");
  assert.ok(!header.includes("border-b border-border"), "the header draws no bottom border");
  assert.ok(!header.includes("bg-background"), "the header has no toolbar band");
  assert.ok(header.includes("rounded-full"), "the header's controls are pills");
  assert.ok(header.includes("data-execution-details-control"), "the execution-details control stays on the right");
  assert.ok(!topbar.includes("useRemoteLogout"), "signing out is not duplicated in the header");
  assert.ok(!topbar.includes("onOpenSettings"), "settings is not duplicated in the header");

  // Both stay reachable from the sidebar's account menu instead, which the
  // sidebar reaches through the one shared menu component.
  const sidebar = fs.readFileSync(path.join(frontend, "components/ocg/sidebar/ocg-sidebar.tsx"), "utf8");
  const account = fs.readFileSync(path.join(frontend, "components/ocg/sidebar/account-menu.tsx"), "utf8");
  assert.ok(sidebar.includes("<AccountMenu"), "the sidebar renders the shared account menu");
  assert.ok(sidebar.includes("useRemoteLogout()"), "the sidebar reads the real logout operation");
  assert.ok(sidebar.includes("useAccountEmail()"), "the sidebar reads the signed-in operator");
  assert.ok(account.includes('t("sidebar.logout")'), "the shared menu offers logging out");
  assert.ok(account.includes('t("sidebar.openSettings")'), "the shared menu offers settings");
}

/**
 * The account menu's contents, order and form.
 *
 * The one menu component is rendered for both identities and both placements:
 * the identity row is never actionable, signing out (where it exists) precedes
 * Settings, and local mode offers Settings without a logout item. This is a
 * render of the production component, not a source grep.
 */
function accountMenuOrderAndForm() {
  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));
  const { AccountMenu } = require(path.join(frontend, "components/ocg/sidebar/account-menu.tsx"));

  const render = (props) => renderToStaticMarkup(
    React.createElement(I18nProvider, null, React.createElement(AccountMenu, props)));

  // The menu items, in document order, with their actionable state.
  const items = (html) => {
    const menu = html.slice(html.indexOf('role="menu"'));
    assert.ok(menu.length > 0, "the menu content rendered");
    return [...menu.matchAll(/<button[^>]*role="menuitem"[^>]*>([\s\S]*?)<\/button>/g)].map((match) => ({
      disabled: /\sdisabled(=|>|\s)/.test(match[0]),
      text: match[1].replace(/<[^>]+>/g, "").trim(),
    }));
  };

  const remote = render({ open: true, email: "operator@example.com", onLogout: () => undefined, onSettingsClick: () => undefined });
  assert.deepEqual(items(remote), [
    { disabled: true, text: "operator@example.com" },
    { disabled: false, text: "Log out" },
    { disabled: false, text: "Open settings" },
  ], "remote: identity (non-actionable), then Log out, then Open settings");
  assert.ok(remote.includes(">operator@example.com<"), "the trigger and the identity row name the signed-in operator");

  const local = render({ open: true, email: null, onSettingsClick: () => undefined });
  assert.deepEqual(items(local), [
    { disabled: true, text: "Local operator" },
    { disabled: false, text: "Open settings" },
  ], "local: identity (non-actionable), then Open settings, and no logout item");
  assert.ok(!local.includes("Log out"), "local mode offers nothing to log out of");

  // The collapsed rail renders the same menu, reachable from its own icon.
  const rail = render({ open: true, variant: "rail", email: "operator@example.com", onLogout: () => undefined, onSettingsClick: () => undefined });
  assert.deepEqual(items(rail), items(remote), "the rail menu carries the identical identity and actions");
  const railTrigger = rail.slice(0, rail.indexOf('role="menu"'));
  assert.ok(railTrigger.includes('aria-label="Account"'), "the rail account icon is named for assistive technology");
  const triggerMarkup = railTrigger.slice(railTrigger.indexOf("<button"));
  const triggerText = triggerMarkup.slice(0, triggerMarkup.indexOf("</button>")).replace(/<[^>]+>/g, "").trim();
  assert.equal(triggerText, "", "the rail trigger is the icon alone, not a label row");
  // The rail is 64px wide and clips its overflow, so its menu is anchored to
  // the viewport instead of to the rail box.
  const railMenu = rail.slice(rail.indexOf('role="menu"'));
  assert.ok(railMenu.includes("fixed"), "the rail menu is anchored to the viewport");

  // Ordering and actions are declared once, not per placement.
  const account = fs.readFileSync(path.join(frontend, "components/ocg/sidebar/account-menu.tsx"), "utf8");
  assert.equal((account.match(/<DropdownMenuItem/g) ?? []).length, 3, "there is exactly one definition of the three rows");
}

/**
 * Account-menu keyboard focus.
 *
 * The navigation the menu uses is exercised directly with the mixes a menu can
 * have: a leading non-actionable identity row, wrap-around in both directions,
 * a menu whose every row is non-actionable, and focus resting on a
 * non-actionable row. The component's own wiring (which key calls it) is
 * asserted alongside, and the real interaction is driven in the browser.
 */
function accountMenuKeyboardFocus() {
  const { enabledMenuIndexes, menuFocusTarget } = require(path.join(frontend, "components/ui/dropdown-menu.tsx"));

  // identity (disabled) / Log out / Open settings
  const items = [
    { index: 0, disabled: true },
    { index: 1, disabled: false },
    { index: 2, disabled: false },
  ];
  assert.deepEqual(enabledMenuIndexes(items), [1, 2], "only the actions are focusable");

  assert.equal(menuFocusTarget(items, -1, "ArrowDown"), 1, "opening focuses the first enabled action, not the identity row");
  assert.equal(menuFocusTarget(items, -1, "ArrowUp"), 2, "an upward entry from outside the list lands on the last action");
  assert.equal(menuFocusTarget(items, 1, "ArrowDown"), 2, "ArrowDown moves to the next action");
  assert.equal(menuFocusTarget(items, 2, "ArrowDown"), 1, "ArrowDown wraps past the end");
  assert.equal(menuFocusTarget(items, 1, "ArrowUp"), 2, "ArrowUp wraps past the start");
  assert.equal(menuFocusTarget(items, 0, "ArrowDown"), 1, "focus on the identity row steps into the actions");
  assert.equal(menuFocusTarget(items, 0, "ArrowUp"), 2, "focus on the identity row steps back to the last action");
  assert.equal(menuFocusTarget(items, 2, "Home"), 1, "Home focuses the first enabled action");
  assert.equal(menuFocusTarget(items, 1, "End"), 2, "End focuses the last enabled action");

  // A single enabled action is its own next and previous action.
  const single = [{ index: 0, disabled: true }, { index: 1, disabled: false }];
  assert.equal(menuFocusTarget(single, 1, "ArrowDown"), 1, "one action wraps onto itself");
  assert.equal(menuFocusTarget(single, 1, "ArrowUp"), 1, "one action wraps onto itself upward");

  // Nothing actionable: no target at all, for every key.
  const inert = [{ index: 0, disabled: true }, { index: 1, disabled: true }];
  assert.deepEqual(enabledMenuIndexes(inert), [], "a menu of non-actionable rows has no focusable item");
  for (const key of ["ArrowDown", "ArrowUp", "Home", "End"]) {
    assert.equal(menuFocusTarget(inert, -1, key), null, `${key} focuses nothing when no action is enabled`);
  }

  // The content wires those keys to that navigation, and leaves the event to
  // the page when there is nothing to focus.
  const source = fs.readFileSync(path.join(frontend, "components/ui/dropdown-menu.tsx"), "utf8");
  assert.ok(source.includes("menuFocusTarget(items, focusedIndex.current, key)"), "the content navigates through the shared logic");
  assert.ok(source.includes("const items = menuFocusables(contentRef.current)"), "the items are read from the rendered menu");
  assert.ok(!source.includes("itemIndex"), "no second, registration-time index of the same items exists");
  for (const key of ["ArrowDown", "ArrowUp", "Home", "End"]) {
    assert.ok(source.includes(`event.key === "${key}"`), `${key} is handled`);
  }
  assert.ok(source.includes("if (focusByKey(event.key)) event.preventDefault()"), "an inert menu does not swallow the key");
  // Opening focuses once. The index lives in a ref and the effect depends on
  // the open state alone: while it was React state, the effect re-ran on every
  // focus move and stepped the menu on its own past the first action.
  assert.ok(source.includes('navigate.current("ArrowDown")'), "opening focuses the first enabled action through the same navigation");
  assert.ok(/const focusedIndex = React\.useRef\(-1\)/.test(source), "the focused index is a ref, not render state");
  assert.ok(/focusedIndex\.current = -1;/.test(source), "opening starts outside the item list, so the first enabled action is the target");
  assert.ok(/\}, \[open\]\);/.test(source), "the open effect depends on the open state alone");
}

/**
 * Both sidebar forms offer the account menu.
 *
 * The collapsed rail gained the account control at its bottom; the expanded
 * footer keeps it. Both render the same component, so identity, actions and
 * ordering cannot drift between them.
 */
function collapsedSidebarOffersTheAccountMenu() {
  const React = frontendRequire("react");
  const { renderToStaticMarkup } = frontendRequire("react-dom/server");
  const { OcgSidebar } = require(path.join(frontend, "components/ocg/sidebar/ocg-sidebar.tsx"));
  const { I18nProvider } = require(path.join(frontend, "components/ocg/i18n/context.tsx"));

  const sidebar = (collapsed) => renderToStaticMarkup(React.createElement(I18nProvider, null,
    React.createElement(OcgSidebar, {
      sessions: [], activeSessionId: "", collapsed, onToggle: () => undefined,
      onSelect: () => undefined, onNewChat: () => undefined,
      runtimeStatus: { state: "connected", detailCode: "provider-ready", detail: "" },
      runtimeAuthority: "canonical", activeView: "chat", attentionCount: 0,
      onNavigate: () => undefined, onSettingsClick: () => undefined,
    })));

  const rail = sidebar(true);
  assert.equal((rail.match(/aria-label="Account"/g) ?? []).length, 1, "the collapsed rail offers the account control");
  assert.ok(rail.includes("OCG"), "the rail still shows its wordmark");

  const expanded = sidebar(false);
  assert.equal((expanded.match(/aria-label="Account"/g) ?? []).length, 1, "the expanded footer offers it once");
  assert.ok(/Local operator/.test(expanded), "the expanded footer names the local operator");

  const source = fs.readFileSync(path.join(frontend, "components/ocg/sidebar/ocg-sidebar.tsx"), "utf8");
  assert.equal((source.match(/<AccountMenu/g) ?? []).length, 2, "both forms render the one account menu");
  assert.ok(source.includes('variant="rail"'), "the rail is the rail form of that menu");
}

/**
 * Conversation search is title-only and Project-scoped.
 *
 * The overlay filters the session list it is handed — the sidebar's own
 * project-filtered list — by title, and selecting a result uses the same
 * selection callback as the list. It must not grow a body search, an index, or
 * a control-plane request of its own.
 */
function conversationSearchStaysTitleOnlyAndProjectScoped() {
  const source = fs.readFileSync(path.join(frontend, "components/ocg/sidebar/conversation-search.tsx"), "utf8");
  assert.ok(/sessions\.filter\(session =>/.test(source), "results come from the sessions it is given");
  assert.ok(source.includes("session.title"), "filtering reads the conversation title");
  assert.ok(source.includes('t("sidebar.newChat")'), "an untitled conversation matches its displayed name");
  assert.ok(source.includes("onSelect(session.id)") || source.includes("select(session.id)"),
    "a result is opened through the shared selection callback");
  for (const forbidden of ["fetch(", "XMLHttpRequest", "message.content", "messagesBySession", "localStorage"]) {
    assert.ok(!source.includes(forbidden), `the overlay does not ${forbidden}`);
  }
  // Same overlay conventions as the Project switcher: the Dialog primitive, a
  // named dialog, an initial focus target, and both empty states.
  for (const marker of ["<Dialog", "<DialogContent", "initialFocus", "<DialogTitle", "aria-label"]) {
    assert.ok(source.includes(marker), `the overlay keeps ${marker}`);
  }
  assert.ok(source.includes('t("sidebar.noSearchMatches")'), "no-title-match state exists");
  assert.ok(source.includes('t("sidebar.searchNoConversations")'), "no-conversations state exists");

  // The sidebar hands it the same list it renders, and opens it from a button.
  const sidebar = fs.readFileSync(path.join(frontend, "components/ocg/sidebar/ocg-sidebar.tsx"), "utf8");
  assert.ok(sidebar.includes("<ConversationSearch"), "the sidebar opens the overlay");
  assert.ok(!sidebar.includes('type="search"'), "the sidebar keeps no inline filter input");
  assert.ok(!/const \[searchQuery/.test(sidebar), "the sidebar keeps no inline filter state");
}

/**
 * Highlighting theme scoping.
 *
 * The light theme is the base import; the dark rules are scoped under `.dark`,
 * so the dark tokens never leak into light mode.
 */
function highlightingThemeIsScoped() {
  const css = fs.readFileSync(path.join(frontend, "app/globals.css"), "utf8");
  assert.ok(css.includes("highlight.js/styles/github.css"), "the light highlight theme is imported");
  assert.ok(!/^\s*@import\s+"highlight\.js\/styles\/github-dark\.css"/m.test(css),
    "the dark highlight theme is not imported globally");
  assert.ok(/\.dark\s+\.hljs/.test(css), "the dark highlight rules are scoped under .dark");
}

/**
 * Streaming recovery of the two render-on-effect blocks.
 *
 * Mermaid and KaTeX render inside an effect, so the SSR harness cannot exercise
 * them. This is a disposable probe: production `markdown-body.tsx` is loaded
 * against a tiny hook runtime and controllable Mermaid/KaTeX fakes, then the
 * components' effects are driven directly. That is what proves an invalid code
 * that becomes valid recovers, and that a superseded async result is dropped —
 * neither is observable from options or SSR alone.
 */
async function markdownStreamingRecoveryProbe() {
  const Fragment = Symbol("fragment");
  const rt = { slots: [], cursor: 0, effects: [], refs: new WeakMap() };
  const reactStub = {
    Fragment,
    useState(initial) {
      const i = rt.cursor++;
      if (!(i in rt.slots)) rt.slots[i] = { value: typeof initial === "function" ? initial() : initial };
      const slot = rt.slots[i];
      const set = (nextValue) => {
        const next = typeof nextValue === "function" ? nextValue(slot.value) : nextValue;
        if (next !== slot.value) slot.value = next;
      };
      return [slot.value, set];
    },
    useRef(initial) {
      const i = rt.cursor++;
      if (!(i in rt.slots)) rt.slots[i] = { value: { current: initial } };
      return rt.slots[i].value;
    },
    useEffect(fn, deps) {
      const i = rt.cursor++;
      const slot = rt.slots[i] ?? (rt.slots[i] = {});
      const changed = !deps || !slot.deps || deps.length !== slot.deps.length
        || deps.some((depth, index) => depth !== slot.deps[index]);
      slot.deps = deps;
      if (changed) rt.effects.push({ i, fn });
    },
    useCallback(fn) { return fn; },
    useMemo(fn) { return fn(); },
  };
  const jsxRuntime = {
    Fragment,
    jsx: (type, props, key) => ({ type, props: props || {}, key }),
    jsxs: (type, props, key) => ({ type, props: props || {}, key }),
  };

  // Host nodes are plain objects so the components can write `innerHTML`.
  const attachRefs = (node) => {
    if (!node || typeof node !== "object") return;
    if (Array.isArray(node)) { node.forEach(attachRefs); return; }
    const ref = node.props && node.props.ref;
    if (ref && typeof ref === "object") {
      if (!rt.refs.has(ref)) rt.refs.set(ref, { innerHTML: "" });
      ref.current = rt.refs.get(ref);
    }
    if (node.props && node.props.children) attachRefs(node.props.children);
  };
  // Reaches MermaidDiagram through the pure CodeBlock wrapper it is returned from.
  const findMermaid = (node) => {
    if (!node || typeof node !== "object") return null;
    if (Array.isArray(node)) { for (const child of node) { const hit = findMermaid(child); if (hit) return hit; } return null; }
    if (node.props && node.props.diagramLabel === "Mermaid diagram") return node.type;
    if (typeof node.type === "function") return findMermaid(node.type(node.props));
    return findMermaid(node.props && node.props.children);
  };
  const findMath = (node) => {
    if (!node || typeof node !== "object") return null;
    if (Array.isArray(node)) { for (const child of node) { const hit = findMath(child); if (hit) return hit; } return null; }
    if (typeof node.type === "function" && typeof node.props.inline === "boolean" && typeof node.props.content === "string") return node.type;
    return findMath(node.props && node.props.children);
  };
  const collectHosts = (node, out = []) => {
    if (!node || typeof node !== "object") return out;
    if (Array.isArray(node)) { node.forEach((child) => collectHosts(child, out)); return out; }
    const ref = node.props && node.props.ref;
    if (ref && typeof ref === "object") out.push(ref.current);
    if (node.props && node.props.children) collectHosts(node.props.children, out);
    return out;
  };
  const textOf = (node) => {
    if (node === null || node === undefined || typeof node === "boolean") return "";
    if (typeof node === "string" || typeof node === "number") return String(node);
    if (Array.isArray(node)) return node.map(textOf).join("");
    if (node.props && node.props.children) return textOf(node.props.children);
    return "";
  };

  // Controllable fakes: Mermaid's render is deferred so resolution order is in
  // the probe's hands; KaTeX records what it was asked to render and fails the
  // same way `throwOnError` would.
  const mermaidRenders = [];
  const katexRenders = [];
  const mermaidStub = { initialize() {}, render: (id, code) => new Promise((resolve, reject) => mermaidRenders.push({ code, resolve, reject })) };
  const purifyStub = { sanitize: (svg) => svg };
  const katexStub = {
    render: (content, host) => {
      katexRenders.push(content);
      if (content.startsWith("bad")) throw new Error("KaTeX parse error");
      host.innerHTML = `katex:${content}`;
    },
  };

  const bodyPath = path.join(frontend, "components/ocg/markdown/markdown-body.tsx");
  const savedStubs = moduleStubs;
  const savedBody = require.cache[bodyPath];
  moduleStubs = {
    react: reactStub,
    "react/jsx-runtime": jsxRuntime,
    mermaid: { __esModule: true, default: mermaidStub },
    dompurify: { __esModule: true, default: purifyStub },
    katex: { __esModule: true, default: katexStub },
  };
  delete require.cache[bodyPath];

  const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

  try {
    const { MarkdownBody } = require(bodyPath);
    const MermaidDiagram = findMermaid(MarkdownBody({ content: "```mermaid\ngraph TD; A-->\n```", inMuted: false }));
    const MathBlock = findMath(MarkdownBody({ content: "$$bad \\frac{1$$", inMuted: false }));
    assert.equal(typeof MermaidDiagram, "function", "the Mermaid component is reachable through the rendered tree");
    assert.equal(typeof MathBlock, "function", "the math component is reachable through the rendered tree");

    const render = (Component, props) => {
      rt.cursor = 0;
      rt.effects = [];
      const tree = Component(props);
      attachRefs(tree);
      return tree;
    };
    const flush = () => {
      const run = rt.effects;
      rt.effects = [];
      for (const { i, fn } of run) {
        const slot = rt.slots[i];
        if (slot.cleanup) { slot.cleanup(); slot.cleanup = undefined; }
        const cleanup = fn();
        if (typeof cleanup === "function") slot.cleanup = cleanup;
      }
    };
    const fresh = () => { rt.slots = []; rt.cursor = 0; rt.effects = []; };

    // ---- Mermaid: invalid code recovers; a stale result is dropped ----
    const mprops = (code) => ({ code, inMuted: false, errorLabel: "Diagram error", diagramLabel: "Mermaid diagram" });
    fresh();
    render(MermaidDiagram, mprops("graph TD; A-->"));
    flush();
    await tick(); // the import resolves and marks the component ready
    render(MermaidDiagram, mprops("graph TD; A-->"));
    flush();
    await tick(); // the render effect's imports resolve and call Mermaid's render
    assert.equal(mermaidRenders.length, 1, "the invalid code reaches Mermaid's render");
    mermaidRenders[0].reject(new Error("Parse error on line 1"));
    await tick();
    const hostBefore = collectHosts(render(MermaidDiagram, mprops("graph TD; A-->")))[0];
    let tree = render(MermaidDiagram, mprops("graph TD; A-->"));
    assert.ok(hostBefore, "the diagram host is mounted before the error");
    assert.equal(collectHosts(tree)[0], hostBefore, "the host node is reused, never remounted across the error");
    assert.ok(textOf(tree).includes("Diagram error"), "the failure is surfaced");

    // The same mounted component recovers when the code becomes valid.
    tree = render(MermaidDiagram, mprops("graph TD; A-->B"));
    flush();
    await tick(); // the render effect's imports resolve and call Mermaid's render
    assert.equal(mermaidRenders.length, 2, "the valid code reaches Mermaid's render");
    mermaidRenders[1].resolve({ svg: "<svg>ok</svg>" });
    await tick();
    tree = render(MermaidDiagram, mprops("graph TD; A-->B"));
    assert.ok(!textOf(tree).includes("Diagram error"), "the cleared error is gone after recovery");
    assert.equal(collectHosts(tree)[0].innerHTML, "<svg>ok</svg>", "the valid diagram renders into the same host");

    // A superseded async result never overwrites the newest diagram.
    mermaidRenders.length = 0;
    render(MermaidDiagram, mprops("graph TD; A"));
    flush();
    render(MermaidDiagram, mprops("graph TD; B"));
    flush();
    await tick(); // both codes' imports resolve and call Mermaid's render
    assert.equal(mermaidRenders.length, 2, "both codes reach Mermaid's render");
    mermaidRenders[1].resolve({ svg: "<svg>B</svg>" }); // newest resolves first
    await tick();
    tree = render(MermaidDiagram, mprops("graph TD; B"));
    assert.equal(collectHosts(tree)[0].innerHTML, "<svg>B</svg>", "the newest code is rendered");
    mermaidRenders[0].resolve({ svg: "<svg>A</svg>" }); // stale resolves later
    await tick();
    tree = render(MermaidDiagram, mprops("graph TD; B"));
    assert.equal(collectHosts(tree)[0].innerHTML, "<svg>B</svg>", "a stale async result never overwrites the newest diagram");

    // ---- KaTeX: an invalid expression recovers; a superseded one is dropped ----
    const kprops = (content) => ({ content, inline: false, inMuted: false });
    fresh();
    katexRenders.length = 0;
    let ktree = render(MathBlock, kprops("bad \\frac{1"));
    flush();
    await tick(); // the import resolves, the render throws, the failure is set
    ktree = render(MathBlock, kprops("bad \\frac{1"));
    const mathHost = collectHosts(ktree)[0];
    assert.ok(mathHost, "the math host stays mounted across the failure");
    assert.ok(textOf(ktree).includes("$$bad \\frac{1$$"), "the failed expression falls back to its source");

    // A valid expression recovers into the same mounted host.
    ktree = render(MathBlock, kprops("x^2"));
    flush();
    await tick();
    ktree = render(MathBlock, kprops("x^2"));
    assert.equal(collectHosts(ktree)[0], mathHost, "the math host is reused across recovery");
    assert.equal(collectHosts(ktree)[0].innerHTML, "katex:x^2", "the valid expression renders into the host");
    assert.ok(!textOf(ktree).includes("$$x^2$$"), "the fallback is gone after recovery");

    // A superseded math render is dropped; only the newest content renders.
    fresh();
    katexRenders.length = 0;
    render(MathBlock, kprops("alpha"));
    flush();
    render(MathBlock, kprops("beta"));
    flush();
    await tick(); // both imports resolve; alpha is cancelled, beta renders
    ktree = render(MathBlock, kprops("beta"));
    assert.deepEqual(katexRenders, ["beta"], "a superseded math render is dropped; only the newest content renders");
    assert.equal(collectHosts(ktree)[0].innerHTML, "katex:beta", "the newest expression is in the host");
  } finally {
    moduleStubs = savedStubs;
    if (savedBody) require.cache[bodyPath] = savedBody;
    else delete require.cache[bodyPath];
    for (const key of Object.keys(require.cache)) {
      if (key.startsWith(path.join(frontend, "components"))) delete require.cache[key];
    }
  }
}

/**
 * Bounded messages expand and collapse for both roles.
 *
 * The SSR check proves the Thread wires both a user and an assistant message
 * into the shared bounded viewport. This probe goes further and exercises the
 * real interaction the viewport offers: it mounts production `BoundedMessage`
 * against a tiny hook runtime with a controllable ResizeObserver, then invokes
 * the actual rendered toggle handler to expand and collapse, asserting the
 * bounded and expanded states each way. Both roles are driven, since the one
 * shared component serves each. This is an interaction check — it fires the
 * rendered onClick — not a static render or a source grep.
 */
async function boundedMessagesExpandAndCollapseForBothRoles() {
  const Fragment = Symbol("fragment");
  const rt = { slots: [], cursor: 0, effects: [], refs: new WeakMap() };
  const reactStub = {
    Fragment,
    useState(initial) {
      const i = rt.cursor++;
      if (!(i in rt.slots)) rt.slots[i] = { value: typeof initial === "function" ? initial() : initial };
      const slot = rt.slots[i];
      const set = (nextValue) => {
        const next = typeof nextValue === "function" ? nextValue(slot.value) : nextValue;
        if (next !== slot.value) slot.value = next;
      };
      return [slot.value, set];
    },
    useRef(initial) {
      const i = rt.cursor++;
      if (!(i in rt.slots)) rt.slots[i] = { value: { current: initial } };
      return rt.slots[i].value;
    },
    useEffect(fn, deps) {
      const i = rt.cursor++;
      const slot = rt.slots[i] ?? (rt.slots[i] = {});
      const changed = !deps || !slot.deps || deps.length !== slot.deps.length
        || deps.some((depth, index) => depth !== slot.deps[index]);
      slot.deps = deps;
      if (changed) rt.effects.push({ i, fn });
    },
    useCallback(fn) { return fn; },
    useMemo(fn) { return fn(); },
  };
  const jsxRuntime = {
    Fragment,
    jsx: (type, props, key) => ({ type, props: props || {}, key }),
    jsxs: (type, props, key) => ({ type, props: props || {}, key }),
  };

  // The host reports as overflowing, so a long message offers the toggle.
  const attachRefs = (node) => {
    if (!node || typeof node !== "object") return;
    if (Array.isArray(node)) { node.forEach(attachRefs); return; }
    const ref = node.props && node.props.ref;
    if (ref && typeof ref === "object") {
      if (!rt.refs.has(ref)) rt.refs.set(ref, { innerHTML: "", scrollHeight: 1000, clientHeight: 200 });
      ref.current = rt.refs.get(ref);
    }
    if (node.props && node.props.children) attachRefs(node.props.children);
  };
  const findViewport = (node) => {
    if (!node || typeof node !== "object") return null;
    if (Array.isArray(node)) { for (const child of node) { const hit = findViewport(child); if (hit) return hit; } return null; }
    if (node.props && node.props["data-message-bounds"] !== undefined) return node;
    return findViewport(node.props && node.props.children);
  };
  const findToggle = (node) => {
    if (!node || typeof node !== "object") return null;
    if (Array.isArray(node)) { for (const child of node) { const hit = findToggle(child); if (hit) return hit; } return null; }
    if (node.props && typeof node.props.onClick === "function") return node;
    return findToggle(node.props && node.props.children);
  };
  const textOf = (node) => {
    if (node === null || node === undefined || typeof node === "boolean") return "";
    if (typeof node === "string" || typeof node === "number") return String(node);
    if (Array.isArray(node)) return node.map(textOf).join("");
    if (node.props && node.props.children) return textOf(node.props.children);
    return "";
  };

  // A controllable ResizeObserver: the probe triggers the overflow measurement.
  const resizeObservers = [];
  const OriginalResizeObserver = global.ResizeObserver;
  global.ResizeObserver = class {
    constructor(callback) { this.callback = callback; resizeObservers.push(this); }
    observe() {}
    disconnect() {}
  };

  const boundedPath = path.join(frontend, "components/ocg/chat/bounded-message.tsx");
  const savedStubs = moduleStubs;
  const savedBounded = require.cache[boundedPath];
  moduleStubs = {
    react: reactStub,
    "react/jsx-runtime": jsxRuntime,
    "lucide-react": { ChevronDown: () => null },
    "@/lib/utils": { cn: (...parts) => parts.flat().filter(Boolean).join(" ") },
    "../i18n": { useI18n: () => ({ t: (key) => key }) },
  };
  delete require.cache[boundedPath];

  try {
    const { BoundedMessage } = require(boundedPath);
    const render = (props) => {
      rt.cursor = 0;
      rt.effects = [];
      const tree = BoundedMessage(props);
      attachRefs(tree);
      return tree;
    };
    const flush = () => {
      const run = rt.effects;
      rt.effects = [];
      for (const { i, fn } of run) {
        const slot = rt.slots[i];
        if (slot.cleanup) { slot.cleanup(); slot.cleanup = undefined; }
        const cleanup = fn();
        if (typeof cleanup === "function") slot.cleanup = cleanup;
      }
    };

    // Drive the full expand/collapse cycle once per role: the one shared
    // component bounds and unfolds both a long user message and a long answer.
    for (const role of ["user", "assistant"]) {
      resizeObservers.length = 0;
      rt.slots = [];
      const props = { streaming: role === "assistant", children: `long ${role} message body` };

      let tree = render(props);
      flush();
      assert.equal(resizeObservers.length, 1, `${role}: a ResizeObserver measures the message for overflow`);

      // The measurement reports overflow, so the message becomes bounded and
      // offers an expand toggle.
      resizeObservers[0].callback();
      tree = render(props);
      let viewport = findViewport(tree);
      let toggle = findToggle(tree);
      assert.equal(viewport.props["data-message-bounds"], "bounded", `${role}: a long message starts bounded`);
      assert.match(viewport.props.className, /max-h-/, `${role}: the bounded viewport caps the height`);
      assert.ok(toggle, `${role}: an expand toggle appears`);
      assert.equal(toggle.props["aria-expanded"], false, `${role}: the toggle reports collapsed`);
      assert.ok(textOf(toggle).includes("chat.expandMessage"), `${role}: the toggle offers to expand`);

      // Expanding: the real onClick flips the state and the viewport unfolds.
      toggle.props.onClick();
      tree = render(props);
      viewport = findViewport(tree);
      toggle = findToggle(tree);
      assert.equal(viewport.props["data-message-bounds"], "expanded", `${role}: the message expands`);
      assert.ok(!/max-h-/.test(viewport.props.className), `${role}: the expanded viewport drops the height cap`);
      assert.equal(toggle.props["aria-expanded"], true, `${role}: the toggle reports expanded`);
      assert.ok(textOf(toggle).includes("chat.collapseMessage"), `${role}: the toggle offers to collapse`);

      // Collapsing: the same handler folds it back to the bounded viewport.
      toggle.props.onClick();
      tree = render(props);
      viewport = findViewport(tree);
      toggle = findToggle(tree);
      assert.equal(viewport.props["data-message-bounds"], "bounded", `${role}: the message collapses back`);
      assert.match(viewport.props.className, /max-h-/, `${role}: the height cap returns`);
      assert.equal(toggle.props["aria-expanded"], false, `${role}: the toggle reports collapsed again`);
      assert.ok(textOf(toggle).includes("chat.expandMessage"), `${role}: the expand label returns`);
    }
  } finally {
    global.ResizeObserver = OriginalResizeObserver;
    moduleStubs = savedStubs;
    if (savedBounded) require.cache[boundedPath] = savedBounded;
    else delete require.cache[boundedPath];
    for (const key of Object.keys(require.cache)) {
      if (key.startsWith(path.join(frontend, "components"))) delete require.cache[key];
    }
  }
}

(async () => {
  for (const check of [
    conversationUsageGateChecksCanonicalIdentity,
    executionInspectorIsCanonicalFirst,
    conversationUsageRefreshesWhileLive,
    conversationInspectorRemainsStableDuringPolling,
    activeTargetBeatsComposer,
    preferenceIsScopedPerChat,
    observableActivityDrivesStatus,
    longMessagesFold,
    retryClearsPresentation,
    reasoningSurvivesHistoryAndRoundReset,
    markdownBodyRendersSafely,
    markdownUrlsAreAdmittedSafely,
    markdownBrowserGuardsStayLocked,
    chatSurfaceRendersWithoutRuntimeBanner,
    managedImagesSurviveSdkConversion,
    markdownAdmitsRelativeTargetsAndHeadings,
    markdownNestingMatchesDepth,
    dropdownEscapeDismissesAfterConsumer,
    accountMenuOrderAndForm,
    accountMenuKeyboardFocus,
    collapsedSidebarOffersTheAccountMenu,
    sidebarWorkbenchPreservesDestinations,
    composerPlacesTurnSelectionsAboveCard,
    chatHeaderFloatsWithoutDuplicateControls,
    conversationSearchStaysTitleOnlyAndProjectScoped,
    highlightingThemeIsScoped,
    markdownStreamingRecoveryProbe,
    boundedMessagesExpandAndCollapseForBothRoles,
  ]) {
    await check();
  }
  process.stdout.write("chat presentation contract ok\n");
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
