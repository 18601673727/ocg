import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createWriteStream } from "node:fs";
import { writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const [directory, backendPath, uiDirectory] = process.argv.slice(2);
const turnCount = 56;
const updateCount = 72;
const backendToken = randomBytes(32).toString("hex");
function id() {
  const bytes = randomBytes(16);
  bytes.writeUIntBE(Date.now(), 0, 6);
  bytes[6] = (bytes[6] & 15) | 112;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = bytes.toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
}
function listen(server) {
  return new Promise((resolve) =>
    server.listen(0, "127.0.0.1", () => resolve(server.address().port)),
  );
}
const reservation = createServer();
const backendPort = await listen(reservation);
await new Promise((resolve) => reservation.close(resolve));
const origin = `http://127.0.0.1:${backendPort}`;
const backend = spawn(backendPath, [], {
  env: {
    ...process.env,
    OCG_DATA_DIR: join(directory, "data"),
    OCG_API_PORT: String(backendPort),
    OCG_API_TOKEN: backendToken,
    OCG_UI_DIR: uiDirectory,
  },
  stdio: ["ignore", "pipe", "pipe"],
});
const backendLog = createWriteStream(join(directory, "rust.log"));
backend.stdout.pipe(backendLog, { end: false });
backend.stderr.pipe(backendLog, { end: false });
let backendError;
backend.once("error", (error) => {
  backendError = error;
});
backend.once("close", () => backendLog.end());

let stopping = false;
function stop() {
  if (stopping) return;
  stopping = true;
  provider.close();
  provider.closeAllConnections();
  if (backend.exitCode === null && backend.signalCode === null) backend.kill("SIGTERM");
}
process.once("disconnect", stop);
process.once("SIGTERM", stop);

function assistantText(n) {
  return `### Review ${n}\n\nThe API contract handles the cursor and the stream as separate concerns. The browser app uses the saved identifier to hydrate the transcript. This note records how the selection and status changed.\n\n- Keep the current anchor when older messages arrive.\n- Render new output in the active response.\n- Check the settled state before offering a retry.\n\n\`\`\`typescript\nconst response = await client.readConversation(job);\nif (response.ok) restoreScrollAnchor();\n\`\`\`\n\nResume after the cursor from the last observed event.\n`;
}
let liveJobs = 0;
let gated;
let fixture;
const provider = createServer(async (req, res) => {
  try {
    if (req.method === "GET" && req.url === "/control/status") {
      const status = { waiting: Boolean(gated) };
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(status));
      return;
    }
    if (req.method === "POST" && req.url === "/control/new") {
      const prefix = "LIVE" + String(++liveJobs).padStart(2, "0");
      const newJob = await job(fixture.config, prefix, fixture.root);
      res.end(JSON.stringify({ prefix, job: newJob.id }));
      return;
    }
    if (req.method === "POST" && req.url === "/control/release") {
      if (!gated) {
        res.statusCode = 409;
        res.end("no pending stream");
        return;
      }
      const resume = gated;
      gated = undefined;
      resume();
      res.end("ok");
      return;
    }
    if (req.method === "GET" && req.url?.endsWith("/models")) {
      res.setHeader("Content-Type", "application/json");
      res.end(
        JSON.stringify({
          object: "list",
          data: [{ id: "ocg-fixture", object: "model", owned_by: "local" }],
        }),
      );
      return;
    }
    if (req.method !== "POST" || !req.url?.endsWith("/chat/completions")) {
      res.statusCode = 404;
      res.end();
      return;
    }
    let raw = "";
    for await (const chunk of req) raw += chunk;
    const request = JSON.parse(raw);
    const user = request.messages?.findLast((message) => message.role === "user");
    const latest = JSON.stringify(user?.content);
    const live = latest?.match(/LIVE\d+/)?.[0];
    const turn = latest?.match(/Turn (\d+)/)?.[1] ?? "00";
    if (live) {
      await new Promise((resolve) => {
        gated = resolve;
      });
    }
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });
    res.flushHeaders();
    const send = (choices, usage) =>
      res.write(
        `data: ${JSON.stringify({ id: "chatcmpl-fixture", object: "chat.completion.chunk", model: "ocg-fixture", created: Math.floor(Date.now() / 1000), choices, usage })}\n\n`,
      );
    send([{ index: 0, delta: { role: "assistant" }, finish_reason: null }]);
    if (live) {
      const events = {};
      await writeFile(join(directory, live + ".json"), "{}");
      for (let i = 1; i <= updateCount; i++) {
        if (res.destroyed) return;
        const marker = live + "-" + String(i).padStart(3, "0");
        events[marker] = Date.now();
        const body = i % 6 === 1 ? `\n\n### Update ${i}\n\n` : "";
        send([
          {
            index: 0,
            delta: {
              content: `${body}${marker} The conversation output advances while the existing messages stay in place. `,
            },
            finish_reason: null,
          },
        ]);
        await sleep(35);
      }
      await writeFile(join(directory, live + ".json"), JSON.stringify(events));
    } else {
      send([{ index: 0, delta: { content: assistantText(turn) }, finish_reason: null }]);
    }
    send([{ index: 0, delta: {}, finish_reason: "stop" }]);
    send([], { prompt_tokens: 256, completion_tokens: 256, total_tokens: 512 });
    res.end("data: [DONE]\n\n");
  } catch (error) {
    console.error(error);
    if (!res.headersSent) res.statusCode = 500;
    res.end("fixture error");
  }
});
const providerPort = await listen(provider);
async function api(route, body, extra = {}) {
  const res = await fetch(origin + "/api/v1" + route, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      authorization: `Bearer ${backendToken}`,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
      ...extra,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`${route} HTTP ${res.status} ${(await res.text()).slice(0, 500)}`);
  return res.json();
}
async function waitUntil(action, timeout = 15000) {
  const stop = Date.now() + timeout;
  while (Date.now() < stop) {
    const current = await action();
    if (current) return current;
    await sleep(40);
  }
  throw new Error("Fixture deadline exceeded");
}
async function job(config, objective, root) {
  const context = id();
  const headers = () => ({ "x-ocg-composer-context": context, "idempotency-key": id() });
  const result = await api(
    "/jobs",
    {
      kind: "generative",
      text: objective,
      project: config.project,
      ...(root ? { conversation: root } : {}),
      configuration: { llm_profile: config.llm, agent_profile: config.agent },
    },
    headers(),
  );
  const { job: created } = result;
  const submitted = await api(
    `/jobs/${created.id}/submit`,
    { revision: created.revision },
    headers(),
  );
  return submitted.job;
}
try {
  await waitUntil(async () => {
    if (backendError) throw backendError;
    if (backend.exitCode !== null || backend.signalCode !== null) {
      throw new Error("Rust backend exited. See " + join(directory, "rust.log"));
    }
    return api("/health").catch(() => false);
  });
  const pool = id();
  await api("/pools", { id: pool, name: "fixture pool", capacity: 8, enabled: true });
  await api("/executors", {
    id: id(),
    pool,
    name: "fixture executor",
    enabled: true,
    capabilities: [{ operation: "llm_request", protocol: "openai_chat_completions" }],
  });
  const llm = await api("/profiles", {
    kind: "llm",
    name: "fixture model",
    model: "ocg-fixture",
    protocol: "openai_chat_completions",
    endpoint: `http://127.0.0.1:${providerPort}/v1/chat/completions`,
    credential_ref: "BENCHMARK_FIXTURE",
    credential: "local-only-fixture",
    declared_capabilities: ["chat", "tools"],
    context_window: 131072,
    parameters: { max_tokens: 2048 },
  });
  const agent = await api("/profiles", {
    kind: "agent",
    name: "fixture agent",
    capabilities: ["chat", "coding"],
    llm_targets: [llm.profile_id],
  });
  const { project } = await api("/projects", {
    name: "Conversation benchmark",
    root: join(directory, "project"),
  });
  await api(`/projects/${project.id}/decision`, {
    enabled: false,
    decision_profile: null,
    minimum_confidence: 0.6,
  });
  const config = { project: project.id, llm: llm.profile_id, agent: agent.profile_id };
  let root;
  for (let i = 1; i <= turnCount; i++) {
    const created = await job(
      config,
      `Turn ${String(i).padStart(2, "0")}: Review cursor pagination and transcript navigation in the operator console.`,
      root,
    );
    root ??= created.conversation;
    await waitUntil(async () => {
      const { job: detail } = await api(`/jobs/${created.id}`);
      const status = detail.settlement.state;
      if (status === "failed" || status === "cancelled" || status === "unknown")
        throw new Error("Fixture job " + i + " " + JSON.stringify(detail.settlement));
      return status === "completed";
    });
    if (i % 10 === 0 || i === turnCount) console.log("Seeded turns:", i);
  }
  fixture = { root, config };
  process.send({ origin, providerPort, root, turnCount, updateCount });
} catch (error) {
  console.error(error);
  process.exitCode = 1;
  stop();
  if (process.connected) process.disconnect();
}
