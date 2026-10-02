import type {
  ChatMessage,
  ChatSession,
  RuntimeStatus,
  ToolActivity,
} from "../types";
import type { ScenarioId } from "./runtime-types";
import type {
  RuntimeActivityItem,
  RuntimeObservability,
  UsageTimelinePoint,
  WorkerRuntimeStats,
} from "./observability";
import { usage } from "./observability";
import { createBootstrapFixture } from "../bootstrap/fixtures";
import { createResourceLedgerFixture } from "../resource-ledger/fixtures";
import type { BootstrapState } from "../bootstrap/types";
import type { ResourceLedger } from "../resource-ledger/types";

/**
 * What a route opens when the URL does not name a scenario.
 *
 * `local-ready` is the workspace the loopback control endpoint serves for an
 * operational backend, so an unattributed URL binds Chat to the canonical
 * runtime instead of silently landing on a fixture. Mock scenarios stay
 * reachable, but only by naming one explicitly with `?scenario=`.
 */
export const DEFAULT_SCENARIO: ScenarioId = "local-ready";
export const SCENARIO_IDS: readonly ScenarioId[] = [
  "normal-chat",
  "long-stream",
  "tool-heavy",
  "worker-parallel",
  "build-failed",
  "retry-success",
  "job-completed",
  "budget-exhausted",
  "runtime-disconnected",
  "runtime-connecting",
  "runtime-failed",
  "permission-required",
  "observability-live",
  "resource-ledger",
  "local-ready",
  "local-first-run",
  "remote-unauthenticated",
  "remote-session-expired",
  "remote-denied",
  "remote-authenticated-ready",
  "remote-authenticated-first-run",
  "onboarding-resume",
  "onboarding-migration",
  "onboarding-recovery",
  "onboarding-invalid-configuration",
  "onboarding-auth-required",
  "onboarding-connection-failure",
  "onboarding-discovery",
  "onboarding-ready",
  "profiles-models",
  "job-execution",
  "logs-live",
  "home-overview",
  "home-calm",
  "attention-overview",
  "attention-calm",
];

export function resolveScenario(value: string | undefined | null): ScenarioId {
  return SCENARIO_IDS.includes(value as ScenarioId) ? (value as ScenarioId) : DEFAULT_SCENARIO;
}

const baseSession: ChatSession = {
  id: "design-pwa-shell",
  title: "OCG PWA shell",
  workType: "design",
  updatedAt: "now",
};

const sessions: ChatSession[] = [
  baseSession,
  { id: "research-example-model", title: "Example model architecture study", workType: "research", updatedAt: "2h ago" },
  { id: "research-rust-graph", title: "Rust graph storage options", workType: "research", updatedAt: "1d ago" },
  { id: "coding-mission-runtime", title: "OCG durable job runtime", workType: "coding", updatedAt: "12m ago" },
  { id: "coding-tool-gateway", title: "Tool Call Gateway", workType: "coding", updatedAt: "3h ago" },
  { id: "design-resource-controls", title: "Job resource controls", workType: "design", updatedAt: "2d ago" },
  { id: "devops-debian-runtime", title: "Debian runtime setup", workType: "devops", updatedAt: "5h ago" },
  { id: "devops-cloudflare-access", title: "Cloudflare remote access", workType: "devops", updatedAt: "4d ago" },
];

function message(
  id: string,
  role: ChatMessage["role"],
  content: string,
  status: ChatMessage["status"] = "completed",
  tool?: ToolActivity,
): ChatMessage {
  return { id, role, content, status, createdAt: "10:03", ...(tool ? { tool } : {}) };
}

function tool(id: string, status: ToolActivity["status"], name: string, summary: string): ToolActivity {
  return {
    id,
    name,
    status,
    durationMs: status === "running" ? 0 : 184,
    summary,
    detail: `scenario fixture: ${name}\nresult: ${status}`,
    ...(status === "retrying" ? { retryCount: 1 } : {}),
  };
}

function normalMessages(): ChatMessage[] {
  return [
    message("normal-u1", "user", "What should we preserve while the runtime boundary is still local?"),
    message(
      "normal-a1",
      "assistant",
      "Keep the shell, conversation state, and Job state separate. The mock runtime owns the data today, so a future transport can replace it without changing the panels.",
    ),
  ];
}

function genericMessages(session: ChatSession): ChatMessage[] {
  return [
    message(
      `${session.id}-u1`,
      "user",
      `Continuing "${session.title}". What is the current open question for this ${session.workType} thread?`,
    ),
    message(
      `${session.id}-a1`,
      "assistant",
      `Open question for **${session.title}**:\n\n- Scope the smallest deliverable first.\n- Keep UI state local until the runtime contract is frozen.\n- Record decisions in the thread for the later durable Job.\n\nThis is mock content — no backend is connected in this phase.`,
    ),
  ];
}

function runtimeWorker(
  workerId: string,
  label: string,
  provider: string,
  model: string,
  overrides: Partial<WorkerRuntimeStats> = {},
): WorkerRuntimeStats {
  return {
    workerId,
    role: workerId === "lead" ? "lead" : "worker",
    label,
    provider,
    model,
    status: "idle",
    invocationCount: 0,
    retryCount: 0,
    tokenUsage: {},
    ...overrides,
  };
}

function timeline(...points: UsageTimelinePoint[]): UsageTimelinePoint[] {
  return points;
}

function activity(
  id: string,
  elapsedMs: number,
  kind: RuntimeActivityItem["kind"],
  summary: string,
  worker?: Pick<WorkerRuntimeStats, "workerId" | "label" | "role" | "provider" | "model" | "status">,
): RuntimeActivityItem {
  return {
    id,
    timestamp: `00:${String(Math.floor(elapsedMs / 1000)).padStart(2, "0")}`,
    elapsedMs,
    kind,
    summary,
    ...(worker
      ? {
          workerId: worker.workerId,
          workerLabel: worker.label,
          role: worker.role,
          provider: worker.provider,
          model: worker.model,
          status: worker.status,
        }
      : {}),
  };
}

function createDefaultObservability(jobId: string): RuntimeObservability {
  const workers = [
    runtimeWorker("lead", "Lead-Mid", "Provider", "example-model", {
      status: "active",
      variant: "mid",
      elapsedMs: 2520000,
      invocationCount: 2,
      retryCount: 0,
      successCount: 2,
      tokenUsage: { input: usage(8400), output: usage(3100), total: usage(11500, "estimated") },
      costMicros: usage(92000, "estimated"),
      latencyMs: 940,
      ttftMs: 180,
      tokensPerSecond: 18.2,
    }),
    runtimeWorker("ocg-local", "Local worker", "Catalog Provider", "example-model-free", {
      status: "completed",
      elapsedMs: 86000,
      invocationCount: 1,
      retryCount: 0,
      successCount: 1,
      tokenUsage: { input: usage(2600), output: usage(1200), total: usage(3800) },
      costMicros: usage(0),
      latencyMs: 510,
      tokensPerSecond: 22.3,
    }),
  ];
  const total = usage(15300, "estimated");
  return {
    job: {
      jobId,
      tokenUsage: { input: usage(11000), output: usage(4300), total },
      costMicros: usage(92000, "estimated"),
      elapsedMs: 2520000,
      invocationCount: 3,
      retryCount: 0,
      activeWorkerCount: 1,
      estimatedFinalSpend: usage(11, "estimated"),
    },
    workers,
    activities: [
      activity("default-lead-started", 0, "worker-started", "Lead runtime connected", workers[0]),
      activity("default-worker-completed", 86000, "worker-completed", "Local worker completed", workers[1]),
    ],
    timeline: timeline(
      { timestamp: "00:00", elapsedMs: 0, cumulativeUsage: { input: usage(0), output: usage(0), total: usage(0) } },
      { timestamp: "21:00", elapsedMs: 1260000, cumulativeUsage: { input: usage(7600), output: usage(2600), total: usage(10200, "estimated") } },
      { timestamp: "42:00", elapsedMs: 2520000, cumulativeUsage: { input: usage(11000), output: usage(4300), total } },
    ),
  };
}

function createLiveObservability(jobId: string): RuntimeObservability {
  return {
    job: {
      jobId,
      tokenUsage: { input: usage(6400), output: usage(1800), total: usage(8200, "estimated") },
      costMicros: usage(51000, "estimated"),
      elapsedMs: 18000,
      invocationCount: 4,
      retryCount: 0,
      activeWorkerCount: 3,
      estimatedFinalSpend: usage(10.4, "estimated"),
    },
    workers: [
      runtimeWorker("lead", "Lead-Mid", "Provider", "example-model", {
        role: "lead", status: "active", variant: "mid", elapsedMs: 18000, invocationCount: 1,
        tokenUsage: { input: usage(2400), output: usage(900), total: usage(3300, "estimated") },
        costMicros: usage(22000, "estimated"), latencyMs: 820, ttftMs: 140, tokensPerSecond: 34,
      }),
      runtimeWorker("explore", "Explore", "Fallback Provider", "fallback-example-model", {
        status: "active", variant: "standard", elapsedMs: 14000, invocationCount: 1,
        tokenUsage: { input: usage(1900), output: usage(500), total: usage(2400, "estimated") },
        costMicros: usage(12000, "estimated"), latencyMs: 610, ttftMs: 110, tokensPerSecond: 36,
      }),
      runtimeWorker("explore-deep", "Explore Deep", "Fallback Provider", "fallback-example-model", {
        status: "active", variant: "deep", elapsedMs: 9000, invocationCount: 1,
        tokenUsage: { input: usage(1300), output: usage(400), total: usage(1700, "estimated") },
        costMicros: usage(9000, "estimated"), latencyMs: 730, tokensPerSecond: 31,
      }),
      runtimeWorker("build", "Build", "Provider", "DeepSeek V4.1 Flash", { status: "queued" }),
      runtimeWorker("verify", "Verify", "Catalog Provider", "example-model-free", { status: "waiting" }),
      runtimeWorker("debug", "Debug", "Fallback Provider", "fallback-example-model", { status: "queued" }),
      runtimeWorker("docs", "Docs", "Provider", "example-model", { status: "queued" }),
    ],
    timeline: timeline(
      { timestamp: "00:00", elapsedMs: 0, cumulativeUsage: { total: usage(0) } },
      { timestamp: "00:06", elapsedMs: 6000, cumulativeUsage: { input: usage(3900), output: usage(900), total: usage(4800, "estimated") } },
      { timestamp: "00:12", elapsedMs: 12000, cumulativeUsage: { input: usage(5600), output: usage(1400), total: usage(7000, "estimated") } },
      { timestamp: "00:18", elapsedMs: 18000, cumulativeUsage: { input: usage(6400), output: usage(1800), total: usage(8200, "estimated") } },
    ),
    activities: [
      activity("live-lead-started", 0, "worker-started", "Lead started coordinating the Job", { workerId: "lead", label: "Lead-Mid", role: "lead", provider: "Provider", model: "example-model", status: "active" }),
      activity("live-explore-started", 4000, "worker-started", "Explore started runtime mapping", { workerId: "explore", label: "Explore", role: "worker", provider: "Fallback Provider", model: "fallback-example-model", status: "active" }),
      activity("live-deep-started", 9000, "worker-started", "Explore Deep started architecture review", { workerId: "explore-deep", label: "Explore Deep", role: "worker", provider: "Fallback Provider", model: "fallback-example-model", status: "active" }),
      activity("live-usage-estimated", 18000, "invocation-started", "Streaming usage is estimated"),
    ],
  };
}

function updateLiveObservability(base: RuntimeObservability, step: 1 | 2 | 3): RuntimeObservability {
  const next = JSON.parse(JSON.stringify(base)) as RuntimeObservability;
  if (step === 1) {
    const explore = next.workers.find((worker) => worker.workerId === "explore");
    const build = next.workers.find((worker) => worker.workerId === "build");
    if (explore) Object.assign(explore, {
      status: "completed", finishedAt: "00:25", elapsedMs: 25000, invocationCount: 1,
      successCount: 1, tokenUsage: { input: usage(2500), output: usage(760), total: usage(3260) },
      costMicros: usage(15000), latencyMs: 640, tokensPerSecond: 32,
    });
    if (build) Object.assign(build, {
      status: "starting", startedAt: "00:24", elapsedMs: 9000, invocationCount: 1,
      tokenUsage: { input: usage(1500), output: usage(300), total: usage(1800, "estimated") },
      costMicros: usage(7000, "estimated"), latencyMs: 480, ttftMs: 120,
    });
    next.job = { ...next.job, tokenUsage: { input: usage(8300), output: usage(2300), total: usage(10600, "estimated") }, costMicros: usage(68000, "estimated"), elapsedMs: 25000, invocationCount: 5, activeWorkerCount: 3 };
    next.activities = [...next.activities, activity("live-explore-completed", 25000, "worker-completed", "Explore completed runtime mapping", next.workers.find((worker) => worker.workerId === "explore")), activity("live-build-started", 25000, "worker-started", "Build started implementation", next.workers.find((worker) => worker.workerId === "build")), activity("live-verify-waiting", 25000, "worker-waiting", "Verify is waiting for the build", next.workers.find((worker) => worker.workerId === "verify"))];
    next.timeline = [...next.timeline, { timestamp: "00:25", elapsedMs: 25000, cumulativeUsage: { input: usage(8300), output: usage(2300), total: usage(10600, "estimated") } }];
  }
  if (step === 2) {
    const build = next.workers.find((worker) => worker.workerId === "build");
    const verify = next.workers.find((worker) => worker.workerId === "verify");
    const deep = next.workers.find((worker) => worker.workerId === "explore-deep");
    if (deep) Object.assign(deep, { status: "completed", finishedAt: "00:31", elapsedMs: 31000, successCount: 1, tokenUsage: { input: usage(1500), output: usage(490), total: usage(1990) }, costMicros: usage(10000), latencyMs: 760, tokensPerSecond: 29 });
    if (build) Object.assign(build, { status: "active", elapsedMs: 18000, invocationCount: 2, retryCount: 1, tokenUsage: { input: usage(2100), output: usage(600), total: usage(2700, "estimated") }, costMicros: usage(11000, "estimated"), latencyMs: 540 });
    if (verify) Object.assign(verify, { status: "active", startedAt: "00:33", elapsedMs: 5000, invocationCount: 1, tokenUsage: { input: usage(900), output: usage(120), total: usage(1020, "estimated") }, latencyMs: 390 });
    next.job = { ...next.job, tokenUsage: { input: usage(10800), output: usage(3200), total: usage(14000, "estimated") }, costMicros: usage(88000, "estimated"), elapsedMs: 36000, invocationCount: 7, retryCount: 1, activeWorkerCount: 3 };
    next.activities = [...next.activities, activity("live-build-failed", 32000, "invocation-failed", "Build invocation failed transiently", build), activity("live-build-retry", 33000, "retry-scheduled", "Build retry scheduled after transient failure", build), activity("live-verify-started", 36000, "worker-started", "Verify started the post-build check", verify)];
    next.timeline = [...next.timeline, { timestamp: "00:36", elapsedMs: 36000, cumulativeUsage: { input: usage(10800), output: usage(3200), total: usage(14000, "estimated") } }];
  }
  if (step === 3) {
    next.workers = next.workers.map((worker) => worker.workerId === "docs"
      ? { ...worker, status: "completed", elapsedMs: 6000, invocationCount: 1, successCount: 1, tokenUsage: { input: usage(700), output: usage(220), total: usage(920) } }
      : { ...worker, status: worker.status === "idle" ? "completed" : "completed", finishedAt: "00:44", successCount: worker.successCount ?? 1, tokenUsage: Object.fromEntries(Object.entries(worker.tokenUsage).map(([key, value]) => [key, value && { ...value, provenance: "reported" }])) as WorkerRuntimeStats["tokenUsage"] });
    next.job = { ...next.job, tokenUsage: { input: usage(12400, "reported"), output: usage(3900, "reported"), total: usage(16300, "reported") }, costMicros: usage(104000, "reported"), estimatedFinalSpend: usage(10.4, "reported"), elapsedMs: 44000, invocationCount: 9, retryCount: 1, activeWorkerCount: 0 };
    next.activities = [...next.activities, activity("live-usage-finalized", 44000, "usage-finalized", "Provider-reported usage finalized"), activity("live-job-complete", 44000, "job-transition", "Job completed")];
    next.timeline = [...next.timeline, { timestamp: "00:44", elapsedMs: 44000, cumulativeUsage: { input: usage(12400, "reported"), output: usage(3900, "reported"), total: usage(16300, "reported") } }];
  }
  return next;
}

export type ScenarioFixture = {
  id: ScenarioId;
  runtimeStatus: RuntimeStatus;
  sessions: ChatSession[];
  messagesBySession: Record<string, ChatMessage[]>;
  observabilityBySession: Record<string, RuntimeObservability | null>;
  resourceLedger: ResourceLedger | null;
  bootstrap: BootstrapState;
  observabilityUpdates?: { afterMs: number; sessionId: string; observability: RuntimeObservability }[];
  streamChunks?: string[];
  streamDelayMs?: number;
};

export function createScenarioFixture(id: ScenarioId): ScenarioFixture {
  const fixture: ScenarioFixture = {
    id,
    runtimeStatus: { state: "connected", detail: "local mock runtime" },
    bootstrap: createBootstrapFixture(id),
    sessions,
    messagesBySession: Object.fromEntries(
      sessions.map((session) => [session.id, session.id === baseSession.id ? normalMessages() : genericMessages(session)]),
    ),
    observabilityBySession: Object.fromEntries(sessions.map((session) => [session.id, createDefaultObservability(session.id)])),
    resourceLedger: createResourceLedgerFixture(id),
    streamChunks: [
      "Mock runtime received your message. ",
      "This response is streamed locally, ",
      "one deterministic chunk at a time.",
    ],
    streamDelayMs: 90,
  };

  switch (id) {
    case "long-stream":
      fixture.messagesBySession[baseSession.id] = [
        message("stream-u1", "user", "Show me the runtime state as it arrives."),
        message("stream-a1", "assistant", "The stream is still arriving…", "streaming"),
      ];
      fixture.streamChunks = [
        "The stream is still arriving… ",
        "Each delta updates the frontend-owned message, ",
        "without exposing transport event names to ChatView.",
      ];
      fixture.streamDelayMs = 180;
      break;
    case "tool-heavy":
      fixture.messagesBySession[baseSession.id] = [
        message("tool-u1", "user", "Inspect the workspace and validate the shell."),
        message("tool-t1", "tool", "", "completed", tool("tool-t1", "success", "workspace.read", "Read package and shell files")),
        message("tool-t2", "tool", "", "completed", tool("tool-t2", "running", "pnpm lint", "Checking frontend boundaries")),
        message("tool-t3", "tool", "", "completed", tool("tool-t3", "retrying", "build.verify", "Retrying after a transient failure")),
        message("tool-a1", "assistant", "The mock runtime can show several activities without coupling the UI to a backend protocol."),
      ];
      break;
    case "observability-live": {
      const live = createLiveObservability(baseSession.id);
      fixture.observabilityBySession[baseSession.id] = live;
      const step1 = updateLiveObservability(live, 1);
      const step2 = updateLiveObservability(step1, 2);
      const step3 = updateLiveObservability(step2, 3);
      fixture.observabilityUpdates = [
        { afterMs: 650, sessionId: baseSession.id, observability: step1 },
        { afterMs: 1300, sessionId: baseSession.id, observability: step2 },
        { afterMs: 1950, sessionId: baseSession.id, observability: step3 },
      ];
      break;
    }
    case "build-failed":
      fixture.runtimeStatus = { state: "failed", detail: "Build verification reported a fixture failure" };
      fixture.messagesBySession[baseSession.id] = [
        message("failed-u1", "user", "Run the build verification."),
        message("failed-t1", "tool", "", "failed", tool("failed-t1", "failure", "build.verify", "Build failed with fixture error")),
        message("failed-a1", "assistant", "The build step failed in this scenario. Retry is available from the future runtime.", "failed"),
      ];
      break;
    case "retry-success":
      fixture.messagesBySession[baseSession.id] = [
        message("retry-u1", "user", "Retry the failed build."),
        message("retry-t1", "tool", "", "completed", tool("retry-t1", "success", "build.verify", "Succeeded on retry")),
        message("retry-a1", "assistant", "The retry succeeded. The Job can continue without hiding the earlier failure."),
      ];
      break;
    case "runtime-disconnected":
      fixture.runtimeStatus = { state: "disconnected", detail: "No local runtime is connected" };
      break;
    case "runtime-connecting":
      fixture.runtimeStatus = { state: "connecting", detail: "Connecting to the local mock runtime" };
      break;
    case "runtime-failed":
      fixture.runtimeStatus = { state: "failed", detail: "The local mock runtime failed to start" };
      break;
    case "permission-required":
      fixture.messagesBySession[baseSession.id] = [
        message("permission-u1", "user", "Apply the proposed workspace change."),
        message("permission-t1", "tool", "", "pending", tool("permission-t1", "waiting-approval", "workspace.write", "Waiting for operator approval")),
        message("permission-a1", "assistant", "The next activity is waiting for approval before it can continue."),
      ];
      break;
    case "home-overview": {
      // Deterministic busy workspace for the Home overview surface.
      // Uses profiles-models bootstrap for full resource data. Canonical Job
      // execution comes from the execution fixtures, not a parallel object.
      fixture.bootstrap = createBootstrapFixture("profiles-models");
      break;
    }
    case "home-calm": {
      // Calm workspace with no active jobs and no attention items.
      fixture.bootstrap = createBootstrapFixture("local-ready");
      fixture.resourceLedger = createResourceLedgerFixture("home-calm");
      break;
    }
    case "attention-overview": {
      // Representative actionable mix: spend + retry + launch approvals,
      // a runtime failure needing inspection, provider degradation from the
      // shared bootstrap, real execution-blocked work, and resolved history.
      // Reads the canonical Job execution projection so cross-links stay
      // consistent with Job Execution.
      fixture.bootstrap = createBootstrapFixture("profiles-models");
      fixture.resourceLedger = createResourceLedgerFixture("attention-overview");
      break;
    }
    case "attention-calm": {
      // Healthy workspace: local-ready bootstrap with the optional
      // auth-required connection resolved, no jobs, no ledger,
      // history-only fixture queue. Empty state must look intentional.
      fixture.bootstrap = createBootstrapFixture("local-ready");
      fixture.bootstrap.connections = fixture.bootstrap.connections.filter(
        (connection) => connection.state !== "auth-required",
      );
      fixture.resourceLedger = null;
      break;
    }
  }

  return fixture;
}
