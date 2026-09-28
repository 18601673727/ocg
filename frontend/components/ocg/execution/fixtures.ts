import type { MissionExecution, ExecutionActivityItem, ExecutionEdge, ExecutionTask, WorkerExecution } from "./domain";
import { taskStatusCounts } from "./domain";

const missionId = "mission-architecture-consolidation";

function task(input: Omit<ExecutionTask, "missionId" | "dependents">): ExecutionTask {
  return { ...input, missionId, dependents: [] };
}

function edge(fromTaskId: string, toTaskId: string, kind: ExecutionEdge["kind"] = "dependency"): ExecutionEdge {
  return { id: `edge-${fromTaskId}-${toTaskId}`, fromTaskId, toTaskId, kind, status: "completed" };
}

const tasks: ExecutionTask[] = [
  task({ id: "recon", title: "Recon current surfaces", description: "Map the existing OCG shell, runtime boundary, and product surfaces before proposing changes.", category: "Recon", status: "completed", wave: 1, dependencies: [], workerId: "explore", workerRole: "Explore", provider: "OpenCode Go", model: "Space Bunny Free", variant: "deep", elapsedMs: 420000, finishedAt: "00:07:00", outputSummary: "Surface inventory captured with no backend changes.", usageSummary: { tokens: 12800, cost: 0.04 }, schedulingReason: "Independent repository reconnaissance can start immediately.", priority: "high" }),
  task({ id: "inventory", title: "Inventory runtime state", description: "Record existing normalized Mission, Worker, observability, and resource data available to the UI.", category: "Recon", status: "completed", wave: 1, dependencies: [], workerId: "explore-deep", workerRole: "Explore Deep", provider: "OpenCode Go", model: "Space Bunny Free", variant: "deep", elapsedMs: 510000, finishedAt: "00:08:30", outputSummary: "Existing domain seams and fixture conventions documented.", usageSummary: { tokens: 16400, cost: 0.05 }, schedulingReason: "Runs in parallel with surface reconnaissance.", priority: "high" }),
  task({ id: "foundation", title: "Define execution foundation", description: "Add the frontend-owned execution read model without introducing a transport or scheduler implementation.", category: "Foundation", status: "completed", wave: 2, dependencies: ["recon", "inventory"], workerId: "lead", workerRole: "Lead", provider: "Command Code", model: "Muse Spark 1.3 Contributor", variant: "mid", elapsedMs: 720000, finishedAt: "00:21:00", outputSummary: "Execution identity, status, wave, gate, and activity projections are defined.", usageSummary: { tokens: 21900, cost: 0.09 }, schedulingReason: "Scheduled after Recon and State Inventory because both define the compatibility boundary.", priority: "high" }),
  task({ id: "design-system", title: "Set status language", description: "Align graph statuses, badges, and accessible labels with the existing restrained OCG visual language.", category: "Foundation", status: "completed", wave: 2, dependencies: ["recon"], workerId: "lead", workerRole: "Lead", provider: "Command Code", model: "Muse Spark 1.3 Contributor", variant: "mid", elapsedMs: 360000, finishedAt: "00:18:00", outputSummary: "Status semantics remain readable without relying on color alone.", usageSummary: { tokens: 9500, cost: 0.04 }, schedulingReason: "Can proceed from surface inventory while execution types are being shaped." }),
  task({ id: "runtime-boundary", title: "Preserve runtime boundary", description: "Keep Mission Control fixture data behind the existing runtime snapshot so a future adapter can replace it.", category: "Runtime", status: "completed", wave: 2, dependencies: ["inventory"], workerId: "lead", workerRole: "Lead", provider: "Command Code", model: "Muse Spark 1.3 Contributor", variant: "mid", elapsedMs: 480000, finishedAt: "00:20:00", outputSummary: "No HTTP, SSE, WebSocket, or Rust changes are required for this foundation.", usageSummary: { tokens: 11200, cost: 0.05 }, schedulingReason: "Shared runtime state must remain normalized before projections are wired." }),
  task({ id: "mission-inspector", title: "Keep Inspector compact", description: "Keep current Mission Inspector as quick context and add a discoverable path into deep execution analysis.", category: "Surface", status: "completed", wave: 3, dependencies: ["foundation"], workerId: "build", workerRole: "Build", provider: "Command Code", model: "DeepSeek V4.1 Flash", variant: "flash", elapsedMs: 540000, finishedAt: "00:30:00", outputSummary: "Mission Inspector remains a side surface; graph does not move into it.", usageSummary: { tokens: 14300, cost: 0.06 }, schedulingReason: "The Inspector is an existing entry point for the new workspace." }),
  task({ id: "onboarding", title: "Protect onboarding paths", description: "Verify login, onboarding, and bootstrap gates remain separate from Mission Control navigation.", category: "Surface", status: "completed", wave: 3, dependencies: ["foundation"], workerId: "build", workerRole: "Build", provider: "Command Code", model: "DeepSeek V4.1 Flash", variant: "flash", elapsedMs: 390000, finishedAt: "00:27:00", outputSummary: "Bootstrap fixtures and entry gating remain unchanged in behavior.", usageSummary: { tokens: 10200, cost: 0.04 } }),
  task({ id: "resource-ledger", title: "Connect Ledger context", description: "Show compact budget and token context while leaving detailed accounting to Resource Ledger.", category: "Surface", status: "retrying", wave: 3, dependencies: ["foundation", "design-system"], workerId: "build", workerRole: "Build", provider: "Command Code", model: "Sol", variant: "standard", elapsedMs: 1380000, startedAt: "00:32:00", attempt: 2, maxAttempts: 3, retryCount: 1, progress: 64, outputSummary: "Retrying after a fixture-level provider timeout; context selectors remain bounded.", usageSummary: { tokens: 28700, cost: 0.16 }, attemptHistory: [
    { number: 1, status: "failed", provider: "Command Code", model: "DeepSeek V4.1 Flash", elapsedMs: 420000, reason: "Provider timeout while deriving compact usage context." },
    { number: 2, status: "running", provider: "Command Code", model: "Sol", elapsedMs: 960000, reason: "Escalated to a more reliable model for the retry." },
  ], escalationHistory: [{ provider: "Command Code", model: "DeepSeek V4.1 Flash", reason: "Initial assignment" }, { provider: "Command Code", model: "Sol", reason: "Escalated after provider timeout" }], schedulingReason: "Runs after the shared execution foundation and status language are stable." }),
  task({ id: "responsive", title: "Responsive execution views", description: "Make graph, task list, and selected detail usable at desktop, tablet, and narrow widths.", category: "Surface", status: "running", wave: 3, dependencies: ["mission-inspector", "onboarding"], workerId: "build", workerRole: "Build", provider: "Command Code", model: "DeepSeek V4.1 Flash", variant: "flash", elapsedMs: 780000, startedAt: "00:38:00", progress: 48, usageSummary: { tokens: 19200, cost: 0.11 }, schedulingReason: "Parallel with Ledger context; shares only the compact surface contract.", priority: "high" }),
  task({ id: "integration-gate", title: "Integration check", description: "Confirm the execution projections agree across Overview, Graph, Tasks, Workers, and Activity.", category: "Verification", kind: "gate", status: "verifying", wave: 4, dependencies: ["mission-inspector", "onboarding", "resource-ledger", "runtime-boundary"], workerId: "verify", workerRole: "Verify", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", variant: "standard", elapsedMs: 240000, startedAt: "00:49:00", progress: 72, verificationSummary: "Checking assignment, budget context, and bounded activity invariants.", usageSummary: { tokens: 8100, cost: 0.0 }, schedulingReason: "Assigned to Verify because independent verification is required.", priority: "high" }),
  task({ id: "a11y-sweep", title: "Accessibility sweep", description: "Ensure graph nodes, controls, status labels, and the task-list equivalent expose the same execution facts.", category: "Quality", status: "completed", wave: 4, dependencies: ["design-system"], workerId: "explore", workerRole: "Explore", provider: "OpenCode Go", model: "Space Bunny Free", variant: "standard", elapsedMs: 510000, finishedAt: "00:46:00", outputSummary: "Keyboard and non-spatial task representations are covered.", usageSummary: { tokens: 12100, cost: 0.03 }, schedulingReason: "Independent quality work can complete in parallel with the integration gate." }),
  task({ id: "verification", title: "Verification follow-up", description: "Wait for the integration gate before validating the final execution path.", category: "Verification", kind: "gate", status: "waiting", wave: 5, dependencies: ["integration-gate", "a11y-sweep"], workerId: "verify", workerRole: "Verify", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", variant: "standard", waitingReason: "Waiting for Integration check to pass.", verificationSummary: "Gate is pending; no approval interaction is implemented in this fixture.", schedulingReason: "Scheduled after the integration gate and accessibility sweep converge." }),
  task({ id: "conflict-debug", title: "Resolve overlap warning", description: "Investigate a simulated file-overlap conflict before the final gate can proceed.", category: "Debug", status: "blocked", wave: 5, dependencies: ["resource-ledger"], workerId: "debug", workerRole: "Debug", provider: "OpenCode Go", model: "Space Bunny Free", variant: "deep", blockedReason: "Blocked: files overlap with the active Resource Ledger context task.", outputSummary: "No work started while the conflict group is held.", schedulingReason: "Debug is held until the active conflict group is released.", capabilityRequirement: "filesystem-inspection", priority: "high" }),
  task({ id: "release-gate", title: "Release readiness", description: "Final read-only gate for the dogfood Mission, including all completed work and explicit blockers.", category: "Verification", kind: "gate", status: "queued", wave: 6, dependencies: ["verification", "conflict-debug"], workerId: "lead", workerRole: "Lead", provider: "Command Code", model: "Muse Spark 1.3 Contributor", variant: "mid", schedulingReason: "Will run after verification and the conflict investigation are resolved.", verificationSummary: "Queued behind two wave-five prerequisites." }),
];

const edges: ExecutionEdge[] = [
  edge("recon", "foundation"), edge("inventory", "foundation"), edge("recon", "design-system"), edge("inventory", "runtime-boundary"),
  edge("foundation", "mission-inspector"), edge("foundation", "onboarding"), edge("foundation", "resource-ledger"), edge("design-system", "resource-ledger"),
  edge("mission-inspector", "responsive"), edge("onboarding", "responsive"), edge("mission-inspector", "integration-gate", "gate"), edge("onboarding", "integration-gate", "gate"),
  edge("resource-ledger", "integration-gate", "gate"), edge("runtime-boundary", "integration-gate", "gate"), edge("design-system", "a11y-sweep"),
  edge("integration-gate", "verification", "gate"), edge("a11y-sweep", "verification"), edge("resource-ledger", "conflict-debug", "conflict"), edge("verification", "release-gate", "gate"), edge("conflict-debug", "release-gate", "gate"),
];

for (const source of tasks) source.dependents = [];
for (const item of edges) tasks.find((task) => task.id === item.fromTaskId)?.dependents.push(item.toTaskId);

const workers: WorkerExecution[] = [
  { id: "lead", role: "lead", label: "Lead", status: "active", provider: "Command Code", model: "Muse Spark 1.3 Contributor", variant: "mid", currentTaskId: "integration-gate", completedTaskIds: ["foundation", "design-system", "runtime-boundary"], invocationCount: 7, retryCount: 0, elapsedMs: 2940000, usageSummary: { tokens: 42600, cost: 0.18 } },
  { id: "explore", role: "worker", label: "Explore", status: "completed", provider: "OpenCode Go", model: "Space Bunny Free", variant: "standard", completedTaskIds: ["recon", "a11y-sweep"], invocationCount: 2, retryCount: 0, elapsedMs: 930000, usageSummary: { tokens: 24900, cost: 0.07 } },
  { id: "explore-deep", role: "worker", label: "Explore Deep", status: "completed", provider: "OpenCode Go", model: "Space Bunny Free", variant: "deep", completedTaskIds: ["inventory"], invocationCount: 1, retryCount: 0, elapsedMs: 510000, usageSummary: { tokens: 16400, cost: 0.05 } },
  { id: "build", role: "worker", label: "Build", status: "active", provider: "Command Code", model: "DeepSeek V4.1 Flash", variant: "flash", currentTaskId: "resource-ledger", completedTaskIds: ["mission-inspector", "onboarding"], invocationCount: 5, retryCount: 1, elapsedMs: 2490000, usageSummary: { tokens: 58200, cost: 0.37 } },
  { id: "verify", role: "worker", label: "Verify", status: "active", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", variant: "standard", currentTaskId: "integration-gate", completedTaskIds: [], invocationCount: 2, retryCount: 0, elapsedMs: 240000, usageSummary: { tokens: 8100, cost: 0 } },
  { id: "debug", role: "worker", label: "Debug", status: "blocked", provider: "OpenCode Go", model: "Space Bunny Free", variant: "deep", currentTaskId: "conflict-debug", completedTaskIds: [], invocationCount: 0, retryCount: 0, usageSummary: { tokens: 0, cost: 0 } },
];

const activities: ExecutionActivityItem[] = [
  { id: "activity-1", elapsedMs: 0, timestamp: "00:00", missionId, kind: "mission-started", message: "Mission started", status: "running" },
  { id: "activity-2", elapsedMs: 12000, timestamp: "00:12", missionId, kind: "planner", message: "Planner created 15 Tasks across 6 waves" },
  { id: "activity-3", elapsedMs: 30000, timestamp: "00:30", missionId, kind: "wave-started", message: "Wave 1 started · Recon and State inventory can run in parallel" },
  { id: "activity-4", elapsedMs: 510000, timestamp: "08:30", missionId, taskId: "recon", workerId: "explore", kind: "task-completed", message: "Explore completed Recon current surfaces", status: "completed" },
  { id: "activity-5", elapsedMs: 1260000, timestamp: "21:00", missionId, taskId: "foundation", workerId: "lead", kind: "task-completed", message: "Lead completed Define execution foundation", status: "completed" },
  { id: "activity-6", elapsedMs: 1800000, timestamp: "30:00", missionId, kind: "wave-started", message: "Wave 3 started · Mission Inspector, Onboarding, Ledger, and Responsive branches" },
  { id: "activity-7", elapsedMs: 1920000, timestamp: "32:00", missionId, taskId: "resource-ledger", workerId: "build", kind: "worker-assigned", message: "Build assigned Connect Ledger context", status: "retrying" },
  { id: "activity-8", elapsedMs: 2340000, timestamp: "39:00", missionId, taskId: "resource-ledger", workerId: "build", kind: "retry-scheduled", message: "Retry scheduled · escalated DeepSeek V4.1 Flash → Sol", status: "retrying" },
  { id: "activity-9", elapsedMs: 2820000, timestamp: "47:00", missionId, taskId: "integration-gate", workerId: "verify", kind: "gate", message: "Verify started Integration check", status: "verifying" },
  { id: "activity-10", elapsedMs: 2940000, timestamp: "49:00", missionId, kind: "wave-started", message: "Wave 4 active · independent quality work completed alongside verification" },
  { id: "activity-11", elapsedMs: 3000000, timestamp: "50:00", missionId, taskId: "verification", workerId: "verify", kind: "task-waiting", message: "Verification follow-up waiting for Integration check", status: "waiting" },
  { id: "activity-12", elapsedMs: 3060000, timestamp: "51:00", missionId, taskId: "conflict-debug", workerId: "debug", kind: "task-blocked", message: "Debug blocked · files overlap with active Ledger context", status: "blocked" },
  { id: "activity-13", elapsedMs: 3180000, timestamp: "53:00", missionId, kind: "planner", message: "Scheduled after Build API because it depends on generated schema" },
  { id: "activity-14", elapsedMs: 3300000, timestamp: "55:00", missionId, kind: "gate", message: "Integration gate holding progress · independent verification required", status: "verifying" },
];

export function createMissionControlExecution(): MissionExecution {
  const waves = [
    { index: 1, taskIds: ["recon", "inventory"], status: "completed" as const },
    { index: 2, taskIds: ["foundation", "design-system", "runtime-boundary"], status: "completed" as const },
    { index: 3, taskIds: ["mission-inspector", "onboarding", "resource-ledger", "responsive"], status: "active" as const },
    { index: 4, taskIds: ["integration-gate", "a11y-sweep"], status: "active" as const },
    { index: 5, taskIds: ["verification", "conflict-debug"], status: "planned" as const },
    { index: 6, taskIds: ["release-gate"], status: "planned" as const },
  ];
  return {
    missionId,
    title: "Consolidate OCG frontend architecture",
    status: "running",
    currentWave: 4,
    totalWaves: 6,
    startedAt: "00:00",
    taskIds: tasks.map((item) => item.id),
    edgeIds: edges.map((item) => item.id),
    workerIds: workers.map((item) => item.id),
    tasks: tasks.map((item) => ({ ...item, dependencies: [...item.dependencies], dependents: [...item.dependents] })),
    edges: edges.map((item) => ({ ...item })),
    workers: workers.map((item) => ({ ...item, completedTaskIds: [...item.completedTaskIds] })),
    waves,
    gates: [
      { id: "gate-integration", taskId: "integration-gate", type: "integration", status: "running", reason: "Independent verification is checking projection consistency." },
      { id: "gate-follow-up", taskId: "verification", type: "verification", status: "pending", reason: "Waiting for Integration check." },
      { id: "gate-release", taskId: "release-gate", type: "approval", status: "pending", reason: "Queued until all wave-five work is resolved." },
    ],
    activities,
    summary: taskStatusCounts(tasks),
    budget: { spent: 4.2, limit: 25, tokens: 139300, commitmentPercent: 62 },
    nextTaskIds: ["verification", "conflict-debug"],
    criticalPathTaskIds: ["recon", "foundation", "mission-inspector", "integration-gate", "verification", "release-gate"],
  };
}
