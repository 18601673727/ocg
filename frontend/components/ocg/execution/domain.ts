import type { MissionStatus } from "../types";

export type ExecutionStatus =
  | "planned"
  | "queued"
  | "waiting"
  | "ready"
  | "starting"
  | "running"
  | "blocked"
  | "verifying"
  | "retrying"
  | "completed"
  | "failed"
  | "cancelled"
  | "skipped";

export type ExecutionTaskKind = "task" | "gate";
export type ExecutionEdgeKind = "dependency" | "gate" | "conflict";
export type ExecutionGateType = "verification" | "review" | "approval" | "integration";
export type ExecutionGateStatus = "pending" | "running" | "passed" | "failed" | "blocked";

export type ExecutionUsageSummary = {
  tokens?: number;
  cost?: number;
};

export type ExecutionAttempt = {
  number: number;
  status: "failed" | "running" | "completed";
  model?: string;
  provider?: string;
  elapsedMs?: number;
  reason?: string;
};

export type ExecutionTask = {
  id: string;
  missionId: string;
  title: string;
  description?: string;
  kind?: ExecutionTaskKind;
  category?: string;
  status: ExecutionStatus;
  wave?: number;
  dependencies: string[];
  dependents: string[];
  workerId?: string;
  workerRole?: string;
  provider?: string;
  model?: string;
  variant?: string;
  startedAt?: string;
  finishedAt?: string;
  elapsedMs?: number;
  attempt?: number;
  maxAttempts?: number;
  retryCount?: number;
  progress?: number;
  blockedReason?: string;
  waitingReason?: string;
  outputSummary?: string;
  verificationSummary?: string;
  usageSummary?: ExecutionUsageSummary;
  attemptHistory?: ExecutionAttempt[];
  escalationHistory?: { provider: string; model: string; reason: string }[];
  schedulingReason?: string;
  capabilityRequirement?: string;
  priority?: "low" | "normal" | "high";
};

export type ExecutionEdge = {
  id: string;
  fromTaskId: string;
  toTaskId: string;
  kind: ExecutionEdgeKind;
  status?: "pending" | "active" | "completed" | "blocked";
};

export type WorkerExecution = {
  id: string;
  role: "lead" | "worker";
  label: string;
  status: "queued" | "active" | "waiting" | "idle" | "completed" | "blocked";
  provider?: string;
  model?: string;
  variant?: string;
  currentTaskId?: string;
  completedTaskIds: string[];
  invocationCount?: number;
  retryCount?: number;
  elapsedMs?: number;
  usageSummary?: ExecutionUsageSummary;
};

export type ExecutionWave = {
  index: number;
  taskIds: string[];
  status: "planned" | "active" | "completed";
};

export type ExecutionGate = {
  id: string;
  taskId?: string;
  type: ExecutionGateType;
  status: ExecutionGateStatus;
  reason?: string;
};

export type ExecutionActivityKind =
  | "mission-started"
  | "planner"
  | "wave-started"
  | "task-started"
  | "task-completed"
  | "task-blocked"
  | "task-waiting"
  | "retry-scheduled"
  | "gate"
  | "worker-assigned"
  | "mission-transition";

export type ExecutionActivityItem = {
  id: string;
  elapsedMs: number;
  timestamp: string;
  missionId: string;
  taskId?: string;
  workerId?: string;
  kind: ExecutionActivityKind;
  message: string;
  status?: ExecutionStatus;
};

export type ExecutionSummary = {
  completed: number;
  total: number;
  running: number;
  waiting: number;
  blocked: number;
  failed: number;
  retrying: number;
};

export type MissionExecution = {
  missionId: string;
  title: string;
  status: MissionStatus | "waiting" | "blocked" | "cancelled";
  currentWave?: number;
  totalWaves?: number;
  startedAt?: string;
  finishedAt?: string;
  taskIds: string[];
  edgeIds: string[];
  workerIds: string[];
  tasks: ExecutionTask[];
  edges: ExecutionEdge[];
  workers: WorkerExecution[];
  waves: ExecutionWave[];
  gates: ExecutionGate[];
  activities: ExecutionActivityItem[];
  summary: ExecutionSummary;
  budget?: { spent: number; limit: number; tokens: number; commitmentPercent: number };
  nextTaskIds?: string[];
  criticalPathTaskIds?: string[];
};

export type GraphNode = {
  id: string;
  task: ExecutionTask;
  x: number;
  y: number;
  width: number;
  height: number;
};

export type GraphEdge = ExecutionEdge & {
  source: GraphNode;
  target: GraphNode;
};

export type ExecutionGraph = {
  nodes: GraphNode[];
  edges: GraphEdge[];
  width: number;
  height: number;
};

export type ExecutionTaskFilters = {
  query?: string;
  status?: ExecutionStatus;
  wave?: number;
  workerRole?: string;
  provider?: string;
  model?: string;
};

export function formatExecutionStatus(status: ExecutionStatus | MissionExecution["status"]): string {
  return status.replaceAll("-", " ");
}

export function taskStatusCounts(tasks: ExecutionTask[]): ExecutionSummary {
  return {
    completed: tasks.filter((task) => task.status === "completed").length,
    total: tasks.length,
    running: tasks.filter((task) => ["starting", "running", "verifying"].includes(task.status)).length,
    waiting: tasks.filter((task) => ["waiting", "queued", "planned", "ready"].includes(task.status)).length,
    blocked: tasks.filter((task) => task.status === "blocked").length,
    failed: tasks.filter((task) => ["failed", "cancelled", "skipped"].includes(task.status)).length,
    retrying: tasks.filter((task) => task.status === "retrying" || (task.retryCount ?? 0) > 0).length,
  };
}

export function filterExecutionTasks(tasks: ExecutionTask[], filters: ExecutionTaskFilters = {}): ExecutionTask[] {
  const query = filters.query?.trim().toLowerCase();
  return tasks.filter((task) => (
    (!query || task.title.toLowerCase().includes(query)) &&
    (filters.status === undefined || task.status === filters.status) &&
    (filters.wave === undefined || task.wave === filters.wave) &&
    (filters.workerRole === undefined || task.workerRole === filters.workerRole) &&
    (filters.provider === undefined || task.provider === filters.provider) &&
    (filters.model === undefined || task.model === filters.model)
  ));
}

export function executionWaves(execution: MissionExecution): ExecutionWave[] {
  if (execution.waves.length > 0) return execution.waves;
  const groups = new Map<number, string[]>();
  for (const task of execution.tasks) {
    if (task.wave === undefined) continue;
    groups.set(task.wave, [...(groups.get(task.wave) ?? []), task.id]);
  }
  return [...groups.entries()].sort(([left], [right]) => left - right).map(([index, taskIds]) => ({
    index,
    taskIds,
    status: "planned",
  }));
}

export function currentWave(execution: MissionExecution): number | undefined {
  if (execution.currentWave !== undefined) return execution.currentWave;
  return executionWaves(execution).find((wave) => wave.status === "active")?.index;
}

export function parallelTasks(execution: MissionExecution, waveIndex?: number): ExecutionTask[] {
  const index = waveIndex ?? currentWave(execution);
  if (index === undefined) return [];
  return execution.tasks.filter((task) => task.wave === index && ["starting", "running", "verifying", "retrying"].includes(task.status));
}

export function dependencyClosure(execution: MissionExecution, taskId: string): Set<string> {
  const byId = new Map(execution.tasks.map((task) => [task.id, task]));
  const found = new Set<string>();
  const visit = (id: string) => {
    if (found.has(id)) return;
    found.add(id);
    for (const dependency of byId.get(id)?.dependencies ?? []) visit(dependency);
  };
  visit(taskId);
  return found;
}

export function dependentClosure(execution: MissionExecution, taskId: string): Set<string> {
  const found = new Set<string>();
  const visit = (id: string) => {
    if (found.has(id)) return;
    found.add(id);
    for (const dependent of execution.tasks.find((task) => task.id === id)?.dependents ?? []) visit(dependent);
  };
  visit(taskId);
  return found;
}

export function deriveExecutionGraph(execution: MissionExecution): ExecutionGraph {
  const byWave = new Map<number, ExecutionTask[]>();
  for (const task of execution.tasks) {
    const wave = task.wave ?? 1;
    byWave.set(wave, [...(byWave.get(wave) ?? []), task]);
  }
  const waves = [...byWave.keys()].sort((left, right) => left - right);
  const width = 188;
  const gapX = 60;
  const height = 92;
  const gapY = 26;
  const nodes = waves.flatMap((wave) => (byWave.get(wave) ?? []).map((task, index) => ({
    id: task.id,
    task,
    x: 28 + (wave - 1) * (width + gapX),
    y: 56 + index * (height + gapY),
    width,
    height,
  })));
  const nodesById = new Map(nodes.map((node) => [node.id, node]));
  const edges = execution.edges.flatMap((edge) => {
    const source = nodesById.get(edge.fromTaskId);
    const target = nodesById.get(edge.toTaskId);
    return source && target ? [{ ...edge, source, target }] : [];
  });
  const maxRows = Math.max(1, ...waves.map((wave) => byWave.get(wave)?.length ?? 0));
  return {
    nodes,
    edges,
    width: Math.max(680, 28 + waves.length * (width + gapX)),
    height: Math.max(310, 56 + maxRows * (height + gapY)),
  };
}

export function hasDependencyCycle(execution: MissionExecution): boolean {
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const byId = new Map(execution.tasks.map((task) => [task.id, task]));
  const visit = (id: string): boolean => {
    if (visiting.has(id)) return true;
    if (visited.has(id)) return false;
    visiting.add(id);
    for (const dependency of byId.get(id)?.dependencies ?? []) {
      if (visit(dependency)) return true;
    }
    visiting.delete(id);
    visited.add(id);
    return false;
  };
  return execution.tasks.some((task) => visit(task.id));
}
