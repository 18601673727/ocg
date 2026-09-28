/**
 * Backend-backed canonical WorkNode/Run projection.
 *
 * The OCG substrate (`ocg.canonical.v1`) is the execution authority. This
 * module is the *projection* boundary: it validates the durable backend
 * contract and maps it onto the existing RuntimeStore presentation shapes.
 *
 * Invariants preserved here:
 * - the API version, project identity and canonical cursor are carried
 *   through unchanged, so a stale generation or a wrong-project payload is
 *   rejected rather than rendered;
 * - a canonical WorkNode, Run generation, frozen contract, dispatch witness
 *   and verification record are never invented or inferred from a label;
 * - the frontend owns no execution authority. It cannot dispatch, complete or
 *   replace a Run, and an active Run's frozen contract is never editable here.
 */

import type {
  ExecutionAttempt,
  ExecutionEdge,
  ExecutionStatus,
  ExecutionTask,
  MissionExecution,
  WorkerExecution,
} from "../execution/domain";
import type { MissionStatus } from "../types";
import type { ProjectId } from "../project/domain";

// The protocol version and the dispatch witness are owned by Rust and projected
// here, so the envelope validator and the control client cannot disagree with
// the definition that actually confers authority.
export { CANONICAL_API_VERSION } from "../contracts";

import { CANONICAL_API_VERSION, decodeWitness } from "../contracts";
import type { DispatchWitness } from "../contracts";

export const CANONICAL_STREAM_ID = "ocg.canonical.work";

/** The durable dispatch witness as OCG persists and returns it. */
export type CanonicalWitness = DispatchWitness;

export type CanonicalRunState =
  | "active"
  | "completed"
  | "failed"
  | "cancelled"
  | "superseded"
  | "fenced";

export type CanonicalWorkState = "ready" | "running" | "completed" | "failed" | "cancelled";

/** The frozen executor/model/role contract of one Run generation. */
export type CanonicalRunContract = {
  executor: string;
  model: string;
  role: string;
};

export type CanonicalWorkNode = {
  node_id: number;
  parent_node_id: number | null;
  spawned_by_run_id: number | null;
  state: CanonicalWorkState;
  generation: number;
  active_run_id: number | null;
  payload: string;
};

export type CanonicalRun = {
  run_id: number;
  node_id: number;
  generation: number;
  state: CanonicalRunState;
  contract: CanonicalRunContract;
  runtime_execution_id: string | null;
  host_session_id: string | null;
  result: string | null;
  witness: CanonicalWitness | null;
};

export type CanonicalDependency = {
  node_id: number;
  depends_on_node_id: number;
};

export type CanonicalEvent = {
  seq: number;
  kind: string;
  payload: string;
  by_run_id: number | null;
};

export type CanonicalVerification = {
  verification_id: string;
  node_id: number;
  run_id: number;
  dispatch_id: string;
  outcome: "passed" | "failed";
  passed: boolean;
  commands: string[];
  created_at: number;
};

export type CanonicalLateResult = {
  node_id: number;
  run_id: number;
  dispatch_id: string;
  result: string;
};

/** The validated backend snapshot. */
export type CanonicalWorkProjection = {
  apiVersion: string;
  projectId: ProjectId;
  missionId: string;
  rootNodeId: number;
  cursor: number;
  workNodes: CanonicalWorkNode[];
  dependencies: CanonicalDependency[];
  runs: CanonicalRun[];
  events: CanonicalEvent[];
  verifications: CanonicalVerification[];
  lateResults: CanonicalLateResult[];
};

export type CanonicalProjectionIssue = {
  code: "api-version" | "malformed" | "project-mismatch" | "cursor";
  message: string;
};

export type CanonicalProjectionResult =
  | { ok: true; projection: CanonicalWorkProjection }
  | { ok: false; issue: CanonicalProjectionIssue };

export type CanonicalSnapshotInput = {
  api_version?: unknown;
  project_id?: unknown;
  cursor?: unknown;
  mission?: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isIndex(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function isText(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function fail(code: CanonicalProjectionIssue["code"], message: string): CanonicalProjectionResult {
  return { ok: false, issue: { code, message } };
}

/**
 * A witness is read against the Rust definition in `components/ocg/contracts`.
 *
 * A witness that does not match that definition yields `null`, so the
 * projection refuses to show authority it cannot verify rather than render a
 * partially populated stand-in.
 */
function parseWitness(value: unknown): CanonicalWitness | null {
  return decodeWitness(value);
}

const RUN_STATES: readonly CanonicalRunState[] = [
  "active",
  "completed",
  "failed",
  "cancelled",
  "superseded",
  "fenced",
];

const WORK_STATES: readonly CanonicalWorkState[] = ["ready", "running", "completed", "failed", "cancelled"];

function parseContract(value: unknown): CanonicalRunContract | null {
  if (!isRecord(value)) return null;
  if (!isText(value.executor) || !isText(value.model) || !isText(value.role)) return null;
  return { executor: value.executor, model: value.model, role: value.role };
}

/**
 * Validate one backend snapshot against the durable canonical contract.
 *
 * A wrong API version, a foreign project identity, a non-monotonic cursor or a
 * malformed entity is rejected here; nothing is partially rendered.
 */
export function projectCanonicalSnapshot(
  input: CanonicalSnapshotInput,
  expectedProjectId: ProjectId,
): CanonicalProjectionResult {
  if (!isRecord(input)) return fail("malformed", "Canonical snapshot must be an object.");
  if (input.api_version !== CANONICAL_API_VERSION) {
    return fail("api-version", `Unsupported canonical API version "${String(input.api_version)}".`);
  }
  if (input.project_id !== expectedProjectId) {
    return fail("project-mismatch", "Canonical snapshot belongs to another Project.");
  }
  if (!isIndex(input.cursor)) return fail("cursor", "Canonical snapshot cursor must be a non-negative integer.");
  const mission = input.mission;
  if (!isRecord(mission)) return fail("malformed", "Canonical snapshot mission payload is required.");
  if (!isText(mission.mission_id)) return fail("malformed", "Canonical Mission id is required.");
  if (!isIndex(mission.root_node_id)) return fail("malformed", "Canonical root node id is required.");

  const rawNodes = mission.work_nodes;
  if (!Array.isArray(rawNodes) || rawNodes.some((node) => !isRecord(node))) {
    return fail("malformed", "Canonical work_nodes must be an array of records.");
  }
  const workNodes: CanonicalWorkNode[] = [];
  for (const node of rawNodes) {
    if (!isIndex(node!.node_id)) return fail("malformed", "WorkNode id must be a non-negative integer.");
    if (node!.parent_node_id !== null && !isIndex(node!.parent_node_id)) {
      return fail("malformed", "WorkNode parent must be null or a node id.");
    }
    if (node!.spawned_by_run_id !== null && !isIndex(node!.spawned_by_run_id)) {
      return fail("malformed", "WorkNode spawn provenance must be null or a run id.");
    }
    if (node!.active_run_id !== null && !isIndex(node!.active_run_id)) {
      return fail("malformed", "WorkNode active run must be null or a run id.");
    }
    if (!Number.isInteger(node!.generation) || (node!.generation as number) < 0) {
      return fail("malformed", "WorkNode generation must be a non-negative integer.");
    }
    if (typeof node!.state !== "string" || !WORK_STATES.includes(node!.state as CanonicalWorkState)) {
      return fail("malformed", `Unknown canonical WorkNode state "${String(node!.state)}".`);
    }
    workNodes.push({
      node_id: node!.node_id as number,
      parent_node_id: (node!.parent_node_id ?? null) as number | null,
      spawned_by_run_id: (node!.spawned_by_run_id ?? null) as number | null,
      state: node!.state as CanonicalWorkState,
      generation: node!.generation as number,
      active_run_id: (node!.active_run_id ?? null) as number | null,
      payload: typeof node!.payload === "string" ? node!.payload : "",
    });
  }

  const rawRuns = mission.runs;
  if (!Array.isArray(rawRuns) || rawRuns.some((run) => !isRecord(run))) {
    return fail("malformed", "Canonical runs must be an array of records.");
  }
  const runs: CanonicalRun[] = [];
  for (const run of rawRuns) {
    if (!isIndex(run!.run_id) || !isIndex(run!.node_id)) return fail("malformed", "Run ids must be non-negative integers.");
    if (!Number.isInteger(run!.generation) || (run!.generation as number) < 1) {
      return fail("malformed", "Run generation must be a positive integer.");
    }
    if (typeof run!.state !== "string" || !RUN_STATES.includes(run!.state as CanonicalRunState)) {
      return fail("malformed", `Unknown canonical Run state "${String(run!.state)}".`);
    }
    const contract = parseContract(run!.contract);
    if (!contract) return fail("malformed", "Every Run must carry a complete frozen contract.");
    runs.push({
      run_id: run!.run_id as number,
      node_id: run!.node_id as number,
      generation: run!.generation as number,
      state: run!.state as CanonicalRunState,
      contract,
      runtime_execution_id: typeof run!.runtime_execution_id === "string" ? run!.runtime_execution_id : null,
      host_session_id: typeof run!.host_session_id === "string" ? run!.host_session_id : null,
      result: typeof run!.result === "string" ? run!.result : null,
      witness: run!.witness === null || run!.witness === undefined ? null : parseWitness(run!.witness),
    });
  }

  const rawDependencies = mission.dependencies ?? [];
  if (!Array.isArray(rawDependencies)) return fail("malformed", "Canonical dependencies must be an array.");
  const dependencies: CanonicalDependency[] = [];
  for (const edge of rawDependencies) {
    if (!isRecord(edge) || !isIndex(edge.node_id) || !isIndex(edge.depends_on_node_id)) {
      return fail("malformed", "Dependency edges must name two node ids.");
    }
    dependencies.push({ node_id: edge.node_id as number, depends_on_node_id: edge.depends_on_node_id as number });
  }

  const rawEvents = mission.events ?? [];
  if (!Array.isArray(rawEvents) || rawEvents.some((event) => !isRecord(event) || !isIndex(event.seq))) {
    return fail("malformed", "Canonical events must carry a sequence.");
  }
  const events: CanonicalEvent[] = rawEvents.map((event) => ({
    seq: (event as { seq: number }).seq,
    kind: isText((event as { kind?: unknown }).kind) ? ((event as { kind: string }).kind) : "unknown",
    payload: typeof (event as { payload?: unknown }).payload === "string" ? ((event as { payload: string }).payload) : "",
    by_run_id: isIndex((event as { by_run_id?: unknown }).by_run_id) ? ((event as { by_run_id: number }).by_run_id) : null,
  }));
  const headSequence = events.reduce((highest, event) => Math.max(highest, event.seq), 0);
  if (input.cursor < headSequence) {
    return fail("cursor", "Canonical cursor is behind its own event stream.");
  }

  const verifications = Array.isArray(mission.verifications) ? (mission.verifications as unknown[]).map((entry) => {
    const record = entry as Record<string, unknown>;
    return {
      verification_id: isText(record.verification_id) ? record.verification_id : "unknown",
      node_id: isIndex(record.node_id) ? record.node_id : 0,
      run_id: isIndex(record.run_id) ? record.run_id : 0,
      dispatch_id: isText(record.dispatch_id) ? record.dispatch_id : "",
      outcome: record.outcome === "passed" ? ("passed" as const) : ("failed" as const),
      passed: record.passed === true,
      commands: Array.isArray(record.commands) ? record.commands.filter(isText) : [],
      created_at: typeof record.created_at === "number" ? record.created_at : 0,
    };
  }) : [];

  const lateResults = Array.isArray(mission.late_results) ? (mission.late_results as unknown[]).map((entry) => {
    const record = entry as Record<string, unknown>;
    return {
      node_id: isIndex(record.node_id) ? record.node_id : 0,
      run_id: isIndex(record.run_id) ? record.run_id : 0,
      dispatch_id: isText(record.dispatch_id) ? record.dispatch_id : "",
      result: typeof record.result === "string" ? record.result : "",
    };
  }) : [];

  return {
    ok: true,
    projection: {
      apiVersion: CANONICAL_API_VERSION,
      projectId: expectedProjectId,
      missionId: mission.mission_id,
      rootNodeId: mission.root_node_id as number,
      cursor: input.cursor,
      workNodes,
      dependencies,
      runs,
      events,
      verifications,
      lateResults,
    },
  };
}

/* -------------------------------------------------------------------------- */
/* Presentation projection                                                    */
/* -------------------------------------------------------------------------- */

const NODE_STATUS: Record<CanonicalWorkState, ExecutionStatus> = {
  ready: "ready",
  running: "running",
  completed: "completed",
  failed: "failed",
  cancelled: "cancelled",
};

const WORKER_STATUS: Record<CanonicalRunState, WorkerExecution["status"]> = {
  active: "active",
  completed: "completed",
  failed: "blocked",
  cancelled: "idle",
  superseded: "idle",
  fenced: "idle",
};

const NODE_ID_PREFIX = "wn";

/** A stable, project-scoped task id for one canonical WorkNode. */
export function canonicalTaskId(projectId: ProjectId, missionId: string, nodeId: number): string {
  return `${projectId}:${missionId}:${NODE_ID_PREFIX}${nodeId}`;
}

function attemptsForRuns(runs: CanonicalRun[]): ExecutionAttempt[] {
  return runs.map((run) => ({
    number: run.generation,
    status: run.state === "completed" ? "completed" : run.state === "active" ? "running" : "failed",
    model: run.contract.model,
    provider: run.contract.model.includes("/") ? run.contract.model.split("/")[0]! : undefined,
    reason: run.state === "fenced" ? "superseded by a replacement Run generation" : undefined,
  }));
}

/**
 * Project canonical WorkNode/Run state onto the existing Execution Graph and
 * Mission Control contract. The result is a read-only view: it never becomes
 * execution authority, and it never rewrites a frozen Run contract.
 */
export function toMissionExecution(
  projection: CanonicalWorkProjection,
  title: string,
): MissionExecution {
  const byNode = new Map<number, CanonicalRun[]>();
  for (const run of projection.runs) {
    const list = byNode.get(run.node_id) ?? [];
    list.push(run);
    byNode.set(run.node_id, list);
  }
  const dependents = new Map<number, number[]>();
  for (const edge of projection.dependencies) {
    const list = dependents.get(edge.depends_on_node_id) ?? [];
    list.push(edge.node_id);
    dependents.set(edge.depends_on_node_id, list);
  }

  const tasks: ExecutionTask[] = projection.workNodes.map((node) => {
    const runs = (byNode.get(node.node_id) ?? []).slice().sort((a, b) => a.generation - b.generation);
    const active = node.active_run_id === null
      ? null
      : runs.find((run) => run.run_id === node.active_run_id) ?? null;
    const attempts = attemptsForRuns(runs);
    return {
      id: canonicalTaskId(projection.projectId, projection.missionId, node.node_id),
      missionId: projection.missionId,
      title: node.payload || `WorkNode ${node.node_id}`,
      description: node.parent_node_id === null ? "Mission root WorkNode" : `Child of WorkNode ${node.parent_node_id}`,
      kind: "task",
      category: `node-${node.node_id}`,
      status: NODE_STATUS[node.state],
      dependencies: projection.dependencies
        .filter((edge) => edge.node_id === node.node_id)
        .map((edge) => canonicalTaskId(projection.projectId, projection.missionId, edge.depends_on_node_id)),
      dependents: (dependents.get(node.node_id) ?? []).map(
        (dependent) => canonicalTaskId(projection.projectId, projection.missionId, dependent),
      ),
      workerId: active === null ? undefined : canonicalTaskId(projection.projectId, projection.missionId, active.run_id),
      workerRole: active?.contract.role,
      provider: active?.contract.model.includes("/") ? active.contract.model.split("/")[0]! : active?.contract.model,
      model: active?.contract.model,
      attempt: active?.generation,
      maxAttempts: attempts.length,
      retryCount: Math.max(0, runs.length - 1),
    };
  });

  const edges: ExecutionEdge[] = projection.dependencies.map((edge) => ({
    id: `${projection.missionId}:${edge.depends_on_node_id}->${edge.node_id}`,
    fromTaskId: canonicalTaskId(projection.projectId, projection.missionId, edge.depends_on_node_id),
    toTaskId: canonicalTaskId(projection.projectId, projection.missionId, edge.node_id),
    kind: "dependency",
    status: "pending",
  }));

  const workers: WorkerExecution[] = projection.runs
    .filter((run) => run.witness !== null)
    .map((run) => ({
      id: canonicalTaskId(projection.projectId, projection.missionId, run.run_id),
      role: run.contract.role === "lead" ? "lead" : "worker",
      label: `${run.contract.executor} · ${run.contract.model}`,
      status: WORKER_STATUS[run.state],
      provider: run.contract.model.includes("/") ? run.contract.model.split("/")[0]! : run.contract.model,
      model: run.contract.model,
      currentTaskId: run.state === "active"
        ? canonicalTaskId(projection.projectId, projection.missionId, run.node_id)
        : undefined,
      completedTaskIds: run.state === "completed"
        ? [canonicalTaskId(projection.projectId, projection.missionId, run.node_id)]
        : [],
      retryCount: Math.max(0, run.generation - 1),
    }));

  const summary = tasks.reduce(
    (counts, task) => {
      if (task.status === "completed") counts.completed += 1;
      if (task.status === "running") counts.running += 1;
      if (task.status === "ready" || task.status === "queued") counts.waiting += 1;
      if (task.status === "blocked" || task.status === "waiting") counts.blocked += 1;
      if (task.status === "failed") counts.failed += 1;
      if (task.status === "retrying") counts.retrying += 1;
      counts.total += 1;
      return counts;
    },
    { completed: 0, running: 0, waiting: 0, blocked: 0, failed: 0, retrying: 0, total: 0 },
  );

  const rootState = projection.workNodes.find((node) => node.node_id === projection.rootNodeId)?.state ?? "ready";
  const status: MissionStatus | "waiting" | "blocked" | "cancelled" =
    rootState === "completed"
      ? "completed"
      : rootState === "cancelled"
        ? "cancelled"
        : rootState === "failed"
          ? "failed"
          : summary.running > 0
            ? "running"
            : "waiting";

  return {
    missionId: projection.missionId,
    title,
    status,
    taskIds: tasks.map((task) => task.id),
    edgeIds: edges.map((edge) => edge.id),
    workerIds: workers.map((worker) => worker.id),
    tasks,
    edges,
    workers,
    waves: [{ index: 0, taskIds: tasks.map((task) => task.id), status: status === "completed" ? "completed" : "active" }],
    gates: [],
    activities: projection.events.slice(-20).map((event) => ({
      id: `${projection.missionId}:${event.seq}`,
      elapsedMs: event.seq,
      timestamp: new Date(event.seq * 1000).toISOString(),
      missionId: projection.missionId,
      kind: "mission-transition" as const,
      message: event.kind,
    })),
    summary,
    nextTaskIds: tasks.filter((task) => task.status === "ready").map((task) => task.id),
  };
}

/**
 * Whether the backend is currently allowed to accept pre-run Mission
 * configuration edits. Once any Run is dispatched the contract is frozen and
 * the PWA must present it as read-only rather than pretending to rewrite it.
 */
export function isPreRunConfigurationMutable(projection: CanonicalWorkProjection): boolean {
  return projection.runs.length === 0;
}
