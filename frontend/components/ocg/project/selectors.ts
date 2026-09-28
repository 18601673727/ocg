/**
 * Pure Project selectors.
 *
 * Every selector resolves an unknown/missing project ID to Zhuju before
 * filtering, so callers can pass raw URL or storage values safely. Filtering
 * is projection-only: inputs are never mutated.
 */

import type { RuntimeSnapshot } from "../runtime/runtime-types";
import type { ChatSession } from "../types";
import type { AttentionItem } from "../attention/domain";
import type { AttentionQueue } from "../attention/selectors";
import type { ProjectId, ProjectSummary } from "./domain";
import { resolveProjectId, selectProject } from "./domain";
import { projectLedgerMissionIds, projectSessionIds } from "./fixtures";

/** Active project lookup with the deterministic Zhuju fallback. */
export function selectActiveProject(projectId: unknown): ProjectSummary {
  return selectProject(projectId);
}

/** Fixture session IDs for a project plus any registered extra session IDs. */
export function selectProjectSessionIds(
  projectId: ProjectId,
  extraSessionIds: readonly string[] = [],
): string[] {
  const id = resolveProjectId(projectId);
  return [...new Set([...projectSessionIds(id), ...extraSessionIds])];
}

/** Sessions that belong to the project (fixture-owned plus registered). */
export function selectProjectSessions(
  sessions: readonly ChatSession[],
  projectId: ProjectId,
  extraSessionIds: readonly string[] = [],
): ChatSession[] {
  const allowed = new Set(selectProjectSessionIds(projectId, extraSessionIds));
  return sessions.filter((session) => allowed.has(session.id));
}

/** Keep a selected session valid when a project projection changes. */
export function resolveSelectedSessionId(
  selectedSessionId: string | null | undefined,
  sessions: readonly ChatSession[],
): string | null {
  if (sessions.some((session) => session.id === selectedSessionId)) return selectedSessionId ?? null;
  return sessions[0]?.id ?? null;
}

/**
 * Project-scoped RuntimeSnapshot projection.
 *
 * Sessions and their per-session maps are filtered to the project's session
 * IDs (plus any registered extras). The resource ledger is filtered by the
 * project's Mission IDs. Everything else (scenario, status, bootstrap) is
 * shared and passes through unchanged.
 */
export function selectProjectSnapshot(
  snapshot: RuntimeSnapshot,
  projectId: ProjectId,
  extraSessionIds: readonly string[] = [],
): RuntimeSnapshot {
  const id = resolveProjectId(projectId);
  const allowed = new Set(selectProjectSessionIds(id, extraSessionIds));
  const keepBySession = <T>(record: Record<string, T>): Record<string, T> =>
    Object.fromEntries(Object.entries(record).filter(([key]) => allowed.has(key)));

  const missionIds = new Set(projectLedgerMissionIds(id));
  const ledgerEntries = snapshot.resourceLedger?.entries.filter((entry) =>
    missionIds.has(entry.missionId) || (entry.sessionId !== null && allowed.has(entry.sessionId)),
  ) ?? [];
  const resourceLedger = snapshot.resourceLedger && ledgerEntries.length > 0
    ? { ...snapshot.resourceLedger, entries: ledgerEntries }
    : null;
  const attentionItems = snapshot.attentionItems?.filter((item) => item.projectId === id) ?? [];
  const logs = snapshot.logs?.filter((entry) => entry.sessionId !== undefined && allowed.has(entry.sessionId)) ?? [];

  return {
    ...snapshot,
    sessions: snapshot.sessions.filter((session) => allowed.has(session.id)),
    messagesBySession: keepBySession(snapshot.messagesBySession),
    missionsBySession: keepBySession(snapshot.missionsBySession),
    observabilityBySession: keepBySession(snapshot.observabilityBySession),
    executionBySession: keepBySession(snapshot.executionBySession),
    resourceLedger,
    ...(snapshot.attentionItems !== undefined ? { attentionItems } : {}),
    ...(snapshot.logs !== undefined ? { logs } : {}),
  };
}

/**
 * Keep only attention items explicitly tagged with the given project.
 * Untagged items are snapshot-derived and are scoped by the snapshot itself,
 * so they are intentionally not part of a project fixture queue.
 */
export function selectProjectAttentionItems(
  items: readonly AttentionItem[],
  projectId: ProjectId,
): AttentionItem[] {
  const id = resolveProjectId(projectId);
  return items.filter((item) => item.projectId === id);
}

/** Project-scoped fixture queue. Never leaks another project's tagged items. */
export function selectProjectAttentionQueue(
  queue: AttentionQueue,
  projectId: ProjectId,
): AttentionQueue {
  return {
    approvals: selectProjectAttentionItems(queue.approvals, projectId),
    history: selectProjectAttentionItems(queue.history, projectId),
  };
}
