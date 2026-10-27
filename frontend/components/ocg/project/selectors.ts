/**
 * Pure Project selectors.
 *
 * Every selector resolves an unknown/missing project ID to an empty selection before
 * filtering, so callers can pass raw URL or storage values safely. Filtering
 * is projection-only: inputs are never mutated.
 */

import type { RuntimeSnapshot } from "../runtime/runtime-types";
import type { AttentionItem } from "../attention/domain";
import type { ProjectId } from "./domain";
import { resolveProjectId } from "./domain";

/**
 * Project-scoped RuntimeSnapshot projection.
 *
 * Project-owned Job projections are selected by Project ID. Sessions and
 * their maps are filtered separately for conversational presentation. The
 * resource ledger is filtered by the Project's Job IDs. Everything else
 * (scenario, status, bootstrap) is shared and passes through unchanged.
 */
export function selectProjectSnapshot(
  snapshot: RuntimeSnapshot,
  projectId: ProjectId,
): RuntimeSnapshot {
  const id = resolveProjectId(projectId);
  const allowed = new Set(
    snapshot.sessions.filter((session) => session.projectId === id).map((session) => session.id),
  );
  const keepBySession = <T>(record: Record<string, T>): Record<string, T> =>
    Object.fromEntries(Object.entries(record).filter(([key]) => allowed.has(key)));

  const ledgerEntries =
    snapshot.resourceLedger?.entries.filter(
      (entry) => entry.sessionId !== null && allowed.has(entry.sessionId),
    ) ?? [];
  const resourceLedger =
    snapshot.resourceLedger && ledgerEntries.length > 0
      ? { ...snapshot.resourceLedger, entries: ledgerEntries }
      : null;
  const attentionItems = snapshot.attentionItems?.filter((item) => item.projectId === id) ?? [];
  const logs =
    snapshot.logs?.filter(
      (entry) => entry.sessionId !== undefined && allowed.has(entry.sessionId),
    ) ?? [];
  const projectExecutions = snapshot.executionsByProject?.[id] ?? {};

  return {
    ...snapshot,
    sessions: snapshot.sessions.filter((session) => allowed.has(session.id)),
    messagesBySession: keepBySession(snapshot.messagesBySession),
    ...(snapshot.chatQueues ? { chatQueues: keepBySession(snapshot.chatQueues) } : {}),
    observabilityBySession: keepBySession(snapshot.observabilityBySession),
    ...(snapshot.executionsByProject !== undefined
      ? { executionsByProject: id ? { [id]: projectExecutions } : {} }
      : {}),
    executionBySession: keepBySession(snapshot.executionBySession),
    accountingBySession: keepBySession(snapshot.accountingBySession),
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
