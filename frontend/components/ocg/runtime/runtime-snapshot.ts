/**
 * Authoritative runtime snapshot envelope.
 *
 * A snapshot is installed as one coherent baseline together with the cursor it
 * was produced at. It is never decomposed into synthetic events: the reconciler
 * treats it as the new truth and resumes from its cursor.
 */

import type { BootstrapState } from "../bootstrap/types";
import type { ProjectId } from "../project/domain";
import type { RuntimeSnapshot } from "./runtime-types";
import type {
  RuntimeCursor,
  RuntimeGeneration,
  RuntimeProtocolVersion,
  RuntimeResumeCursor,
  RuntimeStreamId,
} from "./runtime-envelope";
export type RuntimeSnapshotScope =
  | { kind: "all-projects" }
  | { kind: "project"; projectId: ProjectId };

export type RuntimeSnapshotEnvelope = {
  protocolVersion: RuntimeProtocolVersion;
  streamId: RuntimeStreamId;
  generation: RuntimeGeneration;
  cursor: RuntimeCursor;
  scope: RuntimeSnapshotScope;
  snapshot: RuntimeSnapshot;
  snapshotRevision?: string;
  resumeCursor?: RuntimeResumeCursor;
  occurredAt?: string;
};

export function snapshotScopeKey(scope: RuntimeSnapshotScope): string {
  return scope.kind === "all-projects" ? "all" : `project:${scope.projectId}`;
}

export function sameSnapshotScope(a: RuntimeSnapshotScope, b: RuntimeSnapshotScope): boolean {
  return snapshotScopeKey(a) === snapshotScopeKey(b);
}

/** An explicitly empty baseline. It never pretends to hold fixture data. */
export function emptyRuntimeSnapshot(): RuntimeSnapshot {
  const bootstrap: BootstrapState = {
    access: { state: "local", remote: false, handoffState: "unavailable" },
    onboarding: null,
    resources: [],
    connections: [],
    models: [],
    capabilities: [],
    profiles: [],
    providers: [],
    ready: false,
  };
  return {
    status: { state: "connecting", detail: "Runtime snapshot is not loaded." },
    sessions: [],
    messagesBySession: {},
    observabilityBySession: {},
    executionsByProject: {},
    executionBySession: {},
    accountingBySession: {},
    resourceLedger: null,
    attentionItems: [],
    logs: [],
    bootstrap,
  };
}
