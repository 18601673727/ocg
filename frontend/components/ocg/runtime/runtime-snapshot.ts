/**
 * Authoritative runtime snapshot envelope.
 *
 * A snapshot is installed as one coherent baseline together with the cursor it
 * was produced at. It is never decomposed into synthetic events: the reconciler
 * treats it as the new truth and resumes from its cursor.
 */

import type { BootstrapState } from "../bootstrap/types";
import { createBootstrapFixture } from "../bootstrap/fixtures";
import { isProjectId, type ProjectId } from "../project/domain";
import { selectProjectSnapshot } from "../project/selectors";
import type { RuntimeSnapshot, ScenarioId } from "./runtime-types";
import type { ScenarioFixture } from "./scenarios";
import type {
  RuntimeCursor,
  RuntimeGeneration,
  RuntimeProtocolVersion,
  RuntimeResumeCursor,
  RuntimeStreamId,
} from "./runtime-envelope";
import { RUNTIME_PROTOCOL_VERSION } from "./runtime-envelope";
import type { RuntimeDiagnostic } from "./runtime-envelope";

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

export type RuntimeSnapshotValidation =
  | { ok: true; envelope: RuntimeSnapshotEnvelope }
  | { ok: false; diagnostic: RuntimeDiagnostic };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function invalidSnapshot(message: string): RuntimeSnapshotValidation {
  return {
    ok: false,
    diagnostic: { code: "schema-invalid", severity: "error", message },
  };
}

/** Validate the untrusted snapshot boundary without pretending to validate every domain field. */
export function validateRuntimeSnapshotEnvelope(input: unknown): RuntimeSnapshotValidation {
  if (!isRecord(input)) return invalidSnapshot("Runtime snapshot envelope must be an object.");
  if (input.protocolVersion !== RUNTIME_PROTOCOL_VERSION) {
    return {
      ok: false,
      diagnostic: {
        code: "protocol-incompatible",
        severity: "error",
        message: `Unsupported runtime protocol version "${String(input.protocolVersion)}".`,
      },
    };
  }
  if (!isNonEmptyString(input.streamId)) return invalidSnapshot("Snapshot streamId is required.");
  if (!isNonNegativeInteger(input.generation)) return invalidSnapshot("Snapshot generation must be a non-negative integer.");
  if (!isRecord(input.cursor) || !isNonEmptyString(input.cursor.streamId) || !isNonNegativeInteger(input.cursor.sequence)) {
    return invalidSnapshot("Snapshot cursor must contain a streamId and non-negative sequence.");
  }
  if (input.cursor.streamId !== input.streamId) return invalidSnapshot("Snapshot cursor streamId must match streamId.");
  if (!isRecord(input.scope) || typeof input.scope.kind !== "string") return invalidSnapshot("Snapshot scope is required.");
  if (input.scope.kind === "project" && !isProjectId(input.scope.projectId)) {
    return invalidSnapshot("Project-scoped snapshot must name a known Project.");
  }
  if (input.scope.kind !== "project" && input.scope.kind !== "all-projects") {
    return invalidSnapshot(`Unknown snapshot scope "${String(input.scope.kind)}".`);
  }
  if (!isRecord(input.snapshot)) return invalidSnapshot("Snapshot payload is required.");
  if (!isNonEmptyString(input.snapshot.scenario)) return invalidSnapshot("Snapshot scenario is required.");
  if (!Array.isArray(input.snapshot.sessions)) return invalidSnapshot("Snapshot sessions must be an array.");
  if (!isRecord(input.snapshot.messagesBySession)) return invalidSnapshot("Snapshot messagesBySession must be an object.");
  if (!isRecord(input.snapshot.missionsBySession)) return invalidSnapshot("Snapshot missionsBySession must be an object.");
  if (!isRecord(input.snapshot.observabilityBySession)) return invalidSnapshot("Snapshot observabilityBySession must be an object.");
  if (!isRecord(input.snapshot.executionBySession)) return invalidSnapshot("Snapshot executionBySession must be an object.");
  if (input.snapshot.resourceLedger !== null && (!isRecord(input.snapshot.resourceLedger) || !Array.isArray(input.snapshot.resourceLedger.entries) || input.snapshot.resourceLedger.entries.some((entry) => !isRecord(entry) || !isNonEmptyString(entry.id)))) {
    return invalidSnapshot("Snapshot resourceLedger must be null or contain an entries array.");
  }
  if (input.snapshot.attentionItems !== undefined && (!Array.isArray(input.snapshot.attentionItems) || input.snapshot.attentionItems.some((item) => !isRecord(item) || !isNonEmptyString(item.id)))) {
    return invalidSnapshot("Snapshot attentionItems must contain stable item IDs.");
  }
  if (input.snapshot.logs !== undefined && (!Array.isArray(input.snapshot.logs) || input.snapshot.logs.some((entry) => !isRecord(entry) || !isNonEmptyString(entry.id) || !isNonEmptyString(entry.timestamp)))) {
    return invalidSnapshot("Snapshot logs must contain stable IDs and timestamps.");
  }
  if (!isRecord(input.snapshot.bootstrap)) return invalidSnapshot("Snapshot bootstrap is required.");
  return { ok: true, envelope: input as unknown as RuntimeSnapshotEnvelope };
}

/** Stable, comparable key for a snapshot scope. */
export function snapshotScopeKey(scope: RuntimeSnapshotScope): string {
  return scope.kind === "all-projects" ? "all" : `project:${scope.projectId}`;
}

export function sameSnapshotScope(a: RuntimeSnapshotScope, b: RuntimeSnapshotScope): boolean {
  return snapshotScopeKey(a) === snapshotScopeKey(b);
}

/** An explicitly empty baseline. It never pretends to hold fixture data. */
export function emptyRuntimeSnapshot(scenario: ScenarioId): RuntimeSnapshot {
  const bootstrap: BootstrapState = createBootstrapFixture(scenario);
  return {
    scenario,
    status: { state: "connecting", detail: "Runtime snapshot is not loaded." },
    sessions: [],
    messagesBySession: {},
    missionsBySession: {},
    observabilityBySession: {},
    executionBySession: {},
    resourceLedger: null,
    attentionItems: [],
    logs: [],
    bootstrap,
  };
}

/** Deep clone a fixture into the canonical snapshot shape. */
export function createRuntimeSnapshotFromFixture(fixture: ScenarioFixture): RuntimeSnapshot {
  return {
    scenario: fixture.id,
    status: JSON.parse(JSON.stringify(fixture.runtimeStatus)),
    sessions: JSON.parse(JSON.stringify(fixture.sessions)),
    messagesBySession: JSON.parse(JSON.stringify(fixture.messagesBySession)),
    missionsBySession: JSON.parse(JSON.stringify(fixture.missionsBySession)),
    observabilityBySession: JSON.parse(JSON.stringify(fixture.observabilityBySession)),
    executionBySession: JSON.parse(JSON.stringify(fixture.executionBySession)),
    resourceLedger: JSON.parse(JSON.stringify(fixture.resourceLedger)),
    attentionItems: [],
    logs: [],
    bootstrap: JSON.parse(JSON.stringify(fixture.bootstrap)),
  };
}

export type FixtureSeedOptions = {
  streamId?: RuntimeStreamId;
  generation?: RuntimeGeneration;
  sequence?: number;
  /** When set, produce a project-scoped snapshot projection. */
  projectId?: ProjectId | null;
  /** Extra session IDs registered to the project (for newly created sessions). */
  extraSessionIds?: readonly string[];
  snapshotRevision?: string;
  resumeCursor?: RuntimeResumeCursor;
  occurredAt?: string;
};

/**
 * Deterministic fixture seed helper. The default is a global (all-projects)
 * snapshot because Project selectors already filter the shared snapshot.
 */
export function createSnapshotEnvelopeFromFixture(
  fixture: ScenarioFixture,
  options: FixtureSeedOptions = {},
): RuntimeSnapshotEnvelope {
  const streamId = options.streamId ?? `stream:${fixture.id}`;
  const generation = options.generation ?? 1;
  const sequence = options.sequence ?? 0;
  const base = createRuntimeSnapshotFromFixture(fixture);
  const snapshot =
    options.projectId != null
      ? selectProjectSnapshot(base, options.projectId, options.extraSessionIds ?? [])
      : base;

  return {
    protocolVersion: RUNTIME_PROTOCOL_VERSION,
    streamId,
    generation,
    cursor: { streamId, sequence },
    scope: options.projectId != null
      ? { kind: "project", projectId: options.projectId }
      : { kind: "all-projects" },
    snapshot,
    ...(options.snapshotRevision !== undefined ? { snapshotRevision: options.snapshotRevision } : {}),
    ...(options.resumeCursor !== undefined ? { resumeCursor: options.resumeCursor } : {}),
    ...(options.occurredAt !== undefined ? { occurredAt: options.occurredAt } : {}),
  };
}
