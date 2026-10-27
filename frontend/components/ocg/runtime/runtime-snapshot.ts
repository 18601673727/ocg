/**
 * Authoritative runtime snapshot envelope.
 *
 * A snapshot is installed as one coherent baseline together with the cursor it
 * was produced at. It is never decomposed into synthetic events: the reconciler
 * treats it as the new truth and resumes from its cursor.
 */

import type { BootstrapState } from "../bootstrap/types";
import { isRecord, isNonEmptyString, isNonNegativeInteger, isOneOf } from "@/lib/narrow";
import { isProjectId, type ProjectId } from "../project/domain";
import type { RuntimeSnapshot } from "./runtime-types";
import type {
  RuntimeCursor,
  RuntimeGeneration,
  RuntimeProtocolVersion,
  RuntimeResumeCursor,
  RuntimeStreamId,
} from "./runtime-envelope";
import {
  RUNTIME_PROTOCOL_VERSION,
  SNAPSHOT_AUTHORITIES,
  validateAttentionItemValue,
  validateBootstrapValue,
  validateChatMessageValue,
  validateChatSessionValue,
  validateJobExecutionValue,
  validateLedgerEntryValue,
  validateLogEntryValue,
  validateObservabilityValue,
  validateQueuedMessageValue,
  validateRuntimeStatusValue,
} from "./runtime-envelope";
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

function invalidSnapshot(message: string): RuntimeSnapshotValidation {
  return {
    ok: false,
    diagnostic: { code: "schema-invalid", severity: "error", message },
  };
}

/** Validate the untrusted snapshot boundary before it becomes the new truth. */
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
  if (!isNonNegativeInteger(input.generation))
    return invalidSnapshot("Snapshot generation must be a non-negative integer.");
  if (
    !isRecord(input.cursor) ||
    !isNonEmptyString(input.cursor.streamId) ||
    !isNonNegativeInteger(input.cursor.sequence)
  ) {
    return invalidSnapshot("Snapshot cursor must contain a streamId and non-negative sequence.");
  }
  if (input.cursor.streamId !== input.streamId)
    return invalidSnapshot("Snapshot cursor streamId must match streamId.");
  if (!isRecord(input.scope) || typeof input.scope.kind !== "string")
    return invalidSnapshot("Snapshot scope is required.");
  if (input.scope.kind === "project" && !isProjectId(input.scope.projectId)) {
    return invalidSnapshot("Project-scoped snapshot must name a known Project.");
  }
  if (input.scope.kind !== "project" && input.scope.kind !== "all-projects") {
    return invalidSnapshot(`Unknown snapshot scope "${String(input.scope.kind)}".`);
  }
  if (input.snapshotRevision !== undefined && typeof input.snapshotRevision !== "string") {
    return invalidSnapshot("Snapshot revision must be a string when present.");
  }
  if (input.resumeCursor !== undefined && typeof input.resumeCursor !== "string") {
    return invalidSnapshot("Snapshot resume cursor must be a string when present.");
  }
  if (input.occurredAt !== undefined && typeof input.occurredAt !== "string") {
    return invalidSnapshot("Snapshot occurredAt must be a string when present.");
  }
  if (!isRecord(input.snapshot)) return invalidSnapshot("Snapshot payload is required.");
  const snapshot = input.snapshot;
  if (snapshot.authority !== undefined && !isOneOf(snapshot.authority, SNAPSHOT_AUTHORITIES)) {
    return invalidSnapshot(`Unknown snapshot authority "${String(snapshot.authority)}".`);
  }
  if (!isRecord(snapshot.status)) return invalidSnapshot("Snapshot status is required.");
  const statusError = validateRuntimeStatusValue(snapshot.status);
  if (statusError) return invalidSnapshot(`Snapshot status is invalid: ${statusError}`);
  if (!Array.isArray(snapshot.sessions))
    return invalidSnapshot("Snapshot sessions must be an array.");
  for (const [index, session] of snapshot.sessions.entries()) {
    const error = validateChatSessionValue(session);
    if (error) return invalidSnapshot(`Snapshot session ${index} is invalid: ${error}`);
  }
  if (!isRecord(snapshot.messagesBySession))
    return invalidSnapshot("Snapshot messagesBySession must be an object.");
  for (const [sessionId, messages] of Object.entries(snapshot.messagesBySession)) {
    if (!Array.isArray(messages)) {
      return invalidSnapshot(`Snapshot messages for session "${sessionId}" must be an array.`);
    }
    for (const message of messages) {
      const error = validateChatMessageValue(message);
      if (error)
        return invalidSnapshot(`Snapshot message in session "${sessionId}" is invalid: ${error}`);
    }
  }
  if (snapshot.chatQueues !== undefined) {
    if (!isRecord(snapshot.chatQueues))
      return invalidSnapshot("Snapshot chatQueues must be an object when present.");
    for (const [sessionId, queue] of Object.entries(snapshot.chatQueues)) {
      if (!isRecord(queue) || typeof queue.paused !== "boolean" || !Array.isArray(queue.queue)) {
        return invalidSnapshot(
          `Snapshot queue for session "${sessionId}" must carry a queue and paused flag.`,
        );
      }
      for (const item of queue.queue) {
        const error = validateQueuedMessageValue(item);
        if (error)
          return invalidSnapshot(
            `Snapshot queued message in session "${sessionId}" is invalid: ${error}`,
          );
      }
    }
  }
  if (!isRecord(snapshot.observabilityBySession))
    return invalidSnapshot("Snapshot observabilityBySession must be an object.");
  for (const [sessionId, observability] of Object.entries(snapshot.observabilityBySession)) {
    if (observability === null) continue;
    const error = validateObservabilityValue(observability);
    if (error)
      return invalidSnapshot(
        `Snapshot observability for session "${sessionId}" is invalid: ${error}`,
      );
  }
  if (!isRecord(snapshot.executionBySession))
    return invalidSnapshot("Snapshot executionBySession must be an object.");
  for (const [sessionId, execution] of Object.entries(snapshot.executionBySession)) {
    if (execution === null) continue;
    const error = validateJobExecutionValue(execution);
    if (error)
      return invalidSnapshot(`Snapshot execution for session "${sessionId}" is invalid: ${error}`);
  }
  if (!isRecord(snapshot.accountingBySession))
    return invalidSnapshot("Snapshot accountingBySession must be an object.");
  for (const [sessionId, accounting] of Object.entries(snapshot.accountingBySession)) {
    if (accounting === null) continue;
    if (!isRecord(accounting)) {
      return invalidSnapshot(
        `Snapshot accounting for session "${sessionId}" must be an object or null.`,
      );
    }
  }
  if (snapshot.executionsByProject !== undefined) {
    if (!isRecord(snapshot.executionsByProject)) {
      return invalidSnapshot("Snapshot executionsByProject must be an object when present.");
    }
    for (const [projectId, executions] of Object.entries(snapshot.executionsByProject)) {
      if (!isRecord(executions)) {
        return invalidSnapshot(`Snapshot executions for project "${projectId}" must be an object.`);
      }
      for (const [jobId, execution] of Object.entries(executions)) {
        const error = validateJobExecutionValue(execution);
        if (error) return invalidSnapshot(`Snapshot execution "${jobId}" is invalid: ${error}`);
      }
    }
  }
  if (snapshot.resourceLedger === undefined) {
    return invalidSnapshot("Snapshot resourceLedger must be null or contain an entries array.");
  }
  if (snapshot.resourceLedger !== null) {
    if (!isRecord(snapshot.resourceLedger)) {
      return invalidSnapshot("Snapshot resourceLedger must be null or contain an entries array.");
    }
    if (typeof snapshot.resourceLedger.generatedAt !== "string") {
      return invalidSnapshot("Snapshot resourceLedger must carry a generatedAt timestamp.");
    }
    if (!Array.isArray(snapshot.resourceLedger.entries)) {
      return invalidSnapshot("Snapshot resourceLedger must contain an entries array.");
    }
    for (const entry of snapshot.resourceLedger.entries) {
      const error = validateLedgerEntryValue(entry);
      if (error) return invalidSnapshot(`Snapshot ledger entry is invalid: ${error}`);
    }
  }
  if (snapshot.attentionItems !== undefined) {
    if (!Array.isArray(snapshot.attentionItems))
      return invalidSnapshot("Snapshot attentionItems must be an array.");
    for (const item of snapshot.attentionItems) {
      const error = validateAttentionItemValue(item);
      if (error) return invalidSnapshot(`Snapshot attention item is invalid: ${error}`);
    }
  }
  if (snapshot.logs !== undefined) {
    if (!Array.isArray(snapshot.logs)) return invalidSnapshot("Snapshot logs must be an array.");
    for (const entry of snapshot.logs) {
      const error = validateLogEntryValue(entry);
      if (error) return invalidSnapshot(`Snapshot log entry is invalid: ${error}`);
    }
  }
  const bootstrapError = validateBootstrapValue(snapshot.bootstrap);
  if (bootstrapError) return invalidSnapshot(`Snapshot bootstrap is invalid: ${bootstrapError}`);
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
