/**
 * External runtime store. Pure: no React import, no transport knowledge.
 *
 * The store owns the single `RuntimeState` (authoritative snapshot + sync
 * metadata). State is committed before listeners are notified, so a React
 * `useSyncExternalStore` read never observes a torn update.
 */

import type {
  AnyRuntimeEnvelope,
} from "./runtime-envelope";
import { validateRuntimeEnvelope } from "./runtime-envelope";
import type { RuntimeSnapshotEnvelope } from "./runtime-snapshot";
import { validateRuntimeSnapshotEnvelope } from "./runtime-snapshot";
import type { RuntimeState } from "./reconciler";
import {
  createUninitializedRuntimeState,
  markLoadingSnapshot,
  markReconnecting,
  reconcileEvent,
  reconcileSnapshot,
  recordRuntimeDiagnostic,
} from "./reconciler";
import type { RuntimeSnapshot, ScenarioId } from "./runtime-types";
import type { ProjectId } from "../project/domain";
import {
  applyCanonicalSnapshot,
  applyCanonicalEvents,
  applyCanonicalCommandAck,
  createCanonicalState,
  selectCanonical,
} from "./canonical-store";
import type { CanonicalBackendEvent, CanonicalCommandAck, CanonicalSelectors, CanonicalState } from "./canonical-store";

export type RuntimeStoreListener = () => void;

export class RuntimeStore {
  private state: RuntimeState;
  private canonical: CanonicalState = createCanonicalState();
  private readonly listeners = new Set<RuntimeStoreListener>();

  constructor(initialState: RuntimeState) {
    this.state = initialState;
  }

  getState(): RuntimeState {
    return this.state;
  }

  getSnapshot(): RuntimeSnapshot {
    return this.state.snapshot;
  }

  getSync() {
    return this.state.sync;
  }

  getDiagnostics() {
    return this.state.sync.diagnostics;
  }

  subscribe(listener: RuntimeStoreListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  installSnapshot(envelope: RuntimeSnapshotEnvelope): RuntimeState {
    this.commit(reconcileSnapshot(this.state, envelope));
    return this.state;
  }

  applyEnvelope(envelope: AnyRuntimeEnvelope): RuntimeState {
    this.commit(reconcileEvent(this.state, envelope));
    return this.state;
  }

  applyUnknown(input: unknown): RuntimeState {
    const validation = validateRuntimeEnvelope(input);
    if (!validation.ok) {
      const degraded = validation.diagnostic.code === "schema-invalid" || validation.diagnostic.code === "protocol-incompatible";
      this.commit(recordRuntimeDiagnostic(this.state, validation.diagnostic, degraded ? "error" : undefined));
      return this.state;
    }
    return this.applyEnvelope(validation.envelope);
  }

  installUnknownSnapshot(input: unknown): RuntimeState {
    const validation = validateRuntimeSnapshotEnvelope(input);
    if (!validation.ok) {
      this.commit(recordRuntimeDiagnostic(this.state, validation.diagnostic, "error"));
      return this.state;
    }
    return this.installSnapshot(validation.envelope);
  }

  applyMany(envelopes: readonly AnyRuntimeEnvelope[]): RuntimeState {
    let next = this.state;
    for (const envelope of envelopes) next = reconcileEvent(next, envelope);
    this.commit(next);
    return this.state;
  }

  resetSnapshot(envelope: RuntimeSnapshotEnvelope): RuntimeState {
    const scenario: ScenarioId = envelope.snapshot.scenario;
    const next = reconcileSnapshot(createUninitializedRuntimeState(scenario), envelope);
    this.commit(next);
    return this.state;
  }

  markLoading(): RuntimeState {
    this.commit(markLoadingSnapshot(this.state));
    return this.state;
  }

  markReconnecting(): RuntimeState {
    this.commit(markReconnecting(this.state));
    return this.state;
  }

  getCanonical(): CanonicalState {
    return this.canonical;
  }

  applyCanonicalSnapshot(input: { payload: unknown; projectId: ProjectId; generation?: number }): CanonicalState {
    this.canonical = applyCanonicalSnapshot(this.canonical, {
      payload: input.payload,
      projectId: input.projectId,
      generation: input.generation ?? this.canonical.generation,
    });
    this.notify();
    return this.canonical;
  }

  applyCanonicalEvents(
    events: readonly CanonicalBackendEvent[],
    options: { projectId: ProjectId; generation?: number },
  ): CanonicalState {
    this.canonical = applyCanonicalEvents(this.canonical, events, options);
    this.notify();
    return this.canonical;
  }

  applyCanonicalCommandAck(ack: CanonicalCommandAck): CanonicalState {
    this.canonical = applyCanonicalCommandAck(this.canonical, ack);
    this.notify();
    return this.canonical;
  }

  selectCanonical(): CanonicalSelectors {
    return selectCanonical(this.canonical);
  }

  private notify(): void {
    for (const listener of [...this.listeners]) listener();
  }

  private commit(next: RuntimeState): boolean {
    if (next === this.state) return false;
    this.state = next;
    for (const listener of [...this.listeners]) listener();
    return true;
  }
}
