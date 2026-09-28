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
  applyCanonicalCommandAck,
  applyCanonicalEvents,
  applyCanonicalSnapshot,
  createCanonicalState,
  type CanonicalBackendEvent,
  type CanonicalCommandAck,
  type CanonicalSelectors,
  type CanonicalState,
} from "./canonical-store";
import { selectCanonical } from "./canonical-store";

export type RuntimeStoreListener = () => void;

export class RuntimeStore {
  private state: RuntimeState;
  /**
   * The backend-backed canonical WorkNode/Run projection, held by the same
   * store so a canonical snapshot, canonical event and runtime envelope all
   * commit together and notify listeners once. It is a read model of durable
   * backend state, never execution authority.
   */
  private canonical: CanonicalState = createCanonicalState();
  private readonly listeners = new Set<RuntimeStoreListener>();

  constructor(initialState: RuntimeState) {
    this.state = initialState;
  }

  /** Stable identity between commits. */
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

  /** Install an authoritative baseline snapshot envelope. */
  installSnapshot(envelope: RuntimeSnapshotEnvelope): RuntimeState {
    this.commit(reconcileSnapshot(this.state, envelope));
    return this.state;
  }

  /** Apply one validated event envelope. */
  applyEnvelope(envelope: AnyRuntimeEnvelope): RuntimeState {
    this.commit(reconcileEvent(this.state, envelope));
    return this.state;
  }

  /** Normalize an untrusted transport value before it reaches reconciliation. */
  applyUnknown(input: unknown): RuntimeState {
    const validation = validateRuntimeEnvelope(input);
    if (!validation.ok) {
      const degraded = validation.diagnostic.code === "schema-invalid" || validation.diagnostic.code === "protocol-incompatible";
      this.commit(recordRuntimeDiagnostic(this.state, validation.diagnostic, degraded ? "error" : undefined));
      return this.state;
    }
    return this.applyEnvelope(validation.envelope);
  }

  /** Normalize an untrusted snapshot value before installing a baseline. */
  installUnknownSnapshot(input: unknown): RuntimeState {
    const validation = validateRuntimeSnapshotEnvelope(input);
    if (!validation.ok) {
      this.commit(recordRuntimeDiagnostic(this.state, validation.diagnostic, "error"));
      return this.state;
    }
    return this.installSnapshot(validation.envelope);
  }

  /** Apply a batch atomically and notify once. */
  applyMany(envelopes: readonly AnyRuntimeEnvelope[]): RuntimeState {
    let next = this.state;
    for (const envelope of envelopes) next = reconcileEvent(next, envelope);
    this.commit(next);
    return this.state;
  }

  /** Reset to a fresh baseline, discarding prior diagnostics and seen sets. */
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

  /* ---------------------------------------------------------------------- */
  /* Canonical WorkNode/Run projection                                       */
  /* ---------------------------------------------------------------------- */

  /** The current canonical projection state. */
  getCanonical(): CanonicalState {
    return this.canonical;
  }

  /**
   * Install a versioned, project-scoped canonical snapshot. A stale generation,
   * an older cursor, a wrong Project or a malformed entity is refused and
   * recorded as a canonical diagnostic instead of being rendered.
   */
  applyCanonicalSnapshot(input: { payload: unknown; projectId: ProjectId; generation?: number }): CanonicalState {
    this.canonical = applyCanonicalSnapshot(this.canonical, {
      payload: input.payload,
      projectId: input.projectId,
      generation: input.generation ?? this.canonical.generation,
    });
    this.notify();
    return this.canonical;
  }

  /** Apply a canonical event tail strictly after the installed baseline. */
  applyCanonicalEvents(
    events: readonly CanonicalBackendEvent[],
    options: { projectId: ProjectId; generation?: number },
  ): CanonicalState {
    this.canonical = applyCanonicalEvents(this.canonical, events, {
      projectId: options.projectId,
      generation: options.generation ?? this.canonical.generation,
    });
    this.notify();
    return this.canonical;
  }

  /** Correlate one control-command acknowledgement by stable command identity. */
  applyCanonicalCommandAck(ack: CanonicalCommandAck): CanonicalState {
    this.canonical = applyCanonicalCommandAck(this.canonical, ack);
    this.notify();
    return this.canonical;
  }

  /** Read-only selectors over the canonical projection. */
  selectCanonical(title: string): CanonicalSelectors {
    return selectCanonical(this.canonical, title);
  }

  /** Notify listeners after a canonical-only commit. */
  private notify(): void {
    for (const listener of [...this.listeners]) listener();
  }

  /** Returns true when the commit changed state and listeners were notified. */
  private commit(next: RuntimeState): boolean {
    if (next === this.state) return false;
    this.state = next;
    for (const listener of [...this.listeners]) listener();
    return true;
  }
}

/** Convenience constructor for a store that owns one baseline. */
export function createRuntimeStore(envelope: RuntimeSnapshotEnvelope): RuntimeStore {
  const store = new RuntimeStore(createUninitializedRuntimeState(envelope.snapshot.scenario));
  store.installSnapshot(envelope);
  return store;
}
