/**
 * External runtime store. Pure: no React import, no transport knowledge.
 *
 * The store owns the single `RuntimeState` (authoritative snapshot + sync
 * metadata). State is committed before listeners are notified, so a React
 * `useSyncExternalStore` read never observes a torn update.
 */

import type { AnyRuntimeEnvelope } from "./runtime-envelope";
import type { RuntimeSnapshotEnvelope } from "./runtime-snapshot";
import type { RuntimeState } from "./reconciler";
import {
  createUninitializedRuntimeState,
  markLoadingSnapshot,
  markReconnecting,
  reconcileEvent,
  reconcileSnapshot,
} from "./reconciler";
import type { RuntimeSnapshot } from "./runtime-types";

export type RuntimeStoreListener = () => void;

export class RuntimeStore {
  private state: RuntimeState;
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

  applyMany(envelopes: readonly AnyRuntimeEnvelope[]): RuntimeState {
    let next = this.state;
    for (const envelope of envelopes) next = reconcileEvent(next, envelope);
    this.commit(next);
    return this.state;
  }

  resetSnapshot(envelope: RuntimeSnapshotEnvelope): RuntimeState {
    const next = reconcileSnapshot(createUninitializedRuntimeState(), envelope);
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

  private commit(next: RuntimeState): boolean {
    if (next === this.state) return false;
    this.state = next;
    for (const listener of [...this.listeners]) listener();
    return true;
  }
}
