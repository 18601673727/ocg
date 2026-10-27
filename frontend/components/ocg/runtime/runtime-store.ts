import type { OcgRuntimeEvent } from "../types";
import type { RuntimeSnapshot } from "./runtime-types";
import { reconcileRuntimeEvent, type RuntimeState } from "./reconciler";

export function emptyRuntimeSnapshot(): RuntimeSnapshot {
  return {
    status: { state: "connecting", detail: "Runtime snapshot is not loaded." },
    sessions: [],
    messagesBySession: {},
    observabilityBySession: {},
    executionBySession: {},
    accountingBySession: {},
  };
}

export class RuntimeStore {
  private state: RuntimeState;

  constructor(snapshot: RuntimeSnapshot) {
    this.state = { snapshot, diagnostics: [] };
  }

  getSnapshot(): RuntimeSnapshot {
    return this.state.snapshot;
  }

  getDiagnostics(): RuntimeState["diagnostics"] {
    return this.state.diagnostics;
  }

  apply(event: OcgRuntimeEvent): void {
    this.state = reconcileRuntimeEvent(this.state, event);
  }
}
