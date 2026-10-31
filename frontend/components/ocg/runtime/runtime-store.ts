import type { OcgRuntimeEvent } from "../types";
import type { RuntimeSnapshot } from "./runtime-types";
import { reconcileRuntimeEvent } from "./reconciler";

export function emptyRuntimeSnapshot(): RuntimeSnapshot {
  return {
    status: { state: "connecting", detail: "Runtime snapshot is not loaded." },
    sessions: [],
    messagesBySession: {},
    executionBySession: {},
  };
}

export class RuntimeStore {
  private snapshot: RuntimeSnapshot;

  constructor(snapshot: RuntimeSnapshot) {
    this.snapshot = snapshot;
  }

  getSnapshot(): RuntimeSnapshot {
    return this.snapshot;
  }

  apply(event: OcgRuntimeEvent): void {
    this.snapshot = reconcileRuntimeEvent(this.snapshot, event);
  }
}
