import type { OcgRuntimeEvent } from "../types";
import { reconcileRuntimeEvent } from "./reconciler";
import type { RuntimeSnapshot } from "./runtime-types";

export function emptyRuntimeSnapshot(): RuntimeSnapshot {
  return {
    status: { state: "connecting", detail: "Runtime snapshot is not loaded." },
    sessions: [],
    messagesBySession: {},
    conversationList: "loading",
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
