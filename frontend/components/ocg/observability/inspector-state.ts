export type InspectorTab = "overview" | "runtime" | "usage";
export type InspectorMode = "collapsed" | "docked" | "expanded";

export const INSPECTOR_TABS: readonly InspectorTab[] = ["overview", "runtime", "usage"];

export function isInspectorTab(value: string): value is InspectorTab {
  return INSPECTOR_TABS.includes(value as InspectorTab);
}

export function restoreWorkerSelection(previousId: string | null, workerIds: string[]): string | null {
  return previousId && workerIds.includes(previousId) ? previousId : null;
}

export function toggleInspectorMode(mode: InspectorMode): Exclude<InspectorMode, "collapsed"> {
  return mode === "expanded" ? "docked" : "expanded";
}
