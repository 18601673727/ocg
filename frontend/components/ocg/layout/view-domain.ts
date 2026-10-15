/**
 * Pure Workspace view resolution for the 0.5 operator console.
 *
 * Adapted from the 0.4.5 shell's `?view=` routing. Only views the Control
 * Plane can actually back are `available`; the others are named so the
 * navigation states what is missing instead of pretending.
 */

export type WorkspaceView = "jobs" | "usage" | "logs" | "settings";

export type ViewAvailability =
  | { available: true }
  | {
      available: false;
      /** The Control Plane contract this view would read, which does not exist. */
      missing: string;
    };

export const WORKSPACE_VIEWS: readonly WorkspaceView[] = ["jobs", "usage", "logs", "settings"];

export const VIEW_AVAILABILITY: Record<WorkspaceView, ViewAvailability> = {
  jobs: { available: true },
  settings: { available: true },
  usage: {
    available: false,
    missing: "a usage / metrics read route (Metrics and AvailabilityStat are stored, but nothing serves them)",
  },
  logs: {
    available: false,
    missing: "a bounded log query route (the process logs to its own files; no route reads them)",
  },
};

export const VIEW_TITLE: Record<WorkspaceView, string> = {
  jobs: "Jobs",
  usage: "Usage",
  logs: "Logs",
  settings: "Settings",
};

export function isWorkspaceView(value: unknown): value is WorkspaceView {
  return typeof value === "string" && (WORKSPACE_VIEWS as readonly string[]).includes(value);
}

const JOB_IDENTITY = /^job:(0|[1-9][0-9]*)$/;

export function isJobIdentity(value: unknown): value is string {
  return typeof value === "string" && JOB_IDENTITY.test(value);
}

export type WorkspaceLocation = {
  view: WorkspaceView;
  /** The Job the Jobs view watches, when the address names one. */
  job: string | null;
};

/** Reads a location's query. Anything it does not recognize falls back to the Jobs view. */
export function parseWorkspaceLocation(search: string): WorkspaceLocation {
  const params = new URLSearchParams(search);
  const view = params.get("view");
  const job = params.get("job");
  return {
    view: isWorkspaceView(view) ? view : "jobs",
    job: isJobIdentity(job) ? job : null,
  };
}

/** The query that names `location`; the default view with no Job is the bare root. */
export function workspaceLocationHref(location: WorkspaceLocation): string {
  const params = new URLSearchParams();
  if (location.view !== "jobs") params.set("view", location.view);
  if (location.view === "jobs" && location.job) params.set("job", location.job);
  const query = params.toString();
  return query ? `/?${query}` : "/";
}
