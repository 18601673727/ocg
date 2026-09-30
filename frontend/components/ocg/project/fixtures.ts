/**
 * Deterministic Project fixtures.
 *
 * Projects are a frontend-only grouping in this slice. The metadata below is
 * intentionally compact: it maps the existing session IDs and resource-ledger
 * Job IDs that already exist in the runtime fixtures to a project. No new
 * backend fields are implied.
 */

import {
  PROJECTS,
  type KnownProjectId,
  type ProjectId,
  selectProject,
} from "./domain";

export { PROJECTS };

export type ProjectFixture = {
  id: KnownProjectId;
  /** Existing runtime session IDs owned by this project. */
  sessionIds: readonly string[];
  /** Existing resource-ledger Job IDs owned by this project. */
  ledgerJobIds: readonly string[];
};

/**
 * Stable mapping over existing fixture identifiers. Every project owns at
 * least one session so the shell always has an active session to fall back
 * to. Zhuju owns the spend/retry/runtime history; RouteLace owns the launch
 * approval and some history.
 */
export const PROJECT_FIXTURES: Record<ProjectId, ProjectFixture> = {
  zhuju: {
    id: "zhuju",
    sessionIds: ["design-pwa-shell", "coding-mission-runtime"],
    ledgerJobIds: ["job-runtime", "job-ledger"],
  },
  "route-lace": {
    id: "route-lace",
    sessionIds: ["research-space-bunny", "research-rust-graph"],
    ledgerJobIds: ["job-deploy"],
  },
  ocg: {
    id: "ocg",
    sessionIds: ["coding-tool-gateway", "design-resource-controls"],
    ledgerJobIds: [],
  },
  cecece: {
    id: "cecece",
    sessionIds: ["devops-debian-runtime", "devops-cloudflare-access"],
    ledgerJobIds: [],
  },
};


export function projectFixture(id: ProjectId): ProjectFixture {
  return PROJECT_FIXTURES[selectProject(id).id as KnownProjectId];
}

export function projectSessionIds(id: ProjectId): readonly string[] {
  return projectFixture(id).sessionIds;
}

export function projectLedgerJobIds(id: ProjectId): readonly string[] {
  return projectFixture(id).ledgerJobIds;
}
