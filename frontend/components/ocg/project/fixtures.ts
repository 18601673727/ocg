/**
 * Deterministic session and ledger fixtures.
 *
 * Canonical project identity comes from the backend. This file only maps the
 * legacy mock runtime's session and ledger identifiers.
 */

import {
  type ProjectId,
} from "./domain";

export type ProjectFixture = {
  id: ProjectId;
  /** Existing runtime session IDs owned by this project. */
  sessionIds: readonly string[];
  /** Existing resource-ledger Job IDs owned by this project. */
  ledgerJobIds: readonly string[];
};

/**
 * Stable mapping over existing mock identifiers. These entries are not the
 * canonical project registry and are used only by the mock runtime.
 */
const PROJECT_FIXTURES: Record<ProjectId, ProjectFixture> = {
  zhuju: {
    id: "zhuju",
    sessionIds: ["design-pwa-shell", "coding-mission-runtime"],
    ledgerJobIds: ["job-runtime", "job-ledger"],
  },
  "route-lace": {
    id: "route-lace",
    sessionIds: ["research-example-model", "research-rust-graph"],
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

export const FIXTURE_PROJECT_IDS = Object.keys(PROJECT_FIXTURES);

export function projectFixture(id: ProjectId): ProjectFixture {
  return PROJECT_FIXTURES[id] ?? { id, sessionIds: [], ledgerJobIds: [] };
}

export function projectSessionIds(id: ProjectId): readonly string[] {
  return projectFixture(id).sessionIds;
}

export function projectLedgerJobIds(id: ProjectId): readonly string[] {
  return projectFixture(id).ledgerJobIds;
}
