"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { OcgEntryGate } from "./entry-gate";
import { resolveWorkspaceView, type WorkspaceView } from "../layout/view-domain";
import { resolveControlCenterView } from "../control-center/domain";
import { resolveProjectParam } from "../project/domain";
import { resolveScenario } from "../runtime/scenarios";
import type { ScenarioId } from "../runtime/runtime-types";

export type StaticEntryRoute =
  | "home"
  | "onboarding"
  | "login"
  | "logs"
  | "resource-ledger"
  | "settings"
  | "canonical";

/** What a route opens when the query string does not say otherwise. */
type RouteEntry = {
  scenario: ScenarioId;
  /** View to open; omitted where the entry gate decides (login, onboarding). */
  view?: WorkspaceView;
  /** Whether the route hands the `project` parameter to the workspace. */
  forwardsProject?: true;
};

/**
 * The entry of every route but the root. A route missing from this table is the
 * root itself, which derives its scenario and view from the query string so
 * that one in-memory runtime instance survives a switch between views.
 */
const ROUTE_ENTRY: Partial<Record<StaticEntryRoute, RouteEntry>> = {
  onboarding: { scenario: "local-first-run", forwardsProject: true },
  login: { scenario: "remote-unauthenticated" },
  logs: { scenario: "logs-live", view: "logs", forwardsProject: true },
  "resource-ledger": { scenario: "resource-ledger", view: "ledger", forwardsProject: true },
  settings: { scenario: "local-ready", view: "settings", forwardsProject: true },
  canonical: { scenario: "local-ready", view: "canonical", forwardsProject: true },
};

/**
 * Static-export entrypoint. Query parameters are browser state, so they must
 * be read by a client component rather than awaited in a prerendered page.
 */
export function StaticEntry({ route }: { route: StaticEntryRoute }) {
  return (
    <Suspense fallback={null}>
      <StaticEntryContent route={route} />
    </Suspense>
  );
}

function StaticEntryContent({ route }: { route: StaticEntryRoute }) {
  const params = useSearchParams();
  const requestedScenario = params.get("scenario") ?? undefined;
  const requestedView = params.get("view") ?? undefined;
  const project = resolveProjectParam(params.get("project"));

  const entry = ROUTE_ENTRY[route];

  const scenario = resolveScenario(requestedScenario ?? entry?.scenario);

  return (
    <OcgEntryGate
      scenario={scenario}
      view={resolveWorkspaceView(scenario, requestedView ?? entry?.view)}
      controlCenterView={resolveControlCenterView(requestedView)}
      initialProjectId={entry === undefined || entry.forwardsProject ? project : undefined}
    />
  );
}
