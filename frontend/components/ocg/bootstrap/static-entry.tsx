"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { OcgEntryGate } from "./entry-gate";
import { resolveWorkspaceView } from "../layout/view-domain";
import { resolveControlCenterView } from "../control-center/domain";
import { resolveProjectParam } from "../project/domain";
import { resolveScenario } from "../runtime/scenarios";

export type StaticEntryRoute =
  | "home"
  | "onboarding"
  | "login"
  | "logs"
  | "resource-ledger"
  | "settings"
  | "canonical";

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

  if (route === "onboarding") {
    return <OcgEntryGate scenario={resolveScenario(requestedScenario ?? "local-first-run")} />;
  }
  if (route === "login") {
    return <OcgEntryGate scenario={resolveScenario(requestedScenario ?? "remote-unauthenticated")} />;
  }
  if (route === "logs") {
    return <OcgEntryGate scenario={resolveScenario(requestedScenario ?? "logs-live")} view="logs" initialProjectId={project} />;
  }
  if (route === "resource-ledger") {
    return <OcgEntryGate scenario={resolveScenario(requestedScenario ?? "resource-ledger")} view="ledger" initialProjectId={project} />;
  }
  if (route === "settings") {
    return <OcgEntryGate scenario={resolveScenario(requestedScenario ?? "local-ready")} view="settings" initialProjectId={project} />;
  }
  if (route === "canonical") {
    return <OcgEntryGate scenario={resolveScenario(requestedScenario ?? "local-ready")} view="canonical" initialProjectId={project} />;
  }

  const scenario = resolveScenario(requestedScenario);
  return (
    <OcgEntryGate
      scenario={scenario}
      view={resolveWorkspaceView(scenario, requestedView)}
      controlCenterView={resolveControlCenterView(requestedView)}
      initialProjectId={project}
    />
  );
}
