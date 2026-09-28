"use client";

import { OcgRuntimeProvider, useOcgRuntime } from "../runtime/runtime-context";
import type { ScenarioId } from "../runtime/runtime-types";
import { RuntimeWorkspace } from "../layout/app-shell";
import type { WorkspaceView } from "../layout/app-shell";
import type { ControlCenterView } from "../control-center/domain";
import type { ProjectId } from "../project/domain";
import { ProjectProvider } from "../project/project-context";
import { LoginView } from "../login/login-view";
import { OnboardingWizard } from "../onboarding/onboarding-wizard";
import { selectBootstrapEntry } from "./selectors";

/**
 * Clean root entry gate. The normalized bootstrap state decides whether the
 * operator sees the workspace, the access surface, or the setup wizard. Local
 * scenarios always resolve to the workspace, so login is never shown for them.
 */
export function OcgEntryGate({
  scenario,
  view = "chat",
  controlCenterView = "profiles",
  initialProjectId,
}: {
  scenario: ScenarioId;
  view?: WorkspaceView;
  controlCenterView?: ControlCenterView;
  initialProjectId?: ProjectId;
}) {
  return (
    <OcgRuntimeProvider scenario={scenario}>
      <ProjectProvider initialProjectId={initialProjectId}>
        <BootstrapSurface view={view} controlCenterView={controlCenterView} />
      </ProjectProvider>
    </OcgRuntimeProvider>
  );
}

function BootstrapSurface({ view, controlCenterView }: { view: WorkspaceView; controlCenterView: ControlCenterView }) {
  const { snapshot } = useOcgRuntime();
  const entry = selectBootstrapEntry(snapshot.bootstrap);

  if (entry === "login") return <LoginView />;
  if (entry === "onboarding") return <OnboardingWizard />;
  return <RuntimeWorkspace view={view} controlCenterView={controlCenterView} />;
}
