"use client";

import { useEffect, useState, type ReactNode } from "react";
import { usePathname, useRouter } from "next/navigation";
import { OcgRuntimeProvider, useOcgRuntime } from "../runtime/runtime-context";
import type { ScenarioId } from "../runtime/runtime-types";
import { RuntimeWorkspace } from "../layout/app-shell";
import type { WorkspaceView } from "../layout/app-shell";
import type { ControlCenterView } from "../control-center/domain";
import type { ProjectId } from "../project/domain";
import { ProjectProvider } from "../project/project-context";
import { LoginView } from "../login/login-view";
import { SetupWizard } from "../setup/setup-wizard";
import { selectBootstrapEntry } from "./selectors";
import { useOcgControlUrl } from "../profile/control-url";
import { createProfileClient } from "../profile/profile-client";
import { useI18n } from "../i18n";
import { resolveProjectParam, withProjectParam } from "../project/domain";

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
  const path = usePathname();
  const { snapshot, authority } = useOcgRuntime();
  if (authority === "canonical") {
    if (path === "/onboarding") return <SetupWizard />;
    return (
      <CanonicalReadinessGate>
        <RuntimeWorkspace view={view} controlCenterView={controlCenterView} />
      </CanonicalReadinessGate>
    );
  }
  const entry = selectBootstrapEntry(snapshot.bootstrap);

  if (entry === "login") return <LoginView />;
  if (entry === "onboarding") return <SetupWizard />;
  return <RuntimeWorkspace view={view} controlCenterView={controlCenterView} />;
}

/**
 * Every canonical workspace route enters through the backend readiness
 * authority (`runnable_choices` on `GET /api/v1/profile`). With no runnable
 * model the workspace is never rendered, whatever the URL says; setup opens
 * instead. The check is a read, not a re-verification, so a configured
 * installation passes straight through.
 */
function CanonicalReadinessGate({ children }: { children: ReactNode }) {
  const controlUrl = useOcgControlUrl();
  const router = useRouter();
  const { t } = useI18n();
  const [state, setState] = useState<{ status: "checking" | "ready" | "setup" } | { status: "failed"; error: string }>({ status: "checking" });
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    if (!controlUrl) return;
    let cancelled = false;
    createProfileClient(controlUrl, fetch).read().then((view) => {
      if (cancelled) return;
      if (view.runnable_choices.length > 0) {
        setState({ status: "ready" });
      } else {
        setState({ status: "setup" });
        // Canonical onboarding URL carries only `project=`; no fixture state.
        const project = typeof window !== "undefined"
          ? resolveProjectParam(new URLSearchParams(window.location.search).get("project"))
          : undefined;
        router.replace(project ? withProjectParam("/onboarding", project) : "/onboarding");
      }
    }).catch((cause: unknown) => {
      if (!cancelled) setState({ status: "failed", error: cause instanceof Error ? cause.message : t("shell.profileReadFailed") });
    });
    return () => { cancelled = true; };
  }, [controlUrl, router, t, attempt]);

  if (state.status === "ready") return children;
  return (
    <main className="flex min-h-dvh flex-col items-center justify-center gap-3 p-6 text-center text-sm">
      {state.status === "failed" ? (
        <>
          <p role="alert" className="max-w-md break-words">{t("shell.readinessFailed", { error: state.error })}</p>
          <button type="button" className="rounded-md border px-3 py-2" onClick={() => { setState({ status: "checking" }); setAttempt((value) => value + 1); }}>
            {t("shell.retry")}
          </button>
        </>
      ) : (
        <p role="status" className="text-muted-foreground">{t("shell.starting")}</p>
      )}
    </main>
  );
}
