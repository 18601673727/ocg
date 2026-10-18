"use client";

import { useEffect, useState } from "react";

import type { HttpCanonicalControlClient } from "@/components/ocg/runtime/canonical-client";
import { JobsSurface } from "@/components/ocg/job/jobs-surface";
import { ChatSurface } from "@/components/ocg/chat/chat-surface";
import { OcgSidebar } from "@/components/ocg/sidebar/ocg-sidebar";
import { OcgTopbar } from "@/components/ocg/topbar/ocg-topbar";
import { SettingsSurface } from "@/components/ocg/settings/settings-surface";
import { LogsSurface } from "@/components/ocg/dashboard/logs-surface";
import { ObservationsSurface } from "@/components/ocg/dashboard/observations-surface";
import type { decodeHealth } from "@/components/ocg/contracts";
import {
  parseWorkspaceLocation,
  workspaceLocationHref,
  type WorkspaceLocation,
  type WorkspaceView,
} from "./view-domain";

type Health = ReturnType<typeof decodeHealth>;

/** Jobs this tab remembers having opened; the Control Plane has no Job list to ask. */
const OPENED_JOBS_LIMIT = 20;

/**
 * The signed-in console: the 0.4.5 sidebar / topbar / view layout over the
 * 0.5 operator session. Every view is either backed by a real Control Plane
 * route or says that it is not available.
 */
export function AppShell({
  client,
  health,
  sessionExpiresAtUs,
  signingOut,
  signOutError,
  onSignOut,
}: {
  client: HttpCanonicalControlClient;
  health: Health | null;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  signOutError: string | null;
  onSignOut: () => void;
}) {
  // The shell mounts only after the browser has read its session, so the
  // address is always available here.
  const [initial] = useState(() => parseWorkspaceLocation(window.location.search));
  const [location, setLocation] = useState<WorkspaceLocation>(initial);
  const [opened, setOpened] = useState<string[]>(initial.job ? [initial.job] : []);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(false);

  useEffect(() => {
    const onPopState = () => {
      const next = parseWorkspaceLocation(window.location.search);
      setLocation(next);
      const job = next.job;
      if (job) setOpened((current) => [job, ...current.filter((entry) => entry !== job)].slice(0, OPENED_JOBS_LIMIT));
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  function go(next: WorkspaceLocation) {
    const href = workspaceLocationHref(next);
    if (href !== window.location.pathname + window.location.search) {
      window.history.pushState(null, "", href);
    }
    setLocation(next);
  }

  // A Job opens in the surface that asked for it, so sending a Chat message stays in Chat.
  function openJob(job: string, surface: "chat" | "jobs") {
    setOpened((current) => [job, ...current.filter((entry) => entry !== job)].slice(0, OPENED_JOBS_LIMIT));
    go({ view: surface, job });
  }

  function navigate(view: WorkspaceView) {
    go({ view, job: location.job });
  }

  const { view } = location;
  return (
    <div className="flex h-dvh min-h-0 w-full bg-background">
      <OcgSidebar
        mobileOpen={mobileOpen}
        collapsed={collapsed}
        onCollapse={() => setCollapsed((current) => !current)}
        onCloseMobile={() => setMobileOpen(false)}
        activeView={view}
        onNavigate={navigate}
        openedJobs={opened}
        activeJob={location.job}
        onOpenJob={(job) => openJob(job, view === "chat" ? "chat" : "jobs")}
        signingOut={signingOut}
        onSignOut={onSignOut}
      />
      <div className="flex min-w-0 flex-1 flex-col">
        <OcgTopbar
          activeView={view}
          job={location.job}
          collapsed={collapsed}
          onOpenNavigation={() => {
            setCollapsed(false);
            setMobileOpen(true);
          }}
          health={health}
        />
        {signOutError ? (
          <p role="alert" className="px-4 pb-2 text-sm text-destructive">
            {signOutError}
          </p>
        ) : null}
        <main className="ocg-subtle-scrollbar min-h-0 flex-1 overflow-y-auto">
          {/* Kept mounted while another view is open: a pending submission keeps its
              Idempotency-Key and its lookup, and a watched Job keeps its stream. */}
          <div hidden={view !== "chat"}>
            {/* Only the visible view watches the Job: a Job has one event stream per client. */}
            <ChatSurface
              client={client}
              job={view === "chat" ? location.job : null}
              onJob={(job) => openJob(job, "chat")}
              onNewConversation={() => go({ view: "chat", job: null })}
              onInspectJob={(targetJob) => openJob(targetJob, "jobs")}
            />
          </div>
          <div hidden={view !== "jobs"}>
            <JobsSurface client={client} active={view === "jobs"} job={view === "jobs" ? location.job : null} onJob={(job) => openJob(job, "jobs")} />
          </div>
          {view === "settings" ? (
            <SettingsSurface
              client={client}
              health={health}
              sessionExpiresAtUs={sessionExpiresAtUs}
              signingOut={signingOut}
              onSignOut={onSignOut}
            />
          ) : null}
          {view === "usage" || view === "health" ? <ObservationsSurface key={view} client={client} kind={view} health={health} onJob={(job) => openJob(job, "jobs")} /> : null}
          {view === "logs" ? <LogsSurface client={client} /> : null}
        </main>
      </div>
    </div>
  );
}
