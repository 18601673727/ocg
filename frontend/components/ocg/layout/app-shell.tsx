"use client";

import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { HttpCanonicalControlClient } from "@/components/ocg/runtime/canonical-client";
import { JobSubmissionForm } from "@/components/ocg/job/job-submission-form";
import { JobWatch } from "@/components/ocg/job/job-watch";
import { OcgSidebar } from "@/components/ocg/sidebar/ocg-sidebar";
import { OcgTopbar } from "@/components/ocg/topbar/ocg-topbar";
import { SettingsSurface } from "@/components/ocg/settings/settings-surface";
import { UnavailableSurface } from "./unavailable-surface";
import {
  VIEW_AVAILABILITY,
  isJobIdentity,
  parseWorkspaceLocation,
  workspaceLocationHref,
  type WorkspaceLocation,
  type WorkspaceView,
} from "./view-domain";

type Health = {
  host: string;
  database: {
    status: string;
    accepting_commands: boolean;
    readable: boolean;
  };
};

/** Jobs this tab remembers having opened; the Control Plane has no Job list to ask. */
const OPENED_JOBS_LIMIT = 20;

function JobsView({
  client,
  job,
  onJob,
}: {
  client: HttpCanonicalControlClient;
  job: string | null;
  onJob: (job: string) => void;
}) {
  const [inspect, setInspect] = useState("");
  return (
    <div className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-4 py-6 sm:px-6">
      <JobSubmissionForm client={client} onJob={onJob} />
      <section className="flex flex-col gap-3 rounded-lg border border-border bg-card p-5 text-card-foreground">
        <h2 className="font-heading text-lg">Inspect a Job</h2>
        <form
          className="flex flex-wrap items-end gap-2"
          onSubmit={(event) => {
            event.preventDefault();
            const target = inspect.trim();
            if (isJobIdentity(target)) onJob(target);
          }}
        >
          <label className="flex flex-1 flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
            Job
            <Input
              value={inspect}
              onChange={(event) => setInspect(event.target.value)}
              placeholder="job:<n>"
              spellCheck={false}
            />
          </label>
          <Button type="submit" variant="outline">
            Watch
          </Button>
        </form>
      </section>
      {job ? <JobWatch key={job} client={client} jobId={job} /> : null}
    </div>
  );
}

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

  function openJob(job: string) {
    setOpened((current) => [job, ...current.filter((entry) => entry !== job)].slice(0, OPENED_JOBS_LIMIT));
    go({ view: "jobs", job });
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
        onOpenJob={openJob}
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
          {view === "jobs" ? <JobsView client={client} job={location.job} onJob={openJob} /> : null}
          {view === "settings" ? (
            <SettingsSurface
              health={health}
              sessionExpiresAtUs={sessionExpiresAtUs}
              signingOut={signingOut}
              onSignOut={onSignOut}
            />
          ) : null}
          {!VIEW_AVAILABILITY[view].available ? <UnavailableSurface view={view} /> : null}
        </main>
      </div>
    </div>
  );
}
