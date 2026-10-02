"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { cn } from "@/lib/utils";
import { ChatView } from "../chat/chat-view";
import { JobInspector } from "../execution/job-inspector";
import { JobDraftSurface } from "../job/job-draft-surface";
import { OcgSidebar } from "../sidebar/ocg-sidebar";
import { OcgTopbar } from "../topbar/ocg-topbar";
import { ResourceLedgerSurface } from "../resource-ledger/resource-ledger-surface";
import { ControlCenterSurface } from "../control-center/control-center-surface";
import { JobExecutionSurface } from "../execution/job-execution-surface";
import { LogsSurface } from "../logs/logs-surface";
import { SettingsSurface } from "../settings/settings-surface";
import { CanonicalControlSurface } from "../canonical/canonical-control-surface";
import { useOcgControlUrl } from "../profile/control-url";
import type { ControlCenterView } from "../control-center/domain";
import { useOcgRuntime } from "../runtime/runtime-context";
import type { InspectorMode } from "../observability/inspector-state";
import { HomeSurface } from "../home/home-surface";
import { AttentionSurface } from "../attention/attention-surface";
import { createAttentionQueue } from "../attention/fixtures";
import { isUnresolved } from "../attention/domain";
import { selectAttentionItems } from "../attention/selectors";
import { useProject } from "../project/project-context";
import {
  selectProjectAttentionQueue,
  resolveSelectedSessionId,
  selectProjectSessions,
  selectProjectSnapshot,
} from "../project/selectors";
import { withProjectParam, type ProjectId } from "../project/domain";
import { dispatchComposerIntent, type ComposerIntent } from "../composer/domain";
import {
  createJobDraft,
  draftScopeKey,
  jobDraftReducer,
  toJobLaunchCommand,
  type HardBudgetSource,
  type JobDraft,
  type JobDraftAction,
  type JobDraftTextField,
} from "../job/draft-domain";
import type { JobLaunchResult } from "../runtime/runtime-types";
import { workspaceViewHref, type WorkspaceView } from "./view-domain";

export type { WorkspaceView } from "./view-domain";

function EmptyProjectWorkspace({
  activeProjectId,
  activeProjectName,
  projects,
  onProjectChange,
  onNewChat,
}: {
  activeProjectId: ProjectId;
  activeProjectName: string;
  projects: readonly { id: ProjectId; name: string; root?: string }[];
  onProjectChange: (id: ProjectId) => void;
  onNewChat: () => void;
}) {
  return (
    <main aria-label="Project workspace" className="flex min-h-0 flex-1 items-center justify-center overflow-auto p-6">
      <section className="w-full max-w-lg space-y-4 rounded-lg border border-border bg-background p-6">
        <div>
          <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">Project</p>
          <h1 className="mt-1 text-lg font-semibold">{activeProjectName}</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            This Project has no local chat session yet. Create one to start working in this Project.
          </p>
        </div>
        <button
          type="button"
          onClick={onNewChat}
          className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          New chat
        </button>
        {projects.length > 1 ? (
          <div className="border-t border-border pt-4">
            <p className="mb-2 text-xs font-medium text-muted-foreground">Registered Projects</p>
            <div className="flex flex-wrap gap-2">
              {projects.map((project) => (
                <button
                  key={project.id}
                  type="button"
                  aria-pressed={project.id === activeProjectId}
                  onClick={() => onProjectChange(project.id)}
                  className={cn(
                    "rounded-md border px-2.5 py-1.5 text-xs",
                    project.id === activeProjectId
                      ? "border-foreground/30 bg-muted font-medium"
                      : "border-border text-muted-foreground hover:bg-muted/60",
                  )}
                >
                  <span className="block">{project.name}</span>
                  <span className="block break-all text-[11px] font-normal text-muted-foreground">{project.root ?? project.id}</span>
                </button>
              ))}
            </div>
          </div>
        ) : null}
      </section>
    </main>
  );
}

export function RuntimeWorkspace({
  view = "chat",
  controlCenterView = "profiles",
}: {
  view?: WorkspaceView;
  controlCenterView?: ControlCenterView;
}) {
  const { snapshot: runtimeSnapshot, authority: runtimeAuthority, createSession, sendMessage, setActiveProfile, cancel, launchJob, sync } = useOcgRuntime();
  const {
    activeProjectId,
    activeProject,
    projects,
    setActiveProject,
    registerProjectSession,
    activeProjectSessionIds,
  } = useProject();
  const router = useRouter();
  const [activeSessionId, setActiveSessionId] = useState("design-pwa-shell");
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [inspectorMode, setInspectorMode] = useState<InspectorMode>("collapsed");
  const [mobileInspectorOpen, setMobileInspectorOpen] = useState(false);
  // Job drafts are held keyed by Project + session scope so one Project's
  // in-progress launch can never appear in another.
  const [jobDrafts, setJobDrafts] = useState<Record<string, JobDraft>>({});
  const [visibleDraftScopes, setVisibleDraftScopes] = useState<Record<string, boolean>>({});
  const [launchResults, setLaunchResults] = useState<Record<string, JobLaunchResult>>({});
  const activeDraftScopeRef = useRef<string | null>(null);

  // Project-scoped projection of the shared runtime snapshot. Every surface
  // below consumes this, so project switches cannot leak sessions, jobs,
  // executions, or ledger entries across projects.
  // The backend-backed OCG control endpoint. It is a projection/control surface:
  // the PWA never becomes the execution authority.
  const ocgControlUrl = useOcgControlUrl();

  const snapshot = useMemo(
    () => selectProjectSnapshot(runtimeSnapshot, activeProjectId, activeProjectSessionIds),
    [runtimeSnapshot, activeProjectId, activeProjectSessionIds],
  );

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setMobileNavOpen(false);
        setMobileInspectorOpen(false);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const activeSession = snapshot.sessions.find((session) => session.id === activeSessionId) ?? snapshot.sessions[0];
  const activeSessionKey = activeSession?.id;
  const activeWorkType = activeSession?.workType;
  const isLedger = view === "ledger";
  const isControlCenter = view === "control-center";
  const isJobExecution = view === "job-execution";
  const isLogs = view === "logs";
  const isSettings = view === "settings";
  const isCanonical = view === "canonical";
  const isHome = view === "home";
  const isAttention = view === "attention";

  // Project-scoped fixture queue plus the scoped snapshot. Snapshot-derived
  // items come from the scoped snapshot; fixture items are filtered by the
  // explicit projectId tag.
  const attentionQueue = useMemo(
    () => {
      const fixtureQueue = selectProjectAttentionQueue(createAttentionQueue(snapshot.scenario), activeProjectId);
      const runtimeItems = snapshot.attentionItems ?? [];
      const runtimeIds = new Set(runtimeItems.map((item) => item.id));
      return {
        approvals: [
          ...fixtureQueue.approvals.filter((item) => !runtimeIds.has(item.id)),
          ...runtimeItems.filter(isUnresolved),
        ],
        history: [
          ...fixtureQueue.history.filter((item) => !runtimeIds.has(item.id)),
          ...runtimeItems.filter((item) => !isUnresolved(item)),
        ],
      };
    },
    [snapshot.attentionItems, snapshot.scenario, activeProjectId],
  );

  // Quiet unresolved-attention badge for product navigation. Derived from
  // the same normalized selectors as the Attention surface itself.
  const attentionCount = useMemo(
    () => selectAttentionItems(snapshot, attentionQueue).filter(isUnresolved).length,
    [snapshot, attentionQueue],
  );

  const withProject = useCallback(
    (path: string, projectId: ProjectId = activeProjectId) => projectId ? withProjectParam(path, projectId) : path,
    [activeProjectId],
  );

  const handleNewChat = useCallback(async () => {
    if (!activeWorkType) return;
    const session = await createSession({ workType: activeWorkType });
    // Register before selecting so the new chat stays in the current project.
    registerProjectSession(session.id);
    setActiveSessionId(session.id);
    setMobileNavOpen(false);
  }, [activeWorkType, createSession, registerProjectSession]);

  const handleNewProjectChat = useCallback(async () => {
    const session = await createSession({ workType: "coding" });
    registerProjectSession(session.id, activeProjectId);
    setActiveSessionId(session.id);
  }, [activeProjectId, createSession, registerProjectSession]);

  /**
   * The one navigation path for every workspace control in the shell. Where a
   * view lives, and whether selecting it again closes it, is decided by the
   * view domain, so the topbar, sidebar and surfaces all agree.
   */
  const navigate = useCallback((target: WorkspaceView) => {
    setMobileNavOpen(false);
    setMobileInspectorOpen(false);
    const href = workspaceViewHref(view, target);
    if (href !== null) {
      // Update the view without replacing the page and its runtime providers,
      // including when the workspace was entered through a standalone route.
      window.history.pushState(null, "", withProject(`${href}&scenario=${encodeURIComponent(snapshot.scenario)}`));
    }
  }, [snapshot.scenario, view, withProject]);

  const selectSession = useCallback((id: string) => {
    setActiveSessionId(id);
    navigate("chat");
  }, [navigate]);

  // --- Job draft lifecycle (single pure reducer, scoped per Project) ----

  const activeDraftScope = activeSession ? draftScopeKey(activeProjectId, activeSession.id) : null;
  useEffect(() => {
    activeDraftScopeRef.current = activeDraftScope;
  }, [activeDraftScope]);
  const activeDraft = activeDraftScope && visibleDraftScopes[activeDraftScope]
    ? jobDrafts[activeDraftScope] ?? null
    : null;
  const activeLaunchResult = activeDraftScope ? launchResults[activeDraftScope] ?? null : null;

  const dispatchDraft = useCallback((scopeKey: string, action: JobDraftAction) => {
    setJobDrafts((current) => {
      const draft = current[scopeKey];
      if (!draft) return current;
      const next = jobDraftReducer(draft, action);
      if (next === draft) return current;
      return { ...current, [scopeKey]: next };
    });
  }, []);

  const handleCreateJobDraft = useCallback((seed?: string) => {
    if (!activeSession) return;
    const scopeKey = draftScopeKey(activeProjectId, activeSession.id);
    const existingDraft = jobDrafts[scopeKey];
    setJobDrafts((current) => {
      const existing = current[scopeKey];
      if (!existing) {
        return {
          ...current,
          [scopeKey]: createJobDraft({
            projectId: activeProjectId,
            sessionId: activeSession.id,
            objective: seed,
          }),
        };
      }
      if (existing.lifecycle === "launched") {
        // A completed draft is immutable. Opening the command again starts a
        // new local draft identity rather than rendering a settled card whose
        // Launch button would be a no-op.
        return {
          ...current,
          [scopeKey]: createJobDraft({
            projectId: activeProjectId,
            sessionId: activeSession.id,
            objective: seed,
            id: `${existing.id}:next`,
          }),
        };
      }
      if (seed && existing.objective.trim().length === 0) {
        return {
          ...current,
          [scopeKey]: jobDraftReducer(existing, { type: "update-field", field: "objective", value: seed }),
        };
      }
      return current;
    });
    if (existingDraft?.lifecycle === "launched") {
      setLaunchResults((current) => {
        if (!current[scopeKey]) return current;
        const next = { ...current };
        delete next[scopeKey];
        return next;
      });
    }
    setVisibleDraftScopes((current) => ({ ...current, [scopeKey]: true }));
    setMobileNavOpen(false);
    setMobileInspectorOpen(false);
  }, [activeProjectId, activeSession, jobDrafts]);

  const handleCloseJobDraft = useCallback(() => {
    if (!activeDraftScope) return;
    setVisibleDraftScopes((current) => ({ ...current, [activeDraftScope]: false }));
  }, [activeDraftScope]);

  const handleJobDraftFieldChange = useCallback((field: JobDraftTextField, value: string) => {
    if (!activeDraftScope) return;
    dispatchDraft(activeDraftScope, { type: "update-field", field, value });
    setLaunchResults((current) => {
      if (!current[activeDraftScope]) return current;
      const next = { ...current };
      delete next[activeDraftScope];
      return next;
    });
  }, [activeDraftScope, dispatchDraft]);

  const handleJobDraftBudgetChange = useCallback((micros: number | null, source: HardBudgetSource) => {
    if (!activeDraftScope) return;
    dispatchDraft(activeDraftScope, { type: "set-hard-budget", micros, source });
    setLaunchResults((current) => {
      if (!current[activeDraftScope]) return current;
      const next = { ...current };
      delete next[activeDraftScope];
      return next;
    });
  }, [activeDraftScope, dispatchDraft]);

  const handleJobDraftCommitmentChange = useCallback((value: number) => {
    if (!activeDraftScope) return;
    dispatchDraft(activeDraftScope, { type: "set-resource-commitment", value });
    setLaunchResults((current) => {
      if (!current[activeDraftScope]) return current;
      const next = { ...current };
      delete next[activeDraftScope];
      return next;
    });
  }, [activeDraftScope, dispatchDraft]);

  const handleJobDraftValidate = useCallback(() => {
    if (!activeDraftScope) return;
    dispatchDraft(activeDraftScope, { type: "validate" });
  }, [activeDraftScope, dispatchDraft]);

  const handleLaunchJob = useCallback(() => {
    if (!activeSession || !activeDraftScope) return;
    const draft = jobDrafts[activeDraftScope];
    if (!draft || draft.lifecycle === "launching" || draft.lifecycle === "launched") return;

    const launching = jobDraftReducer(draft, { type: "start-launch" });
    setJobDrafts((current) => ({ ...current, [activeDraftScope]: launching }));
    if (launching.lifecycle !== "launching") return;

    const command = toJobLaunchCommand(launching);
    if (!command) {
      setJobDrafts((current) => ({
        ...current,
        [activeDraftScope]: jobDraftReducer(launching, {
          type: "launch-failed",
          message: "Draft validation failed before launch.",
        }),
      }));
      return;
    }

    const settleLaunch = (result: JobLaunchResult) => {
      setJobDrafts((current) => {
        const currentDraft = current[activeDraftScope] ?? launching;
        const settled = result.outcome === "accepted"
          ? jobDraftReducer(currentDraft, {
              type: "launch-succeeded",
              jobId: result.jobId ?? command.draftId,
              message: result.message,
            })
          : jobDraftReducer(currentDraft, { type: "launch-failed", message: result.message });
        return { ...current, [activeDraftScope]: settled };
      });
      setLaunchResults((current) => ({ ...current, [activeDraftScope]: result }));

      // A Project/session switch can happen while an adapter command is in
      // flight. The old draft may settle in its own scope, but it must not
      // navigate the operator away from the newly selected Project.
      if (result.outcome === "accepted") {
        setVisibleDraftScopes((current) => ({ ...current, [activeDraftScope]: false }));
      }
      if (result.outcome === "accepted" && activeDraftScopeRef.current === activeDraftScope && view !== "job-execution") {
        // Keep the same scenario so the in-memory runtime instance (and its
        // freshly projected Job execution) survives the navigation.
        navigate("job-execution");
      }
    };

    void launchJob(command)
      .then(settleLaunch)
      .catch((error: unknown) => {
        const message = error instanceof Error && error.message
          ? error.message
          : "The runtime adapter failed while launching this Job.";
        settleLaunch({
          outcome: "failed",
          commandId: command.commandId,
          draftId: command.draftId,
          projectId: command.projectId,
          sessionId: command.sessionId,
          message,
          duplicate: false,
        });
      });
  }, [activeDraftScope, activeSession, launchJob, jobDrafts, navigate, view]);

  const handleComposerIntent = useCallback((intent: ComposerIntent) => {
    dispatchComposerIntent(intent, {
      chat: ({ text }) => {
        if (!activeProjectId) return;
        if (activeSessionKey) void sendMessage(activeSessionKey, { content: text, projectId: activeProjectId });
      },
      "job.create": ({ seed }) => handleCreateJobDraft(seed),
    });
  }, [activeProjectId, activeSessionKey, handleCreateJobDraft, sendMessage]);

  const handleSelectProfile = useCallback((profileId: string) => {
    void setActiveProfile(profileId);
  }, [setActiveProfile]);

  // Switching projects closes mobile overlays, cancels any in-flight stream,
  // persists the new selection, and keeps the URL in sync so a refresh keeps
  // the operator in the same project.
  const handleProjectChange = useCallback((id: ProjectId) => {
    setMobileNavOpen(false);
    setMobileInspectorOpen(false);
    if (activeSessionKey) void cancel(activeSessionKey);
    // Reset the active session to one the target project owns so a stale
    // selection cannot survive the switch.
    const nextSessionId = resolveSelectedSessionId(
      null,
      selectProjectSessions(runtimeSnapshot.sessions, id),
    );
    setActiveSessionId(nextSessionId ?? "");
    setActiveProject(id);
    if (typeof window !== "undefined") {
      const url = new URL(window.location.href);
      url.searchParams.set("project", id);
      window.history.replaceState(null, "", `${url.pathname}${url.search}`);
    }
  }, [activeSessionKey, cancel, runtimeSnapshot.sessions, setActiveProject]);

  const messages = activeSession ? snapshot.messagesBySession[activeSession.id] ?? [] : [];
  const execution = activeSession ? snapshot.executionBySession[activeSession.id] ?? null : null;
  const accounting = activeSession ? snapshot.accountingBySession[activeSession.id] ?? null : null;
  const observability = activeSession ? snapshot.observabilityBySession[activeSession.id] : undefined;
  const inspectorOpen = inspectorMode !== "collapsed";

  const sidebar = (
    <OcgSidebar
      sessions={snapshot.sessions}
      activeSessionId={activeSession?.id ?? ""}
      collapsed={sidebarCollapsed}
      onToggle={() => setSidebarCollapsed((value) => !value)}
      onSelect={selectSession}
      onNewChat={activeSession ? handleNewChat : handleNewProjectChat}
      runtimeStatus={snapshot.status}
      runtimeAuthority={runtimeAuthority}
      projects={projects}
      activeProjectId={activeProjectId}
      onProjectChange={handleProjectChange}
      activeView={view}
      attentionCount={attentionCount}
      onNavigate={navigate}
    />
  );

  return (
    <div className="flex h-dvh overflow-hidden bg-background text-foreground">
      <aside
        aria-label="OCG navigation"
        className={cn(
          "hidden shrink-0 overflow-hidden border-r border-border bg-sidebar transition-[width] duration-200 ease-out lg:block",
          sidebarCollapsed ? "w-16" : "w-[272px]",
        )}
      >
        {sidebar}
      </aside>

      <div
        className={cn("fixed inset-0 z-50 lg:hidden", !mobileNavOpen && "pointer-events-none")}
        aria-hidden={!mobileNavOpen}
      >
        <div
          onClick={() => setMobileNavOpen(false)}
          className={cn(
            "absolute inset-0 bg-black/40 transition-opacity duration-200",
            mobileNavOpen ? "opacity-100" : "opacity-0",
          )}
        />
        <aside
          aria-label="OCG navigation"
          className={cn(
            "absolute inset-y-0 left-0 w-[272px] border-r border-border bg-sidebar transition-transform duration-200 ease-out",
            mobileNavOpen ? "translate-x-0" : "-translate-x-full",
          )}
        >
          <OcgSidebar
            sessions={snapshot.sessions}
            activeSessionId={activeSession?.id ?? ""}
            collapsed={false}
            onToggle={() => setMobileNavOpen(false)}
            onSelect={selectSession}
            onNewChat={activeSession ? handleNewChat : handleNewProjectChat}
            runtimeStatus={snapshot.status}
            runtimeAuthority={runtimeAuthority}
            projects={projects}
            activeProjectId={activeProjectId}
            onProjectChange={handleProjectChange}
            activeView={view}
            attentionCount={attentionCount}
            onNavigate={navigate}
          />
        </aside>
      </div>

      <div className="flex min-w-0 flex-1 flex-col">
        <OcgTopbar
          session={activeSession}
          projectName={activeProject.name}
          sidebarCollapsed={sidebarCollapsed}
          inspectorOpen={inspectorOpen}
          inspectorControls={Boolean(activeSession) && view === "chat"}
          activeView={view}
          onToggleSidebar={() => setSidebarCollapsed((value) => !value)}
          onToggleInspector={() => setInspectorMode((value) => value === "collapsed" ? "docked" : "collapsed")}
          onOpenMobileSidebar={() => setMobileNavOpen(true)}
          onOpenMobileInspector={() => setMobileInspectorOpen(true)}
          runtimeStatus={snapshot.status}
          runtimeAuthority={runtimeAuthority}
          syncStatus={sync?.status ?? null}
        />
        {isLedger ? (
          <main aria-label="Resource ledger" className="flex min-h-0 flex-1 overflow-hidden">
            <ResourceLedgerSurface key={activeProjectId} ledger={snapshot.resourceLedger} />
          </main>
        ) : isControlCenter ? (
          <main aria-label="Control Center" className="flex min-h-0 flex-1 overflow-hidden">
            <ControlCenterSurface
              key={`${activeProjectId}:${controlCenterView}`}
              bootstrap={snapshot.bootstrap}
              ledger={snapshot.resourceLedger}
              initialView={controlCenterView}
              onSelectProfile={handleSelectProfile}
            />
          </main>
        ) : isJobExecution ? (
          <main aria-label="Job Execution" className="flex min-h-0 flex-1 overflow-hidden">
            {execution ? (
              <JobExecutionSurface
                key={`${activeProjectId}:${activeSession?.id}`}
                execution={execution}
                onOpenInspector={() => navigate("chat")}
              />
            ) : (
              <div className="flex flex-1 items-center justify-center p-6 text-[12px] text-muted-foreground">No Job execution is available.</div>
            )}
          </main>
        ) : isLogs ? (
          <main aria-label="Logs and diagnostics" className="flex min-h-0 flex-1 overflow-hidden">
            <LogsSurface
              key={`${activeProjectId}:${snapshot.scenario}`}
              snapshot={snapshot}
              projectId={activeProjectId}
            />
          </main>
        ) : isSettings ? (
          <main aria-label="Settings" className="flex min-h-0 flex-1 overflow-hidden">
            <SettingsSurface key={activeProjectId} snapshot={snapshot} />
          </main>
        ) : isCanonical && ocgControlUrl ? (
          <main aria-label="OCG control" className="flex min-h-0 flex-1 overflow-hidden">
            <CanonicalControlSurface baseUrl={ocgControlUrl} fetchImpl={fetch} />
          </main>
        ) : isCanonical ? (
          <main aria-label="OCG control" className="flex min-h-0 flex-1 items-center justify-center p-6">
            <p className="max-w-md text-center text-[12px] text-muted-foreground">
              No OCG control endpoint is configured. Start one with{" "}
              <code className="rounded bg-muted px-1 py-0.5 text-[11px]">ocg serve --addr 127.0.0.1:8710</code> and
              reload.
            </p>
          </main>
        ) : isHome ? (
          <main aria-label="Workspace home" className="flex min-h-0 flex-1 overflow-hidden">
            <HomeSurface
              key={activeProjectId}
              snapshot={snapshot}
              onNavigate={navigate}
              onSelectSession={selectSession}
            />
          </main>
        ) : isAttention ? (
          <main aria-label="Attention and approvals" className="flex min-h-0 flex-1 overflow-hidden">
            <AttentionSurface
              key={activeProjectId}
              snapshot={snapshot}
              queue={attentionQueue}
              onNavigate={navigate}
            />
          </main>
        ) : !activeProjectId ? (
          <main aria-label="No project selected" className="flex min-h-0 flex-1 items-center justify-center">
            <div className="flex flex-col items-center gap-3 text-center">
              <p className="text-[14px] font-medium">No project selected</p>
              <p className="max-w-sm text-[12px] text-muted-foreground">
                Add a Project to start chatting. Projects define your working directory for OCG operations.
              </p>
              <button
                type="button"
                className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
                onClick={() => router.push("/onboarding?scenario=local-first-run")}
              >
                Add a Project
              </button>
            </div>
          </main>
        ) : !activeSession ? (
          <EmptyProjectWorkspace
            activeProjectId={activeProjectId}
            activeProjectName={activeProject.name}
            projects={projects}
            onProjectChange={handleProjectChange}
            onNewChat={() => void handleNewProjectChat()}
          />
        ) : (
          <main aria-label="OCG workspace" className="flex min-h-0 flex-1">
            <div className="flex min-w-0 flex-1 flex-col">
              <ChatView
                key={`${activeProjectId}:${activeSession.id}`}
                session={activeSession}
                messages={messages}
                runtimeStatus={snapshot.status}
                onComposerIntent={handleComposerIntent}
                composerSurface={activeDraft ? (
                  <JobDraftSurface
                    project={activeProject}
                    draft={activeDraft}
                    launchResult={activeLaunchResult}
                    onFieldChange={handleJobDraftFieldChange}
                    onBudgetChange={handleJobDraftBudgetChange}
                    onCommitmentChange={handleJobDraftCommitmentChange}
                    onValidate={handleJobDraftValidate}
                    onLaunch={handleLaunchJob}
                    onClose={handleCloseJobDraft}
                  />
                ) : null}
                composerSurfaceKey={activeDraft?.id}
              />
            </div>

            <aside
              aria-label="Job inspector"
              className={cn(
                "hidden shrink-0 overflow-hidden border-border bg-background transition-[width,opacity] duration-200 ease-out lg:block",
                 inspectorMode === "expanded" ? "w-[min(640px,42vw)] border-l opacity-100" : inspectorMode === "docked" ? "w-[min(360px,28vw)] border-l opacity-100" : "w-0 border-l-0 opacity-0",
              )}
            >
              <div className={cn("h-full", inspectorMode === "expanded" ? "w-[min(640px,42vw)]" : "w-[min(360px,28vw)]")}>
                {execution && inspectorOpen && (
                  <JobInspector
                    execution={execution}
                    accounting={accounting}
                    observability={observability}
                    mode={inspectorMode}
                    onModeChange={setInspectorMode}
                    onClose={() => setInspectorMode("collapsed")}
                    onOpenJobExecution={() => navigate("job-execution")}
                  />
                )}
              </div>
            </aside>
          </main>
        )}
      </div>

      {!isLedger && !isControlCenter && !isJobExecution && !isLogs && !isSettings && !isHome && !isAttention && (
        <div
          className={cn("fixed inset-0 z-50 lg:hidden", !mobileInspectorOpen && "pointer-events-none")}
          aria-hidden={!mobileInspectorOpen}
        >
          <div
            onClick={() => setMobileInspectorOpen(false)}
            className={cn(
              "absolute inset-0 bg-black/40 transition-opacity duration-200",
              mobileInspectorOpen ? "opacity-100" : "opacity-0",
            )}
          />
          <aside
            aria-label="Job inspector"
            className={cn(
               "absolute inset-y-0 right-0 w-full max-w-none border-l border-border bg-background transition-transform duration-200 ease-out sm:w-[640px] sm:max-w-[85vw]",
               mobileInspectorOpen ? "translate-x-0" : "translate-x-full",
             )}
            >
            {mobileInspectorOpen && execution && (
              <JobInspector
                execution={execution}
                accounting={accounting}
                observability={observability}
                mode="expanded"
                onClose={() => setMobileInspectorOpen(false)}
                onOpenJobExecution={() => navigate("job-execution")}
              />
            )}
          </aside>
        </div>
      )}
    </div>
  );
}
