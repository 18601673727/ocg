"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { cn } from "@/lib/utils";
import { ChatView } from "../chat/chat-view";
import { UsageSurface } from "../usage/usage-surface";
import { ConversationInspector } from "../usage/conversation-inspector";
import { useConversationUsage, useJobUsage } from "../usage/use-usage";
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
import { retryContent } from "../chat/retry";
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
import { useI18n } from "../i18n";

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
  const { t } = useI18n();
  return (
    <main aria-label="Project workspace" className="flex min-h-0 flex-1 items-center justify-center overflow-auto p-6">
      <section className="w-full max-w-lg space-y-4 rounded-lg border border-border bg-background p-6">
        <div>
          <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">{t("project.emptyTitle")}</p>
          <h1 className="mt-1 text-lg font-semibold">{activeProjectName}</h1>
          <p className="mt-2 text-sm text-muted-foreground">
            {t("project.emptyBody")}
          </p>
        </div>
        <button
          type="button"
          onClick={onNewChat}
          className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        >
          {t("sidebar.newChat")}
        </button>
        {projects.length > 1 ? (
          <div className="border-t border-border pt-4">
            <p className="mb-2 text-xs font-medium text-muted-foreground">{t("project.registered")}</p>
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

function NoExecutionNotice() {
  const { t } = useI18n();
  return (
    <div className="flex flex-1 items-center justify-center p-6 text-[12px] text-muted-foreground">{t("execution.noExecution")}</div>
  );
}

function NoEndpointNotice() {
  const { t } = useI18n();
  return (
    <p className="max-w-md text-center text-[12px] text-muted-foreground">
      {t("canonical.noEndpoint")}
    </p>
  );
}

function NoProjectNotice({ onAddProject }: { onAddProject: () => void }) {
  const { t } = useI18n();
  return (
    <div className="flex flex-col items-center gap-3 text-center">
      <p className="text-[14px] font-medium">{t("project.noSelectionTitle")}</p>
      <p className="max-w-sm text-[12px] text-muted-foreground">
        {t("project.noSelectionBody")}
      </p>
      <button
        type="button"
        className="rounded-md bg-primary px-3 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
        onClick={onAddProject}
      >
        {t("project.addProject")}
      </button>
    </div>
  );
}

export function RuntimeWorkspace({
  view = "chat",
  controlCenterView = "profiles",
}: {
  view?: WorkspaceView;
  controlCenterView?: ControlCenterView;
}) {
  const { snapshot: runtimeSnapshot, authority: runtimeAuthority, createSession, deleteSession, sendMessage, retryMessage, cancel, client, setActiveProfile, launchJob, sync } = useOcgRuntime();
  const {
    activeProjectId,
    activeProject,
    projects,
    setActiveProject,
    registerProjectSession,
    activeProjectSessionIds,
    historyStatus,
    historyError,
    retryHistory,
  } = useProject();
  const { t } = useI18n();
  const router = useRouter();
  const searchParams = useSearchParams();
  const [activeSessionId, setActiveSessionId] = useState(() => searchParams.get("session") ?? "");
  const selectedSessions = useRef<Record<string, string>>({});
  const rememberSession = useCallback((id: string) => {
    selectedSessions.current[activeProjectId] = id;
    const url = new URL(window.location.href);
    if (id) url.searchParams.set("session", id);
    else url.searchParams.delete("session");
    window.history.replaceState(null, "", `${url.pathname}${url.search}`);
  }, [activeProjectId]);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [inspectorMode, setInspectorMode] = useState<InspectorMode>(runtimeAuthority === "canonical" ? "docked" : "collapsed");
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
  const busySessionIds = snapshot.sessions.filter(session => (snapshot.messagesBySession[session.id] ?? []).some(message =>
    message.role === "assistant" && (message.status === "pending" || message.status === "streaming"),
  )).map(session => session.id);
  const handleDeleteChat = useCallback(async (id: string) => {
    const session = snapshot.sessions.find(item => item.id === id);
    if (!session) throw new Error(t("sidebar.deleteMissing"));
    await deleteSession(id);
    const remaining = selectProjectSnapshot(client.getSnapshot(), activeProjectId, activeProjectSessionIds).sessions;
    if (activeSessionKey === id) {
      const nextId = remaining[0]?.id ?? "";
      setActiveSessionId(nextId);
      rememberSession(nextId);
    }
    const scope = draftScopeKey(activeProjectId, id);
    const withoutDraft = <T,>(current: Record<string, T>) => Object.fromEntries(
      Object.entries(current).filter(([key]) => key !== scope),
    );
    setJobDrafts(withoutDraft);
    setVisibleDraftScopes(withoutDraft);
    setLaunchResults(withoutDraft);
  }, [activeProjectId, activeProjectSessionIds, activeSessionKey, client, deleteSession, rememberSession, snapshot.sessions, t]);
  useEffect(() => {
    if (runtimeAuthority === "canonical" && historyStatus === "ready" && activeSessionKey) rememberSession(activeSessionKey);
  }, [activeSessionKey, historyStatus, rememberSession, runtimeAuthority]);
  const isUsage = view === "usage";
  const isLedger = view === "ledger" && runtimeAuthority === "mock";
  const isControlCenter = view === "control-center" && runtimeAuthority === "mock";
  const isJobExecution = view === "job-execution";
  const isLogs = view === "logs" && runtimeAuthority === "mock";
  const isSettings = view === "settings" || (runtimeAuthority === "canonical" && ["control-center", "ledger", "logs"].includes(view));
  const isCanonical = view === "canonical";
  const isHome = view === "home";
  const isAttention = view === "attention";

  // Project-scoped fixture queue plus the scoped snapshot. Snapshot-derived
  // items come from the scoped snapshot; fixture items are filtered by the
  // explicit projectId tag.
  const attentionQueue = useMemo(
    () => {
      const fixtureQueue = runtimeAuthority === "mock" ? selectProjectAttentionQueue(createAttentionQueue(snapshot.scenario), activeProjectId) : { approvals: [], history: [] };
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
    [snapshot.attentionItems, snapshot.scenario, activeProjectId, runtimeAuthority],
  );

  // Quiet unresolved-attention badge for product navigation. Derived from
  // the same normalized selectors as the Attention surface itself.
  const attentionCount = useMemo(
    () => selectAttentionItems(snapshot, attentionQueue).filter(isUnresolved).length,
    [snapshot, attentionQueue],
  );

  const withProject = useCallback(
    (path: string, projectId: ProjectId = activeProjectId) => {
      const scoped = projectId ? withProjectParam(path, projectId) : path;
      return activeSessionKey ? `${scoped}${scoped.includes("?") ? "&" : "?"}session=${encodeURIComponent(activeSessionKey)}` : scoped;
    },
    [activeProjectId, activeSessionKey],
  );

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
      const url = new URL(withProject(`${href}&scenario=${encodeURIComponent(snapshot.scenario)}`), window.location.origin);
      if (target !== "job-execution") url.searchParams.delete("job");
      if (new URLSearchParams(window.location.search).get("demo") === "1") url.searchParams.set("demo", "1");
      window.history.pushState(null, "", url.pathname + url.search);
    }
  }, [snapshot.scenario, view, withProject]);

  const openJob = useCallback((jobId: string) => {
    setMobileNavOpen(false);
    setMobileInspectorOpen(false);
    const href = workspaceViewHref(view, "job-execution") ?? workspaceViewHref("chat", "job-execution");
    if (!href) return;
    const url = new URL(withProject(`${href}&scenario=${encodeURIComponent(snapshot.scenario)}`), window.location.origin);
    url.searchParams.set("job", jobId);
    if (new URLSearchParams(window.location.search).get("demo") === "1") url.searchParams.set("demo", "1");
    window.history.pushState(null, "", url.pathname + url.search);
  }, [snapshot.scenario, view, withProject]);

  const handleNewChat = useCallback(async () => {
    if (!activeWorkType) return;
    const session = await createSession({ workType: activeWorkType, projectId: activeProjectId });
    // Register before selecting so the new chat stays in the current project.
    registerProjectSession(session.id);
    setActiveSessionId(session.id);
    navigate("chat");
    rememberSession(session.id);
  }, [activeProjectId, activeWorkType, createSession, registerProjectSession, rememberSession, navigate]);

  const handleNewProjectChat = useCallback(async () => {
    const session = await createSession({ workType: "coding", projectId: activeProjectId });
    registerProjectSession(session.id, activeProjectId);
    setActiveSessionId(session.id);
    navigate("chat");
    rememberSession(session.id);
  }, [activeProjectId, createSession, registerProjectSession, rememberSession, navigate]);

  const selectSession = useCallback((id: string) => {
    setActiveSessionId(id);
    navigate("chat");
    rememberSession(id);
  }, [navigate, rememberSession]);

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

  const handleComposerIntent = useCallback(async (intent: ComposerIntent) => {
    if (intent.kind === "chat") {
      if (!activeProjectId || !activeSessionKey) throw new Error("Select a conversation and project first.");
      await sendMessage(activeSessionKey, { content: intent.text, images: intent.images, projectId: activeProjectId, selection: intent.selection, mode: intent.mode });
      return;
    }
    dispatchComposerIntent(intent, {
      chat: () => {},
      "job.create": ({ seed }) => handleCreateJobDraft(seed),
    });
  }, [activeProjectId, activeSessionKey, handleCreateJobDraft, sendMessage]);

  const handleSelectProfile = useCallback((profileId: string) => {
    void setActiveProfile(profileId);
  }, [setActiveProfile]);

  // Switching projects changes only the visible context and closes overlays,
  // persists the new selection, and keeps the URL in sync so a refresh keeps
  // the operator in the same project.
  const handleProjectChange = useCallback((id: ProjectId) => {
    setMobileNavOpen(false);
    setMobileInspectorOpen(false);
    // Reset the active session to one the target project owns so a stale
    // selection cannot survive the switch.
    const nextSessionId = resolveSelectedSessionId(
      selectedSessions.current[id],
      selectProjectSessions(runtimeSnapshot.sessions, id, runtimeSnapshot.sessions.filter((session) => session.projectId === id).map((session) => session.id)),
    );
    setActiveSessionId(nextSessionId ?? "");
    setActiveProject(id);
    if (typeof window !== "undefined") {
      const url = new URL(window.location.href);
      url.searchParams.set("project", id);
      if (nextSessionId) url.searchParams.set("session", nextSessionId);
      else url.searchParams.delete("session");
      url.searchParams.delete("job");
      window.history.replaceState(null, "", `${url.pathname}${url.search}`);
    }
  }, [runtimeSnapshot.sessions, setActiveProject]);

  const messages = activeSession ? snapshot.messagesBySession[activeSession.id] ?? [] : [];
  const execution = activeSession ? snapshot.executionBySession[activeSession.id] ?? null : null;
  const projectExecutions = Object.values(snapshot.executionsByProject?.[activeProjectId] ?? {})
    .sort((left, right) => right.updatedAt - left.updatedAt || left.jobId.localeCompare(right.jobId));
  const requestedJobId = searchParams.get("job");
  const jobExecution = projectExecutions.find((item) => item.jobId === requestedJobId)
    ?? projectExecutions.find((item) => item.jobId === execution?.jobId)
    ?? projectExecutions[0]
    ?? null;
  const selectedJobSessionId = jobExecution
    ? Object.entries(snapshot.executionBySession).find(([, item]) => item?.jobId === jobExecution.jobId)?.[0]
    : undefined;
  const lastAssistant = messages.findLast(message => message.role === "assistant");
  const retryableMessage = lastAssistant && retryContent(messages, lastAssistant.id) ? lastAssistant.id : null;
  const usageRefreshKey = messages.filter(message => message.role === "assistant" && ["completed", "failed", "cancelled"].includes(message.status)).map(message => `${message.id}:${message.status}`).join("|");
  const usageBaseUrl = runtimeAuthority === "canonical" && view === "chat" ? ocgControlUrl : null;
  const conversationUsage = useConversationUsage(usageBaseUrl, activeProjectId, activeSession?.sessionId ?? activeSession?.id ?? "", usageRefreshKey, messages.some(message => !message.optimistic));
  const jobUsage = useJobUsage(usageBaseUrl, activeProjectId, execution?.jobId ?? "", `${usageRefreshKey}:${execution?.state ?? ""}`);
  const recordedAccounting = activeSession ? snapshot.accountingBySession[activeSession.id] ?? null : null;
  const accounting = jobUsage.data ? { ceiling: recordedAccounting?.ceiling ?? null, consumption: jobUsage.data.totals.cost } : recordedAccounting;
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
      onDelete={handleDeleteChat}
      busySessionIds={busySessionIds}
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
    <div className="flex h-dvh overflow-hidden bg-background pt-[env(safe-area-inset-top)] pr-[env(safe-area-inset-right)] pl-[env(safe-area-inset-left)] text-foreground">
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
            "absolute inset-y-0 left-0 w-[272px] border-r border-border bg-sidebar pt-[env(safe-area-inset-top)] pb-[env(safe-area-inset-bottom)] transition-transform duration-200 ease-out",
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
            onDelete={handleDeleteChat}
            busySessionIds={busySessionIds}
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
          projectName={activeProjectId ? activeProject.name : t("project.noProject")}
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
        {isUsage ? (
          <main aria-label={t("nav.usage")} className="flex min-h-0 flex-1 overflow-hidden">
            <UsageSurface key={activeProjectId} baseUrl={runtimeAuthority === "canonical" ? ocgControlUrl : null} projectId={activeProjectId} onSelectSession={id => {
              const session = snapshot.sessions.find(item => item.sessionId === id || item.id === id);
              if (session) selectSession(session.id);
            }} />
          </main>
        ) : isLedger ? (
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
            {jobExecution ? (
              <JobExecutionSurface
                key={`${activeProjectId}:${jobExecution.jobId}`}
                execution={jobExecution}
                executions={projectExecutions}
                onSelectJob={openJob}
                onOpenInspector={selectedJobSessionId ? () => {
                  setActiveSessionId(selectedJobSessionId);
                  rememberSession(selectedJobSessionId);
                  navigate("chat");
                } : undefined}
                onReexecute={activeSession && selectedJobSessionId === activeSession.id && retryableMessage && snapshot.status.state === "connected" ? () => retryMessage(activeSession.id, retryableMessage) : undefined}
              />
            ) : (
              <NoExecutionNotice />
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
            <NoEndpointNotice />
          </main>
        ) : isHome ? (
          <main aria-label="Workspace home" className="flex min-h-0 flex-1 overflow-hidden">
            <HomeSurface
              key={activeProjectId}
              snapshot={snapshot}
              onNavigate={navigate}
              onSelectJob={openJob}
              onSelectSession={selectSession}
            />
          </main>
        ) : isAttention ? (
          <main aria-label="Attention and approvals" className="flex min-h-0 flex-1 overflow-hidden">
            <AttentionSurface
              key={activeProjectId}
              snapshot={snapshot}
              queue={attentionQueue}
              onSelectSession={selectSession}
              onSelectJob={openJob}
              onNavigate={navigate}
            />
          </main>
        ) : !activeProjectId ? (
          <main aria-label="No project selected" className="flex min-h-0 flex-1 items-center justify-center">
            <NoProjectNotice onAddProject={() => router.push("/onboarding?scenario=local-first-run")} />
          </main>
        ) : historyStatus !== "ready" ? (
          <main aria-label="Chat history" className="flex min-h-0 flex-1 items-center justify-center p-6">
            {historyStatus === "loading" ? <p>{t("chat.historyLoading")}</p> : (
              <div role="alert" className="space-y-3 text-center">
                <p>{t("chat.historyFailed", { error: historyError ?? "" })}</p>
                <button type="button" onClick={retryHistory} className="rounded-md border px-3 py-2">{t("common.retry")}</button>
              </div>
            )}
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
                onCancel={() => cancel(activeSession.id)}
                queueState={runtimeSnapshot.chatQueues?.[activeSession.id]}
                onRemoveQueued={id => client.removeQueuedMessage?.(activeSession.id, id)}
                onResumeQueue={() => client.resumeQueue?.(activeSession.id)}
                onRetryMessage={(messageId) => retryMessage(activeSession.id, messageId)}
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
              aria-label={t("usage.inspector")}
              className={cn(
                "hidden shrink-0 overflow-hidden border-border bg-background transition-[width,opacity] duration-200 ease-out lg:block",
                 inspectorMode === "expanded" ? "w-[min(640px,42vw)] border-l opacity-100" : inspectorMode === "docked" ? "w-[min(360px,28vw)] border-l opacity-100" : "w-0 border-l-0 opacity-0",
              )}
            >
              <div className={cn("h-full", inspectorMode === "expanded" ? "w-[min(640px,42vw)]" : "w-[min(360px,28vw)]")}>
                {inspectorOpen && runtimeAuthority === "canonical" ? (
                  <ConversationInspector key={`${activeProjectId}:${activeSession.id}`} usage={conversationUsage} execution={execution} accounting={accounting} observability={observability} mode={inspectorMode} onModeChange={setInspectorMode} onClose={() => setInspectorMode("collapsed")} onOpenJobExecution={() => navigate("job-execution")} />
                ) : execution && inspectorOpen ? (
                  <JobInspector
                    execution={execution}
                    accounting={accounting}
                    observability={observability}
                    mode={inspectorMode}
                    onModeChange={setInspectorMode}
                    onClose={() => setInspectorMode("collapsed")}
                    onOpenJobExecution={() => navigate("job-execution")}
                  />
                ) : null}
              </div>
            </aside>
          </main>
        )}
      </div>

      {!isUsage && !isLedger && !isControlCenter && !isJobExecution && !isLogs && !isSettings && !isHome && !isAttention && (
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
            aria-label={t("usage.inspector")}
            className={cn(
               "absolute inset-y-0 right-0 w-full max-w-none border-l border-border bg-background pt-[env(safe-area-inset-top)] pb-[env(safe-area-inset-bottom)] transition-transform duration-200 ease-out sm:w-[640px] sm:max-w-[85vw]",
               mobileInspectorOpen ? "translate-x-0" : "translate-x-full",
             )}
            >
            {mobileInspectorOpen && runtimeAuthority === "canonical" ? (
              <ConversationInspector key={`${activeProjectId}:${activeSession?.id ?? ""}`} usage={conversationUsage} execution={execution} accounting={accounting} observability={observability} mode="expanded" onClose={() => setMobileInspectorOpen(false)} onOpenJobExecution={() => navigate("job-execution")} />
            ) : mobileInspectorOpen && execution ? (
              <JobInspector
                execution={execution}
                accounting={accounting}
                observability={observability}
                mode="expanded"
                onClose={() => setMobileInspectorOpen(false)}
                onOpenJobExecution={() => navigate("job-execution")}
              />
            ) : null}
          </aside>
        </div>
      )}
    </div>
  );
}
