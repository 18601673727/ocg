"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { cn } from "@/lib/utils";
import { ChatView } from "../chat/chat-view";
import { MissionView } from "../mission/mission-view";
import { MissionDraftSurface } from "../mission/mission-draft-surface";
import { OcgSidebar } from "../sidebar/ocg-sidebar";
import { OcgTopbar } from "../topbar/ocg-topbar";
import { ResourceLedgerSurface } from "../resource-ledger/resource-ledger-surface";
import { ControlCenterSurface } from "../control-center/control-center-surface";
import { MissionControlSurface } from "../mission-control/mission-control-surface";
import { LogsSurface } from "../logs/logs-surface";
import { SettingsSurface } from "../settings/settings-surface";
import { CanonicalControlSurface } from "../canonical/canonical-control-surface";
import { useOcgControlUrl } from "../profile/control-url";
import type { ControlCenterView } from "../control-center/domain";
import { OcgRuntimeProvider, useOcgRuntime } from "../runtime/runtime-context";
import type { ScenarioId } from "../runtime/runtime-types";
import type { InspectorMode } from "../observability/inspector-state";
import { HomeSurface } from "../home/home-surface";
import { AttentionSurface } from "../attention/attention-surface";
import { createAttentionQueue } from "../attention/fixtures";
import { isUnresolved } from "../attention/domain";
import { selectAttentionItems } from "../attention/selectors";
import { ProjectProvider, useProject } from "../project/project-context";
import {
  selectProjectAttentionQueue,
  resolveSelectedSessionId,
  selectProjectSessions,
  selectProjectSnapshot,
} from "../project/selectors";
import { withProjectParam, type ProjectId } from "../project/domain";
import { dispatchComposerIntent, type ComposerIntent } from "../composer/domain";
import {
  createMissionDraft,
  draftScopeKey,
  missionDraftReducer,
  toMissionLaunchCommand,
  type HardBudgetSource,
  type MissionDraft,
  type MissionDraftAction,
  type MissionDraftTextField,
} from "../mission/draft-domain";
import type { MissionLaunchResult } from "../runtime/runtime-types";
import { workspaceViewHref, type WorkspaceView } from "./view-domain";

export type { WorkspaceView } from "./view-domain";

export function AppShell({
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
        <RuntimeWorkspace view={view} controlCenterView={controlCenterView} />
      </ProjectProvider>
    </OcgRuntimeProvider>
  );
}

export function RuntimeWorkspace({
  view = "chat",
  controlCenterView = "profiles",
}: {
  view?: WorkspaceView;
  controlCenterView?: ControlCenterView;
}) {
  const { snapshot: runtimeSnapshot, createSession, sendMessage, setActiveProfile, cancel, launchMission, sync } = useOcgRuntime();
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
  const [missionMode, setMissionMode] = useState<InspectorMode>("docked");
  const [mobileMissionOpen, setMobileMissionOpen] = useState(false);
  // Mission drafts are held keyed by Project + session scope so one Project's
  // in-progress launch can never appear in another.
  const [missionDrafts, setMissionDrafts] = useState<Record<string, MissionDraft>>({});
  const [visibleDraftScopes, setVisibleDraftScopes] = useState<Record<string, boolean>>({});
  const [launchResults, setLaunchResults] = useState<Record<string, MissionLaunchResult>>({});
  const activeDraftScopeRef = useRef<string | null>(null);

  // Project-scoped projection of the shared runtime snapshot. Every surface
  // below consumes this, so project switches cannot leak sessions, missions,
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
        setMobileMissionOpen(false);
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
  const isMissionControl = view === "mission-control";
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
    (path: string, projectId: ProjectId = activeProjectId) => withProjectParam(path, projectId),
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

  /**
   * The one navigation path for every workspace control in the shell. Where a
   * view lives, and whether selecting it again closes it, is decided by the
   * view domain, so the topbar, sidebar and surfaces all agree.
   */
  const navigate = useCallback((target: WorkspaceView) => {
    setMobileNavOpen(false);
    setMobileMissionOpen(false);
    const href = workspaceViewHref(view, target);
    if (href !== null) router.push(withProject(href));
  }, [router, view, withProject]);

  const selectSession = useCallback((id: string) => {
    setActiveSessionId(id);
    navigate("chat");
  }, [navigate]);

  // --- Mission draft lifecycle (single pure reducer, scoped per Project) ----

  const activeDraftScope = activeSession ? draftScopeKey(activeProjectId, activeSession.id) : null;
  useEffect(() => {
    activeDraftScopeRef.current = activeDraftScope;
  }, [activeDraftScope]);
  const activeDraft = activeDraftScope && visibleDraftScopes[activeDraftScope]
    ? missionDrafts[activeDraftScope] ?? null
    : null;
  const activeLaunchResult = activeDraftScope ? launchResults[activeDraftScope] ?? null : null;

  const dispatchDraft = useCallback((scopeKey: string, action: MissionDraftAction) => {
    setMissionDrafts((current) => {
      const draft = current[scopeKey];
      if (!draft) return current;
      const next = missionDraftReducer(draft, action);
      if (next === draft) return current;
      return { ...current, [scopeKey]: next };
    });
  }, []);

  const handleCreateMissionDraft = useCallback((seed?: string) => {
    if (!activeSession) return;
    const scopeKey = draftScopeKey(activeProjectId, activeSession.id);
    const existingDraft = missionDrafts[scopeKey];
    setMissionDrafts((current) => {
      const existing = current[scopeKey];
      if (!existing) {
        return {
          ...current,
          [scopeKey]: createMissionDraft({
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
          [scopeKey]: createMissionDraft({
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
          [scopeKey]: missionDraftReducer(existing, { type: "update-field", field: "objective", value: seed }),
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
    setMobileMissionOpen(false);
  }, [activeProjectId, activeSession, missionDrafts]);

  const handleCloseMissionDraft = useCallback(() => {
    if (!activeDraftScope) return;
    setVisibleDraftScopes((current) => ({ ...current, [activeDraftScope]: false }));
  }, [activeDraftScope]);

  const handleMissionDraftFieldChange = useCallback((field: MissionDraftTextField, value: string) => {
    if (!activeDraftScope) return;
    dispatchDraft(activeDraftScope, { type: "update-field", field, value });
    setLaunchResults((current) => {
      if (!current[activeDraftScope]) return current;
      const next = { ...current };
      delete next[activeDraftScope];
      return next;
    });
  }, [activeDraftScope, dispatchDraft]);

  const handleMissionDraftBudgetChange = useCallback((micros: number | null, source: HardBudgetSource) => {
    if (!activeDraftScope) return;
    dispatchDraft(activeDraftScope, { type: "set-hard-budget", micros, source });
    setLaunchResults((current) => {
      if (!current[activeDraftScope]) return current;
      const next = { ...current };
      delete next[activeDraftScope];
      return next;
    });
  }, [activeDraftScope, dispatchDraft]);

  const handleMissionDraftCommitmentChange = useCallback((value: number) => {
    if (!activeDraftScope) return;
    dispatchDraft(activeDraftScope, { type: "set-resource-commitment", value });
    setLaunchResults((current) => {
      if (!current[activeDraftScope]) return current;
      const next = { ...current };
      delete next[activeDraftScope];
      return next;
    });
  }, [activeDraftScope, dispatchDraft]);

  const handleMissionDraftValidate = useCallback(() => {
    if (!activeDraftScope) return;
    dispatchDraft(activeDraftScope, { type: "validate" });
  }, [activeDraftScope, dispatchDraft]);

  const handleLaunchMission = useCallback(() => {
    if (!activeSession || !activeDraftScope) return;
    const draft = missionDrafts[activeDraftScope];
    if (!draft || draft.lifecycle === "launching" || draft.lifecycle === "launched") return;

    const launching = missionDraftReducer(draft, { type: "start-launch" });
    setMissionDrafts((current) => ({ ...current, [activeDraftScope]: launching }));
    if (launching.lifecycle !== "launching") return;

    const command = toMissionLaunchCommand(launching);
    if (!command) {
      setMissionDrafts((current) => ({
        ...current,
        [activeDraftScope]: missionDraftReducer(launching, {
          type: "launch-failed",
          message: "Draft validation failed before launch.",
        }),
      }));
      return;
    }

    const settleLaunch = (result: MissionLaunchResult) => {
      setMissionDrafts((current) => {
        const currentDraft = current[activeDraftScope] ?? launching;
        const settled = result.outcome === "accepted"
          ? missionDraftReducer(currentDraft, {
              type: "launch-succeeded",
              missionId: result.missionId ?? command.draftId,
              message: result.message,
            })
          : missionDraftReducer(currentDraft, { type: "launch-failed", message: result.message });
        return { ...current, [activeDraftScope]: settled };
      });
      setLaunchResults((current) => ({ ...current, [activeDraftScope]: result }));

      // A Project/session switch can happen while an adapter command is in
      // flight. The old draft may settle in its own scope, but it must not
      // navigate the operator away from the newly selected Project.
      if (result.outcome === "accepted") {
        setVisibleDraftScopes((current) => ({ ...current, [activeDraftScope]: false }));
      }
      if (result.outcome === "accepted" && activeDraftScopeRef.current === activeDraftScope) {
        // Keep the same scenario so the in-memory runtime instance (and its
        // freshly projected Mission execution) survives the navigation.
        router.push(withProject(`/?view=mission-control&scenario=${encodeURIComponent(snapshot.scenario)}`));
      }
    };

    void launchMission(command)
      .then(settleLaunch)
      .catch((error: unknown) => {
        const message = error instanceof Error && error.message
          ? error.message
          : "The runtime adapter failed while launching this Mission.";
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
  }, [activeDraftScope, activeSession, launchMission, missionDrafts, router, snapshot.scenario, withProject]);

  const handleComposerIntent = useCallback((intent: ComposerIntent) => {
    dispatchComposerIntent(intent, {
      chat: ({ text }) => {
        if (activeSessionKey) void sendMessage(activeSessionKey, { content: text });
      },
      "mission.create": ({ seed }) => handleCreateMissionDraft(seed),
    });
  }, [activeSessionKey, handleCreateMissionDraft, sendMessage]);

  const handleSelectProfile = useCallback((profileId: string) => {
    void setActiveProfile(profileId);
  }, [setActiveProfile]);

  // Switching projects closes mobile overlays, cancels any in-flight stream,
  // persists the new selection, and keeps the URL in sync so a refresh keeps
  // the operator in the same project.
  const handleProjectChange = useCallback((id: ProjectId) => {
    setMobileNavOpen(false);
    setMobileMissionOpen(false);
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
      router.replace(`${url.pathname}${url.search}`);
    }
  }, [activeSessionKey, cancel, router, runtimeSnapshot.sessions, setActiveProject]);

  if (!activeSession) return null;

  const messages = snapshot.messagesBySession[activeSession.id] ?? [];
  const mission = snapshot.missionsBySession[activeSession.id];
  const observability = snapshot.observabilityBySession[activeSession.id];
  const missionOpen = missionMode !== "collapsed";

  const sidebar = (
    <OcgSidebar
      sessions={snapshot.sessions}
      activeSessionId={activeSession.id}
      collapsed={sidebarCollapsed}
      onToggle={() => setSidebarCollapsed((value) => !value)}
      onSelect={selectSession}
      onNewChat={handleNewChat}
      runtimeStatus={snapshot.status}
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
            activeSessionId={activeSession.id}
            collapsed={false}
            onToggle={() => setMobileNavOpen(false)}
            onSelect={selectSession}
            onNewChat={handleNewChat}
            runtimeStatus={snapshot.status}
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
          sidebarCollapsed={sidebarCollapsed}
          missionOpen={missionOpen}
          missionControls={view === "chat" || view === "canonical"}
          activeView={view}
          onToggleSidebar={() => setSidebarCollapsed((value) => !value)}
          onToggleMission={() => setMissionMode((value) => value === "collapsed" ? "docked" : "collapsed")}
          onOpenMobileSidebar={() => setMobileNavOpen(true)}
          onOpenMobileMission={() => setMobileMissionOpen(true)}
          onNavigate={navigate}
          runtimeStatus={snapshot.status}
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
        ) : isMissionControl ? (
          <main aria-label="Mission Control" className="flex min-h-0 flex-1 overflow-hidden">
            {snapshot.executionBySession[activeSession.id] ? (
              <MissionControlSurface
                key={`${activeProjectId}:${activeSession.id}`}
                execution={snapshot.executionBySession[activeSession.id]!}
                onOpenInspector={() => navigate("chat")}
              />
            ) : (
              <div className="flex flex-1 items-center justify-center p-6 text-[12px] text-muted-foreground">No Mission execution is available.</div>
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
        ) : (
          <main aria-label="OCG workspace" className="flex min-h-0 flex-1">
            <div className="flex min-w-0 flex-1 flex-col">
              <ChatView
                key={`${activeProjectId}:${activeSession.id}`}
                session={activeSession}
                messages={messages}
                runtimeStatus={snapshot.status}
                mission={mission}
                onComposerIntent={handleComposerIntent}
                composerSurface={activeDraft ? (
                  <MissionDraftSurface
                    project={activeProject}
                    draft={activeDraft}
                    launchResult={activeLaunchResult}
                    onFieldChange={handleMissionDraftFieldChange}
                    onBudgetChange={handleMissionDraftBudgetChange}
                    onCommitmentChange={handleMissionDraftCommitmentChange}
                    onValidate={handleMissionDraftValidate}
                    onLaunch={handleLaunchMission}
                    onClose={handleCloseMissionDraft}
                  />
                ) : null}
                composerSurfaceKey={activeDraft?.id}
              />
            </div>

            <aside
              aria-label="Mission panel"
              className={cn(
                "hidden shrink-0 overflow-hidden border-border bg-background transition-[width,opacity] duration-200 ease-out lg:block",
                 missionMode === "expanded" ? "w-[min(640px,42vw)] border-l opacity-100" : missionMode === "docked" ? "w-[min(360px,28vw)] border-l opacity-100" : "w-0 border-l-0 opacity-0",
              )}
            >
              <div className={cn("h-full", missionMode === "expanded" ? "w-[min(640px,42vw)]" : "w-[min(360px,28vw)]")}>
                {mission && missionOpen && <MissionView mission={mission} observability={observability} mode={missionMode} onModeChange={setMissionMode} onClose={() => setMissionMode("collapsed")} onOpenMissionControl={() => navigate("mission-control")} />}
              </div>
            </aside>
          </main>
        )}
      </div>

      {!isLedger && !isControlCenter && !isMissionControl && !isLogs && !isSettings && !isHome && !isAttention && (
        <div
          className={cn("fixed inset-0 z-50 lg:hidden", !mobileMissionOpen && "pointer-events-none")}
          aria-hidden={!mobileMissionOpen}
        >
          <div
            onClick={() => setMobileMissionOpen(false)}
            className={cn(
              "absolute inset-0 bg-black/40 transition-opacity duration-200",
              mobileMissionOpen ? "opacity-100" : "opacity-0",
            )}
          />
          <aside
            aria-label="Mission panel"
            className={cn(
               "absolute inset-y-0 right-0 w-full max-w-none border-l border-border bg-background transition-transform duration-200 ease-out sm:w-[640px] sm:max-w-[85vw]",
               mobileMissionOpen ? "translate-x-0" : "translate-x-full",
             )}
            >
            {mobileMissionOpen && mission && <MissionView mission={mission} observability={observability} mode="expanded" onClose={() => setMobileMissionOpen(false)} onOpenMissionControl={() => navigate("mission-control")} />}
          </aside>
        </div>
      )}
    </div>
  );
}
