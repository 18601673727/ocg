"use client";

import type * as Api from "@/components/ocg/contracts/api";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useRef, useState } from "react";
import dynamic from "next/dynamic";
import { ArrowLeft, PanelRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import { FloatingIconButton } from "@/components/assistant-ui/elements/tooltip-icon-button";
import { cn } from "@/lib/utils";
import { OcgSidebar } from "../sidebar/ocg-sidebar";
import { OcgTopbar } from "../topbar/ocg-topbar";
import { WorkspaceHome } from "./workspace-home";
import { useOcgRuntime } from "../runtime/runtime-context";
import {
  MUTABLE_CONTEXT_KEY,
  bindComposerContext,
  confirmMutableComposerNavigation,
} from "../chat/mutable-composer";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { isJobIdentity, isReachableView, type WorkspaceView } from "./view-domain";
import { useI18n } from "../i18n";
import { PageTitleVisibilityContext } from "./page-title";
import { JobDetailsContext } from "../job/job-details-context";
import { JobDetailsDialog } from "../job/job-details-dialog";
import {
  ConversationErrorsProvider,
  ConversationErrorScope,
  useConversationErrors,
} from "../chat/conversation-errors";
import type { RuntimeStatus } from "../types";
import {
  conversationAncestors,
  conversationPath,
  parseConversationPath,
} from "./conversation-path";
import {
  parseSettingsPath,
  settingsPath,
  type SettingsSection,
} from "../settings/settings-sections";

function SurfaceLoading() {
  const { t } = useI18n();
  return (
    <p role="status" className="m-auto text-sm text-muted-foreground">
      {t("common.loading")}
    </p>
  );
}

const ChatInspector = dynamic(
  () => import("../chat/chat-inspector").then((module) => module.ChatInspector),
  { loading: SurfaceLoading },
);
const ChatScreen = dynamic(
  () => import("../chat/chat-screen").then((module) => module.ChatScreen),
  { loading: SurfaceLoading },
);
const JobsSurface = dynamic(
  () => import("../job/jobs-surface").then((module) => module.JobsSurface),
  { loading: SurfaceLoading },
);
const ObservationsSurface = dynamic(
  () => import("../dashboard/observations-surface").then((module) => module.ObservationsSurface),
  { loading: SurfaceLoading },
);
const LogsSurface = dynamic(
  () => import("../dashboard/logs-surface").then((module) => module.LogsSurface),
  { loading: SurfaceLoading },
);
const SettingsSurface = dynamic(
  () => import("../settings/control-settings-surface").then((module) => module.SettingsSurface),
  { loading: SurfaceLoading },
);

export type { WorkspaceView } from "./view-domain";

type Health = Api.Health;
function currentLocation() {
  const url = new URL(window.location.href);
  const settingsSection = parseSettingsPath(url.pathname);
  const requested =
    url.searchParams.get("view") ??
    (settingsSection ? "settings" : url.pathname.slice(1).replace(/\.html$/, ""));
  const job = url.searchParams.get("job");
  const chain = parseConversationPath(url.pathname);
  return {
    view: chain
      ? ("chat" as WorkspaceView)
      : isReachableView(requested)
        ? requested
        : ("chat" as WorkspaceView),
    job: chain?.at(-1) ?? (isJobIdentity(job) ? job : null),
    chain,
    settingsSection: settingsSection ?? "models",
  };
}

type AppShellProps = {
  client: HttpCanonicalControlClient;
  health: Health | null;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  signOutError: string | null;
  onSignOut: () => void;
};

export function AppShell(props: AppShellProps) {
  return (
    <ConversationErrorsProvider>
      <AppShellContent {...props} />
    </ConversationErrorsProvider>
  );
}

function AppShellContent({
  client,
  health,
  sessionExpiresAtUs,
  signingOut,
  signOutError,
  onSignOut,
}: AppShellProps) {
  const runtime = useOcgRuntime();
  const { t } = useI18n();
  const [location, setLocation] = useState(currentLocation);
  const lastLocationUrl = useRef(window.location.href);
  const [detailJob, setDetailJob] = useState(() =>
    location.view === "jobs" ? location.job : null,
  );
  const [sessionId, setSessionId] = useState("");
  const restoreMutableOnce = useRef(false);
  const [ancestors, setAncestors] = useState<string[]>([]);
  const [collapsed, setCollapsed] = useState(false);
  const [mobile, setMobile] = useState(false);
  useEffect(() => {
    if (!mobile) return;
    const desktop = window.matchMedia("(min-width: 48rem)");
    const closeOnDesktop = () => {
      if (desktop.matches) setMobile(false);
    };
    desktop.addEventListener("change", closeOnDesktop);
    return () => desktop.removeEventListener("change", closeOnDesktop);
  }, [mobile]);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [openedJob, setOpenedJob] = useState<string | null>(null);
  const opening =
    location.view === "chat" &&
    location.job !== null &&
    Boolean(runtime.client.openConversation) &&
    openedJob !== location.job;
  const [attempt, setAttempt] = useState(0);
  const [pageTitleInView, setPageTitleInView] = useState(true);
  const session = runtime.snapshot.sessions.find((session) => session.id === sessionId);
  const inspectorAvailable = Boolean(
    session?.sessionId && (runtime.snapshot.messagesBySession[session.id]?.length ?? 0) > 0,
  );
  const errors = useConversationErrors();
  const report = errors?.report;
  const transfer = errors?.transfer;
  const conversationKey = sessionId || location.job || "new";
  const observedStatus = useRef<RuntimeStatus | null>(null);
  const activeConversation = useRef(conversationKey);
  const currentLocationRef = useRef({ location, sessionId });
  const listedConversations = useRef(new Set<string>());
  useEffect(() => {
    currentLocationRef.current = { location, sessionId };
  });
  useEffect(() => {
    for (const listed of runtime.snapshot.sessions)
      if (listed.conversationId) listedConversations.current.add(listed.conversationId);
  }, [runtime.snapshot.sessions]);
  useEffect(() => {
    if (restoreMutableOnce.current || location.view !== "chat" || location.job || sessionId) return;
    restoreMutableOnce.current = true;
    let context: string | null;
    try {
      context =
        sessionStorage.getItem(MUTABLE_CONTEXT_KEY) ?? localStorage.getItem(MUTABLE_CONTEXT_KEY);
    } catch {
      return;
    }
    if (!context) return;
    void client
      .currentMutableJob(context)
      .then(async () => {
        if (
          currentLocationRef.current.sessionId ||
          currentLocationRef.current.location.view !== "chat" ||
          currentLocationRef.current.location.job
        )
          return;
        const restored = await runtime.createSession({
          workType: "research",
          title: t("sidebar.newChat"),
        });
        if (
          currentLocationRef.current.sessionId ||
          currentLocationRef.current.location.view !== "chat" ||
          currentLocationRef.current.location.job
        )
          return;
        bindComposerContext(restored.id, context);
        setSessionId(restored.id);
      })
      .catch(() => {});
  }, [client, location.view, location.job, sessionId, runtime, t]);
  useEffect(() => {
    activeConversation.current = conversationKey;
  }, [conversationKey]);

  useEffect(() => {
    if (location.view === "chat" && location.job)
      void import("../chat/chat-screen").catch(() => {});
  }, [location.view, location.job]);

  useEffect(() => {
    const status = runtime.snapshot.status;
    if (observedStatus.current === status) return;
    observedStatus.current = status;
    if (location.view === "chat" && status.state === "failed" && status.detail) {
      const owner = status.sessionId ?? conversationKey;
      report?.(owner, status.detail);
    }
  }, [runtime.snapshot.status, runtime.client, conversationKey, location.view, report, t]);
  const closeInspector = useCallback(() => {
    setInspectorOpen(false);
    requestAnimationFrame(() => document.getElementById("chat-inspector-toggle")?.focus());
  }, []);

  useEffect(() => {
    if (!inspectorOpen || location.view !== "chat" || detailJob !== null) return;
    document.getElementById("chat-inspector-close")?.focus();
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeInspector();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [inspectorOpen, location.view, detailJob, closeInspector]);

  useEffect(() => {
    const pop = () => {
      void (async () => {
        if (!(await confirmMutableComposerNavigation())) {
          window.history.replaceState(window.history.state, "", lastLocationUrl.current);
          return;
        }
        const next = currentLocation();
        if (sessionId.startsWith("draft-")) {
          void runtime.deleteSession(sessionId).catch((cause) => report?.(sessionId, cause));
        }
        setLocation(next);
        lastLocationUrl.current = window.location.href;
        setDetailJob(next.view === "jobs" ? next.job : null);
        if (next.view !== "settings") setSessionId("");
      })();
    };
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, [runtime, sessionId, report]);

  useEffect(() => {
    if (!location.job || location.view !== "chat" || !runtime.client.openConversation) return;
    let alive = true;
    void (async () => {
      const chain = await conversationAncestors(
        (id) =>
          listedConversations.current.has(id)
            ? Promise.resolve(id)
            : client.resolveConversation(id),
        location.job!,
      );
      if (location.chain && conversationPath(chain) !== conversationPath(location.chain))
        throw new Error(t("chat.child.invalidPath"));
      if (!alive) return;
      const id = await runtime.client.openConversation!(location.job!);
      if (alive) {
        transfer?.(location.job!, id);
        setSessionId(id);
        setAncestors(chain);
        if (location.chain || chain.length > 1)
          window.history.replaceState(null, "", conversationPath(chain));
      }
    })()
      .catch((cause) => {
        if (alive)
          report?.(activeConversation.current, cause, {
            label: t("app.retryLoading"),
            run: () => setAttempt((value) => value + 1),
          });
      })
      .finally(() => {
        if (alive) setOpenedJob(location.job);
      });
    return () => {
      alive = false;
      setOpenedJob(null);
    };
  }, [
    location.job,
    location.view,
    location.chain,
    runtime.client,
    client,
    attempt,
    report,
    transfer,
    t,
  ]);

  useEffect(() => {
    if (
      location.view === "chat" &&
      !location.chain &&
      ancestors.length <= 1 &&
      !opening &&
      session?.sessionId
    ) {
      window.history.replaceState(null, "", `/?job=${session.sessionId}`);
    }
  }, [location.view, location.chain, ancestors.length, opening, session?.sessionId]);

  const discardEmptyDraft = useCallback(
    (keep?: string) => {
      if (sessionId.startsWith("draft-") && sessionId !== keep) {
        void runtime.deleteSession(sessionId).catch((cause) => report?.(sessionId, cause));
      }
    },
    [runtime, sessionId, report],
  );

  const goDirect = useCallback(
    (view: WorkspaceView, job: string | null = null, keepDraft?: string, keepAncestors = false) => {
      if (!isReachableView(view)) return;
      discardEmptyDraft(keepDraft);
      const query = new URLSearchParams();
      if (job) query.set("job", job);
      const path = view === "chat" ? "/" : `/${view}`;
      const href = `${path}${query.size ? `?${query}` : ""}`;
      window.history.pushState(null, "", href);
      lastLocationUrl.current = window.location.href;
      setLocation({ view, job, chain: null, settingsSection: "models" });
      if (!keepAncestors) setAncestors([]);
      setMobile(false);
    },
    [discardEmptyDraft],
  );
  const go = useCallback(
    (view: WorkspaceView, job: string | null = null, keepDraft?: string, keepAncestors = false) => {
      void confirmMutableComposerNavigation().then((allowed) => {
        if (allowed) goDirect(view, job, keepDraft, keepAncestors);
      });
    },
    [goDirect],
  );

  const goSettings = useCallback((section: SettingsSection) => {
    const path = settingsPath(section);
    if (window.location.pathname === path) return;
    window.history.pushState(null, "", path);
    lastLocationUrl.current = window.location.href;
    setLocation({ view: "settings", job: null, chain: null, settingsSection: section });
  }, []);

  function openNestedConversation(job: string) {
    void confirmMutableComposerNavigation().then((allowed) => {
      if (!allowed) return;
      if (!ancestors.length && session?.conversationId) setAncestors([session.conversationId]);
      setSessionId("");
      goDirect("chat", job, undefined, true);
    });
  }

  function closeJobDetails() {
    setDetailJob(null);
    if (location.view === "jobs" && location.job) {
      const url = new URL(window.location.href);
      url.searchParams.delete("job");
      window.history.replaceState(
        window.history.state,
        "",
        `${url.pathname}${url.search}${url.hash}`,
      );
      setLocation({ ...location, job: null });
    }
  }

  const newChat = useCallback(async () => {
    const session = await runtime.createSession({
      workType: "research",
      title: t("sidebar.newChat"),
    });
    if (!(await confirmMutableComposerNavigation())) {
      await runtime.deleteSession(session.id);
      return;
    }
    setSessionId(session.id);
    goDirect("chat", null, session.id);
  }, [runtime, goDirect, t]);

  const runningSessionIds = new Set(
    runtime.snapshot.sessions
      .filter(
        (session) =>
          runtime.snapshot.executionBySession[session.id]?.state === "running" ||
          (runtime.snapshot.messagesBySession[session.id] ?? []).some(
            (message) => message.status === "pending" || message.status === "streaming",
          ),
      )
      .map((session) => session.id),
  );
  const listedSessions = runtime.snapshot.sessions
    .filter((session) => runtime.client.isListedConversation?.(session.id) ?? true)
    .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  function selectSession(id: string) {
    void confirmMutableComposerNavigation().then((allowed) => {
      if (!allowed) return;
      const selected = runtime.snapshot.sessions.find((session) => session.id === id);
      setSessionId(id);
      goDirect("chat", selected?.sessionId ?? null, id);
    });
  }
  function goHome() {
    void confirmMutableComposerNavigation().then((allowed) => {
      if (!allowed) return;
      setSessionId("");
      setInspectorOpen(false);
      goDirect("chat");
    });
  }
  const sidebar = (
    <OcgSidebar
      conversationList={runtime.snapshot.conversationList}
      onRetryConversations={
        runtime.client.retryConversations ? () => runtime.client.retryConversations!() : undefined
      }
      sessions={listedSessions}
      runningSessionIds={runningSessionIds}
      control={client}
      activeAncestors={ancestors}
      onOpenConversation={(job) => void openNestedConversation(job)}
      activeSessionId={sessionId}
      collapsed={!mobile && collapsed}
      drawer={mobile}
      onToggle={() => (mobile ? setMobile(false) : setCollapsed((value) => !value))}
      onSelect={selectSession}
      onNewChat={() => void newChat()}
      onHome={goHome}
      onRename={
        runtime.client.renameSession
          ? (id, title) => runtime.client.renameSession!(id, title)
          : undefined
      }
      onRenameChild={async (conversation, title) => {
        const child = runtime.snapshot.sessions.find(
          (item) => item.conversationId === conversation,
        );
        if (child && runtime.client.renameSession) {
          await runtime.client.renameSession(child.id, title);
        } else {
          await client.editConversation(conversation, { title: title.trim() });
        }
      }}
      onHide={
        runtime.client.hideSession
          ? async (id) => {
              if (id === sessionId && !(await confirmMutableComposerNavigation())) return;
              await runtime.client.hideSession!(id);
              if (id === sessionId) {
                setSessionId("");
                goDirect("chat");
              }
            }
          : undefined
      }
      runtimeStatus={runtime.snapshot.status}
      runtimeAuthority="canonical"
      activeView={location.view}
      onNavigate={(view) => go(view)}
      onSettingsClick={() => go("settings")}
    />
  );

  return (
    <JobDetailsContext.Provider value={setDetailJob}>
      <ConversationErrorScope value={location.view === "chat" ? conversationKey : null}>
        <div data-ocg-app-shell className="flex h-dvh min-h-0 w-full overflow-hidden bg-background">
          <aside
            className={cn(
              "hidden shrink-0 border-r border-sidebar-border bg-sidebar md:flex",
              collapsed ? "w-16" : "w-72",
            )}
          >
            {!mobile && sidebar}
          </aside>
          <Dialog open={mobile} onOpenChange={setMobile}>
            <DialogContent
              showCloseButton={false}
              finalFocus={() => document.getElementById("workspace-navigation-trigger")}
              className="inset-y-0 left-0 flex h-dvh w-72 max-w-[calc(100%-1.5rem)] translate-x-0 translate-y-0 flex-col gap-0 rounded-none bg-sidebar p-0 sm:max-w-72"
            >
              <DialogTitle className="sr-only">{t("nav.workspaceNavigation")}</DialogTitle>
              {mobile && sidebar}
            </DialogContent>
          </Dialog>
          <div className="relative flex min-h-0 min-w-0 flex-1 flex-col">
            <OcgTopbar
              session={location.view === "chat" ? session : undefined}
              activeView={location.view}
              titleVisible={location.view === "chat" || !pageTitleInView}
              onOpenMobileSidebar={() => setMobile(true)}
              onHome={goHome}
              inspectorToggle={
                location.view === "chat" && inspectorAvailable && !inspectorOpen ? (
                  <Button
                    id="chat-inspector-toggle"
                    type="button"
                    variant="ghost"
                    size="icon"
                    className="size-11 shrink-0 rounded-full bg-muted/80 shadow-sm backdrop-blur-sm hover:bg-muted"
                    aria-label={t("topbar.expandInspector")}
                    title={t("topbar.expandInspector")}
                    aria-expanded={false}
                    aria-controls="chat-inspector"
                    onClick={() => setInspectorOpen(true)}
                  >
                    <PanelRight className="size-4" />
                  </Button>
                ) : undefined
              }
            />
            {signOutError && (
              <DismissibleNotice message={signOutError} className="px-5 text-sm text-destructive">
                {signOutError}
              </DismissibleNotice>
            )}
            <PageTitleVisibilityContext.Provider value={setPageTitleInView}>
              <main
                key={location.view}
                className={cn(
                  "flex min-h-0 flex-1 flex-col",
                  location.view === "chat"
                    ? "overflow-hidden"
                    : "ocg-subtle-scrollbar overflow-y-auto overscroll-contain",
                )}
              >
                {location.view === "chat" ? (
                  <>
                    {opening ? (
                      <p role="status" className="m-auto text-sm text-muted-foreground">
                        {t("app.loadingConversation")}
                      </p>
                    ) : session ? (
                      <div className="flex min-h-0 flex-1 flex-col">
                        <ChatScreen
                          key={session.id}
                          onOpenConversation={(job) => void openNestedConversation(job)}
                          backButton={
                            ancestors.length > 1 ? (
                              <FloatingIconButton
                                type="button"
                                tooltip={t("chat.child.back")}
                                aria-label={t("chat.child.back")}
                                onClick={() => {
                                  void confirmMutableComposerNavigation().then((allowed) => {
                                    if (!allowed) return;
                                    window.history.pushState(
                                      null,
                                      "",
                                      conversationPath(ancestors.slice(0, -1)),
                                    );
                                    lastLocationUrl.current = window.location.href;
                                    setSessionId("");
                                    setLocation(currentLocation());
                                  });
                                }}
                              >
                                <ArrowLeft />
                              </FloatingIconButton>
                            ) : undefined
                          }
                          session={session}
                        />
                      </div>
                    ) : (
                      <WorkspaceHome
                        sessions={listedSessions}
                        onSelect={selectSession}
                        onNewChat={() => void newChat()}
                        onNavigate={go}
                      />
                    )}
                  </>
                ) : null}
                {location.view === "jobs" && (
                  <JobsSurface
                    client={client}
                    active
                    job={detailJob}
                    onJob={setDetailJob}
                    onOpenChat={(job) => {
                      setSessionId("");
                      go("chat", job);
                    }}
                  />
                )}
                {(location.view === "usage" || location.view === "health") && (
                  <ObservationsSurface
                    key={location.view}
                    client={client}
                    kind={location.view}
                    health={health}
                    onJob={setDetailJob}
                  />
                )}
                {location.view === "logs" && <LogsSurface client={client} />}
                {location.view === "settings" && (
                  <SettingsSurface
                    client={client}
                    sessionExpiresAtUs={sessionExpiresAtUs}
                    signingOut={signingOut}
                    onSignOut={onSignOut}
                    section={location.settingsSection}
                    onNavigate={goSettings}
                  />
                )}
              </main>
            </PageTitleVisibilityContext.Provider>
          </div>
          {location.view === "chat" && inspectorAvailable && inspectorOpen && (
            <ChatInspector
              session={session}
              messages={session ? (runtime.snapshot.messagesBySession[session.id] ?? []) : []}
              onClose={closeInspector}
              onOpenJob={setDetailJob}
            />
          )}
        </div>
        <JobDetailsDialog
          client={client}
          job={detailJob}
          onJob={(job) => {
            if (job === null) closeJobDetails();
            else setDetailJob(job);
          }}
          onOpenChat={(job) => {
            void confirmMutableComposerNavigation().then((allowed) => {
              if (!allowed) return;
              setDetailJob(null);
              setSessionId("");
              goDirect("chat", job);
            });
          }}
        />
      </ConversationErrorScope>
    </JobDetailsContext.Provider>
  );
}
