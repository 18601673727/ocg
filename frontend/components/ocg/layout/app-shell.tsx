"use client";

import type * as Api from "@/components/ocg/contracts/api";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useRef, useState } from "react";
import dynamic from "next/dynamic";
import { ArrowLeft, PanelRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { OcgSidebar } from "../sidebar/ocg-sidebar";
import { OcgTopbar } from "../topbar/ocg-topbar";
import { useOcgRuntime } from "../runtime/runtime-context";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import type { ComposerIntent } from "../composer/domain";
import { isJobIdentity, isReachableView, type WorkspaceView } from "./view-domain";
import { useI18n } from "../i18n";
import { PageTitleVisibilityContext } from "./page-title";
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
  const [sessionId, setSessionId] = useState("");
  const [ancestors, setAncestors] = useState<string[]>([]);
  const [collapsed, setCollapsed] = useState(false);
  const [mobile, setMobile] = useState(false);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [opening, setOpening] = useState(false);
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
    if (!inspectorOpen || location.view !== "chat") return;
    document.getElementById("chat-inspector-close")?.focus();
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeInspector();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [inspectorOpen, location.view, closeInspector]);

  useEffect(() => {
    const pop = () => {
      const next = currentLocation();
      if (sessionId.startsWith("draft-")) {
        void runtime.deleteSession(sessionId).catch((cause) => report?.(sessionId, cause));
      }
      setLocation(next);
      if (next.view !== "settings") setSessionId("");
    };
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, [runtime, sessionId, report]);

  useEffect(() => {
    if (!location.job || location.view !== "chat" || !runtime.client.openConversation) return;
    let alive = true;
    setOpening(true);
    void (async () => {
      const chain = await conversationAncestors((id) => client.readJob(id), location.job!);
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
        if (alive) setOpening(false);
      });
    return () => {
      alive = false;
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

  // Runtime actions such as Retry also admit successors, so their addresses must recover on reload.
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

  const go = useCallback(
    (view: WorkspaceView, job: string | null = null, keepDraft?: string) => {
      if (!isReachableView(view)) return;
      discardEmptyDraft(keepDraft);
      const query = new URLSearchParams();
      if (job) query.set("job", job);
      const path = view === "chat" ? "/" : `/${view}`;
      const href = `${path}${query.size ? `?${query}` : ""}`;
      window.history.pushState(null, "", href);
      setLocation({ view, job, chain: null, settingsSection: "models" });
      setAncestors([]);
      setMobile(false);
    },
    [discardEmptyDraft],
  );

  const goSettings = useCallback((section: SettingsSection) => {
    const path = settingsPath(section);
    if (window.location.pathname === path) return;
    window.history.pushState(null, "", path);
    setLocation({ view: "settings", job: null, chain: null, settingsSection: section });
  }, []);

  function openNestedConversation(job: string) {
    setSessionId("");
    go("chat", job);
  }

  const newChat = useCallback(async () => {
    const session = await runtime.createSession({ workType: "research" });
    setSessionId(session.id);
    go("chat", null, session.id);
  }, [runtime, go]);

  async function intent(intent: ComposerIntent) {
    if (intent.kind !== "chat") throw new Error(t("app.jobLaunchUnavailable"));
    if (!session) throw new Error(t("app.createConversationFirst"));
    await runtime.sendMessage(session.id, {
      content: intent.text,
      selection: intent.selection,
      images: intent.images,
      mode: intent.mode,
    });
    const updated = runtime.client.getSnapshot().sessions.find((item) => item.id === session.id);
    if (updated?.sessionId && !location.chain && ancestors.length <= 1) {
      // Admission already installed the turn in the store; this only makes reload recover it.
      window.history.replaceState(null, "", `/?job=${updated.sessionId}`);
    }
  }

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
  const sidebar = (
    <OcgSidebar
      sessions={runtime.snapshot.sessions
        .filter((session) => runtime.client.isListedConversation?.(session.id) ?? true)
        .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))}
      runningSessionIds={runningSessionIds}
      control={client}
      activeAncestors={ancestors}
      onOpenConversation={(job) => void openNestedConversation(job)}
      activeSessionId={sessionId}
      collapsed={collapsed}
      onToggle={() => setCollapsed((value) => !value)}
      onSelect={(id) => {
        const selected = runtime.snapshot.sessions.find((session) => session.id === id);
        setSessionId(id);
        go("chat", selected?.sessionId ?? null);
      }}
      onNewChat={() => void newChat()}
      onHome={() => {
        setSessionId("");
        setInspectorOpen(false);
        go("chat");
      }}
      onRename={
        runtime.client.renameSession
          ? (id, title) => runtime.client.renameSession!(id, title)
          : undefined
      }
      onHide={
        runtime.client.hideSession
          ? async (id) => {
              await runtime.client.hideSession!(id);
              if (id === sessionId) {
                setSessionId("");
                go("chat");
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
    <ConversationErrorScope value={location.view === "chat" ? conversationKey : null}>
      <div className="flex h-dvh min-h-0 w-full overflow-hidden bg-background">
        <aside
          className={cn(
            "hidden shrink-0 border-r border-sidebar-border bg-sidebar md:flex",
            collapsed ? "w-16" : "w-72",
          )}
        >
          {sidebar}
        </aside>
        {mobile && (
          <div className="fixed inset-0 z-50 flex md:hidden">
            <button
              type="button"
              aria-label={t("app.closeNavigation")}
              className="absolute inset-0 bg-black/40"
              onClick={() => setMobile(false)}
            />
            <aside className="relative flex h-full w-72 max-w-[calc(100%-3rem)] bg-sidebar">
              {sidebar}
            </aside>
          </div>
        )}
        <div className="relative flex min-h-0 min-w-0 flex-1 flex-col">
          <OcgTopbar
            session={location.view === "chat" ? session : undefined}
            activeView={location.view}
            titleVisible={location.view === "chat" || !pageTitleInView}
            onOpenMobileSidebar={() => setMobile(true)}
            backButton={
              location.view === "chat" && ancestors.length > 1 ? (
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  className="size-11 shrink-0 rounded-full bg-muted/80 backdrop-blur-sm"
                  aria-label={t("chat.child.back")}
                  title={t("chat.child.back")}
                  onClick={() => {
                    window.history.pushState(null, "", conversationPath(ancestors.slice(0, -1)));
                    setSessionId("");
                    setLocation(currentLocation());
                  }}
                >
                  <ArrowLeft className="size-4" />
                </Button>
              ) : undefined
            }
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
                        session={session}
                        execution={null}
                        runtimeStatus={runtime.snapshot.status}
                        onComposerIntent={intent}
                        onCancel={() => runtime.cancel(session.id)}
                      />
                    </div>
                  ) : (
                    <div className="flex min-h-0 flex-1 flex-col items-center justify-center gap-4 p-6 text-center">
                      <h1 className="text-2xl font-medium">{t("chat.newThread")}</h1>
                      <button
                        type="button"
                        className="rounded-md border border-border px-4 py-2 text-sm"
                        onClick={() => void newChat()}
                      >
                        {t("sidebar.newChat")}
                      </button>
                    </div>
                  )}
                </>
              ) : null}
              {location.view === "jobs" && (
                <JobsSurface
                  client={client}
                  active
                  job={location.job}
                  onJob={(job) => go("jobs", job)}
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
                  onJob={(job) => go("jobs", job)}
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
            onOpenJob={(job) => go("jobs", job)}
          />
        )}
      </div>
    </ConversationErrorScope>
  );
}
