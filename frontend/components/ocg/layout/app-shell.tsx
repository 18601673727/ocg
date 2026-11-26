import { PanelRight } from "@lucide/solid";
import { createEffect, createMemo, createSignal, Loading, lazy, Show } from "solid-js";
import type * as Api from "@/components/ocg/contracts/api";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import { elementRef, shallowSelector } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import {
  ConversationErrorScope,
  ConversationErrorsProvider,
  useConversationErrors,
} from "../chat/conversation-errors";
import {
  bindComposerContext,
  confirmMutableComposerNavigation,
  MUTABLE_CONTEXT_KEY,
} from "../chat/mutable-composer";
import type { ThreadSearchTarget } from "../chat/thread-position";
import { useI18n } from "../i18n";
import { JobDetailsContext } from "../job/job-details-context";
import type { ApiClient } from "../runtime/api-client";
import { useOcgRuntime, useOcgRuntimeSnapshot } from "../runtime/runtime-context";
import {
  parseSettingsPath,
  type SettingsSection,
  settingsPath,
} from "../settings/settings-sections";
import { OcgSidebar } from "../sidebar/ocg-sidebar";
import { OcgTopbar } from "../topbar/ocg-topbar";
import type { RuntimeStatus } from "../types";
import {
  conversationAncestors,
  conversationPath,
  parseConversationPath,
} from "./conversation-path";
import { PageTitleVisibilityContext } from "./page-title";
import { isJobIdentity, isReachableView, type WorkspaceView } from "./view-domain";
import { WorkspaceHome } from "./workspace-home";
import { WorkspaceNavigation } from "./workspace-link";

function SurfaceLoading() {
  const i18n = useI18n();
  return (
    <p role="status" class="m-auto text-sm text-muted-foreground">
      {i18n.t("common.loading")}
    </p>
  );
}

const ChatInspector = lazy(() =>
  import("../chat/chat-inspector").then((module) => ({ default: module.ChatInspector })),
);
const ChatScreen = lazy(() =>
  import("../chat/chat-screen").then((module) => ({ default: module.ChatScreen })),
);
const JobDetailsDialog = lazy(() =>
  import("../job/job-details-dialog").then((module) => ({ default: module.JobDetailsDialog })),
);
const JobsSurface = lazy(() =>
  import("../job/jobs-surface").then((module) => ({ default: module.JobsSurface })),
);
const ObservationsSurface = lazy(() =>
  import("../dashboard/observations-surface").then((module) => ({
    default: module.ObservationsSurface,
  })),
);
const LogsSurface = lazy(() =>
  import("../dashboard/logs-surface").then((module) => ({ default: module.LogsSurface })),
);
const SettingsSurface = lazy(() =>
  import("../settings/settings-surface").then((module) => ({ default: module.SettingsSurface })),
);

type Health = Api.Health;
function currentLocation() {
  const url = new URL(window.location.href);
  const settingsSection = parseSettingsPath(url.pathname);
  const requested =
    url.searchParams.get("view") ??
    (settingsSection ? "settings" : url.pathname.replace(/^\/|\/$/g, "").replace(/\.html$/, ""));
  const chain = parseConversationPath(url.pathname);
  const job = url.searchParams.get("job");
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
  client: ApiClient;
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

function AppShellContent(props: AppShellProps) {
  const runtime = useOcgRuntime();
  const i18n = useI18n();
  const [location, setLocation] = createSignal<
    ReturnType<typeof currentLocation> & { searchTarget?: ThreadSearchTarget }
  >(currentLocation());
  const lastLocationUrl = elementRef(window.location.href);
  const [detailJob, setDetailJob] = createSignal(
    (() => (location().view === "jobs" ? location().job : null))(),
  );
  const [sessionId, setSessionId] = createSignal("");
  const restoreMutableOnce = elementRef(false);
  const [ancestors, setAncestors] = createSignal<string[]>([]);
  const [collapsed, setCollapsed] = createSignal(false);
  const [mobile, setMobile] = createSignal(false);
  createEffect(
    () => [mobile()],
    () => {
      if (!mobile()) return;
      const desktop = window.matchMedia("(min-width: 48rem)");
      const closeOnDesktop = () => {
        if (desktop.matches) setMobile(false);
      };
      desktop.addEventListener("change", closeOnDesktop);
      return () => desktop.removeEventListener("change", closeOnDesktop);
    },
  );
  const [inspectorOpen, setInspectorOpen] = createSignal(false);
  const [planningFocus, setPlanningFocus] = createSignal<{
    sessionId: string;
    decisionId: string;
  }>();
  const [openedJob, setOpenedJob] = createSignal<string | null>(null);
  const opening = createMemo(
    () =>
      location().view === "chat" &&
      location().job !== null &&
      Boolean(runtime.client.openConversation) &&
      openedJob() !== location().job,
  );
  const [attempt, setAttempt] = createSignal(0);
  const [pageTitleInView, setPageTitleInView] = createSignal(true);
  const sessions = useOcgRuntimeSnapshot((snapshot) => snapshot.sessions);
  const status = useOcgRuntimeSnapshot((snapshot) => snapshot.status);
  const conversationList = useOcgRuntimeSnapshot((snapshot) => snapshot.conversationList);
  const session = createMemo(() => sessions().find((session) => session.id === sessionId()));
  const inspectorAvailable = useOcgRuntimeSnapshot((snapshot) =>
    Boolean(session()?.sessionId && (snapshot.messagesBySession[session()!.id]?.length ?? 0) > 0),
  );
  const inspectorMessages = useOcgRuntimeSnapshot((snapshot) =>
    location().view === "chat" && inspectorOpen() && session()
      ? snapshot.messagesBySession[session()!.id]
      : undefined,
  );
  const errors = useConversationErrors();
  const report = errors?.report;
  const transfer = errors?.transfer;
  const conversationKey = createMemo(() => sessionId() || location().job || "new");
  const observedStatus = elementRef<RuntimeStatus | null>(null);
  const activeConversation = elementRef(conversationKey());
  const currentLocationRef = elementRef({ location: location(), sessionId: sessionId() });
  createEffect(
    () => [location(), sessionId()],
    () => {
      currentLocationRef.current = { location: location(), sessionId: sessionId() };
    },
  );
  createEffect(
    () => [props.client, location().view, location().job, sessionId(), runtime, i18n.t],
    () => {
      if (restoreMutableOnce.current || location().view !== "chat" || location().job || sessionId())
        return;
      restoreMutableOnce.current = true;
      let context: string | null;
      try {
        context =
          sessionStorage.getItem(MUTABLE_CONTEXT_KEY) ?? localStorage.getItem(MUTABLE_CONTEXT_KEY);
      } catch {
        return;
      }
      if (!context) return;
      void props.client
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
            title: i18n.t("sidebar.newChat"),
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
    },
  );
  createEffect(
    () => [conversationKey()],
    () => {
      activeConversation.current = conversationKey();
    },
  );

  createEffect(
    () => [location().view, location().job],
    () => {
      if (location().view === "chat" && location().job)
        void import("../chat/chat-screen").catch(() => {});
    },
  );

  createEffect(
    () => [status(), conversationKey(), location().view, report],
    () => {
      if (observedStatus.current === status()) return;
      observedStatus.current = status();
      if (location().view === "chat" && status().state === "failed" && status().detail) {
        const owner = status().sessionId ?? conversationKey();
        report?.(owner, status().detail);
      }
    },
  );
  const closeInspector = () => {
    setInspectorOpen(false);
    setPlanningFocus(undefined);
    requestAnimationFrame(() => {
      const trigger =
        planningFocus()?.sessionId === sessionId()
          ? document.getElementById(`planning-notice-${planningFocus()!.decisionId}`)
          : null;
      (trigger ?? document.getElementById("chat-inspector-toggle"))?.focus();
    });
  };

  createEffect(
    () => [
      inspectorOpen(),
      location().view,
      detailJob(),
      closeInspector,
      planningFocus(),
      sessionId(),
    ],
    () => {
      if (!inspectorOpen() || location().view !== "chat" || detailJob() !== null) return;
      if (planningFocus()?.sessionId !== sessionId())
        document.getElementById("chat-inspector-close")?.focus();
      const onKeyDown = (event: KeyboardEvent) => {
        if (event.key === "Escape") closeInspector();
      };
      window.addEventListener("keydown", onKeyDown);
      return () => window.removeEventListener("keydown", onKeyDown);
    },
  );

  const applyLocation = () => {
    const next = currentLocation();
    if (sessionId().startsWith("draft-") && next.view !== "settings") {
      void runtime.deleteSession(sessionId()).catch((cause) => report?.(sessionId(), cause));
    }
    setLocation(next);
    lastLocationUrl.current = window.location.href;
    setDetailJob(next.view === "jobs" ? next.job : null);
    setMobile(false);
    if (next.view !== "settings") setSessionId("");
  };

  const navigateHref = (href: string) => {
    if (href === window.location.href) return;
    void confirmMutableComposerNavigation().then((allowed) => {
      if (!allowed) return;
      window.history.pushState(null, "", href);
      applyLocation();
    });
  };

  createEffect(
    () => [applyLocation],
    () => {
      const pop = () => {
        void confirmMutableComposerNavigation().then((allowed) => {
          if (allowed) applyLocation();
          else window.history.replaceState(window.history.state, "", lastLocationUrl.current);
        });
      };
      window.addEventListener("popstate", pop);
      return () => window.removeEventListener("popstate", pop);
    },
  );

  createEffect(
    () => [
      location().job,
      location().view,
      location().chain,
      runtime.client,
      props.client,
      attempt(),
      report,
      transfer,
      i18n.t,
    ],
    () => {
      if (!location().job || location().view !== "chat" || !runtime.client.openConversation) return;
      let alive = true;
      void (async () => {
        const chain = await conversationAncestors(props.client, location().job!);
        if (
          location().chain &&
          location().chain!.length > 1 &&
          conversationPath(chain) !== conversationPath(location().chain!)
        )
          throw new Error(i18n.t("chat.child.invalidPath"));
        if (!alive) return;
        const id = await runtime.client.openConversation!(location().job!);
        if (alive) {
          transfer?.(location().job!, id);
          setSessionId(id);
          setAncestors(chain);
          window.history.replaceState(null, "", conversationPath(chain));
          lastLocationUrl.current = window.location.href;
        }
      })()
        .catch((cause) => {
          if (alive)
            report?.(activeConversation.current, cause, {
              label: i18n.t("app.retryLoading"),
              run: () => setAttempt((value) => value + 1),
            });
        })
        .finally(() => {
          if (alive) setOpenedJob(location().job);
        });
      return () => {
        alive = false;
        setOpenedJob(null);
      };
    },
  );

  createEffect(
    () => [location().view, location().chain, ancestors().length, opening(), session()?.sessionId],
    () => {
      if (
        location().view === "chat" &&
        !location().chain &&
        ancestors().length <= 1 &&
        !opening() &&
        session()?.sessionId
      ) {
        const chain = ancestors().length ? ancestors() : [session()!.conversationId!];
        if (!chain[0]) return;
        window.history.replaceState(null, "", conversationPath(chain));
        lastLocationUrl.current = window.location.href;
      }
    },
  );

  const discardEmptyDraft = (keep?: string) => {
    if (sessionId().startsWith("draft-") && sessionId() !== keep) {
      void runtime.deleteSession(sessionId()).catch((cause) => report?.(sessionId(), cause));
    }
  };

  const goDirect = (
    view: WorkspaceView,
    job: string | null = null,
    keepDraft?: string,
    keepAncestors = false,
    searchTarget?: ThreadSearchTarget,
  ) => {
    if (!isReachableView(view)) return;
    discardEmptyDraft(keepDraft);
    const href = view === "chat" ? (job ? conversationPath([job]) : "/") : `/${view}`;
    window.history.pushState(null, "", href);
    lastLocationUrl.current = window.location.href;
    setLocation({ view, job, chain: null, settingsSection: "models", searchTarget });
    if (!keepAncestors) setAncestors([]);
    setMobile(false);
  };
  const go = (
    view: WorkspaceView,
    job: string | null = null,
    keepDraft?: string,
    keepAncestors = false,
  ) => {
    void confirmMutableComposerNavigation().then((allowed) => {
      if (allowed) goDirect(view, job, keepDraft, keepAncestors);
    });
  };

  const goSettings = (section: SettingsSection) => {
    const path = settingsPath(section);
    if (window.location.pathname === path) return;
    window.history.pushState(null, "", path);
    lastLocationUrl.current = window.location.href;
    setLocation({ view: "settings", job: null, chain: null, settingsSection: section });
  };

  function openNestedConversation(job: string, searchTarget?: ThreadSearchTarget) {
    void confirmMutableComposerNavigation().then((allowed) => {
      if (!allowed) return;
      const active = session();
      if (
        location().view === "chat" &&
        active &&
        (active.conversationId === job || active.id === job)
      ) {
        setLocation((previous) => ({ ...previous, searchTarget }));
        setMobile(false);
        return;
      }
      const keepAncestors = !searchTarget;
      const conversationId = active?.conversationId;
      if (keepAncestors && !ancestors().length && conversationId) setAncestors([conversationId]);
      setSessionId("");
      goDirect("chat", job, undefined, keepAncestors, searchTarget);
    });
  }

  async function openParentConversation() {
    const parent = ancestors().slice(0, -1);
    if (!parent.length || !(await confirmMutableComposerNavigation())) return;
    window.history.pushState(null, "", conversationPath(parent));
    lastLocationUrl.current = window.location.href;
    setSessionId("");
    setLocation(currentLocation());
  }

  async function openArchivedConversation(conversation: Api.Conversation) {
    if (!(await confirmMutableComposerNavigation())) return false;
    runtime.client.presentArchivedConversation?.(conversation);
    setSessionId("");
    goDirect("chat", conversation.root_job);
    return true;
  }

  function closeJobDetails() {
    setDetailJob(null);
    if (location().view === "jobs" && location().job) {
      const url = new URL(window.location.href);
      url.searchParams.delete("job");
      window.history.replaceState(
        window.history.state,
        "",
        `${url.pathname}${url.search}${url.hash}`,
      );
      setLocation({ ...location(), job: null });
    }
  }

  const newChat = async () => {
    const session = await runtime.createSession({
      workType: "research",
      title: i18n.t("sidebar.newChat"),
    });
    if (!(await confirmMutableComposerNavigation())) {
      await runtime.deleteSession(session.id);
      return;
    }
    setSessionId(session.id);
    goDirect("chat", null, session.id);
  };

  const runningSessionIds = useOcgRuntimeSnapshot(
    shallowSelector(
      (snapshot) =>
        new Set(
          snapshot.sessions
            .filter((session) =>
              snapshot.messagesBySession[session.id]?.some(
                (message) => message.status === "pending" || message.status === "streaming",
              ),
            )
            .map((session) => session.id),
        ),
    ),
  );
  const listedSessions = createMemo(() =>
    sessions()
      .filter((session) => runtime.client.isListedConversation?.(session.id) ?? true)
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)),
  );
  function selectSession(id: string) {
    void confirmMutableComposerNavigation().then((allowed) => {
      if (!allowed) return;
      const selected = sessions().find((session) => session.id === id);
      setSessionId(id);
      goDirect("chat", selected?.sessionId ?? null, id);
    });
  }
  function goHome() {
    void confirmMutableComposerNavigation().then((allowed) => {
      if (!allowed) return;
      setSessionId("");
      setInspectorOpen(false);
      setPlanningFocus(undefined);
      goDirect("chat");
    });
  }
  const sidebar = createMemo(() => (
    <OcgSidebar
      conversationList={conversationList()}
      onRetryConversations={
        runtime.client.retryConversations ? () => runtime.client.retryConversations!() : undefined
      }
      sessions={listedSessions()}
      runningSessionIds={runningSessionIds()}
      api={props.client}
      activeAncestors={ancestors()}
      onOpenConversation={openNestedConversation}
      onOpenArchivedConversation={openArchivedConversation}
      onRestoreConversation={
        runtime.client.restoreConversation
          ? (conversation) => runtime.client.restoreConversation!(conversation)
          : undefined
      }
      activeSessionId={sessionId()}
      collapsed={!mobile() && collapsed()}
      drawer={mobile()}
      onToggle={() => (mobile() ? setMobile(false) : setCollapsed((value) => !value))}
      onSelect={selectSession}
      onNewChat={() => void newChat()}
      onHome={goHome}
      onRename={
        runtime.client.renameSession
          ? (id, title) => runtime.client.renameSession!(id, title)
          : undefined
      }
      onRenameChild={async (conversation, title) => {
        const child = sessions().find((item) => item.conversationId === conversation);
        if (child && runtime.client.renameSession) {
          await runtime.client.renameSession(child.id, title);
        } else {
          await props.client.editConversation(conversation, { title: title.trim() });
        }
      }}
      onHide={
        runtime.client.hideSession
          ? async (id, stopFirst) => {
              if (id === sessionId() && !(await confirmMutableComposerNavigation())) return;
              const result = await runtime.client.hideSession!(id, stopFirst);
              if (result) return result;
              if (id === sessionId()) {
                setSessionId("");
                goDirect("chat");
              }
            }
          : undefined
      }
      runtimeStatus={status()}
      activeView={location().view}
      onNavigate={(view) => go(view)}
      onSettingsClick={() => go("settings")}
    />
  ));

  return (
    <WorkspaceNavigation value={navigateHref}>
      <JobDetailsContext value={setDetailJob}>
        <ConversationErrorScope
          value={() => (location().view === "chat" ? conversationKey() : null)}
        >
          <div data-ocg-app-shell class="flex h-dvh min-h-0 w-full overflow-hidden bg-background">
            <aside
              class={cn(
                "hidden shrink-0 border-r border-sidebar-border bg-sidebar md:flex",
                collapsed() ? "w-16" : "w-72",
              )}
            >
              {!mobile() && sidebar()}
            </aside>
            <Dialog open={mobile()} onOpenChange={setMobile}>
              <DialogContent
                showCloseButton={false}
                finalFocus={() => document.getElementById("workspace-navigation-trigger")}
                class="inset-y-0 left-0 flex h-dvh w-72 max-w-[calc(100%-1.5rem)] translate-x-0 translate-y-0 flex-col gap-0 rounded-none bg-sidebar p-0 sm:max-w-72"
              >
                <DialogTitle class="sr-only">{i18n.t("nav.workspaceNavigation")}</DialogTitle>
                {mobile() && sidebar()}
              </DialogContent>
            </Dialog>
            <div class="relative flex min-h-0 min-w-0 flex-1 flex-col">
              <OcgTopbar
                session={location().view === "chat" ? session() : undefined}
                activeView={location().view}
                titleVisible={location().view === "chat" || !pageTitleInView()}
                onOpenMobileSidebar={() => setMobile(true)}
                onHome={goHome}
                onBackToParent={
                  location().view === "chat" && session() && ancestors().length > 1
                    ? () => void openParentConversation()
                    : undefined
                }
                inspectorToggle={
                  location().view === "chat" && inspectorAvailable() && !inspectorOpen() ? (
                    <Button
                      id="chat-inspector-toggle"
                      type="button"
                      variant="ghost"
                      size="icon"
                      class="size-11 shrink-0 rounded-full bg-muted/80 shadow-sm backdrop-blur-sm hover:bg-muted"
                      aria-label={i18n.t("topbar.expandInspector")}
                      title={i18n.t("topbar.expandInspector")}
                      aria-expanded="false"
                      aria-controls="chat-inspector"
                      onClick={() => setInspectorOpen(true)}
                    >
                      <PanelRight class="size-4" />
                    </Button>
                  ) : undefined
                }
              />
              {props.signOutError && (
                <DismissibleNotice
                  message={props.signOutError}
                  class="px-5 text-sm text-destructive"
                >
                  {props.signOutError}
                </DismissibleNotice>
              )}
              <PageTitleVisibilityContext value={setPageTitleInView}>
                <main
                  class={cn(
                    "flex min-h-0 flex-1 flex-col",
                    location().view === "chat"
                      ? "overflow-hidden"
                      : "ocg-subtle-scrollbar overflow-y-auto overscroll-contain",
                  )}
                >
                  <Loading fallback={<SurfaceLoading />}>
                    {location().view === "chat" ? (
                      <>
                        {opening() ? (
                          <p role="status" class="m-auto text-sm text-muted-foreground">
                            {i18n.t("app.loadingConversation")}
                          </p>
                        ) : session() ? (
                          <Show when={session()?.id} keyed>
                            <div class="flex min-h-0 flex-1 flex-col">
                              <ChatScreen
                                onOpenConversation={(job) => void openNestedConversation(job)}
                                searchTarget={location().searchTarget}
                                onOpenPlanningDecision={(decisionId) => {
                                  setPlanningFocus({ sessionId: session()!.id, decisionId });
                                  setInspectorOpen(true);
                                }}
                                session={session()!}
                              />
                            </div>
                          </Show>
                        ) : (
                          <WorkspaceHome
                            sessions={listedSessions()}
                            onSelect={selectSession}
                            onNewChat={() => void newChat()}
                            onNavigate={go}
                          />
                        )}
                      </>
                    ) : null}
                    {location().view === "jobs" && (
                      <JobsSurface
                        client={props.client}
                        active
                        job={detailJob()}
                        onJob={setDetailJob}
                        onOpenChat={(job) => {
                          setSessionId("");
                          go("chat", job);
                        }}
                      />
                    )}
                    {(location().view === "usage" || location().view === "health") && (
                      <ObservationsSurface
                        client={props.client}
                        kind={location().view === "health" ? "health" : "usage"}
                        health={props.health}
                        onJob={setDetailJob}
                      />
                    )}
                    {location().view === "logs" && <LogsSurface client={props.client} />}
                    {location().view === "settings" && (
                      <SettingsSurface
                        client={props.client}
                        sessionExpiresAtUs={props.sessionExpiresAtUs}
                        signingOut={props.signingOut}
                        onSignOut={props.onSignOut}
                        section={location().settingsSection}
                        onNavigate={goSettings}
                      />
                    )}
                  </Loading>
                </main>
              </PageTitleVisibilityContext>
            </div>
            {location().view === "chat" && inspectorAvailable() && inspectorOpen() && (
              <Loading fallback={<SurfaceLoading />}>
                <ChatInspector
                  session={session()}
                  messages={inspectorMessages() ?? []}
                  planningFocus={
                    planningFocus()?.sessionId === session()?.id ? planningFocus() : undefined
                  }
                  onClose={closeInspector}
                  onOpenJob={setDetailJob}
                />
              </Loading>
            )}
          </div>
          <Show when={detailJob() !== null}>
            <Loading fallback={null}>
              <JobDetailsDialog
                client={props.client}
                job={detailJob()}
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
            </Loading>
          </Show>
        </ConversationErrorScope>
      </JobDetailsContext>
    </WorkspaceNavigation>
  );
}
