import {
  Activity,
  BarChart3,
  ChevronDown,
  ChevronsLeft,
  FolderOpen,
  LoaderCircle,
  MessageSquare,
  MoreVertical,
  Pin,
  PinOff,
  Plus,
  ScrollText,
  Search,
  Workflow,
  X,
} from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import { WorkspaceLink } from "@/components/ocg/layout/workspace-link";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import {
  ariaState,
  bindElementRef,
  createSubscription,
  elementRef,
  type FormEvent,
  type View,
} from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import type { Project } from "../contracts";
import { runtimeStateLabel, useI18n } from "../i18n";
import type { WorkspaceView } from "../layout/view-domain";
import { useAccountEmail, useRemoteLogout } from "../login/access-boundary";
import { OcgLogo } from "../primitives/ocg-logo";
import { SELECTED_ROW } from "../primitives/selection";
import { type ApiClient, ApiError } from "../runtime/api-client";
import type { ChatSession, RuntimeStatus } from "../types";
import { AccountMenu } from "./account-menu";
import { SidebarConversationChildren } from "./conversation-children";
import { ConversationPreview, type ConversationPreviewDetails } from "./conversation-preview";
import { ConversationSearch } from "./conversation-search";
import { ConversationStatus } from "./conversation-status";
import { HoverMarquee } from "./hover-marquee";
import { usePinnedConversations } from "./sidebar-preferences";

const subscribePlatform = () => () => {};
const isApplePlatform = () => /Mac|iPhone|iPad|iPod/i.test(navigator.platform);

function ShortcutHint(props: { keys: readonly string[] }) {
  return (
    <span aria-hidden="true" class="flex shrink-0 items-center gap-0.5">
      {props.keys.map((key) => (
        <kbd class="rounded border border-current/15 px-1 font-sans text-[10px] leading-4 font-medium">
          {key}
        </kbd>
      ))}
    </span>
  );
}

const GROUP_PAGE_SIZE = 20;

const WORKSPACE_NAV: {
  target: WorkspaceView;
  labelKey:
    | "nav.usage"
    | "nav.home"
    | "nav.attention"
    | "nav.chat"
    | "nav.ledger"
    | "nav.jobExecution"
    | "nav.health"
    | "nav.logs";
  icon: typeof Search;
}[] = [
  { target: "chat", labelKey: "nav.chat", icon: MessageSquare },
  { target: "jobs", labelKey: "nav.jobExecution", icon: Workflow },
  { target: "usage", labelKey: "nav.usage", icon: BarChart3 },
  { target: "health", labelKey: "nav.health", icon: Activity },
  { target: "logs", labelKey: "nav.logs", icon: ScrollText },
];

type OcgSidebarProps = {
  sessions: ChatSession[];
  runningSessionIds?: ReadonlySet<string>;
  activeSessionId: string;
  collapsed: boolean;
  drawer?: boolean;
  onToggle: () => void;
  onSelect: (id: string) => void;
  api?: ApiClient;
  activeAncestors?: string[];
  onOpenConversation?: (conversation: string) => void;
  onNewChat: () => void;
  onHome?: () => void;
  onRename?: (id: string, title: string) => Promise<void>;
  onRenameChild?: (conversation: string, title: string) => Promise<void>;
  onHide?: (id: string) => Promise<void>;
  runtimeStatus: RuntimeStatus;
  activeView?: WorkspaceView;
  attentionCount?: number;
  onNavigate?: (view: WorkspaceView) => void;
  onSettingsClick?: () => void;
  conversationList?: "loading" | "loaded" | "failed";
  onRetryConversations?: () => void;
};

function RailButton(props: {
  label: string;
  onClick?: () => void;
  children: View;
  active?: boolean;
  shortcut?: string;
  preview?: ConversationPreviewDetails;
}) {
  const trigger = createMemo(() => (
    <button
      type="button"
      aria-label={props.label}
      title={
        props.preview
          ? undefined
          : props.shortcut
            ? `${props.label} (${props.shortcut})`
            : props.label
      }
      onClick={props.onClick}
      aria-current={(props.active === undefined ? false : props.active) ? "page" : undefined}
      class={cn(
        "flex size-9 items-center justify-center rounded-lg border border-transparent text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring",
        (props.active === undefined ? false : props.active) && cn(SELECTED_ROW, "font-medium"),
      )}
    />
  ));
  return (
    <>
      {(() => {
        if (props.preview)
          return (
            <ConversationPreview title={props.label} trigger={trigger()} {...props.preview}>
              {props.children}
            </ConversationPreview>
          );
        return (
          <Tooltip>
            <TooltipTrigger render={trigger()}>{props.children}</TooltipTrigger>
            <TooltipContent side="right">
              {props.label}
              {props.shortcut && ` (${props.shortcut})`}
            </TooltipContent>
          </Tooltip>
        );
      })()}
    </>
  );
}

export function OcgSidebar(props: OcgSidebarProps) {
  const i18n = useI18n();
  const isApple = createSubscription(subscribePlatform, isApplePlatform);
  const modifier = createMemo(() => (isApple() ? "⌘" : "Ctrl"));
  const alt = createMemo(() => (isApple() ? "⌥" : "Alt"));
  const newChatShortcut = createMemo(() => `${modifier()}+${alt()}+N`);
  const searchShortcut = createMemo(() => `${modifier()}+K`);
  const selectedPinnedTogglePinned = usePinnedConversations();
  const [projects, setProjects] = createSignal<Project[]>([]);
  const [projectError, setProjectError] = createSignal<string | null>(null);
  const [now, setNow] = createSignal((() => Date.now())());
  createEffect(
    () => [props.onNewChat],
    () => {
      const onKeyDown = (event: KeyboardEvent) => {
        if (
          event.defaultPrevented ||
          event.isComposing ||
          event.repeat ||
          event.shiftKey ||
          !(event.metaKey || event.ctrlKey) ||
          !event.altKey ||
          (!event.metaKey && event.getModifierState("AltGraph")) ||
          event.code !== "KeyN" ||
          document.querySelector('[role="dialog"]')
        )
          return;
        event.preventDefault();
        props.onNewChat();
      };
      window.addEventListener("keydown", onKeyDown);
      return () => window.removeEventListener("keydown", onKeyDown);
    },
  );
  createEffect(
    () => [props.api],
    () => {
      if (!props.api) return;
      return props.api.subscribeProjects(
        (value) => {
          setProjects(value.projects);
          setProjectError(null);
        },
        (cause) => setProjectError(cause instanceof Error ? cause.message : String(cause)),
      );
    },
  );
  createEffect(
    () => [],
    () => {
      const timer = setInterval(() => setNow(Date.now()), 60000);
      return () => clearInterval(timer);
    },
  );
  const projectNames = new Map(projects().map(({ id, name }) => [id, name]));
  const previewFor = (session: ChatSession): ConversationPreviewDetails => {
    const status =
      session.jobState ??
      ((props.runningSessionIds === undefined ? new Set<string>() : props.runningSessionIds).has(
        session.id,
      )
        ? "running"
        : undefined);
    return {
      project: session.projectId ? projectNames.get(session.projectId) : null,
      status: status && runtimeStateLabel(i18n.t, status),
      updatedAt: session.updatedAt,
    };
  };
  const groups = createMemo(() =>
    [
      ...props.sessions.reduce((groups, session) => {
        const id = session.projectId ?? "";
        const items = groups.get(id) ?? [];
        items.push(session);
        groups.set(id, items);
        return groups;
      }, new Map<string, ChatSession[]>()),
    ].map(([id, items]) => ({
      id,
      name: projectNames.get(id) ?? (id || i18n.t("sidebar.noProject")),
      items: [...items].sort(
        (a, b) =>
          Number(selectedPinnedTogglePinned().pinned.has(b.conversationId ?? b.id)) -
            Number(selectedPinnedTogglePinned().pinned.has(a.conversationId ?? a.id)) ||
          Date.parse(b.updatedAt) - Date.parse(a.updatedAt),
      ),
    })),
  );
  const relativeTime = (value: string) => {
    const seconds = Math.max(0, (now() - Date.parse(value)) / 1000);
    if (!Number.isFinite(seconds)) return "";
    const unit = seconds < 3600 ? "minute" : seconds < 86400 ? "hour" : "day";
    const amount = Math.max(
      1,
      Math.floor(seconds / (unit === "minute" ? 60 : unit === "hour" ? 3600 : 86400)),
    );
    return new Intl.NumberFormat(i18n.locale, {
      style: "unit",
      unit,
      unitDisplay: "narrow",
    }).format(amount);
  };
  const [hideTarget, setHideTarget] = createSignal<ChatSession | null>(null);
  const [hiding, setHiding] = createSignal(false);
  const [hideError, setHideError] = createSignal<string | null>(null);

  const [renameTarget, setRenameTarget] = createSignal<{
    id: string;
    kind: "session" | "child";
  } | null>(null);
  const [renameTitle, setRenameTitle] = createSignal("");
  const [renaming, setRenaming] = createSignal(false);
  const [renameError, setRenameError] = createSignal<string | null>(null);

  const [openMenuId, setOpenMenuId] = createSignal<string | null>(null);
  const [collapsedGroups, setCollapsedGroups] = createSignal<Record<string, boolean>>({});
  const [groupLimits, setGroupLimits] = createSignal<Record<string, number>>({});
  const [expandedSessions, setExpandedSessions] = createSignal<Record<string, boolean>>({});
  const [childAvailability, setChildAvailability] = createSignal<Record<string, boolean>>({});
  const activeRoot = createMemo(() => props.activeAncestors?.[0] ?? props.activeSessionId);
  const handleChildAvailability = (sessionId: string, available: boolean) => {
    setChildAvailability((current) =>
      current[sessionId] === available ? current : { ...current, [sessionId]: available },
    );
  };
  const cancelHideRef = elementRef<HTMLButtonElement>(null);

  const email = useAccountEmail();
  const logout = useRemoteLogout();

  const conversationError = (error: unknown, action: "rename" | "hide"): string => {
    if (error instanceof ApiError) {
      if (error.code === "not_found") return i18n.t("sidebar.conversationNotFound");
      if (error.code === "invalid_request" && action === "rename")
        return i18n.t("sidebar.titleInvalid");
    }
    return error instanceof Error ? error.message : String(error);
  };

  const openRename = (id: string, title: string, kind: "session" | "child") => {
    setRenameError(null);
    setRenameTitle(title);
    setRenameTarget({ id, kind });
  };

  const confirmRename = async (event?: FormEvent) => {
    event?.preventDefault();
    if (!renameTarget() || renaming()) return;
    setRenaming(true);
    setRenameError(null);
    try {
      if (renameTarget()!.kind === "session") {
        if (!props.onRename) return;
        await props.onRename(renameTarget()!.id, renameTitle());
      } else {
        if (!props.onRenameChild) return;
        await props.onRenameChild(renameTarget()!.id, renameTitle());
      }
      setRenameTarget(null);
    } catch (cause: unknown) {
      setRenameError(i18n.t("sidebar.renameFailed", { error: conversationError(cause, "rename") }));
    } finally {
      setRenaming(false);
    }
  };

  const confirmHide = async () => {
    if (!hideTarget() || !props.onHide || hiding()) return;
    setHiding(true);
    setHideError(null);
    try {
      await props.onHide(hideTarget()!.id);
      setHideTarget(null);
    } catch (cause: unknown) {
      setHideError(i18n.t("sidebar.hideFailed", { error: conversationError(cause, "hide") }));
    } finally {
      setHiding(false);
    }
  };
  const workbenchNav = WORKSPACE_NAV.filter((item) => item.target !== "chat");
  const hasNav = createMemo(() => Boolean(props.onNavigate));
  const navLabel = (item: (typeof WORKSPACE_NAV)[number]) =>
    item.target === "jobs" ? i18n.t("nav.jobExecution") : i18n.t(item.labelKey);

  const renderWorkspaceItem = (item: (typeof WORKSPACE_NAV)[number]) => {
    const Icon = item.icon;
    if (!props.onNavigate) return null;
    const active = (props.activeView === undefined ? "chat" : props.activeView) === item.target;
    const label = navLabel(item);
    return (
      <li>
        <button
          type="button"
          onClick={() => props!.onNavigate!(item.target)}
          aria-current={active ? "page" : undefined}
          class={cn(
            "group flex min-h-9 w-full items-center gap-3 rounded-lg px-3 py-1.5 text-left focus-visible:outline-2 focus-visible:outline-ring",
            TYPOGRAPHY_SCALE.body_sm,
            "leading-5 transition-colors",
            active
              ? cn(SELECTED_ROW, "font-medium")
              : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
          )}
        >
          <Icon class={cn("size-4 shrink-0", active && "text-primary")} aria-hidden="true" />
          <span class="min-w-0 flex-1 truncate">{label}</span>
          {item.target === "attention" &&
            (props.attentionCount === undefined ? 0 : props.attentionCount) > 0 && (
              <span
                class={cn(
                  "shrink-0 rounded-md bg-muted px-1.5",
                  TYPOGRAPHY_SCALE.caption,
                  "font-semibold tabular-nums text-muted-foreground",
                )}
              >
                <span aria-hidden="true">
                  {(props.attentionCount === undefined ? 0 : props.attentionCount) > 99
                    ? "99+"
                    : props.attentionCount === undefined
                      ? 0
                      : props.attentionCount}
                </span>
                <span class="sr-only">
                  {i18n.t("sidebar.attentionBadge", {
                    count: props.attentionCount === undefined ? 0 : props.attentionCount,
                  })}
                </span>
              </span>
            )}
        </button>
      </li>
    );
  };

  return (
    <>
      {(() => {
        if (props.collapsed) {
          return (
            <TooltipProvider delay={100}>
              <div class="flex h-full w-full flex-col items-center gap-1 px-2 py-3">
                <WorkspaceLink
                  href="/"
                  onNavigate={props.onHome}
                  aria-label={i18n.t("nav.home")}
                  class="mb-2 rounded-sm focus-visible:outline-2 focus-visible:outline-ring"
                >
                  <OcgLogo compact />
                </WorkspaceLink>
                <RailButton label={i18n.t("sidebar.expand")} onClick={props.onToggle}>
                  <ChevronsLeft class="size-4 rotate-180" />
                </RailButton>
                <RailButton
                  label={i18n.t("sidebar.newChat")}
                  shortcut={newChatShortcut()}
                  onClick={props.onNewChat}
                >
                  <Plus class="size-4" />
                </RailButton>
                <ConversationSearch
                  sessions={props.sessions}
                  activeSessionId={props.activeSessionId}
                  onSelect={props.onSelect}
                  onOpenConversation={props.onOpenConversation}
                  api={props.api}
                  conversationList={props.conversationList}
                  onRetryConversations={props.onRetryConversations}
                  trigger={
                    <button
                      type="button"
                      aria-label={i18n.t("sidebar.searchConversations")}
                      title={`${i18n.t("sidebar.searchConversations")} (${searchShortcut()})`}
                      aria-keyshortcuts={isApple() ? "Meta+K" : "Control+K"}
                      class="flex size-9 items-center justify-center rounded-md border border-transparent text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
                    >
                      <Search class="size-4" aria-hidden="true" />
                    </button>
                  }
                />
                {hasNav() && (
                  <>
                    <div class="my-2 h-px w-8 bg-border" aria-hidden="true" />
                    {WORKSPACE_NAV.map((item) => {
                      const Icon = item.icon;
                      if (!props.onNavigate) return null;
                      const label = navLabel(item);
                      return (
                        <RailButton
                          label={
                            item.target === "attention" &&
                            (props.attentionCount === undefined ? 0 : props.attentionCount) > 0
                              ? i18n.t("sidebar.attentionBadgeShort", {
                                  label,
                                  count:
                                    props.attentionCount === undefined ? 0 : props.attentionCount,
                                })
                              : label
                          }
                          active={
                            (props.activeView === undefined ? "chat" : props.activeView) ===
                            item.target
                          }
                          onClick={() => props!.onNavigate!(item.target)}
                        >
                          <span class="relative">
                            <Icon class="size-4" />
                            {item.target === "attention" &&
                              (props.attentionCount === undefined ? 0 : props.attentionCount) >
                                0 && (
                                <span
                                  aria-hidden="true"
                                  class="absolute -top-1 -right-1.5 min-w-[14px] rounded-full bg-muted-foreground/80 px-0.5 text-center text-[9px] leading-[14px] font-semibold text-background"
                                >
                                  {(props.attentionCount === undefined ? 0 : props.attentionCount) >
                                  9
                                    ? "9+"
                                    : props.attentionCount === undefined
                                      ? 0
                                      : props.attentionCount}
                                </span>
                              )}
                          </span>
                        </RailButton>
                      );
                    })}
                  </>
                )}
                <div class="my-2 h-px w-8 bg-border" aria-hidden="true" />
                <ScrollArea
                  class="min-h-0 w-full flex-1"
                  viewportProps={{ class: "ocg-subtle-scrollbar" }}
                  contentProps={{ class: "flex flex-col items-center gap-1" }}
                >
                  {props.sessions.map((session) => (
                    <RailButton
                      label={session.title || i18n.t("sidebar.newChat")}
                      preview={previewFor(session)}
                      active={
                        (props.activeView === undefined ? "chat" : props.activeView) === "chat" &&
                        (session.id === props.activeSessionId ||
                          session.conversationId === activeRoot() ||
                          session.id === activeRoot())
                      }
                      onClick={() => props.onSelect(session.id)}
                    >
                      <span aria-hidden="true" class="text-xs font-medium uppercase">
                        {(session.title || i18n.t("sidebar.newChat")).trim().charAt(0)}
                      </span>
                    </RailButton>
                  ))}
                  {props.conversationList === "loading" &&
                    Array.from({ length: 5 }, (_, i) => (
                      <Skeleton class="size-9 shrink-0 rounded-md" />
                    ))}
                </ScrollArea>
                <div class="mt-2 w-full border-t border-border pt-2">
                  <div class="flex w-full justify-center">
                    <AccountMenu
                      variant="rail"
                      email={email}
                      onLogout={
                        logout
                          ? () => {
                              void logout();
                            }
                          : undefined
                      }
                      onSettingsClick={props.onSettingsClick}
                    />
                  </div>
                </div>
              </div>
            </TooltipProvider>
          );
        }

        return (
          <TooltipProvider delay={100}>
            <div class="flex h-full w-full flex-col bg-sidebar text-sidebar-foreground">
              <div class="flex h-16 shrink-0 items-center gap-2 px-5">
                <WorkspaceLink
                  href="/"
                  onNavigate={props.onHome}
                  aria-label={i18n.t("nav.home")}
                  class="min-w-0 flex-1 rounded-sm focus-visible:outline-2 focus-visible:outline-ring"
                >
                  <OcgLogo />
                </WorkspaceLink>
                <Tooltip>
                  <TooltipTrigger
                    render={
                      <Button
                        variant="ghost"
                        size="icon-sm"
                        onClick={props.onToggle}
                        aria-label={i18n.t(
                          (props.drawer === undefined ? false : props.drawer)
                            ? "app.closeNavigation"
                            : "sidebar.collapse",
                        )}
                        aria-expanded="true"
                        title={i18n.t(
                          (props.drawer === undefined ? false : props.drawer)
                            ? "app.closeNavigation"
                            : "sidebar.collapse",
                        )}
                      >
                        {(props.drawer === undefined ? false : props.drawer) ? (
                          <X class="size-4" />
                        ) : (
                          <ChevronsLeft class="size-4" />
                        )}
                      </Button>
                    }
                  />
                  <TooltipContent side="right">
                    {i18n.t(
                      (props.drawer === undefined ? false : props.drawer)
                        ? "app.closeNavigation"
                        : "sidebar.collapse",
                    )}
                  </TooltipContent>
                </Tooltip>
              </div>

              <div class="space-y-1 border-b border-sidebar-border px-3 pb-3">
                <Button
                  variant="outline"
                  size="sm"
                  class="h-10 w-full justify-start gap-2.5 rounded-lg bg-card px-3 text-sm font-medium tracking-normal normal-case shadow-xs"
                  onClick={props.onNewChat}
                  aria-keyshortcuts={isApple() ? "Meta+Alt+N" : "Control+Alt+N"}
                >
                  <Plus class="size-4 text-primary" aria-hidden="true" />
                  <span class="min-w-0 flex-1 truncate text-left">{i18n.t("sidebar.newChat")}</span>
                  <span class="text-muted-foreground">
                    <ShortcutHint keys={[modifier(), alt(), "N"]} />
                  </span>
                </Button>
                <ConversationSearch
                  sessions={props.sessions}
                  activeSessionId={props.activeSessionId}
                  onSelect={props.onSelect}
                  onOpenConversation={props.onOpenConversation}
                  api={props.api}
                  conversationList={props.conversationList}
                  onRetryConversations={props.onRetryConversations}
                  trigger={
                    <Button
                      variant="ghost"
                      size="sm"
                      aria-label={i18n.t("sidebar.searchConversations")}
                      title={`${i18n.t("sidebar.searchConversations")} (${searchShortcut()})`}
                      aria-keyshortcuts={isApple() ? "Meta+K" : "Control+K"}
                      class="h-9 w-full justify-start gap-2.5 rounded-lg px-3 text-sm font-normal tracking-normal text-muted-foreground normal-case"
                    >
                      <Search class="size-4" aria-hidden="true" />
                      <span class="min-w-0 flex-1 truncate text-left">
                        {i18n.t("sidebar.searchConversations")}
                      </span>
                      <ShortcutHint keys={[modifier(), "K"]} />
                    </Button>
                  }
                />
              </div>

              <ScrollArea
                role="navigation"
                aria-label={i18n.t("nav.workspaceNavigation")}
                class="min-h-0 flex-1"
                viewportProps={{ class: "ocg-subtle-scrollbar" }}
                contentProps={{
                  class: "px-3 pb-3",
                  style: { "min-width": `${0}px`, width: "100%" },
                }}
              >
                {hasNav() && workbenchNav.length > 0 && (
                  <div class="border-b border-sidebar-border py-3">
                    <details open class="group/workbench" data-workbench-menu="">
                      <summary class="flex h-8 cursor-pointer items-center gap-2 rounded-md px-3 text-xs font-semibold text-muted-foreground select-none hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring">
                        <span class="min-w-0 flex-1 truncate">{i18n.t("nav.workbench")}</span>
                        <ChevronDown
                          class="size-3.5 -rotate-90 transition-transform group-open/workbench:rotate-0 motion-reduce:transition-none"
                          aria-hidden="true"
                        />
                      </summary>
                      <ul class="mt-0.5 space-y-px">{workbenchNav.map(renderWorkspaceItem)}</ul>
                    </details>
                  </div>
                )}
                {projectError() && (
                  <DismissibleNotice message={projectError()!} class="mb-2 px-2 text-xs" />
                )}
                <ul aria-label={i18n.t("nav.chatSessions")} class="mt-3 space-y-3">
                  {groups().map((group) => {
                    const groupLimit = groupLimits()[group.id] ?? GROUP_PAGE_SIZE;
                    const visibleItems = group.items.filter(
                      (session, index) =>
                        index < groupLimit ||
                        session.id === props.activeSessionId ||
                        session.conversationId === activeRoot() ||
                        session.id === activeRoot(),
                    );
                    return (
                      <li class="min-w-0">
                        <button
                          type="button"
                          class="mb-1 flex h-8 w-full min-w-0 items-center gap-2 rounded-lg px-3 text-left text-[13px] text-muted-foreground hover:bg-sidebar-accent focus-visible:outline-ring"
                          title={group.name}
                          aria-expanded={ariaState(!collapsedGroups()[group.id])}
                          onClick={() =>
                            setCollapsedGroups((current) => ({
                              ...current,
                              [group.id]: !current[group.id],
                            }))
                          }
                        >
                          <FolderOpen class="size-4 shrink-0" aria-hidden="true" />
                          <span class="min-w-0 flex-1 truncate">{group.name}</span>
                          <ChevronDown
                            class={cn(
                              "size-3.5 shrink-0 transition-transform",
                              collapsedGroups()[group.id] && "-rotate-90",
                            )}
                            aria-hidden="true"
                          />
                        </button>
                        {!collapsedGroups()[group.id] && (
                          <ul class="space-y-0.5">
                            {visibleItems.map((session) => {
                              const active =
                                (props.activeView === undefined ? "chat" : props.activeView) ===
                                  "chat" &&
                                (session.id === props.activeSessionId ||
                                  session.conversationId === activeRoot() ||
                                  session.id === activeRoot());
                              const isMenuOpen = openMenuId() === session.id;
                              const expanded = expandedSessions()[session.id] ?? active;
                              const canLoadChildren = Boolean(
                                props.api && props.onOpenConversation && session.conversationId,
                              );
                              const canExpand =
                                canLoadChildren &&
                                (session.hasChildren ?? childAvailability()[session.id] === true);
                              const jobState =
                                session.jobState ??
                                ((props.runningSessionIds === undefined
                                  ? new Set<string>()
                                  : props.runningSessionIds
                                ).has(session.id)
                                  ? "running"
                                  : undefined);
                              const preview = previewFor(session);
                              const status =
                                jobState === "queued" || jobState === "running" ? (
                                  <ConversationStatus state={jobState} />
                                ) : null;
                              const pinId = session.conversationId ?? session.id;
                              const isPinned = selectedPinnedTogglePinned().pinned.has(pinId);
                              const hasActions = Boolean(
                                session.conversationId && (props.onRename || props.onHide),
                              );
                              const actions =
                                "opacity-0 group-hover/session:opacity-100 group-focus-within/session:opacity-100 [@media(hover:none)]:opacity-100";
                              return (
                                <li class="min-w-0" data-sidebar-level="1">
                                  <ConversationPreview
                                    title={session.title || i18n.t("sidebar.newChat")}
                                    {...preview}
                                    suppressed={isMenuOpen}
                                    trigger={
                                      <div
                                        class={cn(
                                          "group/session relative flex h-9 min-w-0 items-center rounded-lg text-sidebar-foreground/90 transition-colors hover:bg-sidebar-accent",
                                          active && cn(SELECTED_ROW, "font-medium"),
                                        )}
                                      />
                                    }
                                  >
                                    <span class="flex w-9 shrink-0 items-center justify-center">
                                      {canExpand ? (
                                        <button
                                          type="button"
                                          aria-expanded={ariaState(expanded)}
                                          aria-label={i18n.t(
                                            expanded
                                              ? "sidebar.hideChildren"
                                              : "sidebar.showChildren",
                                            { title: session.title || i18n.t("sidebar.newChat") },
                                          )}
                                          class="flex size-6 items-center justify-center rounded text-muted-foreground hover:text-sidebar-foreground focus-visible:outline-ring"
                                          onClick={() =>
                                            setExpandedSessions((current) => ({
                                              ...current,
                                              [session.id]: !expanded,
                                            }))
                                          }
                                        >
                                          <ChevronDown
                                            class={cn(
                                              "size-3.5 transition-transform motion-reduce:transition-none",
                                              !expanded && "-rotate-90",
                                            )}
                                            aria-hidden="true"
                                          />
                                        </button>
                                      ) : (
                                        status
                                      )}
                                    </span>
                                    <button
                                      type="button"
                                      onClick={() => props.onSelect(session.id)}
                                      onDblClick={() => {
                                        if (props.onRename && session.sessionId)
                                          openRename(session.id, session.title || "", "session");
                                      }}
                                      aria-current={active ? "true" : undefined}
                                      class="flex h-full min-w-0 flex-1 items-center gap-1.5 pr-1 text-left text-[13px] leading-5 outline-none focus-visible:rounded focus-visible:ring-2 focus-visible:ring-sidebar-ring"
                                    >
                                      {canExpand && status}
                                      <HoverMarquee class="min-w-0 flex-1">
                                        {session.title || i18n.t("sidebar.newChat")}
                                      </HoverMarquee>
                                    </button>
                                    <div
                                      class={cn(
                                        "relative mr-2 flex h-full shrink-0 items-center justify-end",
                                        hasActions && "min-w-12",
                                      )}
                                    >
                                      <span
                                        class={cn(
                                          "flex items-center gap-1 text-xs whitespace-nowrap tabular-nums text-muted-foreground transition-opacity",
                                          hasActions &&
                                            "group-hover/session:opacity-0 group-focus-within/session:opacity-0 [@media(hover:none)]:opacity-0",
                                          isMenuOpen && "opacity-0",
                                        )}
                                      >
                                        <time datetime={session.updatedAt}>
                                          {relativeTime(session.updatedAt)}
                                        </time>
                                        {isPinned && (
                                          <Pin
                                            class="size-3"
                                            aria-label={i18n.t("sidebar.pinned")}
                                          />
                                        )}
                                      </span>
                                      {hasActions && (
                                        <div
                                          class={cn(
                                            "absolute inset-0 flex items-center justify-end gap-0.5 transition-opacity",
                                            isMenuOpen ? "opacity-100" : actions,
                                          )}
                                        >
                                          <button
                                            type="button"
                                            class="flex size-6 items-center justify-center rounded text-muted-foreground hover:text-sidebar-foreground focus-visible:outline-ring"
                                            aria-label={i18n.t(
                                              isPinned ? "sidebar.unpin" : "sidebar.pin",
                                            )}
                                            title={i18n.t(
                                              isPinned ? "sidebar.unpin" : "sidebar.pin",
                                            )}
                                            onClick={() =>
                                              selectedPinnedTogglePinned().toggle(pinId)
                                            }
                                          >
                                            {isPinned ? (
                                              <PinOff class="size-3.5" />
                                            ) : (
                                              <Pin class="size-3.5" />
                                            )}
                                          </button>
                                          {(props.onRename || props.onHide) &&
                                            session.sessionId && (
                                              <DropdownMenu
                                                open={isMenuOpen}
                                                onOpenChange={(open) =>
                                                  setOpenMenuId(open ? session.id : null)
                                                }
                                              >
                                                <DropdownMenuTrigger asChild>
                                                  <button
                                                    type="button"
                                                    aria-label={i18n.t("sidebar.chatActions", {
                                                      title:
                                                        session.title || i18n.t("sidebar.newChat"),
                                                    })}
                                                    class="flex size-5 items-center justify-center rounded text-muted-foreground hover:text-sidebar-foreground focus-visible:outline-ring"
                                                  >
                                                    <MoreVertical class="size-3.5" />
                                                  </button>
                                                </DropdownMenuTrigger>
                                                <DropdownMenuContent align="end">
                                                  {props.onRename && (
                                                    <DropdownMenuItem
                                                      onSelect={() =>
                                                        openRename(
                                                          session.id,
                                                          session.title || "",
                                                          "session",
                                                        )
                                                      }
                                                    >
                                                      {i18n.t("sidebar.renameChat")}
                                                    </DropdownMenuItem>
                                                  )}
                                                  {props.onHide && (
                                                    <DropdownMenuItem
                                                      onSelect={() => {
                                                        setHideError(null);
                                                        setHideTarget(session);
                                                      }}
                                                    >
                                                      {i18n.t("sidebar.hideChat")}
                                                    </DropdownMenuItem>
                                                  )}
                                                </DropdownMenuContent>
                                              </DropdownMenu>
                                            )}
                                        </div>
                                      )}
                                    </div>
                                  </ConversationPreview>
                                  {canExpand &&
                                    expanded &&
                                    props.api &&
                                    props.onOpenConversation &&
                                    session.conversationId && (
                                      <SidebarConversationChildren
                                        conversation={session.conversationId}
                                        api={props.api}
                                        activeChild={props.activeAncestors?.[1]}
                                        project={preview.project}
                                        expanded
                                        sessionId={session.id}
                                        onAvailabilityChange={handleChildAvailability}
                                        onOpen={props.onOpenConversation}
                                        onRename={
                                          props.onRenameChild
                                            ? (child) =>
                                                openRename(child.id, child.title || "", "child")
                                            : undefined
                                        }
                                      />
                                    )}
                                </li>
                              );
                            })}
                            {visibleItems.length < group.items.length && (
                              <li>
                                <button
                                  type="button"
                                  class="ml-9 rounded py-1 text-xs text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-foreground focus-visible:outline-ring"
                                  onClick={() =>
                                    setGroupLimits((current) => ({
                                      ...current,
                                      [group.id]:
                                        (current[group.id] ?? GROUP_PAGE_SIZE) + GROUP_PAGE_SIZE,
                                    }))
                                  }
                                >
                                  {i18n.t("common.loadMore")}
                                </button>
                              </li>
                            )}
                          </ul>
                        )}
                      </li>
                    );
                  })}
                </ul>
                {props.conversationList === "loading" && (
                  <ul aria-label={i18n.t("common.loading")} class="mt-2 space-y-1">
                    {Array.from({ length: 5 }, (_, i) => (
                      <li>
                        <Skeleton class="h-8 w-full rounded-lg" />
                      </li>
                    ))}
                  </ul>
                )}
                {props.conversationList === "failed" && (
                  <div
                    role="alert"
                    class="mt-2 flex items-center justify-between gap-2 rounded-lg border border-destructive/20 bg-destructive/10 px-3 py-2 text-xs text-destructive"
                  >
                    <span class="truncate">{i18n.t("sidebar.loadFailed")}</span>
                    {props.onRetryConversations && (
                      <Button
                        variant="outline"
                        size="sm"
                        class="h-7 shrink-0 px-2 text-xs"
                        onClick={props.onRetryConversations}
                      >
                        {i18n.t("common.retry")}
                      </Button>
                    )}
                  </div>
                )}
                {props.conversationList === "loaded" && props.sessions.length === 0 && (
                  <p class="my-1 rounded-lg border border-dashed border-sidebar-border px-3 py-4 text-center text-xs leading-5 text-muted-foreground">
                    {i18n.t("sidebar.searchNoConversations")}
                  </p>
                )}
              </ScrollArea>

              <div class="relative border-t border-border px-2 py-2">
                <AccountMenu
                  email={email}
                  onLogout={
                    logout
                      ? () => {
                          void logout();
                        }
                      : undefined
                  }
                  onSettingsClick={props.onSettingsClick}
                />
              </div>
            </div>
            <Dialog
              open={renameTarget() !== null}
              onOpenChange={(open) => {
                if (!open && !renaming()) setRenameTarget(null);
              }}
            >
              <DialogContent showCloseButton={!renaming()} class="rounded-lg">
                <DialogHeader>
                  <DialogTitle class="normal-case tracking-normal">
                    {i18n.t("sidebar.renameChatTitle")}
                  </DialogTitle>
                </DialogHeader>
                <form onSubmit={(e) => void confirmRename(e)} class="flex flex-col gap-4">
                  <Input
                    aria-label={i18n.t("sidebar.renameChatTitle")}
                    value={renameTitle()}
                    onInput={(e) => setRenameTitle(e.target.value)}
                    disabled={renaming()}
                    autofocus
                  />
                  {renameError() && (
                    <DismissibleNotice
                      message={renameError()!}
                      class="break-words text-xs text-destructive"
                    >
                      {renameError()}
                    </DismissibleNotice>
                  )}
                  <DialogFooter>
                    <Button
                      type="button"
                      variant="outline"
                      disabled={renaming()}
                      onClick={() => setRenameTarget(null)}
                    >
                      {i18n.t("common.cancel")}
                    </Button>
                    <Button type="submit" disabled={renaming()}>
                      {renaming() && (
                        <LoaderCircle class="size-3.5 animate-spin" aria-hidden="true" />
                      )}
                      {i18n.t("common.save")}
                    </Button>
                  </DialogFooter>
                </form>
              </DialogContent>
            </Dialog>
            <Dialog
              open={hideTarget() !== null}
              onOpenChange={(open) => {
                if (!open && !hiding()) setHideTarget(null);
              }}
            >
              <DialogContent
                initialFocus={() => cancelHideRef.current}
                showCloseButton={!hiding()}
                class="rounded-lg"
              >
                <DialogHeader>
                  <DialogTitle class="normal-case tracking-normal">
                    {i18n.t("sidebar.hideChatTitle")}
                  </DialogTitle>
                  <DialogDescription>
                    {i18n.t("sidebar.hideChatBody", {
                      title: hideTarget()?.title || i18n.t("sidebar.newChat"),
                    })}
                  </DialogDescription>
                </DialogHeader>
                {hideError() && (
                  <DismissibleNotice
                    message={hideError()!}
                    class="break-words text-xs text-destructive"
                  >
                    {hideError()}
                  </DismissibleNotice>
                )}
                <DialogFooter>
                  <Button
                    ref={bindElementRef(cancelHideRef)}
                    variant="outline"
                    disabled={hiding()}
                    onClick={() => setHideTarget(null)}
                  >
                    {i18n.t("common.cancel")}
                  </Button>
                  <Button
                    variant="destructive"
                    disabled={hiding()}
                    onClick={() => void confirmHide()}
                  >
                    {hiding() && <LoaderCircle class="size-3.5 animate-spin" aria-hidden="true" />}
                    {i18n.t(hiding() ? "sidebar.hiding" : "sidebar.hideChat")}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </TooltipProvider>
        );
      })()}
    </>
  );
}
