"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  ChevronsLeft,
  ChevronDown,
  Activity,
  BarChart3,
  LayoutGrid,
  MessageSquare,
  Plus,
  Search,
  ScrollText,
  Workflow,
  LoaderCircle,
  MoreVertical,
  FolderOpen,
  Pin,
  PinOff,
  Archive,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
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
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import type { ChatSession, RuntimeStatus } from "../types";
import type { RuntimeAuthority } from "../runtime/runtime-types";
import { ControlTransportError, type HttpCanonicalControlClient } from "../runtime/control-client";
import { useAccountEmail, useRemoteLogout } from "../login/access-boundary";
import { AccountMenu } from "./account-menu";
import { ConversationSearch } from "./conversation-search";
import { useI18n } from "../i18n";
import type { WorkspaceView } from "../layout/view-domain";
import { OcgLogo } from "../primitives/ocg-logo";
import { SELECTED_ROW } from "../primitives/selection";
import type { ControlProject } from "../contracts";
import { usePinnedConversations } from "./sidebar-preferences";
import { SidebarConversationChildren } from "./conversation-children";

const WORKSPACE_NAV: {
  target: WorkspaceView;
  labelKey:
    | "nav.usage"
    | "nav.home"
    | "nav.attention"
    | "nav.chat"
    | "nav.controlCenter"
    | "nav.ledger"
    | "nav.jobExecution"
    | "nav.health"
    | "nav.logs"
    | "nav.canonical";
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
  onToggle: () => void;
  onSelect: (id: string) => void;
  control?: HttpCanonicalControlClient;
  activeAncestors?: string[];
  onOpenConversation?: (conversation: string) => void;
  onNewChat: () => void;
  onHome?: () => void;
  onRename?: (id: string, title: string) => Promise<void>;
  onHide?: (id: string) => Promise<void>;
  runtimeStatus: RuntimeStatus;
  /** Which runtime answers Chat, so the footer never claims a fixture runtime. */
  runtimeAuthority: RuntimeAuthority;
  /** Active top-level workspace, used to highlight the navigation group. */
  activeView?: WorkspaceView;
  /** Unresolved attention count shown as a quiet badge next to Attention. */
  attentionCount?: number;
  /**
   * Opens a workspace, or closes it back to the chat root when it is already
   * open. Omitting it hides the workspace navigation group entirely, which is
   * how a page renders the sidebar without shell navigation.
   */
  onNavigate?: (view: WorkspaceView) => void;
  /** Opens Settings from the account menu. Omitting it hides that item. */
  onSettingsClick?: () => void;
};

function RailButton({
  label,
  onClick,
  children,
  active = false,
}: {
  label: string;
  onClick?: () => void;
  children: React.ReactNode;
  active?: boolean;
}) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            aria-label={label}
            title={label}
            onClick={onClick}
            className={cn(
              "flex size-9 items-center justify-center rounded-md border border-transparent text-muted-foreground transition-colors hover:bg-muted hover:text-foreground",
              active && cn(SELECTED_ROW, "font-medium"),
            )}
          >
            {children}
          </button>
        }
      />
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}

export function OcgSidebar({
  sessions,
  runningSessionIds = new Set<string>(),
  activeSessionId,
  collapsed,
  onToggle,
  onSelect,
  control,
  activeAncestors,
  onOpenConversation,
  onNewChat,
  onHome,
  onRename,
  onHide,
  runtimeAuthority,
  activeView = "chat",
  attentionCount = 0,
  onNavigate,
  onSettingsClick,
}: OcgSidebarProps) {
  const { t, locale } = useI18n();
  const { pinned, toggle: togglePinned } = usePinnedConversations();
  const [projects, setProjects] = useState<ControlProject[]>([]);
  const [projectError, setProjectError] = useState<string | null>(null);
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!control) return;
    return control.subscribeProjects(
      (value) => {
        setProjects(value.projects);
        setProjectError(null);
      },
      (cause) => setProjectError(cause instanceof Error ? cause.message : String(cause)),
    );
  }, [control]);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 60000);
    return () => clearInterval(timer);
  }, []);
  const groups = [
    ...sessions.reduce((groups, session) => {
      const id = session.projectId ?? "";
      const items = groups.get(id) ?? [];
      items.push(session);
      groups.set(id, items);
      return groups;
    }, new Map<string, ChatSession[]>()),
  ].map(([id, items]) => ({
    id,
    name: projects.find((project) => project.id === id)?.name ?? (id || t("sidebar.noProject")),
    items: [...items].sort(
      (a, b) =>
        Number(pinned.has(b.conversationId ?? b.id)) -
          Number(pinned.has(a.conversationId ?? a.id)) ||
        Date.parse(b.updatedAt) - Date.parse(a.updatedAt),
    ),
  }));
  const relativeTime = (value: string) => {
    const seconds = Math.max(0, (now - Date.parse(value)) / 1000);
    if (!Number.isFinite(seconds)) return "";
    const unit = seconds < 3600 ? "minute" : seconds < 86400 ? "hour" : "day";
    const amount = Math.max(
      1,
      Math.floor(seconds / (unit === "minute" ? 60 : unit === "hour" ? 3600 : 86400)),
    );
    return new Intl.NumberFormat(locale, { style: "unit", unit, unitDisplay: "narrow" }).format(
      amount,
    );
  };
  const [hideTarget, setHideTarget] = useState<ChatSession | null>(null);
  const [hiding, setHiding] = useState(false);
  const [hideError, setHideError] = useState<string | null>(null);

  const [renameTarget, setRenameTarget] = useState<ChatSession | null>(null);
  const [renameTitle, setRenameTitle] = useState("");
  const [renaming, setRenaming] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);

  const [openMenuId, setOpenMenuId] = useState<string | null>(null);
  const [expandedSessions, setExpandedSessions] = useState<Record<string, boolean>>({});
  const [childAvailability, setChildAvailability] = useState<Record<string, boolean>>({});
  const activeRoot = activeAncestors?.[0] ?? activeSessionId;
  const handleChildAvailability = useCallback((sessionId: string, available: boolean) => {
    setChildAvailability((current) =>
      current[sessionId] === available ? current : { ...current, [sessionId]: available },
    );
  }, []);
  const cancelHideRef = useRef<HTMLButtonElement>(null);

  const email = useAccountEmail();
  const logout = useRemoteLogout();

  const conversationError = (error: unknown, action: "rename" | "hide"): string => {
    if (error instanceof ControlTransportError) {
      if (error.code === "not_found") return t("sidebar.conversationNotFound");
      if (error.code === "invalid_request" && action === "rename") return t("sidebar.titleInvalid");
    }
    return error instanceof Error ? error.message : String(error);
  };

  const confirmRename = async (event?: React.FormEvent) => {
    event?.preventDefault();
    if (!renameTarget || !onRename || renaming) return;
    setRenaming(true);
    setRenameError(null);
    try {
      await onRename(renameTarget.id, renameTitle);
      setRenameTarget(null);
    } catch (cause: unknown) {
      setRenameError(t("sidebar.renameFailed", { error: conversationError(cause, "rename") }));
    } finally {
      setRenaming(false);
    }
  };

  const confirmHide = async () => {
    if (!hideTarget || !onHide || hiding) return;
    setHiding(true);
    setHideError(null);
    try {
      await onHide(hideTarget.id);
      setHideTarget(null);
    } catch (cause: unknown) {
      setHideError(t("sidebar.hideFailed", { error: conversationError(cause, "hide") }));
    } finally {
      setHiding(false);
    }
  };
  const canonicalMode = runtimeAuthority === "canonical";
  const workspaceNav = canonicalMode ? WORKSPACE_NAV : WORKSPACE_NAV;
  // In canonical mode every non-Chat destination lives in the Workbench menu
  // while the conversation list and New chat serve Chat directly.
  const workbenchNav = canonicalMode ? workspaceNav.filter((item) => item.target !== "chat") : [];
  const hasNav = Boolean(onNavigate);
  const navLabel = (item: (typeof WORKSPACE_NAV)[number]) =>
    item.target === "jobs" ? t("nav.jobExecution") : t(item.labelKey);

  const renderWorkspaceItem = (item: (typeof WORKSPACE_NAV)[number]) => {
    const Icon = item.icon;
    if (!onNavigate) return null;
    const active = activeView === item.target;
    const label = navLabel(item);
    return (
      <li key={item.target}>
        <button
          type="button"
          onClick={() => onNavigate(item.target)}
          aria-current={active ? "page" : undefined}
          className={cn(
            "group flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left",
            TYPOGRAPHY_SCALE.body_sm,
            "leading-5 transition-colors",
            active
              ? cn(SELECTED_ROW, "font-medium")
              : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
          )}
        >
          <Icon className="size-3.5 shrink-0" aria-hidden="true" />
          <span className="min-w-0 flex-1 truncate">{label}</span>
          {item.target === "attention" && attentionCount > 0 && (
            <span
              aria-label={t("sidebar.attentionBadge", { count: attentionCount })}
              className={cn(
                "shrink-0 rounded-md bg-muted px-1.5",
                TYPOGRAPHY_SCALE.caption,
                "font-semibold tabular-nums text-muted-foreground",
              )}
            >
              {attentionCount > 99 ? "99+" : attentionCount}
            </span>
          )}
        </button>
      </li>
    );
  };

  // New chat and the Workbench menu sit below the header controls.
  const primaryControls = (
    <>
      <div className="flex items-center gap-1 px-2 pb-2">
        <Button
          variant="ghost"
          size="sm"
          className="h-11 min-w-0 flex-1 justify-start rounded-lg px-3 text-sm font-normal"
          onClick={onNewChat}
        >
          <Plus className="size-3.5" data-icon="inline-start" />
          {t("sidebar.newChat")}
        </Button>
      </div>

      {hasNav && workbenchNav.length > 0 && (
        /*
          Canonical mode keeps Chat as the single primary destination and folds
          every other destination into this compact Workbench menu. Each item
          keeps its icon, label, attention badge and active state.
        */
        <details
          className="group/workbench px-2 pb-2"
          open={workbenchNav.some((item) => item.target === activeView) || undefined}
          data-workbench-menu=""
        >
          <summary
            className={cn(
              "group flex min-h-11 cursor-pointer items-center gap-2 rounded-lg px-3 py-2 select-none",
              TYPOGRAPHY_SCALE.body_sm,
              "text-muted-foreground hover:bg-muted/60",
            )}
          >
            <LayoutGrid className="size-3.5 shrink-0" aria-hidden="true" />
            <span className="min-w-0 flex-1 truncate">{t("nav.workbench")}</span>
            <ChevronDown
              className="size-3.5 shrink-0 -rotate-90 text-muted-foreground transition-transform duration-[180ms] group-open/workbench:rotate-0 motion-reduce:transition-none"
              aria-hidden="true"
            />
          </summary>
          <ul className="mt-1 flex flex-col gap-px pl-2">
            {workbenchNav.map(renderWorkspaceItem)}
          </ul>
        </details>
      )}
    </>
  );

  if (collapsed) {
    return (
      <TooltipProvider delay={100}>
        <div className="flex h-full w-full flex-col items-center gap-1 px-2 py-3">
          <Link
            href="/"
            prefetch={false}
            onNavigate={
              onHome
                ? (event) => {
                    event.preventDefault();
                    onHome();
                  }
                : undefined
            }
            aria-label={t("nav.home")}
            className="mb-2 rounded-sm focus-visible:outline-2 focus-visible:outline-ring"
          >
            <OcgLogo compact />
          </Link>
          <RailButton label={t("sidebar.expand")} onClick={onToggle}>
            <ChevronsLeft className="size-4 rotate-180" />
          </RailButton>
          <RailButton label={t("sidebar.newChat")} onClick={onNewChat}>
            <Plus className="size-4" />
          </RailButton>
          <ConversationSearch
            sessions={sessions}
            activeSessionId={activeSessionId}
            onSelect={onSelect}
            onOpenConversation={onOpenConversation}
            control={control}
            trigger={
              <button
                type="button"
                aria-label={t("sidebar.searchConversations")}
                title={t("sidebar.searchConversations")}
                className="flex size-9 items-center justify-center rounded-md border border-transparent text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
              >
                <Search className="size-4" aria-hidden="true" />
              </button>
            }
          />
          {hasNav && (
            <>
              <div className="my-2 h-px w-8 bg-border" aria-hidden="true" />
              {workspaceNav.map((item) => {
                const Icon = item.icon;
                if (!onNavigate) return null;
                const label = navLabel(item);
                return (
                  <RailButton
                    key={item.target}
                    label={
                      item.target === "attention" && attentionCount > 0
                        ? t("sidebar.attentionBadgeShort", { label, count: attentionCount })
                        : label
                    }
                    active={activeView === item.target}
                    onClick={() => onNavigate(item.target)}
                  >
                    <span className="relative">
                      <Icon className="size-4" />
                      {item.target === "attention" && attentionCount > 0 && (
                        <span
                          aria-hidden="true"
                          className="absolute -top-1 -right-1.5 min-w-[14px] rounded-full bg-muted-foreground/80 px-0.5 text-center text-[9px] leading-[14px] font-semibold text-background"
                        >
                          {attentionCount > 9 ? "9+" : attentionCount}
                        </span>
                      )}
                    </span>
                  </RailButton>
                );
              })}
            </>
          )}
          <div className="my-2 h-px w-8 bg-border" aria-hidden="true" />
          <ScrollArea
            className="min-h-0 w-full flex-1"
            viewportProps={{ className: "ocg-subtle-scrollbar" }}
            contentProps={{ className: "flex flex-col items-center gap-1" }}
          >
            {sessions.map((session) => (
              <RailButton
                key={session.id}
                label={session.title || t("sidebar.newChat")}
                active={
                  session.id === activeSessionId ||
                  session.conversationId === activeRoot ||
                  session.id === activeRoot
                }
                onClick={() => onSelect(session.id)}
              >
                <span aria-hidden="true" className="text-xs font-medium uppercase">
                  {(session.title || t("sidebar.newChat")).trim().charAt(0)}
                </span>
              </RailButton>
            ))}
          </ScrollArea>
          {/*
            The collapsed rail keeps the account menu: same identity, same
            actions, same order as the expanded footer, from the same component.
          */}
          <div className="mt-2 w-full border-t border-border pt-2">
            <div className="flex w-full justify-center">
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
                onSettingsClick={onSettingsClick}
              />
            </div>
          </div>
        </div>
      </TooltipProvider>
    );
  }

  return (
    <TooltipProvider delay={100}>
      <div className="flex h-full w-full flex-col bg-sidebar text-sidebar-foreground">
        <div className="flex items-center gap-2 px-3 pt-3 pb-2">
          <Link
            href="/"
            prefetch={false}
            onNavigate={
              onHome
                ? (event) => {
                    event.preventDefault();
                    onHome();
                  }
                : undefined
            }
            aria-label={t("nav.home")}
            className="min-w-0 flex-1 rounded-sm focus-visible:outline-2 focus-visible:outline-ring"
          >
            <OcgLogo />
          </Link>
          <ConversationSearch
            sessions={sessions}
            activeSessionId={activeSessionId}
            onSelect={onSelect}
            onOpenConversation={onOpenConversation}
            control={control}
            trigger={
              <Button
                variant="ghost"
                size="icon-xs"
                aria-label={t("sidebar.searchConversations")}
                title={t("sidebar.searchConversations")}
              >
                <Search className="size-4" aria-hidden="true" />
              </Button>
            }
          />
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon-xs"
                  onClick={onToggle}
                  aria-label={t("sidebar.collapse")}
                  aria-expanded="true"
                  title={t("sidebar.collapse")}
                >
                  <ChevronsLeft className="size-4" />
                </Button>
              }
            />
            <TooltipContent side="right">{t("sidebar.collapse")}</TooltipContent>
          </Tooltip>
        </div>

        {primaryControls}

        {hasNav && !canonicalMode && (
          <nav aria-label={t("nav.workspaceNavigation")} className="px-2 pb-2">
            <ul className="flex flex-col gap-px">
              {workspaceNav.slice(0, 4).map(renderWorkspaceItem)}
            </ul>
            {runtimeAuthority === "mock" && (
              <details
                open={workspaceNav.slice(4).some((item) => item.target === activeView) || undefined}
                className="mt-1"
              >
                <summary
                  className={cn(
                    "cursor-pointer rounded-md px-2 py-1.5",
                    TYPOGRAPHY_SCALE.caption,
                    "text-muted-foreground hover:bg-muted/60",
                  )}
                >
                  {t("nav.tools")}
                </summary>
                <ul className="flex flex-col gap-px pl-2">
                  {workspaceNav.slice(4).map(renderWorkspaceItem)}
                </ul>
              </details>
            )}
          </nav>
        )}

        <ScrollArea
          role="navigation"
          aria-label={t("nav.chatSessions")}
          className="min-h-0 flex-1"
          viewportProps={{ className: "ocg-subtle-scrollbar" }}
          contentProps={{ className: "px-2 pb-2", style: { minWidth: 0, width: "100%" } }}
        >
          {projectError && (
            <DismissibleNotice message={projectError} className="mb-2 px-2 text-xs" />
          )}
          <ul className="space-y-3">
            {groups.map((group) => (
              <li key={group.id} className="min-w-0">
                <div
                  className="mb-1 flex h-8 min-w-0 items-center gap-2 px-2 text-[13px] text-muted-foreground"
                  title={group.name}
                >
                  <FolderOpen className="size-4 shrink-0" aria-hidden="true" />
                  <span className="truncate">{group.name}</span>
                </div>
                <ul className="space-y-0.5">
                  {group.items.map((session) => {
                    const active =
                      session.id === activeSessionId ||
                      session.conversationId === activeRoot ||
                      session.id === activeRoot;
                    const isMenuOpen = openMenuId === session.id;
                    const expanded = expandedSessions[session.id] ?? active;
                    const canLoadChildren = Boolean(
                      control && onOpenConversation && session.conversationId,
                    );
                    const canExpand =
                      canLoadChildren &&
                      (session.hasChildren ?? childAvailability[session.id] === true);
                    const running = session.jobState
                      ? session.jobState === "running"
                      : runningSessionIds.has(session.id);
                    const pinId = session.conversationId ?? session.id;
                    const isPinned = pinned.has(pinId);
                    const hasActions = Boolean(session.conversationId && (onRename || onHide));
                    const actions =
                      "opacity-0 group-hover/session:opacity-100 group-focus-within/session:opacity-100 [@media(hover:none)]:opacity-100";
                    return (
                      <li key={session.id} className="min-w-0" data-sidebar-level="1">
                        <div
                          className={cn(
                            "group/session relative flex h-8 min-w-0 items-center rounded-lg text-sidebar-foreground/90 transition-colors hover:bg-sidebar-accent",
                            active && "bg-sidebar-accent text-sidebar-accent-foreground",
                          )}
                        >
                          <span className="flex w-8 shrink-0 items-center justify-center">
                            {canExpand && (
                              <button
                                type="button"
                                aria-expanded={expanded}
                                aria-label={t(
                                  expanded ? "sidebar.hideChildren" : "sidebar.showChildren",
                                  { title: session.title || t("sidebar.newChat") },
                                )}
                                className="flex size-6 items-center justify-center rounded text-muted-foreground hover:text-sidebar-foreground focus-visible:outline-ring"
                                onClick={() =>
                                  setExpandedSessions((current) => ({
                                    ...current,
                                    [session.id]: !expanded,
                                  }))
                                }
                              >
                                <ChevronDown
                                  className={cn(
                                    "size-3.5 transition-transform motion-reduce:transition-none",
                                    !expanded && "-rotate-90",
                                  )}
                                  aria-hidden="true"
                                />
                              </button>
                            )}
                          </span>
                          <button
                            type="button"
                            onClick={() => onSelect(session.id)}
                            aria-current={active ? "true" : undefined}
                            title={session.title || t("sidebar.newChat")}
                            className="flex h-full min-w-0 flex-1 items-center pr-2 text-left text-[13px] leading-5 outline-none focus-visible:rounded focus-visible:ring-2 focus-visible:ring-sidebar-ring"
                          >
                            <span className="min-w-0 flex-1 overflow-hidden whitespace-nowrap [mask-image:linear-gradient(to_right,black_calc(100%-1rem),transparent)]">
                              {session.title || t("sidebar.newChat")}
                            </span>
                          </button>
                          <div className="relative mr-2 flex h-full w-18 shrink-0 items-center justify-end">
                            <span
                              className={cn(
                                "flex items-center gap-1 text-xs tabular-nums text-muted-foreground transition-opacity",
                                hasActions &&
                                  "group-hover/session:opacity-0 group-focus-within/session:opacity-0 [@media(hover:none)]:opacity-0",
                                isMenuOpen && "opacity-0",
                              )}
                            >
                              {running ? (
                                <LoaderCircle
                                  className="size-3.5 animate-spin text-primary motion-reduce:animate-none"
                                  aria-label={t("sidebar.running")}
                                />
                              ) : (
                                <>
                                  <time
                                    dateTime={session.updatedAt}
                                    title={new Date(session.updatedAt).toLocaleString(locale)}
                                  >
                                    {relativeTime(session.updatedAt)}
                                  </time>
                                  {isPinned && (
                                    <Pin className="size-3" aria-label={t("sidebar.pinned")} />
                                  )}
                                </>
                              )}
                            </span>
                            {hasActions && (
                              <div
                                className={cn(
                                  "absolute inset-0 flex items-center justify-end gap-0.5 transition-opacity",
                                  isMenuOpen ? "opacity-100" : actions,
                                )}
                              >
                                <button
                                  type="button"
                                  className="flex size-6 items-center justify-center rounded text-muted-foreground hover:text-sidebar-foreground focus-visible:outline-ring"
                                  aria-label={t(isPinned ? "sidebar.unpin" : "sidebar.pin")}
                                  title={t(isPinned ? "sidebar.unpin" : "sidebar.pin")}
                                  onClick={() => togglePinned(pinId)}
                                >
                                  {isPinned ? (
                                    <PinOff className="size-3.5" />
                                  ) : (
                                    <Pin className="size-3.5" />
                                  )}
                                </button>
                                {onHide && session.sessionId && (
                                  <button
                                    type="button"
                                    className="flex size-6 items-center justify-center rounded text-muted-foreground hover:text-sidebar-foreground focus-visible:outline-ring"
                                    aria-label={t("sidebar.hideChat")}
                                    title={t("sidebar.hideChat")}
                                    onClick={() => {
                                      setHideError(null);
                                      setHideTarget(session);
                                    }}
                                  >
                                    <Archive className="size-3.5" />
                                  </button>
                                )}
                                {onRename && session.sessionId && (
                                  <DropdownMenu
                                    open={isMenuOpen}
                                    onOpenChange={(open) => setOpenMenuId(open ? session.id : null)}
                                  >
                                    <DropdownMenuTrigger asChild>
                                      <button
                                        type="button"
                                        aria-label={t("sidebar.chatActions", {
                                          title: session.title || t("sidebar.newChat"),
                                        })}
                                        className="flex size-5 items-center justify-center rounded text-muted-foreground hover:text-sidebar-foreground focus-visible:outline-ring"
                                      >
                                        <MoreVertical className="size-3.5" />
                                      </button>
                                    </DropdownMenuTrigger>
                                    <DropdownMenuContent align="end">
                                      <DropdownMenuItem
                                        onSelect={() => {
                                          setRenameError(null);
                                          setRenameTitle(session.title || "");
                                          setRenameTarget(session);
                                        }}
                                      >
                                        {t("sidebar.renameChat")}
                                      </DropdownMenuItem>
                                    </DropdownMenuContent>
                                  </DropdownMenu>
                                )}
                              </div>
                            )}
                          </div>
                        </div>
                        {canExpand &&
                          expanded &&
                          control &&
                          onOpenConversation &&
                          session.conversationId && (
                            <SidebarConversationChildren
                              key={session.conversationId}
                              conversation={session.conversationId}
                              control={control}
                              activeChild={activeAncestors?.[1]}
                              expanded
                              sessionId={session.id}
                              onAvailabilityChange={handleChildAvailability}
                              onOpen={onOpenConversation}
                            />
                          )}
                      </li>
                    );
                  })}
                </ul>
              </li>
            ))}
          </ul>
        </ScrollArea>

        {/*
          The account menu holds who is signed in and what they can do here.
          Local mode has no signed identity and no logout to run, so it names
          that state and offers Settings only.
        */}
        <div className="relative border-t border-border px-2 py-2">
          <AccountMenu
            email={email}
            onLogout={
              logout
                ? () => {
                    void logout();
                  }
                : undefined
            }
            onSettingsClick={onSettingsClick}
          />
        </div>
      </div>
      <Dialog
        open={renameTarget !== null}
        onOpenChange={(open) => {
          if (!open && !renaming) setRenameTarget(null);
        }}
      >
        <DialogContent showCloseButton={!renaming} className="rounded-lg">
          <DialogHeader>
            <DialogTitle className="normal-case tracking-normal">
              {t("sidebar.renameChatTitle")}
            </DialogTitle>
          </DialogHeader>
          <form onSubmit={(e) => void confirmRename(e)} className="flex flex-col gap-4">
            <Input
              aria-label={t("sidebar.renameChatTitle")}
              value={renameTitle}
              onChange={(e) => setRenameTitle(e.target.value)}
              disabled={renaming}
              autoFocus
            />
            {renameError && (
              <DismissibleNotice
                message={renameError}
                className="break-words text-xs text-destructive"
              >
                {renameError}
              </DismissibleNotice>
            )}
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={renaming}
                onClick={() => setRenameTarget(null)}
              >
                {t("common.cancel")}
              </Button>
              <Button type="submit" disabled={renaming}>
                {renaming && <LoaderCircle className="size-3.5 animate-spin" aria-hidden="true" />}
                {t("common.save")}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
      <Dialog
        open={hideTarget !== null}
        onOpenChange={(open) => {
          if (!open && !hiding) setHideTarget(null);
        }}
      >
        <DialogContent
          initialFocus={cancelHideRef}
          showCloseButton={!hiding}
          className="rounded-lg"
        >
          <DialogHeader>
            <DialogTitle className="normal-case tracking-normal">
              {t("sidebar.hideChatTitle")}
            </DialogTitle>
            <DialogDescription>
              {t("sidebar.hideChatBody", { title: hideTarget?.title || t("sidebar.newChat") })}
            </DialogDescription>
          </DialogHeader>
          {hideError && (
            <DismissibleNotice message={hideError} className="break-words text-xs text-destructive">
              {hideError}
            </DismissibleNotice>
          )}
          <DialogFooter>
            <Button
              ref={cancelHideRef}
              variant="outline"
              disabled={hiding}
              onClick={() => setHideTarget(null)}
            >
              {t("common.cancel")}
            </Button>
            <Button variant="destructive" disabled={hiding} onClick={() => void confirmHide()}>
              {hiding && <LoaderCircle className="size-3.5 animate-spin" aria-hidden="true" />}
              {t(hiding ? "sidebar.hiding" : "sidebar.hideChat")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </TooltipProvider>
  );
}
