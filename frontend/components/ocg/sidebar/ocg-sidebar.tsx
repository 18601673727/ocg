"use client";

import { useRef, useState } from "react";
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
} from "lucide-react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { ChatSession, RuntimeStatus } from "../types";
import type { RuntimeAuthority } from "../runtime/runtime-types";
import { ControlTransportError } from "../runtime/control-client";
import { useAccountEmail, useRemoteLogout } from "../login/access-boundary";
import { AccountMenu } from "./account-menu";
import { ConversationSearch } from "./conversation-search";
import { useI18n } from "../i18n";
import type { WorkspaceView } from "../layout/view-domain";

const WORKSPACE_NAV: { target: WorkspaceView; labelKey: "nav.usage" | "nav.home" | "nav.attention" | "nav.chat" | "nav.controlCenter" | "nav.ledger" | "nav.jobExecution" | "nav.health" | "nav.logs" | "nav.canonical"; icon: typeof Search }[] = [
  { target: "chat", labelKey: "nav.chat", icon: MessageSquare },
  { target: "jobs", labelKey: "nav.jobExecution", icon: Workflow },
  { target: "usage", labelKey: "nav.usage", icon: BarChart3 },
  { target: "logs", labelKey: "nav.logs", icon: ScrollText },
  { target: "health", labelKey: "nav.health", icon: Activity },
];

type OcgSidebarProps = {
  sessions: ChatSession[];
  activeSessionId: string;
  collapsed: boolean;
  onToggle: () => void;
  onSelect: (id: string) => void;
  onNewChat: () => void;
  onRename?: (id: string, title: string) => Promise<void>;
  onHide?: (id: string) => Promise<void>;
  runtimeStatus: RuntimeStatus;
  /** Which runtime answers Chat, so the footer never claims a fixture runtime. */
  runtimeAuthority: RuntimeAuthority;
  /** Active top-level workspace, used to highlight the navigation group. */
  activeView?: WorkspaceView;
  /** Name of the active Project, shown as the conversation search's scope line. */
  projectName?: string;
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
              active && "bg-muted text-foreground",
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
  activeSessionId,
  collapsed,
  onToggle,
  onSelect,
  onNewChat,
  onRename,
  onHide,
  runtimeAuthority,
  activeView = "chat",
  projectName,
  attentionCount = 0,
  onNavigate,
  onSettingsClick,
}: OcgSidebarProps) {
  const { t } = useI18n();
  const [hideTarget, setHideTarget] = useState<ChatSession | null>(null);
  const [hiding, setHiding] = useState(false);
  const [hideError, setHideError] = useState<string | null>(null);

  const [renameTarget, setRenameTarget] = useState<ChatSession | null>(null);
  const [renameTitle, setRenameTitle] = useState("");
  const [renaming, setRenaming] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);

  const [openMenuId, setOpenMenuId] = useState<string | null>(null);
  const cancelHideRef = useRef<HTMLButtonElement>(null);

  const email = useAccountEmail();
  const logout = useRemoteLogout();

  const conversationError = (error: unknown): string => {
    if (error instanceof ControlTransportError) {
      if (error.code === "not_found") return t("sidebar.conversationNotFound");
      if (error.code === "invalid_request") return t("sidebar.titleInvalid");
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
      setRenameError(t("sidebar.renameFailed", { error: conversationError(cause) }));
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
      setHideError(t("sidebar.hideFailed", { error: conversationError(cause) }));
    } finally {
      setHiding(false);
    }
  };
  const canonicalMode = runtimeAuthority === "canonical";
  const workspaceNav = canonicalMode
    ? WORKSPACE_NAV
    : WORKSPACE_NAV;
  // In canonical mode every non-Chat destination lives in the Workbench menu
  // while the conversation list and New chat serve Chat directly.
  const workbenchNav = canonicalMode ? workspaceNav.filter(item => item.target !== "chat") : [];
  const hasNav = Boolean(onNavigate);
  const navLabel = (item: (typeof WORKSPACE_NAV)[number]) => (item.target === "jobs" ? "Jobs" : t(item.labelKey));

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
            "group flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left", TYPOGRAPHY_SCALE.body_sm, "leading-5 transition-colors",
            active
              ? "bg-muted font-medium text-foreground"
              : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
          )}
        >
          <Icon className="size-3.5 shrink-0" aria-hidden="true" />
          <span className="min-w-0 flex-1 truncate">{label}</span>
          {item.target === "attention" && attentionCount > 0 && (
            <span
              aria-label={t("sidebar.attentionBadge", { count: attentionCount })}
              className={cn("shrink-0 rounded-md bg-muted px-1.5", TYPOGRAPHY_SCALE.caption, "font-semibold tabular-nums text-muted-foreground")}
            >
              {attentionCount > 99 ? "99+" : attentionCount}
            </span>
          )}
        </button>
      </li>
    );
  };

  // The primary controls sit directly under the wordmark: New chat, the
  // conversation search, and (in canonical mode) the Workbench menu that holds
  // every destination other than Chat.
  const primaryControls = (
    <>
      <div className="flex items-center gap-1 px-2 pb-2">
        <Button
          variant="outline"
          size="sm"
          className="min-w-0 flex-1 justify-start"
          onClick={onNewChat}
        >
          <Plus className="size-3.5" data-icon="inline-start" />
          {t("sidebar.newChat")}
        </Button>
        {/*
          The search button opens the full-screen conversation overlay. It keeps
          its own accessible name and tooltip title rather than a Tooltip
          wrapper, because the overlay owns the trigger's props.
        */}
        <ConversationSearch
          sessions={sessions}
          activeSessionId={activeSessionId}
          projectName={projectName}
          onSelect={onSelect}
          trigger={
            <Button variant="outline" size="icon-sm" aria-label={t("sidebar.searchConversations")} title={t("sidebar.searchConversations")}>
              <Search className="size-4" aria-hidden="true" />
            </Button>
          }
        />
      </div>

      {hasNav && workbenchNav.length > 0 && (
        /*
          Canonical mode keeps Chat as the single primary destination and folds
          every other destination into this compact Workbench menu. Each item
          keeps its icon, label, attention badge and active state.
        */
        <details
          className="px-2 pb-2"
          open={workbenchNav.some((item) => item.target === activeView) || undefined}
          data-workbench-menu=""
        >
          <summary className={cn("group flex cursor-pointer items-center gap-2 rounded-md border border-border px-2 py-1.5 select-none", TYPOGRAPHY_SCALE.body_sm, "text-muted-foreground hover:bg-muted/60")}>
            <LayoutGrid className="size-3.5 shrink-0" aria-hidden="true" />
            <span className="min-w-0 flex-1 truncate">{t("nav.workbench")}</span>
            <ChevronDown className="size-3.5 shrink-0 text-muted-foreground transition-transform group-open:rotate-180" aria-hidden="true" />
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
          <span aria-label="OCG" className="mb-2 scale-y-75 text-[18px] leading-none font-black tracking-[-0.06em]">OCG</span>
          <RailButton label={t("sidebar.expand")} onClick={onToggle}>
            <ChevronsLeft className="size-4 rotate-180" />
          </RailButton>
          <RailButton label={t("sidebar.newChat")} onClick={onNewChat}>
            <Plus className="size-4" />
          </RailButton>
          <ConversationSearch
            sessions={sessions}
            activeSessionId={activeSessionId}
            projectName={projectName}
            onSelect={onSelect}
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
                    label={item.target === "attention" && attentionCount > 0 ? t("sidebar.attentionBadgeShort", { label, count: attentionCount }) : label}
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
          <div className="flex min-h-0 flex-1 flex-col items-center gap-1 overflow-y-auto">
            {sessions.map((session) => (
              <RailButton
                key={session.id}
                label={session.title || t("sidebar.newChat")}
                active={session.id === activeSessionId}
                onClick={() => onSelect(session.id)}
              >
                <MessageSquare className="size-4" />
              </RailButton>
            ))}
          </div>
          {/*
            The collapsed rail keeps the account menu: same identity, same
            actions, same order as the expanded footer, from the same component.
          */}
          <div className="mt-2 w-full border-t border-border pt-2">
            <div className="flex w-full justify-center">
              <AccountMenu
                variant="rail"
                email={email}
                onLogout={logout ? () => { void logout(); } : undefined}
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
      <div className="flex h-full w-full flex-col">
        <div className="flex items-center gap-2 px-3 pt-3 pb-2">
          <span aria-label="OCG" className="min-w-0 flex-1 origin-left scale-y-75 text-[28px] leading-none font-black tracking-[-0.06em]">
            OCG
          </span>
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
            {runtimeAuthority === "mock" && <details open={workspaceNav.slice(4).some((item) => item.target === activeView) || undefined} className="mt-1">
              <summary className={cn("cursor-pointer rounded-md px-2 py-1.5", TYPOGRAPHY_SCALE.caption, "text-muted-foreground hover:bg-muted/60")}>{t("nav.tools")}</summary>
              <ul className="flex flex-col gap-px pl-2">
                {workspaceNav.slice(4).map(renderWorkspaceItem)}
              </ul>
            </details>}
          </nav>
        )}

        <nav
          aria-label={t("nav.chatSessions")}
          className="min-h-0 flex-1 overflow-y-auto px-2 pb-2"
        >
          <ul className="flex flex-col gap-px">
            {sessions.map((session) => {
              const active = session.id === activeSessionId;
              const isMenuOpen = openMenuId === session.id;
              return (
                <li key={session.id} className={cn("group/session relative flex min-w-0 items-center rounded-md transition-colors", active ? "bg-muted" : "hover:bg-muted/60")}>
                  <button
                    type="button"
                    onClick={() => onSelect(session.id)}
                    aria-current={active ? "true" : undefined}
                    className={cn(
                      "group flex min-w-0 flex-1 items-center gap-2 rounded-md px-2 py-1.5 text-left", TYPOGRAPHY_SCALE.body_sm, "leading-5 outline-none focus-visible:ring-2 focus-visible:ring-ring/30",
                      active
                        ? "bg-muted font-medium text-foreground"
                        : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                    )}
                  >
                    <span
                      aria-hidden="true"
                      className={cn(
                        "h-4 w-0.5 shrink-0 rounded-full",
                        active ? "bg-foreground" : "bg-transparent group-hover:bg-border",
                      )}
                    />
                    <span className="min-w-0 flex-1 truncate">
                      {session.title || t("sidebar.newChat")}
                    </span>
                  </button>
                  {(onRename || onHide) && session.sessionId && (
                    <DropdownMenu open={isMenuOpen} onOpenChange={(open) => setOpenMenuId(open ? session.id : null)}>
                      <DropdownMenuTrigger asChild>
                        <button
                          type="button"
                          aria-label={t("sidebar.chatActions", { title: session.title || t("sidebar.newChat") })}
                          onClick={(e) => {
                            e.stopPropagation();
                          }}
                          className={cn(
                            "mr-1 flex size-7 shrink-0 items-center justify-center rounded-md text-muted-foreground/60 outline-none transition-opacity hover:bg-muted hover:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring/30",
                            // On hover-capable devices: hide by default, show on hover/focus/menu-open
                            // On touch devices: always visible
                            isMenuOpen
                              ? "opacity-100"
                              : "[@media(hover:hover)]:opacity-0 [@media(hover:hover)]:group-hover/session:opacity-100 [@media(hover:hover)]:group-focus-within/session:opacity-100 [@media(hover:none)]:opacity-100",
                          )}
                        >
                          <MoreVertical className="size-3.5" aria-hidden="true" />
                        </button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        {onRename && (
                          <DropdownMenuItem
                            onSelect={() => {
                              setRenameError(null);
                              setRenameTitle(session.title || "");
                              setRenameTarget(session);
                            }}
                          >
                            {t("sidebar.renameChat")}
                          </DropdownMenuItem>
                        )}
                        {onHide && (
                          <DropdownMenuItem
                            onSelect={() => {
                              setHideError(null);
                              setHideTarget(session);
                            }}
                            className="text-destructive focus-visible:bg-destructive/10 focus-visible:text-destructive"
                          >
                            {t("sidebar.hideChat")}
                          </DropdownMenuItem>
                        )}
                      </DropdownMenuContent>
                    </DropdownMenu>
                  )}
                </li>
              );
            })}
          </ul>
        </nav>

        {/*
          The account menu holds who is signed in and what they can do here.
          Local mode has no signed identity and no logout to run, so it names
          that state and offers Settings only.
        */}
        <div className="relative border-t border-border px-2 py-2">
          <AccountMenu
            email={email}
            onLogout={logout ? () => { void logout(); } : undefined}
            onSettingsClick={onSettingsClick}
          />
        </div>
      </div>
      <Dialog open={renameTarget !== null} onOpenChange={open => { if (!open && !renaming) setRenameTarget(null); }}>
        <DialogContent showCloseButton={!renaming} className="rounded-lg">
          <DialogHeader>
            <DialogTitle className="normal-case tracking-normal">{t("sidebar.renameChatTitle")}</DialogTitle>
          </DialogHeader>
          <form onSubmit={(e) => void confirmRename(e)} className="flex flex-col gap-4">
            <Input
              value={renameTitle}
              onChange={(e) => setRenameTitle(e.target.value)}
              disabled={renaming}
              autoFocus
            />
            {renameError && <p role="alert" className="break-words text-xs text-destructive">{renameError}</p>}
            <DialogFooter>
              <Button type="button" variant="outline" disabled={renaming} onClick={() => setRenameTarget(null)}>
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
      <Dialog open={hideTarget !== null} onOpenChange={open => { if (!open && !hiding) setHideTarget(null); }}>
        <DialogContent initialFocus={cancelHideRef} showCloseButton={!hiding} className="rounded-lg">
          <DialogHeader>
            <DialogTitle className="normal-case tracking-normal">{t("sidebar.hideChatTitle")}</DialogTitle>
            <DialogDescription>{t("sidebar.hideChatBody", { title: hideTarget?.title || t("sidebar.newChat") })}</DialogDescription>
          </DialogHeader>
          {hideError && <p role="alert" className="break-words text-xs text-destructive">{hideError}</p>}
          <DialogFooter>
            <Button ref={cancelHideRef} variant="outline" disabled={hiding} onClick={() => setHideTarget(null)}>{t("common.cancel")}</Button>
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
