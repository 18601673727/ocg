"use client";

import { useRef, useState } from "react";
import {
  ChevronsLeft,
  Home,
  BarChart3,
  Bell,
  MessageSquare,
  Plus,
  Search,
  ScrollText,
  SlidersHorizontal,
  Table2,
  Workflow,
  LoaderCircle,
  Trash2,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { ChatSession, RuntimeStatus } from "../types";
import type { RuntimeAuthority } from "../runtime/runtime-types";
import { ProjectSwitcher } from "../project/project-switcher";
import type { ProjectId, ProjectSummary } from "../project/domain";
import { DEFAULT_PROJECT_ID } from "../project/domain";
import { useI18n } from "../i18n";
import type { WorkspaceView } from "../layout/view-domain";

const WORKSPACE_NAV: { target: WorkspaceView; labelKey: "nav.usage" | "nav.home" | "nav.attention" | "nav.chat" | "nav.controlCenter" | "nav.ledger" | "nav.jobExecution" | "nav.logs" | "nav.canonical"; icon: typeof Search }[] = [
  { target: "home", labelKey: "nav.home", icon: Home },
  { target: "chat", labelKey: "nav.chat", icon: MessageSquare },
  { target: "job-execution", labelKey: "nav.jobExecution", icon: Workflow },
  { target: "attention", labelKey: "nav.attention", icon: Bell },
  { target: "usage", labelKey: "nav.usage", icon: BarChart3 },
  { target: "control-center", labelKey: "nav.controlCenter", icon: SlidersHorizontal },
  { target: "ledger", labelKey: "nav.ledger", icon: Table2 },
  { target: "logs", labelKey: "nav.logs", icon: ScrollText },
  { target: "canonical", labelKey: "nav.canonical", icon: SlidersHorizontal },
];

type OcgSidebarProps = {
  sessions: ChatSession[];
  activeSessionId: string;
  collapsed: boolean;
  onToggle: () => void;
  onSelect: (id: string) => void;
  onNewChat: () => void;
  onDelete?: (id: string) => Promise<void>;
  busySessionIds?: readonly string[];
  runtimeStatus: RuntimeStatus;
  /** Which runtime answers Chat, so the footer never claims a fixture runtime. */
  runtimeAuthority: RuntimeAuthority;
  /** Active top-level workspace, used to highlight the navigation group. */
  activeView?: WorkspaceView;
  /** Project switcher inputs. The switcher renders only when onChange is given. */
  projects?: readonly ProjectSummary[];
  activeProjectId?: ProjectId;
  onProjectChange?: (id: ProjectId) => void;
  /** Unresolved attention count shown as a quiet badge next to Attention. */
  attentionCount?: number;
  /**
   * Opens a workspace, or closes it back to the chat root when it is already
   * open. Omitting it hides the workspace navigation group entirely, which is
   * how a page renders the sidebar without shell navigation.
   */
  onNavigate?: (view: WorkspaceView) => void;
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
  onDelete,
  busySessionIds = [],
  runtimeAuthority,
  activeView = "chat",
  projects = [],
  activeProjectId = DEFAULT_PROJECT_ID,
  onProjectChange,
  attentionCount = 0,
  onNavigate,
}: OcgSidebarProps) {
  const { t } = useI18n();
  const [deleteTarget, setDeleteTarget] = useState<ChatSession | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const cancelDeleteRef = useRef<HTMLButtonElement>(null);
  const targetBusy = deleteTarget !== null && busySessionIds.includes(deleteTarget.id);
  const confirmDelete = async () => {
    if (!deleteTarget || !onDelete || deleting || targetBusy) return;
    setDeleting(true);
    setDeleteError(null);
    try {
      await onDelete(deleteTarget.id);
      setDeleteTarget(null);
    } catch (cause: unknown) {
      setDeleteError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setDeleting(false);
    }
  };
  const workspaceNav = runtimeAuthority === "canonical"
    ? WORKSPACE_NAV.filter(item => ["home", "chat", "job-execution", "attention", "usage", "control-center"].includes(item.target))
    : WORKSPACE_NAV;
  const hasNav = Boolean(onNavigate);
  const navLabel = (item: (typeof WORKSPACE_NAV)[number]) => t(item.labelKey);

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
            "group flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] leading-5 transition-colors",
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
              className="shrink-0 rounded-md bg-muted px-1.5 text-[10px] font-semibold tabular-nums text-muted-foreground"
            >
              {attentionCount > 99 ? "99+" : attentionCount}
            </span>
          )}
        </button>
      </li>
    );
  };

  if (collapsed) {
    return (
      <TooltipProvider delay={100}>
        <div className="flex h-full w-full flex-col items-center gap-1 px-2 py-3">
          <span aria-label="OCG" className="mb-2 scale-y-75 text-[18px] leading-none font-black tracking-[-0.06em]">OCG</span>
          <RailButton label={t("sidebar.expand")} onClick={onToggle}>
            <ChevronsLeft className="size-4 rotate-180" />
          </RailButton>
          {onProjectChange && (
            <ProjectSwitcher
              projects={projects}
              activeProjectId={activeProjectId}
              collapsed
              onChange={onProjectChange}
            />
          )}
          <RailButton label={t("sidebar.newChat")} onClick={onNewChat}>
            <Plus className="size-4" />
          </RailButton>
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

        {onProjectChange && (
          <div className="px-3 pb-2">
            <ProjectSwitcher
              projects={projects}
              activeProjectId={activeProjectId}
              onChange={onProjectChange}
            />
          </div>
        )}

        {hasNav && (
          <nav aria-label={t("nav.workspaceNavigation")} className="px-2 pb-2">
            <ul className="flex flex-col gap-px">
              {(runtimeAuthority === "canonical" ? workspaceNav : workspaceNav.slice(0, 4)).map(renderWorkspaceItem)}
            </ul>
            {runtimeAuthority === "mock" && <details open={workspaceNav.slice(4).some((item) => item.target === activeView) || undefined} className="mt-1">
              <summary className="cursor-pointer rounded-md px-2 py-1.5 text-[12px] text-muted-foreground hover:bg-muted/60">{t("nav.tools")}</summary>
              <ul className="flex flex-col gap-px pl-2">
                {workspaceNav.slice(4).map(renderWorkspaceItem)}
              </ul>
            </details>}
          </nav>
        )}

        <div className="px-3 pb-2">
          <Button
            variant="outline"
            size="sm"
            className="w-full justify-start"
            onClick={onNewChat}
          >
            <Plus className="size-3.5" data-icon="inline-start" />
            {t("sidebar.newChat")}
          </Button>
        </div>

        <nav
          aria-label={t("nav.chatSessions")}
          className="min-h-0 flex-1 overflow-y-auto px-2 pb-2"
        >
          <ul className="flex flex-col gap-px">
            {sessions.map((session) => {
              const active = session.id === activeSessionId;
              return (
                <li key={session.id} className={cn("group/session flex min-w-0 items-center rounded-md transition-colors", active ? "bg-muted" : "hover:bg-muted/60")}>
                  <button
                    type="button"
                    onClick={() => onSelect(session.id)}
                    aria-current={active ? "true" : undefined}
                    className={cn(
                      "group flex min-w-0 flex-1 items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] leading-5 outline-none focus-visible:ring-2 focus-visible:ring-ring/30",
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
                  {onDelete && <button
                    type="button"
                    aria-label={t("sidebar.deleteChatNamed", { title: session.title || t("sidebar.newChat") })}
                    title={t(busySessionIds.includes(session.id) ? "sidebar.deleteRunning" : "sidebar.deleteChat")}
                    disabled={busySessionIds.includes(session.id) || deleting}
                    onClick={() => { setDeleteError(null); setDeleteTarget(session); }}
                    className="mr-1 flex size-7 shrink-0 items-center justify-center rounded-md text-muted-foreground/60 outline-none hover:bg-destructive/10 hover:text-destructive focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    <Trash2 className="size-3.5" aria-hidden="true" />
                  </button>}
                </li>
              );
            })}
          </ul>
        </nav>
      </div>
      <Dialog open={deleteTarget !== null} onOpenChange={open => { if (!open && !deleting) setDeleteTarget(null); }}>
        <DialogContent initialFocus={cancelDeleteRef} showCloseButton={!deleting} className="rounded-lg">
          <DialogHeader>
            <DialogTitle className="normal-case tracking-normal">{t("sidebar.deleteChatTitle")}</DialogTitle>
            <DialogDescription>{t("sidebar.deleteChatBody", { title: deleteTarget?.title || t("sidebar.newChat") })}</DialogDescription>
          </DialogHeader>
          {targetBusy && <p role="status" className="text-xs text-amber-600 dark:text-amber-400">{t("sidebar.deleteRunning")}</p>}
          {deleteError && <p role="alert" className="break-words text-xs text-destructive">{t("sidebar.deleteFailed", { error: deleteError })}</p>}
          <DialogFooter>
            <Button ref={cancelDeleteRef} variant="outline" disabled={deleting} onClick={() => setDeleteTarget(null)}>{t("common.cancel")}</Button>
            <Button variant="destructive" disabled={deleting || targetBusy} onClick={() => void confirmDelete()}>
              {deleting && <LoaderCircle className="size-3.5 animate-spin" aria-hidden="true" />}
              {t(deleting ? "sidebar.deletingChat" : "sidebar.deleteChat")}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </TooltipProvider>
  );
}
