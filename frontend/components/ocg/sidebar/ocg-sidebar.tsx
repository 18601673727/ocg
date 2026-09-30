"use client";

import {
  ChevronsLeft,
  Code2,
  FlaskConical,
  Home,
  Bell,
  LifeBuoy,
  MessageSquare,
  PenTool,
  Plus,
  Search,
  Server,
  ScrollText,
  Settings,
  SlidersHorizontal,
  Table2,
  Workflow,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { ChatSession, RuntimeStatus, WorkType } from "../types";
import { WORK_TYPE_LABEL } from "../types";
import { ProjectSwitcher } from "../project/project-switcher";
import type { ProjectId, ProjectSummary } from "../project/domain";
import { DEFAULT_PROJECT_ID, PROJECTS } from "../project/domain";
import {
  RUNTIME_CONNECTION,
  RUNTIME_CONNECTION_LABEL,
  StatusDot,
} from "@/components/ocg/primitives";
import type { WorkspaceView } from "../layout/view-domain";

const GROUP_ORDER: WorkType[] = ["research", "coding", "design", "devops"];

const GROUP_ICON: Record<WorkType, typeof Search> = {
  research: FlaskConical,
  coding: Code2,
  design: PenTool,
  devops: Server,
};

const WORKSPACE_NAV: { target: WorkspaceView; label: string; icon: typeof Search }[] = [
  { target: "home", label: "Home", icon: Home },
  { target: "attention", label: "Attention", icon: Bell },
  { target: "chat", label: "Chat", icon: MessageSquare },
  { target: "control-center", label: "Control Center", icon: SlidersHorizontal },
  { target: "ledger", label: "Resource Ledger", icon: Table2 },
  { target: "job-execution", label: "Job Execution", icon: Workflow },
  { target: "logs", label: "Logs / Diagnostics", icon: ScrollText },
  { target: "canonical", label: "OCG Control", icon: SlidersHorizontal },
];

type OcgSidebarProps = {
  sessions: ChatSession[];
  activeSessionId: string;
  collapsed: boolean;
  onToggle: () => void;
  onSelect: (id: string) => void;
  onNewChat: () => void;
  runtimeStatus: RuntimeStatus;
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
  runtimeStatus,
  activeView = "chat",
  projects = PROJECTS,
  activeProjectId = DEFAULT_PROJECT_ID,
  onProjectChange,
  attentionCount = 0,
  onNavigate,
}: OcgSidebarProps) {
  const hasNav = Boolean(onNavigate);
  const connection = RUNTIME_CONNECTION[runtimeStatus.state];

  if (collapsed) {
    return (
      <TooltipProvider delay={100}>
        <div className="flex h-full w-full flex-col items-center gap-1 px-2 py-3">
          <RailButton label="Expand sidebar" onClick={onToggle}>
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
          <RailButton label="New chat" onClick={onNewChat}>
            <Plus className="size-4" />
          </RailButton>
          {hasNav && (
            <>
              <div className="my-2 h-px w-8 bg-border" aria-hidden="true" />
              {WORKSPACE_NAV.map((item) => {
                const Icon = item.icon;
                if (!onNavigate) return null;
                return (
                  <RailButton
                    key={item.target}
                    label={item.target === "attention" && attentionCount > 0 ? `${item.label} (${attentionCount} need action)` : item.label}
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
            {GROUP_ORDER.map((group) => {
              const Icon = GROUP_ICON[group];
              const groupActive = sessions.some(
                (s) => s.workType === group && s.id === activeSessionId,
              );
              const firstInGroup = sessions.find((s) => s.workType === group);
              return (
                <RailButton
                  key={group}
                  label={WORK_TYPE_LABEL[group]}
                  active={groupActive}
                  onClick={() => firstInGroup && onSelect(firstInGroup.id)}
                >
                  <Icon className="size-4" />
                </RailButton>
              );
            })}
          </div>
          <div className="flex flex-col items-center gap-1">
            <RailButton label="Settings" onClick={onNavigate ? () => onNavigate("settings") : undefined}>
              <Settings className="size-4" />
            </RailButton>
            <Avatar size="sm">
              <AvatarFallback className="text-[11px]">OC</AvatarFallback>
            </Avatar>
          </div>
        </div>
      </TooltipProvider>
    );
  }

  return (
    <TooltipProvider delay={100}>
      <div className="flex h-full w-full flex-col">
        <div className="flex items-center gap-2 px-3 pt-3 pb-2">
          <div className="flex size-8 shrink-0 items-center justify-center rounded-md bg-primary text-[13px] font-semibold text-primary-foreground">
            O
          </div>
          <div className="min-w-0 flex-1">
            <p className="truncate text-[13px] font-semibold tracking-tight">
              OCG Server
            </p>
            <p className="truncate text-[11px] text-muted-foreground">
              local · mock state
            </p>
          </div>
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon-xs"
                  onClick={onToggle}
                  aria-label="Collapse sidebar"
                  aria-expanded="true"
                  title="Collapse sidebar"
                >
                  <ChevronsLeft className="size-4" />
                </Button>
              }
            />
            <TooltipContent side="right">Collapse sidebar</TooltipContent>
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
          <nav aria-label="Workspace navigation" className="px-2 pb-2">
            <ul className="flex flex-col gap-px">
              {WORKSPACE_NAV.map((item) => {
                const Icon = item.icon;
                if (!onNavigate) return null;
                const active = activeView === item.target;
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
                      <span className="min-w-0 flex-1 truncate">{item.label}</span>
                      {item.target === "attention" && attentionCount > 0 && (
                        <span
                          aria-label={`${attentionCount} items need action`}
                          className="shrink-0 rounded-full bg-muted px-1.5 text-[10px] font-semibold tabular-nums text-muted-foreground"
                        >
                          {attentionCount > 99 ? "99+" : attentionCount}
                        </span>
                      )}
                    </button>
                  </li>
                );
              })}
            </ul>
          </nav>
        )}

        <div className="px-3 pb-2">
          <Button
            variant="default"
            size="sm"
            className="w-full justify-start"
            onClick={onNewChat}
          >
            <Plus className="size-3.5" data-icon="inline-start" />
            New chat
          </Button>
          <div
            className="mt-2 flex items-center gap-2 rounded-md border border-border bg-muted/40 px-2.5 py-1.5 text-[12px] text-muted-foreground"
            title="Session search is a placeholder in Phase 1"
          >
            <Search className="size-3.5 shrink-0" aria-hidden="true" />
            <span className="truncate">Search sessions…</span>
            <kbd className="ml-auto rounded border border-border bg-background px-1 text-[10px]">
              ⌘K
            </kbd>
          </div>
        </div>

        <nav
          aria-label="Chat sessions grouped by work type"
          className="min-h-0 flex-1 overflow-y-auto px-2 pb-2"
        >
          {GROUP_ORDER.map((group) => {
            const Icon = GROUP_ICON[group];
            const items = sessions.filter((s) => s.workType === group);
            return (
              <section key={group} aria-label={WORK_TYPE_LABEL[group]} className="mt-1">
                <h2 className="flex items-center gap-1.5 px-2 pt-3 pb-1 text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">
                  <Icon className="size-3.5" aria-hidden="true" />
                  {WORK_TYPE_LABEL[group]}
                </h2>
                <ul className="flex flex-col gap-px">
                  {items.map((session) => {
                    const active = session.id === activeSessionId;
                    return (
                      <li key={session.id}>
                        <button
                          type="button"
                          onClick={() => onSelect(session.id)}
                          aria-current={active ? "true" : undefined}
                          className={cn(
                            "group flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] leading-5 transition-colors",
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
                            {session.title}
                          </span>
                        </button>
                      </li>
                    );
                  })}
                </ul>
              </section>
            );
          })}
        </nav>

        <div className="border-t border-border px-3 py-2.5">
          <div className="flex items-center gap-1.5 text-[12px] text-muted-foreground">
             <StatusDot tone={connection.tone} pulse={connection.pulse} />
             <span className="truncate">Runtime {RUNTIME_CONNECTION_LABEL[runtimeStatus.state]} · local mock</span>
          </div>
          <div className="mt-2 flex items-center gap-2">
            <Avatar size="sm">
              <AvatarFallback className="text-[11px]">OC</AvatarFallback>
            </Avatar>
            <div className="min-w-0 flex-1">
              <p className="truncate text-[12px] font-medium">Operator</p>
              <p className="truncate text-[11px] text-muted-foreground">
                local profile
              </p>
            </div>
            <Tooltip>
              <TooltipTrigger
                render={
                  <Button
                    variant="ghost"
                    size="icon-xs"
                     aria-label="Open settings"
                     title="Open settings"
                     onClick={onNavigate ? () => onNavigate("settings") : undefined}
                  >
                    <Settings className="size-4" />
                  </Button>
                }
              />
              <TooltipContent side="top">Open settings</TooltipContent>
            </Tooltip>
            <Tooltip>
              <TooltipTrigger
                render={
                  <Button
                    variant="ghost"
                    size="icon-xs"
                    aria-label="Help (placeholder)"
                    title="Help (placeholder)"
                  >
                    <LifeBuoy className="size-4" />
                  </Button>
                }
              />
              <TooltipContent side="top">Help (placeholder)</TooltipContent>
            </Tooltip>
          </div>
        </div>
      </div>
    </TooltipProvider>
  );
}
