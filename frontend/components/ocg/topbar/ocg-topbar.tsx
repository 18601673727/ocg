"use client";

import {
  Bell,
  ChevronsLeft,
  ChevronsRight,
  Home,
  MoreHorizontal,
  PanelLeft,
  PanelRight,
  ScrollText,
  SlidersHorizontal,
  Settings,
  Table2,
  Workflow,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type { ChatSession, RuntimeStatus } from "../types";
import { WORK_TYPE_LABEL } from "../types";
import { isDegradedSyncStatus, type RuntimeSyncStatus } from "../runtime/reconciler";
import type { WorkspaceView } from "../layout/view-domain";

type OcgTopbarProps = {
  session: ChatSession;
  sidebarCollapsed: boolean;
  missionOpen: boolean;
  /** When false, the mission panel toggles are hidden (for example on the ledger view). Defaults to true. */
  missionControls?: boolean;
  onToggleSidebar: () => void;
  onToggleMission: () => void;
  onOpenMobileSidebar: () => void;
  onOpenMobileMission: () => void;
  /**
   * Opens a workspace, or closes it when it is already the active view. Omitting
   * it hides the whole shortcut group, which is how a surface renders the topbar
   * without workspace navigation.
   */
  onNavigate?: (view: WorkspaceView) => void;
  /** Current workspace, used to highlight the matching shortcut. */
  activeView?: WorkspaceView;
  runtimeStatus: RuntimeStatus;
  /** Canonical reconciler sync status. Only degraded states are surfaced. */
  syncStatus?: RuntimeSyncStatus | null;
};

/**
 * Workspace shortcuts, in bar order. The placeholder "more actions" control sat
 * between Attention and the surfaces, so the group is kept in two pieces.
 */
const SHORTCUTS_BEFORE_MENU: WorkspaceShortcut[] = [
  { view: "home", icon: Home, label: "Home" },
  { view: "attention", icon: Bell, label: "Attention" },
];

const SHORTCUTS_AFTER_MENU: WorkspaceShortcut[] = [
  { view: "ledger", icon: Table2, label: "Resource ledger" },
  { view: "control-center", icon: SlidersHorizontal, label: "Control Center" },
  { view: "mission-control", icon: Workflow, label: "Mission Control" },
  { view: "logs", icon: ScrollText, label: "Logs and diagnostics" },
  { view: "settings", icon: Settings, label: "Settings" },
];

type WorkspaceShortcut = {
  view: WorkspaceView;
  icon: typeof Home;
  label: string;
};

function Shortcut({
  shortcut,
  active,
  onNavigate,
}: {
  shortcut: WorkspaceShortcut;
  active: boolean;
  onNavigate: (view: WorkspaceView) => void;
}) {
  const Icon = shortcut.icon;
  const action = `${active ? "Close" : "Open"} ${shortcut.label}`;
  return (
    <Button
      variant={active ? "secondary" : "ghost"}
      size="icon-xs"
      onClick={() => onNavigate(shortcut.view)}
      aria-label={action}
      aria-current={active ? "page" : undefined}
      title={action}
    >
      <Icon className="size-4" />
    </Button>
  );
}

const WORK_TYPE_DOT: Record<ChatSession["workType"], string> = {
  research: "bg-sky-500",
  coding: "bg-emerald-500",
  design: "bg-violet-500",
  devops: "bg-amber-500",
};

export function OcgTopbar({
  session,
  sidebarCollapsed,
  missionOpen,
  missionControls = true,
  onToggleSidebar,
  onToggleMission,
  onOpenMobileSidebar,
  onOpenMobileMission,
  onNavigate,
  activeView = "chat",
  runtimeStatus,
  syncStatus,
}: OcgTopbarProps) {
  return (
    <header className="flex h-12 shrink-0 items-center gap-1.5 border-b border-border bg-background px-2 sm:px-3">
      {/* Mobile sidebar toggle */}
      <Button
        variant="ghost"
        size="icon-xs"
        className="lg:hidden"
        onClick={onOpenMobileSidebar}
        aria-label="Open navigation"
        title="Open navigation"
      >
        <PanelLeft className="size-4" />
      </Button>
      {/* Desktop sidebar toggle */}
      <Button
        variant="ghost"
        size="icon-xs"
        className="hidden lg:inline-flex"
        onClick={onToggleSidebar}
        aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
        aria-expanded={!sidebarCollapsed}
        title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
      >
        <PanelLeft className="size-4" />
      </Button>

      <div className="mx-1 h-5 w-px bg-border" aria-hidden="true" />

      <div className="flex min-w-0 flex-1 items-center gap-2">
        <span
          aria-hidden="true"
          className={cn("size-1.5 shrink-0 rounded-full", WORK_TYPE_DOT[session.workType])}
        />
        <h1 className="truncate text-[13px] font-semibold tracking-tight">
          {session.title}
        </h1>
        <span className="hidden shrink-0 rounded border border-border bg-muted/60 px-1.5 py-0.5 text-[11px] font-medium text-muted-foreground sm:inline">
          {WORK_TYPE_LABEL[session.workType]}
        </span>
      </div>

      <div
        className="hidden shrink-0 items-center gap-1.5 rounded-full border border-border bg-muted/40 px-2.5 py-1 text-[11px] text-muted-foreground md:flex"
        title={runtimeStatus.detail ?? "Local mock runtime"}
      >
        <span
          className={cn(
            "size-1.5 rounded-full",
            runtimeStatus.state === "connected" && "bg-emerald-500",
            runtimeStatus.state === "connecting" && "animate-pulse bg-amber-500",
            runtimeStatus.state === "disconnected" && "bg-muted-foreground/50",
            runtimeStatus.state === "failed" && "bg-red-500",
          )}
          aria-hidden="true"
        />
        <span className="font-medium">local mock</span>
        <span aria-hidden="true">·</span>
        <span>{runtimeStatus.state}</span>
      </div>

      {syncStatus && isDegradedSyncStatus(syncStatus) && (
        <div
          className="hidden shrink-0 items-center gap-1.5 rounded-full border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 text-[11px] text-amber-700 md:flex dark:text-amber-300"
          title={`Runtime synchronization: ${syncStatus}`}
        >
          <span
            className={cn(
              "size-1.5 rounded-full",
              syncStatus === "error" ? "bg-red-500" : "animate-pulse bg-amber-500",
            )}
            aria-hidden="true"
          />
          <span className="font-medium">
            {syncStatus === "stale" ? "resync needed" : syncStatus === "error" ? "sync error" : "syncing"}
          </span>
        </div>
      )}

      {onNavigate &&
        SHORTCUTS_BEFORE_MENU.map((shortcut) => (
          <Shortcut
            key={shortcut.view}
            shortcut={shortcut}
            active={activeView === shortcut.view}
            onNavigate={onNavigate}
          />
        ))}

      <Button
        variant="ghost"
        size="icon-xs"
        aria-label="More actions (placeholder)"
        title="More actions (placeholder)"
      >
        <MoreHorizontal className="size-4" />
      </Button>

      {onNavigate &&
        SHORTCUTS_AFTER_MENU.map((shortcut) => (
          <Shortcut
            key={shortcut.view}
            shortcut={shortcut}
            active={activeView === shortcut.view}
            onNavigate={onNavigate}
          />
        ))}

      {missionControls && (
        <>
          {/* Mobile mission toggle */}
          <Button
            variant="ghost"
            size="icon-xs"
            className="lg:hidden"
            onClick={onOpenMobileMission}
            aria-label="Open mission panel"
            title="Open mission panel"
          >
            <PanelRight className="size-4" />
          </Button>
          {/* Desktop mission toggle */}
          <Button
            variant={!missionOpen ? "secondary" : "ghost"}
            size="icon-xs"
            className="hidden lg:inline-flex"
            onClick={onToggleMission}
            aria-label={missionOpen ? "Collapse mission panel" : "Expand mission panel"}
            aria-expanded={missionOpen}
            title={missionOpen ? "Collapse mission panel" : "Expand mission panel"}
          >
            {missionOpen ? (
              <ChevronsRight className="size-4" />
            ) : (
              <ChevronsLeft className="size-4" />
            )}
          </Button>
        </>
      )}
    </header>
  );
}
