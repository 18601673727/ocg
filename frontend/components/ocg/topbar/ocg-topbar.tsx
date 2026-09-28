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

type OcgTopbarProps = {
  session: ChatSession;
  sidebarCollapsed: boolean;
  missionOpen: boolean;
  /** When false, the mission panel toggles are hidden (for example on the ledger view). Defaults to true. */
  missionControls?: boolean;
  /** Highlights the ledger action while the ledger workspace is the active view. */
  ledgerActive?: boolean;
  /** Highlights the Control Center action while it is the active view. */
  controlCenterActive?: boolean;
  missionControlActive?: boolean;
  logsActive?: boolean;
  settingsActive?: boolean;
  homeActive?: boolean;
  attentionActive?: boolean;
  onToggleSidebar: () => void;
  onToggleMission: () => void;
  onOpenMobileSidebar: () => void;
  onOpenMobileMission: () => void;
  onOpenChat?: () => void;
  onOpenHome?: () => void;
  onOpenAttention?: () => void;
  onOpenLedger?: () => void;
  onOpenControlCenter?: () => void;
  onOpenMissionControl?: () => void;
  onOpenLogs?: () => void;
  onOpenSettings?: () => void;
  runtimeStatus: RuntimeStatus;
  /** Canonical reconciler sync status. Only degraded states are surfaced. */
  syncStatus?: RuntimeSyncStatus | null;
};

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
  ledgerActive = false,
  controlCenterActive = false,
  missionControlActive = false,
  logsActive = false,
  settingsActive = false,
  homeActive = false,
  attentionActive = false,
  onToggleSidebar,
  onToggleMission,
  onOpenMobileSidebar,
  onOpenMobileMission,
  onOpenChat,
  onOpenHome,
  onOpenAttention,
  onOpenLedger,
  onOpenControlCenter,
  onOpenMissionControl,
  onOpenLogs,
  onOpenSettings,
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

      {onOpenHome && (
        <Button
          variant={homeActive ? "secondary" : "ghost"}
          size="icon-xs"
          onClick={onOpenHome}
          aria-label={homeActive ? "Close Home" : "Open Home"}
          aria-current={homeActive ? "page" : undefined}
          title="Open Home"
        >
          <Home className="size-4" />
        </Button>
      )}

      {onOpenAttention && (
        <Button
          variant={attentionActive ? "secondary" : "ghost"}
          size="icon-xs"
          onClick={onOpenAttention}
          aria-label={attentionActive ? "Close Attention" : "Open Attention"}
          aria-current={attentionActive ? "page" : undefined}
          title="Open Attention"
        >
          <Bell className="size-4" />
        </Button>
      )}

      <Button
        variant="ghost"
        size="icon-xs"
        aria-label="More actions (placeholder)"
        title="More actions (placeholder)"
      >
        <MoreHorizontal className="size-4" />
      </Button>

      {onOpenLedger && (
        <Button
          variant={ledgerActive ? "secondary" : "ghost"}
          size="icon-xs"
          onClick={onOpenLedger}
          aria-label={ledgerActive ? "Open chat workspace" : "Open resource ledger"}
          aria-current={ledgerActive ? "page" : undefined}
          title={ledgerActive ? "Open chat workspace" : "Open resource ledger"}
        >
          <Table2 className="size-4" />
        </Button>
      )}

      {onOpenControlCenter && (
        <Button
          variant={controlCenterActive ? "secondary" : "ghost"}
          size="icon-xs"
          onClick={onOpenControlCenter}
          aria-label={controlCenterActive ? "Close Control Center" : "Open Control Center"}
          aria-current={controlCenterActive ? "page" : undefined}
          title={controlCenterActive ? "Close Control Center" : "Open Control Center"}
        >
          <SlidersHorizontal className="size-4" />
        </Button>
      )}

      {onOpenMissionControl && (
        <Button
          variant={missionControlActive ? "secondary" : "ghost"}
          size="icon-xs"
          onClick={onOpenMissionControl}
          aria-label={missionControlActive ? "Close Mission Control" : "Open Mission Control"}
          aria-current={missionControlActive ? "page" : undefined}
          title={missionControlActive ? "Close Mission Control" : "Open Mission Control"}
        >
          <Workflow className="size-4" />
        </Button>
      )}

      {onOpenLogs && (
        <Button
          variant={logsActive ? "secondary" : "ghost"}
          size="icon-xs"
          onClick={onOpenLogs}
          aria-label={logsActive ? "Close Logs" : "Open Logs and diagnostics"}
          aria-current={logsActive ? "page" : undefined}
          title={logsActive ? "Close Logs" : "Open Logs and diagnostics"}
        >
          <ScrollText className="size-4" />
        </Button>
      )}

      {onOpenSettings && (
        <Button
          variant={settingsActive ? "secondary" : "ghost"}
          size="icon-xs"
          onClick={onOpenSettings}
          aria-label={settingsActive ? "Close Settings" : "Open Settings"}
          aria-current={settingsActive ? "page" : undefined}
          title={settingsActive ? "Close Settings" : "Open Settings"}
        >
          <Settings className="size-4" />
        </Button>
      )}

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
