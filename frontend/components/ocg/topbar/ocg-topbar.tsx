"use client";

import {
  ChevronsLeft,
  ChevronsRight,
  PanelLeft,
  PanelRight,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type { ChatSession, RuntimeStatus } from "../types";
import { isDegradedSyncStatus, type RuntimeSyncStatus } from "../runtime/reconciler";
import {
  RUNTIME_AUTHORITY_LABEL,
  RUNTIME_CONNECTION,
  StatusDot,
  SURFACE_TONE,
  SYNC_STATUS,
  TEXT_TONE,
  syncStatusLabel,
} from "@/components/ocg/primitives";
import type { RuntimeAuthority } from "../runtime/runtime-types";
import type { WorkspaceView } from "../layout/view-domain";

type OcgTopbarProps = {
  session?: ChatSession;
  projectName?: string;
  sidebarCollapsed: boolean;
  inspectorOpen: boolean;
  /** When false, the job inspector toggles are hidden (for example on the ledger view). Defaults to true. */
  inspectorControls?: boolean;
  onToggleSidebar: () => void;
  onToggleInspector: () => void;
  onOpenMobileSidebar: () => void;
  onOpenMobileInspector: () => void;
  activeView?: WorkspaceView;
  runtimeStatus: RuntimeStatus;
  /** Which runtime answers Chat, so the pill never names a fixture. */
  runtimeAuthority: RuntimeAuthority;
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
  projectName = "Workspace",
  sidebarCollapsed,
  inspectorOpen,
  inspectorControls = true,
  onToggleSidebar,
  onToggleInspector,
  onOpenMobileSidebar,
  onOpenMobileInspector,
  activeView = "chat",
  runtimeStatus,
  runtimeAuthority,
  syncStatus,
}: OcgTopbarProps) {
  const connection = RUNTIME_CONNECTION[runtimeStatus.state];
  // Degraded sync states are the only ones the bar surfaces; the visual comes
  // from the shared scale so it matches the inspector and the ledger.
  const sync = syncStatus ? SYNC_STATUS[syncStatus] : null;
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
        {session && activeView === "chat" ? (
          <>
            <span aria-hidden="true" className={cn("size-1.5 shrink-0 rounded-full", WORK_TYPE_DOT[session.workType])} />
            <h1 className="truncate text-[13px] font-semibold tracking-tight">{session.title}</h1>
          </>
        ) : (
          <h1 className="truncate text-[13px] font-semibold tracking-tight">{projectName}</h1>
        )}
      </div>

      <div
        className="hidden shrink-0 items-center gap-1.5 rounded-full border border-border bg-muted/40 px-2.5 py-1 text-[11px] text-muted-foreground md:flex"
        title={runtimeStatus.detail ?? RUNTIME_AUTHORITY_LABEL[runtimeAuthority]}
      >
        <StatusDot tone={connection.tone} pulse={connection.pulse} />
        <span className="font-medium">{RUNTIME_AUTHORITY_LABEL[runtimeAuthority]}</span>
        <span aria-hidden="true">·</span>
        <span>{runtimeStatus.state}</span>
      </div>

      {syncStatus && sync && isDegradedSyncStatus(syncStatus) && (
        <div
          className={cn(
            "hidden shrink-0 items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px] md:flex",
            SURFACE_TONE[sync.tone],
            TEXT_TONE[sync.tone],
          )}
          title={`Runtime synchronization: ${syncStatus}`}
        >
          <StatusDot tone={sync.tone} pulse={sync.pulse} />
          <span className="font-medium">{syncStatusLabel(syncStatus)}</span>
        </div>
      )}

      {inspectorControls && (
        <>
          {/* Mobile job inspector toggle */}
          <Button
            variant="ghost"
            size="icon-xs"
            className="lg:hidden"
            onClick={onOpenMobileInspector}
            aria-label="Open job inspector"
            title="Open job inspector"
          >
            <PanelRight className="size-4" />
          </Button>
          {/* Desktop job inspector toggle */}
          <Button
            variant={!inspectorOpen ? "secondary" : "ghost"}
            size="icon-xs"
            className="hidden lg:inline-flex"
            onClick={onToggleInspector}
            aria-label={inspectorOpen ? "Collapse job inspector" : "Expand job inspector"}
            aria-expanded={inspectorOpen}
            title={inspectorOpen ? "Collapse job inspector" : "Expand job inspector"}
          >
            {inspectorOpen ? (
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
