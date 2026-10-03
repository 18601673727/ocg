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
  RUNTIME_CONNECTION,
  StatusDot,
  SURFACE_TONE,
  SYNC_STATUS,
  TEXT_TONE,
} from "@/components/ocg/primitives";
import { useI18n } from "../i18n";
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
  projectName,
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
  const { t } = useI18n();
  const fallbackProjectName = t("topbar.workspace");
  const displayProjectName = projectName ?? fallbackProjectName;
  const connection = RUNTIME_CONNECTION[runtimeStatus.state];
  // Degraded sync states are the only ones the bar surfaces; the visual comes
  // from the shared scale so it matches the inspector and the ledger.
  const sync = syncStatus ? SYNC_STATUS[syncStatus] : null;
  const authorityLabel = t(
    runtimeAuthority === "canonical" ? "topbar.authority.canonical" : "topbar.authority.mock",
  );
  const connectionLabel = t(
    runtimeStatus.state === "connected"
      ? "topbar.connection.ready"
      : runtimeStatus.state === "connecting"
        ? "topbar.connection.connecting"
        : runtimeStatus.state === "failed"
          ? "topbar.connection.failed"
          : "topbar.connection.disconnected",
  );
  const syncLabel = t(
    syncStatus === "stale"
      ? "topbar.syncStale"
      : syncStatus === "error"
        ? "topbar.syncError"
        : "topbar.syncing",
  );
  const sidebarLabel = sidebarCollapsed ? t("topbar.expandSidebar") : t("topbar.collapseSidebar");
  const inspectorLabel = inspectorOpen ? t("topbar.collapseInspector") : t("topbar.expandInspector");
  return (
    <header className="flex h-12 shrink-0 items-center gap-1.5 border-b border-border bg-background px-2 sm:px-3">
      {/* Mobile sidebar toggle */}
      <Button
        variant="ghost"
        size="icon-xs"
        className="lg:hidden"
        onClick={onOpenMobileSidebar}
        aria-label={t("topbar.openNavigation")}
        title={t("topbar.openNavigation")}
      >
        <PanelLeft className="size-4" />
      </Button>
      {/* Desktop sidebar toggle */}
      <Button
        variant="ghost"
        size="icon-xs"
        className="hidden lg:inline-flex"
        onClick={onToggleSidebar}
        aria-label={sidebarLabel}
        aria-expanded={!sidebarCollapsed}
        title={sidebarLabel}
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
          <h1 className="truncate text-[13px] font-semibold tracking-tight">{displayProjectName}</h1>
        )}
      </div>

      <div
        className="hidden shrink-0 items-center gap-1.5 rounded-full border border-border bg-muted/40 px-2.5 py-1 text-[11px] text-muted-foreground md:flex"
        title={runtimeStatus.detail ?? authorityLabel}
      >
        <StatusDot tone={connection.tone} pulse={connection.pulse} />
        <span className="font-medium">{authorityLabel}</span>
        <span aria-hidden="true">·</span>
        <span>{connectionLabel}</span>
      </div>

      {syncStatus && sync && isDegradedSyncStatus(syncStatus) && (
        <div
          className={cn(
            "hidden shrink-0 items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px] md:flex",
            SURFACE_TONE[sync.tone],
            TEXT_TONE[sync.tone],
          )}
          title={t("topbar.sync", { status: syncStatus })}
        >
          <StatusDot tone={sync.tone} pulse={sync.pulse} />
          <span className="font-medium">{syncLabel}</span>
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
            aria-label={t("topbar.openInspector")}
            title={t("topbar.openInspector")}
          >
            <PanelRight className="size-4" />
          </Button>
          {/* Desktop job inspector toggle */}
          <Button
            variant={!inspectorOpen ? "secondary" : "ghost"}
            size="icon-xs"
            className="hidden lg:inline-flex"
            onClick={onToggleInspector}
            aria-label={inspectorLabel}
            aria-expanded={inspectorOpen}
            title={inspectorLabel}
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
