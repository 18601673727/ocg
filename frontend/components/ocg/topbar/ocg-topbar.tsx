"use client";

import {
  ChevronsLeft,
  ChevronsRight,
  PanelLeft,
  PanelRight,
  Settings,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type { ChatSession } from "../types";
import { isDegradedSyncStatus, type RuntimeSyncStatus } from "../runtime/reconciler";
import {
  StatusDot,
  SURFACE_TONE,
  SYNC_STATUS,
  TEXT_TONE,
} from "@/components/ocg/primitives";
import { useI18n } from "../i18n";
import type { WorkspaceView } from "../layout/view-domain";

type OcgTopbarProps = {
  session?: ChatSession;
  projectName?: string;
  inspectorOpen: boolean;
  /** When false, the job inspector toggles are hidden (for example on the ledger view). Defaults to true. */
  inspectorControls?: boolean;
  onOpenSettings?: () => void;
  onToggleInspector: () => void;
  onOpenMobileSidebar: () => void;
  onOpenMobileInspector: () => void;
  activeView?: WorkspaceView;
  /** Canonical reconciler sync status. Only degraded states are surfaced. */
  syncStatus?: RuntimeSyncStatus | null;
};

export function OcgTopbar({
  session,
  projectName,
  inspectorOpen,
  inspectorControls = true,
  onOpenSettings,
  onToggleInspector,
  onOpenMobileSidebar,
  onOpenMobileInspector,
  activeView = "chat",
  syncStatus,
}: OcgTopbarProps) {
  const { t } = useI18n();
  const fallbackProjectName = t("topbar.workspace");
  const displayProjectName = projectName ?? fallbackProjectName;
  // Degraded sync states are the only ones the bar surfaces; the visual comes
  // from the shared scale so it matches the inspector and the ledger.
  const sync = syncStatus ? SYNC_STATUS[syncStatus] : null;
  const syncLabel = t(
    syncStatus === "stale"
      ? "topbar.syncStale"
      : syncStatus === "error"
        ? "topbar.syncError"
        : "topbar.syncing",
  );
  const inspectorLabel = inspectorOpen ? t("topbar.collapseInspector") : t("topbar.expandInspector");
  return (
    <header className="flex h-12 shrink-0 items-center gap-1.5 border-b border-border bg-background px-2 sm:px-3">
      {/* Mobile sidebar toggle */}
      <Button
        variant="ghost"
        size="icon-xs"
        className="size-11 rounded-md lg:hidden"
        onClick={onOpenMobileSidebar}
        aria-label={t("topbar.openNavigation")}
        title={t("topbar.openNavigation")}
      >
        <PanelLeft className="size-4" />
      </Button>
      <div className="flex min-w-0 flex-1 items-center gap-2">
        {session && activeView === "chat" ? (
          <h1 className="truncate text-[13px] font-semibold tracking-tight">{session.title}</h1>
        ) : (
          <h1 className="truncate text-[13px] font-semibold tracking-tight">{displayProjectName}</h1>
        )}
      </div>

      {syncStatus && sync && isDegradedSyncStatus(syncStatus) && (
        <div
          className={cn(
            "hidden shrink-0 items-center gap-1.5 rounded-md border px-2 py-0.5 text-[11px] md:flex",
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
            className="size-11 rounded-md lg:hidden"
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
      {onOpenSettings && <Button
        variant={activeView === "settings" ? "secondary" : "ghost"}
        size="icon-xs"
        className="size-11 rounded-md sm:size-8"
        onClick={onOpenSettings}
        aria-label={t("sidebar.openSettings")}
        title={t("sidebar.openSettings")}
      >
        <Settings className="size-4" />
      </Button>}
    </header>
  );
}
