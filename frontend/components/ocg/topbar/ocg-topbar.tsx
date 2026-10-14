"use client";

import {
  ChevronsLeft,
  ChevronsRight,
  PanelLeft,
  PanelRight,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
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
  onToggleInspector: () => void;
  onOpenMobileSidebar: () => void;
  onOpenMobileInspector: () => void;
  activeView?: WorkspaceView;
  /** Canonical reconciler sync status. Only degraded states are surfaced. */
  syncStatus?: RuntimeSyncStatus | null;
  /**
   * Compact Project selector for the views that do not present one themselves.
   * Chat owns its selector above the composer, so the shell withholds it here
   * while the Chat surface is up; every other view gets it in this row.
   */
  projectSelector?: React.ReactNode;
};

/** Quiet floating surface shared by the title and the details control. */
const PILL = "rounded-full border border-border/70 bg-muted/40 text-foreground";

export function OcgTopbar({
  session,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  projectName,
  inspectorOpen,
  inspectorControls = true,
  onToggleInspector,
  onOpenMobileSidebar,
  onOpenMobileInspector,
  activeView = "chat",
  syncStatus,
  projectSelector,
}: OcgTopbarProps) {
  const { t } = useI18n();

  // Map view types to i18n title keys
  const getViewTitle = (view: WorkspaceView): string => {
    const viewTitleMap: Record<WorkspaceView, string> = {
      chat: t("nav.chat"),
      home: t("nav.home"),
      usage: t("nav.usage"),
      attention: t("nav.attention"),
      ledger: t("ledger.title"),
      "control-center": t("nav.controlCenter"),
      "job-execution": t("nav.jobExecution"),
      health: t("nav.health"),
      logs: t("logs.title"),
      settings: t("settings.title"),
      canonical: t("nav.canonical"),
    };
    return viewTitleMap[view];
  };

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
  // An untitled conversation has no title to show. The pill is only drawn when
  // there is text for it, rather than leaving an empty outline in the header.
  const title = session && activeView === "chat" ? session.title : getViewTitle(activeView);
  return (
    // The header floats over the conversation: no bottom border and no toolbar
    // band, so the thread reads as one surface. Its controls are pills.
    <header className="flex h-12 shrink-0 items-center gap-1.5 px-2 sm:px-3">
      {/* Mobile sidebar toggle */}
      <Tooltip>
        <TooltipTrigger
          render={
            <Button
              variant="ghost"
              size="icon-xs"
              className="size-11 rounded-md lg:hidden"
              onClick={onOpenMobileSidebar}
              aria-label={t("topbar.openNavigation")}
            >
              <PanelLeft className="size-4" />
            </Button>
          }
        />
        <TooltipContent side="bottom">{t("topbar.openNavigation")}</TooltipContent>
      </Tooltip>
      {/* Conversation title, or the open workspace's name. */}
      {title && (
        <h1 className={cn("min-w-0 max-w-full truncate px-3 py-1.5 text-[13px] font-semibold tracking-tight", PILL)}>
          {title}
        </h1>
      )}
      {projectSelector && <div className="flex min-w-0 shrink-0 items-center">{projectSelector}</div>}

      {syncStatus && sync && isDegradedSyncStatus(syncStatus) && (
        <div
          className={cn(
            "hidden shrink-0 items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs md:flex",
            SURFACE_TONE[sync.tone],
            TEXT_TONE[sync.tone],
          )}
          title={t("topbar.sync", { status: syncStatus })}
        >
          <StatusDot tone={sync.tone} pulse={sync.pulse} />
          <span className="font-medium">{syncLabel}</span>
        </div>
      )}

      {/* The execution-details control, kept clear of the title on the right. */}
      {inspectorControls && (
        <div className="ml-auto flex shrink-0 items-center gap-1" data-execution-details-control="">
          {/* Mobile job inspector toggle */}
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon-xs"
                  className={cn("size-11 rounded-full lg:hidden", PILL)}
                  onClick={onOpenMobileInspector}
                  aria-label={t("topbar.openInspector")}
                >
                  <PanelRight className="size-4" />
                </Button>
              }
            />
            <TooltipContent side="bottom">{t("topbar.openInspector")}</TooltipContent>
          </Tooltip>
          {/* Desktop job inspector toggle */}
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon-xs"
                  className={cn("hidden rounded-full lg:inline-flex", PILL, inspectorOpen && "bg-muted")}
                  onClick={onToggleInspector}
                  aria-label={inspectorLabel}
                  aria-expanded={inspectorOpen}
                >
                  {inspectorOpen ? (
                    <ChevronsRight className="size-4" />
                  ) : (
                    <ChevronsLeft className="size-4" />
                  )}
                </Button>
              }
            />
            <TooltipContent side="bottom">{inspectorLabel}</TooltipContent>
          </Tooltip>
        </div>
      )}
    </header>
  );
}
