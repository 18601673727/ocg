"use client";

import { PanelLeft } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import type { ChatSession } from "../types";
import { isDegradedSyncStatus, type RuntimeSyncStatus } from "../runtime/reconciler";
import { StatusDot, SURFACE_TONE, SYNC_STATUS, TEXT_TONE } from "@/components/ocg/primitives";
import { useI18n } from "../i18n";
import type { WorkspaceView } from "../layout/view-domain";
import { ConversationErrorHeader } from "../chat/conversation-errors";

type OcgTopbarProps = {
  session?: ChatSession;
  projectName?: string;
  onOpenMobileSidebar: () => void;
  activeView?: WorkspaceView;
  titleVisible?: boolean;
  /** Canonical reconciler sync status. Only degraded states are surfaced. */
  syncStatus?: RuntimeSyncStatus | null;
  /**
   * Compact Project selector for the views that do not present one themselves.
   * Chat owns its selector above the composer, so the shell withholds it here
   * while the Chat surface is up; every other view gets it in this row.
   */
  projectSelector?: React.ReactNode;
  inspectorToggle?: React.ReactNode;
  backButton?: React.ReactNode;
};

export function OcgTopbar({
  session,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  projectName,
  onOpenMobileSidebar,
  activeView = "chat",
  titleVisible = true,
  syncStatus,
  projectSelector,
  inspectorToggle,
  backButton,
}: OcgTopbarProps) {
  const { t } = useI18n();

  // Map view types to i18n title keys
  const getViewTitle = (view: WorkspaceView): string => {
    const viewTitleMap: Record<WorkspaceView, string> = {
      jobs: t("jobs.title"),
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
  const title = session && activeView === "chat" ? session.title : getViewTitle(activeView);
  return (
    <header
      className={cn(
        "flex h-14 shrink-0 items-center gap-1.5 px-2 sm:px-5",
        activeView === "chat" &&
          "pointer-events-none absolute inset-x-0 top-0 z-20 h-[4.25rem] gap-3 bg-transparent px-3 sm:px-3 [&_button]:pointer-events-auto",
      )}
    >
      {/* Mobile sidebar toggle */}
      <Tooltip>
        <TooltipTrigger
          render={
            <Button
              variant="ghost"
              size="icon-xs"
              className="size-11 rounded-md md:hidden"
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
        <h1
          aria-hidden={!titleVisible}
          className={cn(
            "min-w-0 max-w-full truncate px-2 text-sm font-medium transition-[opacity,transform] duration-200 ease-out motion-reduce:transition-none",
            titleVisible
              ? "translate-y-0 opacity-100"
              : "pointer-events-none translate-y-1 opacity-0",
            activeView === "chat" && "sr-only",
          )}
        >
          {title}
        </h1>
      )}
      {projectSelector && (
        <div className="flex min-w-0 shrink-0 items-center">{projectSelector}</div>
      )}
      {activeView === "chat" && (
        <div className="flex min-w-0 flex-1 justify-center">
          <ConversationErrorHeader key={session?.id ?? "new"} />
        </div>
      )}

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

      {backButton}
      {inspectorToggle}
    </header>
  );
}
