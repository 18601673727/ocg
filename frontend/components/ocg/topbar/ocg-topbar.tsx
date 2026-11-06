"use client";

import Link from "next/link";
import { ChevronRight, LayoutGrid, PanelLeft } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";
import type { ChatSession } from "../types";
import { useI18n } from "../i18n";
import type { WorkspaceView } from "../layout/view-domain";
import { ConversationErrorHeader } from "../chat/conversation-errors";

type OcgTopbarProps = {
  session?: ChatSession;
  onOpenMobileSidebar: () => void;
  onHome: () => void;
  activeView?: WorkspaceView;
  titleVisible?: boolean;
  projectSelector?: React.ReactNode;
  inspectorToggle?: React.ReactNode;
};

export function OcgTopbar({
  session,
  onOpenMobileSidebar,
  onHome,
  activeView = "chat",
  titleVisible = true,
  projectSelector,
  inspectorToggle,
}: OcgTopbarProps) {
  const { t } = useI18n();

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

  const title = session && activeView === "chat" ? session.title : getViewTitle(activeView);
  return (
    <header
      className={cn(
        "flex h-12 shrink-0 items-center gap-1.5 border-b border-border px-3 sm:px-8 lg:px-10",
        activeView === "chat" &&
          "pointer-events-none absolute inset-x-0 top-0 z-20 h-[4.25rem] gap-3 border-0 bg-transparent px-3 sm:px-3 lg:px-3 [&_button]:pointer-events-auto",
      )}
    >
      <Tooltip>
        <TooltipTrigger
          render={
            <Button
              id="workspace-navigation-trigger"
              variant="ghost"
              size="icon-xs"
              className="size-10 rounded-lg border-border bg-card/90 shadow-xs md:hidden"
              onClick={onOpenMobileSidebar}
              aria-haspopup="dialog"
              aria-label={t("topbar.openNavigation")}
            >
              <PanelLeft className="size-4" />
            </Button>
          }
        />
        <TooltipContent side="bottom">{t("topbar.openNavigation")}</TooltipContent>
      </Tooltip>
      {activeView !== "chat" && (
        <Link
          href="/"
          prefetch={false}
          onNavigate={(event) => {
            event.preventDefault();
            onHome();
          }}
          className="flex shrink-0 items-center gap-2 rounded-md px-1.5 py-1 text-xs text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
        >
          <LayoutGrid className="size-3.5" aria-hidden="true" />
          {t("settings.nav.workspace")}
        </Link>
      )}
      {activeView !== "chat" && titleVisible && (
        <ChevronRight className="size-3 shrink-0 text-muted-foreground" aria-hidden="true" />
      )}
      {title && (activeView !== "chat" || session) && (activeView !== "chat" || session) && (
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
      {inspectorToggle}
    </header>
  );
}
