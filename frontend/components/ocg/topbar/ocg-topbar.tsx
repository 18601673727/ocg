import { ChevronRight, LayoutGrid, PanelLeft } from "@lucide/solid";
import { createMemo } from "solid-js";
import { WorkspaceLink } from "@/components/ocg/layout/workspace-link";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ariaState, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { ConversationErrorHeader } from "../chat/conversation-errors";
import { useI18n } from "../i18n";
import type { WorkspaceView } from "../layout/view-domain";
import type { ChatSession } from "../types";

type OcgTopbarProps = {
  session?: ChatSession;
  onOpenMobileSidebar: () => void;
  onHome: () => void;
  activeView?: WorkspaceView;
  titleVisible?: boolean;
  projectSelector?: View;
  inspectorToggle?: View;
};

export function OcgTopbar(props: OcgTopbarProps) {
  const i18n = useI18n();

  const getViewTitle = (view: WorkspaceView): string => {
    const viewTitleMap: Record<WorkspaceView, string> = {
      jobs: i18n.t("jobs.title"),
      chat: i18n.t("nav.chat"),
      home: i18n.t("nav.home"),
      usage: i18n.t("nav.usage"),
      attention: i18n.t("nav.attention"),
      ledger: i18n.t("ledger.title"),
      "job-execution": i18n.t("nav.jobExecution"),
      health: i18n.t("nav.health"),
      logs: i18n.t("logs.title"),
      settings: i18n.t("settings.title"),
    };
    return viewTitleMap[view];
  };

  const title = createMemo(() =>
    props.session && (props.activeView === undefined ? "chat" : props.activeView) === "chat"
      ? props.session.title
      : getViewTitle(props.activeView === undefined ? "chat" : props.activeView),
  );
  return (
    <header
      class={cn(
        "flex h-12 shrink-0 items-center gap-1.5 border-b border-border px-3 sm:px-8 lg:px-10",
        (props.activeView === undefined ? "chat" : props.activeView) === "chat" &&
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
              class="size-10 rounded-lg border-border bg-card/90 shadow-xs md:hidden"
              onClick={props.onOpenMobileSidebar}
              aria-haspopup="dialog"
              aria-label={i18n.t("topbar.openNavigation")}
            >
              <PanelLeft class="size-4" />
            </Button>
          }
        />
        <TooltipContent side="bottom">{i18n.t("topbar.openNavigation")}</TooltipContent>
      </Tooltip>
      {(props.activeView === undefined ? "chat" : props.activeView) !== "chat" && (
        <WorkspaceLink
          href="/"
          onNavigate={props.onHome}
          class="flex shrink-0 items-center gap-2 rounded-md px-1.5 py-1 text-xs text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
        >
          <LayoutGrid class="size-3.5" aria-hidden="true" />
          {i18n.t("settings.nav.workspace")}
        </WorkspaceLink>
      )}
      {(props.activeView === undefined ? "chat" : props.activeView) !== "chat" &&
        (props.titleVisible === undefined ? true : props.titleVisible) && (
          <ChevronRight class="size-3 shrink-0 text-muted-foreground" aria-hidden="true" />
        )}
      {title() &&
        ((props.activeView === undefined ? "chat" : props.activeView) !== "chat" ||
          props.session) && (
          <h1
            aria-hidden={ariaState(!(props.titleVisible === undefined ? true : props.titleVisible))}
            class={cn(
              "min-w-0 max-w-full truncate px-2 text-sm font-medium transition-[opacity,transform] duration-200 ease-out motion-reduce:transition-none",
              (props.titleVisible === undefined ? true : props.titleVisible)
                ? "translate-y-0 opacity-100"
                : "pointer-events-none translate-y-1 opacity-0",
              (props.activeView === undefined ? "chat" : props.activeView) === "chat" && "sr-only",
            )}
          >
            {title()}
          </h1>
        )}
      {props.projectSelector && (
        <div class="flex min-w-0 shrink-0 items-center">{props.projectSelector}</div>
      )}
      {(props.activeView === undefined ? "chat" : props.activeView) === "chat" && (
        <div class="flex min-w-0 flex-1 justify-center">
          <ConversationErrorHeader />
        </div>
      )}
      {props.inspectorToggle}
    </header>
  );
}
