"use client";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Pill } from "@/components/ocg/primitives";
import { VIEW_TITLE, type WorkspaceView } from "../layout/view-domain";

type OcgTopbarProps = {
  activeView: WorkspaceView;
  /** The Job on screen, when the Jobs view shows one. */
  job: string | null;
  /** Whether the navigation rail is hidden on a wide screen. */
  collapsed: boolean;
  onOpenNavigation: () => void;
  /** What the Control Plane established about itself, or null when it did not answer. */
  health: {
    host: string;
    database: { status: string; accepting_commands: boolean; readable: boolean };
  } | null;
};

/** Quiet floating surface shared by the title and the readiness pill. */
const PILL = "rounded-full border border-border/70 bg-muted/40 text-foreground";

export function OcgTopbar({ activeView, job, collapsed, onOpenNavigation, health }: OcgTopbarProps) {
  const database = health?.database;
  const tone = !database ? "red" : database.accepting_commands ? "emerald" : database.readable ? "amber" : "red";
  const label = !database
    ? "Control Plane unreachable"
    : database.accepting_commands
      ? "Ready"
      : database.readable
        ? "Read-only"
        : `Database ${database.status}`;
  return (
    <header className="flex h-12 shrink-0 items-center gap-1.5 px-2 sm:px-3">
      <Button
        variant="ghost"
        size="xs"
        className={cn("h-9 rounded-md tracking-normal normal-case", !collapsed && "lg:hidden")}
        onClick={onOpenNavigation}
      >
        Menu
      </Button>
      <h1 className={cn("min-w-0 max-w-full truncate px-3 py-1.5 text-[13px] font-semibold tracking-tight", PILL)}>
        {(activeView === "jobs" || activeView === "chat") && job ? job : VIEW_TITLE[activeView]}
      </h1>
      <div className="ml-auto flex shrink-0 items-center">
        <Pill
          tone={tone}
          dot
          variant="quiet"
          title={health ? `host ${health.host}, database ${health.database.status}` : undefined}
          className="normal-case"
        >
          {label}
        </Pill>
      </div>
    </header>
  );
}
