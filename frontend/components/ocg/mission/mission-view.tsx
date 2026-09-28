"use client";

import { CircleDot, ExternalLink, Maximize2, Minimize2, X } from "lucide-react";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type { Mission } from "../types";
import type { RuntimeObservability } from "../runtime/observability";
import { ObservabilityPanel } from "../observability/observability-panel";
import { INSPECTOR_TABS, toggleInspectorMode, type InspectorMode, type InspectorTab } from "../observability/inspector-state";

type MissionViewProps = {
  mission: Mission;
  observability?: RuntimeObservability | null;
  mode?: InspectorMode;
  onModeChange?: (mode: InspectorMode) => void;
  onClose: () => void;
  onOpenMissionControl?: () => void;
};

const TAB_LABELS: Record<InspectorTab, string> = {
  overview: "Overview",
  runtime: "Runtime",
  usage: "Usage",
};

function MissionContext({ mission, compact = false }: { mission: Mission; compact?: boolean }) {
  const pct = mission.total > 0 ? Math.round((mission.completed / mission.total) * 100) : 0;
  return (
    <div className={cn(compact ? "rounded-md border border-border bg-muted/20 px-2.5 py-2" : "", "min-w-0")}>
      <p className={cn("truncate font-semibold tracking-tight", compact ? "text-[12px]" : "text-[14px]")} title={mission.title}>{mission.title}</p>
      <div className="mt-1.5 flex items-center gap-1.5">
        <span className="inline-flex items-center gap-1.5 rounded-full border border-border bg-muted/50 px-2 py-0.5 text-[11px] font-medium capitalize">
          <span className={cn("size-1.5 rounded-full", mission.status === "running" && "animate-pulse bg-amber-500", mission.status === "completed" && "bg-emerald-500", (mission.status === "failed" || mission.status === "budget-exhausted") && "bg-red-500", mission.status === "paused" && "bg-muted-foreground", mission.status === "planning" && "bg-sky-500")} aria-hidden="true" />
          {mission.status.replace("-", " ")}
        </span>
        <span className="text-[11px] text-muted-foreground">{mission.completed} / {mission.total} tasks</span>
        <span className="ml-auto text-[11px] tabular-nums text-muted-foreground">{pct}%</span>
      </div>
      <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-muted" role="progressbar" aria-label="Mission progress" aria-valuemin={0} aria-valuemax={mission.total} aria-valuenow={mission.completed}>
        <div className="h-full rounded-full bg-foreground transition-[width] duration-300" style={{ width: `${pct}%` }} />
      </div>
      {!compact && <p className="mt-1 text-[11px] text-muted-foreground">{pct}% complete · local fixture</p>}
    </div>
  );
}

export function MissionView({ mission, observability, mode = "docked", onModeChange, onClose, onOpenMissionControl }: MissionViewProps) {
  const [tab, setTab] = useState<InspectorTab>("overview");
  const hasObservability = Boolean(observability);
  const nextMode = toggleInspectorMode(mode);
  return (
    <div className="flex h-full w-full min-w-0 flex-col">
      <header className="flex shrink-0 items-center gap-2 border-b border-border px-3 py-2.5">
        <CircleDot className="size-4 text-muted-foreground" aria-hidden="true" />
        <h2 className="flex-1 text-[13px] font-semibold tracking-tight">Mission Inspector</h2>
        {onOpenMissionControl && <Button variant="ghost" size="icon-xs" onClick={onOpenMissionControl} aria-label="Open Mission Control" title="Open Mission Control"><ExternalLink className="size-3.5" /></Button>}
        {onModeChange && <Button variant="ghost" size="icon-xs" className="hidden lg:inline-flex" onClick={() => onModeChange(nextMode)} aria-label={mode === "expanded" ? "Dock mission inspector" : "Expand mission inspector"} title={mode === "expanded" ? "Dock mission inspector" : "Expand mission inspector"}>{mode === "expanded" ? <Minimize2 className="size-3.5" /> : <Maximize2 className="size-3.5" />}</Button>}
        <Button variant="ghost" size="icon-xs" onClick={onClose} aria-label="Collapse mission inspector" title="Collapse mission inspector"><X className="size-4" /></Button>
      </header>

      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
        {!hasObservability ? (
          <div className="flex flex-col gap-3"><MissionContext mission={mission} /><p className="rounded-md border border-dashed border-border px-2.5 py-3 text-[11px] text-muted-foreground">Runtime observability is not available for this Mission yet.</p></div>
        ) : (
          <>
            {tab === "overview" ? <MissionContext mission={mission} /> : <MissionContext mission={mission} compact />}
            <nav className="sticky top-0 z-10 -mx-3 mt-3 border-y border-border bg-background/95 px-3 py-1.5 backdrop-blur" aria-label="Mission inspector surfaces" role="tablist">
              <div className="grid grid-cols-3 gap-1 rounded-md bg-muted/50 p-0.5">
                {INSPECTOR_TABS.map((item) => <button key={item} type="button" role="tab" aria-selected={tab === item} aria-controls={`mission-inspector-${item}`} onClick={() => setTab(item)} className={cn("rounded px-2 py-1.5 text-[11px] font-medium transition-colors focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring", tab === item ? "bg-background text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground")}>{TAB_LABELS[item]}</button>)}
              </div>
            </nav>
            <div id={`mission-inspector-${tab}`} role="tabpanel" aria-label={TAB_LABELS[tab]} className="mt-3"><ObservabilityPanel mission={mission} observability={observability!} tab={tab} /></div>
          </>
        )}
      </div>
    </div>
  );
}
