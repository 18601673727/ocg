"use client";

import { useMemo, useState } from "react";
import { Activity, Coins, Cpu, Layers, Server, Users } from "lucide-react";
import { formatCostMicros, formatCount } from "@/lib/format";
import { Metric, Pill, SegmentedTabs, type TabItem } from "@/components/ocg/primitives";
import type {
  BootstrapModelStatus,
  BootstrapProviderState,
  BootstrapState,
} from "../bootstrap/types";
import {
  CONTROL_CENTER_VIEWS,
  selectActiveProfile,
  selectControlCenterSummary,
  selectProviders,
  type ControlCenterView,
} from "./domain";
import { summarize } from "../resource-ledger/selectors";
import type { ResourceLedger } from "../resource-ledger/types";
import { ModelsView } from "./models-view";
import { ProfilesView } from "./profiles-view";
import { ProvidersView } from "./providers-view";

const VIEW_ICON: Record<ControlCenterView, typeof Layers> = {
  profiles: Users,
  providers: Server,
  models: Cpu,
};

const VIEW_LABEL: Record<ControlCenterView, string> = {
  profiles: "Profiles",
  providers: "Providers",
  models: "Models",
};

const TABS: TabItem<ControlCenterView>[] = CONTROL_CENTER_VIEWS.map((id) => ({
  id,
  label: VIEW_LABEL[id],
  icon: VIEW_ICON[id],
}));

function LedgerStrip({ ledger }: { ledger: ResourceLedger | null }) {
  const summary = useMemo(() => (ledger ? summarize(ledger.entries) : null), [ledger]);
  if (!summary) {
    return (
      <p className="text-[10px] text-muted-foreground">
        No normalized resource ledger for this scenario. Call and cost attribution stays with the Resource Ledger surface.
      </p>
    );
  }
  return (
    <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted-foreground">
      <span className="inline-flex items-center gap-1">
        <Layers className="size-3 shrink-0" aria-hidden="true" />
        <strong className="font-semibold tabular-nums text-foreground">{formatCount(summary.entryCount)}</strong> calls
      </span>
      <span className="inline-flex items-center gap-1">
        <Coins className="size-3 shrink-0" aria-hidden="true" />
        <strong className="font-semibold tabular-nums text-foreground">{formatCostMicros(summary.costMicros)}</strong>
        <span>({summary.costProvenance})</span>
      </span>
      <span>{summary.missionCount} missions</span>
      <span>{summary.workerCount} workers</span>
      <span>{summary.leadEntryCount} lead calls</span>
    </div>
  );
}

export type ControlCenterSurfaceProps = {
  bootstrap: BootstrapState;
  ledger: ResourceLedger | null;
  initialView?: ControlCenterView;
  onSelectProfile: (profileId: string) => void;
};

/**
 * Frontend-only Profiles + Providers + Models Control Center. All data is
 * derived from the normalized bootstrap state; the only mutation exposed is the
 * mock active-profile switch.
 */
export function ControlCenterSurface({
  bootstrap,
  ledger,
  initialView = "profiles",
  onSelectProfile,
}: ControlCenterSurfaceProps) {
  const [view, setView] = useState<ControlCenterView>(initialView);
  const activeProfile = selectActiveProfile(bootstrap);
  const [selectedProfileId, setSelectedProfileId] = useState<string | null>(activeProfile?.id ?? null);
  const [selectedProviderId, setSelectedProviderId] = useState<string | null>(selectProviders(bootstrap)[0]?.id ?? null);
  const [selectedModelId, setSelectedModelId] = useState<string | null>(bootstrap.models[0]?.id ?? null);
  const [providerQuery, setProviderQuery] = useState("");
  const [providerStateFilter, setProviderStateFilter] = useState<BootstrapProviderState | "all">("all");
  const [modelQuery, setModelQuery] = useState("");
  const [modelProviderFilter, setModelProviderFilter] = useState("");
  const [modelStatusFilter, setModelStatusFilter] = useState<BootstrapModelStatus | "all">("all");
  const [modelAssignmentFilter, setModelAssignmentFilter] = useState<"assigned" | "unassigned" | "all">("all");
  const [modelCapabilityFilter, setModelCapabilityFilter] = useState("");
  const summary = useMemo(() => selectControlCenterSummary(bootstrap), [bootstrap]);

  return (
    <div className="flex h-full min-h-0 w-full min-w-0 flex-col">
      <header className="shrink-0 border-b border-border px-3 py-2.5">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <h1 className="text-[13px] font-semibold tracking-tight">Control Center</h1>
          <Pill tone="sky" title="Active profile in the mock runtime">
            {summary.activeProfileLabel ?? "No active profile"}
          </Pill>
          <span className="text-[10px] text-muted-foreground">
            {summary.profileCount} profiles · {summary.providerCount} providers · {summary.modelCount} models
          </span>
          <span className="ml-auto flex shrink-0 items-center gap-1 text-[10px] text-muted-foreground">
            <Activity className="size-3" aria-hidden="true" />
            {summary.availableModelCount} available · {summary.unavailableModelCount} unavailable · {summary.unknownModelCount} unknown
          </span>
        </div>
        <div className="mt-2 grid grid-cols-2 gap-1.5 sm:grid-cols-3 lg:grid-cols-4">
          <Metric label="Profiles" value={String(summary.profileCount)} detail={`${summary.activeProfileLabel ?? "none"} active`} />
          <Metric label="Providers" value={String(summary.providerCount)} detail={`${summary.degradedProviderCount} degraded · ${summary.unavailableProviderCount} unavailable`} />
          <Metric label="Auth required" value={String(summary.authRequiredProviderCount)} detail={`${summary.unknownProviderCount} unknown`} />
          <Metric label="Available models" value={String(summary.availableModelCount)} detail={`${summary.modelCount} total`} />
        </div>
        <div className="mt-2 min-w-0">
          <LedgerStrip ledger={ledger} />
        </div>
      </header>

      <nav className="shrink-0 border-b border-border px-3 py-1.5">
        <SegmentedTabs
          tabs={TABS}
          value={view}
          onSelect={setView}
          ariaLabel="Control Center surfaces"
          panelIdBase="control-center"
          className="grid-cols-3"
        />
      </nav>

      <div
        id={`control-center-${view}`}
        role="tabpanel"
        aria-label={VIEW_LABEL[view]}
        className="min-h-0 min-w-0 flex-1 overflow-y-auto px-3 py-3"
      >
        {view === "profiles" && (
          <ProfilesView
            bootstrap={bootstrap}
            selectedProfileId={selectedProfileId}
            onSelectProfile={setSelectedProfileId}
            onActivateProfile={(id) => {
              setSelectedProfileId(id);
              onSelectProfile(id);
            }}
          />
        )}
        {view === "providers" && (
          <ProvidersView
            bootstrap={bootstrap}
            selectedProviderId={selectedProviderId}
            onSelectProvider={setSelectedProviderId}
            query={providerQuery}
            onQueryChange={setProviderQuery}
            stateFilter={providerStateFilter}
            onStateFilterChange={setProviderStateFilter}
          />
        )}
        {view === "models" && (
          <ModelsView
            bootstrap={bootstrap}
            selectedModelId={selectedModelId}
            onSelectModel={setSelectedModelId}
            query={modelQuery}
            onQueryChange={setModelQuery}
            providerFilter={modelProviderFilter}
            onProviderFilterChange={setModelProviderFilter}
            statusFilter={modelStatusFilter}
            onStatusFilterChange={setModelStatusFilter}
            assignmentFilter={modelAssignmentFilter}
            onAssignmentFilterChange={setModelAssignmentFilter}
            capabilityFilter={modelCapabilityFilter}
            onCapabilityFilterChange={setModelCapabilityFilter}
          />
        )}
      </div>
    </div>
  );
}
