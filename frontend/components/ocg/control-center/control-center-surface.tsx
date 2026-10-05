"use client";

import { startTransition, useMemo, useState, ViewTransition } from "react";
import { Activity, Coins, Cpu, Layers, Server, Users } from "lucide-react";
import { formatCostMicros } from "@/lib/format";
import { Button } from "@/components/ui/button";
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
import type { WorkspaceView } from "../layout/view-domain";
import { ModelsView } from "./models-view";
import { ProfilesView } from "./profiles-view";
import { ProvidersView } from "./providers-view";
import { useI18n } from "../i18n";

const VIEW_ICON: Record<ControlCenterView, typeof Layers> = {
  profiles: Users,
  providers: Server,
  models: Cpu,
};

const VIEW_LABEL_KEY = {
  profiles: "control.profiles",
  providers: "control.providers",
  models: "control.models",
} as const;

function LedgerStrip({ ledger }: { ledger: ResourceLedger | null }) {
  const summary = useMemo(() => (ledger ? summarize(ledger.entries) : null), [ledger]);
  if (!summary) {
    return (
      <p className="text-[10px] text-muted-foreground">
        Cost data is not reported in this summary. Detailed attribution stays in the Resource Ledger.
      </p>
    );
  }
  return (
    <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted-foreground">
      <span className="inline-flex items-center gap-1">
        <Coins className="size-3 shrink-0" aria-hidden="true" />
        <strong className="font-semibold tabular-nums text-foreground">{formatCostMicros(summary.costMicros)}</strong>
        <span>({summary.costProvenance})</span>
      </span>
    </div>
  );
}

export type ControlCenterSurfaceProps = {
  bootstrap: BootstrapState;
  ledger: ResourceLedger | null;
  initialView?: ControlCenterView;
  canActivateProfiles: boolean;
  attentionCount: number;
  onNavigate: (view: WorkspaceView) => void;
  onSelectProfile: (profileId: string) => void;
};

/**
 * Operational attention summary with Providers and Models as drill-downs.
 * Profiles are shown only where the runtime supports their activation action.
 */
export function ControlCenterSurface({
  bootstrap,
  ledger,
  initialView = "profiles",
  canActivateProfiles,
  attentionCount,
  onNavigate,
  onSelectProfile,
}: ControlCenterSurfaceProps) {
  const { t } = useI18n();
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
  const visibleViews = useMemo(
    () => canActivateProfiles ? CONTROL_CENTER_VIEWS : CONTROL_CENTER_VIEWS.filter((id) => id !== "profiles"),
    [canActivateProfiles],
  );
  const tabs: TabItem<ControlCenterView>[] = useMemo(
    () =>
      visibleViews.map((id) => ({
        id,
        label: t(VIEW_LABEL_KEY[id]),
        icon: VIEW_ICON[id],
      })),
    [t, visibleViews],
  );

  return (
    <div className="flex h-full min-h-0 w-full min-w-0 flex-col">
      <header className="shrink-0 border-b border-border px-3 py-2.5">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <h1 className="text-[13px] font-semibold tracking-tight">{t("control.title")}</h1>
          <Pill tone="sky" title="Profile reported by the runtime">
            {summary.activeProfileLabel ?? t("common.notReported")}
          </Pill>
          <span className="text-[10px] text-muted-foreground">
            {summary.profileCount} profiles reported · {summary.providerCount === null ? t("common.notReported") : `${summary.providerCount} providers`} · {summary.modelCount} models reported
          </span>
          <span className="ml-auto flex shrink-0 items-center gap-1 text-[10px] text-muted-foreground">
            <Activity className="size-3" aria-hidden="true" />
            {summary.availableModelCount} available · {summary.pendingModelCount} pending · {summary.unavailableModelCount} unavailable · {summary.unknownModelCount} unknown
          </span>
        </div>
        {summary.stateIncomplete && <p className="mt-1 text-[10px] text-muted-foreground">{t("control.stateIncomplete")}</p>}
        <div className="mt-2 flex flex-wrap items-center gap-2 rounded-md border border-border bg-muted/10 px-2.5 py-2">
          <div className="min-w-0 flex-1">
            <p className="text-[11px] font-semibold">{t("control.needsAttention")}</p>
            <p className="text-[10px] text-muted-foreground">
              {attentionCount === 0 ? t("home.noAttentionRecorded") : t(attentionCount === 1 ? "home.attention.one" : "home.attention.other", { count: attentionCount })}
            </p>
          </div>
          <Button size="xs" variant="outline" onClick={() => onNavigate("attention")}>{t("home.viewAll")}</Button>
        </div>
        <div className="mt-2 grid grid-cols-2 gap-1.5 sm:grid-cols-3 lg:grid-cols-4">
          <Metric
            label={t("control.profiles")}
            value={String(summary.profileCount)}
            detail={summary.activeProfileLabel ? `${summary.activeProfileLabel} active` : t("common.notReported")}
          />
          <Metric
            label={t("control.providers")}
            value={summary.providerCount === null ? t("common.notReported") : String(summary.providerCount)}
            detail={`${summary.degradedProviderCount ?? t("common.notReported")} degraded · ${summary.unavailableProviderCount ?? t("common.notReported")} unavailable`}
          />
          <Metric
            label="Auth required"
            value={summary.authRequiredProviderCount === null ? t("common.notReported") : String(summary.authRequiredProviderCount)}
            detail={`${summary.unknownProviderCount ?? t("common.notReported")} unknown`}
          />
          <Metric label="Available models" value={String(summary.availableModelCount)} detail={`${summary.modelCount} reported`} />
        </div>
        <div className="mt-2 min-w-0">
          <LedgerStrip ledger={ledger} />
        </div>
      </header>

      <nav className="shrink-0 border-b border-border px-3 py-1.5">
        <SegmentedTabs
          tabs={tabs}
          value={view}
          onSelect={(next) => startTransition(() => setView(next))}
          ariaLabel={t("control.surfaces")}
          panelIdBase="control-center"
          className={canActivateProfiles ? "grid-cols-3" : "grid-cols-2"}
        />
      </nav>

      <ViewTransition key={view} enter="vt-detail" exit="vt-detail" default="none">
        <div id={`control-center-${view}`} role="tabpanel" aria-label={t(VIEW_LABEL_KEY[view])} className="min-h-0 min-w-0 flex-1 overflow-y-auto px-3 py-3">
          {view === "profiles" && canActivateProfiles && (
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
              onSelectProvider={(id) => startTransition(() => setSelectedProviderId(id))}
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
              onSelectModel={(id) => startTransition(() => setSelectedModelId(id))}
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
      </ViewTransition>
    </div>
  );
}
