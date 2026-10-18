"use client";

import { startTransition, useMemo, useState, ViewTransition } from "react";
import { Coins, Cpu, Layers, Server, Users } from "lucide-react";
import { formatCostMicros } from "@/lib/format";
import { Button } from "@/components/ui/button";
import { Pill, SegmentedTabs, type TabItem } from "@/components/ocg/primitives";
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
  const { t } = useI18n();
  const summary = useMemo(() => (ledger ? summarize(ledger.entries) : null), [ledger]);
  if (!summary) {
    return (
      <p className="text-xs text-muted-foreground">
        {t("control.costDataNotReported")}
      </p>
    );
  }
  return (
    <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
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
      <header className="shrink-0 border-b border-border px-4 sm:px-6 lg:px-8 py-3 sm:py-4 lg:py-5">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <h1 className="text-[13px] font-semibold tracking-tight">{t("control.title")}</h1>
          <Pill tone="sky" title={t("control.profileReportedByRuntime")}>
            {summary.activeProfileLabel ?? t("common.notReported")}
          </Pill>
        </div>
        <div className="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          <span>
            {summary.profileCount} {t("control.profiles").toLowerCase()}
          </span>
          <span>·</span>
          <span>
            {summary.providerCount === null ? t("common.notReported") : summary.providerCount} {t("control.providers").toLowerCase()}
            {summary.providerCount !== null && summary.degradedProviderCount !== null && summary.degradedProviderCount > 0 && (
              <> ({summary.degradedProviderCount} {t("control.degraded")})</>
            )}
            {summary.providerCount !== null && summary.unavailableProviderCount !== null && summary.unavailableProviderCount > 0 && (
              <> ({summary.unavailableProviderCount} {t("control.unavailable")})</>
            )}
          </span>
          {summary.authRequiredProviderCount !== null && summary.authRequiredProviderCount > 0 && (
            <>
              <span>·</span>
              <span className="text-foreground">
                {summary.authRequiredProviderCount} {t("control.authRequired").toLowerCase()}
              </span>
            </>
          )}
          <span>·</span>
          <span>
            {summary.modelCount} {t("control.models").toLowerCase()} ({summary.availableModelCount} {t("control.available").toLowerCase()})
          </span>
        </div>
        {summary.stateIncomplete && <p className="mt-1 text-xs text-muted-foreground">{t("control.stateIncomplete")}</p>}
        <div className="mt-2.5 flex flex-wrap items-center gap-2 rounded-md border border-border bg-muted/10 px-2.5 py-2">
          <div className="min-w-0 flex-1">
            <p className="text-xs font-semibold">{t("control.needsAttention")}</p>
            <p className="text-xs text-muted-foreground">
              {attentionCount === 0 ? t("home.noAttentionRecorded") : t(attentionCount === 1 ? "home.attention.one" : "home.attention.other", { count: attentionCount })}
            </p>
          </div>
          <Button size="xs" variant="outline" onClick={() => onNavigate("attention")}>{t("home.viewAll")}</Button>
        </div>
        <div className="mt-2.5 min-w-0">
          <LedgerStrip ledger={ledger} />
        </div>
      </header>

      <nav className="shrink-0 border-b border-border px-4 sm:px-6 lg:px-8 py-2 sm:py-3 lg:py-4">
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
        <div id={`control-center-${view}`} role="tabpanel" aria-label={t(VIEW_LABEL_KEY[view])} className="min-h-0 min-w-0 flex-1 overflow-y-auto px-4 sm:px-6 lg:px-8 py-4 sm:py-6 lg:py-8">
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
