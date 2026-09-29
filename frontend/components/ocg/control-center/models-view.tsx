"use client";

/**
 * Models projection: availability, capability support, and which profile routes
 * a given model serves.
 */

import { useMemo } from "react";
import {
  Ban,
  CheckCircle2,
  CircleHelp,
  Cpu,
  Search,
  ShieldAlert,
} from "lucide-react";
import {
  EmptyState,
  Pill,
  SectionTitle,
  TEXT_TONE,
} from "@/components/ocg/primitives";
import { humanizeStatus } from "@/lib/format";
import { cn } from "@/lib/utils";
import type {
  BootstrapModel,
  BootstrapModelStatus,
  BootstrapState,
} from "../bootstrap/types";
import { MODEL_STATUS_TONE } from "../bootstrap/presentation";
import { CAPABILITY_TONE, CAPABILITY_LABEL } from "./status-tone";
import {
  controlModelKey,
  filterModels,
  selectAssignedModelIds,
  selectModelCapabilities,
  selectModelAssignments,
  selectModelProvider,
  selectProviders,
} from "./domain";
function ModelListRow({
  bootstrap,
  model,
  selected,
  onSelect,
}: {
  bootstrap: BootstrapState;
  model: BootstrapModel;
  selected: boolean;
  onSelect: () => void;
}) {
  const provider = selectModelProvider(bootstrap, model);
  const capabilities = selectModelCapabilities(model);
  const assignments = selectModelAssignments(bootstrap, model.id);
  const supported = capabilities.filter((entry) => entry.support === "supported").length;
  const unknown = capabilities.filter((entry) => entry.support === "unknown").length;
  return (
    <li className="min-w-0">
      <button
        type="button"
        onClick={onSelect}
        aria-current={selected ? "true" : undefined}
        className={cn(
          "flex w-full min-w-0 items-center gap-2 rounded-md border px-2.5 py-1.5 text-left transition-colors",
          selected ? "border-foreground/40 bg-muted/40" : "border-border hover:bg-muted/30",
        )}
      >
        <Cpu className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
        <span className="min-w-0 flex-1">
          <span className="block truncate text-[12px] font-medium">{model.model}</span>
          <span className="block truncate text-[10px] text-muted-foreground">{provider?.label ?? model.provider}</span>
        </span>
        <span className="hidden shrink-0 text-[10px] text-muted-foreground sm:inline">
           {model.variants?.length ? `${model.variants.length} variant(s)` : "no variants"}
        </span>
        <span className="hidden shrink-0 text-[10px] tabular-nums text-muted-foreground md:inline">
          {assignments.length ? `${assignments.length} role(s)` : "unassigned"}
        </span>
        <span className="shrink-0 text-[10px] tabular-nums text-muted-foreground">{supported}✓ / {unknown}?</span>
        <Pill tone={MODEL_STATUS_TONE[model.status]}>{humanizeStatus(model.status)}</Pill>
      </button>
    </li>
  );
}

function ModelDetail({ bootstrap, model }: { bootstrap: BootstrapState; model: BootstrapModel }) {
  const provider = selectModelProvider(bootstrap, model);
  const capabilities = selectModelCapabilities(model);
  const assignments = selectModelAssignments(bootstrap, model.id);
  const capabilityLabels = new Map(bootstrap.capabilities.map((capability) => [capability.id, capability.label]));

  return (
    <div className="flex min-w-0 flex-col gap-3">
      <div className="min-w-0 rounded-md border border-border bg-muted/10 p-3">
        <div className="flex min-w-0 flex-wrap items-center gap-1.5">
           <h2 title={model.model} className="min-w-0 truncate text-[14px] font-semibold tracking-tight">{model.model}</h2>
          <Pill tone={MODEL_STATUS_TONE[model.status]}>{humanizeStatus(model.status)}</Pill>
        </div>
        <dl className="mt-2 grid gap-1.5 sm:grid-cols-2">
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Provider</dt>
            <dd className="truncate text-[11px] font-medium">
              {provider?.label ?? model.provider}
              {provider ? ` · ${humanizeStatus(provider.state)}` : " · state not reported"}
            </dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Model id</dt>
             <dd title={model.id} className="truncate text-[11px] font-medium">{model.id}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Provider-distinct key</dt>
            <dd className="truncate text-[11px] font-medium" title={controlModelKey(provider?.label ?? model.provider, model.model)}>
              {controlModelKey(provider?.label ?? model.provider, model.model)}
            </dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Provenance</dt>
            <dd className="truncate text-[11px] font-medium">{model.provenanceNote ?? "Not reported"}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Context window</dt>
            <dd className="truncate text-[11px] font-medium">{model.contextWindow ? `${model.contextWindow.toLocaleString()} tokens` : "Not reported"}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Economics</dt>
            <dd className="truncate text-[11px] font-medium">{model.economics?.detail ?? "Not reported"}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Latency observation</dt>
            <dd className="truncate text-[11px] font-medium">{model.latencyObservation?.detail ?? model.latencyObservation?.status ?? "Not reported"}</dd>
          </div>
        </dl>
      </div>

      <section aria-label="Model variants">
        <SectionTitle detail={model.variants?.length ? `${model.variants.length} variant(s)` : "none"}>Variants</SectionTitle>
        {model.variants && model.variants.length > 0 ? (
          <div className="flex flex-wrap gap-1">
            {model.variants.map((variant) => (
              <Pill key={variant} tone="slate">{variant}</Pill>
            ))}
          </div>
        ) : (
          <EmptyState compact>This model reported no variants.</EmptyState>
        )}
      </section>

      <section aria-label="Capability support">
        <SectionTitle detail="unknown ≠ unsupported">Capability support</SectionTitle>
        {capabilities.length === 0 ? (
          <EmptyState>No capability was reported or mapped for this model.</EmptyState>
        ) : (
          <ul className="flex flex-col gap-1.5">
            {capabilities.map((entry) => {
              const label = capabilityLabels.get(entry.id) ?? humanizeStatus(entry.id);
              return (
                <li key={entry.id} className="flex min-w-0 items-center gap-2 rounded-md border border-border px-2.5 py-1.5">
                  {entry.support === "supported" && <CheckCircle2 className={cn("size-3.5 shrink-0", TEXT_TONE.emerald)} aria-hidden="true" />}
                  {entry.support === "unsupported" && <Ban className={cn("size-3.5 shrink-0", TEXT_TONE.red)} aria-hidden="true" />}
                  {entry.support === "unknown" && <CircleHelp className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />}
                  <span className="min-w-0 flex-1 truncate text-[12px]">{label}</span>
                  <span className="shrink-0 text-[10px] text-muted-foreground">{entry.id}</span>
                  <Pill tone={CAPABILITY_TONE[entry.support]}>{CAPABILITY_LABEL[entry.support]}</Pill>
                </li>
              );
            })}
          </ul>
        )}
      </section>

      <section aria-label="Model route assignments">
        <SectionTitle detail={`${assignments.length} assignment(s)`}>Current role assignments</SectionTitle>
        {assignments.length === 0 ? (
          <EmptyState>Unassigned model; no profile route currently points here.</EmptyState>
        ) : (
          <ul className="flex flex-wrap gap-1">
            {assignments.map((assignment) => (
              <li key={`${assignment.profileId}-${assignment.roleId}`}>
                <Pill tone={assignment.overridden ? "violet" : "slate"}>
                  {assignment.profileLabel} · {assignment.roleLabel}{assignment.overridden ? " · override" : ""}
                </Pill>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}

export function ModelsView({
  bootstrap,
  selectedModelId,
  onSelectModel,
  query,
  onQueryChange,
  providerFilter,
  onProviderFilterChange,
  statusFilter,
  onStatusFilterChange,
  assignmentFilter,
  onAssignmentFilterChange,
  capabilityFilter,
  onCapabilityFilterChange,
}: {
  bootstrap: BootstrapState;
  selectedModelId: string | null;
  onSelectModel: (id: string) => void;
  query: string;
  onQueryChange: (value: string) => void;
  providerFilter: string;
  onProviderFilterChange: (value: string) => void;
  statusFilter: BootstrapModelStatus | "all";
  onStatusFilterChange: (value: BootstrapModelStatus | "all") => void;
  assignmentFilter: "assigned" | "unassigned" | "all";
  onAssignmentFilterChange: (value: "assigned" | "unassigned" | "all") => void;
  capabilityFilter: string;
  onCapabilityFilterChange: (value: string) => void;
}) {
  const providers = selectProviders(bootstrap);
  const capabilities = bootstrap.capabilities;
  const assignedModelIds = useMemo(() => selectAssignedModelIds(bootstrap), [bootstrap]);
  const filtered = useMemo(
    () => filterModels(bootstrap.models, {
      query,
      providerId: providerFilter || null,
      status: statusFilter,
      assignment: assignmentFilter,
      capability: capabilityFilter || null,
      assignedModelIds,
    }),
    [assignedModelIds, assignmentFilter, bootstrap.models, capabilityFilter, providerFilter, query, statusFilter],
  );
  const selected = filtered.find((model) => model.id === selectedModelId) ?? filtered[0] ?? null;

  return (
    <div className="flex min-w-0 flex-col gap-2">
      <div className="flex min-w-0 flex-wrap items-center gap-1.5">
        <label className="flex min-w-0 flex-1 items-center gap-1.5 rounded border border-border bg-background px-2 py-1 sm:max-w-xs">
          <Search className="size-3 shrink-0 text-muted-foreground" aria-hidden="true" />
          <input
            type="search"
            value={query}
            onChange={(event) => onQueryChange(event.target.value)}
            placeholder="Search models, providers, variants…"
            aria-label="Search models"
            className="h-5 w-full min-w-0 bg-transparent text-[11px] outline-none placeholder:text-muted-foreground"
          />
        </label>
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="sr-only">Filter by provider</span>
        <select
            value={providerFilter}
            onChange={(event) => onProviderFilterChange(event.target.value)}
            aria-label="Filter models by provider"
            className="h-7 min-w-0 max-w-[13rem] truncate rounded border border-border bg-background px-1.5 text-[11px] outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30"
          >
            <option value="">All providers</option>
            {providers.map((provider) => (
              <option key={provider.id} value={provider.id}>{provider.label}</option>
            ))}
        </select>
        <select
          value={statusFilter}
          onChange={(event) => onStatusFilterChange(event.target.value as BootstrapModelStatus | "all")}
          aria-label="Filter models by availability"
          className="h-7 min-w-0 max-w-[9rem] truncate rounded border border-border bg-background px-1.5 text-[11px] outline-none"
        >
          <option value="all">All availability</option>
          <option value="available">Available</option>
          <option value="pending">Pending</option>
          <option value="unavailable">Unavailable</option>
          <option value="unknown">Unknown</option>
        </select>
        <select
          value={assignmentFilter}
          onChange={(event) => onAssignmentFilterChange(event.target.value as "assigned" | "unassigned" | "all")}
          aria-label="Filter models by assignment"
          className="h-7 min-w-0 max-w-[9rem] truncate rounded border border-border bg-background px-1.5 text-[11px] outline-none"
        >
          <option value="all">All assignments</option>
          <option value="assigned">Assigned</option>
          <option value="unassigned">Unassigned</option>
        </select>
        <select
          value={capabilityFilter}
          onChange={(event) => onCapabilityFilterChange(event.target.value)}
          aria-label="Filter models by capability"
          className="h-7 min-w-0 max-w-[10rem] truncate rounded border border-border bg-background px-1.5 text-[11px] outline-none"
        >
          <option value="">All capabilities</option>
          {capabilities.map((capability) => <option key={capability.id} value={capability.id}>{capability.label}</option>)}
        </select>
        </div>
        <span className="ml-auto shrink-0 text-[10px] tabular-nums text-muted-foreground">
          {filtered.length} / {bootstrap.models.length} models
        </span>
      </div>

      <div className="grid min-w-0 gap-3 lg:grid-cols-[minmax(240px,360px)_minmax(0,1fr)]">
        <section aria-label="Models" className="min-w-0">
          <SectionTitle detail="same name on different providers stays distinct">Dense model list</SectionTitle>
          {filtered.length === 0 ? (
            <EmptyState>No model matches the current search and provider filter.</EmptyState>
          ) : (
            <ul className="flex flex-col gap-1">
              {filtered.map((model) => (
                <ModelListRow
                  key={model.id}
                  bootstrap={bootstrap}
                  model={model}
                  selected={selected?.id === model.id}
                  onSelect={() => onSelectModel(model.id)}
                />
              ))}
            </ul>
          )}
        </section>
        <section aria-label="Model detail" className="min-w-0">
          {selected ? (
            <ModelDetail bootstrap={bootstrap} model={selected} />
          ) : (
            <EmptyState className="py-4">Select a model to inspect its full identity and capability support.</EmptyState>
          )}
        </section>
      </div>

      <p className="flex items-start gap-1.5 text-[10px] text-muted-foreground">
        <ShieldAlert className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
        <span className="min-w-0 break-words">
          The model catalogue is fully dynamic. OCG renders whatever the normalized state reports; it has no baked-in provider or model list.
        </span>
      </p>
    </div>
  );
}
