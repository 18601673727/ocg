"use client";

/**
 * Providers projection: connection state, the models each one offers, and which
 * profile routes point at them.
 */

import { useMemo } from "react";
import {
  AlertTriangle,
  Cpu,
  Lock,
  Search,
  Server,
  ShieldQuestion,
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
  BootstrapProvider,
  BootstrapProviderState,
  BootstrapState,
} from "../bootstrap/types";
import { MODEL_STATUS_TONE } from "../bootstrap/presentation";
import { PROVIDER_STATE_TONE } from "./status-tone";
import {
  filterProviders,
  selectProviderModels,
  selectProviderAssignments,
  selectProviders,
} from "./domain";
function ProviderListCard({
  provider,
  modelCount,
  selected,
  onSelect,
}: {
  provider: BootstrapProvider;
  modelCount: number;
  selected: boolean;
  onSelect: () => void;
}) {
  return (
    <li className="min-w-0">
      <button
        type="button"
        onClick={onSelect}
        aria-current={selected ? "true" : undefined}
        className={cn(
          "flex w-full min-w-0 items-center gap-2 rounded-md border px-2.5 py-2 text-left transition-colors",
          selected ? "border-foreground/40 bg-muted/40" : "border-border hover:bg-muted/30",
        )}
      >
        <Server className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
        <span className="min-w-0 flex-1 truncate text-[12px] font-medium">{provider.label}</span>
        {provider.authRequired && <Lock className={cn("size-3 shrink-0", TEXT_TONE.sky)} aria-label="Authentication required" />}
        <Pill tone={PROVIDER_STATE_TONE[provider.state]}>{humanizeStatus(provider.state)}</Pill>
        <span className="shrink-0 text-[10px] tabular-nums text-muted-foreground">{modelCount} model(s)</span>
      </button>
    </li>
  );
}

function ProviderDetail({ bootstrap, provider }: { bootstrap: BootstrapState; provider: BootstrapProvider }) {
  const models = selectProviderModels(bootstrap, provider);
  const assignments = selectProviderAssignments(bootstrap, provider);
  return (
    <div className="flex min-w-0 flex-col gap-3">
      <div className="min-w-0 rounded-md border border-border bg-muted/10 p-3">
        <div className="flex min-w-0 flex-wrap items-center gap-1.5">
          <h2 className="min-w-0 truncate text-[14px] font-semibold tracking-tight">{provider.label}</h2>
          <Pill tone={PROVIDER_STATE_TONE[provider.state]}>{humanizeStatus(provider.state)}</Pill>
          <Pill tone={provider.authRequired ? "sky" : "slate"}>{provider.authRequired ? "auth required" : "no auth step"}</Pill>
        </div>
        {provider.detail && <p className="mt-1.5 text-[11px] leading-5 text-muted-foreground">{provider.detail}</p>}
        <dl className="mt-2 grid gap-1.5 sm:grid-cols-2">
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Provider id</dt>
            <dd className="truncate text-[11px] font-medium">{provider.id}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Endpoint identity</dt>
            <dd className="truncate text-[11px] font-medium">{provider.endpointLabel ?? "Not reported"} · {provider.endpointType ?? "type unknown"}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Plan / subscription</dt>
            <dd className="truncate text-[11px] font-medium">{provider.plan ?? "Not reported"}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Capacity</dt>
            <dd className="truncate text-[11px] font-medium">{provider.capacity?.detail ?? "Not reported"}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Economics</dt>
            <dd className="truncate text-[11px] font-medium">{provider.economics?.detail ?? "Not reported"}</dd>
          </div>
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Last checked</dt>
            <dd className="truncate text-[11px] font-medium">{provider.lastCheckedAt ?? "Not reported"}</dd>
          </div>
        </dl>
        <p className="mt-2 flex items-start gap-1.5 text-[10px] text-muted-foreground">
          <Lock className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
          <span className="min-w-0 break-words">
            Only reachability and authentication state are modeled. Credential contents are never stored or displayed here.
          </span>
        </p>
      </div>

      <section aria-label="Provider models">
        <SectionTitle detail={`${models.length} reported · ${provider.discoveredModelCount ?? "—"} discovered`}>Models on this provider</SectionTitle>
        {models.length === 0 ? (
          <EmptyState>No model has been reported for this provider.</EmptyState>
        ) : (
          <ul className="flex flex-col gap-1.5">
            {models.map((model) => (
              <li key={model.id} className="flex min-w-0 items-center gap-2 rounded-md border border-border px-2.5 py-2">
                <Cpu className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span className="min-w-0 flex-1 truncate text-[12px]">{model.model}</span>
                <Pill tone={MODEL_STATUS_TONE[model.status]}>{humanizeStatus(model.status)}</Pill>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section aria-label="Provider route assignments">
        <SectionTitle detail={`${assignments.length} assignment(s)`}>Assigned profile routes</SectionTitle>
        {assignments.length === 0 ? (
          <EmptyState>No profile route currently points to this provider.</EmptyState>
        ) : (
          <ul className="flex flex-col gap-1">
            {assignments.map((assignment) => (
              <li key={`${assignment.profileId}-${assignment.roleId}`} className="flex min-w-0 flex-wrap items-center gap-1.5 rounded-md border border-border px-2.5 py-1.5 text-[11px]">
                <span className="min-w-0 flex-1 truncate">{assignment.profileLabel} · {assignment.roleLabel}</span>
                {assignment.overridden && <Pill tone="violet">overridden</Pill>}
              </li>
            ))}
          </ul>
        )}
      </section>
      {provider.warnings?.map((warning) => (
        <p key={warning} className={cn("flex items-start gap-1.5 text-[10px]", TEXT_TONE.amber)}>
          <AlertTriangle className="mt-0.5 size-3 shrink-0" aria-hidden="true" />{warning}
        </p>
      ))}
    </div>
  );
}

export function ProvidersView({
  bootstrap,
  selectedProviderId,
  onSelectProvider,
  query,
  onQueryChange,
  stateFilter,
  onStateFilterChange,
}: {
  bootstrap: BootstrapState;
  selectedProviderId: string | null;
  onSelectProvider: (id: string) => void;
  query: string;
  onQueryChange: (value: string) => void;
  stateFilter: BootstrapProviderState | "all";
  onStateFilterChange: (value: BootstrapProviderState | "all") => void;
}) {
  const providers = selectProviders(bootstrap);
  const filtered = useMemo(
    () => filterProviders(providers, query).filter((provider) => stateFilter === "all" || provider.state === stateFilter),
    [providers, query, stateFilter],
  );
  const selected = filtered.find((provider) => provider.id === selectedProviderId) ?? filtered[0] ?? null;
  const stateCounts = useMemo(() => {
    const counts: Record<BootstrapProviderState, number> = { connected: 0, "auth-required": 0, degraded: 0, unavailable: 0, unknown: 0 };
    for (const provider of providers) counts[provider.state] += 1;
    return counts;
  }, [providers]);

  return (
    <div className="flex min-w-0 flex-col gap-2">
      <div className="flex min-w-0 flex-wrap items-center gap-1.5">
        <Pill tone="emerald">{stateCounts.connected} connected</Pill>
        <Pill tone="sky">{stateCounts["auth-required"]} auth required</Pill>
        <Pill tone="amber">{stateCounts.degraded} degraded</Pill>
        <Pill tone="red">{stateCounts.unavailable} unavailable</Pill>
        <Pill tone="slate">{stateCounts.unknown} unknown</Pill>
        <label className="ml-auto flex min-w-0 items-center gap-1.5 rounded border border-border bg-background px-2 py-1">
          <Search className="size-3 shrink-0 text-muted-foreground" aria-hidden="true" />
          <input
            type="search"
            value={query}
            onChange={(event) => onQueryChange(event.target.value)}
            placeholder="Filter providers…"
            aria-label="Filter providers"
            className="h-5 w-36 min-w-0 bg-transparent text-[11px] outline-none placeholder:text-muted-foreground"
          />
        </label>
        <select
          value={stateFilter}
          onChange={(event) => onStateFilterChange(event.target.value as BootstrapProviderState | "all")}
          aria-label="Filter providers by connection state"
          className="h-7 min-w-0 max-w-[10rem] truncate rounded border border-border bg-background px-1.5 text-[11px] outline-none"
        >
          <option value="all">All states</option>
          <option value="connected">Connected</option>
          <option value="auth-required">Auth required</option>
          <option value="degraded">Degraded</option>
          <option value="unavailable">Unavailable</option>
          <option value="unknown">Unknown</option>
        </select>
      </div>

      <div className="grid min-w-0 gap-3 lg:grid-cols-[minmax(220px,300px)_minmax(0,1fr)]">
        <section aria-label="Providers" className="min-w-0">
          <SectionTitle detail={`${filtered.length} / ${providers.length}`}>Providers</SectionTitle>
          {filtered.length === 0 ? (
            <EmptyState>No provider matches the filter.</EmptyState>
          ) : (
            <ul className="flex flex-col gap-1.5">
              {filtered.map((provider) => (
                <ProviderListCard
                  key={provider.id}
                  provider={provider}
                  modelCount={selectProviderModels(bootstrap, provider).length}
                  selected={selected?.id === provider.id}
                  onSelect={() => onSelectProvider(provider.id)}
                />
              ))}
            </ul>
          )}
        </section>
        <section aria-label="Provider detail" className="min-w-0">
          {selected ? (
            <ProviderDetail bootstrap={bootstrap} provider={selected} />
          ) : (
            <EmptyState className="py-4">Select a provider to inspect its state and models.</EmptyState>
          )}
        </section>
      </div>

      <p className="flex items-start gap-1.5 text-[10px] text-muted-foreground">
        <ShieldQuestion className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
        <span className="min-w-0 break-words">
          Unknown means the runtime did not report a state. It is not offline or unavailable. This surface never runs discovery or network calls.
        </span>
      </p>
    </div>
  );
}
