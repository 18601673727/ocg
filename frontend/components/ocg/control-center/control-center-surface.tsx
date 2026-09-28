"use client";

import { useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  BadgeCheck,
  Ban,
  CheckCircle2,
  CircleHelp,
  Coins,
  Cpu,
  GitBranch,
  Layers,
  Lock,
  Search,
  Server,
  ShieldAlert,
  ShieldQuestion,
  Star,
  Users,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type {
  BootstrapModel,
  BootstrapModelStatus,
  BootstrapProfile,
  BootstrapProvider,
  BootstrapProviderState,
  BootstrapState,
} from "../bootstrap/types";
import {
  CONTROL_CENTER_VIEWS,
  controlModelKey,
  filterModels,
  filterProviders,
  selectActiveProfile,
  selectControlCenterSummary,
  selectLeadRoutes,
  selectModelCapabilities,
  selectModelAssignments,
  selectModelProvider,
  selectProfileById,
  selectProfileHealth,
  selectProfileRoutes,
  selectProfileSource,
  selectProfileWarnings,
  selectProviderModels,
  selectProviderAssignments,
  selectProviders,
  selectRoutesForRole,
  selectWorkerRoles,
  type ControlCenterView,
  type ProfileHealth,
  type ResolvedRoute,
  type RouteStatus,
} from "./domain";
import { summarize } from "../resource-ledger/selectors";
import { formatCostMicros, formatCount } from "../resource-ledger/format";
import type { ResourceLedger } from "../resource-ledger/types";

type Tone = "emerald" | "sky" | "amber" | "red" | "violet" | "slate";

const TONE_CLASS: Record<Tone, string> = {
  emerald: "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400",
  sky: "border-sky-500/30 bg-sky-500/10 text-sky-700 dark:text-sky-400",
  amber: "border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-400",
  red: "border-red-500/30 bg-red-500/10 text-red-700 dark:text-red-400",
  violet: "border-violet-500/30 bg-violet-500/10 text-violet-700 dark:text-violet-400",
  slate: "border-border bg-muted/50 text-muted-foreground",
};

const HEALTH_TONE: Record<ProfileHealth, Tone> = {
  healthy: "emerald",
  degraded: "amber",
  unavailable: "red",
  incomplete: "violet",
  unknown: "slate",
};

const HEALTH_LABEL: Record<ProfileHealth, string> = {
  healthy: "Healthy",
  degraded: "Degraded",
  unavailable: "Unavailable",
  incomplete: "Incomplete",
  unknown: "Unknown",
};

const ROUTE_TONE: Record<RouteStatus, Tone> = {
  ready: "emerald",
  degraded: "amber",
  fallback: "amber",
  unavailable: "red",
  pending: "violet",
  unknown: "slate",
  unassigned: "violet",
  "auth-required": "sky",
};

const ROUTE_LABEL: Record<RouteStatus, string> = {
  ready: "Ready",
  degraded: "Degraded",
  fallback: "Fallback",
  unavailable: "Unavailable",
  pending: "Pending",
  unknown: "Unknown",
  unassigned: "Unassigned",
  "auth-required": "Auth required",
};

const PROVIDER_STATE_TONE: Record<BootstrapProviderState, Tone> = {
  connected: "emerald",
  "auth-required": "sky",
  degraded: "amber",
  unavailable: "red",
  unknown: "slate",
};

const MODEL_STATUS_TONE: Record<BootstrapModelStatus, Tone> = {
  available: "emerald",
  pending: "violet",
  unavailable: "red",
  unknown: "slate",
};

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

function titleCase(value: string): string {
  return value.replace(/-/g, " ");
}

function Pill({ children, tone = "slate", title }: { children: React.ReactNode; tone?: Tone; title?: string }) {
  return (
    <span
      title={title}
      className={cn(
        "inline-flex shrink-0 items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] font-medium capitalize",
        TONE_CLASS[tone],
      )}
    >
      {children}
    </span>
  );
}

function SectionTitle({ children, detail }: { children: React.ReactNode; detail?: React.ReactNode }) {
  return (
    <div className="mb-1.5 flex min-w-0 items-center gap-1.5">
      <h3 className="truncate text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">{children}</h3>
      {detail !== undefined && <span className="min-w-0 truncate text-[10px] text-muted-foreground">{detail}</span>}
    </div>
  );
}

function SummaryMetric({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className="min-w-0 rounded-md border border-border bg-background px-2 py-1.5">
      <p className="truncate text-[10px] text-muted-foreground">{label}</p>
      <p className="mt-0.5 truncate text-[13px] font-semibold tabular-nums">{value}</p>
      {detail && <p className="truncate text-[10px] text-muted-foreground">{detail}</p>}
    </div>
  );
}

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

function ProfileListCard({
  profile,
  health,
  active,
  selected,
  onSelect,
}: {
  profile: BootstrapProfile;
  health: ProfileHealth;
  active: boolean;
  selected: boolean;
  onSelect: () => void;
}) {
  const source = selectProfileSource(profile);
  return (
    <li className="min-w-0">
      <button
        type="button"
        onClick={onSelect}
        aria-current={selected ? "true" : undefined}
        className={cn(
          "flex w-full min-w-0 flex-col gap-1 rounded-md border px-2.5 py-2 text-left transition-colors",
          selected ? "border-foreground/40 bg-muted/40" : "border-border hover:bg-muted/30",
        )}
      >
        <div className="flex min-w-0 items-center gap-1.5">
          {profile.recommended && <BadgeCheck className="size-3.5 shrink-0 text-emerald-600 dark:text-emerald-400" aria-hidden="true" />}
          <span className="min-w-0 flex-1 truncate text-[12px] font-medium">{profile.label}</span>
          {active && <Pill tone="sky" title="Currently active profile">active</Pill>}
        </div>
        <div className="flex min-w-0 flex-wrap items-center gap-1">
          <Pill tone="slate">{profile.tier}</Pill>
          <Pill tone={source === "recommended" ? "sky" : "violet"}>{source}</Pill>
          <Pill tone={HEALTH_TONE[health]}>{HEALTH_LABEL[health]}</Pill>
        </div>
        <p className="line-clamp-2 text-[10px] text-muted-foreground">{profile.rationale}</p>
      </button>
    </li>
  );
}

function RouteRow({ resolved }: { resolved: ResolvedRoute }) {
  const { route, model, provider, status } = resolved;
  const target = model
    ? `${provider?.label ?? model.provider} · ${model.model}${route.variant ? ` · ${route.variant}` : ""}`
    : route.modelId
      ? `Referenced model ${route.modelId}`
      : "No model assigned";

  return (
    <li className="min-w-0 rounded-md border border-border px-2.5 py-2">
      <div className="flex min-w-0 flex-wrap items-center gap-1.5">
        <span className="inline-flex min-w-0 items-center gap-1">
          <GitBranch className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
          <span className="truncate text-[12px] font-medium">{route.roleLabel}</span>
        </span>
        {route.tier && <Pill tone="slate" title="Lead tier">{route.tier}</Pill>}
        <span className="text-muted-foreground" aria-hidden="true">→</span>
        <span className="min-w-0 flex-1 truncate text-[11px] text-muted-foreground" title={target}>{target}</span>
        <Pill tone={ROUTE_TONE[status]} title={`Route status: ${ROUTE_LABEL[status]}`}>{ROUTE_LABEL[status]}</Pill>
        {route.overridden && <Pill tone="violet" title="Overridden from the profile default">overridden</Pill>}
        {route.fallback && <Pill tone="amber" title="Presented as using a fallback model">fallback</Pill>}
      </div>
      {route.fallbackModelId && route.fallback && (
        <p className="mt-1 truncate text-[10px] text-muted-foreground">Fallback model id: {route.fallbackModelId}</p>
      )}
      {route.warning && <p className="mt-1 text-[10px] text-amber-700 dark:text-amber-300">{route.warning}</p>}
    </li>
  );
}

function ProfileDetail({
  bootstrap,
  profile,
  active,
  onActivate,
}: {
  bootstrap: BootstrapState;
  profile: BootstrapProfile;
  active: boolean;
  onActivate: () => void;
}) {
  const health = selectProfileHealth(bootstrap, profile);
  const routes = selectProfileRoutes(bootstrap, profile);
  const leadRoutes = selectLeadRoutes(routes);
  const workerRoles = selectWorkerRoles(routes);
  const warnings = selectProfileWarnings(bootstrap, profile);
  const source = selectProfileSource(profile);

  return (
    <div className="flex min-w-0 flex-col gap-3">
      <div className="min-w-0 rounded-md border border-border bg-muted/10 p-3">
        <div className="flex min-w-0 flex-wrap items-center gap-1.5">
          <h2 className="min-w-0 truncate text-[14px] font-semibold tracking-tight">{profile.label}</h2>
          <Pill tone={HEALTH_TONE[health]} title="Derived profile health">{HEALTH_LABEL[health]}</Pill>
          <Pill tone="slate">{profile.tier}</Pill>
          <Pill tone={source === "recommended" ? "sky" : "violet"}>
            {source === "recommended" ? "recommended" : "customized"}
          </Pill>
          {profile.advanced && <Pill tone="violet">advanced</Pill>}
          {profile.recommended && <Pill tone="emerald" title="Runtime recommendation">recommended</Pill>}
        </div>
        <p className="mt-1.5 text-[11px] leading-5 text-muted-foreground">{profile.rationale}</p>
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <Button
            size="xs"
            variant={active ? "outline" : "default"}
            disabled={active}
            onClick={onActivate}
            title={active ? "This profile is already active" : "Set this profile active in the mock runtime"}
          >
            {active ? <CheckCircle2 className="size-3" data-icon="inline-start" aria-hidden="true" /> : <Star className="size-3" data-icon="inline-start" aria-hidden="true" />}
            {active ? "Active" : "Set active"}
          </Button>
          <span className="text-[10px] text-muted-foreground">Frontend-only mock switch. No configuration is written.</span>
        </div>
      </div>

      <section aria-label="Lead tiers">
        <SectionTitle detail={`${leadRoutes.length} lead route(s)`}>Lead tiers</SectionTitle>
        {leadRoutes.length === 0 ? (
          <p className="rounded-md border border-dashed border-border px-2 py-3 text-[11px] text-muted-foreground">
            This profile declares no lead route.
          </p>
        ) : (
          <ul className="flex flex-col gap-1.5">
            {leadRoutes.map((resolved) => (
              <RouteRow key={resolved.route.id} resolved={resolved} />
            ))}
          </ul>
        )}
      </section>

      <section aria-label="Worker roles">
        <SectionTitle detail={`${workerRoles.length} dynamic worker role(s)`}>Worker roles</SectionTitle>
        {workerRoles.length === 0 ? (
          <p className="rounded-md border border-dashed border-border px-2 py-3 text-[11px] text-muted-foreground">
            This profile declares no worker routes.
          </p>
        ) : (
          <div className="flex flex-col gap-2">
            {workerRoles.map((role) => (
              <div key={role.roleId} className="min-w-0">
                <p className="mb-1 truncate text-[10px] font-semibold tracking-wider text-muted-foreground uppercase">
                  {role.roleLabel}
                  <span className="ml-1 font-normal normal-case tracking-normal">({role.roleId})</span>
                </p>
                <ul className="flex flex-col gap-1.5">
                  {selectRoutesForRole(routes, role.roleId).map((resolved) => (
                    <RouteRow key={resolved.route.id} resolved={resolved} />
                  ))}
                </ul>
              </div>
            ))}
          </div>
        )}
      </section>

      <section aria-label="Profile warnings">
        <SectionTitle detail={warnings.length > 0 ? `${warnings.length} item(s)` : "none"}>Warnings</SectionTitle>
        {warnings.length === 0 ? (
          <p className="rounded-md border border-border bg-muted/10 px-2.5 py-2 text-[11px] text-muted-foreground">
            No fallback, override, or availability warnings for this profile.
          </p>
        ) : (
          <ul className="flex flex-col gap-1">
            {warnings.map((warning) => (
              <li key={warning} className="flex items-start gap-1.5 rounded-md border border-amber-500/30 bg-amber-500/5 px-2.5 py-1.5 text-[11px] text-amber-800 dark:text-amber-300">
                <AlertTriangle className="mt-0.5 size-3.5 shrink-0" aria-hidden="true" />
                <span className="min-w-0 break-words">{warning}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}

function ProfilesView({
  bootstrap,
  selectedProfileId,
  onSelectProfile,
  onActivateProfile,
}: {
  bootstrap: BootstrapState;
  selectedProfileId: string | null;
  onSelectProfile: (id: string) => void;
  onActivateProfile: (id: string) => void;
}) {
  const active = selectActiveProfile(bootstrap);
  const selected = selectProfileById(bootstrap, selectedProfileId) ?? active;

  return (
    <div className="grid min-w-0 gap-3 lg:grid-cols-[minmax(220px,300px)_minmax(0,1fr)]">
      <section aria-label="Profiles" className="min-w-0">
        <SectionTitle detail={`${bootstrap.profiles.length} profile(s)`}>Profiles</SectionTitle>
        <ul className="flex flex-col gap-1.5">
          {bootstrap.profiles.map((profile) => (
            <ProfileListCard
              key={profile.id}
              profile={profile}
              health={selectProfileHealth(bootstrap, profile)}
              active={active?.id === profile.id}
              selected={selected?.id === profile.id}
              onSelect={() => onSelectProfile(profile.id)}
            />
          ))}
        </ul>
      </section>
      <section aria-label="Profile detail" className="min-w-0">
        {selected ? (
          <ProfileDetail
            bootstrap={bootstrap}
            profile={selected}
            active={active?.id === selected.id}
            onActivate={() => onActivateProfile(selected.id)}
          />
        ) : (
          <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">
            No profile is available in this workspace.
          </p>
        )}
      </section>
    </div>
  );
}

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
        {provider.authRequired && <Lock className="size-3 shrink-0 text-sky-600 dark:text-sky-400" aria-label="Authentication required" />}
        <Pill tone={PROVIDER_STATE_TONE[provider.state]}>{titleCase(provider.state)}</Pill>
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
          <Pill tone={PROVIDER_STATE_TONE[provider.state]}>{titleCase(provider.state)}</Pill>
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
          <p className="rounded-md border border-dashed border-border px-2 py-3 text-[11px] text-muted-foreground">
            No model has been reported for this provider.
          </p>
        ) : (
          <ul className="flex flex-col gap-1.5">
            {models.map((model) => (
              <li key={model.id} className="flex min-w-0 items-center gap-2 rounded-md border border-border px-2.5 py-2">
                <Cpu className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span className="min-w-0 flex-1 truncate text-[12px]">{model.model}</span>
                <Pill tone={MODEL_STATUS_TONE[model.status]}>{titleCase(model.status)}</Pill>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section aria-label="Provider route assignments">
        <SectionTitle detail={`${assignments.length} assignment(s)`}>Assigned profile routes</SectionTitle>
        {assignments.length === 0 ? (
          <p className="rounded-md border border-dashed border-border px-2 py-3 text-[11px] text-muted-foreground">No profile route currently points to this provider.</p>
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
        <p key={warning} className="flex items-start gap-1.5 text-[10px] text-amber-700 dark:text-amber-300">
          <AlertTriangle className="mt-0.5 size-3 shrink-0" aria-hidden="true" />{warning}
        </p>
      ))}
    </div>
  );
}

function ProvidersView({
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
            <p className="rounded-md border border-dashed border-border px-2 py-3 text-[11px] text-muted-foreground">
              No provider matches the filter.
            </p>
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
            <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">
              Select a provider to inspect its state and models.
            </p>
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

const CAPABILITY_TONE: Record<"supported" | "unsupported" | "unknown", Tone> = {
  supported: "emerald",
  unsupported: "red",
  unknown: "slate",
};

const CAPABILITY_LABEL: Record<"supported" | "unsupported" | "unknown", string> = {
  supported: "Supported",
  unsupported: "Unsupported",
  unknown: "Unknown",
};

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
        <Pill tone={MODEL_STATUS_TONE[model.status]}>{titleCase(model.status)}</Pill>
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
          <Pill tone={MODEL_STATUS_TONE[model.status]}>{titleCase(model.status)}</Pill>
        </div>
        <dl className="mt-2 grid gap-1.5 sm:grid-cols-2">
          <div className="min-w-0 rounded border border-border bg-background px-2 py-1.5">
            <dt className="text-[10px] text-muted-foreground">Provider</dt>
            <dd className="truncate text-[11px] font-medium">
              {provider?.label ?? model.provider}
              {provider ? ` · ${titleCase(provider.state)}` : " · state not reported"}
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
          <p className="rounded-md border border-dashed border-border px-2 py-2 text-[11px] text-muted-foreground">
            This model reported no variants.
          </p>
        )}
      </section>

      <section aria-label="Capability support">
        <SectionTitle detail="unknown ≠ unsupported">Capability support</SectionTitle>
        {capabilities.length === 0 ? (
          <p className="rounded-md border border-dashed border-border px-2 py-3 text-[11px] text-muted-foreground">
            No capability was reported or mapped for this model.
          </p>
        ) : (
          <ul className="flex flex-col gap-1.5">
            {capabilities.map((entry) => {
              const label = capabilityLabels.get(entry.id) ?? titleCase(entry.id);
              return (
                <li key={entry.id} className="flex min-w-0 items-center gap-2 rounded-md border border-border px-2.5 py-1.5">
                  {entry.support === "supported" && <CheckCircle2 className="size-3.5 shrink-0 text-emerald-600 dark:text-emerald-400" aria-hidden="true" />}
                  {entry.support === "unsupported" && <Ban className="size-3.5 shrink-0 text-red-600 dark:text-red-400" aria-hidden="true" />}
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
          <p className="rounded-md border border-dashed border-border px-2 py-3 text-[11px] text-muted-foreground">Unassigned model; no profile route currently points here.</p>
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

function ModelsView({
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
  const assignedModelIds = useMemo(
    () => new Set(bootstrap.profiles.flatMap((profile) => (profile.routes ?? [])
      .filter((route) => route.modelId !== null)
      .map((route) => route.modelId as string))),
    [bootstrap.profiles],
  );
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
            <p className="rounded-md border border-dashed border-border px-2 py-3 text-[11px] text-muted-foreground">
              No model matches the current search and provider filter.
            </p>
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
            <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">
              Select a model to inspect its full identity and capability support.
            </p>
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
export function ControlCenterSurface({ bootstrap, ledger, initialView = "profiles", onSelectProfile }: ControlCenterSurfaceProps) {
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
          <SummaryMetric label="Profiles" value={String(summary.profileCount)} detail={`${summary.activeProfileLabel ?? "none"} active`} />
          <SummaryMetric label="Providers" value={String(summary.providerCount)} detail={`${summary.degradedProviderCount} degraded · ${summary.unavailableProviderCount} unavailable`} />
          <SummaryMetric label="Auth required" value={String(summary.authRequiredProviderCount)} detail={`${summary.unknownProviderCount} unknown`} />
          <SummaryMetric label="Available models" value={String(summary.availableModelCount)} detail={`${summary.modelCount} total`} />
        </div>
        <div className="mt-2 min-w-0">
          <LedgerStrip ledger={ledger} />
        </div>
      </header>

      <nav aria-label="Control Center surfaces" role="tablist" className="shrink-0 border-b border-border px-3 py-1.5">
        <div className="grid grid-cols-3 gap-1 rounded-md bg-muted/50 p-0.5">
          {CONTROL_CENTER_VIEWS.map((item) => {
            const Icon = VIEW_ICON[item];
            return (
              <button
                key={item}
                type="button"
                role="tab"
                aria-selected={view === item}
                aria-controls={`control-center-${item}`}
                onClick={() => setView(item)}
                className={cn(
                  "inline-flex items-center justify-center gap-1.5 rounded px-1 py-1.5 text-[11px] font-medium transition-colors focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring",
                  view === item ? "bg-background text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground",
                )}
              >
                <Icon className="size-3.5" aria-hidden="true" />
                {VIEW_LABEL[item]}
              </button>
            );
          })}
        </div>
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
