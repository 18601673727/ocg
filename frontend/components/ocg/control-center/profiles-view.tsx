"use client";

/**
 * Profiles projection: the mock runtime's role-to-model routing, per profile.
 */

import {
  AlertTriangle,
  BadgeCheck,
  CheckCircle2,
  GitBranch,
  Star,
} from "lucide-react";
import {
  EmptyState,
  Pill,
  SectionTitle,
  TEXT_TONE,
  TONE_CLASS,
} from "@/components/ocg/primitives";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type {
  BootstrapProfile,
  BootstrapState,
} from "../bootstrap/types";
import { HEALTH_TONE, HEALTH_LABEL, ROUTE_TONE, ROUTE_LABEL } from "./status-tone";
import {
  selectActiveProfile,
  selectLeadRoutes,
  selectProfileById,
  selectProfileHealth,
  selectProfileRoutes,
  selectProfileSource,
  selectProfileWarnings,
  selectRoutesForRole,
  selectWorkerRoles,
  type ProfileHealth,
  type ResolvedRoute,
} from "./domain";
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
          {profile.recommended && <BadgeCheck className={cn("size-3.5 shrink-0", TEXT_TONE.emerald)} aria-hidden="true" />}
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
      {route.warning && <p className={cn("mt-1 text-[10px]", TEXT_TONE.amber)}>{route.warning}</p>}
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
          <EmptyState>This profile declares no lead route.</EmptyState>
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
          <EmptyState>This profile declares no worker routes.</EmptyState>
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
              <li key={warning} className={cn("flex items-start gap-1.5 rounded-md border px-2.5 py-1.5 text-[11px]", TONE_CLASS.amber)}>
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

export function ProfilesView({
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
          <EmptyState className="py-4">No profile is available in this workspace.</EmptyState>
        )}
      </section>
    </div>
  );
}
