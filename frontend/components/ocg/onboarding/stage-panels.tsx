"use client";

import { useState } from "react";
import { BadgeCheck, Boxes, Cpu, Plug, Plus, Sparkles, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import type { BootstrapResource, BootstrapState, OnboardingStageId } from "../bootstrap/types";
import {
  BOOTSTRAP_MODE_DESCRIPTION,
  BOOTSTRAP_MODE_LABEL,
  CONNECTION_STATE_TONE,
  MODEL_STATUS_TONE,
  ONBOARDING_STAGE_LABEL,
  RESOURCE_STATUS_TONE,
  titleCaseStatus,
} from "../bootstrap/presentation";
import {
  normalizeOnboardingMode,
  selectActiveOnboardingStage,
  selectAvailableModels,
  selectAvailableResources,
} from "../bootstrap/selectors";

type StagePanelProps = {
  bootstrap: BootstrapState;
  onRequestHandoff: () => void;
};

function StatusPill({ label, tone }: { label: string; tone: string }) {
  return (
    <span
      className={cn(
        "inline-flex shrink-0 items-center gap-1.5 rounded-full border border-border bg-muted/40 px-2 py-0.5 text-[10px] capitalize",
        tone,
      )}
    >
      {label}
    </span>
  );
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border bg-muted/20 px-2.5 py-2">
      <dt className="text-[10px] font-semibold tracking-wider text-muted-foreground uppercase">{label}</dt>
      <dd className="mt-0.5 text-[13px] font-medium">{value}</dd>
    </div>
  );
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <h3 className="text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">{children}</h3>
  );
}

function WelcomePanel({ bootstrap }: { bootstrap: BootstrapState }) {
  const mode = normalizeOnboardingMode(bootstrap.onboarding?.mode);
  const stage = selectActiveOnboardingStage(bootstrap);
  const knownResources = bootstrap.resources.filter((resource) => resource.status !== "unknown").length;
  return (
    <div className="flex flex-col gap-3">
      <p className="text-[13px] leading-6 text-muted-foreground">{BOOTSTRAP_MODE_DESCRIPTION[mode]}</p>
      <dl className="grid gap-2 sm:grid-cols-2">
        <Fact label="Mode" value={BOOTSTRAP_MODE_LABEL[mode]} />
        <Fact label="Resources detected" value={String(knownResources)} />
        <Fact label="Connections" value={String(bootstrap.connections.length)} />
        <Fact label="Models discovered" value={String(selectAvailableModels(bootstrap).length)} />
      </dl>
      {bootstrap.onboarding?.canResume && (
        <p
          role="status"
          className="rounded-md border border-border bg-muted/30 px-2.5 py-2 text-[11px] text-muted-foreground"
        >
          Saved progress found. Setup will resume at {ONBOARDING_STAGE_LABEL[stage]}.
        </p>
      )}
    </div>
  );
}

function ResourcesPanel({ bootstrap }: { bootstrap: BootstrapState }) {
  const [addedResources, setAddedResources] = useState<BootstrapResource[]>([]);
  const resources = [...bootstrap.resources, ...addedResources];

  function addResource() {
    const number = addedResources.length + 1;
    setAddedResources((current) => [
      ...current,
      {
        id: `review-resource-${number}`,
        label: `Resource to review ${number}`,
        kind: "unknown",
        source: "imported",
        status: "unknown",
        detail: "No provider metadata has been reported yet.",
        economics: { kind: "unknown", detail: "Pricing not reported." },
        capacity: { status: "unknown", detail: "Capacity not reported." },
        required: false,
      },
    ]);
  }

  return (
    <div className="flex flex-col gap-2">
      <p className="text-[12px] leading-5 text-muted-foreground">
        What resources do you already have access to? Review detected resources or add one for a later runtime check. Entries the runtime has not reported stay unknown.
      </p>
      <div className="flex flex-wrap items-center gap-2">
        <Button size="xs" variant="outline" onClick={addResource}>
          <Plus className="size-3.5" aria-hidden="true" />
          Add resource
        </Button>
        <span className="text-[11px] text-muted-foreground">Detected resources are not changed by this mock.</span>
      </div>
      <ul className="divide-y divide-border rounded-md border border-border">
        {resources.map((resource) => (
          <li key={resource.id} className="flex items-start gap-2 px-3 py-2.5">
            <Boxes className="mt-0.5 size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
            <div className="min-w-0 flex-1">
              <p className="text-[12px] font-medium">{resource.label}</p>
              <p className="text-[11px] text-muted-foreground">
                {titleCaseStatus(resource.kind)} · {resource.source}
                {resource.required ? " · required" : ""}
              </p>
              {(resource.provider || resource.plan) && (
                <p className="mt-0.5 truncate text-[11px] text-muted-foreground">
                  {[resource.provider, resource.plan].filter(Boolean).join(" · ")}
                </p>
              )}
              {resource.detail && <p className="mt-0.5 text-[11px] text-muted-foreground">{resource.detail}</p>}
            </div>
            <div className="flex shrink-0 items-center gap-1.5">
              <StatusPill label={titleCaseStatus(resource.status)} tone={RESOURCE_STATUS_TONE[resource.status]} />
              {resource.id.startsWith("review-resource-") && (
                <Button
                  size="icon-xs"
                  variant="ghost"
                  onClick={() => setAddedResources((current) => current.filter((item) => item.id !== resource.id))}
                  aria-label={`Remove ${resource.label}`}
                >
                  <X className="size-3.5" aria-hidden="true" />
                </Button>
              )}
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}

function ConnectionsPanel({ bootstrap, onRequestHandoff }: StagePanelProps) {
  return (
    <div className="flex flex-col gap-2">
      <p className="text-[12px] leading-5 text-muted-foreground">
        Connections describe reachability only. No credential is requested or stored here.
      </p>
      <ul className="divide-y divide-border rounded-md border border-border">
        {bootstrap.connections.map((connection) => (
          <li key={connection.id} className="flex items-start gap-2 px-3 py-2.5">
            <Plug className="mt-0.5 size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
            <div className="min-w-0 flex-1">
              <p className="text-[12px] font-medium">
                {connection.label}
                {connection.advanced && (
                  <span className="ml-1.5 rounded border border-border px-1 text-[9px] text-muted-foreground">advanced</span>
                )}
              </p>
              <p className="text-[11px] text-muted-foreground">
                {titleCaseStatus(connection.kind)}
                {connection.required ? " · required" : ""}
              </p>
              {connection.detail && <p className="mt-0.5 text-[11px] text-muted-foreground">{connection.detail}</p>}
            </div>
            <div className="flex shrink-0 flex-col items-end gap-1.5">
              <StatusPill label={titleCaseStatus(connection.state)} tone={CONNECTION_STATE_TONE[connection.state]} />
              {connection.requiresHandoff && connection.state !== "connected" && (
                <Button
                  size="xs"
                  variant="outline"
                  onClick={() => {
                    void onRequestHandoff();
                  }}
                >
                  Cloudflare Access handoff
                </Button>
              )}
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}

function DiscoveryPanel({ bootstrap }: { bootstrap: BootstrapState }) {
  return (
    <div className="flex flex-col gap-4">
      <section className="flex flex-col gap-2">
        <SectionTitle>Models</SectionTitle>
        {bootstrap.models.length === 0 ? (
          <p className="rounded-md border border-dashed border-border px-2.5 py-3 text-[11px] text-muted-foreground">
            No model has been reported yet.
          </p>
        ) : (
          <ul className="divide-y divide-border rounded-md border border-border">
            {bootstrap.models.map((model) => (
              <li key={model.id} className="flex items-center gap-2 px-3 py-2.5">
                <Cpu className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
                <div className="min-w-0 flex-1">
                  <p className="truncate text-[12px] font-medium">{model.model}</p>
                  <p className="text-[11px] text-muted-foreground">{model.provider}</p>
                </div>
                <StatusPill label={titleCaseStatus(model.status)} tone={MODEL_STATUS_TONE[model.status]} />
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="flex flex-col gap-2">
        <SectionTitle>Capabilities</SectionTitle>
        {bootstrap.capabilities.length === 0 ? (
          <p className="rounded-md border border-dashed border-border px-2.5 py-3 text-[11px] text-muted-foreground">
            No capability has been reported yet.
          </p>
        ) : (
          <ul className="grid gap-2 sm:grid-cols-2">
            {bootstrap.capabilities.map((capability) => (
              <li key={capability.id} className="flex items-center gap-2 rounded-md border border-border px-2.5 py-2">
                <Sparkles className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span className="min-w-0 flex-1 truncate text-[12px]">{capability.label}</span>
                <StatusPill
                  label={titleCaseStatus(capability.status)}
                  tone={capability.status === "available" ? "text-emerald-600 dark:text-emerald-400" : "text-muted-foreground"}
                />
              </li>
            ))}
          </ul>
        )}
      </section>

      <p className="text-[11px] text-muted-foreground">
        Unavailable facts are shown as unknown rather than filled in.
      </p>
    </div>
  );
}

function OverviewPanel({ bootstrap }: { bootstrap: BootstrapState }) {
  const availableModels = selectAvailableModels(bootstrap);
  const availableResources = selectAvailableResources(bootstrap);
  return (
    <div className="flex flex-col gap-3">
      <dl className="grid gap-2 sm:grid-cols-3">
        <Fact label="Available resources" value={String(availableResources.length)} />
        <Fact label="Total resources" value={String(bootstrap.resources.length)} />
        <Fact label="Available models" value={String(availableModels.length)} />
      </dl>
      <section className="flex flex-col gap-2">
        <SectionTitle>Resource inventory</SectionTitle>
        <ul className="divide-y divide-border rounded-md border border-border">
          {bootstrap.resources.map((resource) => (
            <li key={resource.id} className="flex items-center gap-2 px-3 py-2">
              <span className="min-w-0 flex-1 truncate text-[12px]">{resource.label}</span>
              <span className="max-w-[45%] truncate text-right text-[11px] text-muted-foreground">
                {resource.economics.kind === "unknown" ? "Pricing unknown" : titleCaseStatus(resource.economics.kind)}
                {" · "}
                {resource.capacity.status === "unknown" ? "Capacity unknown" : resource.capacity.detail ?? "Capacity known"}
              </span>
              <StatusPill label={titleCaseStatus(resource.status)} tone={RESOURCE_STATUS_TONE[resource.status]} />
            </li>
          ))}
        </ul>
      </section>
      <section className="flex flex-col gap-2">
        <SectionTitle>Model inventory</SectionTitle>
        <ul className="divide-y divide-border rounded-md border border-border">
          {bootstrap.models.map((model) => (
            <li key={model.id} className="flex items-center gap-2 px-3 py-2">
              <span className="min-w-0 flex-1 truncate text-[12px]">{model.provider} · {model.model}</span>
              <StatusPill label={titleCaseStatus(model.status)} tone={MODEL_STATUS_TONE[model.status]} />
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}

function ProfilePanel({ bootstrap }: { bootstrap: BootstrapState }) {
  return (
    <div className="flex flex-col gap-2">
      <p className="text-[12px] leading-5 text-muted-foreground">
        The recommendation is a starting point. Advanced profiles remain available later.
      </p>
      <ul className="flex flex-col gap-2">
        {bootstrap.profiles.map((profile) => (
          <li
            key={profile.id}
            className={cn(
              "rounded-md border px-3 py-2.5",
              profile.recommended ? "border-foreground/40 bg-muted/40" : "border-border",
            )}
          >
            <div className="flex items-center gap-2">
              {profile.recommended && <BadgeCheck className="size-4 text-emerald-600" aria-hidden="true" />}
              <p className="min-w-0 flex-1 text-[12px] font-medium">{profile.label}</p>
              <span className="rounded border border-border px-1.5 text-[10px] capitalize text-muted-foreground">
                {profile.tier}
              </span>
              {profile.recommended && (
                <span className="rounded border border-border bg-background px-1.5 text-[10px] text-muted-foreground">
                  recommended
                </span>
              )}
            </div>
            <p className="mt-1 text-[11px] text-muted-foreground">{profile.rationale}</p>
            {profile.advanced && <p className="mt-0.5 text-[10px] text-muted-foreground">Advanced option</p>}
          </li>
        ))}
      </ul>
    </div>
  );
}

function ReadyPanel({ bootstrap }: { bootstrap: BootstrapState }) {
  const profile = bootstrap.profiles.find((item) => item.recommended);
  const attention = bootstrap.resources.filter((resource) => resource.status !== "available");
  const unknownEconomics = bootstrap.resources.filter((resource) => resource.economics.kind === "unknown").length;
  const unknownCapacity = bootstrap.resources.filter((resource) => resource.capacity.status === "unknown").length;
  return (
    <div className="flex flex-col gap-3">
      <p className="text-[13px] leading-6 text-muted-foreground">
        Setup is ready to confirm. The workspace opens in the app shell afterward.
      </p>
      <dl className="grid gap-2 sm:grid-cols-2">
        <Fact label="Resources" value={String(selectAvailableResources(bootstrap).length)} />
        <Fact label="Models" value={String(selectAvailableModels(bootstrap).length)} />
        <Fact label="Profile" value={profile?.label ?? "Not selected"} />
        <Fact label="Access" value={titleCaseStatus(bootstrap.access.state)} />
      </dl>
      <div className="rounded-md border border-border bg-muted/20 px-2.5 py-2 text-[11px] text-muted-foreground">
        <p className="font-medium text-foreground">Capability coverage</p>
        <p className="mt-0.5">{bootstrap.capabilities.filter((capability) => capability.status === "available").length} of {bootstrap.capabilities.length} capabilities reported as available.</p>
        <p className="mt-0.5">{unknownEconomics} resource economics and {unknownCapacity} capacity values remain unknown; OCG will not treat them as zero.</p>
      </div>
      {attention.length > 0 && (
        <p role="status" className="rounded-md border border-amber-500/40 bg-amber-500/5 px-2.5 py-2 text-[11px] text-amber-700 dark:text-amber-300">
          {attention.length} resource{attention.length === 1 ? "" : "s"} require attention before they can be used.
        </p>
      )}
    </div>
  );
}

export function StagePanel({ bootstrap, onRequestHandoff }: StagePanelProps) {
  const stage: OnboardingStageId = selectActiveOnboardingStage(bootstrap);
  switch (stage) {
    case "welcome":
      return <WelcomePanel bootstrap={bootstrap} />;
    case "resources":
      return <ResourcesPanel bootstrap={bootstrap} />;
    case "connections":
      return <ConnectionsPanel bootstrap={bootstrap} onRequestHandoff={onRequestHandoff} />;
    case "discovery":
      return <DiscoveryPanel bootstrap={bootstrap} />;
    case "overview":
      return <OverviewPanel bootstrap={bootstrap} />;
    case "profile":
      return <ProfilePanel bootstrap={bootstrap} />;
    case "ready":
      return <ReadyPanel bootstrap={bootstrap} />;
  }
}
