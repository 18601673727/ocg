"use client";

import type { ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { PageSurface, Pill } from "@/components/ocg/primitives";
import { cn } from "@/lib/utils";
import { useTheme } from "../appearance/theme-provider";

type Health = {
  host: string;
  database: { status: string; accepting_commands: boolean; readable: boolean };
};

function ChoiceGroup<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: readonly { value: T; label: string }[];
  onChange: (value: T) => void;
}) {
  return (
    <div className="flex flex-wrap gap-2" role="group" aria-label={label}>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={value === option.value}
          onClick={() => onChange(option.value)}
          className={cn(
            "min-h-11 rounded-md border px-3 py-2 text-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring/30",
            value === option.value
              ? "border-primary/50 bg-primary/10 font-medium text-foreground"
              : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground",
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

function SettingRow({
  label,
  description,
  origin,
  children,
}: {
  label: string;
  description: string;
  origin: "browser" | "runtime";
  children: ReactNode;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-2 border-b border-border/70 py-3 last:border-b-0 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h3 className="text-sm font-medium">{label}</h3>
          <Pill tone={origin === "browser" ? "violet" : "sky"} className="text-xs normal-case">
            {origin === "browser" ? "Browser only" : "Reported by OCG"}
          </Pill>
        </div>
        <p className="mt-1 max-w-2xl text-[13px] leading-5 text-muted-foreground">{description}</p>
      </div>
      <div className="min-w-0 shrink-0 sm:max-w-[48%] sm:text-right">{children}</div>
    </div>
  );
}

function Section({
  id,
  title,
  description,
  action,
  children,
}: {
  id: string;
  title: string;
  description: string;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section
      aria-labelledby={id}
      className="rounded-lg border border-border bg-background px-3 py-2.5 sm:px-4"
    >
      <div className="flex items-start justify-between gap-3 border-b border-border pb-3">
        <div className="min-w-0">
          <h2 id={id} className="text-base font-semibold tracking-tight">{title}</h2>
          <p className="mt-1 text-[13px] leading-5 text-muted-foreground">{description}</p>
        </div>
        {action}
      </div>
      <div>{children}</div>
    </section>
  );
}

const value = "[overflow-wrap:anywhere] text-[13px] font-medium text-foreground";

/**
 * Adapted from the 0.4.5 Settings surface. Only settings with a real owner
 * are here: appearance is this browser's own, and the rest is what the
 * Control Plane itself reports. The 0.4.5 Provider, model, workspace and
 * language settings have no 0.5 contract, so they are listed as unavailable
 * rather than drawn as controls.
 */
export function SettingsSurface({
  health,
  sessionExpiresAtUs,
  signingOut,
  onSignOut,
}: {
  health: Health | null;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  onSignOut: () => void;
}) {
  const theme = useTheme();
  return (
    <PageSurface className="mx-auto w-full max-w-4xl">
      <header className="mb-6 flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
          <p className="mt-2 max-w-xl text-sm leading-6 text-muted-foreground">
            What this browser and this OCG run can show today.
          </p>
        </div>
        <div role="status" className="rounded-lg border border-border bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
          <p className="font-medium text-foreground">
            {theme.resolvedTheme === "dark" ? "Dark" : "Light"} theme active
          </p>
          <p className="mt-1">{theme.hydrated ? "Saved in this browser" : "Loading preferences…"}</p>
        </div>
      </header>
      <div className="flex min-w-0 flex-col gap-4">
        <Section
          id="settings-appearance"
          title="Appearance"
          description="Presentation preferences, kept in this browser alone."
          action={
            <Button
              variant="ghost"
              size="sm"
              className="h-11 rounded-md tracking-normal normal-case"
              onClick={theme.resetAppearance}
            >
              Reset
            </Button>
          }
        >
          <SettingRow label="Theme" description="Follow the system, or fix light or dark." origin="browser">
            <ChoiceGroup
              label="Theme"
              value={theme.preferences.theme}
              onChange={theme.setTheme}
              options={[
                { value: "system", label: "System" },
                { value: "light", label: "Light" },
                { value: "dark", label: "Dark" },
              ]}
            />
          </SettingRow>
          <SettingRow label="Density" description="How tightly surfaces are spaced." origin="browser">
            <ChoiceGroup
              label="Density"
              value={theme.preferences.density}
              onChange={theme.setDensity}
              options={[
                { value: "comfortable", label: "Comfortable" },
                { value: "compact", label: "Compact" },
              ]}
            />
          </SettingRow>
          <SettingRow label="Accent" description="One of the predefined accents." origin="browser">
            <ChoiceGroup
              label="Accent"
              value={theme.preferences.accent}
              onChange={theme.setAccent}
              options={[
                { value: "ochre", label: "Ochre" },
                { value: "slate", label: "Slate" },
                { value: "teal", label: "Teal" },
              ]}
            />
          </SettingRow>
        </Section>
        <Section
          id="settings-session"
          title="Session"
          description="This browser's sign-in to the local Control Plane."
          action={
            <Button
              variant="outline"
              size="sm"
              className="h-11 rounded-md tracking-normal normal-case"
              disabled={signingOut}
              onClick={onSignOut}
            >
              {signingOut ? "Signing out…" : "Sign out"}
            </Button>
          }
        >
          <SettingRow
            label="Signed in until"
            description="A session lasts eight hours and ends with the OCG process."
            origin="runtime"
          >
            <span className={value}>
              {sessionExpiresAtUs === null ? "—" : new Date(sessionExpiresAtUs / 1000).toLocaleString()}
            </span>
          </SettingRow>
        </Section>
        <Section
          id="settings-control-plane"
          title="Control Plane"
          description="What the host establishes about itself, read every few seconds."
        >
          <SettingRow label="Host" description="Whether the OCG host answers." origin="runtime">
            <span className={value}>{health ? health.host : "unreachable"}</span>
          </SettingRow>
          <SettingRow label="Database" description="The state of the persisted store." origin="runtime">
            <span className={value}>{health ? health.database.status : "unknown"}</span>
          </SettingRow>
          <SettingRow label="Reads" description="Whether Job records can be read." origin="runtime">
            <span className={value}>{health ? (health.database.readable ? "available" : "unavailable") : "unknown"}</span>
          </SettingRow>
          <SettingRow label="Commands" description="Whether new Jobs are accepted." origin="runtime">
            <span className={value}>
              {health ? (health.database.accepting_commands ? "accepting" : "not accepting") : "unknown"}
            </span>
          </SettingRow>
        </Section>
        <Section
          id="settings-unavailable"
          title="Not available in this build"
          description="0.4.5 settings that have no Control Plane contract in 0.5, so they are not shown as controls."
        >
          <ul className="flex list-disc flex-col gap-1.5 py-3 pl-5 text-[13px] leading-5 text-muted-foreground">
            <li>Providers, models and Profiles: no route lists or edits them; Jobs name a Profile and an Executor by identity.</li>
            <li>Workspace and startup preferences: no route owns them.</li>
            <li>Interface language: the 0.4.5 dictionaries describe the legacy surfaces and were not migrated.</li>
          </ul>
        </Section>
      </div>
    </PageSurface>
  );
}
