"use client";

import { useMemo } from "react";
import { ArrowRight, RotateCcw, Settings2, ShieldCheck } from "lucide-react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useTheme } from "../appearance/theme-provider";
import { ProfilePanel } from "../profile/profile-panel";
import type { AppearancePreferences, AccentPreference, DensityPreference, ThemePreference } from "../appearance/theme-domain";
import type { RuntimeSnapshot } from "../runtime/runtime-types";
import {
  createSettingsState,
  isBackendOwned,
  RECONFIGURE_PATH,
  selectSettingsSections,
  type NormalizedSetting,
  type SettingsSection,
} from "./domain";

function OwnershipBadge({ setting }: { setting: NormalizedSetting }) {
  const backend = isBackendOwned(setting);
  return (
    <span className={cn("inline-flex shrink-0 items-center gap-1 rounded-full border px-1.5 py-0.5 text-[9px] font-medium", backend ? "border-sky-500/30 bg-sky-500/10 text-sky-700 dark:text-sky-300" : setting.ownership === "frontend-only" ? "border-violet-500/30 bg-violet-500/10 text-violet-700 dark:text-violet-300" : "border-border bg-muted/50 text-muted-foreground")}>
      {backend ? <ShieldCheck className="size-2.5" /> : null}
      {backend ? "runtime-owned" : setting.ownership === "frontend-only" ? "browser-only" : "effective"}
    </span>
  );
}

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
    <div className="mt-2 flex flex-wrap gap-1" role="group" aria-label={label}>
      {options.map((option) => (
        <button key={option.value} type="button" aria-pressed={value === option.value} onClick={() => onChange(option.value)} className={cn("rounded-md border px-2 py-1 text-[10px] capitalize transition-colors", value === option.value ? "border-foreground/30 bg-muted font-medium text-foreground" : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground")}>
          {option.label}
        </button>
      ))}
    </div>
  );
}

function AppearanceSetting({ setting, preferences, setTheme, setDensity, setAccent }: { setting: NormalizedSetting; preferences: AppearancePreferences; setTheme: (value: ThemePreference) => void; setDensity: (value: DensityPreference) => void; setAccent: (value: AccentPreference) => void }) {
  if (setting.id === "appearance.theme") return <ChoiceGroup label="Theme" value={preferences.theme} onChange={setTheme} options={[{ value: "system", label: "System" }, { value: "light", label: "Light" }, { value: "dark", label: "Dark" }]} />;
  if (setting.id === "appearance.density") return <ChoiceGroup label="Density" value={preferences.density} onChange={setDensity} options={[{ value: "comfortable", label: "Comfortable" }, { value: "compact", label: "Compact" }]} />;
  return <ChoiceGroup label="Accent" value={preferences.accent} onChange={setAccent} options={[{ value: "ochre", label: "Ochre" }, { value: "slate", label: "Slate" }, { value: "teal", label: "Teal" }]} />;
}

function SettingRow({ setting, appearance, onReconfigure }: { setting: NormalizedSetting; appearance: AppearancePreferences; onReconfigure: () => void }) {
  const { setTheme, setDensity, setAccent } = useTheme();
  const isAppearance = setting.id.startsWith("appearance.");
  return (
    <div className="flex min-w-0 flex-col gap-2 border-b border-border/70 py-3 last:border-b-0 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
      <div className="min-w-0"><div className="flex flex-wrap items-center gap-2"><h3 className="text-[12px] font-medium">{setting.label}</h3><OwnershipBadge setting={setting} />{setting.readOnly && <span className="text-[9px] uppercase tracking-wider text-muted-foreground">read-only</span>}</div><p className="mt-0.5 max-w-2xl text-[10px] leading-4 text-muted-foreground">{setting.description}</p></div>
      <div className="min-w-0 shrink-0 sm:max-w-[48%] sm:text-right">
        {isAppearance ? <div className="sm:flex sm:flex-col sm:items-end"><span className="text-[11px] font-medium capitalize">{String(setting.value)}</span><AppearanceSetting setting={setting} preferences={appearance} setTheme={setTheme} setDensity={setDensity} setAccent={setAccent} /></div> : setting.action === "reconfigure" ? <Button size="xs" variant="outline" onClick={onReconfigure}>{setting.value}<ArrowRight className="size-3" /></Button> : <span className="break-words text-[11px] font-medium text-foreground">{String(setting.value)}</span>}
      </div>
    </div>
  );
}

function SettingsSection({ section, appearance, onReconfigure, onReset }: { section: SettingsSection; appearance: AppearancePreferences; onReconfigure: () => void; onReset?: () => void }) {
  return (
    <section aria-labelledby={`settings-${section.id}`} className="rounded-lg border border-border bg-background px-3 py-2.5 sm:px-4">
      <div className="flex items-start justify-between gap-3 border-b border-border pb-2.5"><div className="min-w-0"><h2 id={`settings-${section.id}`} className="text-[12px] font-semibold tracking-tight">{section.title}</h2><p className="mt-0.5 text-[10px] leading-4 text-muted-foreground">{section.description}</p></div>{onReset && <Button variant="ghost" size="xs" onClick={onReset} title="Reset appearance preferences"><RotateCcw className="size-3" />Reset</Button>}</div>
      <div>{section.items.map((setting) => <SettingRow key={setting.id} setting={setting} appearance={appearance} onReconfigure={onReconfigure} />)}</div>
    </section>
  );
}

export function SettingsSurface({ snapshot }: { snapshot: RuntimeSnapshot }) {
  const router = useRouter();
  const theme = useTheme();
  const state = useMemo(() => createSettingsState(snapshot, theme.preferences), [snapshot, theme.preferences]);
  const sections = selectSettingsSections(state);
  const appearanceSection = sections.find((section) => section.id === "appearance");

  return (
    <div className="min-h-0 flex-1 overflow-y-auto bg-background">
      <div className="mx-auto w-full max-w-5xl px-3 py-4 sm:px-5 sm:py-6">
        <header className="flex flex-wrap items-start justify-between gap-3"><div className="min-w-0"><div className="flex items-center gap-2"><span className="rounded border border-border bg-muted/50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Workspace</span><span className="text-[10px] text-muted-foreground">settings</span></div><h1 className="mt-1.5 flex items-center gap-2 text-[18px] font-semibold tracking-tight"><Settings2 className="size-4 text-muted-foreground" />Settings</h1><p className="mt-1 max-w-2xl text-[11px] leading-5 text-muted-foreground">Review effective runtime facts and control browser-only appearance without pretending to write backend configuration.</p></div><div className="rounded-md border border-border bg-muted/20 px-2.5 py-2 text-right text-[10px] text-muted-foreground"><p className="font-medium text-foreground">{theme.resolvedTheme} theme</p><p className="mt-0.5">{theme.hydrated ? "preferences active" : "loading preferences"}</p></div></header>
        <div className="mt-4 rounded-md border border-violet-500/25 bg-violet-500/5 px-3 py-2.5 text-[10px] leading-4 text-muted-foreground"><strong className="font-semibold text-foreground">Configuration boundary.</strong> The OCG Profile below writes the backend-owned project YAML. Runtime and diagnostic facts remain projections; Appearance alone is stored in the browser.</div>
        <div className="mt-4"><ProfilePanel /></div>
        <div className="mt-4 grid gap-3 lg:grid-cols-2">
          {sections.map((section) => <SettingsSection key={section.id} section={section} appearance={theme.preferences} onReconfigure={() => router.push(RECONFIGURE_PATH)} onReset={section.id === "appearance" ? theme.resetAppearance : undefined} />)}
        </div>
        {appearanceSection && <p className="mt-3 text-[10px] text-muted-foreground">Appearance changes apply to the whole shell, including login and onboarding, before any backend connection is involved.</p>}
      </div>
    </div>
  );
}
