"use client";

import { useCallback, useMemo, useState } from "react";
import { ArrowRight, RotateCcw, Settings2, ShieldCheck } from "lucide-react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { PageSurface, Pill } from "@/components/ocg/primitives";
import { cn } from "@/lib/utils";
import { useTheme } from "../appearance/theme-provider";
import { ProfilePanel } from "../profile/profile-panel";
import { AddSubscriptionPanel } from "../setup/add-subscription-panel";
import { ProviderSyncPanel } from "../setup/provider-sync-panel";
import type { RuntimeSnapshot } from "../runtime/runtime-types";
import { useI18n, LanguageSwitcher, type I18nKey } from "../i18n";
import {
  createSettingsState,
  isBackendOwned,
  RECONFIGURE_PATH,
  selectSettingsSections,
  type NormalizedSetting,
  type SettingsSection,
  type SettingsSectionId,
} from "./domain";

const SECTION_TITLE_KEY: Record<SettingsSectionId, I18nKey> = {
  general: "settings.section.general",
  runtime: "settings.section.runtime",
  resources: "settings.section.resources",
  access: "settings.section.access",
  appearance: "settings.section.appearance",
  diagnostics: "settings.section.diagnostics",
  advanced: "settings.section.advanced",
};

const SECTION_DESC_KEY: Record<SettingsSectionId, I18nKey> = {
  general: "settings.section.generalDesc",
  runtime: "settings.section.runtimeDesc",
  resources: "settings.section.resourcesDesc",
  access: "settings.section.accessDesc",
  appearance: "settings.section.appearanceDesc",
  diagnostics: "settings.section.diagnosticsDesc",
  advanced: "settings.section.advancedDesc",
};

function OwnershipBadge({ setting }: { setting: NormalizedSetting }) {
  const { t } = useI18n();
  const backend = isBackendOwned(setting);
  const browserOnly = setting.ownership === "frontend-only";
  return (
    <Pill tone={backend ? "sky" : browserOnly ? "violet" : "slate"} className="text-[11px] normal-case">
      {backend ? <ShieldCheck className="size-2.5" /> : null}
      {backend ? t("common.runtimeOwned") : browserOnly ? t("common.browserOnly") : t("common.effective")}
    </Pill>
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
    <div className="flex flex-wrap gap-2" role="group" aria-label={label}>
      {options.map((option) => (
        <button key={option.value} type="button" aria-pressed={value === option.value} onClick={() => onChange(option.value)} className={cn("min-h-11 rounded-md border px-3 py-2 text-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring/30", value === option.value ? "border-primary/50 bg-primary/10 font-medium text-foreground" : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground")}>
          {option.label}
        </button>
      ))}
    </div>
  );
}

function AppearanceSetting({ setting }: { setting: NormalizedSetting }) {
  const { t } = useI18n();
  const { preferences, setTheme, setDensity, setAccent } = useTheme();
  if (setting.id === "appearance.theme") return <ChoiceGroup label={t("settings.theme.label")} value={preferences.theme} onChange={setTheme} options={[{ value: "system", label: t("settings.theme.system") }, { value: "light", label: t("settings.theme.light") }, { value: "dark", label: t("settings.theme.dark") }]} />;
  if (setting.id === "appearance.density") return <ChoiceGroup label={t("settings.density.label")} value={preferences.density} onChange={setDensity} options={[{ value: "comfortable", label: t("settings.density.comfortable") }, { value: "compact", label: t("settings.density.compact") }]} />;
  if (setting.id === "appearance.accent") return <ChoiceGroup label={t("settings.accent.label")} value={preferences.accent} onChange={setAccent} options={[{ value: "ochre", label: t("settings.accent.ochre") }, { value: "slate", label: t("settings.accent.slate") }, { value: "teal", label: t("settings.accent.teal") }]} />;
  return null;
}

function LanguageSetting() {
  const { t } = useI18n();
  return (
    <div className="flex min-w-0 flex-col gap-2 border-b border-border/70 py-3 last:border-b-0 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
      <div className="min-w-0"><div className="flex flex-wrap items-center gap-2"><h3 className="text-sm font-medium">{t("settings.language.label")}</h3><Pill tone="violet" className="text-[11px] normal-case">{t("common.browserOnly")}</Pill></div><p className="mt-1 max-w-2xl text-[13px] leading-5 text-muted-foreground">{t("settings.language.desc")}</p></div>
      <div className="min-w-0 shrink-0 sm:max-w-[48%] sm:text-right">
        <div className="sm:flex sm:flex-col sm:items-end"><LanguageSwitcher /></div>
      </div>
    </div>
  );
}

function localizeSetting(
  setting: NormalizedSetting,
  t: ReturnType<typeof useI18n>["t"],
): NormalizedSetting {
  switch (setting.id) {
    case "general.workspace-name":
      return { ...setting, label: t("settings.general.workspaceName"), description: t("settings.general.workspaceNameDesc") };
    case "general.startup-destination":
      return { ...setting, label: t("settings.general.startup"), description: t("settings.general.startupDesc") };
    case "general.confirmations":
      return { ...setting, label: t("settings.general.confirmations"), description: t("settings.general.confirmationsDesc") };
    case "appearance.theme":
      return { ...setting, label: t("settings.theme.label"), description: t("settings.theme.desc") };
    case "appearance.density":
      return { ...setting, label: t("settings.density.label"), description: t("settings.density.desc") };
    case "appearance.accent":
      return { ...setting, label: t("settings.accent.label"), description: t("settings.accent.desc") };
    default:
      return setting;
  }
}

function SettingRow({ setting, onReconfigure }: { setting: NormalizedSetting; onReconfigure: () => void }) {
  const { t } = useI18n();
  const localized = localizeSetting(setting, t);
  const isAppearance = localized.id.startsWith("appearance.");
  return (
    <div className="flex min-w-0 flex-col gap-2 border-b border-border/70 py-3 last:border-b-0 sm:flex-row sm:items-start sm:justify-between sm:gap-4">
      <div className="min-w-0"><div className="flex flex-wrap items-center gap-2"><h3 className="text-sm font-medium">{localized.label}</h3><OwnershipBadge setting={localized} />{localized.readOnly && <span className="text-[11px] text-muted-foreground">{t("common.readOnly")}</span>}</div><p className="mt-1 max-w-2xl text-[13px] leading-5 text-muted-foreground">{localized.description}</p></div>
      <div className="min-w-0 shrink-0 sm:max-w-[48%] sm:text-right">
        {isAppearance ? <div className="sm:flex sm:flex-col sm:items-end"><AppearanceSetting setting={localized} /></div> : localized.action === "reconfigure" ? <Button size="sm" variant="outline" className="h-auto min-h-11 max-w-full whitespace-normal rounded-md py-2 tracking-normal normal-case" onClick={onReconfigure}>{localized.value}<ArrowRight className="size-3" /></Button> : <span className="[overflow-wrap:anywhere] text-[13px] font-medium text-foreground">{String(localized.value)}</span>}
      </div>
    </div>
  );
}

function SettingsSection({ section, onReconfigure, onReset }: { section: SettingsSection; onReconfigure: () => void; onReset?: () => void }) {
  const { t } = useI18n();
  const title = t(SECTION_TITLE_KEY[section.id]);
  const description = t(SECTION_DESC_KEY[section.id]);
  return (
    <section aria-labelledby={`settings-${section.id}`} className="rounded-lg border border-border bg-background px-3 py-2.5 sm:px-4">
      <div className="flex items-start justify-between gap-3 border-b border-border pb-3"><div className="min-w-0"><h2 id={`settings-${section.id}`} tabIndex={-1} className="scroll-mt-4 text-base font-semibold tracking-tight">{title}</h2><p className="mt-1 text-[13px] leading-5 text-muted-foreground">{description}</p></div>{onReset && <Button variant="ghost" size="sm" className="h-11 rounded-md tracking-normal normal-case" onClick={onReset} title={t("settings.resetAppearance")}><RotateCcw className="size-3" />{t("common.reset")}</Button>}</div>
      <div>{section.items.map((setting) => <SettingRow key={setting.id} setting={setting} onReconfigure={onReconfigure} />)}{section.id === "appearance" && <LanguageSetting />}</div>
    </section>
  );
}

export function SettingsSurface({ snapshot }: { snapshot: RuntimeSnapshot }) {
  const router = useRouter();
  const { t } = useI18n();
  const theme = useTheme();
  const state = useMemo(() => createSettingsState(snapshot, theme.preferences), [snapshot, theme.preferences]);
  const sections = selectSettingsSections(state);
  const canonical = snapshot.authority === "canonical";
  const appearanceSection = sections.find((section) => section.id === "appearance");
  const [providerProfileRefresh, setProviderProfileRefresh] = useState(0);
  const refreshProviderProfile = useCallback(() => {
    setProviderProfileRefresh((current) => current + 1);
  }, []);

  return (
    <PageSurface className="mx-auto w-full max-w-5xl pb-[max(2rem,env(safe-area-inset-bottom))]">
      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="flex items-center gap-2 text-2xl font-semibold tracking-tight"><Settings2 className="size-5 text-muted-foreground" />{t("settings.title")}</h1>
          <p className="mt-2 max-w-xl text-sm leading-6 text-muted-foreground">{t(canonical ? "settings.realSubtitle" : "settings.subtitle")}</p>
        </div>
        <div role="status" className="rounded-lg border border-border bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
          <p className="font-medium text-foreground">{t("settings.themeActive", { theme: t(theme.resolvedTheme === "dark" ? "settings.theme.dark" : "settings.theme.light") })}</p>
          <p className="mt-1">{theme.hydrated ? t("settings.autoSaved") : t("settings.preferencesLoading")}</p>
        </div>
      </header>
      <div className="mt-6 grid min-w-0 gap-4 lg:grid-cols-[160px_minmax(0,1fr)] lg:gap-6">
        <nav aria-label={t("settings.navigation")} className="flex min-w-0 gap-1 overflow-x-auto rounded-lg border border-border p-1 lg:sticky lg:top-4 lg:flex-col lg:self-start">
          <a href="#settings-providers" className="flex min-h-11 shrink-0 items-center rounded-md px-3 text-sm text-muted-foreground outline-none hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/30">{t("settings.providerModels")}</a>
          {sections.map(section => <a key={section.id} href={`#settings-${section.id}`} className="flex min-h-11 shrink-0 items-center rounded-md px-3 text-sm text-muted-foreground outline-none hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/30">{t(SECTION_TITLE_KEY[section.id])}</a>)}
          {canonical && <a href="#settings-advanced" className="flex min-h-11 shrink-0 items-center rounded-md px-3 text-sm text-muted-foreground outline-none hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/30">{t("settings.section.advanced")}</a>}
        </nav>
        <div className="min-w-0 space-y-4">
          {!canonical && <div className="rounded-lg border border-violet-500/25 bg-violet-500/5 px-4 py-3 text-[13px] leading-5 text-muted-foreground"><strong className="font-semibold text-foreground">{t("settings.boundaryTitle")}</strong> {t("settings.boundaryBody")}</div>}
          <section aria-labelledby="settings-providers">
            <h2 id="settings-providers" tabIndex={-1} className="mb-3 scroll-mt-4 text-base font-semibold">{t("settings.providerModels")}</h2>
            {canonical ? <div className="space-y-4">
              <AddSubscriptionPanel onSaved={refreshProviderProfile} />
              <ProviderSyncPanel profileRefresh={providerProfileRefresh} />
              <div className="rounded-lg border border-border p-4">
                <p className="text-sm leading-6 text-muted-foreground">{t("settings.providerModelsDesc")}</p>
                <Button className="mt-3 h-auto min-h-11 max-w-full whitespace-normal rounded-md py-2 tracking-normal normal-case" size="sm" onClick={() => router.push(RECONFIGURE_PATH)}>{t("settings.configure")}<ArrowRight className="size-4" /></Button>
              </div>
            </div> : <ProfilePanel />}
          </section>
          {sections.map(section => <SettingsSection key={section.id} section={section} onReconfigure={() => router.push(RECONFIGURE_PATH)} onReset={section.id === "appearance" ? theme.resetAppearance : undefined} />)}
          {appearanceSection && <p className="text-xs leading-5 text-muted-foreground">{t("settings.appearanceNote")}</p>}
          {canonical && <details className="rounded-lg border border-border p-4">
            <summary id="settings-advanced" tabIndex={0} className="min-h-11 cursor-pointer scroll-mt-4 py-2 text-sm font-medium outline-none focus-visible:ring-2 focus-visible:ring-ring/30">{t("settings.section.advanced")}</summary>
            <div className="mt-3 space-y-4">
              <Button size="sm" variant="outline" className="h-11 rounded-md tracking-normal normal-case" onClick={() => {
                const url = new URL(window.location.href);
                url.pathname = "/";
                url.hash = "";
                url.searchParams.set("view", "canonical");
                router.push(url.pathname + url.search);
              }}>{t("nav.canonical")}<ArrowRight className="size-3" /></Button>
              <ProfilePanel />
            </div>
          </details>}
        </div>
      </div>
    </PageSurface>
  );
}
