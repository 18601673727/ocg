"use client";

import { useMemo } from "react";
import { ArrowRight, RotateCcw, Settings2, ShieldCheck } from "lucide-react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Pill } from "@/components/ocg/primitives";
import { cn } from "@/lib/utils";
import { useTheme } from "../appearance/theme-provider";
import { ProfilePanel } from "../profile/profile-panel";
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
    <Pill tone={backend ? "sky" : browserOnly ? "violet" : "slate"} className="text-[9px] normal-case">
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
    <div className="mt-2 flex flex-wrap gap-1" role="group" aria-label={label}>
      {options.map((option) => (
        <button key={option.value} type="button" aria-pressed={value === option.value} onClick={() => onChange(option.value)} className={cn("rounded-md border px-2 py-1 text-[10px] capitalize transition-colors", value === option.value ? "border-foreground/30 bg-muted font-medium text-foreground" : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground")}>
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
      <div className="min-w-0"><div className="flex flex-wrap items-center gap-2"><h3 className="text-[12px] font-medium">{t("settings.language.label")}</h3><Pill tone="violet" className="text-[9px] normal-case">{t("common.browserOnly")}</Pill></div><p className="mt-0.5 max-w-2xl text-[10px] leading-4 text-muted-foreground">{t("settings.language.desc")}</p></div>
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
      <div className="min-w-0"><div className="flex flex-wrap items-center gap-2"><h3 className="text-[12px] font-medium">{localized.label}</h3><OwnershipBadge setting={localized} />{localized.readOnly && <span className="text-[9px] uppercase tracking-wider text-muted-foreground">{t("common.readOnly")}</span>}</div><p className="mt-0.5 max-w-2xl text-[10px] leading-4 text-muted-foreground">{localized.description}</p></div>
      <div className="min-w-0 shrink-0 sm:max-w-[48%] sm:text-right">
        {isAppearance ? <div className="sm:flex sm:flex-col sm:items-end"><AppearanceSetting setting={localized} /></div> : localized.action === "reconfigure" ? <Button size="xs" variant="outline" onClick={onReconfigure}>{localized.value}<ArrowRight className="size-3" /></Button> : <span className="break-words text-[11px] font-medium text-foreground">{String(localized.value)}</span>}
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
      <div className="flex items-start justify-between gap-3 border-b border-border pb-2.5"><div className="min-w-0"><h2 id={`settings-${section.id}`} className="text-[12px] font-semibold tracking-tight">{title}</h2><p className="mt-0.5 text-[10px] leading-4 text-muted-foreground">{description}</p></div>{onReset && <Button variant="ghost" size="xs" onClick={onReset} title={t("settings.resetAppearance")}><RotateCcw className="size-3" />{t("common.reset")}</Button>}</div>
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

  return (
    <div className="min-h-0 flex-1 overflow-y-auto bg-background">
      <div className="mx-auto w-full max-w-5xl px-3 py-4 sm:px-5 sm:py-6">
        <header className="flex flex-wrap items-start justify-between gap-3"><div className="min-w-0"><div className="flex items-center gap-2"><span className="rounded border border-border bg-muted/50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{t("settings.workspace")}</span><span className="text-[10px] text-muted-foreground">{t("settings.settings")}</span></div><h1 className="mt-1.5 flex items-center gap-2 text-[18px] font-semibold tracking-tight"><Settings2 className="size-4 text-muted-foreground" />{t("settings.title")}</h1><p className="mt-1 max-w-2xl text-[11px] leading-5 text-muted-foreground">{t(canonical ? "settings.realSubtitle" : "settings.subtitle")}</p></div><div className="rounded-md border border-border bg-muted/20 px-2.5 py-2 text-right text-[10px] text-muted-foreground"><p className="font-medium text-foreground">{t("settings.themeActive", { theme: t(theme.resolvedTheme === "dark" ? "settings.theme.dark" : "settings.theme.light") })}</p><p className="mt-0.5">{theme.hydrated ? t("settings.preferencesActive") : t("settings.preferencesLoading")}</p></div></header>
        {!canonical && <div className="mt-4 rounded-md border border-violet-500/25 bg-violet-500/5 px-3 py-2.5 text-[10px] leading-4 text-muted-foreground"><strong className="font-semibold text-foreground">{t("settings.boundaryTitle")}</strong> {t("settings.boundaryBody")}</div>}
        {canonical ? <section className="mt-4 rounded-lg border border-border p-4">
          <h2 className="text-sm font-semibold">{t("settings.providerModels")}</h2>
          <p className="mt-1 text-xs text-muted-foreground">{t("settings.providerModelsDesc")}</p>
          <Button className="mt-3" size="sm" variant="outline" onClick={() => router.push(RECONFIGURE_PATH)}>{t("settings.configure")}<ArrowRight className="size-3" /></Button>
        </section> : <div className="mt-4"><ProfilePanel /></div>}
        <div className="mt-4 grid gap-3 lg:grid-cols-2">
          {sections.map((section) => <SettingsSection key={section.id} section={section} onReconfigure={() => router.push(RECONFIGURE_PATH)} onReset={section.id === "appearance" ? theme.resetAppearance : undefined} />)}
        </div>
        {canonical && <details className="mt-4 rounded-lg border border-border p-4">
          <summary className="cursor-pointer text-sm font-medium">{t("settings.section.advanced")}</summary>
          <div className="mt-3 space-y-3">
            <Button size="sm" variant="outline" onClick={() => {
              const url = new URL(window.location.href); url.pathname = "/"; url.searchParams.set("view", "canonical");
              window.history.pushState(null, "", url.pathname + url.search);
            }}>{t("nav.canonical")}<ArrowRight className="size-3" /></Button>
            <ProfilePanel />
          </div>
        </details>}
        {appearanceSection && <p className="mt-3 text-[10px] text-muted-foreground">{t("settings.appearanceNote")}</p>}
      </div>
    </div>
  );
}
