"use client";

import {
  Activity,
  Bot,
  Cpu,
  Folder,
  Layers,
  Palette,
  UserRound,
  Waypoints,
  type LucideIcon,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useI18n } from "../i18n";
import { SELECTED_ROW } from "../primitives/selection";
import {
  isSettingsSection,
  SETTINGS_SECTIONS,
  settingsPath,
  type SettingsSection,
} from "./settings-sections";

const icons: Record<SettingsSection, LucideIcon> = {
  models: Cpu,
  agents: Bot,
  decisions: Waypoints,
  projects: Folder,
  execution: Layers,
  appearance: Palette,
  session: UserRound,
  system: Activity,
};

export function SettingsNavigation({
  section,
  onNavigate,
}: {
  section: SettingsSection;
  onNavigate(section: SettingsSection): void;
}) {
  const { t } = useI18n();
  return (
    <nav
      aria-label={t("settings.nav.label")}
      className="sticky top-0 z-10 self-start bg-background pb-4 lg:top-6 lg:pb-0 lg:pr-5"
    >
      <label className="grid gap-2 text-xs font-medium text-muted-foreground lg:hidden">
        {t("settings.nav.label")}
        <Select
          value={section}
          items={SETTINGS_SECTIONS.map((item) => ({ value: item.id, label: t(item.label) }))}
          onValueChange={(next) => {
            if (next !== null && isSettingsSection(next)) onNavigate(next);
          }}
        >
          <SelectTrigger className="min-h-11 w-full rounded-lg border-border bg-background px-3 text-sm text-foreground">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {SETTINGS_SECTIONS.map((item) => (
              <SelectItem key={item.id} value={item.id}>
                {t(item.label)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </label>
      <div className="hidden space-y-6 lg:block">
        {(["profiles", "workspace", "preferences"] as const).map((group) => (
          <div key={group}>
            <p className="mb-2 px-3 text-[11px] font-medium tracking-wider text-muted-foreground uppercase">
              {t(`settings.nav.${group}`)}
            </p>
            <ul className="space-y-1">
              {SETTINGS_SECTIONS.filter((item) => item.group === group).map((item) => {
                const Icon = icons[item.id];
                return (
                  <li key={item.id}>
                    <a
                      href={settingsPath(item.id)}
                      aria-current={section === item.id ? "page" : undefined}
                      onClick={(event) => {
                        if (
                          event.button !== 0 ||
                          event.metaKey ||
                          event.ctrlKey ||
                          event.shiftKey ||
                          event.altKey
                        )
                          return;
                        event.preventDefault();
                        onNavigate(item.id);
                      }}
                      className={cn(
                        "flex min-h-11 items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring",
                        section === item.id
                          ? cn(SELECTED_ROW, "font-medium")
                          : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                      )}
                    >
                      <Icon aria-hidden className="size-4 shrink-0" />
                      {t(item.label)}
                    </a>
                  </li>
                );
              })}
            </ul>
          </div>
        ))}
      </div>
    </nav>
  );
}
