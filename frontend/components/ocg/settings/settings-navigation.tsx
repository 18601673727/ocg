import {
  Activity,
  Bot,
  Cpu,
  Folder,
  Layers,
  type LucideIcon,
  Palette,
  UserRound,
  Waypoints,
} from "@lucide/solid";
import { Select, SelectItem } from "@/components/ui/select";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";
import { SELECTED_ROW } from "../primitives/selection";
import {
  isSettingsSection,
  SETTINGS_SECTIONS,
  type SettingsSection,
  settingsPath,
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

export function SettingsNavigation(props: {
  section: SettingsSection;
  onNavigate(section: SettingsSection): void;
}) {
  const i18n = useI18n();
  return (
    <nav
      aria-label={i18n.t("settings.nav.label")}
      class="sticky top-0 z-10 self-start bg-background pb-4 lg:top-6 lg:pb-0 lg:pr-5"
    >
      <label class="grid gap-2 text-xs font-medium text-muted-foreground lg:hidden">
        {i18n.t("settings.nav.label")}
        <Select
          value={props.section}
          items={SETTINGS_SECTIONS.map((item) => ({ value: item.id, label: i18n.t(item.label) }))}
          onValueChange={(next) => {
            if (next !== null && isSettingsSection(next)) props.onNavigate(next);
          }}
          class="min-h-11 w-full rounded-lg border-border bg-background px-3 text-sm text-foreground"
        >
          {SETTINGS_SECTIONS.map((item) => (
            <SelectItem value={item.id}>{i18n.t(item.label)}</SelectItem>
          ))}
        </Select>
      </label>
      <div class="hidden space-y-6 lg:block">
        {(["profiles", "workspace", "preferences"] as const).map((group) => (
          <div>
            <p class="mb-2 px-3 text-[11px] font-medium tracking-wider text-muted-foreground uppercase">
              {i18n.t(`settings.nav.${group}`)}
            </p>
            <ul class="space-y-1">
              {SETTINGS_SECTIONS.filter((item) => item.group === group).map((item) => {
                const Icon = icons[item.id];
                return (
                  <li>
                    <a
                      href={settingsPath(item.id)}
                      aria-current={props.section === item.id ? "page" : undefined}
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
                        props.onNavigate(item.id);
                      }}
                      class={cn(
                        "flex min-h-11 items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring",
                        props.section === item.id
                          ? cn(SELECTED_ROW, "font-medium")
                          : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                      )}
                    >
                      <Icon aria-hidden="true" class="size-4 shrink-0" />
                      {i18n.t(item.label)}
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
