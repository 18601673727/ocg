import { Archive, ChevronsUpDown, LogOut, SlidersHorizontal, User } from "@lucide/solid";
import { createMemo } from "solid-js";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";

export type AccountMenuProps = {
  email?: string | null;
  onArchiveClick?: () => void;
  onLogout?: () => void;
  onSettingsClick?: () => void;
  variant?: "expanded" | "rail";
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
};

export function AccountMenu(props: AccountMenuProps) {
  const i18n = useI18n();
  const label = createMemo(() => props.email ?? i18n.t("sidebar.localAccount"));
  const rail = createMemo(
    () => (props.variant === undefined ? "expanded" : props.variant) === "rail",
  );

  return (
    <DropdownMenu open={props.open} onOpenChange={props.onOpenChange}>
      <DropdownMenuTrigger asChild>
        {rail() ? (
          <button
            type="button"
            aria-label={i18n.t("sidebar.account")}
            title={label()}
            class="flex size-9 items-center justify-center rounded-md border border-transparent text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          >
            <User class="size-4" aria-hidden="true" />
          </button>
        ) : (
          <button
            type="button"
            aria-label={i18n.t("sidebar.account")}
            title={label()}
            class="flex w-full min-w-0 items-center gap-3 rounded-lg px-2 py-1.5 text-left outline-none transition-colors hover:bg-sidebar-accent focus-visible:ring-2 focus-visible:ring-ring/50"
          >
            <span class="grid size-8 shrink-0 place-items-center rounded-lg border border-sidebar-border bg-card">
              <User class="size-4 text-muted-foreground" aria-hidden="true" />
            </span>
            <span
              class={cn(
                "min-w-0 flex-1 truncate font-medium text-sidebar-foreground",
                TYPOGRAPHY_SCALE.body_sm,
              )}
            >
              {label()}
            </span>
            <ChevronsUpDown class="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
          </button>
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent
        side={rail() ? "right" : "top"}
        align={rail() ? "end" : "start"}
        sideOffset={rail() ? 8 : 4}
        class={rail() ? "w-64" : "w-(--overlay-anchor-width)"}
      >
        {props.onLogout && (
          <DropdownMenuItem class="gap-2" onSelect={props.onLogout}>
            <LogOut class="size-3.5 shrink-0" aria-hidden="true" />
            {i18n.t("sidebar.logout")}
          </DropdownMenuItem>
        )}
        {props.onArchiveClick && (
          <DropdownMenuItem class="gap-2" aria-haspopup="dialog" onSelect={props.onArchiveClick}>
            <Archive class="size-3.5 shrink-0" aria-hidden="true" />
            {i18n.t("sidebar.archivedChats")}
          </DropdownMenuItem>
        )}
        {props.onSettingsClick && (
          <DropdownMenuItem class="gap-2" onSelect={props.onSettingsClick}>
            <SlidersHorizontal class="size-3.5 shrink-0" aria-hidden="true" />
            {i18n.t("sidebar.openSettings")}
          </DropdownMenuItem>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
