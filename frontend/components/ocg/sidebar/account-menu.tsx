"use client";

/**
 * The one account menu, shared by the expanded sidebar footer and the collapsed
 * rail so both offer the same actions in the same order:
 *
 * 1. Log out, where signing out is a real operation (remote sessions only).
 * 2. Open settings, where Settings is offered.
 *
 * The expanded trigger names who is signed in — the signed Access email, or the
 * local-operator label where there is no signed identity — and the popup omits
 * a duplicate disabled identity row.
 *
 * Signing out precedes Settings because it ends the session this menu names,
 * while Settings stays a workspace destination.
 *
 * The rail form anchors the menu to the viewport: the 64px rail clips its own
 * overflow, so a menu placed inside it would be cut off. The expanded form
 * stays anchored to the footer it belongs to.
 */

import { LogOut, SlidersHorizontal, User } from "lucide-react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useI18n } from "../i18n";

export type AccountMenuProps = {
  /** The signed Access email; null/undefined where no signed identity exists. */
  email?: string | null;
  /** Logging out. Omitted where the installation has no logout to run. */
  onLogout?: () => void;
  /** Opens Settings. Omitted where Settings is not offered. */
  onSettingsClick?: () => void;
  /** `expanded` anchors to the sidebar footer; `rail` to the collapsed rail. */
  variant?: "expanded" | "rail";
  /** Controlled open state, mirroring the menu primitive. */
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
};

export function AccountMenu({
  email,
  onLogout,
  onSettingsClick,
  variant = "expanded",
  open,
  onOpenChange,
}: AccountMenuProps) {
  const { t } = useI18n();
  const label = email ?? t("sidebar.localAccount");
  const rail = variant === "rail";

  return (
    <DropdownMenu open={open} onOpenChange={onOpenChange}>
      <DropdownMenuTrigger asChild>
        {rail ? (
          <button
            type="button"
            aria-label={t("sidebar.account")}
            title={label}
            className="flex size-9 items-center justify-center rounded-md border border-transparent text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          >
            <User className="size-4" aria-hidden="true" />
          </button>
        ) : (
          <button
            type="button"
            aria-label={t("sidebar.account")}
            title={label}
            className="flex w-full min-w-0 items-center gap-2 rounded-md px-2 py-1.5 text-left outline-none transition-colors hover:bg-muted/60 focus-visible:ring-2 focus-visible:ring-ring/30"
          >
            <User className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
            <span
              className={cn(
                "min-w-0 flex-1 truncate text-muted-foreground",
                TYPOGRAPHY_SCALE.body_sm,
              )}
            >
              {label}
            </span>
          </button>
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="start"
        className={
          rail
            ? "fixed top-auto right-auto bottom-3 left-18 mt-0 w-64"
            : "top-auto right-0 bottom-full left-0 mb-1 mt-0"
        }
      >
        {onLogout && (
          <DropdownMenuItem className="gap-2" onSelect={onLogout}>
            <LogOut className="size-3.5 shrink-0" aria-hidden="true" />
            {t("sidebar.logout")}
          </DropdownMenuItem>
        )}
        {onSettingsClick && (
          <DropdownMenuItem className="gap-2" onSelect={onSettingsClick}>
            <SlidersHorizontal className="size-3.5 shrink-0" aria-hidden="true" />
            {t("sidebar.openSettings")}
          </DropdownMenuItem>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
