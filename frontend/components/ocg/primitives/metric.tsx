"use client";

import type { ComponentType } from "react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE, BORDER_RADIUS, BORDER_OPACITY } from "@/lib/design-tokens";

/**
 * Compact label-over-value stat tile.
 *
 * Every surface that shows a number (ledger, control centre, inspector,
 * onboarding) uses this so the tiles stay interchangeable when a panel moves
 * from one surface to another.
 */
export function Metric({
  label,
  value,
  detail,
  title,
  icon: Icon,
  className,
}: {
  label: string;
  value: string;
  /** Formula or unit note shown under the value. */
  detail?: string;
  /** Tooltip, normally the full definition of a derived metric. */
  title?: string;
  icon?: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
  className?: string;
}) {
  return (
    <div className={cn("min-w-0", BORDER_RADIUS.interactive, "border", BORDER_OPACITY.primary, "bg-background px-2 py-1.5", className)} title={title}>
      <div className={cn("flex items-center gap-1", TYPOGRAPHY_SCALE.caption, "text-muted-foreground")}>
        {Icon && <Icon className="size-3 shrink-0" aria-hidden />}
        <span className="truncate">{label}</span>
      </div>
      <p className={cn("mt-0.5 truncate", TYPOGRAPHY_SCALE.body_sm, "font-semibold tabular-nums")}>{value}</p>
      {detail && <p className={cn("truncate", TYPOGRAPHY_SCALE.caption, "text-muted-foreground")}>{detail}</p>}
    </div>
  );
}
