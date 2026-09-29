"use client";

import type { ComponentType, ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * A surface with nothing to show yet.
 *
 * "No data" is a real product state in OCG, and it is stated rather than
 * implied by a blank panel. `EmptyState` is the inline form used inside a
 * panel; `EmptyPanel` is the centred form used when a whole region is empty.
 */
export function EmptyState({
  children,
  className,
  compact = false,
}: {
  children: ReactNode;
  className?: string;
  compact?: boolean;
}) {
  return (
    <p
      className={cn(
        "rounded-md border border-dashed border-border text-[11px] text-muted-foreground",
        compact ? "px-2 py-1.5" : "px-2.5 py-3",
        className,
      )}
    >
      {children}
    </p>
  );
}

export function EmptyPanel({
  icon: Icon,
  title,
  hint,
  iconClassName,
  className,
}: {
  icon?: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
  title: string;
  hint?: string;
  /** Overrides the icon colour, e.g. `text-emerald-500` for an all-clear state. */
  iconClassName?: string;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center rounded-lg border border-dashed border-border bg-muted/10 px-3 py-8 text-center",
        className,
      )}
    >
      {Icon && <Icon className={cn("mb-2 size-6 text-muted-foreground", iconClassName)} aria-hidden />}
      <p className="text-sm font-medium">{title}</p>
      {hint && <p className="mt-1 max-w-[36ch] text-[12px] text-muted-foreground">{hint}</p>}
    </div>
  );
}
