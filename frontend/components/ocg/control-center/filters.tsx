"use client";

import type { SelectHTMLAttributes } from "react";
import { cn } from "@/lib/utils";

/**
 * A filter select in a Control Centre list.
 *
 * Native selects keep the platform keyboard and mobile-wheel behaviour a custom
 * listbox would have to re-implement, so the filters use them. The size, border
 * and focus ring live here because the filter rows of the three views would
 * otherwise each restate them.
 */
export function SelectFilter({ className, children, ...props }: SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select
      {...props}
      className={cn(
        // `cn` concatenates classes rather than resolving conflicts, so a caller
        // that needs a different width should pass a whole class list, not an
        // override; the width below is the default the filters use.
        "h-7 min-w-0 max-w-[10rem] truncate rounded border border-border bg-background px-1.5 text-[11px] outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30",
        className,
      )}
    >
      {children}
    </select>
  );
}
