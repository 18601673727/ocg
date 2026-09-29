"use client";

import { cn } from "@/lib/utils";

export type TabItem<Id extends string> = { id: Id; label: string; /** Live count for the tab, e.g. open approvals. */ count?: number };

/**
 * Segmented tab bar.
 *
 * Panels that switch surfaces (inspector, ledger, control centre, attention)
 * are the same control. Wiring `role`, `aria-selected` and `aria-controls` in
 * one place is what keeps them keyboard- and screen-reader-equivalent. The
 * caller supplies the grid columns, e.g. `className="grid-cols-4"`.
 */
export function SegmentedTabs<Id extends string>({
  tabs,
  value,
  onSelect,
  ariaLabel,
  panelIdBase,
  panelId,
  className,
  size = "md",
}: {
  tabs: readonly TabItem<Id>[];
  value: Id;
  onSelect: (id: Id) => void;
  ariaLabel: string;
  /** When the bar drives a labelled panel per tab, its id is `${panelIdBase}-${tab}`. */
  panelIdBase?: string;
  /** When every tab drives the same panel, that panel's id. */
  panelId?: string;
  className?: string;
  size?: "sm" | "md";
}) {
  return (
    <div
      role="tablist"
      aria-label={ariaLabel}
      className={cn("grid gap-1 rounded-md bg-muted/50 p-0.5", className)}
    >
      {tabs.map((tab) => {
        const selected = tab.id === value;
        return (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={selected}
            aria-controls={panelId ?? (panelIdBase ? `${panelIdBase}-${tab.id}` : undefined)}
            onClick={() => onSelect(tab.id)}
            className={cn(
              "flex min-w-0 items-center justify-center gap-1.5 truncate rounded px-2 font-medium transition-colors focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring",
              size === "sm" ? "py-1 text-[10px]" : "py-1.5 text-[11px]",
              selected ? "bg-background text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground",
            )}
          >
            <span className="min-w-0 truncate">{tab.label}</span>
            {tab.count !== undefined && (
              <span
                className={cn(
                  "shrink-0 rounded px-1 text-[10px] tabular-nums",
                  selected ? "bg-muted text-foreground" : "bg-muted/60 text-muted-foreground",
                )}
              >
                {tab.count}
              </span>
            )}
          </button>
        );
      })}
    </div>
  );
}
