"use client";

import { cn } from "@/lib/utils";

/**
 * Horizontal amount bar with the accessible name and value on the track.
 *
 * Budget, health and progress bars were each rebuilt with their own aria story,
 * and most had none. A bar is only useful if it says what it measures.
 */
export function ProgressBar({
  value,
  max = 100,
  ariaLabel,
  label,
  tone = "bg-foreground",
  className,
  trackClassName,
}: {
  value: number;
  max?: number;
  ariaLabel: string;
  /** Optional caption above the bar; shows the percentage when given. */
  label?: string;
  /** Fill colour, e.g. `bg-emerald-500`. */
  tone?: string;
  className?: string;
  trackClassName?: string;
}) {
  const percent = max > 0 ? Math.min(100, Math.max(0, (value / max) * 100)) : 0;
  return (
    <div className={cn("min-w-0", className)}>
      {label && (
        <div className="mb-1 flex items-center justify-between gap-2 text-[10px] text-muted-foreground">
          <span className="truncate">{label}</span>
          <span className="shrink-0 tabular-nums">{Math.round(percent)}%</span>
        </div>
      )}
      <div
        className={cn("h-1.5 overflow-hidden rounded-full bg-muted", trackClassName)}
        role="progressbar"
        aria-label={ariaLabel}
        aria-valuemin={0}
        aria-valuemax={max}
        aria-valuenow={value}
      >
        <div
          className={cn("h-full rounded-full transition-[width] duration-300", tone)}
          style={{ width: `${percent}%` }}
        />
      </div>
    </div>
  );
}
