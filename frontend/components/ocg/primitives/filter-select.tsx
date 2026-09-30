"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * Labelled native select used by every filter row.
 *
 * The label is always visible and the select is always named, so a filter
 * reads the same in the logs surface and in Job Execution.
 */
export function FilterSelect({
  label,
  value,
  onChange,
  children,
  className,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  children: ReactNode;
  className?: string;
}) {
  return (
    <label className={cn("flex min-w-0 flex-col gap-1 text-[10px] font-medium uppercase tracking-wider text-muted-foreground", className)}>
      {label}
      <select
        aria-label={label}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="h-8 min-w-0 rounded-md border border-border bg-background px-2 text-[11px] font-normal tracking-normal text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30"
      >
        {children}
      </select>
    </label>
  );
}

/** A `select` option list from a value/label table. */
export function FilterOption({ value, label }: { value: string; label: string }) {
  return <option value={value}>{label}</option>;
}
