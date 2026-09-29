"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/** Label-over-value definition list used by every detail sheet. */
export function KeyValueList({ children, className }: { children: ReactNode; className?: string }) {
  return <dl className={cn("divide-y divide-border/70 rounded-md border border-border", className)}>{children}</dl>;
}

export function KeyValue({
  label,
  children,
  mono = false,
  className,
}: {
  label: ReactNode;
  children: ReactNode;
  mono?: boolean;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "grid grid-cols-[minmax(110px,0.45fr)_minmax(0,1fr)] gap-3 px-2.5 py-2 text-[11px]",
        className,
      )}
    >
      <dt className="min-w-0 break-words text-muted-foreground">{label}</dt>
      <dd className={cn("min-w-0 break-words", mono && "font-mono text-[10px]")}>{children}</dd>
    </div>
  );
}
