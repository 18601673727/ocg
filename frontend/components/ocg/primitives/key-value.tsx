"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE, BORDER_RADIUS, BORDER_OPACITY } from "@/lib/design-tokens";

/** Label-over-value definition list used by every detail sheet. */
export function KeyValueList({ children, className }: { children: ReactNode; className?: string }) {
  return <dl className={cn("divide-y", BORDER_OPACITY.secondary, BORDER_RADIUS.interactive, "border border-border", className)}>{children}</dl>;
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
        "grid grid-cols-[minmax(110px,0.45fr)_minmax(0,1fr)] gap-3 px-2.5 py-2", TYPOGRAPHY_SCALE.caption,
        className,
      )}
    >
      <dt className="min-w-0 break-words text-muted-foreground">{label}</dt>
      <dd className={cn("min-w-0 break-words", mono && cn("font-mono", TYPOGRAPHY_SCALE.caption))}>{children}</dd>
    </div>
  );
}
