"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/** Small uppercase label that opens a block inside a panel. */
export function SectionTitle({
  children,
  detail,
  className,
}: {
  children: ReactNode;
  detail?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("mb-1.5 flex min-w-0 items-center gap-1.5", className)}>
      <h3 className="truncate text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">
        {children}
      </h3>
      {detail !== undefined && <span className="min-w-0 truncate text-[10px] text-muted-foreground">{detail}</span>}
    </div>
  );
}

/** Section heading with an optional trailing action, e.g. "View all". */
export function SectionHeading({
  title,
  action,
  className,
}: {
  title: string;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("mb-2 flex items-center justify-between gap-2", className)}>
      <h2 className="min-w-0 truncate text-[13px] font-semibold tracking-wider text-muted-foreground uppercase">
        {title}
      </h2>
      {action}
    </div>
  );
}
