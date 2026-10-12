"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE, GAP_SCALE } from "@/lib/design-tokens";

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
    <div className={cn("mb-1.5 flex min-w-0 items-center", GAP_SCALE.tight, className)}>
      <h3 className={cn("truncate", TYPOGRAPHY_SCALE.caption, "font-semibold tracking-wider text-muted-foreground uppercase")}>
        {children}
      </h3>
      {detail !== undefined && <span className={cn("min-w-0 truncate", TYPOGRAPHY_SCALE.caption, "text-muted-foreground")}>{detail}</span>}
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
      <h2 className={cn("min-w-0 truncate", TYPOGRAPHY_SCALE.body_sm, "font-semibold tracking-wider text-muted-foreground uppercase")}>
        {title}
      </h2>
      {action}
    </div>
  );
}
