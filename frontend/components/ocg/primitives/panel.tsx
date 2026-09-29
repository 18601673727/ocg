"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * Bordered block that owns one idea inside a surface.
 *
 * `bg-card` keeps a panel legible on top of the muted workspace background;
 * `muted` is for panels nested inside another panel. Give it a `title` and it
 * renders the standard title row, so a panel is one component rather than a
 * div plus a hand-rolled heading.
 */
export function Panel({
  children,
  title,
  detail,
  action,
  className,
  contentClassName,
  tone = "card",
  ariaLabel,
}: {
  children: ReactNode;
  title?: ReactNode;
  /** Definition or count shown next to the title. */
  detail?: ReactNode;
  /** Trailing control on the title row, e.g. a button. */
  action?: ReactNode;
  className?: string;
  /** Classes for the block under the title row, e.g. the gap of its content. */
  contentClassName?: string;
  tone?: "card" | "muted";
  ariaLabel?: string;
}) {
  return (
    <section
      aria-label={ariaLabel}
      className={cn(
        "min-w-0 rounded-md border border-border p-2.5",
        tone === "card" ? "bg-card" : "bg-muted/20",
        className,
      )}
    >
      {title !== undefined && (
        <PanelHeader title={title} detail={detail} className="mb-2">
          {action}
        </PanelHeader>
      )}
      {/* Only wrap the body when it needs its own box, so an existing flex
          layout on the panel itself keeps addressing its real children. */}
      {contentClassName ? <div className={contentClassName}>{children}</div> : children}
    </section>
  );}

/** Title row of a panel: title, optional definition, then trailing content. */
export function PanelHeader({
  title,
  detail,
  children,
  className,
}: {
  title: ReactNode;
  detail?: ReactNode;
  children?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1", className)}>
      <h3 className="min-w-0 flex-1 truncate text-[11px] font-semibold">{title}</h3>
      {detail !== undefined && <span className="min-w-0 truncate text-[10px] text-muted-foreground">{detail}</span>}
      {children}
    </div>
  );
}
