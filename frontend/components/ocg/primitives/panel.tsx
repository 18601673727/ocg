"use client";

import { useId, type ReactNode } from "react";
import { cn } from "@/lib/utils";
import { TYPOGRAPHY_SCALE, CONTAINER_PADDING, GAP_SCALE } from "@/lib/design-tokens";

/**
 * Bordered block that owns one idea inside a surface.
 *
 * Give it a `title` and it renders the standard title row and labels the
 * section with it, so a panel is one component rather than a `div`, a
 * hand-rolled heading and the aria wiring that ties them together.
 */
export function Panel({
  children,
  title,
  detail,
  action,
  className,
  contentClassName,
  labelledBy,
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
  /** Heading rendered outside the panel that should name this section instead. */
  labelledBy?: string;
}) {
  const headingId = useId();
  const titled = title !== undefined;
  return (
    <section
      aria-labelledby={labelledBy ?? (titled ? headingId : undefined)}
      className={cn("min-w-0 rounded-md border border-border bg-card", CONTAINER_PADDING.panel, className)}
    >
      {titled && <PanelHeader headingId={headingId} title={title} detail={detail} className="mb-2">{action}</PanelHeader>}
      {/* Only wrap the body when it needs its own box, so an existing flex
          layout on the panel itself keeps addressing its real children. */}
      {contentClassName ? <div className={contentClassName}>{children}</div> : children}
    </section>
  );
}

/** Title row of a panel: heading, optional definition, then trailing content. */
export function PanelHeader({
  title,
  detail,
  children,
  headingId,
  className,
}: {
  title: ReactNode;
  detail?: ReactNode;
  children?: ReactNode;
  headingId?: string;
  className?: string;
}) {
  return (
    <div className={cn("flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-1", className)}>
      <h2
        id={headingId}
        className={cn("min-w-0 truncate", TYPOGRAPHY_SCALE.caption, "font-semibold tracking-wider text-muted-foreground uppercase")}
      >
        {title}
      </h2>
      {detail !== undefined && (
        <span className={cn("ml-auto min-w-0 truncate", TYPOGRAPHY_SCALE.caption, "text-muted-foreground")}>{detail}</span>
      )}
      {children ? <span className={cn("ml-auto flex shrink-0 items-center", GAP_SCALE.tight)}>{children}</span> : null}
    </div>
  );
}
