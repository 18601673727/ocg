"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { StatusDot } from "./status-dot";
import { TONE_CLASS, TEXT_TONE, type Tone } from "./tone";

/**
 * Compact status label.
 *
 * `solid` is the tinted badge used by the ledger and control centre; `quiet`
 * keeps a neutral surface and only borrows the tone for its text and dot.
 */
export function Pill({
  children,
  tone = "slate",
  variant = "solid",
  dot,
  pulse,
  title,
  className,
}: {
  children: ReactNode;
  tone?: Tone;
  variant?: "solid" | "quiet";
  /** Render a leading status dot in this pill's tone. */
  dot?: boolean;
  pulse?: boolean;
  title?: string;
  className?: string;
}) {
  return (
    <span
      title={title}
      className={cn(
        "inline-flex shrink-0 items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] font-medium capitalize",
        variant === "solid"
          ? TONE_CLASS[tone]
          : cn("border-border bg-muted/40", tone === "slate" ? "text-muted-foreground" : TEXT_TONE[tone]),
        className,
      )}
    >
      {dot && <StatusDot tone={tone} pulse={pulse} />}
      {children}
    </span>
  );
}
