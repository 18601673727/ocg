"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { StatusDot } from "./status-dot";
import { TONE_CLASS, TEXT_TONE, type Tone } from "./tone";
import { TYPOGRAPHY_SCALE, BORDER_RADIUS } from "@/lib/design-tokens";

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
  dot?: boolean;
  pulse?: boolean;
  title?: string;
  className?: string;
}) {
  return (
    <span
      title={title}
      className={cn(
        "inline-flex shrink-0 items-center gap-1",
        BORDER_RADIUS.interactive,
        "border px-1.5 py-0.5",
        TYPOGRAPHY_SCALE.caption,
        "font-medium capitalize",
        variant === "solid"
          ? TONE_CLASS[tone]
          : cn(
              "border-border bg-muted/40",
              tone === "slate" ? "text-muted-foreground" : TEXT_TONE[tone],
            ),
        className,
      )}
    >
      {dot && <StatusDot tone={tone} pulse={pulse} />}
      {children}
    </span>
  );
}
