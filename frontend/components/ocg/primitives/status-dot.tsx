"use client";

import { cn } from "@/lib/utils";
import { DOT_TONE, type Tone } from "./tone";

/** A quiet status indicator dot. Colour comes from the shared tone vocabulary. */
export function StatusDot({
  tone,
  pulse = false,
  size = "sm",
  className,
}: {
  tone: Tone;
  pulse?: boolean;
  size?: "sm" | "md";
  className?: string;
}) {
  return (
    <span
      className={cn(
        "shrink-0 rounded-full",
        size === "sm" ? "size-1.5" : "size-2",
        DOT_TONE[tone],
        pulse && "animate-pulse",
        className,
      )}
      aria-hidden="true"
    />
  );
}
