"use client";

import { cn } from "@/lib/utils";
import { DOT_TONE, type Tone } from "./tone";

export function StatusDot({
  tone,
  pulse = false,
  size = "sm",
  title,
  className,
}: {
  tone: Tone;
  pulse?: boolean;
  size?: "sm" | "md";
  title?: string;
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
      title={title}
      aria-hidden="true"
    />
  );
}
