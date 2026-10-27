"use client";

import type { ComponentProps } from "react";
import { cn } from "@/lib/utils";
import { take } from "../utils/range";

export function StreamingText({
  text,
  count,
  streaming,
  className,
  ...props
}: Omit<ComponentProps<"p">, "children" | "count" | "streaming"> & {
  text: string;
  count: number;
  streaming: boolean;
}) {
  const shown = take(text.split(" "), count);

  return (
    <p
      data-slot="streaming-text"
      className={cn(
        "min-h-[8.5rem] max-w-sm whitespace-pre-wrap text-sm leading-relaxed text-pretty",
        className,
      )}
      {...props}
    >
      {shown.map((word, i) => {
        const fresh = streaming && shown.length - 1 - i < 2;
        return (
          <span
            key={i}
            className="fade-in animate-in fill-mode-both duration-500 motion-reduce:animate-none"
          >
            <span
              className={cn(
                "transition-colors duration-700 motion-reduce:transition-none",
                fresh && "text-primary",
              )}
            >
              {word}
            </span>{" "}
          </span>
        );
      })}
      {streaming && shown.length > 0 && (
        <span
          aria-hidden
          className="-mb-0.5 ml-0.5 inline-block h-4 w-0.5 animate-pulse rounded-full bg-primary"
        />
      )}
    </p>
  );
}
