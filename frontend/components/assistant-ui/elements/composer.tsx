"use client";

import type { ComponentProps } from "react";
import { ArrowUpIcon, PlusIcon } from "lucide-react";
import { cn } from "@/lib/utils";
import { ghostButton, inkButton, paper } from "./surfaces";

export function Composer({ className, ...props }: ComponentProps<"div">) {
  return (
    <div data-slot="composer" className={cn("relative w-full max-w-lg", className)} {...props} />
  );
}

export function ComposerBar({
  dragActive = false,
  className,
  ...props
}: ComponentProps<"div"> & { dragActive?: boolean }) {
  return (
    <div
      data-slot="composer-bar"
      data-drag-active={dragActive || undefined}
      className={cn(
        paper,
        "flex w-full flex-col gap-2 rounded-[24px] p-2.5 transition-colors",
        className,
        dragActive && "border-primary bg-accent dark:bg-accent",
      )}
      {...props}
    />
  );
}

export function ComposerAttachments({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      data-slot="composer-attachments"
      className={cn("flex flex-wrap gap-2", className)}
      {...props}
    />
  );
}

export function ComposerToolbar({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      data-slot="composer-toolbar"
      className={cn("flex items-center justify-between", className)}
      {...props}
    />
  );
}

export function ComposerActions({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      data-slot="composer-actions"
      className={cn("flex items-center gap-1.5", className)}
      {...props}
    />
  );
}

export function ComposerAttachButton({
  className,
  ...props
}: Omit<ComponentProps<"button">, "children">) {
  return (
    <button
      type="button"
      aria-label="Add attachment"
      data-slot="composer-attach"
      disabled={!props.onClick}
      className={cn(
        ghostButton,
        "size-8 disabled:pointer-events-none disabled:opacity-30",
        className,
      )}
      {...props}
    >
      <PlusIcon className="size-4" aria-hidden="true" />
    </button>
  );
}

export function ComposerSend({
  idle,
  className,
  ...props
}: Omit<ComponentProps<"button">, "children"> & {
  idle: boolean;
}) {
  return (
    <button
      type="button"
      aria-label="Send message"
      data-slot="composer-send"
      className={cn(
        "grid size-8 place-items-center rounded-full",
        !idle
          ? inkButton
          : "bg-foreground/[0.06] text-muted-foreground inset-ring-border dark:bg-foreground/[0.09] inset-ring transition-colors forced-colors:border forced-colors:border-[GrayText] forced-colors:text-[GrayText]",
        className,
      )}
      {...props}
    >
      <ArrowUpIcon className="size-4" aria-hidden="true" />
    </button>
  );
}
