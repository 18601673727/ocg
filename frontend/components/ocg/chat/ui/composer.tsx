import { ArrowUpIcon, PlusIcon } from "@lucide/solid";
import { type ElementProps, splitProps } from "@/lib/ui-state";

import { cn } from "@/lib/utils";
import { ghostButton, inkButton, paper } from "./surfaces";

export function Composer(viewProps: ElementProps<"div">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return (
    <div data-slot="composer" class={cn("relative w-full max-w-lg", viewProps.class)} {...props} />
  );
}

export function ComposerBar(viewProps: ElementProps<"div"> & { dragActive?: boolean }) {
  const [, props] = splitProps(viewProps, ["dragActive", "class"]);

  return (
    <div
      data-slot="composer-bar"
      data-drag-active={
        (viewProps.dragActive === undefined ? false : viewProps.dragActive) || undefined
      }
      class={cn(
        paper,
        "flex w-full flex-col gap-2 rounded-[24px] p-2.5 transition-colors",
        viewProps.class,
        (viewProps.dragActive === undefined ? false : viewProps.dragActive) &&
          "border-primary bg-accent dark:bg-accent",
      )}
      {...props}
    />
  );
}

export function ComposerAttachments(viewProps: ElementProps<"div">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return (
    <div
      data-slot="composer-attachments"
      class={cn("flex flex-wrap gap-2", viewProps.class)}
      {...props}
    />
  );
}

export function ComposerToolbar(viewProps: ElementProps<"div">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return (
    <div
      data-slot="composer-toolbar"
      class={cn("flex items-center justify-between", viewProps.class)}
      {...props}
    />
  );
}

export function ComposerActions(viewProps: ElementProps<"div">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return (
    <div
      data-slot="composer-actions"
      class={cn("flex items-center gap-1.5", viewProps.class)}
      {...props}
    />
  );
}

export function ComposerAttachButton(viewProps: Omit<ElementProps<"button">, "children">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return (
    <button
      type="button"
      aria-label="Add attachment"
      data-slot="composer-attach"
      disabled={!props.onClick}
      class={cn(
        ghostButton,
        "size-8 disabled:pointer-events-none disabled:opacity-30",
        viewProps.class,
      )}
      {...props}
    >
      <PlusIcon class="size-4" aria-hidden="true" />
    </button>
  );
}

export function ComposerSend(
  viewProps: Omit<ElementProps<"button">, "children"> & {
    idle: boolean;
  },
) {
  const [, props] = splitProps(viewProps, ["idle", "class"]);

  return (
    <button
      type="button"
      aria-label="Send message"
      data-slot="composer-send"
      class={cn(
        "grid size-8 place-items-center rounded-full",
        !viewProps.idle
          ? inkButton
          : "bg-foreground/[0.06] text-muted-foreground inset-ring-border dark:bg-foreground/[0.09] inset-ring transition-colors forced-colors:border forced-colors:border-[GrayText] forced-colors:text-[GrayText]",
        viewProps.class,
      )}
      {...props}
    >
      <ArrowUpIcon class="size-4" aria-hidden="true" />
    </button>
  );
}
