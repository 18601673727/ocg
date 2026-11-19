import { CheckIcon, Loader2Icon, TerminalIcon, XIcon } from "@lucide/solid";
import { createUniqueId } from "solid-js";
import { bindElementRef, type ElementProps, splitProps, type View } from "@/lib/ui-state";

import { cn } from "@/lib/utils";
import { useReceiptFocus } from "./receipt-focus";
import { field, ghostButton, inkButton, mono, paper } from "./surfaces";

export type ApprovalState = "request" | "running" | "done" | "denied";

const receiptText: Record<Exclude<ApprovalState, "request">, string> = {
  running: "Approved, running",
  done: "Finished",
  denied: "Denied",
};

export function ApprovalCard(
  viewProps: Omit<
    ElementProps<"div">,
    | "children"
    | "state"
    | "command"
    | "title"
    | "subtitle"
    | "description"
    | "details"
    | "disabled"
    | "variant"
    | "icon"
    | "onAllowOnce"
    | "onAlwaysAllow"
    | "onDeny"
    | "allowOnceLabel"
    | "alwaysAllowLabel"
    | "denyLabel"
    | "statusLabel"
  > & {
    state: ApprovalState;
    command?: string | undefined;
    title: string;
    subtitle: string;
    description?: string | undefined;
    details?: readonly { label: string; value: string }[] | undefined;
    children?: View;
    disabled?: boolean;
    variant?: "default" | "destructive" | undefined;
    icon?: View | undefined;
    onAllowOnce?: (() => void) | undefined;
    onAlwaysAllow?: (() => void) | undefined;
    onDeny?: (() => void) | undefined;
    allowOnceLabel?: string | undefined;
    alwaysAllowLabel?: string | undefined;
    denyLabel?: string | undefined;
    statusLabel?: string | undefined;
  },
) {
  const [, props] = splitProps(viewProps, [
    "state",
    "command",
    "title",
    "subtitle",
    "description",
    "details",
    "children",
    "disabled",
    "variant",
    "icon",
    "onAllowOnce",
    "onAlwaysAllow",
    "onDeny",
    "allowOnceLabel",
    "alwaysAllowLabel",
    "denyLabel",
    "statusLabel",
    "class",
  ]);

  const titleId = createUniqueId();
  const descriptionId = createUniqueId();
  const { receiptRef, focusHandlers } = useReceiptFocus<HTMLDivElement>(props);

  return (
    <div
      {...props}
      {...focusHandlers}
      role="group"
      data-variant={viewProps.variant === undefined ? "default" : viewProps.variant}
      data-slot="approval-card"
      aria-labelledby={titleId}
      aria-describedby={viewProps.description ? descriptionId : undefined}
      class={cn(paper, "flex w-full max-w-sm flex-col gap-3.5 rounded-[20px] p-4", viewProps.class)}
    >
      <div class="flex items-center gap-3">
        <span
          aria-hidden="true"
          class={cn(
            "flex size-9 shrink-0 items-center justify-center rounded-xl",
            (viewProps.variant === undefined ? "default" : viewProps.variant) === "destructive"
              ? "bg-red-600/10 text-red-600 dark:bg-red-400/10 dark:text-red-400"
              : "bg-foreground/[0.05] text-muted-foreground",
          )}
        >
          {viewProps.icon ?? <TerminalIcon class="size-4" />}
        </span>
        <div class="flex flex-col">
          <p id={titleId} class="text-[13.5px] font-medium">
            {viewProps.title}
          </p>
          <p class="text-muted-foreground text-xs">{viewProps.subtitle}</p>
        </div>
      </div>

      {viewProps.description ? (
        <p id={descriptionId} class="text-foreground/60 text-[13px]">
          {viewProps.description}
        </p>
      ) : null}

      {viewProps.command ? (
        <div
          class={cn(
            field,
            "text-foreground/70 rounded-xl px-3.5 py-2.5 font-mono text-xs break-all",
          )}
        >
          {viewProps.command}
        </div>
      ) : null}

      {viewProps.details?.length ? (
        <dl class={cn(field, "flex flex-col gap-2 rounded-xl px-3.5 py-2.5")}>
          {viewProps.details.map((detail, index) => (
            <div class="grid grid-cols-[minmax(0,1fr)_minmax(0,2fr)] gap-4 text-xs">
              <dt class={cn(mono, "text-muted-foreground")}>{detail.label}</dt>
              <dd class="text-foreground/80 break-words">{detail.value}</dd>
            </div>
          ))}
        </dl>
      ) : null}

      {viewProps.children}
      <div class="flex min-h-8 flex-wrap items-center justify-end gap-2">
        {viewProps.state === "request" ? (
          <>
            {viewProps.onDeny && (
              <button
                type="button"
                data-slot="approval-card-deny"
                disabled={viewProps.disabled}
                onClick={viewProps.onDeny}
                class={cn(
                  ghostButton,
                  "h-8 px-3.5 text-xs font-medium whitespace-nowrap disabled:opacity-50",
                )}
              >
                {viewProps.denyLabel === undefined ? "Deny" : viewProps.denyLabel}
              </button>
            )}
            {viewProps.onAlwaysAllow && (
              <button
                type="button"
                data-slot="approval-card-always-allow"
                disabled={viewProps.disabled}
                onClick={viewProps.onAlwaysAllow}
                class={cn(
                  ghostButton,
                  "h-8 px-3.5 text-xs font-medium whitespace-nowrap disabled:opacity-50",
                )}
              >
                {viewProps.alwaysAllowLabel === undefined
                  ? "Always allow"
                  : viewProps.alwaysAllowLabel}
              </button>
            )}
            {viewProps.onAllowOnce && (
              <button
                type="button"
                data-slot="approval-card-allow-once"
                disabled={viewProps.disabled}
                onClick={viewProps.onAllowOnce}
                class={cn(
                  (viewProps.variant === undefined ? "default" : viewProps.variant) ===
                    "destructive"
                    ? "text-background bg-red-600 transition-[background-color,scale] duration-150 hover:bg-red-600/90 active:scale-[0.96] motion-reduce:transition-none dark:bg-red-400 dark:text-red-950 dark:hover:bg-red-400/90"
                    : inkButton,
                  "flex h-8 items-center rounded-full px-3.5 text-xs font-medium whitespace-nowrap disabled:opacity-50",
                )}
              >
                {viewProps.allowOnceLabel === undefined ? "Allow once" : viewProps.allowOnceLabel}
              </button>
            )}
          </>
        ) : (
          <div
            ref={bindElementRef(receiptRef)}
            tabindex={-1}
            role="status"
            class="fade-in animate-in text-muted-foreground flex items-center gap-2 text-xs duration-300 motion-reduce:animate-none"
          >
            {viewProps.state === "running" ? (
              <>
                <Loader2Icon class="text-muted-foreground size-3.5 animate-spin motion-reduce:animate-none" />
                {viewProps.statusLabel ?? receiptText.running}
              </>
            ) : viewProps.state === "denied" ? (
              <>
                <XIcon class="text-muted-foreground size-3.5" />
                {viewProps.statusLabel ?? receiptText.denied}
              </>
            ) : (
              <>
                <CheckIcon class="size-3.5 text-emerald-500" />
                {viewProps.statusLabel ?? receiptText.done}
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
