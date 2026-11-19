import { CheckIcon, PlugIcon, XIcon } from "@lucide/solid";
import { createMemo, createUniqueId } from "solid-js";
import { ariaState, bindElementRef, type ElementProps, splitProps } from "@/lib/ui-state";

import { cn } from "@/lib/utils";
import { useReceiptFocus } from "./receipt-focus";
import { field, inkButton, mono, paper } from "./surfaces";

export type ElicitationState = "request" | "accepted" | "declined";

export interface ElicitationField {
  name: string;
  label: string;
  value: string;
  kind: "text" | "multiline" | "choice" | "toggle";
  placeholder?: string;
  options?: readonly string[];
  required?: boolean;
}

function Toggle(props: { value: string }) {
  const enabled = createMemo(() => props.value === "true");
  return (
    <>
      <span
        aria-hidden="true"
        class={cn(
          "flex h-4 w-7 items-center rounded-full p-0.5 transition-colors duration-200",
          enabled() ? "bg-foreground/80" : "bg-foreground/15",
        )}
      >
        <span
          class={cn(
            "bg-background size-3 rounded-full transition-transform duration-200 motion-reduce:transition-none",
            enabled() && "translate-x-3",
          )}
        />
      </span>
      <span class="text-muted-foreground text-xs">{enabled() ? "On" : "Off"}</span>
    </>
  );
}

export function ElicitationForm(
  viewProps: Omit<
    ElementProps<"div">,
    | "children"
    | "server"
    | "message"
    | "fields"
    | "state"
    | "onFieldChange"
    | "onAccept"
    | "onDecline"
  > & {
    server: string;
    message: string;
    fields: readonly ElicitationField[];
    state: ElicitationState;
    onFieldChange?: ((name: string, value: string) => void) | undefined;
    onAccept?: () => void;
    onDecline?: () => void;
    disabled?: boolean;
    acceptLabel?: string;
    declineLabel?: string;
    requestLabel?: string;
  },
) {
  const [, props] = splitProps(viewProps, [
    "server",
    "message",
    "fields",
    "state",
    "onFieldChange",
    "onAccept",
    "onDecline",
    "disabled",
    "acceptLabel",
    "declineLabel",
    "requestLabel",
    "class",
  ]);

  const fieldPrefix = createUniqueId();
  const { receiptRef, focusHandlers } = useReceiptFocus(props);
  const interactive = createMemo(
    () => viewProps.state === "request" && viewProps.onFieldChange !== undefined,
  );
  const missingRequired = createMemo(
    () =>
      interactive() &&
      viewProps.fields.some(
        (item) => item.kind !== "toggle" && item.required === true && item.value.trim() === "",
      ),
  );

  return (
    <div
      data-slot="elicitation-form"
      class={cn(paper, "flex w-full max-w-sm flex-col gap-3.5 rounded-[20px] p-4", viewProps.class)}
      {...props}
      {...focusHandlers}
    >
      <div class="flex items-center gap-2.5">
        <span class="bg-foreground/[0.05] text-muted-foreground flex size-7 shrink-0 items-center justify-center rounded-lg">
          <PlugIcon class="size-3.5" />
        </span>
        <span class="min-w-0 flex-1 truncate text-[13.5px] font-medium">{viewProps.server}</span>
        {(viewProps.requestLabel === undefined ? "needs input" : viewProps.requestLabel) && (
          <span class={cn(mono, "text-muted-foreground shrink-0")}>
            {viewProps.requestLabel === undefined ? "needs input" : viewProps.requestLabel}
          </span>
        )}
      </div>

      <p class="text-muted-foreground text-sm whitespace-pre-wrap break-words">
        {viewProps.message}
      </p>

      <div class="flex flex-col gap-2.5">
        {viewProps.fields.map((item, index) => {
          const labelId = `${fieldPrefix}-${index}`;
          const inputId = `${labelId}-input`;
          return (
            <div class="flex flex-col gap-1">
              {(item.kind === "text" || item.kind === "multiline") && interactive() ? (
                <label id={labelId} for={inputId} class={cn(mono, "text-muted-foreground")}>
                  {item.label}
                  {item.required && <span class="text-muted-foreground"> *</span>}
                </label>
              ) : (
                <span id={labelId} class={cn(mono, "text-muted-foreground")}>
                  {item.label}
                  {item.required && <span class="text-muted-foreground"> *</span>}
                </span>
              )}
              {item.kind === "choice" ? (
                <div role="group" aria-labelledby={labelId} class="flex flex-wrap gap-1.5">
                  {item.options?.map((option) =>
                    interactive() ? (
                      <button
                        type="button"
                        disabled={viewProps.disabled}
                        aria-pressed={ariaState(option === item.value)}
                        onClick={() => viewProps!.onFieldChange!(item.name, option)}
                        class={cn(
                          "focus-visible:ring-foreground/20 rounded-full px-2.5 py-1 text-xs transition-colors outline-none focus-visible:ring-1",
                          option === item.value
                            ? "bg-foreground text-background"
                            : cn(field, "text-muted-foreground"),
                        )}
                      >
                        {option}
                      </button>
                    ) : (
                      <span
                        class={cn(
                          "rounded-full px-2.5 py-1 text-xs transition-colors",
                          option === item.value
                            ? "bg-foreground text-background"
                            : cn(field, "text-muted-foreground"),
                        )}
                      >
                        {option}
                      </span>
                    ),
                  )}
                </div>
              ) : item.kind === "toggle" ? (
                interactive() ? (
                  <button
                    type="button"
                    disabled={viewProps.disabled}
                    role="switch"
                    aria-labelledby={labelId}
                    aria-checked={ariaState(item.value === "true")}
                    onClick={() =>
                      viewProps!.onFieldChange!(item.name, item.value === "true" ? "false" : "true")
                    }
                    class="focus-visible:ring-foreground/20 flex w-fit items-center gap-2 rounded-lg outline-none focus-visible:ring-1"
                  >
                    <Toggle value={item.value} />
                  </button>
                ) : (
                  <span class="flex items-center gap-2">
                    <Toggle value={item.value} />
                  </span>
                )
              ) : interactive() && item.kind === "multiline" ? (
                <textarea
                  id={inputId}
                  class={cn(field, "min-h-16 rounded-lg p-2 text-sm focus-visible:outline-ring")}
                  value={item.value}
                  disabled={viewProps.disabled}
                  placeholder={item.placeholder}
                  aria-required={ariaState(item.required || undefined)}
                  onInput={(event) =>
                    viewProps!.onFieldChange!(item.name, event.currentTarget.value)
                  }
                />
              ) : interactive() ? (
                <input
                  id={inputId}
                  value={item.value}
                  disabled={viewProps.disabled}
                  placeholder={item.placeholder}
                  aria-required={ariaState(item.required || undefined)}
                  onInput={(event) =>
                    viewProps!.onFieldChange!(item.name, event.currentTarget.value)
                  }
                  class={cn(
                    field,
                    "text-foreground/80 focus-visible:ring-foreground/20 rounded-lg px-2.5 py-1.5 text-xs outline-none focus-visible:ring-1",
                  )}
                />
              ) : (
                <span class={cn(field, "text-foreground/80 rounded-lg px-2.5 py-1.5 text-xs")}>
                  {item.value}
                </span>
              )}
            </div>
          );
        })}
      </div>

      <div class="flex h-8 items-center justify-end gap-2">
        {viewProps.state === "request" ? (
          <>
            <button
              type="button"
              onClick={viewProps.onDecline}
              disabled={viewProps.disabled}
              class="text-muted-foreground hover:bg-foreground/[0.06] hover:text-foreground/90 h-8 rounded-full px-3.5 text-xs font-medium transition-[background-color,color,scale] duration-150 active:scale-[0.96] disabled:opacity-40"
            >
              {viewProps.declineLabel === undefined ? "Decline" : viewProps.declineLabel}
            </button>
            <button
              type="button"
              onClick={viewProps.onAccept}
              disabled={viewProps.disabled || missingRequired()}
              class={cn(
                inkButton,
                "flex h-8 items-center rounded-full px-3.5 text-xs font-medium disabled:pointer-events-none disabled:opacity-40",
              )}
            >
              {viewProps.acceptLabel === undefined ? "Send" : viewProps.acceptLabel}
            </button>
          </>
        ) : (
          <span
            ref={bindElementRef(receiptRef)}
            tabindex={-1}
            class="fade-in animate-in text-muted-foreground flex items-center gap-2 text-xs duration-300"
          >
            {viewProps.state === "accepted" ? (
              <>
                <CheckIcon class="size-3.5 text-emerald-500" />
                Sent to {viewProps.server}
              </>
            ) : (
              <>
                <XIcon class="text-muted-foreground size-3.5" />
                Declined
              </>
            )}
          </span>
        )}
      </div>
    </div>
  );
}
