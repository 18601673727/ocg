import { CheckIcon, Loader2Icon } from "@lucide/solid";
import { createMemo, createSignal } from "solid-js";
import {
  ariaState,
  bindElementRef,
  type ElementProps,
  splitProps,
  type View,
} from "@/lib/ui-state";

import { cn } from "@/lib/utils";
import { clamp } from "./range";
import { useReceiptFocus } from "./receipt-focus";
import { inkButton, mono, paper } from "./surfaces";

interface OptionListOption {
  id: string;
  label: string;
  description?: View;
  disabled?: boolean | undefined;
}

export interface OptionListProps
  extends Omit<
    ElementProps<"div">,
    "children" | "defaultValue" | "onChange" | "onSelect" | "onSelectionChange"
  > {
  options: readonly OptionListOption[];
  selectionMode?: "single" | "multiple" | undefined;
  defaultValue?: readonly string[] | undefined;
  /** Draft selection controlled by the parent form. */
  selected?: readonly string[];
  onSelectionChange?: (ids: string[]) => void;
  minSelections?: number | undefined;
  maxSelections?: number | undefined;
  /** Commits on pick in single mode and on confirm in multiple mode. Rejection re-enables input. */
  onConfirm?: ((ids: string[]) => void | Promise<void>) | undefined;
  confirmLabel?: string | undefined;
  /** Committed IDs render as a read-only receipt, even when the array is empty. */
  choice?: readonly string[] | undefined;
}

const row =
  "flex w-full items-start gap-2.5 rounded-xl px-2 py-2 text-start transition-colors outline-none focus-visible:ring-1 focus-visible:ring-foreground/20";

function OptionText(props: { option: OptionListOption }) {
  return (
    <span class="flex min-w-0 flex-1 flex-col gap-0.5">
      <span class="text-[13.5px] leading-5 break-words">{props.option.label}</span>
      {props.option.description ? (
        <span class="text-muted-foreground text-xs leading-4 break-words">
          {props.option.description}
        </span>
      ) : null}
    </span>
  );
}

export function OptionList(viewProps: OptionListProps) {
  const [, props] = splitProps(viewProps, [
    "options",
    "selectionMode",
    "defaultValue",
    "selected",
    "onSelectionChange",
    "minSelections",
    "maxSelections",
    "onConfirm",
    "confirmLabel",
    "choice",
    "class",
  ]);

  const multiple = createMemo(
    () =>
      (viewProps.selectionMode === undefined ? "single" : viewProps.selectionMode) === "multiple",
  );
  const [localSelection, setLocalSelection] = createSignal<readonly string[]>(
    (() =>
      (viewProps.defaultValue ?? []).filter((id) => viewProps.options.some((o) => o.id === id)))(),
  );
  const selected = createMemo(() => viewProps.selected ?? localSelection());
  const externalForm = createMemo(() => multiple() && viewProps.onSelectionChange !== undefined);
  const [pending, setPending] = createSignal<readonly string[] | null>(null);
  const [confirmed, setConfirmed] = createSignal<readonly string[] | undefined>();
  const [error, setError] = createSignal<string | null>(null);
  const { receiptRef, focusHandlers } = useReceiptFocus<HTMLDivElement>(props);

  const root = createMemo(() =>
    cn(paper, "flex w-full max-w-sm flex-col gap-1 rounded-2xl p-2", viewProps.class),
  );

  const confirmedChoice = createMemo(() => viewProps.choice ?? confirmed());

  return (
    <>
      {(() => {
        if (confirmedChoice() !== undefined) {
          const chosen = viewProps.options.filter((option) =>
            confirmedChoice()!.includes(option.id),
          );
          return (
            <div
              data-slot="option-list"
              data-state="receipt"
              class={root()}
              {...props}
              {...focusHandlers}
              ref={bindElementRef(receiptRef)}
              tabindex={-1}
            >
              {chosen.length === 0 ? (
                <span class={cn(mono, "text-muted-foreground px-2 py-2")}>Nothing selected</span>
              ) : (
                chosen.map((option) => (
                  <div class={cn(row, "text-foreground/70")}>
                    <span class="flex h-5 w-3.5 shrink-0 items-center justify-center">
                      <CheckIcon aria-hidden="true" class="text-muted-foreground size-3.5" />
                      <span class="sr-only">Selected:</span>
                    </span>
                    <OptionText option={option} />
                  </div>
                ))
              )}
            </div>
          );
        }

        if (!viewProps.onConfirm && !externalForm()) {
          return (
            <div data-slot="option-list" class={root()} {...props} {...focusHandlers}>
              {viewProps.options.map((option) => (
                <div class={cn(row, option.disabled && "text-muted-foreground")}>
                  <OptionText option={option} />
                </div>
              ))}
            </div>
          );
        }

        const locked = createMemo(() => pending() !== null);
        const max = createMemo(() =>
          Math.floor(
            clamp(viewProps.maxSelections ?? viewProps.options.length, 0, viewProps.options.length),
          ),
        );
        const min = createMemo(() =>
          Math.floor(
            clamp(viewProps.minSelections === undefined ? 1 : viewProps.minSelections, 0, max()),
          ),
        );
        const optionIds = new Set(viewProps.options.map((option) => option.id));
        const selectedIds = createMemo(() => selected().filter((id) => optionIds.has(id)));

        const commit = (ids: string[]) => {
          if (locked() || !viewProps.onConfirm) return;
          setPending(ids);
          setError(null);
          void (async () => {
            try {
              await viewProps!.onConfirm!(ids);
              setConfirmed(ids);
            } catch (commitError) {
              setPending(null);
              setError(commitError instanceof Error ? commitError.message : String(commitError));
            }
          })();
        };

        const toggle = (id: string) => {
          if (locked()) return;
          const next = selectedIds().includes(id)
            ? selectedIds().filter((value) => value !== id)
            : selectedIds().length < max()
              ? [...selectedIds(), id]
              : selectedIds();
          if (externalForm()) viewProps!.onSelectionChange!(next);
          else setLocalSelection(next);
        };

        const count = createMemo(() => selectedIds().length);
        const canConfirm = createMemo(() => !locked() && count() >= min() && count() <= max());

        return (
          <div
            role="group"
            data-slot="option-list"
            data-state={locked() ? "pending" : "open"}
            aria-busy={ariaState(locked() || undefined)}
            class={root()}
            {...props}
            {...focusHandlers}
          >
            {viewProps.options.map((option) => {
              const isSelected = multiple()
                ? selectedIds().includes(option.id)
                : (pending() ?? selected()).includes(option.id);
              const unavailable =
                option.disabled === true || (multiple() && !isSelected && count() >= max());
              return (
                <button
                  type="button"
                  {...(multiple()
                    ? { role: "checkbox", "aria-checked": ariaState(isSelected) }
                    : {})}
                  aria-disabled={ariaState(unavailable || locked() || undefined)}
                  disabled={option.disabled}
                  onClick={() => {
                    if (unavailable) return;
                    if (multiple()) toggle(option.id);
                    else commit([option.id]);
                  }}
                  class={cn(
                    row,
                    isSelected
                      ? "bg-foreground/[0.06]"
                      : !unavailable && !locked() && "hover:bg-foreground/[0.035]",
                    unavailable ? "text-muted-foreground" : "text-foreground/90",
                    (unavailable || locked()) && "cursor-default",
                  )}
                >
                  {multiple() ? (
                    <span
                      aria-hidden="true"
                      class="flex h-5 w-3.5 shrink-0 items-center justify-center"
                    >
                      <span
                        class={cn(
                          "flex size-3.5 items-center justify-center rounded-[5px] border transition-colors",
                          isSelected ? "border-foreground bg-foreground" : "border-foreground/15",
                        )}
                      >
                        {isSelected ? (
                          <CheckIcon class="fade-in zoom-in-90 animate-in text-background size-2.5 duration-200 motion-reduce:animate-none" />
                        ) : null}
                      </span>
                    </span>
                  ) : pending()?.includes(option.id) ? (
                    <span
                      aria-hidden="true"
                      class="flex h-5 w-3.5 shrink-0 items-center justify-center"
                    >
                      <Loader2Icon class="text-muted-foreground size-3.5 animate-spin motion-reduce:animate-none" />
                    </span>
                  ) : null}
                  <OptionText option={option} />
                </button>
              );
            })}
            {error() ? (
              <p role="alert" class="px-2 pb-1 text-xs break-words text-red-600 dark:text-red-400">
                {error()}
              </p>
            ) : null}
            {multiple() && !externalForm() ? (
              <div class="flex items-center justify-between gap-3 px-2 pt-1.5 pb-1">
                <span class={cn(mono, "text-muted-foreground tabular-nums")}>
                  {count()} of {max()}
                </span>
                <button
                  type="button"
                  aria-disabled={ariaState(!canConfirm() || undefined)}
                  onClick={() => {
                    if (!canConfirm()) return;
                    commit(
                      viewProps.options
                        .filter((option) => selectedIds().includes(option.id))
                        .map((option) => option.id),
                    );
                  }}
                  class={cn(
                    inkButton,
                    "flex h-8 items-center gap-1.5 rounded-full px-3.5 text-xs font-medium",
                    !canConfirm() && "cursor-default opacity-40 hover:opacity-40",
                  )}
                >
                  {locked() ? (
                    <Loader2Icon
                      aria-hidden="true"
                      class="size-3.5 animate-spin motion-reduce:animate-none"
                    />
                  ) : null}
                  {viewProps.confirmLabel === undefined ? "Confirm" : viewProps.confirmLabel}
                </button>
              </div>
            ) : null}
          </div>
        );
      })()}
    </>
  );
}
