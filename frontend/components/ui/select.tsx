import type { JSX } from "@solidjs/web";
import { createEffect } from "solid-js";
import { splitProps } from "@/lib/ui-state";

import { cn } from "@/lib/utils";
import "./select.css";

type OptionValue = string | number;
type SelectProps<T extends OptionValue> = Omit<
  JSX.IntrinsicElements["select"],
  "value" | "size" | "onChange" | "children"
> & {
  value: T | null | undefined;
  items?: readonly { value: T; label: string }[];
  children?: JSX.Element;
  size?: "sm" | "default";
  fitSelected?: boolean;
  onValueChange?: (value: T | null) => void;
};

export function Select<T extends OptionValue>(props: SelectProps<T>) {
  const [local, rest] = splitProps(props, [
    "class",
    "size",
    "value",
    "items",
    "onValueChange",
    "children",
    "fitSelected",
  ]);
  let select!: HTMLSelectElement;
  createEffect(
    () => [local.value, local.items],
    () => {
      select.value = local.value === undefined || local.value === null ? "" : String(local.value);
    },
  );
  const trigger = () => (
    <select
      {...rest}
      ref={select}
      data-slot="select-trigger"
      data-size={local.size ?? "default"}
      class={cn(
        "ocg-select inline-flex w-fit min-w-0 items-center justify-between gap-2 rounded-lg border border-input bg-background px-3 py-1.5 pr-9 text-sm text-foreground outline-none transition-colors hover:bg-muted/60 focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-50 data-[size=default]:h-9 data-[size=sm]:h-8",
        local.class,
        local.fitSelected && "absolute inset-0 h-full w-full max-w-full",
      )}
      onChange={(event) => {
        const options = local.items;
        const selected = event.currentTarget.value;
        const item = options?.find((option) => String(option.value) === selected);
        local.onValueChange?.(item?.value ?? (selected as T));
      }}
    >
      <button type="button">
        <selectedcontent />
      </button>
      {local.children}
    </select>
  );
  return local.fitSelected ? (
    <span class="relative inline-grid min-w-0 max-w-full align-middle">
      <span
        aria-hidden="true"
        data-size={local.size ?? "default"}
        class={cn(
          "pointer-events-none invisible inline-flex min-w-0 items-center overflow-hidden whitespace-pre rounded-lg border px-3 py-1.5 text-sm data-[size=default]:h-9 data-[size=sm]:h-8",
          local.class,
          // Keep the native picker's width allowance and match the custom icon when supported.
          "pe-10 supports-[appearance:base-select]:pe-9",
        )}
      >
        {local.items?.find((item) => item.value === local.value)?.label ?? ""}
      </span>
      {trigger()}
    </span>
  ) : (
    trigger()
  );
}

export function SelectItem(
  props: Omit<JSX.IntrinsicElements["option"], "value"> & { value: OptionValue },
) {
  return <option {...props} value={String(props.value)} />;
}
