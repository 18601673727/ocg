import type { JSX } from "@solidjs/web";
import { createEffect } from "solid-js";
import { splitProps } from "@/lib/ui-state";

import { cn } from "@/lib/utils";

type OptionValue = string | number;
type SelectProps<T extends OptionValue> = Omit<
  JSX.IntrinsicElements["select"],
  "value" | "size" | "onChange" | "children"
> & {
  value: T | null | undefined;
  items?: readonly { value: T; label: string }[];
  children?: JSX.Element;
  size?: "sm" | "default";
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
  ]);
  let select!: HTMLSelectElement;
  createEffect(
    () => [local.value, local.items],
    () => {
      select.value = local.value === undefined || local.value === null ? "" : String(local.value);
    },
  );
  return (
    <select
      {...rest}
      ref={select}
      data-slot="select-trigger"
      data-size={local.size ?? "default"}
      class={cn(
        "w-fit min-w-0 rounded-lg border border-transparent bg-muted/70 px-3 py-2 text-sm outline-none hover:bg-muted focus-visible:border-ring focus-visible:ring-1 focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50 data-[size=default]:h-9 data-[size=sm]:h-8",
        local.class,
      )}
      onChange={(event) => {
        const options = local.items;
        const selected = event.currentTarget.value;
        const item = options?.find((option) => String(option.value) === selected);
        local.onValueChange?.(item?.value ?? (selected as T));
      }}
    >
      {local.children}
    </select>
  );
}

export function SelectItem(
  props: Omit<JSX.IntrinsicElements["option"], "value"> & { value: OptionValue },
) {
  return <option {...props} value={String(props.value)} />;
}
