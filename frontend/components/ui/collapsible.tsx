import type { JSX } from "@solidjs/web";
import { createContext, createSignal, Show, useContext } from "solid-js";
import { ariaState, handleEvent, splitProps } from "@/lib/ui-state";

type Disclosure = {
  open: () => boolean;
  toggle: () => void;
};
const DisclosureContext = createContext<Disclosure>();

export type CollapsibleProps = JSX.IntrinsicElements["div"] & {
  open?: boolean;
  defaultOpen?: boolean;
  onOpenChange?: (open: boolean, details: { reason: "trigger-press" }) => void;
};

export function Collapsible(props: CollapsibleProps) {
  const [local, rest] = splitProps(props, ["open", "defaultOpen", "onOpenChange", "children"]);
  const [expanded, setExpanded] = createSignal(local.defaultOpen ?? false);
  const open = () => local.open ?? expanded();
  function toggle() {
    const next = !open();
    if (local.open === undefined) setExpanded(next);
    local.onOpenChange?.(next, { reason: "trigger-press" });
  }
  return (
    <DisclosureContext value={{ open, toggle }}>
      <div
        data-slot="collapsible"
        data-open={open() || undefined}
        data-closed={!open() || undefined}
        {...rest}
      >
        {local.children}
      </div>
    </DisclosureContext>
  );
}

export function CollapsibleTrigger(props: JSX.IntrinsicElements["button"]) {
  const disclosure = useContext(DisclosureContext);
  const [local, rest] = splitProps(props, ["onClick", "type"]);
  return (
    <button
      type={local.type ?? "button"}
      data-slot="collapsible-trigger"
      data-open={disclosure?.open() || undefined}
      aria-expanded={ariaState(disclosure?.open() ?? false)}
      {...rest}
      onClick={(event) => {
        handleEvent(local.onClick, event);
        if (!event.defaultPrevented) disclosure?.toggle();
      }}
    />
  );
}

export function CollapsibleContent(
  props: JSX.IntrinsicElements["div"] & { keepMounted?: boolean },
) {
  const disclosure = useContext(DisclosureContext);
  const [local, rest] = splitProps(props, ["keepMounted", "children"]);
  return (
    <div
      data-slot="collapsible-content"
      data-open={disclosure?.open() || undefined}
      hidden={!disclosure?.open()}
      {...rest}
    >
      <Show when={local.keepMounted === true || Boolean(disclosure?.open())}>{local.children}</Show>
    </div>
  );
}
