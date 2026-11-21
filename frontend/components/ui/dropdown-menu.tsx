import { type JSX, Portal } from "@solidjs/web";
import { createContext, createEffect, createSignal, onCleanup, Show, useContext } from "solid-js";
import { overlayDialog, positionOverlay } from "@/lib/overlay-position";
import { handleEvent, splitProps, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

type MenuItemState = { index: number; disabled: boolean };

export type MenuNavigationKey = "ArrowUp" | "ArrowDown" | "Home" | "End";

export type MenuFocusable = { index: number; disabled: boolean; focus: () => void };

function menuFocusables(root: HTMLElement | null): MenuFocusable[] {
  const items = root?.querySelectorAll<HTMLButtonElement>('[role="menuitem"]') ?? [];
  return [...items].map((element, index) => ({
    index,
    disabled: element.disabled,
    focus: () => element.focus(),
  }));
}

function enabledMenuIndexes(items: readonly MenuItemState[]): number[] {
  return items
    .filter((item) => !item.disabled)
    .map((item) => item.index)
    .sort((first, second) => first - second);
}

function menuFocusTarget(
  items: readonly MenuItemState[],
  current: number,
  key: MenuNavigationKey,
): number | null {
  const enabled = enabledMenuIndexes(items);
  if (enabled.length === 0) return null;
  if (key === "Home") return enabled[0];
  if (key === "End") return enabled[enabled.length - 1];
  const position = enabled.indexOf(current);
  if (position === -1) return key === "ArrowDown" ? enabled[0] : enabled[enabled.length - 1];
  const step = key === "ArrowDown" ? 1 : -1;
  return enabled[(position + step + enabled.length) % enabled.length];
}

type MenuState = {
  open: () => boolean;
  setOpen: (open: boolean) => void;
  trigger: HTMLElement | undefined;
};
const MenuContext = createContext<MenuState>();

export function DropdownMenu(props: {
  children: View;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  const [internal, setOpen] = createSignal(false);
  const state: MenuState = {
    open: () => props.open ?? internal(),
    setOpen(value) {
      if (props.open === undefined) setOpen(value);
      props.onOpenChange?.(value);
    },
    trigger: undefined,
  };
  return <MenuContext value={state}>{props.children}</MenuContext>;
}

export function DropdownMenuTrigger(props: { children?: View; asChild?: boolean; class?: string }) {
  const state = useContext(MenuContext);
  const element = props.asChild ? (
    props.children
  ) : (
    <button type="button" class={props.class}>
      {props.children}
    </button>
  );
  if (!state || !(element instanceof HTMLElement)) return element;
  state.trigger = element;
  element.setAttribute("aria-haspopup", "menu");
  createEffect(state.open, (open) => element.setAttribute("aria-expanded", String(open)));
  function toggle(event: Event) {
    if (!event.defaultPrevented) state?.setOpen(!state.open());
  }
  function onKey(event: globalThis.KeyboardEvent) {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      state?.setOpen(true);
    }
  }
  element.addEventListener("click", toggle);
  element.addEventListener("keydown", onKey);
  onCleanup(() => {
    element.removeEventListener("click", toggle);
    element.removeEventListener("keydown", onKey);
  });
  return element;
}

type MenuContentProps = JSX.IntrinsicElements["div"] & {
  align?: "start" | "center" | "end";
  side?: "top" | "bottom" | "left" | "right";
  sideOffset?: number;
  onCloseAutoFocus?: (event: Event) => void;
};

export function DropdownMenuContent(props: MenuContentProps) {
  const state = useContext(MenuContext);
  const [local, rest] = splitProps(props, [
    "class",
    "children",
    "align",
    "side",
    "sideOffset",
    "onCloseAutoFocus",
  ]);
  let content!: HTMLDivElement;
  const [portalHost, setPortalHost] = createSignal<HTMLElement>();
  function mount(element: HTMLDivElement) {
    content = element;
    if (!state?.trigger) return;
    positionOverlay(element, state.trigger, (anchor, bounds, bottom) => {
      const gap = local.sideOffset ?? 6;
      let left =
        local.align === "end"
          ? anchor.right - bounds.width
          : local.align === "center"
            ? anchor.left + (anchor.width - bounds.width) / 2
            : anchor.left;
      let top = local.side === "top" ? anchor.top - bounds.height - gap : anchor.bottom + gap;
      if (local.side === "right") {
        left = anchor.right + gap;
        top = local.align === "end" ? anchor.bottom - bounds.height : anchor.top;
      }
      if (local.side === "left") {
        left = anchor.left - bounds.width - gap;
        top = anchor.top;
      }
      if (!local.side && top + bounds.height > bottom) top = anchor.top - bounds.height - gap;
      return { left, top };
    });
  }
  function focusItem(key: MenuNavigationKey) {
    const items = menuFocusables(content);
    const current = [...content.querySelectorAll('[role="menuitem"]')].indexOf(
      document.activeElement!,
    );
    const next = menuFocusTarget(items, current, key);
    if (next !== null) items[next]?.focus();
  }
  function close(restore: boolean) {
    state?.setOpen(false);
    if (!restore) return;
    const event = new Event("closeautofocus", { cancelable: true });
    local.onCloseAutoFocus?.(event);
    if (!event.defaultPrevented) state?.trigger?.focus();
  }
  createEffect(
    () => state?.open() ?? false,
    (open) => {
      if (!open) {
        setPortalHost(undefined);
        return;
      }
      // A controlled menu can open as its trigger is being mounted in a dialog.
      let frame = requestAnimationFrame(() => {
        setPortalHost(overlayDialog(state?.trigger) ?? document.body);
        frame = requestAnimationFrame(() => {
          content?.focus();
          if (content) focusItem("Home");
        });
      });
      const outside = (event: globalThis.PointerEvent) => {
        const node = event.target;
        if (node instanceof Node && !content?.contains(node) && !state?.trigger?.contains(node))
          close(false);
      };
      document.addEventListener("pointerdown", outside, true);
      return () => {
        cancelAnimationFrame(frame);
        document.removeEventListener("pointerdown", outside, true);
      };
    },
  );
  return (
    <Show when={state?.open() && portalHost()}>
      <Portal mount={portalHost()}>
        <div
          {...rest}
          ref={mount}
          tabindex={-1}
          role="menu"
          class={cn(
            "fixed z-[60] min-w-36 rounded-xl border border-border bg-popover p-1.5 text-popover-foreground shadow-md outline-none",
            local.class,
          )}
          onKeyDown={(event) => {
            if (event.key === "Escape" || event.key === "Tab") {
              if (event.key === "Escape") event.preventDefault();
              close(event.key === "Escape");
            } else if (
              event.key === "ArrowDown" ||
              event.key === "ArrowUp" ||
              event.key === "Home" ||
              event.key === "End"
            ) {
              event.preventDefault();
              focusItem(event.key);
            }
          }}
        >
          {local.children}
        </div>
      </Portal>
    </Show>
  );
}

export function DropdownMenuItem(
  props: JSX.IntrinsicElements["button"] & { onSelect?: () => void; destructive?: boolean },
) {
  const state = useContext(MenuContext);
  const [local, rest] = splitProps(props, ["class", "onClick", "onSelect", "destructive"]);
  return (
    <button
      {...rest}
      type="button"
      role="menuitem"
      tabindex={-1}
      class={cn(
        "flex w-full items-center gap-2 rounded-lg px-2.5 py-2 text-left text-sm outline-none hover:bg-muted focus:bg-muted disabled:pointer-events-none disabled:opacity-50",
        local.destructive && "text-destructive",
        local.class,
      )}
      onClick={(event) => {
        handleEvent(local.onClick, event);
        if (event.defaultPrevented) return;
        state?.setOpen(false);
        state?.trigger?.focus();
        local.onSelect?.();
      }}
    />
  );
}
