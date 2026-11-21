import { type JSX, Portal } from "@solidjs/web";
import { createContext, createSignal, createUniqueId, onCleanup, Show, useContext } from "solid-js";
import { overlayDialog, positionOverlay } from "@/lib/overlay-position";
import { composeButton, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

type Trigger = { bounds: DOMRect; element: HTMLElement };
const TooltipContext = createContext<{
  id: string;
  anchor: () => Trigger | null;
  setAnchor: (target: Trigger | null) => void;
}>();

export function TooltipProvider(props: { children?: View; delay?: number }) {
  return props.children;
}
export function Tooltip(props: { children?: View }) {
  const [anchor, setAnchor] = createSignal<Trigger | null>(null);
  return (
    <TooltipContext value={{ id: createUniqueId(), anchor, setAnchor }}>
      {props.children}
    </TooltipContext>
  );
}
export function TooltipTrigger(props: { render?: View; children?: View }) {
  const state = useContext(TooltipContext);
  const element = props.render ? (
    composeButton(props.render, { children: props.children })
  ) : (
    <button type="button">{props.children}</button>
  );
  if (!(element instanceof HTMLElement) || !state) return element;
  element.setAttribute("aria-describedby", state.id);
  element.setAttribute("data-slot", "tooltip-trigger");
  const open = () => state.setAnchor({ bounds: element.getBoundingClientRect(), element });
  const close = () => state.setAnchor(null);
  const events = [
    ["pointerenter", open],
    ["pointerleave", close],
    ["focus", open],
    ["blur", close],
  ] as const;
  for (const [event, listener] of events) element.addEventListener(event, listener);
  onCleanup(() => {
    for (const [event, listener] of events) element.removeEventListener(event, listener);
  });
  return element;
}
export function TooltipContent(
  props: JSX.IntrinsicElements["div"] & {
    side?: "top" | "bottom" | "left" | "right" | "inline-end" | "inline-start";
    sideOffset?: number;
    align?: "center" | "start" | "end";
    alignOffset?: number;
  },
) {
  const state = useContext(TooltipContext);
  return (
    <Show when={state?.anchor()}>
      {(anchor) => {
        const position = (element: HTMLDivElement) =>
          positionOverlay(element, anchor().element, (bounds, box) => {
            const gap = props.sideOffset ?? 4;
            const center = bounds.left + (bounds.width - box.width) / 2;
            const middle = bounds.top + (bounds.height - box.height) / 2;
            switch (props.side) {
              case "bottom":
                return { left: center, top: bounds.bottom + gap };
              case "left":
              case "inline-start":
                return { left: bounds.left - box.width - gap, top: middle };
              case "right":
              case "inline-end":
                return { left: bounds.right + gap, top: middle };
              default:
                return { left: center, top: bounds.top - box.height - gap };
            }
          });
        return (
          <Portal mount={overlayDialog(anchor().element)}>
            <div
              id={state?.id}
              role="tooltip"
              data-slot="tooltip-content"
              class={cn(
                "pointer-events-none fixed z-[70] w-max max-w-xs rounded-md bg-foreground px-3 py-1.5 text-xs text-background",
                props.class,
              )}
              ref={position}
            >
              {props.children}
            </div>
          </Portal>
        );
      }}
    </Show>
  );
}
