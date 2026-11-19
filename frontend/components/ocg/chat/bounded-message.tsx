import { ChevronDown } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import { ariaState, bindElementRef, elementRef, type View } from "@/lib/ui-state";

import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";
import { BOUNDED_MESSAGE_CLASS, boundedToggle, followAfterScroll } from "./message-bounds";

export function BoundedMessage(props: {
  streaming: boolean;
  children: View;
  expandable?: boolean;
  class?: string;
  label?: string;
}) {
  const i18n = useI18n();
  const viewport = elementRef<HTMLElement>(null);
  const content = elementRef<HTMLDivElement>(null);
  const following = elementRef(true);
  const lastTop = elementRef(0);
  const touchY = elementRef<number | null>(null);
  const [expanded, setExpanded] = createSignal(false);
  const [overflowing, setOverflowing] = createSignal(false);

  createEffect(
    () => [expanded(), props.streaming],
    () => {
      const el = viewport.current;
      const inner = content.current;
      if (!el || !inner) return;
      const observer = new ResizeObserver(() => {
        if (!expanded() && props.streaming && following.current) {
          el.scrollTop = el.scrollHeight;
          lastTop.current = el.scrollTop;
        }
        setOverflowing(el.scrollHeight - el.clientHeight > 1);
      });
      observer.observe(inner);
      observer.observe(el);
      return () => observer.disconnect();
    },
  );

  const toggle = createMemo(() =>
    (props.expandable === undefined ? true : props.expandable)
      ? boundedToggle(overflowing(), expanded())
      : null,
  );
  return (
    <>
      <section
        ref={bindElementRef(viewport)}
        data-message-bounds={expanded() ? "expanded" : "bounded"}
        aria-label={props.label}
        tabindex={props.label ? 0 : undefined}
        onWheel={(event) => {
          if (event.deltaY < 0) following.current = false;
        }}
        onTouchStart={(event) => {
          touchY.current = event.touches[0]?.clientY ?? null;
        }}
        onTouchMove={(event) => {
          const next = event.touches[0]?.clientY ?? null;
          if (next !== null && touchY.current !== null && next > touchY.current)
            following.current = false;
          touchY.current = next;
        }}
        onKeyDown={(event) => {
          if (
            event.key === "ArrowUp" ||
            event.key === "PageUp" ||
            event.key === "Home" ||
            (event.key === " " && event.shiftKey)
          )
            following.current = false;
        }}
        onScroll={() => {
          const el = viewport.current;
          if (!el || expanded()) return;
          following.current = followAfterScroll(following.current, lastTop.current, el);
          lastTop.current = el.scrollTop;
        }}
        class={cn(
          "min-w-0",
          !expanded() && BOUNDED_MESSAGE_CLASS,
          props.label &&
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30",
          props.class,
        )}
      >
        <div ref={bindElementRef(content)}>{props.children}</div>
      </section>
      {toggle() && (
        <button
          type="button"
          aria-expanded={ariaState(expanded())}
          onClick={() => setExpanded((value) => !value)}
          class="mt-1 inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-ring"
        >
          <ChevronDown
            class={cn("size-3 transition-transform", expanded() && "rotate-180")}
            aria-hidden="true"
          />
          {i18n.t(toggle() === "collapse" ? "chat.collapseMessage" : "chat.expandMessage")}
        </button>
      )}
    </>
  );
}
