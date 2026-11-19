import { onCleanup } from "solid-js";
import { type ElementProps, elementRef, handleEvent } from "@/lib/ui-state";

type FocusHandlers = Pick<ElementProps<"div">, "onFocusIn" | "onFocusOut">;

export function useReceiptFocus<T extends HTMLElement = HTMLElement>(handlers: FocusHandlers = {}) {
  const receiptRef = elementRef<T | null>(null);
  let focused: HTMLElement | null = null;
  let observer: MutationObserver | undefined;
  const stopWatching = () => {
    observer?.disconnect();
    focused = null;
  };

  onCleanup(stopWatching);

  return {
    receiptRef,
    focusHandlers: {
      onFocusIn(event) {
        handleEvent(handlers.onFocusIn, event);
        stopWatching();
        if (!(event.target instanceof HTMLElement)) return;
        focused = event.target;
        observer = new MutationObserver(() => {
          if (!focused || focused.isConnected) return;
          const document = focused.ownerDocument;
          stopWatching();
          if (document.activeElement === document.body) {
            receiptRef.current?.focus({ preventScroll: true });
          }
        });
        observer.observe(event.currentTarget, { childList: true, subtree: true });
      },
      onFocusOut(event) {
        handleEvent(handlers.onFocusOut, event);
        if (
          event.target.isConnected &&
          !event.currentTarget.contains(
            event.relatedTarget instanceof Node
              ? event.relatedTarget
              : event.currentTarget.ownerDocument.activeElement,
          )
        ) {
          stopWatching();
        }
      },
    } satisfies FocusHandlers,
  };
}
