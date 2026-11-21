import { onCleanup } from "solid-js";

export function overlayDialog(trigger?: HTMLElement) {
  return trigger?.closest<HTMLDialogElement>("dialog:modal") ?? undefined;
}

export function positionOverlay(
  element: HTMLElement,
  trigger: HTMLElement,
  place: (anchor: DOMRect, box: DOMRect, bottom: number) => { left: number; top: number },
) {
  const dialog = overlayDialog(trigger);
  element.style.position = dialog ? "absolute" : "fixed";
  const position = () => {
    const frame = dialog?.getBoundingClientRect();
    const originX = (frame?.left ?? 0) + (dialog?.clientLeft ?? 0);
    const originY = (frame?.top ?? 0) + (dialog?.clientTop ?? 0);
    const right = Math.min(window.innerWidth, originX + (dialog?.clientWidth ?? window.innerWidth));
    const bottom = Math.min(
      window.innerHeight,
      originY + (dialog?.clientHeight ?? window.innerHeight),
    );
    const x = Math.max(0, originX) + 4;
    const y = Math.max(0, originY) + 4;
    if (dialog) {
      element.style.maxWidth = `min(100%, ${Math.max(0, right - x - 4)}px)`;
      element.style.maxHeight = `${Math.max(0, bottom - y - 4)}px`;
      element.style.overflowY = "auto";
    }
    const box = element.getBoundingClientRect();
    const { left, top } = place(trigger.getBoundingClientRect(), box, bottom);
    element.style.left = `${Math.max(x, Math.min(left, right - box.width - 4)) - originX + (dialog?.scrollLeft ?? 0)}px`;
    element.style.top = `${Math.max(y, Math.min(top, bottom - box.height - 4)) - originY + (dialog?.scrollTop ?? 0)}px`;
  };
  const observer = new ResizeObserver(position);
  observer.observe(element);
  if (dialog) observer.observe(dialog);
  const frame = requestAnimationFrame(position);
  window.addEventListener("resize", position);
  window.addEventListener("scroll", position, true);
  onCleanup(() => {
    cancelAnimationFrame(frame);
    observer.disconnect();
    window.removeEventListener("resize", position);
    window.removeEventListener("scroll", position, true);
  });
}
