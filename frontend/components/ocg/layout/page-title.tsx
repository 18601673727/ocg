import { createContext, createEffect, createSignal, useContext } from "solid-js";
import { ariaState, bindElementRef, elementRef, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

export const PageTitleVisibilityContext = createContext<((visible: boolean) => void) | null>(null);

export function PageTitle(props: { children: View; class?: string }) {
  const onVisibilityChange = useContext(PageTitleVisibilityContext);
  const titleRef = elementRef<HTMLHeadingElement>(null);
  const [visible, setVisible] = createSignal(true);

  createEffect(
    () => [onVisibilityChange],
    () => {
      const title = titleRef.current;
      if (!title || !onVisibilityChange) return;
      onVisibilityChange(true);
      const observer = new IntersectionObserver(
        ([entry]) => {
          if (!entry) return;
          const next = entry.isIntersecting && entry.intersectionRatio >= 0.99;
          setVisible(next);
          onVisibilityChange(next);
        },
        { threshold: [0, 0.99] },
      );
      observer.observe(title);
      return () => observer.disconnect();
    },
  );

  return (
    <h1
      ref={bindElementRef(titleRef)}
      aria-hidden={ariaState(!visible())}
      class={cn(
        "transition-opacity duration-200 ease-out motion-reduce:transition-none",
        visible() ? "opacity-100" : "opacity-0",
        props.class,
      )}
    >
      {props.children}
    </h1>
  );
}
