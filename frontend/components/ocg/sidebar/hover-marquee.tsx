import { createSignal } from "solid-js";
import { bindElementRef, type CSSProperties, elementRef, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

const MARQUEE_FADE_PX = 16;
const MARQUEE_SPEED_PX_PER_S = 40;
const MARQUEE_MIN_DURATION_S = 1.5;
const MARQUEE_MAX_DURATION_S = 12;

export function HoverMarquee(props: { children: View; class?: string }) {
  const viewportRef = elementRef<HTMLSpanElement>(null);
  const [marquee, setMarquee] = createSignal<{ shift: number; duration: number } | null>(null);

  return (
    <span
      ref={bindElementRef(viewportRef)}
      class={cn(
        "block min-w-0 overflow-hidden whitespace-nowrap [mask-image:linear-gradient(to_right,black_calc(100%-1rem),transparent)]",
        props.class,
      )}
      onMouseEnter={() => {
        const viewport = viewportRef.current;
        if (!viewport || window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
        const overflow = viewport.scrollWidth - viewport.clientWidth;
        if (overflow < 1) {
          setMarquee(null);
          return;
        }
        const shift = overflow + MARQUEE_FADE_PX;
        setMarquee({
          shift,
          duration: Math.min(
            MARQUEE_MAX_DURATION_S,
            Math.max(MARQUEE_MIN_DURATION_S, shift / MARQUEE_SPEED_PX_PER_S),
          ),
        });
      }}
      onMouseLeave={() => setMarquee(null)}
    >
      <span
        class={cn("inline-block", marquee() !== null && "sidebar-title-marquee")}
        style={
          marquee() === null
            ? undefined
            : ({
                "--marquee-shift": `-${marquee()!.shift}px`,
                "--marquee-duration": `${marquee()!.duration}s`,
              } as CSSProperties)
        }
      >
        {props.children}
      </span>
    </span>
  );
}
