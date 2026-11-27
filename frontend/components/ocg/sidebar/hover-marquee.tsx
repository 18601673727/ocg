import { createSignal } from "solid-js";
import type { CSSProperties, View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

const MARQUEE_SPEED_PX_PER_S = 40;
const MARQUEE_MIN_DURATION_S = 1.5;
const MARQUEE_MAX_DURATION_S = 12;

export function HoverMarquee(props: { children: View; class?: string }) {
  const [marquee, setMarquee] = createSignal<{ shift: number; duration: number } | null>(null);

  return (
    <span
      class={cn(
        "block min-w-0 overflow-hidden whitespace-nowrap",
        marquee() === null ? "text-ellipsis" : "text-clip",
        props.class,
      )}
      onMouseEnter={(event) => {
        if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
        const viewport = event.currentTarget;
        const shift = viewport.scrollWidth - viewport.clientWidth;
        if (shift < 1) return;
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
        class={marquee() === null ? undefined : "inline-block sidebar-title-marquee"}
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
