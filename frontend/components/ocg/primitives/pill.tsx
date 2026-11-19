import { BORDER_RADIUS, TYPOGRAPHY_SCALE } from "@/lib/design-tokens";
import { type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { StatusDot } from "./status-dot";
import { TEXT_TONE, TONE_CLASS, type Tone } from "./tone";

export function Pill(props: {
  children: View;
  tone?: Tone;
  variant?: "solid" | "quiet";
  dot?: boolean;
  pulse?: boolean;
  title?: string;
  class?: string;
}) {
  return (
    <span
      title={props.title}
      class={cn(
        "inline-flex shrink-0 items-center gap-1",
        BORDER_RADIUS.interactive,
        "border px-1.5 py-0.5",
        TYPOGRAPHY_SCALE.caption,
        "font-medium capitalize",
        (props.variant === undefined ? "solid" : props.variant) === "solid"
          ? TONE_CLASS[props.tone === undefined ? "slate" : props.tone]
          : cn(
              "border-border bg-muted/40",
              (props.tone === undefined ? "slate" : props.tone) === "slate"
                ? "text-muted-foreground"
                : TEXT_TONE[props.tone === undefined ? "slate" : props.tone],
            ),
        props.class,
      )}
    >
      {props.dot && (
        <StatusDot tone={props.tone === undefined ? "slate" : props.tone} pulse={props.pulse} />
      )}
      {props.children}
    </span>
  );
}
