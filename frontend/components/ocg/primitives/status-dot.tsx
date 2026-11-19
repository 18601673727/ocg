import { cn } from "@/lib/utils";
import { DOT_TONE, type Tone } from "./tone";

export function StatusDot(props: {
  tone: Tone;
  pulse?: boolean;
  size?: "sm" | "md";
  title?: string;
  class?: string;
}) {
  return (
    <span
      class={cn(
        "shrink-0 rounded-full",
        (props.size === undefined ? "sm" : props.size) === "sm" ? "size-1.5" : "size-2",
        DOT_TONE[props.tone],
        (props.pulse === undefined ? false : props.pulse) && "animate-pulse",
        props.class,
      )}
      title={props.title}
      aria-hidden="true"
    />
  );
}
