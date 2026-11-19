import { cn } from "@/lib/utils";

export function OcgLogo(props: { compact?: boolean; class?: string }) {
  return (
    <span
      role="img"
      aria-label="OCG"
      data-ocg-logo=""
      class={cn(
        "inline-block align-middle leading-none font-bold tracking-tight",
        (props.compact === undefined ? false : props.compact) ? "text-lg" : "text-2xl",
        props.class,
      )}
    >
      OCG
    </span>
  );
}
