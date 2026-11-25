import type { JSX } from "@solidjs/web";
import { cn } from "cn";
import { splitProps } from "@/lib/ui-state";

function Textarea(props: JSX.IntrinsicElements["textarea"]) {
  const [local, rest] = splitProps(props, ["class"]);
  return (
    <textarea
      data-slot="textarea"
      class={cn(
        "flex field-sizing-content min-h-16 w-full resize-none rounded-md border border-input bg-transparent px-3 py-3 text-base transition-[color,border-color] outline-none text-foreground placeholder:text-muted-foreground/60 placeholder:italic focus-visible:border-ring disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-destructive md:text-sm dark:aria-invalid:border-destructive/50",
        local.class,
      )}
      {...rest}
    />
  );
}

export { Textarea };
