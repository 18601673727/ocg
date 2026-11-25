import type { JSX } from "@solidjs/web";
import { cn } from "cn";
import { splitProps } from "@/lib/ui-state";

function Input(props: JSX.IntrinsicElements["input"]) {
  const [local, rest] = splitProps(props, ["class"]);
  return (
    <input
      data-slot="input"
      class={cn(
        "h-10 w-full min-w-0 rounded-md border border-input bg-transparent px-3 py-1 text-base transition-[color,border-color] outline-none file:inline-flex file:h-7 file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-foreground text-foreground placeholder:text-muted-foreground/60 placeholder:italic focus-visible:border-ring disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-destructive md:text-sm dark:aria-invalid:border-destructive/50",
        local.class,
      )}
      {...rest}
    />
  );
}

export { Input };
