import { type ElementProps, splitProps } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
export function Skeleton(viewProps: ElementProps<"div">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return <div class={cn("animate-pulse rounded-md bg-muted", viewProps.class)} {...props} />;
}
