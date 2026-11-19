import { type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

export function PageSurface(props: { children: View; class?: string }) {
  return (
    <div class="h-full min-h-0 min-w-0 w-full flex-1 bg-background">
      <div class={cn("min-w-0 p-4 sm:p-6 lg:p-8", props.class)}>{props.children}</div>
    </div>
  );
}
