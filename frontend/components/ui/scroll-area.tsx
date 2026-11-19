import type { JSX } from "@solidjs/web";
import { splitProps } from "@/lib/ui-state";

import { cn } from "@/lib/utils";

type Part = JSX.IntrinsicElements["div"] & {
  [attribute: `data-${string}`]: string | number | boolean | undefined;
};

export function ScrollArea(props: Part & { viewportProps?: Part; contentProps?: Part }) {
  const [local, rest] = splitProps(props, ["class", "children", "viewportProps", "contentProps"]);
  return (
    <div
      data-slot="scroll-area"
      {...rest}
      class={cn("min-h-0 min-w-0 overflow-hidden", local.class)}
    >
      <div
        {...local.viewportProps}
        data-slot="scroll-area-viewport"
        class={cn(
          "ocg-subtle-scrollbar h-full max-h-[inherit] w-full overflow-auto",
          local.viewportProps?.class,
        )}
      >
        <div {...local.contentProps} data-slot="scroll-area-content">
          {local.children}
        </div>
      </div>
    </div>
  );
}
