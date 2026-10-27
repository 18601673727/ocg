"use client";

import type { ComponentProps } from "react";
import { ScrollArea as ScrollAreaPrimitive } from "@base-ui/react/scroll-area";
import { cn } from "cn";

type ScrollAreaPartProps = ComponentProps<"div"> & {
  [attribute: `data-${string}`]: string | number | boolean | undefined;
};

function ScrollArea({
  className,
  children,
  viewportProps,
  contentProps,
  ...props
}: ScrollAreaPrimitive.Root.Props & {
  viewportProps?: Omit<ScrollAreaPrimitive.Viewport.Props & ScrollAreaPartProps, "children">;
  contentProps?: Omit<ScrollAreaPrimitive.Content.Props & ScrollAreaPartProps, "children">;
}) {
  const { className: viewportClassName, ...viewportRest } = viewportProps ?? {};
  const { className: contentClassName, ...contentRest } = contentProps ?? {};

  return (
    <ScrollAreaPrimitive.Root
      data-slot="scroll-area"
      className={cn("min-h-0 min-w-0 overflow-hidden", className)}
      {...props}
    >
      <ScrollAreaPrimitive.Viewport
        data-slot="scroll-area-viewport"
        {...viewportRest}
        className={cn("h-full max-h-[inherit] w-full", viewportClassName)}
      >
        <ScrollAreaPrimitive.Content
          data-slot="scroll-area-content"
          {...contentRest}
          className={contentClassName}
        >
          {children}
        </ScrollAreaPrimitive.Content>
      </ScrollAreaPrimitive.Viewport>
      <ScrollAreaPrimitive.Scrollbar orientation="vertical" className="flex w-1.5 touch-none p-px">
        <ScrollAreaPrimitive.Thumb className="relative flex-1 rounded-full bg-muted-foreground/30" />
      </ScrollAreaPrimitive.Scrollbar>
    </ScrollAreaPrimitive.Root>
  );
}

export { ScrollArea };
