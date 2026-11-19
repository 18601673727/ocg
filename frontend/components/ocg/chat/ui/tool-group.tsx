import { ChevronDownIcon } from "@lucide/solid";
import { cva, type VariantProps } from "class-variance-authority";
import { createMemo, createSignal } from "solid-js";
import { BoundedMessage } from "@/components/ocg/chat/bounded-message";
import { useI18n } from "@/components/ocg/i18n";
import { type CSSProperties, type ElementProps, splitProps, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import {
  CollapsibleRoot as Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "./collapsible-root";

const ANIMATION_DURATION = 200;

const toolGroupVariants = cva("aui-tool-group-root group/tool-group w-full", {
  variants: {
    variant: {
      outline: "rounded-lg border py-3",
      ghost: "",
      muted: "border-muted-foreground/30 bg-muted/30 rounded-lg border py-3",
    },
  },
  defaultVariants: { variant: "outline" },
});

export type ToolGroupRootProps = Omit<ElementProps<typeof Collapsible>, "open" | "onOpenChange"> &
  VariantProps<typeof toolGroupVariants> & {
    open?: boolean;
    onOpenChange?: (open: boolean) => void;
    defaultOpen?: boolean;
  };

function ToolGroupRoot(viewProps: ToolGroupRootProps) {
  const [, props] = splitProps(viewProps, [
    "class",
    "variant",
    "open",
    "onOpenChange",
    "defaultOpen",
    "children",
  ]);

  const [uncontrolledOpen, setUncontrolledOpen] = createSignal(
    viewProps.defaultOpen === undefined ? false : viewProps.defaultOpen,
  );

  const isControlled = createMemo(() => viewProps.open !== undefined);
  const isOpen = createMemo(() => (isControlled() ? viewProps.open : uncontrolledOpen()));

  const handleOpenChange = (open: boolean) => {
    if (!isControlled()) {
      setUncontrolledOpen(open);
    }
    viewProps.onOpenChange?.(open);
  };

  return (
    <Collapsible
      data-slot="tool-group-root"
      data-variant={viewProps.variant ?? "outline"}
      open={isOpen()}
      onOpenChange={handleOpenChange}
      class={cn(
        toolGroupVariants({ variant: viewProps.variant }),
        "group/tool-group-root",
        viewProps.class,
      )}
      style={
        {
          "--animation-duration": `${ANIMATION_DURATION}ms`,
        } as CSSProperties
      }
      {...props}
    >
      {viewProps.children}
    </Collapsible>
  );
}

function ToolGroupTrigger(
  viewProps: ElementProps<typeof CollapsibleTrigger> & {
    count: number;
    active?: boolean;
    summary?: View;
  },
) {
  const [, props] = splitProps(viewProps, ["count", "active", "summary", "class"]);

  const i18n = useI18n();
  const label = createMemo(() =>
    i18n.t(viewProps.count === 1 ? "aui.toolCalls.one" : "aui.toolCalls.other", {
      count: viewProps.count,
    }),
  );

  return (
    <CollapsibleTrigger
      data-slot="tool-group-trigger"
      class={cn(
        "aui-tool-group-trigger group/trigger flex origin-left items-center gap-2 text-sm transition-[color,scale] active:scale-[0.98]",
        "group-data-[variant=ghost]/tool-group-root:text-muted-foreground group-data-[variant=ghost]/tool-group-root:hover:text-foreground group-data-[variant=ghost]/tool-group-root:py-1.5",
        "group-data-[variant=outline]/tool-group-root:w-full group-data-[variant=outline]/tool-group-root:px-4",
        "group-data-[variant=muted]/tool-group-root:w-full group-data-[variant=muted]/tool-group-root:px-4",
        viewProps.class,
      )}
      {...props}
    >
      <span
        data-slot="tool-group-trigger-label"
        class={cn(
          "aui-tool-group-trigger-label-wrapper inline-block text-start text-xs leading-none font-medium",
          "group-data-[variant=ghost]/tool-group-root:font-normal",
          "group-data-[variant=outline]/tool-group-root:grow",
          "group-data-[variant=muted]/tool-group-root:grow",
          (viewProps.active === undefined ? false : viewProps.active) &&
            "shimmer motion-reduce:animate-none",
        )}
      >
        {viewProps.summary ?? label()}
      </span>
      <ChevronDownIcon
        data-slot="tool-group-trigger-chevron"
        class={cn(
          "aui-tool-group-trigger-chevron size-3 shrink-0",
          "transition-transform duration-(--animation-duration) ease-[cubic-bezier(0.32,0.72,0,1)] motion-reduce:transition-none",
          "-rotate-90",
          "group-data-open/trigger:rotate-0",
          "group-data-panel-open/trigger:rotate-0",
        )}
      />
    </CollapsibleTrigger>
  );
}

function ToolGroupContent(
  viewProps: ElementProps<typeof CollapsibleContent> & { streaming?: boolean },
) {
  const [, props] = splitProps(viewProps, ["class", "children", "streaming"]);

  const i18n = useI18n();
  return (
    <CollapsibleContent
      data-slot="tool-group-content"
      class={cn(
        "aui-tool-group-content relative overflow-hidden text-sm outline-none",
        viewProps.class,
      )}
      {...props}
    >
      <BoundedMessage
        streaming={viewProps.streaming === undefined ? false : viewProps.streaming}
        expandable={false}
        label={i18n.t("aui.toolHistory")}
        class="max-h-[min(24rem,50dvh)] overscroll-contain"
      >
        <div
          class={cn(
            "mt-2 flex flex-col gap-2",
            "group-data-[variant=ghost]/tool-group-root:mt-1 group-data-[variant=ghost]/tool-group-root:gap-1",
            "group-data-[variant=outline]/tool-group-root:mt-3 group-data-[variant=outline]/tool-group-root:border-t group-data-[variant=outline]/tool-group-root:px-4 group-data-[variant=outline]/tool-group-root:pt-3",
            "group-data-[variant=muted]/tool-group-root:mt-3 group-data-[variant=muted]/tool-group-root:border-t group-data-[variant=muted]/tool-group-root:px-4 group-data-[variant=muted]/tool-group-root:pt-3",
          )}
        >
          {viewProps.children}
        </div>
      </BoundedMessage>
    </CollapsibleContent>
  );
}

export { ToolGroupContent, ToolGroupRoot, ToolGroupTrigger, toolGroupVariants };
