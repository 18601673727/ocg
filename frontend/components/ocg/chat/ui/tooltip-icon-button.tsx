import { type ComponentProps } from "solid-js";
import { Button } from "@/components/ui/button";
import { splitProps } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

export type TooltipIconButtonProps = ComponentProps<typeof Button> & {
  tooltip: string;
  side?: "top" | "bottom" | "left" | "right";
};

export function TooltipIconButton(props: TooltipIconButtonProps) {
  const [local, rest] = splitProps(props, ["tooltip", "side", "class", "children"]);
  return (
    <Button
      variant="ghost"
      size="icon"
      title={local.tooltip}
      aria-label={local.tooltip}
      {...rest}
      class={cn("aui-button-icon size-6 p-1 active:scale-90", local.class)}
    >
      {local.children}
    </Button>
  );
}

export function FloatingIconButton(props: TooltipIconButtonProps) {
  return (
    <TooltipIconButton
      {...props}
      variant="outline"
      class={cn(
        "dark:border-border dark:bg-background dark:hover:bg-accent rounded-full p-4",
        props.class,
      )}
    />
  );
}
