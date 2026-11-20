import { ChevronDown } from "@lucide/solid";
import { createContext, createMemo, createSignal, createUniqueId, useContext } from "solid-js";
import { useI18n } from "@/components/ocg/i18n";
import { Collapsible, CollapsibleTrigger } from "@/components/ui/collapsible";
import { type ElementProps, splitProps, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

export const estimateReasoningTokens = (text: string) =>
  Math.ceil(new TextEncoder().encode(text).length / 3);
export type ReasoningRootProps = ElementProps<typeof Collapsible> & { streaming?: boolean };

type ReasoningDisclosure = { open: boolean; contentId: string };
const ReasoningContext = createContext<ReasoningDisclosure>({ open: false, contentId: "" });

export function ReasoningRoot(viewProps: ReasoningRootProps) {
  const [, props] = splitProps(viewProps, [
    "children",
    "class",
    "streaming",
    "defaultOpen",
    "open",
    "onOpenChange",
  ]);

  const [userOpen, setUserOpen] = createSignal<boolean | undefined>(viewProps.defaultOpen);
  const expanded = createMemo(() => viewProps.open ?? userOpen() ?? false);
  const contentId = createUniqueId();
  return (
    <ReasoningContext
      value={{
        get open() {
          return expanded();
        },
        contentId,
      }}
    >
      <Collapsible
        {...props}
        open={expanded()}
        onOpenChange={(nextOpen, details) => {
          setUserOpen(nextOpen);
          viewProps.onOpenChange?.(nextOpen, details);
        }}
        data-streaming={
          (viewProps.streaming === undefined ? false : viewProps.streaming) ? "true" : undefined
        }
        class={cn("my-1 w-full", viewProps.class)}
      >
        {viewProps.children}
      </Collapsible>
    </ReasoningContext>
  );
}

export function ReasoningTrigger(props: {
  active?: boolean;
  duration?: number;
  tokens?: number;
  children?: View;
}) {
  const i18n = useI18n();
  const seconds = createMemo(() =>
    props.duration === undefined
      ? "—"
      : new Intl.NumberFormat(i18n.locale, { maximumFractionDigits: 1 }).format(props.duration),
  );
  const count = createMemo(() =>
    new Intl.NumberFormat(i18n.locale).format(props.tokens === undefined ? 0 : props.tokens),
  );
  const contextValue1ie = useContext(ReasoningContext);
  const label = createMemo(
    () =>
      props.children ??
      ((props.active === undefined ? false : props.active)
        ? i18n.t("chat.thinking")
        : i18n.t("chat.thoughtFor", { seconds: seconds(), tokens: `≈${count()}` })),
  );
  return (
    <CollapsibleTrigger
      aria-controls={contextValue1ie.contentId}
      title={
        !(props.active === undefined ? false : props.active)
          ? i18n.t("chat.thinkingEstimateHint")
          : undefined
      }
      class="group/reasoning flex items-center gap-2 py-1.5 text-left text-xs text-muted-foreground hover:text-foreground"
    >
      <span
        class={
          (props.active === undefined ? false : props.active)
            ? "shimmer motion-reduce:animate-none"
            : undefined
        }
      >
        {label()}
      </span>
      <ChevronDown
        class="size-3 shrink-0 -rotate-90 transition-transform duration-[180ms] group-aria-expanded/reasoning:rotate-0 motion-reduce:transition-none"
        aria-hidden="true"
      />
    </CollapsibleTrigger>
  );
}

export function ReasoningContent(viewProps: ElementProps<"div"> & { streaming?: boolean }) {
  const [, props] = splitProps(viewProps, ["children", "streaming", "class"]);

  const i18n = useI18n();
  const contextValue290 = useContext(ReasoningContext);
  return (
    <div
      {...props}
      id={contextValue290.contentId}
      hidden={!contextValue290.open}
      role="region"
      aria-label={i18n.t("chat.thinking")}
      data-streaming={
        (viewProps.streaming === undefined ? false : viewProps.streaming) ? "true" : undefined
      }
      class={cn(
        "ocg-subtle-scrollbar max-h-[min(24rem,50dvh)] overflow-auto overscroll-contain border-l border-border py-2 pl-3 text-[13px] leading-6 text-muted-foreground",
        viewProps.class,
      )}
    >
      {contextValue290.open ? viewProps.children : null}
    </div>
  );
}
export function ReasoningText(props: { children: View }) {
  return <div>{props.children}</div>;
}
