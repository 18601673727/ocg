"use client";

import { cn } from "@/lib/utils";
import { Collapsible, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ChevronDown } from "lucide-react";
import {
  createContext,
  useContext,
  useId,
  useState,
  type ComponentProps,
  type ReactNode,
} from "react";
import { useI18n } from "@/components/ocg/i18n";

export const ANIMATION_DURATION = 180;
export const estimateReasoningTokens = (text: string) =>
  Math.ceil(new TextEncoder().encode(text).length / 3);
export type ReasoningRootProps = ComponentProps<typeof Collapsible> & { streaming?: boolean };

type ReasoningDisclosure = { open: boolean; contentId: string };
const ReasoningContext = createContext<ReasoningDisclosure>({ open: false, contentId: "" });

export function ReasoningRoot({
  children,
  className,
  streaming = false,
  defaultOpen,
  open,
  onOpenChange,
  ...props
}: ReasoningRootProps) {
  const [userOpen, setUserOpen] = useState<boolean | undefined>(defaultOpen);
  const expanded = open ?? userOpen ?? false;
  const contentId = useId();
  return (
    <ReasoningContext.Provider value={{ open: expanded, contentId }}>
      <Collapsible
        {...props}
        open={expanded}
        onOpenChange={(nextOpen, details) => {
          setUserOpen(nextOpen);
          onOpenChange?.(nextOpen, details);
        }}
        data-streaming={streaming ? "true" : undefined}
        className={cn("my-1 w-full", className)}
      >
        {children}
      </Collapsible>
    </ReasoningContext.Provider>
  );
}

export function ReasoningTrigger({
  active = false,
  duration,
  tokens = 0,
  children,
}: {
  active?: boolean;
  duration?: number;
  tokens?: number;
  children?: ReactNode;
}) {
  const { t, locale } = useI18n();
  const seconds =
    duration === undefined
      ? "—"
      : new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(duration);
  const count = new Intl.NumberFormat(locale).format(tokens);
  const { contentId } = useContext(ReasoningContext);
  const label =
    children ??
    (active ? t("chat.thinking") : t("chat.thoughtFor", { seconds, tokens: `≈${count}` }));
  return (
    <CollapsibleTrigger
      aria-controls={contentId}
      title={!active ? t("chat.thinkingEstimateHint") : undefined}
      className="group/reasoning flex items-center gap-2 py-1.5 text-left text-xs text-muted-foreground hover:text-foreground"
    >
      <span className={active ? "shimmer motion-reduce:animate-none" : undefined}>{label}</span>
      <ChevronDown
        className="size-3 shrink-0 -rotate-90 transition-transform duration-[180ms] group-aria-expanded/reasoning:rotate-0 motion-reduce:transition-none"
        aria-hidden="true"
      />
    </CollapsibleTrigger>
  );
}

export function ReasoningContent({
  children,
  streaming = false,
  className,
  ...props
}: ComponentProps<"div"> & { streaming?: boolean }) {
  const { t } = useI18n();
  const { open, contentId } = useContext(ReasoningContext);
  return (
    <div
      {...props}
      id={contentId}
      hidden={!open}
      role="region"
      aria-label={t("chat.thinking")}
      data-streaming={streaming ? "true" : undefined}
      className={cn(
        "ocg-subtle-scrollbar max-h-[min(24rem,50dvh)] overflow-auto overscroll-contain border-l border-border py-2 pl-3 text-[13px] leading-6 text-muted-foreground",
        className,
      )}
    >
      {open ? children : null}
    </div>
  );
}
export function ReasoningText({ children }: { children: ReactNode }) {
  return <div>{children}</div>;
}
export function ReasoningFade({ children }: { children: ReactNode }) {
  return <div>{children}</div>;
}
export const reasoningVariants = {};
