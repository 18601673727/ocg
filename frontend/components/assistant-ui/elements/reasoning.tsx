"use client";

import { cn } from "@/lib/utils";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ChevronDown } from "lucide-react";
import { useState, type ComponentProps, type ReactNode } from "react";
import { useI18n } from "@/components/ocg/i18n";

export const ANIMATION_DURATION = 180;
export const estimateReasoningTokens = (text: string) => Math.ceil(new TextEncoder().encode(text).length / 3);
export type ReasoningRootProps = ComponentProps<typeof Collapsible> & { streaming?: boolean; latest?: boolean };

export function ReasoningRoot({ children, className, streaming = false, latest = true, defaultOpen, open, onOpenChange, ...props }: ReasoningRootProps) {
  const [userOpen, setUserOpen] = useState<boolean | undefined>(defaultOpen);
  return <Collapsible {...props} open={open ?? userOpen ?? latest} onOpenChange={(nextOpen, details) => {
    setUserOpen(nextOpen);
    onOpenChange?.(nextOpen, details);
  }} data-streaming={streaming ? "true" : undefined} className={cn("my-2 rounded-md border border-border/60 bg-muted/20", className)}>{children}</Collapsible>;
}

export function ReasoningTrigger({ active = false, duration, tokens = 0, children }: { active?: boolean; duration?: number; tokens?: number; children?: ReactNode }) {
  const { t, locale } = useI18n();
  const seconds = duration === undefined ? "—" : new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(duration);
  const count = new Intl.NumberFormat(locale).format(tokens);
  return <CollapsibleTrigger title={!active ? t("chat.thinkingEstimateHint") : undefined} className="group/reasoning flex w-full items-center gap-2 px-3 py-2 text-left text-xs font-medium text-muted-foreground"><ChevronDown className={cn("size-3.5 shrink-0 -rotate-90 transition-transform duration-[180ms] group-aria-expanded/reasoning:rotate-0 motion-reduce:transition-none", active && "animate-pulse")} aria-hidden="true" />{children ?? (active ? t("chat.thinking") : t("chat.thoughtFor", { seconds, tokens: `≈${count}` }))}</CollapsibleTrigger>;
}

export function ReasoningContent({ children, streaming = false, ...props }: ComponentProps<typeof CollapsibleContent> & { streaming?: boolean }) {
  const { t } = useI18n();
  return <CollapsibleContent {...props} data-streaming={streaming ? "true" : undefined}>
    <div role="region" aria-label={t("chat.thinking")} className="border-b border-border/60 px-3 py-2 text-[13px] leading-6 text-muted-foreground">{children}</div>
  </CollapsibleContent>;
}
export function ReasoningText({ children }: { children: ReactNode }) { return <div>{children}</div>; }
export function ReasoningFade({ children }: { children: ReactNode }) { return <div>{children}</div>; }
export const reasoningVariants = {};
