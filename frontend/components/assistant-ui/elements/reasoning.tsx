"use client";

import { cn } from "@/lib/utils";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ChevronDown } from "lucide-react";
import type { ComponentProps, ReactNode } from "react";

export const ANIMATION_DURATION = 180;
export type ReasoningRootProps = ComponentProps<typeof Collapsible> & { streaming?: boolean };

export function ReasoningRoot({ children, className, streaming = false, ...props }: ReasoningRootProps) {
  return <Collapsible {...props} data-streaming={streaming ? "true" : undefined} defaultOpen={props.open ?? true} className={cn("my-2 rounded-md border border-border/60 bg-muted/20", className)}>{children}</Collapsible>;
}

export function ReasoningTrigger({ active = false, duration, children }: { active?: boolean; duration?: number; children?: ReactNode }) {
  return <CollapsibleTrigger className="flex w-full items-center gap-2 px-3 py-2 text-left text-xs font-medium text-muted-foreground"><ChevronDown className={cn("size-3.5 transition-transform", active && "animate-pulse")} />{children ?? "Thinking"}{duration != null && <span className="ml-auto font-normal">{duration}s</span>}</CollapsibleTrigger>;
}

export function ReasoningContent({ children, ...props }: ComponentProps<typeof CollapsibleContent>) {
  return <CollapsibleContent {...props}><div className="border-t border-border/60 px-3 py-2 text-[13px] leading-6 text-muted-foreground">{children}</div></CollapsibleContent>;
}
export function ReasoningText({ children }: { children: ReactNode }) { return <div>{children}</div>; }
export function ReasoningFade({ children }: { children: ReactNode }) { return <div>{children}</div>; }
export const reasoningVariants = {};
