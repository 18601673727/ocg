"use client";

import { forwardRef, memo, useCallback, useRef } from "react";
import { useScrollLock, type ReasoningMessagePartComponent } from "@assistant-ui/react";
import { MarkdownText } from "@/components/assistant-ui/elements/markdown-text";
import {
  ANIMATION_DURATION,
  estimateReasoningTokens,
  ReasoningRoot as ReasoningRootBase,
  ReasoningTrigger,
  ReasoningContent,
  ReasoningText,
  ReasoningFade,
  reasoningVariants,
  type ReasoningRootProps,
} from "./reasoning";

export type { ReasoningRootProps } from "./reasoning";

/** `ReasoningRoot` with the thread viewport scroll locked during disclosure animations. */
const ReasoningRoot = forwardRef<HTMLDivElement, Omit<ReasoningRootProps, "ref">>(
  function ReasoningRoot({ onAnimationStart, ...props }, ref) {
    const collapsibleRef = useRef<HTMLDivElement | null>(null);
    const lockScroll = useScrollLock(collapsibleRef, ANIMATION_DURATION);

    const handleAnimationStart = useCallback(
      (event: Parameters<NonNullable<ReasoningRootProps["onAnimationStart"]>>[0]) => {
        lockScroll();
        onAnimationStart?.(event);
      },
      [lockScroll, onAnimationStart],
    );

    const composedRef = useCallback(
      (node: HTMLDivElement | null) => {
        collapsibleRef.current = node;
        if (typeof ref === "function") {
          ref(node);
        } else if (ref) {
          ref.current = node;
        }
      },
      [ref],
    );

    return (
      <ReasoningRootBase ref={composedRef} onAnimationStart={handleAnimationStart} {...props} />
    );
  },
);

const ReasoningImpl: ReasoningMessagePartComponent = (props) => (
  <MarkdownText text={props.text} status={props.status} inMuted />
);

const Reasoning = Object.assign(memo(ReasoningImpl), {
  Root: ReasoningRoot,
  Trigger: ReasoningTrigger,
  Content: ReasoningContent,
  Text: ReasoningText,
  Fade: ReasoningFade,
});

Reasoning.displayName = "Reasoning";
Reasoning.Root = ReasoningRoot;
Reasoning.Trigger = ReasoningTrigger;
Reasoning.Content = ReasoningContent;
Reasoning.Text = ReasoningText;
Reasoning.Fade = ReasoningFade;

export {
  estimateReasoningTokens,
  Reasoning,
  ReasoningRoot,
  ReasoningTrigger,
  ReasoningContent,
  ReasoningText,
  ReasoningFade,
  reasoningVariants,
};
