"use client";

import { useEffect, useRef, useState } from "react";

export type UseCopyToClipboardOptions = {
  copiedDuration?: number;
};

export const useCopyToClipboard = ({
  copiedDuration = 3000,
}: UseCopyToClipboardOptions = {}) => {
  const [isCopied, setIsCopied] = useState<boolean>(false);
  const [isCopyFailed, setIsCopyFailed] = useState<boolean>(false);
  const copiedTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );
  const scopeGenerationRef = useRef(0);

  useEffect(
    () => () => {
      scopeGenerationRef.current += 1;
      if (copiedTimerRef.current === undefined) return;

      clearTimeout(copiedTimerRef.current);
      copiedTimerRef.current = undefined;
      setIsCopied(false);
      setIsCopyFailed(false);
    },
    [],
  );

  // One timer for both outcomes: a retry replaces the previous feedback.
  const settle = (succeeded: boolean) => {
    if (copiedTimerRef.current !== undefined) {
      clearTimeout(copiedTimerRef.current);
    }
    setIsCopied(succeeded);
    setIsCopyFailed(!succeeded);
    copiedTimerRef.current = setTimeout(() => {
      copiedTimerRef.current = undefined;
      setIsCopied(false);
      setIsCopyFailed(false);
    }, copiedDuration);
  };

  const copyToClipboard = (value: string) => {
    if (!value) return;

    const scopeGeneration = scopeGenerationRef.current;
    if (typeof navigator === "undefined" || !navigator.clipboard) {
      settle(false);
      return;
    }

    navigator.clipboard.writeText(value).then(
      () => {
        if (scopeGeneration !== scopeGenerationRef.current) return;

        settle(true);
      },
      () => {
        if (scopeGeneration !== scopeGenerationRef.current) return;

        settle(false);
      },
    );
  };

  return { isCopied, isCopyFailed, copyToClipboard };
};
