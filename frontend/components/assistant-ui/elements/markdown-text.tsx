"use client";

import { type FC, memo } from "react";

import type { TextMessagePartProps } from "@assistant-ui/react";

import { MarkdownBody } from "@/components/ocg/markdown/markdown-body";

/**
 * Markdown text for assistant-ui message parts.
 *
 * The assistant-ui runtime hands this component the message part itself, so
 * `text` and the part's stream `status` are the inputs — not an implicit
 * context lookup. Rendering is delegated to the shared Chat Markdown pipeline
 * so an assistant message and a reasoning trace parse identically.
 */
export type MarkdownTextProps = Pick<TextMessagePartProps, "text"> &
  Partial<Pick<TextMessagePartProps, "status">> & {
    /** Renders in the muted tone the reasoning disclosure uses. */
    inMuted?: boolean;
  };

const MarkdownTextImpl: FC<MarkdownTextProps> = ({ text, inMuted }) => (
  <MarkdownBody content={text ?? ""} inMuted={inMuted} className="aui-md" />
);

export const MarkdownText = memo(MarkdownTextImpl);
