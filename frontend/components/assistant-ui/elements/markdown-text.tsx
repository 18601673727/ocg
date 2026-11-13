"use client";

import { type FC, memo } from "react";

import type { TextMessagePartProps } from "@assistant-ui/react";

import { MarkdownBody } from "@/components/ocg/markdown/markdown-body";
import type { FileSnapshot } from "@/components/ocg/markdown/source-snapshot";

export type MarkdownTextProps = Pick<TextMessagePartProps, "text"> &
  Partial<Pick<TextMessagePartProps, "status">> & {
    inMuted?: boolean;
    sourceSnapshots?: readonly FileSnapshot[];
  };

const MarkdownTextImpl: FC<MarkdownTextProps> = ({ text, inMuted, sourceSnapshots }) => (
  <MarkdownBody
    content={text ?? ""}
    inMuted={inMuted}
    sourceSnapshots={sourceSnapshots}
    className="aui-md"
  />
);

export const MarkdownText = memo(MarkdownTextImpl);
