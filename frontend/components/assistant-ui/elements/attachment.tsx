"use client";

import type { FC } from "react";
import { AttachmentPrimitive } from "@assistant-ui/react";
import { FileIcon, XIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export const Attachment: FC = () => {
  return (
    <AttachmentPrimitive.Root className="relative inline-flex items-center gap-2 rounded-lg border border-border bg-card px-3 py-2 text-sm">
      <div className="h-8 w-8 flex-shrink-0 overflow-hidden rounded">
        <FileIcon className="h-full w-full text-muted-foreground" />
      </div>
      <div className="flex flex-col overflow-hidden">
        <AttachmentPrimitive.Name />
      </div>
      <AttachmentPrimitive.Remove asChild>
        <button
          type="button"
          className={cn(
            "ml-auto inline-flex h-6 w-6 items-center justify-center whitespace-nowrap rounded-md text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground disabled:pointer-events-none disabled:opacity-50"
          )}
          aria-label="Remove attachment"
        >
          <XIcon className="h-4 w-4" />
        </button>
      </AttachmentPrimitive.Remove>
    </AttachmentPrimitive.Root>
  );
};
