"use client";

import type { FC } from "react";

// Simplified tool fallback for now
export const ToolFallback: FC<{ toolName?: string; status?: string }> = ({
  toolName,
  status,
}) => {
  return (
    <div className="mb-4 rounded-lg border border-border bg-muted/50 p-4">
      <div className="mb-2 flex items-center gap-2">
        <span className="font-mono text-sm font-semibold">
          {toolName || "Tool"}
        </span>
        {status === "running" && (
          <span className="text-xs text-muted-foreground">Running...</span>
        )}
      </div>
    </div>
  );
};
