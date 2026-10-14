"use client";

import type { FC } from "react";

// Simplified tool group for now
export const ToolGroup: FC<{ toolName: string; children?: React.ReactNode }> = ({
  toolName,
  children,
}) => {
  return (
    <div className="mb-4 rounded-lg border border-border bg-card p-4">
      <div className="mb-2 font-mono text-sm font-semibold">{toolName}</div>
      {children}
    </div>
  );
};

export const makeToolGroupUI = (toolName: string) => {
  function ToolGroupUI() {
    return <ToolGroup toolName={toolName} />;
  }

  ToolGroupUI.displayName = `ToolGroupUI(${toolName})`;
  return ToolGroupUI;
};
