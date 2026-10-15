import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function PageSurface({ children, className }: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className="h-full min-h-0 min-w-0 w-full flex-1 overflow-auto bg-background">
      <div className={cn("min-w-0 p-4 sm:p-6 lg:p-8", className)}>
        {children}
      </div>
    </div>
  );
}
