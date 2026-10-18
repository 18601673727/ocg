"use client";

import type { FC, ComponentPropsWithoutRef } from "react";
import { cn } from "@/lib/utils";

type SurfaceProps = ComponentPropsWithoutRef<"div">;

export const Surface: FC<SurfaceProps> = ({ className, children, ...props }) => {
  return (
    <div
      className={cn(
        "rounded-lg border border-border bg-card text-card-foreground shadow-sm",
        className,
      )}
      {...props}
    >
      {children}
    </div>
  );
};

export const SurfaceContent: FC<SurfaceProps> = ({
  className,
  children,
  ...props
}) => {
  return (
    <div className={cn("p-6", className)} {...props}>
      {children}
    </div>
  );
};

export const SurfaceHeader: FC<SurfaceProps> = ({
  className,
  children,
  ...props
}) => {
  return (
    <div className={cn("border-b border-border p-6", className)} {...props}>
      {children}
    </div>
  );
};

export const SurfaceFooter: FC<SurfaceProps> = ({
  className,
  children,
  ...props
}) => {
  return (
    <div className={cn("border-t border-border p-6", className)} {...props}>
      {children}
    </div>
  );
};
