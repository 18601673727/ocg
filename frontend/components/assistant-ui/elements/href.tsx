"use client";

import type { FC, ComponentPropsWithoutRef } from "react";
import { cn } from "@/lib/utils";

type HrefProps = ComponentPropsWithoutRef<"a">;

export const Href: FC<HrefProps> = ({ className, children, ...props }) => {
  return (
    <a
      className={cn(
        "text-blue-600 hover:underline dark:text-blue-400",
        className,
      )}
      target="_blank"
      rel="noreferrer"
      {...props}
    >
      {children}
    </a>
  );
};
