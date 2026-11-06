import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { PageTitle } from "./page-title";

export function ContentContainer({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "mx-auto flex w-full max-w-7xl flex-col gap-6 px-5 pt-4 pb-8 sm:px-8 lg:px-10",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function ContentHeader({
  title,
  description,
  actions,
}: {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-4">
      <div className="min-w-0 space-y-1.5">
        <PageTitle className="font-heading text-3xl font-semibold tracking-tight">
          {title}
        </PageTitle>
        {description && (
          <p className="max-w-prose text-sm leading-6 text-muted-foreground">{description}</p>
        )}
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
    </div>
  );
}
