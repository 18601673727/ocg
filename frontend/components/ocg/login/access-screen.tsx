import type { ReactNode } from "react";

import { LanguageSwitcher, useI18n } from "../i18n";
import { OcgLogo } from "../primitives/ocg-logo";
import { WorkspaceIllustration } from "../primitives/workspace-illustration";

export function AccessScreen({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  const { t } = useI18n();
  return (
    <div className="flex min-h-svh flex-col">
      <header className="flex h-16 shrink-0 items-center justify-between gap-4 border-b border-border bg-card px-5 sm:px-10">
        <div className="flex items-center gap-3">
          <OcgLogo />
          <span className="border-l border-border pl-3 text-sm text-muted-foreground">
            {t("settings.nav.workspace")}
          </span>
        </div>
        <LanguageSwitcher variant="select" />
      </header>
      <main className="mx-auto grid w-full max-w-5xl flex-1 content-center px-4 py-8 sm:px-8">
        <div className="grid overflow-hidden rounded-2xl border border-border bg-card shadow-sm md:grid-cols-2">
          <div className="flex items-center justify-center border-b border-border bg-primary/5 md:border-r md:border-b-0">
            <WorkspaceIllustration className="h-32 w-full sm:h-48 md:h-auto" />
          </div>
          <div className="flex min-w-0 flex-col justify-center gap-7 px-6 py-8 sm:p-10 md:min-h-96">
            <div>
              <h1 className="text-3xl leading-tight font-semibold tracking-tight">{title}</h1>
              <p className="mt-3 text-sm leading-6 text-muted-foreground">{description}</p>
            </div>
            {children}
          </div>
        </div>
      </main>
    </div>
  );
}
