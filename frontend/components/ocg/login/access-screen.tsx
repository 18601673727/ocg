import { type View } from "@/lib/ui-state";
import { LanguageSwitcher, useI18n } from "../i18n";
import { OcgLogo } from "../primitives/ocg-logo";
import { WorkspaceIllustration } from "../primitives/workspace-illustration";

export function AccessScreen(props: { title: string; description: string; children: View }) {
  const i18n = useI18n();
  return (
    <div class="flex min-h-svh flex-col">
      <header class="flex h-16 shrink-0 items-center justify-between gap-4 border-b border-border bg-card px-5 sm:px-10">
        <div class="flex items-center gap-3">
          <OcgLogo />
          <span class="border-l border-border pl-3 text-sm text-muted-foreground">
            {i18n.t("settings.nav.workspace")}
          </span>
        </div>
        <LanguageSwitcher variant="select" />
      </header>
      <main class="mx-auto grid w-full max-w-5xl flex-1 content-center px-4 py-8 sm:px-8">
        <div class="grid overflow-hidden rounded-2xl border border-border bg-card shadow-sm md:grid-cols-2">
          <div class="flex items-center justify-center border-b border-border bg-primary/5 md:border-r md:border-b-0">
            <WorkspaceIllustration class="h-32 w-full sm:h-48 md:h-auto" />
          </div>
          <div class="flex min-w-0 flex-col justify-center gap-7 px-6 py-8 sm:p-10 md:min-h-96">
            <div>
              <h1 class="text-3xl leading-tight font-semibold tracking-tight">{props.title}</h1>
              <p class="mt-3 text-sm leading-6 text-muted-foreground">{props.description}</p>
            </div>
            {props.children}
          </div>
        </div>
      </main>
    </div>
  );
}
