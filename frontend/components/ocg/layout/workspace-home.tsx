import { ArrowRight, MessageSquare, Plus, SlidersHorizontal, Workflow } from "@lucide/solid";
import { createMemo } from "solid-js";
import { WorkspaceLink } from "@/components/ocg/layout/workspace-link";
import { Button } from "@/components/ui/button";
import { useI18n } from "../i18n";
import { WorkspaceIllustration } from "../primitives/workspace-illustration";
import type { ChatSession } from "../types";

export function WorkspaceHome(props: {
  sessions: readonly ChatSession[];
  onSelect: (id: string) => void;
  onNewChat: () => void;
  onNavigate: (view: "jobs" | "settings") => void;
}) {
  const i18n = useI18n();
  const recent = createMemo(() => props.sessions.slice(0, 3));
  const dateFormat = new Intl.DateTimeFormat(i18n.locale, { month: "short", day: "numeric" });
  const links = [
    {
      view: "jobs",
      title: i18n.t("nav.jobExecution"),
      description: i18n.t("home.jobsDescription"),
      icon: Workflow,
    },
    {
      view: "settings",
      title: i18n.t("settings.title"),
      description: i18n.t("home.settingsDescription"),
      icon: SlidersHorizontal,
    },
  ] as const;

  return (
    <div class="ocg-subtle-scrollbar flex flex-1 flex-col overflow-y-auto px-5 pt-16 pb-8 sm:px-9 lg:px-12">
      <div class="mx-auto my-auto w-full max-w-4xl space-y-6">
        <section class="relative isolate overflow-hidden rounded-2xl border border-border bg-card bg-linear-to-r from-card via-card to-accent/70 shadow-sm">
          <WorkspaceIllustration class="pointer-events-none absolute inset-y-0 right-0 -z-10 hidden h-full w-[44%] sm:block" />
          <div class="px-6 py-9 sm:max-w-[60%] sm:px-10 sm:py-14">
            <h1 class="text-3xl font-semibold tracking-tight text-foreground sm:text-5xl">
              {i18n.t("settings.nav.workspace")}
            </h1>
            <p class="mt-3 max-w-sm text-sm leading-6 text-muted-foreground">
              {i18n.t("home.description")}
            </p>
            <Button
              onClick={props.onNewChat}
              class="mt-6 gap-2 px-4 text-sm tracking-normal normal-case"
            >
              <Plus class="size-4" aria-hidden="true" />
              {i18n.t("sidebar.newChat")}
            </Button>
          </div>
        </section>
        {recent().length > 0 ? (
          <section aria-labelledby="workspace-recent-heading">
            <h2 id="workspace-recent-heading" class="mb-3 text-sm font-semibold">
              {i18n.t("home.recent")}
            </h2>
            <ul class="divide-y divide-border rounded-xl border border-border bg-card">
              {recent().map((session) => {
                const updated = Date.parse(session.updatedAt);
                return (
                  <li>
                    <button
                      type="button"
                      onClick={() => props.onSelect(session.id)}
                      class="group flex w-full items-center gap-3 rounded-xl px-4 py-3.5 text-left hover:bg-accent/60 focus-visible:outline-2 focus-visible:outline-ring"
                    >
                      <MessageSquare
                        class="size-4 shrink-0 text-muted-foreground"
                        aria-hidden="true"
                      />
                      <span class="min-w-0 flex-1 truncate text-sm">
                        {session.title || i18n.t("sidebar.newChat")}
                      </span>
                      {Number.isFinite(updated) && (
                        <time
                          datetime={session.updatedAt}
                          class="shrink-0 text-xs text-muted-foreground"
                        >
                          {dateFormat.format(updated)}
                        </time>
                      )}
                      <ArrowRight
                        class="size-3.5 shrink-0 text-muted-foreground group-hover:text-foreground"
                        aria-hidden="true"
                      />
                    </button>
                  </li>
                );
              })}
            </ul>
          </section>
        ) : (
          <nav aria-label={i18n.t("nav.workbench")} class="grid gap-3 sm:grid-cols-2">
            {links.map(({ view, title, description, icon: Icon }) => (
              <WorkspaceLink
                href={`/${view}`}
                onNavigate={() => props.onNavigate(view)}
                class="group flex items-start gap-3 rounded-xl border border-border bg-card p-4 transition-colors hover:border-primary/45 focus-visible:outline-2 focus-visible:outline-ring sm:p-5"
              >
                <Icon class="mt-0.5 size-5 shrink-0 text-primary" aria-hidden="true" />
                <div class="min-w-0 flex-1">
                  <span class="block text-sm font-semibold">{title}</span>
                  <span class="mt-1 block text-sm leading-5 text-muted-foreground">
                    {description}
                  </span>
                </div>
                <ArrowRight
                  class="mt-1 size-3.5 shrink-0 text-muted-foreground group-hover:text-foreground"
                  aria-hidden="true"
                />
              </WorkspaceLink>
            ))}
          </nav>
        )}
      </div>
    </div>
  );
}
