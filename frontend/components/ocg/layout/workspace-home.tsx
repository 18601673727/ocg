"use client";

import Link from "next/link";
import { ArrowRight, MessageSquare, Plus, SlidersHorizontal, Workflow } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useI18n } from "../i18n";
import { WorkspaceIllustration } from "../primitives/workspace-illustration";
import type { ChatSession } from "../types";

export function WorkspaceHome({
  sessions,
  onSelect,
  onNewChat,
  onNavigate,
}: {
  sessions: readonly ChatSession[];
  onSelect: (id: string) => void;
  onNewChat: () => void;
  onNavigate: (view: "jobs" | "settings") => void;
}) {
  const { t, locale } = useI18n();
  const recent = sessions.slice(0, 3);
  const dateFormat = new Intl.DateTimeFormat(locale, { month: "short", day: "numeric" });
  const links = [
    {
      view: "jobs",
      title: t("nav.jobExecution"),
      description: t("home.jobsDescription"),
      icon: Workflow,
    },
    {
      view: "settings",
      title: t("settings.title"),
      description: t("home.settingsDescription"),
      icon: SlidersHorizontal,
    },
  ] as const;

  return (
    <div className="ocg-subtle-scrollbar flex flex-1 flex-col overflow-y-auto px-5 pt-16 pb-8 sm:px-9 lg:px-12">
      <div className="mx-auto my-auto w-full max-w-4xl space-y-6">
        <section className="relative isolate overflow-hidden rounded-2xl border border-border bg-card bg-linear-to-r from-card via-card to-accent/70 shadow-sm">
          <WorkspaceIllustration className="pointer-events-none absolute inset-y-0 right-0 -z-10 hidden h-full w-[44%] sm:block" />
          <div className="px-6 py-9 sm:max-w-[60%] sm:px-10 sm:py-14">
            <h1 className="text-3xl font-semibold tracking-tight text-foreground sm:text-5xl">
              {t("settings.nav.workspace")}
            </h1>
            <p className="mt-3 max-w-sm text-sm leading-6 text-muted-foreground">
              {t("home.description")}
            </p>
            <Button
              onClick={onNewChat}
              className="mt-6 gap-2 px-4 text-sm tracking-normal normal-case"
            >
              <Plus className="size-4" aria-hidden="true" />
              {t("sidebar.newChat")}
            </Button>
          </div>
        </section>
        {recent.length > 0 ? (
          <section aria-labelledby="workspace-recent-heading">
            <h2 id="workspace-recent-heading" className="mb-3 text-sm font-semibold">
              {t("home.recent")}
            </h2>
            <ul className="divide-y divide-border rounded-xl border border-border bg-card">
              {recent.map((session) => {
                const updated = Date.parse(session.updatedAt);
                return (
                  <li key={session.id}>
                    <button
                      type="button"
                      onClick={() => onSelect(session.id)}
                      className="group flex w-full items-center gap-3 rounded-xl px-4 py-3.5 text-left hover:bg-accent/60 focus-visible:outline-2 focus-visible:outline-ring"
                    >
                      <MessageSquare
                        className="size-4 shrink-0 text-muted-foreground"
                        aria-hidden="true"
                      />
                      <span className="min-w-0 flex-1 truncate text-sm">
                        {session.title || t("sidebar.newChat")}
                      </span>
                      {Number.isFinite(updated) && (
                        <time
                          dateTime={session.updatedAt}
                          className="shrink-0 text-xs text-muted-foreground"
                        >
                          {dateFormat.format(updated)}
                        </time>
                      )}
                      <ArrowRight
                        className="size-3.5 shrink-0 text-muted-foreground group-hover:text-foreground"
                        aria-hidden="true"
                      />
                    </button>
                  </li>
                );
              })}
            </ul>
          </section>
        ) : (
          <nav aria-label={t("nav.workbench")} className="grid gap-3 sm:grid-cols-2">
            {links.map(({ view, title, description, icon: Icon }) => (
              <Link
                key={view}
                href={`/${view}`}
                prefetch={false}
                onNavigate={(event) => {
                  event.preventDefault();
                  onNavigate(view);
                }}
                className="group flex items-start gap-3 rounded-xl border border-border bg-card p-4 transition-colors hover:border-primary/45 focus-visible:outline-2 focus-visible:outline-ring sm:p-5"
              >
                <Icon className="mt-0.5 size-5 shrink-0 text-primary" aria-hidden="true" />
                <div className="min-w-0 flex-1">
                  <span className="block text-sm font-semibold">{title}</span>
                  <span className="mt-1 block text-sm leading-5 text-muted-foreground">
                    {description}
                  </span>
                </div>
                <ArrowRight
                  className="mt-1 size-3.5 shrink-0 text-muted-foreground group-hover:text-foreground"
                  aria-hidden="true"
                />
              </Link>
            ))}
          </nav>
        )}
      </div>
    </div>
  );
}
