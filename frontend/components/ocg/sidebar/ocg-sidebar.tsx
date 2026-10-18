"use client";

import { useCallback, useEffect, useRef } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Pill } from "@/components/ocg/primitives";
import type { HttpCanonicalControlClient } from "../runtime/canonical-client";
import { useControlPage } from "../runtime/use-control-page";
import {
  VIEW_AVAILABILITY,
  VIEW_TITLE,
  WORKSPACE_VIEWS,
  type WorkspaceView,
} from "../layout/view-domain";

type OcgSidebarProps = {
  /** Whether the drawer is open below the `lg` breakpoint. */
  mobileOpen: boolean;
  /** Whether the rail is hidden at and above the `lg` breakpoint. */
  collapsed: boolean;
  onCollapse: () => void;
  onCloseMobile: () => void;
  activeView: WorkspaceView;
  onNavigate: (view: WorkspaceView) => void;
  /** Jobs this browser tab opened, newest first. The Control Plane has no Job list. */
  openedJobs: readonly string[];
  activeJob: string | null;
  onOpenJob: (job: string) => void;
  onOpenChat: (job: string) => void;
  client: HttpCanonicalControlClient;
  signingOut: boolean;
  onSignOut: () => void;
};

/**
 * The shell's navigation. The 0.4.5 sidebar's conversation list, search and
 * delete flows have no 0.5 contract behind them, so they are not here: the
 * Jobs group lists only what this tab itself opened.
 */
export function OcgSidebar({
  mobileOpen,
  collapsed,
  onCollapse,
  onCloseMobile,
  activeView,
  onNavigate,
  openedJobs,
  activeJob,
  onOpenJob,
  onOpenChat,
  client,
  signingOut,
  onSignOut,
}: OcgSidebarProps) {
  const loadConversations = useCallback(
    async (before: string | undefined, signal: AbortSignal) => {
      const page = await client.readConversations({ before, limit: 50, signal });
      return { items: page.conversations, next: page.next_before };
    },
    [client],
  );
  const conversationsPage = useControlPage(loadConversations, true);

  const refreshConversations = useRef(conversationsPage.refresh);
  useEffect(() => {
    refreshConversations.current = conversationsPage.refresh;
  });

  useEffect(() => {
    return client.onConversationsChanged(() => {
      refreshConversations.current();
    });
  }, [client]);

  return (
    <>
      {mobileOpen ? (
        <button
          type="button"
          aria-label="Close navigation"
          className="fixed inset-0 z-30 bg-black/40 lg:hidden"
          onClick={onCloseMobile}
        />
      ) : null}
      <aside
        aria-label="Navigation"
        className={cn(
          "fixed inset-y-0 left-0 z-40 flex w-64 shrink-0 flex-col border-r border-sidebar-border bg-sidebar text-sidebar-foreground transition-transform",
          "lg:static lg:z-auto lg:translate-x-0",
          mobileOpen ? "translate-x-0" : "-translate-x-full",
          collapsed && "lg:hidden",
        )}
      >
        <div className="flex h-12 shrink-0 items-center justify-between px-3">
          <span className="font-heading text-lg tracking-tight">OCG</span>
          <Button
            variant="ghost"
            size="xs"
            className="tracking-normal normal-case"
            onClick={() => {
              onCollapse();
              onCloseMobile();
            }}
          >
            Hide
          </Button>
        </div>
        <nav aria-label="Views" className="flex flex-col gap-0.5 px-2 py-1">
          {WORKSPACE_VIEWS.map((view) => {
            const availability = VIEW_AVAILABILITY[view];
            const active = view === activeView;
            return (
              <button
                key={view}
                type="button"
                aria-current={active ? "page" : undefined}
                onClick={() => {
                  onNavigate(view);
                  onCloseMobile();
                }}
                className={cn(
                  "flex min-h-9 items-center justify-between gap-2 rounded-md px-2.5 text-left text-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring/30",
                  active
                    ? "bg-sidebar-accent font-medium text-sidebar-accent-foreground"
                    : "text-muted-foreground hover:bg-sidebar-accent/60 hover:text-sidebar-foreground",
                )}
              >
                {VIEW_TITLE[view]}
                {availability.available ? null : (
                  <Pill variant="quiet" className="text-[10px] normal-case">
                    unavailable
                  </Pill>
                )}
              </button>
            );
          })}
        </nav>
        <div className="ocg-subtle-scrollbar mt-2 flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto px-2">
          <div className="flex flex-col gap-1">
            <h2 className="px-2.5 text-xs font-semibold tracking-wider text-muted-foreground uppercase">
              Chats
            </h2>
            {conversationsPage.error ? (
              <p role="alert" className="px-2.5 text-xs text-destructive">
                {conversationsPage.error}
              </p>
            ) : null}
            {!conversationsPage.ready && conversationsPage.loading ? (
              <p className="px-2.5 text-xs text-muted-foreground">Loading chats…</p>
            ) : null}
            {conversationsPage.ready && conversationsPage.items.length === 0 ? (
              <p className="px-2.5 text-xs text-muted-foreground">No conversations yet.</p>
            ) : (
              <ul className="flex flex-col gap-0.5">
                {conversationsPage.items.map((entry) => {
                  const active = activeView === "chat" && activeJob === entry.tip_job;
                  return (
                    <li key={entry.root_job}>
                      <button
                        type="button"
                        aria-current={active ? "true" : undefined}
                        onClick={() => {
                          onOpenChat(entry.tip_job);
                          onCloseMobile();
                        }}
                        title={entry.title}
                        className={cn(
                          "flex min-h-8 w-full items-center rounded-md px-2.5 text-left text-xs outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring/30",
                          active
                            ? "bg-sidebar-accent font-medium text-sidebar-accent-foreground"
                            : "text-muted-foreground hover:bg-sidebar-accent/60 hover:text-sidebar-foreground",
                        )}
                      >
                        <span className="truncate">{entry.title || "Untitled chat"}</span>
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
            {conversationsPage.next ? (
              <Button
                variant="ghost"
                size="xs"
                className="mx-2.5 mt-1 justify-start text-xs text-muted-foreground hover:text-foreground"
                disabled={conversationsPage.loading}
                onClick={conversationsPage.loadMore}
              >
                {conversationsPage.loading ? "Loading…" : "Load more"}
              </Button>
            ) : null}
          </div>

          <div className="flex flex-col gap-1">
            <h2 className="px-2.5 text-xs font-semibold tracking-wider text-muted-foreground uppercase">
              Opened in this tab
            </h2>
            {openedJobs.length === 0 ? (
              <p className="px-2.5 text-xs text-muted-foreground">
                No Job opened yet. Submit one, or inspect a Job by its identity.
              </p>
            ) : (
              <ul className="flex flex-col gap-0.5">
                {openedJobs.map((job) => (
                  <li key={job}>
                    <button
                      type="button"
                      aria-current={activeView === "jobs" && job === activeJob ? "true" : undefined}
                      onClick={() => {
                        onOpenJob(job);
                        onCloseMobile();
                      }}
                      className={cn(
                        "flex min-h-8 w-full items-center rounded-md px-2.5 text-left font-mono text-xs outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring/30",
                        activeView === "jobs" && job === activeJob
                          ? "bg-sidebar-accent text-sidebar-accent-foreground"
                          : "text-muted-foreground hover:bg-sidebar-accent/60 hover:text-sidebar-foreground",
                      )}
                    >
                      {job}
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
        <div className="shrink-0 border-t border-sidebar-border p-2">
          <Button
            variant="outline"
            size="sm"
            className="w-full tracking-normal normal-case"
            disabled={signingOut}
            onClick={onSignOut}
          >
            {signingOut ? "Signing out…" : "Sign out"}
          </Button>
        </div>
      </aside>
    </>
  );
}
