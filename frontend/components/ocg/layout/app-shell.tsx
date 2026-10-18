"use client";

import { useCallback, useEffect, useState } from "react";
import { ChatScreen } from "../chat/chat-screen";
import { OcgSidebar } from "../sidebar/ocg-sidebar";
import { OcgTopbar } from "../topbar/ocg-topbar";
import { JobsSurface } from "../job/jobs-surface";
import { ObservationsSurface } from "../dashboard/observations-surface";
import { LogsSurface } from "../dashboard/logs-surface";
import { SettingsSurface } from "../settings/control-settings-surface";
import { useOcgRuntime } from "../runtime/runtime-context";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import type { decodeHealth } from "../contracts";
import type { ComposerIntent } from "../composer/domain";
import { isJobIdentity, isReachableView, type WorkspaceView } from "./view-domain";
import { useI18n } from "../i18n";

export type { WorkspaceView } from "./view-domain";

type Health = ReturnType<typeof decodeHealth>;
function currentLocation() {
  const url = new URL(window.location.href);
  const requested = url.searchParams.get("view") ?? url.pathname.slice(1).replace(/\.html$/, "");
  const job = url.searchParams.get("job");
  return { view: isReachableView(requested) ? requested : "chat" as WorkspaceView, job: isJobIdentity(job) ? job : null };
}

export function AppShell({ client, health, sessionExpiresAtUs, signingOut, signOutError, onSignOut }: {
  client: HttpCanonicalControlClient;
  health: Health | null;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  signOutError: string | null;
  onSignOut: () => void;
}) {
  const runtime = useOcgRuntime();
  const { t } = useI18n();
  const [location, setLocation] = useState(currentLocation);
  const [sessionId, setSessionId] = useState("");
  const [collapsed, setCollapsed] = useState(false);
  const [mobile, setMobile] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [opening, setOpening] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const session = runtime.snapshot.sessions.find(session => session.id === sessionId);

  useEffect(() => {
    const pop = () => { setLocation(currentLocation()); setSessionId(""); };
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, []);

  useEffect(() => {
    if (!location.job || location.view !== "chat" || !runtime.client.openConversation) return;
    let alive = true;
    setOpening(true);
    void runtime.client.openConversation(location.job).then(id => {
      if (alive) { setSessionId(id); setError(null); }
    }).catch(cause => { if (alive) setError(cause instanceof Error ? cause.message : String(cause)); })
      .finally(() => { if (alive) setOpening(false); });
    return () => { alive = false; };
  }, [location.job, location.view, runtime.client, attempt]);

  // Runtime actions such as Retry also admit successors, so their addresses must recover on reload.
  useEffect(() => {
    if (location.view === "chat" && !opening && session?.sessionId) {
      window.history.replaceState(null, "", `/?job=${session.sessionId}`);
    }
  }, [location.view, opening, session?.sessionId]);

  const go = useCallback((view: WorkspaceView, job: string | null = null) => {
    if (!isReachableView(view)) return;
    const query = new URLSearchParams();
    if (job) query.set("job", job);
    const path = view === "chat" ? "/" : `/${view}`;
    const href = `${path}${query.size ? `?${query}` : ""}`;
    window.history.pushState(null, "", href);
    setLocation({ view, job });
    setMobile(false);
  }, []);

  const newChat = useCallback(async () => {
    const session = await runtime.createSession({ workType: "research" });
    setSessionId(session.id);
    setError(null);
    go("chat");
  }, [runtime, go]);

  async function intent(intent: ComposerIntent) {
    if (intent.kind !== "chat") throw new Error("Job launch is not available in this view.");
    if (!session) throw new Error("Create or open a conversation first.");
    await runtime.sendMessage(session.id, { content: intent.text, selection: intent.selection, images: intent.images, mode: intent.mode });
    const updated = runtime.client.getSnapshot().sessions.find(item => item.id === session.id);
    if (updated?.sessionId) {
      // Admission already installed the turn in the store; this only makes reload recover it.
      window.history.replaceState(null, "", `/?job=${updated.sessionId}`);
    }
  }

  const sidebar = <OcgSidebar sessions={[...runtime.snapshot.sessions].sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))}
    activeSessionId={sessionId} collapsed={collapsed} onToggle={() => setCollapsed(value => !value)}
    onSelect={id => { const selected = runtime.snapshot.sessions.find(session => session.id === id); setSessionId(id); setError(null); go("chat", selected?.sessionId ?? null); }}
    onNewChat={() => void newChat()} runtimeStatus={runtime.snapshot.status} runtimeAuthority="canonical"
    activeView={location.view} onNavigate={view => go(view)} onSettingsClick={() => go("settings")} />;

  return <div className="flex h-dvh min-h-0 w-full bg-background">
    <aside className="hidden shrink-0 lg:flex">{sidebar}</aside>
    {mobile && <div className="fixed inset-0 z-50 flex lg:hidden"><button type="button" aria-label="Close navigation" className="absolute inset-0 bg-black/40" onClick={() => setMobile(false)} /><aside className="relative flex h-full">{sidebar}</aside></div>}
    <div className="flex min-w-0 flex-1 flex-col">
      <OcgTopbar session={location.view === "chat" ? session : undefined} activeView={location.view} inspectorOpen={false} inspectorControls={false}
        onToggleInspector={() => {}} onOpenMobileSidebar={() => setMobile(true)} onOpenMobileInspector={() => {}} syncStatus={runtime.sync?.status} />
      {signOutError && <p role="alert" className="px-5 text-sm text-destructive">{signOutError}</p>}
      <main className="flex min-h-0 flex-1 flex-col overflow-auto">
        {location.view === "chat" ? <>
          {runtime.snapshot.status.state === "failed" && <p role="alert" className="px-5 py-2 text-sm text-destructive">{runtime.snapshot.status.detail} <button className="underline" onClick={() => void runtime.client.refreshConversations?.().catch(cause => setError(String(cause)))}>Refresh conversations</button></p>}
          {error && <p role="alert" className="px-5 py-2 text-sm text-destructive">{error} <button className="underline" onClick={() => setAttempt(value => value + 1)}>Retry loading</button></p>}
          {opening ? <p role="status" className="m-auto text-sm text-muted-foreground">Loading conversation…</p> : session ? <ChatScreen key={session.id} session={session} execution={null} runtimeStatus={runtime.snapshot.status} onComposerIntent={intent} onCancel={() => runtime.cancel(session.id)} /> : <div className="m-auto flex flex-col items-center gap-4 p-6"><h1 className="text-2xl font-medium">{t("chat.newThread")}</h1><button type="button" className="rounded-md border border-border px-4 py-2 text-sm" onClick={() => void newChat()}>{t("sidebar.newChat")}</button></div>}
        </> : null}
        {location.view === "jobs" && <JobsSurface client={client} active job={location.job} onJob={job => go("jobs", job)} onOpenChat={job => { setSessionId(""); go("chat", job); }} />}
        {(location.view === "usage" || location.view === "health") && <ObservationsSurface client={client} kind={location.view} health={health} onJob={job => go("jobs", job)} />}
        {location.view === "logs" && <LogsSurface client={client} />}
        {location.view === "settings" && <SettingsSurface client={client} health={health} sessionExpiresAtUs={sessionExpiresAtUs} signingOut={signingOut} onSignOut={onSignOut} />}
      </main>
    </div>
  </div>;
}
