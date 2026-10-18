"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { boundLogEntries, createLogsLiveFixture, deriveRuntimeLogEntries, LOG_HISTORY_LIMIT, type LogEntry } from "./domain";
import type { RuntimeSnapshot } from "../runtime/runtime-types";
import type { ProjectId } from "../project/domain";
import { selectProject } from "../project/domain";
import { useI18n } from "../i18n";
import { LogsView } from "./logs-view";

const INITIAL_LIVE_ENTRIES = 3;
const EMPTY_LOG_ENTRIES: readonly LogEntry[] = [];

export function LogsSurface({ snapshot, projectId }: { snapshot: RuntimeSnapshot; projectId?: ProjectId }) {
  const { t } = useI18n();
  const baseEntries = useMemo(
    () => {
      const fixtureEntries = snapshot.scenario === "logs-live"
        ? createLogsLiveFixture(projectId)
        : deriveRuntimeLogEntries(snapshot, snapshot.sessions[0]?.id ?? "workspace");
      const provenance = snapshot.scenario === "logs-live" ? "demo" : "derived";
      const byId = new Map<string, LogEntry>(fixtureEntries.map((entry) => [entry.id, { ...entry, provenance }]));
      for (const entry of snapshot.logs ?? []) byId.set(entry.id, { ...entry, provenance: "snapshot" });
      return [...byId.values()];
    },
    [projectId, snapshot],
  );
  const live = snapshot.scenario === "logs-live";
  const runtimeEntries = useMemo<LogEntry[]>(() => (snapshot.logs ?? EMPTY_LOG_ENTRIES).map(entry => ({ ...entry, provenance: "snapshot" })), [snapshot.logs]);
  const nextIndex = useRef(live ? INITIAL_LIVE_ENTRIES : baseEntries.length);
  const [storedEntries, setStoredEntries] = useState<LogEntry[]>(() => boundLogEntries(baseEntries.slice(0, live ? INITIAL_LIVE_ENTRIES : baseEntries.length), LOG_HISTORY_LIMIT));
  const entries = useMemo(() => {
    const incoming = live ? runtimeEntries : baseEntries;
    const byId = new Map(storedEntries.map(entry => [entry.id, entry]));
    for (const entry of incoming) byId.set(entry.id, entry);
    return boundLogEntries([...byId.values()], LOG_HISTORY_LIMIT);
  }, [baseEntries, live, runtimeEntries, storedEntries]);
  useEffect(() => {
    if (!live) return;
    const timer = window.setInterval(() => {
      const next = baseEntries[nextIndex.current];
      if (!next) return;
      nextIndex.current += 1;
      setStoredEntries((current) => boundLogEntries([...current, next], LOG_HISTORY_LIMIT));
    }, 900);
    return () => window.clearInterval(timer);
  }, [baseEntries, live]);

  return <LogsView
    entries={entries}
    historyLimit={LOG_HISTORY_LIMIT}
    projectName={projectId ? selectProject(projectId).name : undefined}
    notice={<p className="mt-2 text-xs text-muted-foreground">{t(live ? "logs.demoNotice" : "logs.derivedNotice")}</p>}
  />;
}
