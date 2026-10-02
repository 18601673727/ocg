"use client";

/**
 * Single Project context for the shell.
 *
 * Only the active project ID is persisted, under a versioned key. A URL-provided
 * `initialProjectId` takes precedence over stored state. No global state library
 * is involved.
 */

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import type { ProjectId, ProjectSummary } from "./domain";
import { DEFAULT_PROJECT_ID, isProjectId, resolveProjectId, selectProject } from "./domain";
import { projectSessionIds } from "./fixtures";
import { createHttpCanonicalControlClient } from "../runtime/canonical-client";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useOcgControlUrl } from "../profile/control-url";

export const ACTIVE_PROJECT_STORAGE_KEY = "ocg.project.active.v1";

function readStoredProjectId(): ProjectId | null {
  if (typeof window === "undefined") return null;
  try {
    const stored = window.localStorage.getItem(ACTIVE_PROJECT_STORAGE_KEY);
    return isProjectId(stored) ? stored : null;
  } catch {
    return null;
  }
}

function writeStoredProjectId(id: ProjectId): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(ACTIVE_PROJECT_STORAGE_KEY, id);
  } catch {
    // Persistence is best-effort; the in-memory selection still applies.
  }
}

function projectName(root: string, projectId: string): string {
  const normalized = root.replace(/[\\/]+$/, "");
  const name = normalized.split(/[\\/]/).pop();
  return name || projectId;
}

export type ProjectContextValue = {
  activeProjectId: ProjectId;
  activeProject: ProjectSummary;
  projects: readonly ProjectSummary[];
  setActiveProject: (id: ProjectId) => void;
  /** Register a newly created session with a project (defaults to the active one). */
  registerProjectSession: (sessionId: string, projectId?: ProjectId) => void;
  /** Mock session IDs for the active project plus registered sessions. */
  activeProjectSessionIds: readonly string[];
  historyStatus: "loading" | "ready" | "failed";
  historyError: string | null;
  retryHistory: () => void;
};

const ProjectContext = createContext<ProjectContextValue | null>(null);

export function ProjectProvider({
  initialProjectId,
  children,
}: {
  initialProjectId?: ProjectId;
  children: ReactNode;
}) {
  const [activeProjectId, setActiveProjectIdState] = useState<ProjectId>(() =>
    initialProjectId ? resolveProjectId(initialProjectId) : readStoredProjectId() ?? DEFAULT_PROJECT_ID,
  );
  const [registeredSessionIds, setRegisteredSessionIds] = useState<
    Partial<Record<string, readonly string[]>>
  >({});
  const [projects, setProjects] = useState<readonly ProjectSummary[]>([]);
  const controlUrl = useOcgControlUrl();
  const runtime = useOcgRuntime();
  const [history, setHistory] = useState<{ projectId: string; error: string | null }>({ projectId: "", error: null });
  const historyRequest = useRef(0);
  const loadHistory = useCallback(() => {
    const request = ++historyRequest.current;
    if (!activeProjectId || !runtime.client.hydrateProject) return;
    const projectId = activeProjectId;
    void runtime.client.hydrateProject(projectId).then(() => {
      if (request === historyRequest.current) setHistory({ projectId, error: null });
    }).catch((cause: unknown) => {
      if (request === historyRequest.current) setHistory({ projectId, error: cause instanceof Error ? cause.message : String(cause) });
    });
  }, [activeProjectId, runtime.client]);
  useEffect(loadHistory, [loadHistory]);
  const retryHistory = useCallback(() => {
    setHistory({ projectId: "", error: null });
    loadHistory();
  }, [loadHistory]);
  const historyStatus = runtime.authority !== "canonical" || !activeProjectId ? "ready" :
    history.projectId !== activeProjectId ? "loading" : history.error ? "failed" : "ready";

  useEffect(() => {
    if (!controlUrl) return;
    let cancelled = false;
    const client = createHttpCanonicalControlClient({ baseUrl: controlUrl, fetch });
    void client.listProjects().then((records) => {
      if (cancelled) return;
      setProjects(records.map((record) => ({
        id: record.project_id,
        name: projectName(record.root, record.project_id),
        root: record.root,
      })));
    }).catch(() => {
      if (!cancelled) setProjects([]);
    });
    return () => { cancelled = true; };
  }, [controlUrl]);

  useEffect(() => {
    if (initialProjectId === undefined) return;
    const resolved = resolveProjectId(initialProjectId);
    writeStoredProjectId(resolved);
  }, [initialProjectId]);

  // No silent fallback: when the stored selection is absent or no longer
  // registered, the active project stays empty and chat send fails clearly
  // instead of silently executing against an unrelated Project.
  const selectedProjectId = activeProjectId;

  const setActiveProject = useCallback((id: ProjectId) => {
    const resolved = resolveProjectId(id);
    if (projects.length > 0 && !projects.some((project) => project.id === resolved)) return;
    setActiveProjectIdState(resolved);
    writeStoredProjectId(resolved);
  }, [projects]);

  const registerProjectSession = useCallback(
    (sessionId: string, projectId?: ProjectId) => {
      if (!sessionId) return;
      const target = resolveProjectId(projectId ?? selectedProjectId);
      runtime.client.bindSessionProject?.(sessionId, target);
      setRegisteredSessionIds((current) => {
        const existing = current[target] ?? [];
        if (existing.includes(sessionId)) return current;
        return { ...current, [target]: [...existing, sessionId] };
      });
    },
    [runtime.client, selectedProjectId],
  );

  const activeProjectSessionIds = useMemo(
    () => [
      ...new Set([
        ...(runtime.authority === "mock" ? projectSessionIds(selectedProjectId) :
          runtime.snapshot.sessions.filter((session) => session.projectId === selectedProjectId).map((session) => session.id)),
        ...(registeredSessionIds[selectedProjectId] ?? []),
      ]),
    ],
    [registeredSessionIds, selectedProjectId, runtime.authority, runtime.snapshot.sessions],
  );

  const value = useMemo<ProjectContextValue>(
    () => ({
      activeProjectId: selectedProjectId,
      activeProject: selectProject(selectedProjectId, projects),
      projects,
      setActiveProject,
      registerProjectSession,
      activeProjectSessionIds,
      historyStatus,
      historyError: history.projectId === selectedProjectId ? history.error : null,
      retryHistory,
    }),
    [activeProjectSessionIds, history, historyStatus, projects, registerProjectSession, retryHistory, selectedProjectId, setActiveProject],
  );

  return <ProjectContext.Provider value={value}>{children}</ProjectContext.Provider>;
}

export function useProject(): ProjectContextValue {
  const context = useContext(ProjectContext);
  if (!context) throw new Error("useProject must be used inside ProjectProvider");
  return context;
}
