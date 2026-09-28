"use client";

/**
 * Single Project context for the shell.
 *
 * Only the active project ID is persisted, under a versioned key. A URL-provided
 * `initialProjectId` takes precedence over stored state. No global state library
 * is involved.
 */

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import type { ProjectId, ProjectSummary } from "./domain";
import { DEFAULT_PROJECT_ID, PROJECTS, isProjectId, resolveProjectId, selectProject } from "./domain";
import { projectSessionIds } from "./fixtures";

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

export type ProjectContextValue = {
  activeProjectId: ProjectId;
  activeProject: ProjectSummary;
  projects: readonly ProjectSummary[];
  setActiveProject: (id: ProjectId) => void;
  /** Register a newly created session with a project (defaults to the active one). */
  registerProjectSession: (sessionId: string, projectId?: ProjectId) => void;
  /** Fixture session IDs for the active project plus registered sessions. */
  activeProjectSessionIds: readonly string[];
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

  useEffect(() => {
    if (initialProjectId === undefined) return;
    const resolved = resolveProjectId(initialProjectId);
    writeStoredProjectId(resolved);
  }, [initialProjectId]);

  const setActiveProject = useCallback((id: ProjectId) => {
    const resolved = resolveProjectId(id);
    setActiveProjectIdState(resolved);
    writeStoredProjectId(resolved);
  }, []);

  const registerProjectSession = useCallback(
    (sessionId: string, projectId?: ProjectId) => {
      if (!sessionId) return;
      const target = resolveProjectId(projectId ?? activeProjectId);
      setRegisteredSessionIds((current) => {
        const existing = current[target] ?? [];
        if (existing.includes(sessionId)) return current;
        return { ...current, [target]: [...existing, sessionId] };
      });
    },
    [activeProjectId],
  );

  const activeProjectSessionIds = useMemo(
    () => [
      ...new Set([
        ...projectSessionIds(activeProjectId),
        ...(registeredSessionIds[activeProjectId] ?? []),
      ]),
    ],
    [activeProjectId, registeredSessionIds],
  );

  const value = useMemo<ProjectContextValue>(
    () => ({
      activeProjectId,
      activeProject: selectProject(activeProjectId),
      projects: PROJECTS,
      setActiveProject,
      registerProjectSession,
      activeProjectSessionIds,
    }),
    [activeProjectId, activeProjectSessionIds, registerProjectSession, setActiveProject],
  );

  return <ProjectContext.Provider value={value}>{children}</ProjectContext.Provider>;
}

export function useProject(): ProjectContextValue {
  const context = useContext(ProjectContext);
  if (!context) throw new Error("useProject must be used inside ProjectProvider");
  return context;
}
