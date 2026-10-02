/**
 * Canonical Project domain.
 *
 * A Project is the product-level grouping the operator switches between.
 * It is deliberately separate from the existing Home/session shell concepts.
 *
 * Project IDs are opaque backend-owned strings. Display names are labels only
 * and are never used to derive an ID.
 */

/** Opaque identity type owned by the canonical backend. */
export type ProjectId = string;

export type ProjectSummary = {
  id: ProjectId;
  name: string;
  root?: string;
};

/** Project is the same product concept as ProjectSummary in this slice. */
export type Project = ProjectSummary;

/** Empty until the canonical backend supplies its first Project. */
export const DEFAULT_PROJECT_ID: ProjectId = "";

export function isProjectId(value: unknown): value is ProjectId {
  return typeof value === "string" && value.length > 0;
}

/**
 * Unknown, missing, or malformed IDs fall back deterministically to no project.
 * This is the single fallback used by both selectors and the context.
 */
export function resolveProjectId(value: unknown): ProjectId {
  return isProjectId(value) ? value : DEFAULT_PROJECT_ID;
}

/** Look up a canonical project, falling back to an explicit empty selection. */
export function selectProject(value: unknown, projects: readonly ProjectSummary[] = []): ProjectSummary {
  const id = resolveProjectId(value);
  return projects.find((project) => project.id === id) ?? { id, name: id || "No project" };
}

/**
 * Resolve a URL/search param while preserving absence so the provider can read
 * persisted state when no explicit project was supplied.
 */
export function resolveProjectParam(value: unknown): ProjectId | undefined {
  if (value === undefined || value === null) return undefined;
  return resolveProjectId(value);
}

/**
 * Accessible trigger label. Collapsed triggers are icon-only, so the label
 * carries the full active-project context for screen readers and tooltips.
 */
export function projectSwitcherLabel(project: ProjectSummary, collapsed = false): string {
  return collapsed ? `Project: ${project.name}` : `Switch project, current ${project.name}`;
}

/** Preserve an explicit project context while navigating between shell views. */
export function withProjectParam(path: string, projectId: ProjectId): string {
  return `${path}${path.includes("?") ? "&" : "?"}project=${encodeURIComponent(projectId)}`;
}
