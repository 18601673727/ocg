/**
 * Canonical Project domain.
 *
 * A Project is the product-level grouping the operator switches between.
 * It is deliberately separate from the existing Home/session shell concepts.
 *
 * Project IDs are stable fixture-owned strings. Display names are labels
 * only and are never used to derive an ID.
 */

/** Opaque identity type; fixture IDs below are the currently known values. */
export type ProjectId = string;

export const PROJECT_IDS = ["zhuju", "route-lace", "ocg", "cecece"] as const;
export type KnownProjectId = (typeof PROJECT_IDS)[number];

export type ProjectSummary = {
  id: ProjectId;
  name: string;
};

/** Project is the same product concept as ProjectSummary in this slice. */
export type Project = ProjectSummary;

/** Canonical project list. Names are stable display labels. */
export const PROJECTS: readonly ProjectSummary[] = [
  { id: "zhuju", name: "Zhuju" },
  { id: "route-lace", name: "RouteLace" },
  { id: "ocg", name: "OCG" },
  { id: "cecece", name: "CECECE" },
];

export const DEFAULT_PROJECT_ID: KnownProjectId = "zhuju";

export function isProjectId(value: unknown): value is ProjectId {
  return typeof value === "string" && PROJECT_IDS.includes(value as KnownProjectId);
}

/**
 * Unknown, missing, or malformed IDs fall back deterministically to Zhuju.
 * This is the single fallback used by both selectors and the context.
 */
export function resolveProjectId(value: unknown): KnownProjectId {
  return isProjectId(value) ? (value as KnownProjectId) : DEFAULT_PROJECT_ID;
}

/** Look up the canonical project for any value, falling back to Zhuju. */
export function selectProject(value: unknown): ProjectSummary {
  const id = resolveProjectId(value);
  return PROJECTS.find((project) => project.id === id) ?? PROJECTS[0]!;
}

/**
 * Resolve a URL/search param while preserving absence so the provider can read
 * persisted state when no explicit project was supplied.
 */
export function resolveProjectParam(value: unknown): ProjectId | undefined {
  if (value === undefined || value === null) return undefined;
  return resolveProjectId(value);
}

export function projectById(id: ProjectId): ProjectSummary {
  return selectProject(id);
}

export type ProjectOption = ProjectSummary & {
  active: boolean;
};

/** Switcher option list with exactly one active project. */
export function projectOptions(activeProjectId: unknown): ProjectOption[] {
  const active = resolveProjectId(activeProjectId);
  return PROJECTS.map((project) => ({ ...project, active: project.id === active }));
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
