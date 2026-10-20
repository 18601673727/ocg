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
