/**
 * Structural narrowing helpers for wire and fixture payloads.
 *
 * Every payload that arrives over the control wire, or out of a fixture typed as
 * `unknown`, has to be narrowed before it can be read. The contract decoders in
 * `components/ocg/contracts/decode.ts` already do this with path-addressed
 * errors; the runtime envelope validators need the same primitives without the
 * decoder plumbing, and both used to carry private copies of `isRecord` and
 * friends. One copy, one meaning per name, so "non-empty string" cannot mean two
 * things in two validators.
 */

export type Rec = Record<string, unknown>;

/** A JSON object that is safe to index by name. */
export function isRecord(value: unknown): value is Rec {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Narrows an array whose every member is a record, or returns null.
 *
 * `Array.isArray` narrows `unknown` only to `any[]`, which silently reopens
 * every property access to unchecked reads. Code that then indexes entries needs
 * this instead.
 */
export function asRecordArray(value: unknown): Rec[] | null {
  return Array.isArray(value) && value.every(isRecord) ? (value as Rec[]) : null;
}

/** A string that carries an identity or label; the empty string carries neither. */
export function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

/** An array ordinal: integral, non-negative, and finite by construction. */
export function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

/** A positive integral counter, such as a generation or attempt number. */
export function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value > 0;
}

/** Membership in a literal union without an `as` cast at every call site. */
export function isOneOf<T extends string>(value: unknown, allowed: readonly T[]): value is T {
  return typeof value === "string" && (allowed as readonly string[]).includes(value);
}
