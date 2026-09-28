/**
 * Minimal structural decoders for the Rust-generated control contract.
 *
 * `generated.ts` gives the PWA the *shape* of every control payload, but a
 * TypeScript type is erased at runtime: `JSON.parse` hands back `unknown`, and
 * every consumer would otherwise need an unchecked cast. The clients this
 * replaced used eight `as` casts, so a renamed or retyped Rust field reached
 * the UI as `undefined` with no error anywhere.
 *
 * A decoder closes that gap without pulling in a schema library. Each decoder
 * either yields a value of its declared contract type or reports the path that
 * stopped matching, so a malformed payload becomes a visible contract violation
 * instead of a `TypeError` three components later.
 *
 * Every decoder builds its result value *explicitly* from checked parts rather
 * than casting a record. That is what makes the Rust definition, the generated
 * type, and the runtime check one contract: when Rust adds or renames a
 * **required** field, the object literal here no longer satisfies the contract
 * type and the build fails. A cast-based helper would silently accept the new
 * field as `undefined`.
 *
 * A newly **optional** field is the deliberate exception. It does not stop the
 * build, and the decoder leaves it out of the value it returns, so the PWA
 * ignores a field it does not understand instead of failing on a newer
 * backend's addition. Reading such a field is a product decision, and adding
 * the check to the decoder is how you make it.
 */

import type { JsonValue } from "./generated";

/** Reports where a payload stopped matching the contract. */
export class ContractError extends Error {
  readonly path: string;
  readonly expected: string;

  constructor(path: string, expected: string) {
    super(`OCG control contract violation at ${path || "<root>"}: expected ${expected}`);
    this.name = "ContractError";
    this.path = path;
    this.expected = expected;
  }
}

export type DecodeResult<T> = { ok: true; value: T } | { ok: false; error: ContractError };

export type Decoder<T> = (input: unknown, path: string) => DecodeResult<T>;

export type Rec = Record<string, unknown>;

export const isRecord = (input: unknown): input is Rec =>
  typeof input === "object" && input !== null && !Array.isArray(input);

export const yes = <T>(value: T): DecodeResult<T> => ({ ok: true, value });

export const bad = <T>(path: string, expected: string): DecodeResult<T> => ({
  ok: false,
  error: new ContractError(path, expected),
});

/** Narrow a value to a record, or explain what was expected. */
export function record(input: unknown, path: string, expected: string): DecodeResult<Rec> {
  return isRecord(input) ? yes(input) : bad(path, expected);
}

/** Join a parent path and a field name. */
function join(path: string, key: string): string {
  return path ? `${path}.${key}` : key;
}

/**
 * A required field.
 *
 * `path` is the enclosing record's path, so a violation reports the full
 * route to the offending leaf (`configuration.global.resource_budget.unit`)
 * rather than a bare field name.
 */
export function req<T>(source: Rec, key: string, decoder: Decoder<T>, path: string): DecodeResult<T> {
  return decoder(source[key], join(path, key));
}

/**
 * An optional field.
 *
 * `None` and an absent key are the same thing on the wire, so both yield
 * `undefined` rather than failing: the PWA must treat "not set" as a first
 * class state, exactly as Rust's `Option` does.
 */
export function opt<T>(
  source: Rec,
  key: string,
  decoder: Decoder<T>,
  path: string,
): DecodeResult<T | undefined> {
  const value = source[key];
  if (value === undefined || value === null) return yes(undefined);
  const result = decoder(value, join(path, key));
  return result.ok ? yes(result.value) : result;
}

export const string: Decoder<string> = (input, path) =>
  typeof input === "string" ? yes(input) : bad(path, "a string");

export const number: Decoder<number> = (input, path) =>
  typeof input === "number" && Number.isFinite(input) ? yes(input) : bad(path, "a finite number");

/**
 * A non-negative integer index, which is how Rust's `usize` arrives.
 *
 * Distinct from `number` because a negative or fractional value is not a valid
 * index in any OCG identity, and treating one as a number would let a corrupt
 * witness through as plausible.
 */
export const index: Decoder<number> = (input, path) =>
  typeof input === "number" && Number.isInteger(input) && input >= 0
    ? yes(input)
    : bad(path, "a non-negative integer");

export const boolean: Decoder<boolean> = (input, path) =>
  typeof input === "boolean" ? yes(input) : bad(path, "a boolean");

/** A field whose value is a fixed protocol literal, such as `api_version`. */
export function literal<T extends string>(expected: T): Decoder<T> {
  return (input, path) => (input === expected ? yes(input as T) : bad(path, `"${expected}"`));
}

/**
 * A string that carries an identity.
 *
 * A `String` in Rust accepts `""`, but the contract types that *are* identities
 * reject it on the way in. `DispatchWitness::from_json` is the authority for
 * that rule and this mirrors it, so the PWA refuses the same witnesses the
 * backend would refuse rather than rendering an empty id as if it were one.
 */
export const identity: Decoder<string> = (input, path) =>
  typeof input === "string" && input.length > 0 ? yes(input) : bad(path, "a non-empty string");

/** An integer at or above `floor`, for a generation that starts at 1. */
export function atLeast(floor: number): Decoder<number> {
  return (input, path) => {
    if (typeof input !== "number" || !Number.isInteger(input)) {
      return bad(path, "an integer");
    }
    return input >= floor ? yes(input) : bad(path, `an integer >= ${floor}`);
  };
}

export function nullable<T>(decoder: Decoder<T>): Decoder<T | null> {
  return (input, path) => (input === null ? yes(null) : decoder(input, path));
}

export function array<T>(decoder: Decoder<T>): Decoder<T[]> {
  return (input, path) => {
    if (!Array.isArray(input)) return bad(path, "an array");
    const out: T[] = [];
    for (let index = 0; index < input.length; index += 1) {
      const result = decoder(input[index], `${path}[${index}]`);
      if (!result.ok) return result;
      out.push(result.value);
    }
    return yes(out);
  };
}

/** A homogeneous string-keyed map, the shape serde emits for `BTreeMap`. */
export function stringMap<T>(decoder: Decoder<T>): Decoder<Record<string, T>> {
  return (input, path) => {
    const rec = record(input, path, "an object");
    if (!rec.ok) return bad(path, "an object");
    const out: Record<string, T> = {};
    for (const [key, value] of Object.entries(rec.value)) {
      const result = decoder(value, join(path, key));
      if (!result.ok) return result;
      out[key] = result.value;
    }
    return yes(out);
  };
}

/** A single-key wrapper, e.g. serde's externally tagged enum variant. */
export function wrapped<K extends string, T>(key: K, inner: Decoder<T>): Decoder<{ [P in K]: T }> {
  return (input, path) => {
    const rec = record(input, path, "an object");
    if (!rec.ok) return rec;
    const result = inner(rec.value[key], join(path, key));
    return result.ok ? yes({ [key]: result.value } as { [P in K]: T }) : result;
  };
}

/** A `serde_json::Value` field, which the backend leaves unconstrained. */
export const jsonValue: Decoder<JsonValue> = (input) => yes(input as JsonValue);

/**
 * Run a decoder and throw on violation.
 *
 * A contract violation is a hard error at the control boundary: rendering a
 * half-typed payload would put the PWA into a state it has no authority to be
 * in, so it refuses instead of guessing.
 */
export function decode<T>(decoder: (input: unknown) => DecodeResult<T>, input: unknown): T {
  const result = decoder(input);
  if (result.ok) return result.value;
  throw result.error;
}
