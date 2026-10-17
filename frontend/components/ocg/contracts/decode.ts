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

export type Rec = Record<string, unknown>;

export const isRecord = (input: unknown): input is Rec =>
  typeof input === "object" && input !== null && !Array.isArray(input);
