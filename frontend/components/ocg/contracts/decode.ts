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
