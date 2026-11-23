import { ContractError } from "@/components/ocg/contracts";

export function counter(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0)
    throw new ContractError(path, "a non-negative count");
  return value;
}

export function configurationIdentity(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let time = Date.now();
  for (let i = 5; i >= 0; i--) {
    bytes[i] = time % 256;
    time = Math.floor(time / 256);
  }
  bytes[6] = (bytes[6] & 15) | 0x70;
  bytes[8] = (bytes[8] & 63) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function sequence(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new ContractError("after_seq", "a non-negative safe integer");
  return value;
}

export function jobIdentity(value: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value))
    throw new ContractError("job", "a lowercase hyphenated Job UUID");
  return value;
}

export function identity(value: string, path: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)) {
    throw new ContractError(path, "a lowercase hyphenated UUIDv7 identity");
  }
  return value;
}

export function submissionKey(value: string): string {
  if (!/^[A-Za-z0-9._~:+/=-]{16,128}$/.test(value)) {
    throw new ContractError("Idempotency-Key", "a well-formed idempotency key");
  }
  return value;
}
