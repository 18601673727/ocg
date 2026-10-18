export function take<T>(values: readonly T[], count: number) { return values.slice(0, Math.max(0, Math.min(values.length, Number.isFinite(count) ? Math.floor(count) : 0))); }
