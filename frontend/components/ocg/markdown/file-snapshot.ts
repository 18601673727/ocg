export type FileSnapshot = {
  path: string;
  start_line: number;
  end_line: number | null;
  total_lines: number;
  content: string;
  line_truncated: boolean;
};

export function fileSnapshots(value: unknown): FileSnapshot[] {
  if (!value || typeof value !== "object" || !("file_snapshots" in value)) return [];
  const items = value.file_snapshots;
  if (!Array.isArray(items)) return [];
  return items.filter(
    (item): item is FileSnapshot =>
      item !== null &&
      typeof item === "object" &&
      typeof item.path === "string" &&
      typeof item.content === "string" &&
      Number.isSafeInteger(item.start_line) &&
      item.start_line > 0 &&
      (item.end_line === null ||
        (Number.isSafeInteger(item.end_line) && item.end_line >= item.start_line)) &&
      typeof item.line_truncated === "boolean" &&
      Number.isSafeInteger(item.total_lines),
  );
}
