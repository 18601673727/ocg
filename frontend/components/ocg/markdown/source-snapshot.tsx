"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { highlightCode } from "./markdown-pipeline";
import { useI18n } from "../i18n";

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

export function sourceReference(href: string): { path: string; start: number; end: number } | null {
  if (/^(?:[a-z][\w+.-]*:\/\/|mailto:|\/\/|#)/i.test(href)) return null;
  const match = /(?::(\d+)(?:-(\d+))?|#L(\d+)(?:-L?(\d+))?)$/i.exec(href);
  const path = match ? href.slice(0, match.index) : href;
  if (!/\.[a-z\d][a-z\d_-]*$/i.test(path) || path.includes("?")) return null;
  const start = Number(match?.[1] ?? match?.[3] ?? 1);
  const end = Number(match?.[2] ?? match?.[4] ?? start);
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 1 || end < start)
    return null;
  return { path, start, end };
}

const LANGUAGES: Record<string, string> = {
  rs: "rust",
  ts: "typescript",
  tsx: "typescript",
  js: "javascript",
  jsx: "javascript",
  py: "python",
  sh: "bash",
  yml: "yaml",
  md: "markdown",
  html: "xml",
};

export function SourceLink({
  href,
  snapshots,
  children,
}: {
  href: string;
  snapshots: readonly FileSnapshot[];
  children: ReactNode;
}) {
  const [hovered, setHovered] = useState(false);
  const [pinned, setPinned] = useState(false);
  const { t } = useI18n();
  const open = hovered || pinned;
  const ref = sourceReference(href);
  const selectedLine = ref?.start;
  const viewport = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    if (!open || selectedLine === undefined) return;
    const container = viewport.current;
    const target = container?.querySelector<HTMLElement>(`[data-line="${selectedLine}"]`);
    if (container && target) {
      container.scrollTop +=
        target.getBoundingClientRect().top - container.getBoundingClientRect().top - 100;
    }
  }, [open, selectedLine, snapshots]);
  if (!ref) return null;
  const snapshot = snapshots.find(
    (item) =>
      item.path === ref.path &&
      item.end_line !== null &&
      item.start_line <= ref.start &&
      item.end_line >= ref.start,
  );
  const language =
    LANGUAGES[ref.path.split(".").at(-1)?.toLowerCase() ?? ""] ?? ref.path.split(".").at(-1) ?? "";
  const lines = snapshot?.content.split("\n") ?? [];
  if (lines.at(-1) === "") lines.pop();
  return (
    <span
      className="relative inline-block"
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          setPinned(false);
          setHovered(false);
        }
      }}
    >
      <button
        type="button"
        className="text-primary underline text-left"
        onClick={() => setPinned((value) => !value)}
        onFocus={() => setHovered(true)}
        onBlur={() => setHovered(false)}
        aria-expanded={open}
      >
        {children}
      </button>
      {open && (
        <span
          role="dialog"
          aria-label={t("chat.sourceSnapshot")}
          className="absolute left-0 top-full z-50 mt-1 block w-[min(42rem,80vw)] rounded-md border bg-popover p-3 text-popover-foreground shadow-lg"
        >
          <span className="mb-2 block break-all font-mono text-xs">
            {ref.path} · {t("chat.sourceSnapshot")}
            {snapshot && (snapshot.start_line !== 1 || snapshot.end_line !== snapshot.total_lines)
              ? ` · ${snapshot.start_line}–${snapshot.end_line} / ${snapshot.total_lines}`
              : ""}
          </span>
          {snapshot ? (
            <span
              ref={viewport}
              className="block max-h-80 overflow-auto rounded bg-muted font-mono text-xs leading-5"
            >
              {lines.map((line, index) => {
                const number = snapshot.start_line + index;
                const highlighted = highlightCode(line, language);
                return (
                  <span
                    key={number}
                    data-line={number}
                    className={`flex min-w-max ${number >= ref.start && number <= ref.end ? "bg-primary/15" : ""}`}
                  >
                    <span className="w-14 shrink-0 select-none pr-3 text-right text-muted-foreground">
                      {number}
                    </span>
                    {highlighted ? (
                      <code
                        className="hljs whitespace-pre"
                        dangerouslySetInnerHTML={{ __html: highlighted.html }}
                      />
                    ) : (
                      <code className="whitespace-pre">{line}</code>
                    )}
                  </span>
                );
              })}
              {snapshot.line_truncated && <span className="block text-muted-foreground">…</span>}
            </span>
          ) : (
            <span className="block text-xs text-muted-foreground">
              {t("chat.sourceSnapshotUnavailable")}
            </span>
          )}
        </span>
      )}
    </span>
  );
}
