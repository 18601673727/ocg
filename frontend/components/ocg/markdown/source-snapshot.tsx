import { Portal } from "@solidjs/web";
import { createEffect, createMemo, createSignal } from "solid-js";
import { ariaState, bindElementRef, elementRef, type View } from "@/lib/ui-state";
import { useI18n } from "../i18n";
import type { FileSnapshot } from "./file-snapshot";
import { highlightCode } from "./markdown-pipeline";

export type { FileSnapshot } from "./file-snapshot";

export function sourceReference(href: string): { path: string; start: number; end: number } | null {
  if (/^(?:[a-z][\w+.-]*:\/\/|mailto:|\/\/|#)/i.test(href)) return null;
  const match = /(?::(\d+)(?:-(\d+))?|#L(\d+)(?:-L?(\d+))?)$/i.exec(href);
  const rawPath = match ? href.slice(0, match.index) : href;
  const path = rawPath.startsWith("/") ? rawPath : rawPath.replace(/^(?:\.\/)+/, "");
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

export function sourceLanguage(path: string): string {
  const extension = path.split(".").at(-1)?.toLowerCase() ?? "";
  return LANGUAGES[extension] ?? extension;
}

export function SourceLink(props: {
  href: string;
  snapshots: readonly FileSnapshot[];
  children: View;
}) {
  const [hovered, setHovered] = createSignal(false);
  const [pinned, setPinned] = createSignal(false);
  const [position, setPosition] = createSignal<{
    left: number;
    top: number;
    maxHeight: number;
  } | null>(null);
  const i18n = useI18n();
  const open = createMemo(() => hovered() || pinned());
  const ref = createMemo(() => sourceReference(props.href));
  const selectedLine = createMemo(() => ref()?.start);
  const anchor = elementRef<HTMLSpanElement>(null);
  const popover = elementRef<HTMLSpanElement>(null);
  const viewport = elementRef<HTMLSpanElement>(null);
  const closeTimer = elementRef<ReturnType<typeof setTimeout> | null>(null);
  const show = () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
    setHovered(true);
  };
  const hide = () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
    closeTimer.current = setTimeout(() => setHovered(false), 100);
  };
  createEffect(
    () => [],
    () => () => {
      if (closeTimer.current) clearTimeout(closeTimer.current);
    },
  );
  createEffect(
    () => [open(), props.snapshots],
    () => {
      if (!open()) return;
      const updatePosition = () => {
        const anchorRect = anchor.current?.getBoundingClientRect();
        const popoverRect = popover.current?.getBoundingClientRect();
        if (!anchorRect || !popoverRect) return;
        const margin = 8;
        const left = Math.min(
          Math.max(anchorRect.left, margin),
          Math.max(margin, window.innerWidth - popoverRect.width - margin),
        );
        const top = anchorRect.bottom + 4;
        const maxHeight = Math.max(0, window.innerHeight - top - margin);
        setPosition((current) =>
          current?.left === left && current.top === top && current.maxHeight === maxHeight
            ? current
            : { left, top, maxHeight },
        );
      };
      updatePosition();
      window.addEventListener("resize", updatePosition);
      window.addEventListener("scroll", updatePosition, true);
      return () => {
        window.removeEventListener("resize", updatePosition);
        window.removeEventListener("scroll", updatePosition, true);
      };
    },
  );
  createEffect(
    () => [open(), selectedLine(), props.snapshots],
    () => {
      if (!open() || selectedLine() === undefined) return;
      const container = viewport.current;
      const target = container?.querySelector<HTMLElement>(`[data-line="${selectedLine()}"]`);
      if (container && target) {
        container.scrollTop +=
          target.getBoundingClientRect().top - container.getBoundingClientRect().top - 100;
      }
    },
  );
  return (
    <>
      {(() => {
        if (!ref()) return null;
        const snapshot = createMemo(() =>
          props.snapshots.find(
            (item) =>
              item.path === ref()!.path &&
              item.end_line !== null &&
              item.start_line <= ref()!.start &&
              item.end_line >= ref()!.start,
          ),
        );
        const language = createMemo(() => sourceLanguage(ref()!.path));
        const lines = createMemo(() => snapshot()?.content.split("\n") ?? []);
        if (lines().at(-1) === "") lines().pop();
        return (
          <span
            ref={bindElementRef(anchor)}
            class="relative inline-block"
            onMouseEnter={show}
            onMouseLeave={hide}
            onKeyDown={(event) => {
              if (event.key === "Escape") {
                setPinned(false);
                setHovered(false);
              }
            }}
          >
            <button
              type="button"
              class="text-primary underline text-left"
              onClick={() => setPinned((value) => !value)}
              onFocus={show}
              onBlur={hide}
              aria-expanded={ariaState(open())}
            >
              {props.children}
            </button>
            {open() && (
              <Portal mount={document.body}>
                {
                  <span
                    ref={bindElementRef(popover)}
                    role="dialog"
                    aria-label={i18n.t("chat.sourceSnapshot")}
                    class="fixed z-50 flex w-[min(42rem,80vw)] flex-col overflow-hidden rounded-md border bg-popover p-3 text-popover-foreground shadow-lg"
                    style={{
                      left: `${position()?.left ?? 8}px`,
                      top: `${position()?.top ?? 8}px`,
                      "max-height":
                        position()?.maxHeight === undefined
                          ? "calc(100vh - 16px)"
                          : `${position()!.maxHeight}px`,
                    }}
                    onMouseEnter={show}
                    onMouseLeave={hide}
                  >
                    <span class="mb-2 block shrink-0 break-all font-mono text-xs">
                      {ref()!.path} · {i18n.t("chat.sourceSnapshot")}
                      {snapshot() &&
                      (snapshot()!.start_line !== 1 ||
                        snapshot()!.end_line !== snapshot()!.total_lines)
                        ? ` · ${snapshot()!.start_line}–${snapshot()!.end_line} / ${snapshot()!.total_lines}`
                        : ""}
                    </span>
                    {snapshot() ? (
                      <span
                        ref={bindElementRef(viewport)}
                        class="block max-h-80 min-h-0 flex-1 overflow-auto rounded bg-muted font-mono text-xs leading-5"
                      >
                        <span class="block w-max min-w-full">
                          {lines().map((line, index) => {
                            const number = snapshot()!.start_line + index;
                            const highlighted = highlightCode(line, language());
                            return (
                              <span
                                data-line={number}
                                class={`flex w-full ${number >= ref()!.start && number <= ref()!.end ? "bg-primary/15" : ""}`}
                              >
                                <span class="w-14 shrink-0 select-none pr-3 text-right text-muted-foreground">
                                  {number}
                                </span>
                                {highlighted ? (
                                  <code class="hljs whitespace-pre" innerHTML={highlighted.html} />
                                ) : (
                                  <code class="whitespace-pre">{line}</code>
                                )}
                              </span>
                            );
                          })}
                          {snapshot()!.line_truncated && (
                            <span class="block text-muted-foreground">…</span>
                          )}
                        </span>
                      </span>
                    ) : (
                      <span class="block text-xs text-muted-foreground">
                        {i18n.t("chat.sourceSnapshotUnavailable")}
                      </span>
                    )}
                  </span>
                }
              </Portal>
            )}
          </span>
        );
      })()}
    </>
  );
}
