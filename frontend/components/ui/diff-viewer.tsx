import { CheckIcon, CopyIcon } from "@lucide/solid";
import { cva, type VariantProps } from "class-variance-authority";
import { diffLines } from "diff";
import parseDiff from "parse-diff";
import { createMemo, createSignal, createUniqueId } from "solid-js";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { ariaState, type ElementProps, splitProps } from "@/lib/ui-state";
import { cn } from "@/lib/utils";

type DiffLineType = "add" | "del" | "normal" | "marker";

interface ParsedLine {
  type: DiffLineType;
  content: string;
  oldLineNumber?: number | undefined;
  newLineNumber?: number | undefined;
}

interface ParsedHunk {
  header: string;
  lines: ParsedLine[];
}

interface ParsedFile {
  oldName?: string | undefined;
  newName?: string | undefined;
  lines: ParsedLine[];
  hunks: ParsedHunk[];
  additions: number;
  deletions: number;
}

interface SplitLinePair {
  left: ParsedLine | null;
  right: ParsedLine | null;
}

function parsePatch(patch: string): ParsedFile[] {
  const files = parseDiff(patch);
  return files.map((file) => {
    const hunks = file.chunks.map((chunk) => {
      const lines: ParsedLine[] = [];
      for (const change of chunk.changes) {
        if (change.content.startsWith("\\")) {
          lines.push({ type: "marker", content: change.content });
        } else if (change.type === "add") {
          lines.push({
            type: "add",
            content: change.content.slice(1),
            newLineNumber: change.ln,
          });
        } else if (change.type === "del") {
          lines.push({
            type: "del",
            content: change.content.slice(1),
            oldLineNumber: change.ln,
          });
        } else {
          lines.push({
            type: "normal",
            content: change.content.slice(1),
            oldLineNumber: change.ln1,
            newLineNumber: change.ln2,
          });
        }
      }
      return { header: chunk.content, lines };
    });

    const lines = hunks.flatMap((hunk) => hunk.lines);
    let additions = 0;
    let deletions = 0;
    for (const line of lines) {
      if (line.type === "add") additions++;
      if (line.type === "del") deletions++;
    }
    return {
      oldName: file.from,
      newName: file.to,
      lines,
      hunks,
      additions,
      deletions,
    };
  });
}

function computeDiff(
  oldContent: string,
  newContent: string,
): {
  lines: ParsedLine[];
  hunks: ParsedHunk[];
  additions: number;
  deletions: number;
} {
  const changes = diffLines(oldContent, newContent);
  const lines: ParsedLine[] = [];
  let oldLine = 1;
  let newLine = 1;
  let additions = 0;
  let deletions = 0;

  for (const change of changes) {
    const contentLines = change.value.replace(/\n$/, "").split("\n");
    for (const content of contentLines) {
      if (change.added) {
        additions++;
        lines.push({ type: "add", content, newLineNumber: newLine++ });
      } else if (change.removed) {
        deletions++;
        lines.push({ type: "del", content, oldLineNumber: oldLine++ });
      } else {
        lines.push({
          type: "normal",
          content,
          oldLineNumber: oldLine++,
          newLineNumber: newLine++,
        });
      }
    }
  }
  const oldLines = lines.filter((line) => line.type !== "add").length;
  const newLines = lines.filter((line) => line.type !== "del").length;
  const oldStart = oldLines === 0 ? 0 : 1;
  const newStart = newLines === 0 ? 0 : 1;

  return {
    lines,
    hunks: [
      {
        header: `@@ -${oldStart},${oldLines} +${newStart},${newLines} @@`,
        lines,
      },
    ],
    additions,
    deletions,
  };
}

function formatDiffLine(line: ParsedLine): string {
  if (line.type === "marker") return line.content;
  const indicator = line.type === "add" ? "+" : line.type === "del" ? "-" : " ";
  return `${indicator}${line.content}`;
}

function formatDiffFileName(name: string | undefined, prefix: "a" | "b") {
  return name === "/dev/null" ? name : `${prefix}/${name ?? "file"}`;
}

function formatUnifiedDiff(file: ParsedFile): string {
  return [
    `--- ${formatDiffFileName(file.oldName, "a")}`,
    `+++ ${formatDiffFileName(file.newName, "b")}`,
    ...file.hunks.flatMap((hunk) => [hunk.header, ...hunk.lines.map(formatDiffLine)]),
  ].join("\n");
}

function pairLinesForSplit(lines: ParsedLine[]): SplitLinePair[] {
  const pairs: SplitLinePair[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i]!;
    if (line.type === "normal") {
      pairs.push({ left: line, right: line });
      i++;
    } else if (line.type === "del") {
      const deletions: ParsedLine[] = [];
      while (i < lines.length && lines[i]!.type === "del") {
        deletions.push(lines[i]!);
        i++;
      }
      const additions: ParsedLine[] = [];
      while (i < lines.length && lines[i]!.type === "add") {
        additions.push(lines[i]!);
        i++;
      }
      const maxLen = Math.max(deletions.length, additions.length);
      for (let j = 0; j < maxLen; j++) {
        pairs.push({
          left: deletions[j] ?? null,
          right: additions[j] ?? null,
        });
      }
    } else {
      pairs.push({ left: null, right: line });
      i++;
    }
  }
  return pairs;
}

const diffViewerVariants = cva(
  "aui-diff-viewer overflow-hidden font-mono leading-relaxed [font-variant-ligatures:none]",
  {
    variants: {
      variant: {
        default: "border-foreground/10 bg-foreground/[0.025] dark:bg-foreground/[0.04] border",
        ghost: "bg-transparent",
        muted: "border-foreground/10 bg-foreground/[0.06] dark:bg-foreground/[0.08] border",
      },
      size: {
        sm: "text-[11px]",
        default: "text-[12.5px]",
        lg: "text-[13.5px]",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

const diffLineVariants = cva("flex", {
  variants: {
    type: {
      add: "bg-[var(--diff-add-bg,var(--_diff-add-bg))] shadow-[inset_2px_0_0_var(--diff-add-rule,var(--color-green-500))] [--_diff-add-bg:color-mix(in_oklab,var(--color-green-500)_8%,transparent)] dark:[--_diff-add-bg:color-mix(in_oklab,var(--color-green-500)_15%,transparent)]",
      del: "bg-[var(--diff-del-bg,var(--_diff-del-bg))] shadow-[inset_2px_0_0_var(--diff-del-rule,var(--color-red-500))] [--_diff-del-bg:color-mix(in_oklab,var(--color-red-500)_8%,transparent)] dark:[--_diff-del-bg:color-mix(in_oklab,var(--color-red-500)_15%,transparent)]",
      normal: "",
      marker: "",
      empty: "",
    },
  },
  defaultVariants: {
    type: "normal",
  },
});

const diffLineTextVariants = cva("", {
  variants: {
    type: {
      add: "text-[var(--diff-add-text,var(--color-green-600))] dark:text-[var(--diff-add-text-dark,var(--color-green-400))]",
      del: "text-[var(--diff-del-text,var(--color-red-600))] dark:text-[var(--diff-del-text-dark,var(--color-red-400))]",
      normal: "",
      marker: "",
      empty: "",
    },
  },
  defaultVariants: {
    type: "normal",
  },
});

function getFileExtension(filename?: string | undefined): string {
  const ext = filename?.split(".").pop()?.toLowerCase();
  if (!ext) return "";
  return ext.toUpperCase();
}

function DiffViewerFileBadge(props: { filename?: string | undefined }) {
  const ext = createMemo(() => getFileExtension(props.filename));
  return (
    <>
      {(() => {
        if (!ext()) return null;

        return (
          <span
            data-slot="diff-viewer-file-badge"
            class="border-foreground/15 text-muted-foreground/70 inline-flex h-4 shrink-0 items-center border px-1 text-[9px] leading-none font-medium tracking-wide"
          >
            {ext()}
          </span>
        );
      })()}
    </>
  );
}

function DiffViewerStats(props: { additions: number; deletions: number }) {
  return (
    <span data-slot="diff-viewer-stats" class="flex shrink-0 gap-1.5 text-[11px] tabular-nums">
      <span class="text-green-600 dark:text-green-400">+{props.additions}</span>
      <span class="text-red-600 dark:text-red-400">−{props.deletions}</span>
    </span>
  );
}

function DiffViewerFile(viewProps: ElementProps<"div">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return <div data-slot="diff-viewer-file" class={cn(viewProps.class)} {...props} />;
}

function DiffViewerContent(viewProps: ElementProps<"div">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return (
    <div
      data-slot="diff-viewer-content"
      class={cn("overflow-x-auto", viewProps.class)}
      {...props}
    />
  );
}

interface DiffViewerHeaderProps extends ElementProps<"div"> {
  oldName?: string | undefined;
  newName?: string | undefined;
  additions?: number | undefined;
  deletions?: number | undefined;
  showIcon?: boolean | undefined;
  showStats?: boolean | undefined;
  copyText?: string | undefined;
  copyable?: boolean | undefined;
}

function DiffViewerHeader(viewProps: DiffViewerHeaderProps) {
  const [, props] = splitProps(viewProps, [
    "oldName",
    "newName",
    "additions",
    "deletions",
    "showIcon",
    "showStats",
    "copyText",
    "copyable",
    "class",
  ]);

  const { isCopied, copyToClipboard } = useCopyToClipboard({
    copiedDuration: 1500,
  });

  return (
    <>
      {(() => {
        if (!viewProps.oldName && !viewProps.newName) return null;

        const displayName = createMemo(() => viewProps.newName || viewProps.oldName);

        return (
          <div
            data-slot="diff-viewer-header"
            class={cn(
              "border-foreground/10 text-muted-foreground flex h-9 items-center gap-2 border-b ps-3.5 pe-3 text-[11px] font-medium tracking-wide",
              viewProps.class,
            )}
            {...props}
          >
            {(viewProps.showIcon === undefined ? false : viewProps.showIcon) && (
              <DiffViewerFileBadge filename={displayName()} />
            )}
            <span class="min-w-0 flex-1 truncate">
              {viewProps.oldName && viewProps.newName && viewProps.oldName !== viewProps.newName ? (
                <>
                  <span class="text-muted-foreground/60">{viewProps.oldName}</span>
                  <span class="text-muted-foreground/50">{" → "}</span>
                  <span class="text-foreground/80">{viewProps.newName}</span>
                </>
              ) : (
                displayName()
              )}
            </span>
            {(viewProps.showStats === undefined ? true : viewProps.showStats) &&
              ((viewProps.additions === undefined ? 0 : viewProps.additions) > 0 ||
                (viewProps.deletions === undefined ? 0 : viewProps.deletions) > 0) && (
                <DiffViewerStats
                  additions={viewProps.additions === undefined ? 0 : viewProps.additions}
                  deletions={viewProps.deletions === undefined ? 0 : viewProps.deletions}
                />
              )}
            {(viewProps.copyable === undefined ? true : viewProps.copyable) &&
              viewProps.copyText && (
                <button
                  type="button"
                  aria-label={`Copy diff of ${displayName()}`}
                  onClick={() => copyToClipboard(viewProps.copyText!)}
                  class="text-muted-foreground hover:text-foreground grid size-6 shrink-0 place-items-center rounded-sm transition-colors motion-reduce:transition-none"
                >
                  {isCopied() ? <CheckIcon class="size-3.5" /> : <CopyIcon class="size-3.5" />}
                </button>
              )}
          </div>
        );
      })()}
    </>
  );
}

interface DiffViewerLineProps extends ElementProps<"div"> {
  line: ParsedLine;
  showLineNumbers?: boolean | undefined;
}

function DiffViewerLine(viewProps: DiffViewerLineProps) {
  const [, props] = splitProps(viewProps, ["line", "showLineNumbers", "class"]);

  const indicator = createMemo(() =>
    viewProps.line.type === "add"
      ? "+"
      : viewProps.line.type === "del"
        ? "-"
        : viewProps.line.type === "marker"
          ? ""
          : " ",
  );

  return (
    <div
      data-slot="diff-viewer-line"
      data-type={viewProps.line.type}
      class={cn(diffLineVariants({ type: viewProps.line.type }), viewProps.class)}
      {...props}
    >
      {(viewProps.showLineNumbers === undefined ? true : viewProps.showLineNumbers) && (
        <span
          data-slot="diff-viewer-line-number"
          class="text-muted-foreground/40 w-10 shrink-0 px-2 text-end tabular-nums select-none"
        >
          {viewProps.line.type === "marker"
            ? ""
            : viewProps.line.type === "del"
              ? viewProps.line.oldLineNumber
              : viewProps.line.type === "add"
                ? viewProps.line.newLineNumber
                : viewProps.line.oldLineNumber}
        </span>
      )}
      <span
        data-slot="diff-viewer-indicator"
        class={cn(
          "w-4 shrink-0 text-center select-none",
          diffLineTextVariants({ type: viewProps.line.type }),
        )}
      >
        {indicator()}
      </span>
      <span data-slot="diff-viewer-content" class="flex-1 pe-3.5 break-all whitespace-pre-wrap">
        {viewProps.line.content}
      </span>
    </div>
  );
}

interface DiffViewerSplitLineProps extends ElementProps<"div"> {
  pair: SplitLinePair;
  showLineNumbers?: boolean | undefined;
}

function DiffViewerSplitLine(viewProps: DiffViewerSplitLineProps) {
  const [, props] = splitProps(viewProps, ["pair", "showLineNumbers", "class"]);

  const { left, right } = viewProps.pair;

  return (
    <div data-slot="diff-viewer-split-line" class={cn("flex", viewProps.class)} {...props}>
      <div
        data-slot="diff-viewer-split-left"
        data-type={left?.type ?? "empty"}
        class={cn(
          "border-foreground/10 flex w-1/2 border-e",
          diffLineVariants({ type: left?.type ?? "empty" }),
        )}
      >
        {(viewProps.showLineNumbers === undefined ? true : viewProps.showLineNumbers) && (
          <span class="text-muted-foreground/40 w-10 shrink-0 px-2 text-end tabular-nums select-none">
            {left?.oldLineNumber ?? ""}
          </span>
        )}
        <span
          class={cn(
            "w-4 shrink-0 text-center select-none",
            diffLineTextVariants({ type: left?.type ?? "empty" }),
          )}
        >
          {left ? (left.type === "del" ? "-" : left.type === "marker" ? "" : " ") : ""}
        </span>
        <span class="flex-1 pe-3.5 break-all whitespace-pre-wrap">{left?.content ?? ""}</span>
      </div>
      <div
        data-slot="diff-viewer-split-right"
        data-type={right?.type ?? "empty"}
        class={cn("flex w-1/2", diffLineVariants({ type: right?.type ?? "empty" }))}
      >
        {(viewProps.showLineNumbers === undefined ? true : viewProps.showLineNumbers) && (
          <span class="text-muted-foreground/40 w-10 shrink-0 px-2 text-end tabular-nums select-none">
            {right?.newLineNumber ?? ""}
          </span>
        )}
        <span
          class={cn(
            "w-4 shrink-0 text-center select-none",
            diffLineTextVariants({ type: right?.type ?? "empty" }),
          )}
        >
          {right ? (right.type === "add" ? "+" : right.type === "marker" ? "" : " ") : ""}
        </span>
        <span class="flex-1 pe-3.5 break-all whitespace-pre-wrap">{right?.content ?? ""}</span>
      </div>
    </div>
  );
}

interface DiffViewerFileContentProps {
  file: ParsedFile;
  splitLinePairs: SplitLinePair[];
  viewMode: "split" | "unified";
  showLineNumbers: boolean;
  showIcon: boolean;
  showStats: boolean;
  copyable: boolean;
  maxCollapsedLines: number | undefined;
}

function DiffViewerFileContent(props: DiffViewerFileContentProps) {
  const [isExpanded, setIsExpanded] = createSignal(false);
  const contentId = createUniqueId();
  const renderedLineCount = createMemo(() =>
    props.viewMode === "split" ? props.splitLinePairs.length : props.file.lines.length,
  );
  const isCollapsible = createMemo(
    () =>
      props.maxCollapsedLines !== undefined &&
      props.maxCollapsedLines >= 0 &&
      renderedLineCount() > props.maxCollapsedLines,
  );
  const isCollapsed = createMemo(() => isCollapsible() && !isExpanded());
  const visibleLineCount = createMemo(() =>
    isCollapsed() ? props.maxCollapsedLines : renderedLineCount(),
  );

  return (
    <DiffViewerFile class="border-foreground/10 [contain-intrinsic-size:auto_240px] [content-visibility:auto] not-first:border-t">
      <DiffViewerHeader
        oldName={props.file.oldName}
        newName={props.file.newName}
        additions={props.file.additions}
        deletions={props.file.deletions}
        showIcon={props.showIcon}
        showStats={props.showStats}
        copyText={formatUnifiedDiff(props.file)}
        copyable={props.copyable}
      />
      <DiffViewerContent id={contentId} class="py-2">
        {props.viewMode === "split"
          ? props.splitLinePairs
              .slice(0, visibleLineCount())
              .map((pair, pairIndex) => (
                <DiffViewerSplitLine pair={pair} showLineNumbers={props.showLineNumbers} />
              ))
          : props.file.lines
              .slice(0, visibleLineCount())
              .map((line, lineIndex) => (
                <DiffViewerLine line={line} showLineNumbers={props.showLineNumbers} />
              ))}
      </DiffViewerContent>
      {isCollapsible() && (
        <div class="border-foreground/10 flex border-t px-3.5 py-1.5">
          <button
            type="button"
            aria-expanded={ariaState(!isCollapsed())}
            aria-controls={contentId}
            onClick={() => setIsExpanded((expanded) => !expanded)}
            class="text-muted-foreground hover:text-foreground text-[11px] font-medium transition-colors motion-reduce:transition-none"
          >
            {isCollapsed() ? `Show all ${renderedLineCount()} lines` : "Show less"}
          </button>
        </div>
      )}
    </DiffViewerFile>
  );
}

export type DiffViewerProps = VariantProps<typeof diffViewerVariants> & {
  code?: string | undefined;
  patch?: string | undefined;
  oldFile?: { content: string; name?: string | undefined } | undefined;
  newFile?: { content: string; name?: string | undefined } | undefined;
  viewMode?: "split" | "unified" | undefined;
  showLineNumbers?: boolean | undefined;
  showIcon?: boolean | undefined;
  showStats?: boolean | undefined;
  copyable?: boolean | undefined;
  maxCollapsedLines?: number | undefined;
  class?: string | undefined;
};

function DiffViewer(props: DiffViewerProps) {
  const diffPatch = createMemo(() => props.patch ?? props.code);
  const oldContent = createMemo(() => props.oldFile?.content);
  const oldName = createMemo(() => props.oldFile?.name);
  const newContent = createMemo(() => props.newFile?.content);
  const newName = createMemo(() => props.newFile?.name);

  const parsedFiles = createMemo<ParsedFile[]>(() => {
    if (diffPatch()) {
      return parsePatch(diffPatch()!);
    }
    if (oldContent() !== undefined && newContent() !== undefined) {
      const { lines, hunks, additions, deletions } = computeDiff(oldContent()!, newContent()!);
      return [
        {
          oldName: oldName(),
          newName: newName(),
          lines,
          hunks,
          additions,
          deletions,
        },
      ];
    }
    return [];
  });

  const splitLinePairs = createMemo<SplitLinePair[][]>(() => {
    if ((props.viewMode === undefined ? "unified" : props.viewMode) !== "split") return [];
    return parsedFiles().map((file) => pairLinesForSplit(file.lines));
  });

  return (
    <>
      {(() => {
        if (parsedFiles().length === 0) {
          return (
            <pre
              data-slot="diff-viewer"
              class={cn(
                "border-foreground/10 bg-foreground/[0.025] dark:bg-foreground/[0.04] text-muted-foreground border px-3.5 py-3 font-mono text-xs",
                props.class,
              )}
            >
              No diff content provided
            </pre>
          );
        }

        return (
          <div
            data-slot="diff-viewer"
            data-view-mode={props.viewMode === undefined ? "unified" : props.viewMode}
            data-variant={props.variant ?? "default"}
            data-size={props.size ?? "default"}
            class={cn(
              diffViewerVariants({ variant: props.variant, size: props.size }),
              props.class,
            )}
          >
            {parsedFiles().map((file, fileIndex) => (
              <DiffViewerFileContent
                file={file}
                splitLinePairs={splitLinePairs()[fileIndex] ?? []}
                viewMode={props.viewMode === undefined ? "unified" : props.viewMode}
                showLineNumbers={props.showLineNumbers === undefined ? true : props.showLineNumbers}
                showIcon={props.showIcon === undefined ? false : props.showIcon}
                showStats={props.showStats === undefined ? true : props.showStats}
                copyable={props.copyable === undefined ? true : props.copyable}
                maxCollapsedLines={props.maxCollapsedLines}
              />
            ))}
          </div>
        );
      })()}
    </>
  );
}

export type { ParsedFile, ParsedLine, SplitLinePair };

export {
  computeDiff,
  DiffViewer,
  DiffViewerContent,
  DiffViewerFile,
  DiffViewerFileBadge,
  DiffViewerHeader,
  DiffViewerLine,
  DiffViewerSplitLine,
  DiffViewerStats,
  diffLineTextVariants,
  diffLineVariants,
  diffViewerVariants,
  parsePatch,
};
