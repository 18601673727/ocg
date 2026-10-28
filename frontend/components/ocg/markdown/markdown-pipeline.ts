import MarkdownIt, {
  type MarkdownIt as MarkdownItType,
  type StateBlock,
  type StateInline,
} from "markdown-it";
import taskLists from "markdown-it-task-lists";
import hljs from "highlight.js/lib/common";

/**
 * One Markdown pipeline for every Chat surface.
 *
 * The parser is markdown-it with CommonMark plus the GFM extensions it ships
 * or that we implement here directly. Nothing in this module is allowed to
 * emit document-authored HTML: the parser runs with `html: false`, links and
 * images pass through {@link sanitizeUrl}, and KaTeX runs with `trust: false`.
 */

const DOLLAR = "$";

function lineBreakTag(state: StateInline, silent: boolean): boolean {
  if (state.src[state.pos] !== "<") return false;
  const match = /^<br[ \t]*\/?>/i.exec(state.src.slice(state.pos, state.posMax));
  if (!match) return false;
  if (!silent) state.push("hardbreak", "br", 0);
  state.pos += match[0].length;
  return true;
}

function isEscaped(source: string, position: number): boolean {
  let backslashes = 0;
  for (let index = position - 1; index >= 0 && source[index] === "\\"; index--) {
    backslashes += 1;
  }
  return backslashes % 2 === 1;
}

/** TeX rule: the opening delimiter may not be followed by whitespace and the closing one may not be preceded by it. */
function isMathDelimiter(source: string, open: number, close: number): boolean {
  if (open + 1 >= close) return false;
  if (/\s/.test(source[open + 1])) return false;
  if (/\s/.test(source[close - 1])) return false;
  return true;
}

/**
 * `$...$` inside a paragraph.
 *
 * An unterminated run is left as literal text, so a half-streamed line never
 * swallows the rest of the message.
 */
function mathInline(state: StateInline, silent: boolean): boolean {
  const source = state.src;
  const open = state.pos;

  if (source[open] !== DOLLAR || isEscaped(source, open)) return false;

  let close = open + 1;
  while (close < state.posMax) {
    if (source[close] === "\n") return false;
    if (source[close] === DOLLAR && !isEscaped(source, close)) break;
    close += 1;
  }
  if (close >= state.posMax) return false;
  if (!isMathDelimiter(source, open, close)) return false;

  if (!silent) {
    const token = state.push("math_inline", "math", 0);
    token.content = source.slice(open + 1, close);
    token.markup = DOLLAR;
  }
  state.pos = close + 1;
  return true;
}

/**
 * `$$...$$` as its own block, on one line or across several.
 *
 * An unclosed fence still produces a token covering the rest of the block, so
 * a stream that stops mid-equation renders the partial source rather than
 * nothing.
 */
function mathBlock(
  state: StateBlock,
  startLine: number,
  endLine: number,
  silent: boolean,
): boolean {
  const start = state.bMarks[startLine] + state.tShift[startLine];
  const startMax = state.eMarks[startLine];

  if (start + 2 > startMax) return false;
  if (state.src.slice(start, start + 2) !== DOLLAR + DOLLAR) return false;

  let firstLine = state.src.slice(start + 2, startMax);
  let nextLine = startLine + 1;
  let closed = false;

  const singleLine = firstLine.trimEnd();
  if (singleLine.length > 2 && singleLine.endsWith(DOLLAR + DOLLAR)) {
    firstLine = singleLine.slice(0, -2);
    closed = true;
  } else if (singleLine === DOLLAR + DOLLAR) {
    firstLine = "";
    closed = true;
  }

  const body: string[] = [];
  for (let line = startLine + 1; !closed && line < endLine; line++) {
    const lineStart = state.bMarks[line] + state.tShift[line];
    const lineMax = state.eMarks[line];
    const text = state.src.slice(lineStart, lineMax);
    const closeAt = text.indexOf(DOLLAR + DOLLAR);

    if (closeAt >= 0) {
      // Text before the closing delimiter stays part of the equation.
      const tail = text.slice(0, closeAt);
      if (tail.trim()) body.push(tail);
      nextLine = line + 1;
      closed = true;
      break;
    }
    body.push(text);
    nextLine = line + 1;
  }

  if (silent) return true;

  const token = state.push("math_block", "math", 0);
  token.block = true;
  token.markup = DOLLAR + DOLLAR;
  token.content = [firstLine, ...body].filter((part) => part !== "").join("\n");
  token.map = [startLine, nextLine];

  state.line = nextLine;
  return true;
}

const SAFE_PROTOCOLS = new Set(["http:", "https:", "mailto:"]);

/**
 * KaTeX's render options.
 *
 * `trust: false` is what keeps `\href`, `\url` and `\includegraphics` in a
 * source expression from becoming a live link or a remote fetch. Exported so
 * the guarantee can be asserted directly.
 */
export const KATEX_OPTIONS = {
  throwOnError: true,
  trust: false,
  strict: "error",
  output: "htmlAndMathml",
} as const;

/** Re-exported so the renderer walks the same token type the parser emits. */
export type { Token } from "markdown-it";

/**
 * Returns the URL to navigate to, or `null` when the scheme is not one this
 * app is willing to follow. `javascript:`, `data:` and protocol-relative
 * `//host` references are rejected, and the check is deliberately free of any
 * browser global so it holds during server rendering too.
 */
export function sanitizeUrl(rawUrl: string): string | null {
  const url = rawUrl.trim();
  if (/[\u0000-\u001f\u007f\\]/.test(url)) return null;
  if (!url) return null;
  // Protocol-relative references name another origin; they are not in-site.
  if (url.startsWith("//")) return null;
  // Anything without a scheme is a same-site reference: `/settings`,
  // `#anchor`, `./diagram.png`, `docs/readme.md`. Only a scheme can carry an
  // unsafe payload, so relative targets are admitted rather than parsed.
  if (!/^[a-z][a-z\d+.-]*:/i.test(url)) return url;
  try {
    return SAFE_PROTOCOLS.has(new URL(url).protocol) ? url : null;
  } catch {
    return null;
  }
}

export interface HighlightedCode {
  language: string;
  html: string;
}

/**
 * Syntax highlighting for a fenced block.
 *
 * highlight.js escapes the source it highlights, so the returned markup is
 * limited to its own token spans. Returns `null` when the fence names no
 * language or one highlight.js does not know, which leaves the caller to
 * render the plain code instead of guessing.
 */
export function highlightCode(code: string, info: string): HighlightedCode | null {
  const language = info.trim().split(/\s+/)[0]?.toLowerCase() ?? "";
  if (!language || language === "text" || !hljs.getLanguage(language)) return null;
  try {
    const { value } = hljs.highlight(code, { language, ignoreIllegals: true });
    return { language, html: value };
  } catch {
    return null;
  }
}

let instance: MarkdownItType | null = null;

export function markdownParser(): MarkdownItType {
  if (instance) return instance;

  instance = new MarkdownIt({
    // Never emit document-authored HTML; every element below is ours.
    html: false,
    breaks: true,
    linkify: true,
    typographer: true,
  })
    .use(taskLists, { enabled: true })
    .use((md) => {
      md.inline.ruler.after("escape", "line_break_tag", lineBreakTag);
      md.inline.ruler.after("escape", "math_inline", mathInline);
      md.block.ruler.after("blockquote", "math_block", mathBlock, {
        alt: ["paragraph", "reference", "blockquote", "list"],
      });
    });

  return instance;
}
