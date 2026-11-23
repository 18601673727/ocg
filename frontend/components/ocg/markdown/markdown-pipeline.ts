import type { HLJSApi, LanguageFn } from "highlight.js";
import MarkdownIt, {
  type MarkdownIt as MarkdownItType,
  type StateBlock,
  type StateInline,
} from "markdown-it";
import taskLists from "markdown-it-task-lists";
import { createSignal } from "solid-js";

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

function isMathDelimiter(source: string, open: number, close: number): boolean {
  if (open + 1 >= close) return false;
  if (/\s/.test(source[open + 1])) return false;
  if (/\s/.test(source[close - 1])) return false;
  return true;
}

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
  if (close >= state.posMax) return false; // Keep an unfinished inline equation as text.
  if (!isMathDelimiter(source, open, close)) return false;

  if (!silent) {
    const token = state.push("math_inline", "math", 0);
    token.content = source.slice(open + 1, close);
    token.markup = DOLLAR;
  }
  state.pos = close + 1;
  return true;
}

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

export const KATEX_OPTIONS = {
  throwOnError: true,
  trust: false, // Disable links and external resources in equations.
  strict: "error",
  output: "htmlAndMathml",
} as const;

export type { Token } from "markdown-it";

export function sanitizeUrl(rawUrl: string): string | null {
  const url = rawUrl.trim();
  if (/[\u0000-\u001f\u007f\\]/.test(url)) return null;
  if (!url) return null;
  if (url.startsWith("//")) return null;
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

type Grammar = { name: string; read: () => HLJSApi | undefined };
const grammars = new Map<string, Grammar>();
const inFlight = new Map<string, Promise<boolean>>();
let core: Promise<HLJSApi> | undefined;

function loadCore(): Promise<HLJSApi> {
  core ??= import("highlight.js/lib/core")
    .then(({ default: hljs }) => {
      const engine = hljs.newInstance();
      // The first embedded match requests its grammar. registerLanguage replaces
      // this rule when the module arrives (e.g. XML for JSX, CSS for <style>).
      for (const name of ["xml", "css", "javascript", "graphql", "php", "python", "bash", "ruby"]) {
        engine.registerLanguage(name, () => ({
          contains: [{ match: /[\s\S]+/, "on:begin": () => grammars.get(name)?.read() }],
        }));
      }
      return engine;
    })
    .catch((error) => {
      core = undefined;
      throw error;
    });
  return core;
}

function defineGrammar(names: string, load: () => Promise<{ default: LanguageFn }>) {
  const aliases = names.split(" ");
  const name = aliases[0];
  const [ready, setReady] = createSignal<HLJSApi>();
  function read() {
    const loaded = ready();
    if (!loaded && !inFlight.has(name)) {
      const request = Promise.all([loadCore(), load()])
        .then(([engine, module]) => {
          engine.registerLanguage(name, module.default);
          setReady(engine);
          return true;
        })
        .catch(() => false)
        .finally(() => {
          inFlight.delete(name);
        });
      inFlight.set(name, request);
    }
    return loaded;
  }
  const grammar = { name, read };
  for (const alias of aliases) grammars.set(alias, grammar);
}

defineGrammar("bash sh zsh shell", () => import("highlight.js/lib/languages/bash"));
defineGrammar("c h", () => import("highlight.js/lib/languages/c"));
defineGrammar("cpp cc c++ h++ hpp hh hxx cxx", () => import("highlight.js/lib/languages/cpp"));
defineGrammar("csharp cs c#", () => import("highlight.js/lib/languages/csharp"));
defineGrammar("css", () => import("highlight.js/lib/languages/css"));
defineGrammar("diff patch", () => import("highlight.js/lib/languages/diff"));
defineGrammar("go golang", () => import("highlight.js/lib/languages/go"));
defineGrammar("graphql gql", () => import("highlight.js/lib/languages/graphql"));
defineGrammar("ini toml", () => import("highlight.js/lib/languages/ini"));
defineGrammar("java jsp", () => import("highlight.js/lib/languages/java"));
defineGrammar("javascript js jsx mjs cjs", () => import("highlight.js/lib/languages/javascript"));
defineGrammar("json jsonc json5", () => import("highlight.js/lib/languages/json"));
defineGrammar("kotlin kt kts ktm ktx", () => import("highlight.js/lib/languages/kotlin"));
defineGrammar("less", () => import("highlight.js/lib/languages/less"));
defineGrammar("lua pluto", () => import("highlight.js/lib/languages/lua"));
defineGrammar("makefile mk mak make", () => import("highlight.js/lib/languages/makefile"));
defineGrammar("markdown md mkdown mkd", () => import("highlight.js/lib/languages/markdown"));
defineGrammar(
  "objectivec mm objc obj-c obj-c++ objective-c++",
  () => import("highlight.js/lib/languages/objectivec"),
);
defineGrammar("perl pl pm", () => import("highlight.js/lib/languages/perl"));
defineGrammar("php", () => import("highlight.js/lib/languages/php"));
defineGrammar("php-template", () => import("highlight.js/lib/languages/php-template"));
defineGrammar("python py gyp ipython", () => import("highlight.js/lib/languages/python"));
defineGrammar("python-repl pycon", () => import("highlight.js/lib/languages/python-repl"));
defineGrammar("r", () => import("highlight.js/lib/languages/r"));
defineGrammar("ruby rb gemspec podspec thor irb", () => import("highlight.js/lib/languages/ruby"));
defineGrammar("rust rs", () => import("highlight.js/lib/languages/rust"));
defineGrammar("scss", () => import("highlight.js/lib/languages/scss"));
defineGrammar("console shellsession", () => import("highlight.js/lib/languages/shell"));
defineGrammar("sql", () => import("highlight.js/lib/languages/sql"));
defineGrammar("swift", () => import("highlight.js/lib/languages/swift"));
defineGrammar("typescript ts tsx mts cts", () => import("highlight.js/lib/languages/typescript"));
defineGrammar("vbnet vb", () => import("highlight.js/lib/languages/vbnet"));
defineGrammar("wasm", () => import("highlight.js/lib/languages/wasm"));
defineGrammar(
  "xml html xhtml rss atom xjb xsd xsl plist wsf svg",
  () => import("highlight.js/lib/languages/xml"),
);
defineGrammar("yaml yml", () => import("highlight.js/lib/languages/yaml"));

export function highlightCode(code: string, info: string): HighlightedCode | null {
  const language = info.trim().split(/\s+/, 1)[0].toLowerCase();
  const grammar = grammars.get(language);
  const hljs = grammar?.read();
  if (!grammar || !hljs) return null;
  try {
    const { value } = hljs.highlight(code, { language: grammar.name, ignoreIllegals: true });
    return { language, html: value };
  } catch {
    return null;
  }
}

let instance: MarkdownItType | null = null;

export function markdownParser(): MarkdownItType {
  if (instance) return instance;

  instance = new MarkdownIt({
    html: false,
    breaks: true,
    linkify: true,
    typographer: true,
  })
    .disable("smartquotes")
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
