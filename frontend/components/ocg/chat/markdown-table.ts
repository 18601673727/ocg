export type MarkdownTable = {
  headers: string[];
  alignments: ("left" | "center" | "right")[];
  rows: string[][];
  nextLine: number;
};

function splitRow(line: string): { cells: string[]; hasPipe: boolean } {
  const text = line.trim();
  const cells: string[] = [];
  let cell = "";
  let hasPipe = false;
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    const next = text[index + 1];
    if (character === "\\" && (next === "|" || next === "\\")) {
      cell += next;
      index += 1;
    } else if (character === "|") {
      cells.push(cell.trim());
      cell = "";
      hasPipe = true;
    } else {
      cell += character;
    }
  }
  cells.push(cell.trim());
  if (text.startsWith("|") && cells[0] === "") cells.shift();
  if (text.endsWith("|") && cells.at(-1) === "") cells.pop();
  return { cells, hasPipe };
}

export function parseMarkdownTable(lines: readonly string[], startLine: number): MarkdownTable | null {
  const header = lines[startLine];
  const delimiter = lines[startLine + 1];
  if (!header?.trim() || !delimiter?.trim()) return null;
  const { cells: headers, hasPipe: headerHasPipe } = splitRow(header);
  const { cells: delimiters, hasPipe: delimiterHasPipe } = splitRow(delimiter);
  if ((!headerHasPipe && !delimiterHasPipe) || !headers.length || headers.length !== delimiters.length || delimiters.some(cell => !/^:?-+:?$/.test(cell))) return null;

  const alignments = delimiters.map((cell): "left" | "center" | "right" => {
    if (cell.startsWith(":") && cell.endsWith(":")) return "center";
    return cell.endsWith(":") ? "right" : "left";
  });
  const rows: string[][] = [];
  let nextLine = startLine + 2;
  while (nextLine < lines.length) {
    const line = lines[nextLine].trim();
    if (!line || /^(?:#{1,6}(?:\s|$)|>|[-+*]\s|\d+[.)]\s|`{3,}|~{3,})/.test(line) || /^(?:[-*_]\s*){3,}$/.test(line)) break;
    const { cells } = splitRow(line);
    rows.push(headers.map((_, index) => cells[index] ?? ""));
    nextLine += 1;
  }
  return { headers, alignments, rows, nextLine };
}
