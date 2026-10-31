import type { ControlOutput } from "../contracts";

// A presentation view of a durable record. Non-result payload fields that do
// not affect the transcript stay in the database rather than being fabricated.
export type OutputView = Pick<
  ControlOutput,
  "seq" | "kind" | "created_at_us" | "summary" | "code" | "value"
>;
export type OutputSequence = Iterable<OutputView> & {
  readonly length: number;
  at(index: number): OutputView | undefined;
};

const RAW_TAIL_LIMIT = 256;

/** Ordered, rebuildable presentation data plus a bounded recent raw tail. */
export class OutputHistory implements OutputSequence {
  private readonly sequences: number[] = [];
  private readonly times: number[] = [];
  private readonly kinds: ControlOutput["kind"][] = [];
  private readonly codes: (string | null)[] = [];
  private readonly textStarts: number[] = [];
  private readonly textEnds: number[] = [];
  private readonly summaryStarts: number[] = [];
  private readonly summaryEnds: number[] = [];
  private text = "";
  private summaries = "";
  private tail: OutputView[] = [];
  private tailHead = 0;
  private latestResult: OutputView | null = null;
  private latestResultSeq: number | null = null;

  get length() {
    return this.sequences.length + this.tail.length;
  }
  get firstSeq() {
    return this.sequences[0] ?? this.tail[this.tailHead]?.seq ?? 1;
  }
  get lastSeq() {
    if (!this.tail.length) return 0;
    return this.tail[(this.tailHead + this.tail.length - 1) % this.tail.length].seq;
  }

  append(output: OutputView) {
    if (this.length && output.seq !== this.lastSeq + 1)
      throw new Error("Stored output sequence has a gap.");
    if (this.tail.length < RAW_TAIL_LIMIT) this.tail.push(output);
    else {
      this.compact(this.tail[this.tailHead]);
      this.tail[this.tailHead] = output;
      this.tailHead = (this.tailHead + 1) % RAW_TAIL_LIMIT;
    }
    if (output.kind === "result") {
      this.latestResult = output;
      this.latestResultSeq = output.seq;
    }
  }

  private compact(output: OutputView) {
    this.sequences.push(output.seq);
    this.times.push(output.created_at_us);
    this.kinds.push(output.kind);
    this.codes.push(output.code);
    const value = output.value;
    const text =
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      "text" in value &&
      typeof value.text === "string"
        ? value.text
        : null;
    this.textStarts.push(text === null ? -1 : this.text.length);
    if (text !== null) this.text += text;
    this.textEnds.push(this.text.length);
    const summary = typeof output.summary === "string" ? output.summary : null;
    this.summaryStarts.push(summary === null ? -1 : this.summaries.length);
    if (summary !== null) this.summaries += summary;
    this.summaryEnds.push(this.summaries.length);
  }

  at(index: number): OutputView | undefined {
    if (index < 0) index += this.length;
    if (index < 0 || index >= this.length) return undefined;
    if (index >= this.sequences.length)
      return this.tail[(this.tailHead + index - this.sequences.length) % this.tail.length];
    const seq = this.sequences[index];
    if (seq === this.latestResultSeq && this.latestResult) return this.latestResult;
    const start = this.textStarts[index],
      summaryStart = this.summaryStarts[index];
    return {
      seq,
      kind: this.kinds[index],
      created_at_us: this.times[index],
      code: this.codes[index],
      value: start < 0 ? null : { text: this.text.slice(start, this.textEnds[index]) },
      summary:
        summaryStart < 0 ? null : this.summaries.slice(summaryStart, this.summaryEnds[index]),
    };
  }

  *[Symbol.iterator]() {
    for (let index = 0; index < this.length; index++) {
      const output = this.at(index);
      if (output) yield output;
    }
  }
}

export function mergeOutputHistory(...groups: readonly Iterable<OutputView>[]): OutputHistory {
  const bySequence = new Map<number, OutputView>();
  for (const group of groups) for (const output of group) bySequence.set(output.seq, output);
  const history = new OutputHistory();
  for (const output of [...bySequence.values()].sort((a, b) => a.seq - b.seq))
    history.append(output);
  return history;
}
