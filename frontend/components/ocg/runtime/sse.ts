export type SseFrame = { event: string; data: string; id: string | null } | { comment: string };

// Each connection gets a fresh parser: an unfinished event at EOF is replayed,
// never completed with bytes from the next response.
export class SseParser {
  private line = "";
  private carriageReturn = false;
  private event = "message";
  private data: string[] = [];
  private id: string | null = null;
  private size = 0;

  constructor(private readonly maxFrameCharacters = 2 * 1024 * 1024) {}

  *push(text: string): Generator<SseFrame> {
    for (const character of text) {
      if (this.carriageReturn) {
        this.carriageReturn = false;
        if (character === "\n") continue;
      }
      if (++this.size > this.maxFrameCharacters)
        throw new Error("OCG SSE frame exceeds client limit");
      if (character !== "\r" && character !== "\n") {
        this.line += character;
        continue;
      }
      this.carriageReturn = character === "\r";
      const line = this.line;
      this.line = "";
      if (line === "") {
        const frame = this.data.length
          ? { event: this.event, data: this.data.join("\n"), id: this.id }
          : null;
        this.event = "message";
        this.data = [];
        this.id = null;
        this.size = 0;
        if (frame) yield frame;
      } else if (line.startsWith(":")) {
        yield { comment: line.slice(1).replace(/^ /, "") };
      } else {
        const colon = line.indexOf(":");
        const field = colon < 0 ? line : line.slice(0, colon);
        const value = colon < 0 ? "" : line.slice(colon + 1).replace(/^ /, "");
        if (field === "event") this.event = value;
        if (field === "data") this.data.push(value);
        if (field === "id" && !value.includes("\0")) this.id = value;
      }
    }
  }
}
