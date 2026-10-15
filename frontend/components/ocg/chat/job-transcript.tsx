"use client";

import { cn } from "@/lib/utils";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { STATUS_TEXT } from "@/components/ocg/primitives";
import { BoundedMessage } from "./bounded-message";
import type { TranscriptTurn } from "./job-transcript-model";

const STATUS_NOTE: Partial<Record<TranscriptTurn["status"], string>> = {
  streaming: "Receiving output…",
  pending: "Waiting for output…",
  failed: "The Job failed.",
  cancelled: "The Job was cancelled.",
  unknown: "The Job's outcome is unknown. Check its record before acting on this output.",
};

function TurnRow({ turn }: { turn: TranscriptTurn }) {
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const note = STATUS_NOTE[turn.status];
  return (
    <div className="flex gap-2.5">
      <span
        className="flex size-6 shrink-0 items-center justify-center rounded-md border border-border bg-primary text-[10px] font-semibold text-primary-foreground"
        aria-hidden="true"
      >
        A
      </span>
      <div className="min-w-0 flex-1">
        <div className="mb-1 flex items-center gap-2">
          <span className="text-xs font-semibold">Answer</span>
          {turn.createdAtUs === null ? null : (
            <span className="text-xs text-muted-foreground">
              {new Date(turn.createdAtUs / 1000).toLocaleTimeString()}
            </span>
          )}
          <button
            type="button"
            onClick={() => copyToClipboard(turn.content)}
            disabled={turn.content.length === 0}
            className="ml-auto inline-flex shrink-0 items-center gap-1 rounded px-1.5 py-1 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-ring disabled:cursor-not-allowed disabled:opacity-40"
            aria-label="Copy the answer"
          >
            <span role="status">{isCopied ? "Copied" : "Copy"}</span>
          </button>
        </div>
        <div
          className={cn(
            "text-base leading-7 text-foreground/90",
            turn.status === "failed" && STATUS_TEXT["error"],
            turn.status === "cancelled" && "text-muted-foreground italic",
          )}
        >
          <BoundedMessage streaming={turn.status === "streaming"}>
            {/* The Provider's text is shown as text: it is never interpreted as markup. */}
            <p className="whitespace-pre-wrap break-words">{turn.content}</p>
          </BoundedMessage>
          {turn.detail ? (
            <p role="alert" className="mt-2 whitespace-pre-wrap break-words text-xs">
              {turn.detail}
            </p>
          ) : null}
          {note ? <span className="mt-1 block text-xs text-muted-foreground">{note}</span> : null}
        </div>
      </div>
    </div>
  );
}

/** One Job's answer; see `jobTranscript` for what it is made of. */
export function JobTranscript({ turns }: { turns: readonly TranscriptTurn[] }) {
  return (
    <div className="flex flex-col gap-4" aria-live="polite">
      {turns.map((turn) => (
        <TurnRow key={turn.id} turn={turn} />
      ))}
    </div>
  );
}
