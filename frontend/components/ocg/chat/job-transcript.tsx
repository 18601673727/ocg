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

function TurnRow({
  turn,
  userLabel,
  attachmentContentUrl,
}: {
  turn: TranscriptTurn;
  userLabel: string;
  attachmentContentUrl?: (id: string) => string;
}) {
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const isUser = turn.role === "user";
  const note = isUser ? undefined : STATUS_NOTE[turn.status];
  return (
    <div className="flex gap-2.5">
      <span
        className={cn(
          "flex size-6 shrink-0 items-center justify-center rounded-md border border-border text-[10px] font-semibold",
          isUser ? "bg-muted text-muted-foreground" : "bg-primary text-primary-foreground",
        )}
        aria-hidden="true"
      >
        {isUser ? "U" : "A"}
      </span>
      <div className="min-w-0 flex-1">
        <div className="mb-1 flex items-center gap-2">
          <span className="text-xs font-semibold">{isUser ? userLabel : "Answer"}</span>
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
            aria-label={isUser ? "Copy the message" : "Copy the answer"}
          >
            <span role="status">{isCopied ? "Copied" : "Copy"}</span>
          </button>
        </div>
        <div
          className={cn(
            "text-base leading-7 text-foreground/90",
            isUser && "rounded-md border border-border bg-muted/30 px-3 py-2",
            turn.status === "failed" && STATUS_TEXT["error"],
            turn.status === "cancelled" && "text-muted-foreground italic",
          )}
        >
          {turn.attachments && turn.attachments.length > 0 ? (
            <div className="mb-3 flex flex-wrap gap-2">
              {turn.attachments.map((id) => {
                const url = attachmentContentUrl
                  ? attachmentContentUrl(id)
                  : `/api/v1/attachments/${encodeURIComponent(id)}/content`;
                return (
                  <a
                    key={id}
                    href={url}
                    target="_blank"
                    rel="noreferrer"
                    className="group relative block overflow-hidden rounded-md border border-border bg-background transition-shadow hover:shadow-md"
                  >
                    {/* eslint-disable-next-line @next/next/no-img-element */}
                    <img
                      src={url}
                      alt={id}
                      className="max-h-48 max-w-xs rounded object-contain"
                      loading="lazy"
                    />
                    <span className="absolute bottom-1 right-1 rounded bg-background/80 px-1 py-0.5 font-mono text-[10px] text-muted-foreground">
                      {id}
                    </span>
                  </a>
                );
              })}
            </div>
          ) : null}
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

/** One Job as its objective and answer; see `jobTranscript` for what each is made of. */
export function JobTranscript({
  turns,
  userLabel = "Objective",
  attachmentContentUrl,
}: {
  turns: readonly TranscriptTurn[];
  userLabel?: string;
  attachmentContentUrl?: (id: string) => string;
}) {
  return (
    <div className="flex flex-col gap-4" aria-live="polite">
      {turns.map((turn) => (
        <TurnRow
          key={turn.id}
          turn={turn}
          userLabel={userLabel}
          attachmentContentUrl={attachmentContentUrl}
        />
      ))}
    </div>
  );
}
