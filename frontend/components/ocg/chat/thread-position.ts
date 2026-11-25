import type { ConversationSearchMatch } from "../contracts/api";
import type { PresentedMessage } from "./message-presentation";

const POSITION_PREFIX = "ocg.chat.read-position.v1:";

export type ThreadSearchTarget = ConversationSearchMatch & { snippet?: string };

export type ThreadPosition = {
  message: string;
  turn?: string;
  offset: number;
};

export function readThreadPosition(conversation: string): ThreadPosition | undefined {
  try {
    const position = JSON.parse(
      sessionStorage.getItem(POSITION_PREFIX + conversation) ?? "null",
    ) as Partial<ThreadPosition> | null;
    if (
      typeof position?.message === "string" &&
      typeof position.offset === "number" &&
      Number.isFinite(position.offset)
    ) {
      return {
        message: position.message,
        turn: typeof position.turn === "string" ? position.turn : undefined,
        offset: position.offset,
      };
    }
  } catch {}
  return undefined;
}

export function writeThreadPosition(conversation: string, position?: ThreadPosition) {
  try {
    if (position) sessionStorage.setItem(POSITION_PREFIX + conversation, JSON.stringify(position));
    else sessionStorage.removeItem(POSITION_PREFIX + conversation);
  } catch {}
}

export function captureThreadPosition(viewport: HTMLElement): ThreadPosition | undefined {
  const top = viewport.getBoundingClientRect().top;
  let position: ThreadPosition | undefined;
  for (const row of viewport.querySelectorAll<HTMLElement>("[data-message-id]")) {
    const message = row.dataset.messageId;
    if (!message) continue;
    const bounds = row.getBoundingClientRect();
    position = { message, turn: row.dataset.turnId, offset: bounds.top - top };
    if (bounds.bottom > top) break;
  }
  return position;
}

function messageElement(viewport: HTMLElement, id: string) {
  return viewport.querySelector<HTMLElement>(`[data-message-id="${CSS.escape(id)}"]`);
}

export function savedMessageElement(viewport: HTMLElement, position: ThreadPosition) {
  const row = messageElement(viewport, position.message);
  if (row || !position.turn) return row;
  return viewport.querySelector<HTMLElement>(`[data-turn-id="${CSS.escape(position.turn)}"]`);
}

export function searchMessageElement(
  viewport: HTMLElement,
  messages: readonly PresentedMessage[],
  target: ThreadSearchTarget,
) {
  const role = target.answer ? "assistant" : "user";
  const excerpt = target.snippet?.replace(/^…|…$/g, "");
  let fallback: HTMLElement | null = null;
  let preferred: HTMLElement | null = null;
  for (const message of messages) {
    if (message.jobId !== target.turn) continue;
    const row = messageElement(viewport, message.id);
    if (!row) continue;
    fallback ??= row;
    if (message.role !== role) continue;
    if (excerpt && message.content.replace(/\s+/g, " ").includes(excerpt)) return row;
    if (!preferred || (target.answer && !message.intermediate)) preferred = row;
  }
  return preferred ?? fallback;
}
