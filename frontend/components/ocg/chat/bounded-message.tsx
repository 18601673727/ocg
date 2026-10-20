"use client";

import { useEffect, useRef, useState } from "react";
import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";
import { BOUNDED_MESSAGE_CLASS, boundedToggle, followAfterScroll } from "./message-bounds";

/**
 * A long message scrolls inside its own bounded viewport until the reader
 * expands it. A streaming message keeps its newest output in view until the
 * reader scrolls up inside it.
 *
 * This is the single bounded-message presentation shared by every Chat
 * surface: the assistant-ui Thread messages render through it, so long
 * messages stay bounded and expandable on the active surface too.
 */
export function BoundedMessage({ streaming, children }: { streaming: boolean; children: React.ReactNode }) {
  const { t } = useI18n();
  const viewport = useRef<HTMLDivElement>(null);
  const content = useRef<HTMLDivElement>(null);
  const following = useRef(true);
  const lastTop = useRef(0);
  const [expanded, setExpanded] = useState(false);
  const [overflowing, setOverflowing] = useState(false);

  useEffect(() => {
    const el = viewport.current;
    const inner = content.current;
    if (!el || !inner) return;
    const observer = new ResizeObserver(() => {
      if (!expanded && streaming && following.current) {
        el.scrollTop = el.scrollHeight;
        lastTop.current = el.scrollTop;
      }
      setOverflowing(el.scrollHeight - el.clientHeight > 1);
    });
    observer.observe(inner);
    observer.observe(el);
    return () => observer.disconnect();
  }, [expanded, streaming]);

  const toggle = boundedToggle(overflowing, expanded);
  return (
    <>
      <div
        ref={viewport}
        data-message-bounds={expanded ? "expanded" : "bounded"}
        onScroll={() => {
          const el = viewport.current;
          if (!el || expanded) return;
          following.current = followAfterScroll(following.current, lastTop.current, el);
          lastTop.current = el.scrollTop;
        }}
        className={cn("min-w-0", !expanded && BOUNDED_MESSAGE_CLASS)}
      >
        <div ref={content}>{children}</div>
      </div>
      {toggle && <button
        type="button"
        aria-expanded={expanded}
        onClick={() => setExpanded(value => !value)}
        className="mt-1 inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-ring"
      >
        <ChevronDown className={cn("size-3 transition-transform", expanded && "rotate-180")} aria-hidden="true" />
        {t(toggle === "collapse" ? "chat.collapseMessage" : "chat.expandMessage")}
      </button>}
    </>
  );
}
