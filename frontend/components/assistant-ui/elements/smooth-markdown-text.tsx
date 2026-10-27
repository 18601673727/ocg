"use client";

import { useEffect, useRef, useState } from "react";
import { MarkdownText } from "./markdown-text";

/** Smooth bursts from the provider without changing the Markdown renderer. */
export function SmoothMarkdownText({ text, streaming }: { text: string; streaming: boolean }) {
  const [shown, setShown] = useState(text);
  const latest = useRef(text);
  const displayed = useRef(text);

  useEffect(() => {
    latest.current = text;
  }, [text]);

  useEffect(() => {
    if (!streaming) return;
    const motion = window.matchMedia("(prefers-reduced-motion: reduce)");
    let frame = 0;
    let previous = 0;
    const tick = (now: number) => {
      if (!previous) previous = now;
      if (now - previous >= 40) {
        const target = latest.current;
        const current = displayed.current;
        let next = target;
        if (!motion.matches && target.startsWith(current)) {
          const remaining = target.length - current.length;
          const count = Math.max(1, Math.ceil(((55 + remaining * 4) * (now - previous)) / 1000));
          let end = Math.min(target.length, current.length + count);
          // Avoid displaying half of a UTF-16 surrogate pair.
          const last = target.charCodeAt(end - 1);
          if (end < target.length && last >= 0xd800 && last <= 0xdbff) end++;
          next = target.slice(0, end);
        }
        if (next !== current) {
          displayed.current = next;
          setShown(next);
        }
        previous = now;
      }
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [streaming]);

  return (
    <div
      className={
        streaming
          ? "[&_.aui-md>*]:animate-in [&_.aui-md>*]:fade-in [&_.aui-md>*]:duration-300 motion-reduce:[&_.aui-md>*]:animate-none"
          : undefined
      }
    >
      <MarkdownText text={streaming ? shown : text} />
    </div>
  );
}
