"use client";

import {
  createContext,
  useContext,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { cn } from "@/lib/utils";

export const PageTitleVisibilityContext = createContext<((visible: boolean) => void) | null>(null);

export function PageTitle({ children, className }: { children: ReactNode; className?: string }) {
  const onVisibilityChange = useContext(PageTitleVisibilityContext);
  const titleRef = useRef<HTMLHeadingElement>(null);
  const [visible, setVisible] = useState(true);

  useLayoutEffect(() => {
    const title = titleRef.current;
    if (!title || !onVisibilityChange) return;
    onVisibilityChange(true);
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (!entry) return;
        const next = entry.isIntersecting && entry.intersectionRatio >= 0.99;
        setVisible(next);
        onVisibilityChange(next);
      },
      { threshold: [0, 0.99] },
    );
    observer.observe(title);
    return () => observer.disconnect();
  }, [onVisibilityChange]);

  return (
    <h1
      ref={titleRef}
      aria-hidden={!visible}
      className={cn(
        "transition-opacity duration-200 ease-out motion-reduce:transition-none",
        visible ? "opacity-100" : "opacity-0",
        className,
      )}
    >
      {children}
    </h1>
  );
}
