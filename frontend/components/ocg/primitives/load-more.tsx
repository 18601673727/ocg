"use client";

import { useEffect, useRef } from "react";
import { Button } from "@/components/ui/button";
import { useI18n } from "../i18n";

export function LoadMore({
  loading,
  onLoad,
  auto = true,
}: {
  loading: boolean;
  onLoad: () => void;
  auto?: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const loadingRef = useRef(loading);
  const onLoadRef = useRef(onLoad);
  useEffect(() => {
    loadingRef.current = loading;
  }, [loading]);
  useEffect(() => {
    onLoadRef.current = onLoad;
  }, [onLoad]);
  const { t } = useI18n();
  useEffect(() => {
    const element = ref.current;
    if (!element || !auto) return;
    let readyToLoad = true;
    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries.some((entry) => entry.isIntersecting);
        if (!visible) {
          readyToLoad = true;
          return;
        }
        if (readyToLoad && !loadingRef.current) {
          readyToLoad = false;
          onLoadRef.current();
        }
      },
      { rootMargin: "200px" },
    );
    observer.observe(element);
    return () => observer.disconnect();
  }, [auto]);
  return (
    <div ref={ref} className="flex justify-center py-4">
      <Button type="button" variant="outline" size="sm" disabled={loading} onClick={onLoad}>
        {t(loading ? "common.loading" : "common.loadMore")}
      </Button>
    </div>
  );
}
