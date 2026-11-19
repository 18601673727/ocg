import { createEffect } from "solid-js";
import { Button } from "@/components/ui/button";
import { bindElementRef, elementRef } from "@/lib/ui-state";
import { useI18n } from "../i18n";

export function LoadMore(props: { loading: boolean; onLoad: () => void; auto?: boolean }) {
  const ref = elementRef<HTMLDivElement>(null);
  const loadingRef = elementRef(props.loading);
  const onLoadRef = elementRef(props.onLoad);
  createEffect(
    () => [props.loading],
    () => {
      loadingRef.current = props.loading;
    },
  );
  createEffect(
    () => [props.onLoad],
    () => {
      onLoadRef.current = props.onLoad;
    },
  );
  const i18n = useI18n();
  createEffect(
    () => [props.auto === undefined ? true : props.auto],
    () => {
      const element = ref.current;
      if (!element || !(props.auto === undefined ? true : props.auto)) return;
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
    },
  );
  return (
    <div ref={bindElementRef(ref)} class="flex justify-center py-4">
      <Button
        type="button"
        variant="outline"
        size="sm"
        disabled={props.loading}
        onClick={props.onLoad}
      >
        {i18n.t(props.loading ? "common.loading" : "common.loadMore")}
      </Button>
    </div>
  );
}
