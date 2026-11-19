import { Clock3, LoaderCircle } from "@lucide/solid";
import { Dynamic } from "@solidjs/web";
import { createMemo } from "solid-js";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";

export function ConversationStatus(props: { state: "queued" | "running" }) {
  const i18n = useI18n();
  const running = createMemo(() => props.state === "running");
  const Icon = createMemo(() => (running() ? LoaderCircle : Clock3));
  const label = createMemo(() => i18n.t(running() ? "state.running" : "state.queued"));

  return (
    <span
      role="img"
      aria-label={label()}
      title={label()}
      class={cn("shrink-0", running() ? "text-primary" : "text-muted-foreground")}
    >
      <Dynamic
        component={Icon()}
        class={cn("size-3.5", running() && "animate-spin motion-reduce:animate-none")}
        aria-hidden="true"
      />
    </span>
  );
}
