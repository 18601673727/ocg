import { Clock3, LoaderCircle } from "lucide-react";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";

export function ConversationStatus({ state }: { state: "queued" | "running" }) {
  const { t } = useI18n();
  const running = state === "running";
  const Icon = running ? LoaderCircle : Clock3;
  const label = t(running ? "state.running" : "state.queued");

  return (
    <span
      role="img"
      aria-label={label}
      title={label}
      className={cn("shrink-0", running ? "text-primary" : "text-muted-foreground")}
    >
      <Icon
        className={cn("size-3.5", running && "animate-spin motion-reduce:animate-none")}
        aria-hidden="true"
      />
    </span>
  );
}
