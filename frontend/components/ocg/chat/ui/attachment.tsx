import { X } from "@lucide/solid";
import type { JSX } from "@solidjs/web";
import { Show } from "solid-js";
import { useI18n } from "@/components/ocg/i18n";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle, DialogTrigger } from "@/components/ui/dialog";

export function AttachmentTile(props: {
  src: string;
  name: string;
  isComposer?: boolean;
  isUploading?: boolean;
  uploadProgress?: number;
  isError?: boolean;
  errorMessage?: string;
  remove?: JSX.Element;
}) {
  const { t } = useI18n();
  return (
    <div class="group relative inline-flex items-center gap-2 rounded-lg border border-border bg-background px-2 py-1">
      <Dialog>
        <DialogTrigger
          render={
            <button
              type="button"
              class="flex min-w-0 items-center gap-2 rounded text-left text-xs focus-visible:outline-2"
              title={props.name}
            >
              <span class="relative size-12 shrink-0">
                <img alt="" src={props.src} class="size-full rounded object-cover" />
                <Show when={props.isUploading}>
                  <span
                    role="progressbar"
                    aria-label={`${t("chat.imageUploading")} ${props.name}`}
                    aria-valuemin={0}
                    aria-valuemax={100}
                    aria-valuenow={Math.round((props.uploadProgress ?? 0) * 100)}
                    class="absolute inset-0 grid place-items-center rounded bg-background/70 text-foreground"
                  >
                    <svg viewBox="0 0 36 36" class="size-8 -rotate-90" aria-hidden="true">
                      <circle
                        cx="18"
                        cy="18"
                        r="15"
                        fill="none"
                        stroke="currentColor"
                        stroke-width="2.5"
                        opacity="0.2"
                      />
                      <circle
                        cx="18"
                        cy="18"
                        r="15"
                        fill="none"
                        stroke="currentColor"
                        stroke-width="2.5"
                        stroke-linecap="round"
                        pathLength="100"
                        stroke-dasharray="100"
                        stroke-dashoffset={(1 - (props.uploadProgress ?? 0)) * 100}
                      />
                    </svg>
                    <span class="absolute text-[9px] font-medium tabular-nums">
                      {Math.round((props.uploadProgress ?? 0) * 100)}%
                    </span>
                  </span>
                </Show>
              </span>
              <span class="max-w-36 truncate">{props.name}</span>
            </button>
          }
        />
        <DialogContent class="sm:max-w-[min(64rem,calc(100vw-2rem))]">
          <DialogTitle>{props.name}</DialogTitle>
          <img src={props.src} alt={props.name} class="max-h-[65dvh] w-full object-contain" />
        </DialogContent>
      </Dialog>
      <Show when={props.isError && props.errorMessage}>
        <span role="alert" class="text-xs text-destructive">
          {props.errorMessage}
        </span>
      </Show>
      {props.remove}
    </div>
  );
}

export function AttachmentTileRemove(props: {
  name: string;
  disabled?: boolean;
  onClick: () => void;
}) {
  const { t } = useI18n();
  return (
    <Button
      type="button"
      aria-label={`${t("common.remove")} ${props.name}`}
      variant="ghost"
      size="icon-xs"
      disabled={props.disabled}
      onClick={props.onClick}
    >
      <X />
    </Button>
  );
}
