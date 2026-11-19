import { createMemo, createSignal } from "solid-js";
import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";
import { JobTree } from "./job-tree";
import { JobWatch } from "./job-watch";

export function JobDetailsDialog(props: {
  client: ApiClient;
  job: string | null;
  onJob: (job: string | null) => void;
  onOpenChat: (job: string) => void;
}) {
  const i18n = useI18n();
  const [treeSelection, setTreeSelection] = createSignal<{ job: string; root: string } | null>(
    null,
  );
  if (treeSelection() !== null && treeSelection()!.job !== props.job) setTreeSelection(null);
  const treeRoot = createMemo(() =>
    treeSelection()?.job === props.job ? treeSelection()!.root : props.job,
  );

  return (
    <Dialog
      open={props.job !== null}
      onOpenChange={(open) => {
        if (!open) props.onJob(null);
      }}
    >
      <DialogContent
        class="flex h-[calc(100dvh-2rem)] max-h-[calc(100dvh-2rem)] w-[calc(100vw-2rem)] max-w-none flex-col gap-0 overflow-hidden p-0 sm:max-w-none"
        closeButtonClassName="top-2"
      >
        <div class="border-b border-border px-6 py-4 pr-14">
          <DialogTitle>{i18n.t("jobs.detailsTitle")}</DialogTitle>
          <DialogDescription class="sr-only">{i18n.t("jobs.tree.description")}</DialogDescription>
        </div>
        {props.job && treeRoot() ? (
          <div class="grid min-h-0 flex-1 grid-cols-1 overflow-auto lg:grid-cols-[minmax(18rem,0.8fr)_minmax(0,1.6fr)] lg:overflow-hidden">
            <section class="min-w-0 border-b border-border lg:flex lg:min-h-0 lg:flex-col lg:border-r lg:border-b-0">
              <h2 class="sticky top-0 z-10 flex min-h-16 shrink-0 items-center border-b border-border bg-popover px-5 py-3 font-heading text-lg lg:static">
                {i18n.t("jobs.tree.title")}
              </h2>
              <div class="ocg-subtle-scrollbar min-h-0 p-5 lg:flex-1 lg:overflow-auto">
                <p class="mb-4 text-sm text-muted-foreground">{i18n.t("jobs.tree.description")}</p>
                <JobTree
                  client={props.client}
                  root={treeRoot()!}
                  selected={props.job}
                  onSelect={(next) => {
                    if (next !== props.job) setTreeSelection({ job: next, root: treeRoot()! });
                    props.onJob(next);
                  }}
                  onReroot={(next) => {
                    setTreeSelection(null);
                    props.onJob(next);
                  }}
                  onOpenChat={props.onOpenChat}
                />
              </div>
            </section>
            <div class="min-w-0 lg:min-h-0">
              <JobWatch client={props.client} jobId={props.job} bare />
            </div>
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  );
}
