"use client";

import { useState } from "react";
import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { JobTree } from "./job-tree";
import { JobWatch } from "./job-watch";

export function JobDetailsDialog({
  client,
  job,
  onJob,
  onOpenChat,
}: {
  client: HttpCanonicalControlClient;
  job: string | null;
  onJob: (job: string | null) => void;
  onOpenChat: (job: string) => void;
}) {
  const { t } = useI18n();
  const [treeSelection, setTreeSelection] = useState<{ job: string; root: string } | null>(null);
  if (treeSelection !== null && treeSelection.job !== job) setTreeSelection(null);
  const treeRoot = treeSelection?.job === job ? treeSelection.root : job;

  return (
    <Dialog
      open={job !== null}
      onOpenChange={(open) => {
        if (!open) onJob(null);
      }}
    >
      <DialogContent
        className="flex h-[calc(100dvh-2rem)] max-h-[calc(100dvh-2rem)] w-[calc(100vw-2rem)] max-w-none flex-col gap-0 overflow-hidden p-0 sm:max-w-none"
        closeButtonClassName="top-2"
      >
        <div className="border-b border-border px-6 py-4 pr-14">
          <DialogTitle>{t("jobs.detailsTitle")}</DialogTitle>
          <DialogDescription className="sr-only">{t("jobs.tree.description")}</DialogDescription>
        </div>
        {job && treeRoot ? (
          <div className="grid min-h-0 flex-1 grid-cols-1 overflow-auto lg:grid-cols-[minmax(18rem,0.8fr)_minmax(0,1.6fr)] lg:overflow-hidden">
            <section className="min-w-0 border-b border-border lg:flex lg:min-h-0 lg:flex-col lg:border-r lg:border-b-0">
              <h2 className="sticky top-0 z-10 flex min-h-16 shrink-0 items-center border-b border-border bg-popover px-5 py-3 font-heading text-lg lg:static">
                {t("jobs.tree.title")}
              </h2>
              <div className="ocg-subtle-scrollbar min-h-0 p-5 lg:flex-1 lg:overflow-auto">
                <p className="mb-4 text-sm text-muted-foreground">{t("jobs.tree.description")}</p>
                <JobTree
                  key={treeRoot}
                  client={client}
                  root={treeRoot}
                  selected={job}
                  onSelect={(next) => {
                    if (next !== job) setTreeSelection({ job: next, root: treeRoot });
                    onJob(next);
                  }}
                  onReroot={(next) => {
                    setTreeSelection(null);
                    onJob(next);
                  }}
                  onOpenChat={onOpenChat}
                />
              </div>
            </section>
            <div className="min-w-0 lg:min-h-0">
              <JobWatch key={job} client={client} jobId={job} bare />
            </div>
          </div>
        ) : null}
      </DialogContent>
    </Dialog>
  );
}
