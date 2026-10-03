"use client";

/**
 * Accessible Project switcher built from the existing Button / Dialog / Tooltip
 * primitives. Controlled: the active project and the change callback are owned
 * by the shell.
 */

import { useState } from "react";
import { Check, ChevronsUpDown, FolderKanban, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogClose,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { ProjectId, ProjectSummary } from "./domain";
import { useI18n } from "../i18n";

export type ProjectSwitcherProps = {
  projects: readonly ProjectSummary[];
  activeProjectId: ProjectId;
  /** Compact icon-only trigger used by the collapsed sidebar rail. */
  collapsed?: boolean;
  onChange: (id: ProjectId) => void;
};

export function ProjectSwitcher({
  projects,
  activeProjectId,
  collapsed = false,
  onChange,
}: ProjectSwitcherProps) {
  const [open, setOpen] = useState(false);
  const { t } = useI18n();
  const active = projects.find((project) => project.id === activeProjectId) ?? null;
  if (projects.length === 0) return null;
  // No silent selection: when nothing is selected, the trigger says so and
  // no entry is marked active until the operator picks one explicitly.
  const triggerLabel = active ?? { id: "" as ProjectId, name: t("project.select") };
  const triggerAria = active
    ? t("project.switchCurrent", { name: active.name })
    : t("project.select");
  const collapsedAria = active
    ? t("project.switchCollapsed", { name: active.name })
    : t("project.select");

  const handleSelect = (id: ProjectId) => {
    setOpen(false);
    if (id !== activeProjectId) onChange(id);
  };

  const expandedTrigger = (
    <DialogTrigger
      render={
        <Button
          variant="outline"
          size="sm"
          className="w-full justify-start"
          aria-label={triggerAria}
        >
          <FolderKanban className="size-3.5" data-icon="inline-start" aria-hidden="true" />
          <span className="min-w-0 flex-1 truncate text-left text-[12px] tracking-normal normal-case">
            {triggerLabel.name}
          </span>
          <ChevronsUpDown className="size-3.5 opacity-60" aria-hidden="true" />
        </Button>
      }
    />
  );

  const collapsedTrigger = (
    <Tooltip>
      <TooltipTrigger
        render={
          <DialogTrigger
            render={
              <Button
                variant="ghost"
                size="icon-sm"
                aria-label={collapsedAria}
                title={collapsedAria}
              >
                <FolderKanban className="size-4" aria-hidden="true" />
              </Button>
            }
          />
        }
      />
      <TooltipContent side="right">{collapsedAria}</TooltipContent>
    </Tooltip>
  );

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      {collapsed ? collapsedTrigger : expandedTrigger}
      <DialogContent showCloseButton={false}>
        <DialogClose render={<Button variant="ghost" className="absolute top-5 right-5 bg-secondary" size="icon-sm" aria-label={t("common.close")} />}><X aria-hidden="true" /></DialogClose>
        <DialogHeader>
          <DialogTitle>{t("project.switch")}</DialogTitle>
          <DialogDescription>
            {t("project.contextNote")}
          </DialogDescription>
        </DialogHeader>
        <ul role="listbox" aria-label={t("project.projects")} className="flex flex-col gap-1">
          {projects.map((project) => {
            const isActive = active !== null && project.id === active.id;
            return (
              <li key={project.id}>
                <button
                  type="button"
                  role="option"
                  aria-selected={isActive}
                  onClick={() => handleSelect(project.id)}
                  className={cn(
                    "flex w-full items-center gap-2 rounded-md border border-transparent px-2.5 py-2 text-left text-[13px] transition-colors",
                    isActive
                      ? "bg-muted font-medium text-foreground"
                      : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                  )}
                >
                  <span className="min-w-0 flex-1">
                    <span className="block truncate">{project.name}</span>
                    <span className="block break-all text-[11px] font-normal text-muted-foreground">{project.root ?? project.id}</span>
                  </span>
                  {isActive && <Check className="size-3.5 shrink-0" aria-hidden="true" />}
                </button>
              </li>
            );
          })}
        </ul>
      </DialogContent>
    </Dialog>
  );
}
