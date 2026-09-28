"use client";

/**
 * Accessible Project switcher built from the existing Button / Dialog / Tooltip
 * primitives. Controlled: the active project and the change callback are owned
 * by the shell.
 */

import { useState } from "react";
import { Check, ChevronsUpDown, FolderKanban } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
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
import { projectSwitcherLabel } from "./domain";

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
  const active = projects.find((project) => project.id === activeProjectId) ?? projects[0];
  if (!active) return null;

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
          aria-label={projectSwitcherLabel(active)}
        >
          <FolderKanban className="size-3.5" data-icon="inline-start" aria-hidden="true" />
          <span className="min-w-0 flex-1 truncate text-left text-[12px] tracking-normal normal-case">
            {active.name}
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
                aria-label={projectSwitcherLabel(active, true)}
                title={projectSwitcherLabel(active, true)}
              >
                <FolderKanban className="size-4" aria-hidden="true" />
              </Button>
            }
          />
        }
      />
      <TooltipContent side="right">{projectSwitcherLabel(active, true)}</TooltipContent>
    </Tooltip>
  );

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      {collapsed ? collapsedTrigger : expandedTrigger}
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Switch project</DialogTitle>
          <DialogDescription>
            Project context scopes sessions, attention, and the resource ledger.
          </DialogDescription>
        </DialogHeader>
        <ul role="listbox" aria-label="Projects" className="flex flex-col gap-1">
          {projects.map((project) => {
            const isActive = project.id === active.id;
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
                  <span className="min-w-0 flex-1 truncate">{project.name}</span>
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
