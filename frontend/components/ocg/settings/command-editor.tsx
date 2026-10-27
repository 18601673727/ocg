"use client";

import { useState, type ReactNode } from "react";
import { Check, ChevronDown, ChevronRight, Plus, Search, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { useI18n, type I18nKey } from "../i18n";
import type { ControlProjectCommand } from "../runtime/control-client";
import { nextCommandDraftId, toProjectCommands, type CommandDraft } from "./command-drafts";

type CommandGroup = {
  id: string;
  title: I18nKey;
  description: I18nKey;
  commands: ControlProjectCommand[];
};

const COMMAND_GROUPS: CommandGroup[] = [
  {
    id: "git",
    title: "settings.projects.groupGit",
    description: "settings.projects.groupGitNote",
    commands: [
      { name: "git_status", argv: ["git", "status", "--short", "--branch"] },
      { name: "git_diff", argv: ["git", "diff"] },
      { name: "git_log", argv: ["git", "log"] },
      { name: "git_branch", argv: ["git", "branch"] },
      { name: "git_show", argv: ["git", "show"] },
      { name: "git_merge_base", argv: ["git", "merge-base"] },
      { name: "git_rev_parse", argv: ["git", "rev-parse"] },
      { name: "git_ls_files", argv: ["git", "ls-files"] },
    ],
  },
  {
    id: "rust",
    title: "settings.projects.groupRust",
    description: "settings.projects.groupRustNote",
    commands: [
      { name: "cargo_build", argv: ["cargo", "build", "--locked"] },
      {
        name: "cargo_clippy",
        argv: ["cargo", "clippy", "--locked", "--all-targets", "--", "-D", "warnings"],
      },
      { name: "cargo_fmt_check", argv: ["cargo", "fmt", "--all", "--check"] },
      { name: "cargo_bench_compile", argv: ["cargo", "bench", "--locked", "--no-run"] },
    ],
  },
  {
    id: "node",
    title: "settings.projects.groupNode",
    description: "settings.projects.groupNodeNote",
    commands: [],
  },
  {
    id: "python",
    title: "settings.projects.groupPython",
    description: "settings.projects.groupPythonNote",
    commands: [{ name: "pytest", argv: ["python3", "-m", "pytest"] }],
  },
];

function hasPreset(commands: readonly CommandDraft[], preset: ControlProjectCommand) {
  return toProjectCommands(commands).some(
    (command) =>
      command.status !== "blocked" &&
      (command.name === preset.name ||
        (command.argv.length === preset.argv.length &&
          command.argv.every((arg, index) => arg === preset.argv[index]))),
  );
}

function isEmpty(command: CommandDraft) {
  return !command.name.trim() && !command.argvText.trim();
}

export function CommandEditor({
  commands,
  onChange,
  disabled,
  managementAction,
}: {
  commands: CommandDraft[];
  onChange: (commands: CommandDraft[]) => void;
  disabled: boolean;
  managementAction?: ReactNode;
}) {
  const { t } = useI18n();
  const [query, setQuery] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [presetsOpen, setPresetsOpen] = useState(false);
  const [selectedGroup, setSelectedGroup] = useState("git");
  const [packageManager, setPackageManager] = useState("pnpm");
  const group = COMMAND_GROUPS.find((item) => item.id === selectedGroup) ?? COMMAND_GROUPS[0];
  const presets =
    group.id === "node"
      ? ["build", "lint", "test"].map((script) => ({
          name: `${packageManager}_${script}`,
          argv: [packageManager, "run", script],
        }))
      : group.commands;
  const missing = presets.filter((preset) => !hasPreset(commands, preset));
  const capacity =
    32 - commands.filter((command) => !isEmpty(command) && command.status !== "blocked").length;
  const encoder = new TextEncoder();
  const bytes =
    4 +
    commands
      .filter((command) => !isEmpty(command))
      .reduce((size, command) => {
        const saved = toProjectCommands([command])[0];
        return (
          size +
          3 +
          (command.proposalJob ? 16 : 0) +
          8 +
          encoder.encode(saved.name).length +
          saved.argv.reduce((total, arg) => total + 4 + encoder.encode(arg).length, 0)
        );
      }, 0);
  const visible = commands.filter(
    (command) =>
      command.id === expanded ||
      `${command.name} ${command.argvText}`
        .toLocaleLowerCase()
        .includes(query.trim().toLocaleLowerCase()),
  );

  function addGroup() {
    if (!missing.length || missing.length > capacity) return;
    const next = [...commands];
    for (const preset of missing) {
      const blockedIndex = next.findIndex(
        (command) => command.status === "blocked" && command.name === preset.name,
      );
      const emptyIndex = blockedIndex >= 0 ? blockedIndex : next.findIndex(isEmpty);
      const draft = {
        name: preset.name,
        argvText: preset.argv.join("\n"),
        status: "allowed" as const,
        owner: "user" as const,
      };
      if (emptyIndex >= 0) next[emptyIndex] = { ...next[emptyIndex], ...draft };
      else next.push({ id: nextCommandDraftId(next), ...draft });
    }
    onChange(next);
    setQuery("");
    setExpanded(null);
    setPresetsOpen(false);
  }

  function update(id: string, patch: Partial<CommandDraft>) {
    onChange(
      commands.map((command) =>
        command.id === id ? { ...command, ...patch, owner: "user" } : command,
      ),
    );
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="text-xs font-medium text-muted-foreground">
          {t("settings.projects.commands")}{" "}
          <span className="tabular-nums">
            {32 - capacity}/32 · {bytes}/16384 B
          </span>
        </span>
        <div className="flex flex-wrap items-center gap-2">
          {managementAction}
          <Dialog open={presetsOpen} onOpenChange={setPresetsOpen}>
            <DialogTrigger
              render={<Button type="button" variant="outline" size="xs" />}
              disabled={disabled || capacity === 0}
            >
              <Plus aria-hidden="true" />
              {t("settings.projects.addGroups")}
            </DialogTrigger>
            <DialogContent className="flex max-h-[calc(100dvh-2rem)] flex-col gap-4 sm:max-w-2xl">
              <DialogHeader className="pr-8">
                <DialogTitle>{t("settings.projects.addGroups")}</DialogTitle>
                <DialogDescription>{t("settings.projects.commandGroupsNote")}</DialogDescription>
              </DialogHeader>
              <div className="ocg-subtle-scrollbar grid min-h-0 min-w-0 flex-1 gap-4 overflow-y-auto overscroll-contain sm:grid-cols-[11rem_1fr]">
                <div
                  className="grid content-start grid-cols-2 gap-1 sm:grid-cols-1"
                  role="group"
                  aria-label={t("settings.projects.addGroups")}
                >
                  {COMMAND_GROUPS.map((item) => (
                    <Button
                      key={item.id}
                      type="button"
                      variant={item.id === group.id ? "selected" : "ghost"}
                      aria-pressed={item.id === group.id}
                      className="h-auto justify-start px-3 py-3 text-left whitespace-normal tracking-normal normal-case"
                      onClick={() => setSelectedGroup(item.id)}
                    >
                      {t(item.title)}
                    </Button>
                  ))}
                </div>
                <div className="min-w-0 space-y-3">
                  <p className="text-sm text-muted-foreground">{t(group.description)}</p>
                  {group.id === "node" && (
                    <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
                      <span>{t("settings.projects.packageManager")}</span>
                      <div
                        role="group"
                        aria-label={t("settings.projects.packageManager")}
                        className="flex gap-1 rounded-md border border-border p-1"
                      >
                        {["pnpm", "npm"].map((manager) => (
                          <Button
                            key={manager}
                            type="button"
                            size="xs"
                            variant={packageManager === manager ? "selected" : "ghost"}
                            aria-pressed={packageManager === manager}
                            onClick={() => setPackageManager(manager)}
                            className="tracking-normal normal-case"
                          >
                            {manager}
                          </Button>
                        ))}
                      </div>
                    </div>
                  )}
                  <div className="divide-y divide-border rounded-md border border-border">
                    {presets.map((preset) => (
                      <div key={preset.name} className="flex items-start gap-2 px-3 py-2">
                        <div className="min-w-0 flex-1">
                          <div className="text-xs font-medium">{preset.name}</div>
                          <div className="font-mono text-xs break-words text-muted-foreground">
                            {preset.argv.join(" ")}
                          </div>
                        </div>
                        {hasPreset(commands, preset) && (
                          <span className="flex shrink-0 items-center gap-1 text-xs text-muted-foreground">
                            <Check className="size-3" aria-hidden="true" />
                            {t("settings.projects.commandPresetExists")}
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
              <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border pt-3">
                <p className="text-xs text-muted-foreground" role="status">
                  {t(
                    missing.length > capacity
                      ? "settings.projects.commandGroupCapacity"
                      : "settings.projects.commandGroupCount",
                    { count: missing.length, available: capacity },
                  )}
                </p>
                <Button
                  type="button"
                  size="sm"
                  disabled={disabled || !missing.length || missing.length > capacity}
                  onClick={addGroup}
                >
                  {t(
                    missing.length
                      ? "settings.projects.addGroup"
                      : "settings.projects.commandPresetExists",
                  )}
                </Button>
              </div>
            </DialogContent>
          </Dialog>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={
              disabled || commands.filter((command) => command.status !== "blocked").length >= 32
            }
            onClick={() => {
              const id = nextCommandDraftId(commands);
              onChange([...commands, { id, name: "", argvText: "" }]);
              setQuery("");
              setExpanded(id);
            }}
          >
            {t("settings.projects.addCommand")}
          </Button>
        </div>
      </div>
      {commands.length > 0 && (
        <div className="relative">
          <Search
            aria-hidden="true"
            className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground"
          />
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={t("settings.projects.searchCommands")}
            aria-label={t("settings.projects.searchCommands")}
            className="pl-9"
          />
        </div>
      )}
      {commands.length > 0 && (
        <div
          className="ocg-subtle-scrollbar max-h-96 min-w-0 overflow-y-auto overscroll-contain rounded-md border border-border"
          data-slot="project-command-list"
        >
          {!visible.length && (
            <p className="p-3 text-xs text-muted-foreground">
              {t("settings.projects.noMatchingCommands")}
            </p>
          )}
          {visible.map((command) => (
            <div key={command.id} className="border-b border-border last:border-b-0">
              <div className="flex items-center gap-1 pr-2">
                <button
                  type="button"
                  className="flex min-w-0 flex-1 items-center gap-2 px-3 py-2.5 text-left hover:bg-muted/50"
                  aria-expanded={expanded === command.id}
                  aria-controls={`editor-${command.id}`}
                  onClick={() => setExpanded(expanded === command.id ? null : command.id)}
                >
                  {expanded === command.id ? (
                    <ChevronDown className="size-4 shrink-0" aria-hidden="true" />
                  ) : (
                    <ChevronRight className="size-4 shrink-0" aria-hidden="true" />
                  )}
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-xs font-medium">
                      {command.name || t("settings.projects.untitledCommand")}
                    </span>
                    <span className="flex flex-wrap gap-1 py-1 text-[11px] text-muted-foreground">
                      <span className="rounded border border-border px-1.5">
                        {t(
                          command.owner === "decision"
                            ? "commands.ownerDecision"
                            : "commands.ownerUser",
                        )}
                      </span>
                      {command.status && command.status !== "allowed" && (
                        <span className="rounded border border-border px-1.5">
                          {t(command.status === "review" ? "commands.review" : "commands.blocked")}
                        </span>
                      )}
                    </span>
                    <span
                      className="block truncate font-mono text-xs text-muted-foreground"
                      title={command.argvText.split("\n").join(" ")}
                    >
                      {command.argvText.split("\n").join(" ") || t("settings.projects.commandArgv")}
                    </span>
                  </span>
                </button>
                {(command.owner === "decision" ||
                  command.status === "review" ||
                  command.status === "blocked") && (
                  <Button
                    type="button"
                    variant="outline"
                    size="xs"
                    disabled={disabled}
                    onClick={() => update(command.id, { owner: "user", status: "allowed" })}
                  >
                    {t("commands.grant")}
                  </Button>
                )}
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-xs"
                  disabled={disabled}
                  aria-label={t("settings.projects.removeCommandFor", {
                    name: command.name || t("settings.projects.untitledCommand"),
                  })}
                  title={t("settings.projects.removeCommand")}
                  className="text-muted-foreground hover:text-destructive"
                  onClick={() => {
                    onChange(commands.filter((item) => item.id !== command.id));
                    if (expanded === command.id) setExpanded(null);
                  }}
                >
                  <Trash2 aria-hidden="true" />
                </Button>
              </div>
              {expanded === command.id && (
                <div
                  id={`editor-${command.id}`}
                  className="space-y-2 border-t border-border bg-muted/20 p-3"
                >
                  {command.proposalJob && (
                    <p className="break-all text-xs text-muted-foreground">
                      {t("commands.assessmentJob")}: {command.proposalJob}
                    </p>
                  )}
                  <label className="flex flex-col gap-1 text-xs text-muted-foreground">
                    {t("settings.projects.commandName")}
                    <Input
                      value={command.name}
                      disabled={disabled}
                      onChange={(event) => update(command.id, { name: event.target.value })}
                      spellCheck={false}
                    />
                  </label>
                  <label className="flex flex-col gap-1 text-xs text-muted-foreground">
                    {t("settings.projects.commandArgv")}
                    <Textarea
                      value={command.argvText}
                      disabled={disabled}
                      onChange={(event) => update(command.id, { argvText: event.target.value })}
                      rows={4}
                      className="max-h-48 resize-y overflow-y-auto font-mono text-xs [field-sizing:fixed]"
                      spellCheck={false}
                    />
                  </label>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
      <p className="text-xs text-muted-foreground">{t("settings.projects.commandsNote")}</p>
    </div>
  );
}
