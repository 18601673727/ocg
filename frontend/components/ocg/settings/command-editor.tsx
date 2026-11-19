import { Check, ChevronDown, ChevronRight, Plus, Search, Trash2 } from "@lucide/solid";
import { createMemo, createSignal, For } from "solid-js";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { ariaState, type View } from "@/lib/ui-state";
import { type I18nKey, useI18n } from "../i18n";
import type { ProjectCommand } from "../runtime/api-client";
import { type CommandDraft, nextCommandDraftId, toProjectCommands } from "./command-drafts";

type CommandGroup = {
  id: string;
  title: I18nKey;
  description: I18nKey;
  commands: ProjectCommand[];
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

function hasPreset(commands: readonly CommandDraft[], preset: ProjectCommand) {
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

export function CommandEditor(props: {
  commands: CommandDraft[];
  onChange: (commands: CommandDraft[]) => void;
  disabled: boolean;
  managementAction?: View;
}) {
  const i18n = useI18n();
  const [query, setQuery] = createSignal("");
  const [expanded, setExpanded] = createSignal<string | null>(null);
  const [presetsOpen, setPresetsOpen] = createSignal(false);
  const [selectedGroup, setSelectedGroup] = createSignal("git");
  const [packageManager, setPackageManager] = createSignal("pnpm");
  const group = COMMAND_GROUPS.find((item) => item.id === selectedGroup()) ?? COMMAND_GROUPS[0];
  const presets =
    group.id === "node"
      ? ["build", "lint", "test"].map((script) => ({
          name: `${packageManager()}_${script}`,
          argv: [packageManager(), "run", script],
        }))
      : group.commands;
  const missing = presets.filter((preset) => !hasPreset(props.commands, preset));
  const capacity = createMemo(
    () =>
      32 -
      props.commands.filter((command) => !isEmpty(command) && command.status !== "blocked").length,
  );
  const encoder = new TextEncoder();
  const bytes = createMemo(
    () =>
      4 +
      props.commands
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
        }, 0),
  );
  const visible = createMemo(() =>
    props.commands.filter(
      (command) =>
        command.id === expanded() ||
        `${command.name} ${command.argvText}`
          .toLocaleLowerCase()
          .includes(query().trim().toLocaleLowerCase()),
    ),
  );

  function addGroup() {
    if (!missing.length || missing.length > capacity()) return;
    const next = [...props.commands];
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
    props.onChange(next);
    setQuery("");
    setExpanded(null);
    setPresetsOpen(false);
  }

  function update(id: string, patch: Partial<CommandDraft>) {
    props.onChange(
      props.commands.map((command) =>
        command.id === id ? { ...command, ...patch, owner: "user" } : command,
      ),
    );
  }

  return (
    <div class="flex flex-col gap-2">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <span class="text-xs font-medium text-muted-foreground">
          {i18n.t("settings.projects.commands")}{" "}
          <span class="tabular-nums">
            {32 - capacity()}/32 · {bytes()}/16384 B
          </span>
        </span>
        <div class="flex flex-wrap items-center gap-2">
          {props.managementAction}
          <Dialog open={presetsOpen()} onOpenChange={setPresetsOpen}>
            <DialogTrigger
              render={<Button type="button" variant="outline" size="xs" />}
              disabled={props.disabled || capacity() === 0}
            >
              <Plus aria-hidden="true" />
              {i18n.t("settings.projects.addGroups")}
            </DialogTrigger>
            <DialogContent class="flex max-h-[calc(100dvh-2rem)] flex-col gap-4 sm:max-w-2xl">
              <DialogHeader class="pr-8">
                <DialogTitle>{i18n.t("settings.projects.addGroups")}</DialogTitle>
                <DialogDescription>
                  {i18n.t("settings.projects.commandGroupsNote")}
                </DialogDescription>
              </DialogHeader>
              <div class="ocg-subtle-scrollbar grid min-h-0 min-w-0 flex-1 gap-4 overflow-y-auto overscroll-contain sm:grid-cols-[11rem_1fr]">
                <div
                  class="grid content-start grid-cols-2 gap-1 sm:grid-cols-1"
                  role="group"
                  aria-label={i18n.t("settings.projects.addGroups")}
                >
                  {COMMAND_GROUPS.map((item) => (
                    <Button
                      type="button"
                      variant={item.id === group.id ? "selected" : "ghost"}
                      aria-pressed={ariaState(item.id === group.id)}
                      class="h-auto justify-start px-3 py-3 text-left whitespace-normal tracking-normal normal-case"
                      onClick={() => setSelectedGroup(item.id)}
                    >
                      {i18n.t(item.title)}
                    </Button>
                  ))}
                </div>
                <div class="min-w-0 space-y-3">
                  <p class="text-sm text-muted-foreground">{i18n.t(group.description)}</p>
                  {group.id === "node" && (
                    <div class="flex items-center justify-between gap-2 text-xs text-muted-foreground">
                      <span>{i18n.t("settings.projects.packageManager")}</span>
                      <div
                        role="group"
                        aria-label={i18n.t("settings.projects.packageManager")}
                        class="flex gap-1 rounded-md border border-border p-1"
                      >
                        {["pnpm", "npm"].map((manager) => (
                          <Button
                            type="button"
                            size="xs"
                            variant={packageManager() === manager ? "selected" : "ghost"}
                            aria-pressed={ariaState(packageManager() === manager)}
                            onClick={() => setPackageManager(manager)}
                            class="tracking-normal normal-case"
                          >
                            {manager}
                          </Button>
                        ))}
                      </div>
                    </div>
                  )}
                  <div class="divide-y divide-border rounded-md border border-border">
                    {presets.map((preset) => (
                      <div class="flex items-start gap-2 px-3 py-2">
                        <div class="min-w-0 flex-1">
                          <div class="text-xs font-medium">{preset.name}</div>
                          <div class="font-mono text-xs break-words text-muted-foreground">
                            {preset.argv.join(" ")}
                          </div>
                        </div>
                        {hasPreset(props.commands, preset) && (
                          <span class="flex shrink-0 items-center gap-1 text-xs text-muted-foreground">
                            <Check class="size-3" aria-hidden="true" />
                            {i18n.t("settings.projects.commandPresetExists")}
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
              <div class="flex flex-wrap items-center justify-between gap-2 border-t border-border pt-3">
                <p class="text-xs text-muted-foreground" role="status">
                  {i18n.t(
                    missing.length > capacity()
                      ? "settings.projects.commandGroupCapacity"
                      : "settings.projects.commandGroupCount",
                    { count: missing.length, available: capacity() },
                  )}
                </p>
                <Button
                  type="button"
                  size="sm"
                  disabled={props.disabled || !missing.length || missing.length > capacity()}
                  onClick={addGroup}
                >
                  {i18n.t(
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
              props.disabled ||
              props.commands.filter((command) => command.status !== "blocked").length >= 32
            }
            onClick={() => {
              const id = nextCommandDraftId(props.commands);
              props.onChange([...props.commands, { id, name: "", argvText: "" }]);
              setQuery("");
              setExpanded(id);
            }}
          >
            {i18n.t("settings.projects.addCommand")}
          </Button>
        </div>
      </div>
      {props.commands.length > 0 && (
        <div class="relative">
          <Search
            aria-hidden="true"
            class="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground"
          />
          <Input
            value={query()}
            onInput={(event) => setQuery(event.target.value)}
            placeholder={i18n.t("settings.projects.searchCommands")}
            aria-label={i18n.t("settings.projects.searchCommands")}
            class="pl-9"
          />
        </div>
      )}
      {props.commands.length > 0 && (
        <div
          class="ocg-subtle-scrollbar max-h-96 min-w-0 overflow-y-auto overscroll-contain rounded-md border border-border"
          data-slot="project-command-list"
        >
          {!visible().length && (
            <p class="p-3 text-xs text-muted-foreground">
              {i18n.t("settings.projects.noMatchingCommands")}
            </p>
          )}
          <For each={visible()} keyed={(item) => item.id}>
            {(command) => (
              <div class="border-b border-border last:border-b-0">
                <div class="flex items-center gap-1 pr-2">
                  <button
                    type="button"
                    class="flex min-w-0 flex-1 items-center gap-2 px-3 py-2.5 text-left hover:bg-muted/50"
                    aria-expanded={ariaState(expanded() === command().id)}
                    aria-controls={`editor-${command().id}`}
                    onClick={() => setExpanded(expanded() === command().id ? null : command().id)}
                  >
                    {expanded() === command().id ? (
                      <ChevronDown class="size-4 shrink-0" aria-hidden="true" />
                    ) : (
                      <ChevronRight class="size-4 shrink-0" aria-hidden="true" />
                    )}
                    <span class="min-w-0 flex-1">
                      <span class="block truncate text-xs font-medium">
                        {command().name || i18n.t("settings.projects.untitledCommand")}
                      </span>
                      <span class="flex flex-wrap gap-1 py-1 text-[11px] text-muted-foreground">
                        <span class="rounded border border-border px-1.5">
                          {i18n.t(
                            command().owner === "decision"
                              ? "commands.ownerDecision"
                              : "commands.ownerUser",
                          )}
                        </span>
                        {command().status && command().status !== "allowed" && (
                          <span class="rounded border border-border px-1.5">
                            {i18n.t(
                              command().status === "review"
                                ? "commands.review"
                                : "commands.blocked",
                            )}
                          </span>
                        )}
                      </span>
                      <span
                        class="block truncate font-mono text-xs text-muted-foreground"
                        title={command().argvText.split("\n").join(" ")}
                      >
                        {command().argvText.split("\n").join(" ") ||
                          i18n.t("settings.projects.commandArgv")}
                      </span>
                    </span>
                  </button>
                  {(command().owner === "decision" ||
                    command().status === "review" ||
                    command().status === "blocked") && (
                    <Button
                      type="button"
                      variant="outline"
                      size="xs"
                      disabled={props.disabled}
                      onClick={() => update(command().id, { owner: "user", status: "allowed" })}
                    >
                      {i18n.t("commands.grant")}
                    </Button>
                  )}
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-xs"
                    disabled={props.disabled}
                    aria-label={i18n.t("settings.projects.removeCommandFor", {
                      name: command().name || i18n.t("settings.projects.untitledCommand"),
                    })}
                    title={i18n.t("settings.projects.removeCommand")}
                    class="text-muted-foreground hover:text-destructive"
                    onClick={() => {
                      props.onChange(props.commands.filter((item) => item.id !== command().id));
                      if (expanded() === command().id) setExpanded(null);
                    }}
                  >
                    <Trash2 aria-hidden="true" />
                  </Button>
                </div>
                {expanded() === command().id && (
                  <div
                    id={`editor-${command().id}`}
                    class="space-y-2 border-t border-border bg-muted/20 p-3"
                  >
                    {command().proposalJob && (
                      <p class="break-all text-xs text-muted-foreground">
                        {i18n.t("commands.assessmentJob")}: {command().proposalJob}
                      </p>
                    )}
                    <label class="flex flex-col gap-1 text-xs text-muted-foreground">
                      {i18n.t("settings.projects.commandName")}
                      <Input
                        value={command().name}
                        disabled={props.disabled}
                        onInput={(event) => update(command().id, { name: event.target.value })}
                        spellcheck="false"
                      />
                    </label>
                    <label class="flex flex-col gap-1 text-xs text-muted-foreground">
                      {i18n.t("settings.projects.commandArgv")}
                      <Textarea
                        value={command().argvText}
                        disabled={props.disabled}
                        onInput={(event) => update(command().id, { argvText: event.target.value })}
                        rows={4}
                        class="max-h-48 resize-y overflow-y-auto font-mono text-xs [field-sizing:fixed]"
                        spellcheck="false"
                      />
                    </label>
                  </div>
                )}
              </div>
            )}
          </For>
        </div>
      )}
      <p class="text-xs text-muted-foreground">{i18n.t("settings.projects.commandsNote")}</p>
    </div>
  );
}
